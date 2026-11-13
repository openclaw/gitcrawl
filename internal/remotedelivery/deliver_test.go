//go:build darwin || linux

package remotedelivery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/gitcrawl/internal/store"
)

type deliveryToken func(context.Context) (string, error)

func (f deliveryToken) Token(ctx context.Context) (string, error) { return f(ctx) }

func deliveryLimits() DeliveryLimits {
	return DeliveryLimits{MaxParts: 100, MaxRows: 10000, MaxBytes: 32 << 20}
}

func deliveryFixture(t *testing.T, ctx context.Context, native *store.Store, origin string) (*Outbox, string, [][]byte) {
	t.Helper()
	c, _ := projected(t, ctx, native)
	r := seedRegistration(c)
	r.Origin = origin
	path := filepath.Join(t.TempDir(), "delivery.sqlite")
	o, err := CreateSeedOutbox(ctx, path, r, c, StageLimits{MaxEncodedBytes: 32 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { o.Close() })
	rows, err := o.base.DB().QueryContext(ctx, "select body from parts order by part_index")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var bodies [][]byte
	for rows.Next() {
		var b []byte
		if err := rows.Scan(&b); err != nil {
			t.Fatal(err)
		}
		bodies = append(bodies, b)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return o, path, bodies
}

func serveSeedReceipt(t *testing.T, w http.ResponseWriter, r *http.Request, bodies [][]byte, seen *sync.Map) int {
	t.Helper()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		t.Error(err)
		w.WriteHeader(500)
		return -1
	}
	index := -1
	for i, b := range bodies {
		if bytes.Equal(body, b) {
			index = i
			break
		}
	}
	if index < 0 {
		t.Error("request bytes differ from stored bodies")
		w.WriteHeader(400)
		return -1
	}
	if r.Method != http.MethodPost || r.Header.Get("Authorization") != "Bearer fixture" {
		t.Error("wrong authenticated method")
	}
	var m remote.GitHubSeedManifest
	if err := json.Unmarshal(bodies[0], &m); err != nil {
		t.Error(err)
		w.WriteHeader(500)
		return -1
	}
	_, replayed := seen.LoadOrStore(index, true)
	batch := seedPartBatch(m.SeedID, index, len(bodies))
	_ = json.NewEncoder(w).Encode(remote.SeedReceipt{App: "gitcrawl", Archive: m.Archive, BindingID: m.BindingID, BatchID: batch, Cursor: batch, Sequence: strconv.Itoa(index + 1), PayloadSHA256: privateSeedHash(body), Replayed: replayed})
	return index
}

func TestDeliverSeedExactHTTPResumeAndSourceUnchanged(t *testing.T) {
	ctx, native := nativeFixture(t)
	// This standalone source proves file preservation without WAL housekeeping.
	// The purge/redirect tests keep a live WAL source so current policy is read.
	if _, err := native.DB().ExecContext(ctx, "pragma journal_mode=DELETE"); err != nil {
		t.Fatal(err)
	}
	if err := native.WithTx(ctx, func(tx *store.Store) error {
		// The packer/outbox tests own the 500-row boundary. Two comments still
		// exercise every sender route without repeating their expensive fixture.
		for i := 0; i < 2; i++ {
			if _, err := tx.UpsertComment(ctx, store.Comment{ThreadID: 1, GitHubID: strconv.Itoa(i + 100), CommentType: "issue_comment", Body: "private\x00☃", RawJSON: `{"node_id":"comment-node"}`}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var bodies [][]byte
	seen := &sync.Map{}
	var requests atomic.Int64
	var fail atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if fail.Swap(false) {
			_, _ = io.Copy(io.Discard, r.Body)
			w.WriteHeader(503)
			return
		}
		serveSeedReceipt(t, w, r, bodies, seen)
	}))
	defer server.Close()
	o, path, bodies := deliveryFixture(t, ctx, native, server.URL)
	if err := native.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(native.Path())
	if err != nil {
		t.Fatal(err)
	}
	limit := deliveryLimits()
	limit.MaxParts = 1
	client := &http.Client{}
	result, err := o.DeliverSeed(ctx, native.Path(), remote.StaticToken("fixture"), client, limit)
	if err != nil || result.Receipts != 1 || result.Complete {
		t.Fatalf("first part: %+v %v", result, err)
	}
	if client.Transport != nil || client.Timeout != 0 || client.CheckRedirect != nil {
		t.Fatal("delivery mutated the caller's HTTP client")
	}
	pending := nextSeedPart(t, ctx, o)
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	o, err = OpenSeedOutbox(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	fail.Store(true)
	result, err = o.DeliverSeed(ctx, native.Path(), remote.StaticToken("fixture"), nil, deliveryLimits())
	if err == nil || result.Receipts != 0 || requests.Load() != 2 {
		t.Fatalf("uncertain send retried: %+v %v %d", result, err, requests.Load())
	}
	afterFailure := nextSeedPart(t, ctx, o)
	if !bytes.Equal(pending.Body, afterFailure.Body) || pending.Ref != afterFailure.Ref {
		t.Fatal("uncertainty changed pending bytes")
	}
	result, err = o.DeliverSeed(ctx, native.Path(), remote.StaticToken("fixture"), nil, deliveryLimits())
	if err != nil || !result.Complete || result.Receipts != len(bodies)-1 {
		t.Fatalf("resume: %+v %v", result, err)
	}
	if outboxState(t, ctx, o).BaselineState != BaselineQualified {
		t.Fatal("missing final baseline")
	}
	completed, err := o.DeliverSeed(ctx, "/absent-source", nil, nil, deliveryLimits())
	if err != nil || !completed.Complete || completed.Receipts != 0 {
		t.Fatalf("completed delivery did more work: %+v %v", completed, err)
	}
	after, err := os.ReadFile(native.Path())
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("delivery changed native database", err)
	}
	for _, suffix := range []string{"-wal", "-shm"} {
		if _, err := os.Stat(native.Path() + suffix); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("retained native sidecar", suffix, err)
		}
	}
}

func TestDeliverSeedBoundsBeforeCredentials(t *testing.T) {
	ctx, native := nativeFixture(t)
	o, _, _ := deliveryFixture(t, ctx, native, "https://example.test")
	calls := 0
	token := deliveryToken(func(context.Context) (string, error) { calls++; return "fixture", nil })
	for _, limits := range []DeliveryLimits{
		{MaxParts: 1, MaxRows: 1, MaxBytes: 32 << 20},
		{MaxParts: 1, MaxRows: 10000, MaxBytes: 1},
		{MaxParts: 0, MaxRows: 10000, MaxBytes: 32 << 20},
	} {
		if _, err := o.DeliverSeed(ctx, native.Path(), token, nil, limits); err == nil {
			t.Fatal("invalid or insufficient budget accepted")
		}
	}
	if _, err := o.DeliverSeed(context.Background(), native.Path(), token, nil, deliveryLimits()); err == nil {
		t.Fatal("missing deadline accepted")
	}
	if calls != 0 || outboxState(t, ctx, o).Receipts != 0 {
		t.Fatal("preflight refusal invoked credentials or settled a receipt")
	}
}

func TestDeliverSeedChildChunkRechecksRetainedParentNode(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(strconv.FormatBool(missing), func(t *testing.T) {
			ctx, native := nativeFixture(t)
			if _, err := native.DB().ExecContext(ctx, `update threads set raw_json='{"node_id":"parent-node"}' where id=1`); err != nil {
				t.Fatal(err)
			}
			if _, err := native.UpsertComment(ctx, store.Comment{ThreadID: 1, GitHubID: "child", CommentType: "issue_comment", Body: "private child", RawJSON: `{"node_id":"child-node"}`}); err != nil {
				t.Fatal(err)
			}
			var bodies [][]byte
			var requests atomic.Int64
			seen := &sync.Map{}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				serveSeedReceipt(t, w, r, bodies, seen)
			}))
			defer server.Close()
			o, _, bodies := deliveryFixture(t, ctx, native, server.URL)
			plan, err := remote.DecodeGitHubSeedManifest(bodies[0])
			if err != nil {
				t.Fatal(err)
			}
			childPart := -1
			for index := 1; index < len(bodies)-1; index++ {
				chunk, err := plan.ValidateChunk(bodies[index])
				if err != nil {
					t.Fatal(err)
				}
				if chunk.Changes[0].Table == "comments" {
					childPart = index
					break
				}
			}
			if childPart < 1 {
				t.Fatal("fixture lacks a child-only chunk")
			}
			limits := deliveryLimits()
			limits.MaxParts = childPart
			if result, err := o.DeliverSeed(ctx, native.Path(), remote.StaticToken("fixture"), nil, limits); err != nil || result.Receipts != childPart || result.Complete {
				t.Fatal("seed prefix did not settle", result, err)
			}
			pending := nextSeedPart(t, ctx, o)
			before := requests.Load()
			token := deliveryToken(func(context.Context) (string, error) {
				if missing {
					if _, err := native.DB().ExecContext(ctx, `delete from threads where id=1`); err != nil {
						return "", err
					}
				}
				_, err := native.DB().ExecContext(ctx, `insert into thread_exclusions values('other/repo',90,'issue',90,'other','now','owner_requested','fixture'); insert into thread_excluded_nodes values('parent-node','other/repo',90)`)
				return "fixture", err
			})
			limits.MaxParts = 1
			result, err := o.DeliverSeed(ctx, native.Path(), token, nil, limits)
			if !errors.Is(err, store.ErrThreadExcluded) || result.Receipts != 0 || result.Complete || requests.Load() != before {
				t.Fatal("child chunk escaped current global parent-node policy", result, err)
			}
			after := nextSeedPart(t, ctx, o)
			if after.Ref != pending.Ref || !bytes.Equal(after.Body, pending.Body) || outboxState(t, ctx, o).BaselineState != BaselineUnqualified {
				t.Fatal("parent-node hold advanced seed custody")
			}
		})
	}
}

func TestDeliverSeedParentNodeAffinityAndExactRowBudget(t *testing.T) {
	ctx, native := nativeFixture(t)
	cases := []struct {
		raw, node string
		valid     bool
	}{
		{`{}`, "", false},
		{`{"node_id":null}`, "", false},
		{`not-json`, "", false},
		{`{"node_id":""}`, "", true},
		{`{"node_id":"node\u0000suffix"}`, "node\x00suffix", true},
		{`{"node_id":9007199254740993}`, "9007199254740993", true},
		{`{"node_id":123.0}`, "123.0", true},
	}
	ids := make([]int64, len(cases))
	for i, item := range cases {
		id, err := native.UpsertThread(ctx, store.Thread{RepoID: 1, GitHubID: strconv.Itoa(i + 100), Number: i + 100, Kind: "issue", State: "open", Title: "retained", RawJSON: item.raw, LabelsJSON: "[]", AssigneesJSON: "[]", ContentHash: strconv.Itoa(i), UpdatedAt: "2026-01-01"})
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = id
		if _, err := native.UpsertComment(ctx, store.Comment{ThreadID: id, GitHubID: strconv.Itoa(i + 100), CommentType: "issue_comment", RawJSON: "{}"}); err != nil {
			t.Fatal(err)
		}
	}
	// Enough child rows make completion require three evidence entries per
	// child; a two-times allowance would reject the exact native-row budget.
	for i := 0; i < 10; i++ {
		if _, err := native.UpsertComment(ctx, store.Comment{ThreadID: ids[len(ids)-1], GitHubID: strconv.Itoa(i + 200), CommentType: "issue_comment", RawJSON: "{}"}); err != nil {
			t.Fatal(err)
		}
	}
	var bodies [][]byte
	seen := &sync.Map{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { serveSeedReceipt(t, w, r, bodies, seen) }))
	defer server.Close()
	o, _, bodies := deliveryFixture(t, ctx, native, server.URL)
	// Parent extraction runs inside the existing streaming transaction, not
	// through another connection that would deadlock a one-connection outbox.
	o.base.DB().SetMaxOpenConns(1)
	limits := deliveryLimits()
	limits.MaxRows = outboxState(t, ctx, o).NativeRows
	limits.MaxBytes = 0
	for _, body := range bodies {
		limits.MaxBytes += int64(len(body))
	}
	d, err := o.prepareDelivery(ctx, limits)
	if err != nil {
		t.Fatal(err)
	}
	for i, item := range cases {
		parent := d.parents[ids[i]]
		if parent.number != int64(i+100) || parent.node.Valid != item.valid || parent.node.String != item.node {
			t.Fatalf("native SQLite parent identity mismatch at fixture %d", i)
		}
	}
	result, err := o.DeliverSeed(ctx, native.Path(), remote.StaticToken("fixture"), nil, limits)
	if err != nil || !result.Complete || result.Receipts != len(bodies) {
		t.Fatal("exact seed bounds failed", result, err)
	}
}

func purgeDeliveryThread(t *testing.T, ctx context.Context, native *store.Store) {
	purgeDeliveryNumber(t, ctx, native, 10)
}

func purgeDeliveryNumber(t *testing.T, ctx context.Context, native *store.Store, number int) {
	t.Helper()
	plan, err := native.PlanThreadPurge(ctx, "fixture/repo", []int{number})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = native.PurgeThreads(ctx, "fixture/repo", []int{number}, plan.PlanID); err != nil {
		t.Fatal(err)
	}
}

func prepareDeliveryPurge(t *testing.T, ctx context.Context, native *store.Store) {
	t.Helper()
	// The first real purge installs owner guards and changes schema_cookie.
	// Arm those before staging so later rejection proves policy, not DDL drift.
	if _, err := native.UpsertThread(ctx, store.Thread{RepoID: 1, GitHubID: "three", Number: 30, Kind: "issue", State: "open", Title: "retained", RawJSON: "{}", LabelsJSON: "[]", AssigneesJSON: "[]", ContentHash: "three", UpdatedAt: "2026-01-01"}); err != nil {
		t.Fatal(err)
	}
	purgeDeliveryNumber(t, ctx, native, 20)
}

func TestDeliverSeedCompletionRechecksAcknowledgedContent(t *testing.T) {
	ctx, native := nativeFixture(t)
	prepareDeliveryPurge(t, ctx, native)
	var bodies [][]byte
	seen := &sync.Map{}
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests.Add(1); serveSeedReceipt(t, w, r, bodies, seen) }))
	defer server.Close()
	o, _, bodies := deliveryFixture(t, ctx, native, server.URL)
	limit := deliveryLimits()
	limit.MaxParts = len(bodies) - 1
	result, err := o.DeliverSeed(ctx, native.Path(), remote.StaticToken("fixture"), nil, limit)
	if err != nil || result.Complete || result.Receipts != len(bodies)-1 {
		t.Fatalf("precompletion: %+v %v", result, err)
	}
	before := requests.Load()
	token := deliveryToken(func(ctx context.Context) (string, error) { purgeDeliveryThread(t, ctx, native); return "fixture", nil })
	result, err = o.DeliverSeed(ctx, native.Path(), token, nil, deliveryLimits())
	if !errors.Is(err, store.ErrThreadExcluded) || requests.Load() != before || result.Complete || result.Receipts != 0 {
		t.Fatalf("completion ignored current policy: %+v %v", result, err)
	}
	if outboxState(t, ctx, o).BaselineState != BaselineUnqualified {
		t.Fatal("purged seed qualified")
	}
}

func TestDeliverSeedRedirectRechecksPolicy(t *testing.T) {
	ctx, native := nativeFixture(t)
	prepareDeliveryPurge(t, ctx, native)
	var bodies [][]byte
	seen := &sync.Map{}
	var requests atomic.Int64
	var redirect atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if redirect.Swap(false) {
			purgeDeliveryThread(t, ctx, native)
			w.Header().Set("Location", r.URL.String()+"?again=1")
			w.WriteHeader(http.StatusTemporaryRedirect)
			return
		}
		serveSeedReceipt(t, w, r, bodies, seen)
	}))
	defer server.Close()
	o, _, bodies := deliveryFixture(t, ctx, native, server.URL)
	limit := deliveryLimits()
	limit.MaxParts = 1
	for {
		p := nextSeedPart(t, ctx, o)
		if p.Ref.Index > 0 {
			c, err := remote.DecodeGitHubSeedChunk(p.Body)
			if err == nil && len(c.Changes) > 0 && c.Changes[0].Table == "threads" {
				break
			}
		}
		if _, err := o.DeliverSeed(ctx, native.Path(), remote.StaticToken("fixture"), nil, limit); err != nil {
			t.Fatal(err)
		}
	}
	before := requests.Load()
	redirect.Store(true)
	result, err := o.DeliverSeed(ctx, native.Path(), remote.StaticToken("fixture"), nil, limit)
	if !errors.Is(err, store.ErrThreadExcluded) || requests.Load() != before+1 || result.Receipts != 0 {
		t.Fatalf("redirect bypassed policy: %+v %v requests=%d", result, err, requests.Load()-before)
	}
}
