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
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/gitcrawl/internal/store"
)

func changeSenderFixture(t *testing.T, origin string) (context.Context, *store.Store, *Outbox, string) {
	t.Helper()
	_, native := nativeFixture(t)
	// Includes the actual native 500-row boundary under race instrumentation.
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	if _, err := native.UpsertComment(ctx, store.Comment{ThreadID: 1, GitHubID: "baseline", CommentType: "issue_comment", Body: "private\x00☃", RawJSON: `{"node_id":"baseline-node"}`}); err != nil {
		t.Fatal(err)
	}
	if err := native.PrepareNativeReportIndexes(ctx); err != nil {
		t.Fatal(err)
	}
	o, path, _ := deliveryFixture(t, ctx, native, origin)
	completeInventorySeed(t, ctx, o)
	if err := MigrateSeedOutboxV2(ctx, path, inventoryTestLimits); err != nil {
		t.Fatal(err)
	}
	if err := MigrateReportOutboxV3(ctx, path); err != nil {
		t.Fatal(err)
	}
	return ctx, native, o, path
}

func stageSenderChanges(t *testing.T, ctx context.Context, native *store.Store, o *Outbox) (string, ReportOutboxState, store.NativeReportDelta) {
	t.Helper()
	state, delta := captureChanges(t, ctx, native, o)
	id, err := o.StageReportChanges(ctx, state, delta, changeTestLimits)
	if err != nil {
		t.Fatal(err)
	}
	return id, state, delta
}

type changeSenderServer struct {
	t         *testing.T
	mu        sync.Mutex
	parts     map[string]PendingChangePart
	sequences map[string]int64
	seen      map[string]bool
	requests  atomic.Int64
	drop      atomic.Bool
	wrong     atomic.Bool
}

func (s *changeSenderServer) remember(ctx context.Context, o *Outbox, id string, base int64) {
	s.t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.parts == nil {
		s.parts = make(map[string]PendingChangePart)
		s.sequences = make(map[string]int64)
		s.seen = make(map[string]bool)
	}
	c, err := readChangeCapture(ctx, o.base.DB(), id)
	if err != nil {
		s.t.Fatal(err)
	}
	for i := 0; i < c.PartCount; i++ {
		p, _, err := readChangePart(ctx, o.base.DB(), ChangePartRef{CaptureID: id, Index: i}, c)
		if err != nil {
			s.t.Fatal(err)
		}
		key := changePartID(id, i)
		s.parts[key] = p
		s.sequences[key] = base
	}
}

func (s *changeSenderServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.requests.Add(1)
	body, err := io.ReadAll(io.LimitReader(r.Body, remote.MaxChangeBytes+1))
	if err != nil {
		s.t.Error(err)
		w.WriteHeader(400)
		return
	}
	b, err := remote.DecodeChangeBatch(body)
	if err != nil {
		s.t.Error("invalid request")
		w.WriteHeader(400)
		return
	}
	s.mu.Lock()
	p, ok := s.parts[b.BatchID()]
	base := s.sequences[b.BatchID()]
	replayed := s.seen[b.BatchID()]
	s.seen[b.BatchID()] = true
	s.mu.Unlock()
	if !ok || !bytes.Equal(body, p.Body) || r.Method != http.MethodPost || r.Header.Get("Authorization") != "Bearer fixture" || r.URL.Path != "/v1/apps/gitcrawl/archives/fixture%2Farchive/changes" && r.URL.EscapedPath() != "/v1/apps/gitcrawl/archives/fixture%2Farchive/changes" {
		s.t.Error("request route/auth/exact bytes differ from registered staged part")
		w.WriteHeader(400)
		return
	}
	if s.drop.Swap(false) {
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			s.t.Error(err)
			return
		}
		conn.Close()
		return
	}
	receipt := changeReceipt(s.t, &p, base)
	receipt.Replayed = replayed
	if s.wrong.Swap(false) {
		receipt.Sequence = strconv.FormatInt(base+99, 10)
	}
	_ = json.NewEncoder(w).Encode(receipt)
}

func TestDeliverReportChangesExactResumeFinalPolicyAndCandidateParents(t *testing.T) {
	receiver := &changeSenderServer{t: t}
	server := httptest.NewServer(receiver)
	defer server.Close()
	ctx, native, o, path := changeSenderFixture(t, server.URL)
	seed := inventorySeedEvidence(t, ctx, o)
	// Comments sort before their renumbered parent. The first part contains
	// another parent's node; the final part contains only parent 1's content.
	if _, err := native.DB().ExecContext(ctx, `update threads set number=15 where id=1;
with recursive n(x) as (values(1) union all select x+1 from n where x<500)
insert into comments(id,thread_id,github_id,comment_type,body,raw_json)
select x+100,case when x=1 then 2 else 1 end,'added-'||x,'issue_comment','native private body',case when x=1 then '{"node_id":"earlier-node"}' else '{}' end from n`); err != nil {
		t.Fatal(err)
	}
	id, expected, delta := stageSenderChanges(t, ctx, native, o)
	atStage, _ := projected(t, ctx, native)
	receiver.remember(ctx, o, id, expected.ReceiverSequence)
	first := pendingChange(t, ctx, o)
	if first.PartCount != 2 {
		t.Fatal("fixture did not produce two parts")
	}
	if _, err := native.DB().ExecContext(ctx, `insert into thread_exclusions values('fixture/repo',10,'issue',1,'one','now','owner_requested','fixture')`); err != nil {
		t.Fatal(err)
	}
	// Old number 10 is now excluded. Candidate parent number 15 must be used.
	limit := deliveryLimits()
	limit.MaxParts = 1
	receiver.drop.Store(true)
	result, err := o.DeliverReportChanges(ctx, native.Path(), remote.StaticToken("fixture"), nil, limit)
	if !errors.Is(err, ErrReportDelivery) || result.CaptureID != id || result.Receipts != 0 || result.Complete || receiver.requests.Load() != 1 {
		t.Fatal("lost reply retried or settled", result, err)
	}
	if p := pendingChange(t, ctx, o); p.Ref != first.Ref || !bytes.Equal(p.Body, first.Body) {
		t.Fatal("lost reply changed pending bytes")
	}
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	o, err = OpenReportOutbox(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	client := &http.Client{}
	result, err = o.DeliverReportChanges(ctx, native.Path(), remote.StaticToken("fixture"), client, limit)
	if err != nil || result.Receipts != 1 || result.Complete || result.CaptureID != id || receiver.requests.Load() != 2 {
		t.Fatal("reopen exact replay failed", result, err)
	}
	if client.Transport != nil || client.Timeout != 0 || client.CheckRedirect != nil {
		t.Fatal("sender mutated caller client")
	}
	state := reportState(t, ctx, path)
	state.ActiveChangeID = ""
	if state != expected {
		t.Fatal("partial receipt advanced qualified baseline")
	}
	token := deliveryToken(func(context.Context) (string, error) {
		_, err := native.DB().ExecContext(ctx, `insert into thread_exclusions values('other/repo',90,'issue',90,'other','now','owner_requested','fixture');insert into thread_excluded_nodes values('earlier-node','other/repo',90)`)
		return "fixture", err
	})
	result, err = o.DeliverReportChanges(ctx, native.Path(), token, nil, deliveryLimits())
	if !errors.Is(err, store.ErrThreadExcluded) || result.Receipts != 0 || result.Complete || receiver.requests.Load() != 2 {
		t.Fatal("final part failed to recheck earlier upserts after credentials", result, err)
	}
	if _, err := native.DB().ExecContext(ctx, `delete from thread_excluded_nodes where node_id='earlier-node'`); err != nil {
		t.Fatal(err)
	}
	result, err = o.DeliverReportChanges(ctx, native.Path(), remote.StaticToken("fixture"), nil, deliveryLimits())
	if err != nil || !result.Complete || result.CaptureID != id || result.Receipts != 1 || receiver.requests.Load() != 3 {
		t.Fatal("final part failed", result, err)
	}
	final := reportState(t, ctx, path)
	if final.ActiveChangeID != "" || final.BaselineAnchor != delta.Through || final.InventorySHA256 != independentInventoryDigest(t, ctx, atStage) {
		t.Fatal("final receipt did not promote exact captured inventory")
	}
	if !reflect.DeepEqual(seed, inventorySeedEvidence(t, ctx, o)) {
		t.Fatal("delivery changed retained seed evidence")
	}
	result, err = o.DeliverReportChanges(ctx, "/absent-source", nil, nil, deliveryLimits())
	if err != nil || result != (ChangeDeliveryResult{}) {
		t.Fatal("no-pending invocation touched source or credentials", result, err)
	}
}

func TestDeliverReportChangesRemovesExcludedAbsentRows(t *testing.T) {
	receiver := &changeSenderServer{t: t}
	server := httptest.NewServer(receiver)
	defer server.Close()
	ctx, native, o, path := changeSenderFixture(t, server.URL)
	if _, err := native.DB().ExecContext(ctx, `insert into thread_exclusions values('fixture/repo',10,'issue',1,'one','now','owner_requested','fixture');delete from threads where id=1`); err != nil {
		t.Fatal(err)
	}
	id, expected, delta := stageSenderChanges(t, ctx, native, o)
	if len(delta.Removals) < 2 {
		t.Fatal("fixture missing old parent and child removals")
	}
	receiver.remember(ctx, o, id, expected.ReceiverSequence)
	result, err := o.DeliverReportChanges(ctx, native.Path(), remote.StaticToken("fixture"), nil, deliveryLimits())
	if err != nil || !result.Complete || result.Receipts != 1 {
		t.Fatal("source exclusion prevented bodyless retraction", result, err)
	}
	if reportState(t, ctx, path).ActiveChangeID != "" {
		t.Fatal("removal receipt did not settle")
	}
	// A separate all-removal capture still needs current source incarnation.
	if _, err := native.DB().ExecContext(ctx, `delete from threads where id=2`); err != nil {
		t.Fatal(err)
	}
	id, expected, delta = stageSenderChanges(t, ctx, native, o)
	receiver.remember(ctx, o, id, expected.ReceiverSequence)
	if len(delta.Upserts) != 0 || len(delta.Removals) != 1 {
		t.Fatal("fixture not removal-only")
	}
	if _, err := native.DB().ExecContext(ctx, `update change_contract_state set generation=generation+1`); err != nil {
		t.Fatal(err)
	}
	before := receiver.requests.Load()
	result, err = o.DeliverReportChanges(ctx, native.Path(), remote.StaticToken("fixture"), nil, deliveryLimits())
	if !errors.Is(err, store.ErrNativeReportSource) || result.Receipts != 0 || receiver.requests.Load() != before {
		t.Fatal("removal-only send skipped incarnation check", result, err)
	}
}

func TestDeliverReportChangesChildOnlyRechecksRetainedParentNode(t *testing.T) {
	receiver := &changeSenderServer{t: t}
	server := httptest.NewServer(receiver)
	defer server.Close()
	ctx, native, o, path := changeSenderFixture(t, server.URL)
	if _, err := native.DB().ExecContext(ctx, `update threads set raw_json='{"node_id":"parent-node"}' where id=1`); err != nil {
		t.Fatal(err)
	}
	id, expected, _ := stageSenderChanges(t, ctx, native, o)
	receiver.remember(ctx, o, id, expected.ReceiverSequence)
	if result, err := o.DeliverReportChanges(ctx, native.Path(), remote.StaticToken("fixture"), nil, deliveryLimits()); err != nil || !result.Complete {
		t.Fatal("parent baseline did not settle", result, err)
	}
	if _, err := native.DB().ExecContext(ctx, `update comments set body='child-only edit' where id=1`); err != nil {
		t.Fatal(err)
	}
	id, expected, delta := stageSenderChanges(t, ctx, native, o)
	if len(delta.Upserts) != 1 || delta.Upserts[0].Key.Table != "comments" || len(delta.Removals) != 0 {
		t.Fatal("fixture includes parent content")
	}
	receiver.remember(ctx, o, id, expected.ReceiverSequence)
	before := receiver.requests.Load()
	token := deliveryToken(func(context.Context) (string, error) {
		_, err := native.DB().ExecContext(ctx, `insert into thread_exclusions values('other/repo',90,'issue',90,'other','now','owner_requested','fixture'); insert into thread_excluded_nodes values('parent-node','other/repo',90)`)
		return "fixture", err
	})
	result, err := o.DeliverReportChanges(ctx, native.Path(), token, nil, deliveryLimits())
	if !errors.Is(err, store.ErrThreadExcluded) || result.Receipts != 0 || result.Complete || receiver.requests.Load() != before {
		t.Fatal("child-only send escaped current global parent-node policy", result, err)
	}
	state := reportState(t, ctx, path)
	state.ActiveChangeID = ""
	if state != expected || pendingChange(t, ctx, o).Ref.CaptureID != id {
		t.Fatal("parent-node hold advanced custody")
	}
}

func TestDeliverReportChangesCandidateParentNodeOverrides(t *testing.T) {
	for _, mode := range []string{"null", "empty", "replacement", "removed"} {
		t.Run(mode, func(t *testing.T) {
			receiver := &changeSenderServer{t: t}
			server := httptest.NewServer(receiver)
			defer server.Close()
			ctx, native, o, _ := changeSenderFixture(t, server.URL)
			if _, err := native.DB().ExecContext(ctx, `update threads set raw_json='{"node_id":"old-parent"}' where id=1`); err != nil {
				t.Fatal(err)
			}
			id, state, _ := stageSenderChanges(t, ctx, native, o)
			receiver.remember(ctx, o, id, state.ReceiverSequence)
			if result, err := o.DeliverReportChanges(ctx, native.Path(), remote.StaticToken("fixture"), nil, deliveryLimits()); err != nil || !result.Complete {
				t.Fatal(result, err)
			}
			raw := `{}`
			if mode == "empty" {
				raw = `{"node_id":""}`
			}
			if mode == "replacement" {
				raw = `{"node_id":"new-parent"}`
			}
			if _, err := native.DB().ExecContext(ctx, `update threads set raw_json=? where id=1;update comments set body='child edit' where id=1`, raw); err != nil {
				t.Fatal(err)
			}
			id, state, _ = stageSenderChanges(t, ctx, native, o)
			receiver.remember(ctx, o, id, state.ReceiverSequence)
			if mode == "removed" {
				// Stage rejects this inconsistency; a later damaged mutation must also
				// hold rather than falling back to the still-retained baseline parent.
				if _, err := o.base.DB().ExecContext(ctx, `update change_mutations set operation='remove' where capture_id=? and table_name='threads'`, id); err != nil {
					t.Fatal(err)
				}
			}
			token := deliveryToken(func(context.Context) (string, error) {
				_, err := native.DB().ExecContext(ctx, `insert into thread_exclusions values('other/repo',90,'issue',90,'other','now','owner_requested','fixture');insert into thread_excluded_nodes values('old-parent','other/repo',90)`)
				return "fixture", err
			})
			before := receiver.requests.Load()
			result, err := o.DeliverReportChanges(ctx, native.Path(), token, nil, deliveryLimits())
			if mode == "removed" {
				if !errors.Is(err, ErrSeedOutbox) || receiver.requests.Load() != before || result.Receipts != 0 {
					t.Fatal("removed parent fell back to old inventory", result, err)
				}
			} else if err != nil || !result.Complete || result.Receipts != 1 {
				t.Fatal("candidate override fell back to excluded old node", result, err)
			}
		})
	}
}

func TestDeliverReportChangesParentNodeReadBoundAndPlan(t *testing.T) {
	receiver := &changeSenderServer{t: t}
	server := httptest.NewServer(receiver)
	defer server.Close()
	ctx, native, o, _ := changeSenderFixture(t, server.URL)
	raw, _ := json.Marshal(map[string]string{"node_id": strings.Repeat("large-node", 1000)})
	if _, err := native.DB().ExecContext(ctx, `update threads set raw_json=? where id=1`, string(raw)); err != nil {
		t.Fatal(err)
	}
	id, state, _ := stageSenderChanges(t, ctx, native, o)
	receiver.remember(ctx, o, id, state.ReceiverSequence)
	if result, err := o.DeliverReportChanges(ctx, native.Path(), remote.StaticToken("fixture"), nil, deliveryLimits()); err != nil || !result.Complete {
		t.Fatal(result, err)
	}
	if _, err := native.DB().ExecContext(ctx, `update comments set body='child edit' where id=1`); err != nil {
		t.Fatal(err)
	}
	id, state, _ = stageSenderChanges(t, ctx, native, o)
	receiver.remember(ctx, o, id, state.ReceiverSequence)
	limits := deliveryLimits()
	limits.MaxBytes = 4096
	if len(pendingChange(t, ctx, o).Body) > int(limits.MaxBytes) {
		t.Fatal("fixture wire body already exceeds bound")
	}
	before := receiver.requests.Load()
	if result, err := o.DeliverReportChanges(ctx, native.Path(), remote.StaticToken("fixture"), nil, limits); !errors.Is(err, store.ErrNativeReportLimit) || receiver.requests.Load() != before || result.Receipts != 0 {
		t.Fatal("large retained parent node escaped byte bound", result, err)
	}
	codec, err := privateInventoryCodec()
	if err != nil {
		t.Fatal(err)
	}
	value := "1"
	key, err := codec.EncodeKey("threads", []remote.ChangeValue{{Kind: "integer", Value: &value}})
	if err != nil {
		t.Fatal(err)
	}
	for _, query := range []struct {
		sql   string
		args  []any
		table string
	}{
		{changeParentNodeSQL, []any{4096, id, key}, "change_mutations"},
		{inventoryParentNodeSQL, []any{4096, key}, "inventory_rows"},
	} {
		rows, err := o.base.DB().QueryContext(ctx, "explain query plan "+query.sql, query.args...)
		if err != nil {
			t.Fatal(err)
		}
		var plan strings.Builder
		for rows.Next() {
			var id, parent, unused int
			var detail string
			if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
				t.Fatal(err)
			}
			plan.WriteString(detail)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
		t.Log(plan.String())
		if !strings.Contains(plan.String(), "SEARCH "+query.table+" USING PRIMARY KEY") || strings.Contains(plan.String(), "SCAN ") {
			t.Fatal("parent-node lookup scans inventory")
		}
	}
}

func TestDeliverReportChangesPinsCaptureAcrossConcurrentCompletion(t *testing.T) {
	receiver := &changeSenderServer{t: t}
	server := httptest.NewServer(receiver)
	defer server.Close()
	ctx, native, o, path := changeSenderFixture(t, server.URL)
	id, expected, _ := stageSenderChanges(t, ctx, native, o)
	receiver.remember(ctx, o, id, expected.ReceiverSequence)
	other, err := OpenReportOutbox(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	var successor string
	token := deliveryToken(func(context.Context) (string, error) {
		result, err := other.DeliverReportChanges(ctx, native.Path(), remote.StaticToken("fixture"), nil, deliveryLimits())
		if err != nil || !result.Complete {
			t.Error("concurrent completion failed", result, err)
			return "", ErrSeedOutbox
		}
		var state ReportOutboxState
		successor, state, _ = stageSenderChanges(t, ctx, native, other)
		receiver.remember(ctx, other, successor, state.ReceiverSequence)
		return "fixture", nil
	})
	result, err := o.DeliverReportChanges(ctx, native.Path(), token, nil, deliveryLimits())
	if err != nil || !result.Complete || result.CaptureID != id || result.Receipts != 1 || receiver.requests.Load() != 2 {
		t.Fatal("concurrent historical replay failed", result, err)
	}
	if pendingChange(t, ctx, o).Ref.CaptureID != successor {
		t.Fatal("invocation delivered concurrently staged successor")
	}
}

func TestDeliverReportChangesBoundsReceiptAndSafeErrors(t *testing.T) {
	receiver := &changeSenderServer{t: t}
	server := httptest.NewServer(receiver)
	defer server.Close()
	ctx, native, o, _ := changeSenderFixture(t, server.URL)
	if _, err := native.DB().ExecContext(ctx, `update comments set body='changed';update threads set title='changed' where id=1`); err != nil {
		t.Fatal(err)
	}
	id, expected, _ := stageSenderChanges(t, ctx, native, o)
	receiver.remember(ctx, o, id, expected.ReceiverSequence)
	var tokenCalls int
	token := deliveryToken(func(context.Context) (string, error) { tokenCalls++; return "fixture", nil })
	for _, limits := range []DeliveryLimits{{}, {MaxParts: 1, MaxRows: 1, MaxBytes: 1 << 20}, {MaxParts: 1, MaxRows: 100, MaxBytes: 1}} {
		if result, err := o.DeliverReportChanges(ctx, "/absent-source", token, nil, limits); !errors.Is(err, ErrPrivateChangesLimit) || result.Receipts != 0 {
			t.Fatal("stage bounds did not fail before source", result, err)
		}
	}
	if _, err := o.DeliverReportChanges(context.Background(), native.Path(), token, nil, deliveryLimits()); !errors.Is(err, ErrPrivateChangesLimit) {
		t.Fatal("deadline omission accepted", err)
	}
	if tokenCalls != 0 || receiver.requests.Load() != 0 {
		t.Fatal("preflight read credentials or sent requests")
	}
	if result, err := o.DeliverReportChanges(ctx, native.Path(), nil, nil, deliveryLimits()); !errors.Is(err, remote.ErrMissingToken) || result.Receipts != 0 {
		t.Fatal("missing credentials fell back", result, err)
	}
	secret := "private credential and response detail"
	badToken := deliveryToken(func(context.Context) (string, error) { return "", errors.New(secret) })
	if _, err := o.DeliverReportChanges(ctx, native.Path(), badToken, nil, deliveryLimits()); !errors.Is(err, ErrReportDelivery) || strings.Contains(err.Error(), secret) {
		t.Fatal("credential diagnostic leaked", err)
	}
	receiver.wrong.Store(true)
	if result, err := o.DeliverReportChanges(ctx, native.Path(), token, nil, deliveryLimits()); !errors.Is(err, ErrSeedOutbox) || result.Receipts != 0 || result.Complete {
		t.Fatal("wrong receipt sequence settled", result, err)
	}
	before := receiver.requests.Load()
	if _, err := o.base.DB().ExecContext(ctx, `update change_parts set payload_sha256=? where capture_id=?`, strings.Repeat("0", 64), id); err != nil {
		t.Fatal(err)
	}
	if result, err := o.DeliverReportChanges(ctx, native.Path(), token, nil, deliveryLimits()); !errors.Is(err, ErrSeedOutbox) || result.Receipts != 0 || receiver.requests.Load() != before {
		t.Fatal("corrupt persisted bytes sent", result, err)
	}
}

func TestDeliverReportChangesCancellationAndRedirectPolicy(t *testing.T) {
	for _, mode := range []string{"cancel", "same-origin policy", "cross-origin"} {
		t.Run(mode, func(t *testing.T) {
			var native *store.Store
			var requests atomic.Int64
			entered := make(chan struct{}, 1)
			release := make(chan struct{})
			otherRequests := atomic.Int64{}
			other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { otherRequests.Add(1); w.WriteHeader(500) }))
			defer other.Close()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				if mode == "cancel" {
					// Drain the request so net/http can observe connection closure.
					_, _ = io.Copy(io.Discard, r.Body)
					entered <- struct{}{}
					select {
					case <-r.Context().Done():
					case <-release:
					}
					return
				}
				location := "/redirected"
				if mode == "same-origin policy" {
					if _, err := native.DB().Exec(`insert into thread_exclusions values('fixture/repo',10,'issue',1,'one','now','owner_requested','fixture')`); err != nil {
						t.Error(err)
					}
				} else {
					location = other.URL
				}
				w.Header().Set("Location", location)
				w.WriteHeader(http.StatusTemporaryRedirect)
			}))
			defer server.Close()
			defer close(release)
			ctx, n, o, _ := changeSenderFixture(t, server.URL)
			native = n
			if _, err := native.DB().ExecContext(ctx, `update comments set body='changed'`); err != nil {
				t.Fatal(err)
			}
			stageSenderChanges(t, ctx, native, o)
			if mode == "cancel" {
				child, cancel := context.WithCancel(ctx)
				defer cancel()
				done := make(chan error, 1)
				go func() {
					_, err := o.DeliverReportChanges(child, native.Path(), remote.StaticToken("fixture"), nil, deliveryLimits())
					done <- err
				}()
				select {
				case <-entered:
					cancel()
				case <-ctx.Done():
					t.Fatal("HTTP did not start")
				}
				if err := <-done; !errors.Is(err, context.Canceled) {
					t.Fatal("cancellation lost", err)
				}
			} else {
				_, err := o.DeliverReportChanges(ctx, native.Path(), remote.StaticToken("fixture"), nil, deliveryLimits())
				want := ErrReportDelivery
				if mode == "same-origin policy" {
					want = store.ErrThreadExcluded
				}
				if !errors.Is(err, want) {
					t.Fatal("redirect guard failed", err)
				}
			}
			if requests.Load() != 1 || otherRequests.Load() != 0 {
				t.Fatal("request retried or redirected without policy")
			}
			if p := pendingChange(t, ctx, o); p.Ref.Index != 0 {
				t.Fatal("failed request advanced receipt")
			}
		})
	}
}
