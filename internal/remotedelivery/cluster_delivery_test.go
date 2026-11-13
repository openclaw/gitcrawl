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
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/gitcrawl/internal/store"
)

type clusterTestReceiver struct {
	mu       sync.Mutex
	bodies   map[string][][]byte
	receipts map[string]remote.SeedReceipt
	fail     map[string]bool
	bad      map[string]bool
	sequence int64
	calls    int
}

func newClusterReceiverFixture() *clusterTestReceiver {
	return &clusterTestReceiver{bodies: map[string][][]byte{}, receipts: map[string]remote.SeedReceipt{}, fail: map[string]bool{}, bad: map[string]bool{}}
}
func (h *clusterTestReceiver) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls++
	body, err := io.ReadAll(io.LimitReader(r.Body, remote.MaxChangeBytes+1))
	if err != nil || r.Method != "POST" || r.Header.Get("Authorization") != "Bearer fixture" {
		w.WriteHeader(400)
		return
	}
	for id, bodies := range h.bodies {
		for index, saved := range bodies {
			if bytes.Equal(body, saved) {
				var m remote.GitHubSeedManifest
				json.Unmarshal(bodies[0], &m)
				batch := seedPartBatch(id, index, len(bodies))
				receipt, exists := h.receipts[batch]
				if !exists {
					h.sequence++
					receipt = remote.SeedReceipt{App: "gitcrawl", Archive: m.Archive, BindingID: m.BindingID, BatchID: batch, Cursor: batch, Sequence: strconv.FormatInt(h.sequence, 10), PayloadSHA256: privateSeedHash(body)}
					h.receipts[batch] = receipt
				}
				receipt.Replayed = exists
				if h.fail[batch] {
					delete(h.fail, batch)
					w.WriteHeader(503)
					io.WriteString(w, "private fixture diagnostic")
					return
				}
				if h.bad[batch] {
					n, _ := strconv.ParseInt(receipt.Sequence, 10, 64)
					receipt.Sequence = strconv.FormatInt(n+1, 10)
				}
				json.NewEncoder(w).Encode(receipt)
				return
			}
		}
	}
	w.WriteHeader(400)
}
func (h *clusterTestReceiver) register(id string, bodies [][]byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.bodies[id] = bodies
}

func clusterSenderFixture(t *testing.T, origin, id string, nodes bool) (context.Context, *store.Store, *Outbox, string, [][]byte) {
	t.Helper()
	ctx, _, source := clusterReportSourceFixture(t)
	native, err := store.Open(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { native.Close() })
	if nodes {
		if _, err = native.DB().ExecContext(ctx, `update threads set raw_json=json_object('node_id','NODE_'||id)`); err != nil {
			t.Fatal(err)
		}
	}
	file, err := store.NativeReportFileIdentity(source)
	if err != nil {
		t.Fatal(err)
	}
	capture, err := store.CaptureNativeClusterContext(ctx, source, store.NativeClusterContextRequest{Repository: "fixture/repo", File: file, Limits: store.NativeReportLimits{MaxRows: 1000, MaxBytes: 1 << 20}})
	if err != nil {
		t.Fatal(err)
	}
	report, err := ProjectClusterContext(ctx, capture, time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	request := seedRequest()
	request.SeedID = id
	seed, err := PrepareClusterSeed(ctx, report, request)
	if err != nil {
		t.Fatal(err)
	}
	m := seed.Plan.Manifest()
	registration := SeedRegistration{Origin: origin, Archive: m.Archive, BindingID: m.BindingID, Repository: m.Repository, File: file}
	path := filepath.Join(t.TempDir(), "cluster.sqlite")
	_, err = StageClusterBundle(ctx, path, source, registration, ClusterBundle{Profile: report.Profile, Manifest: seed.Manifest, Chunks: seed.Chunks, Complete: seed.Complete}, clusterLimits())
	if err != nil {
		t.Fatal(err)
	}
	outbox, err := OpenClusterOutbox(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { outbox.Close() })
	bodies := append([][]byte{seed.Manifest}, seed.Chunks...)
	bodies = append(bodies, seed.Complete)
	return ctx, native, outbox, path, bodies
}

func TestClusterDeliveryOffsetsLostRepliesAndReopen(t *testing.T) {
	h := newClusterReceiverFixture()
	server := httptest.NewServer(h)
	defer server.Close()
	id := strings.Repeat("a", 32)
	ctx, native, o, path, bodies := clusterSenderFixture(t, server.URL, id, true)
	h.register(id, bodies)
	for _, index := range []int{0, 1, len(bodies) - 1} {
		h.fail[seedPartBatch(id, index, len(bodies))] = true
	}
	for _, want := range []int{0, 1, len(bodies) - 1} {
		result, err := o.DeliverClusterSeed(ctx, native.Path(), remote.StaticToken("fixture"), nil, deliveryLimits())
		if !errors.Is(err, ErrClusterDelivery) || result.Complete || strings.Contains(err.Error(), "private fixture") {
			t.Fatal("uncertain reply was retried or leaked", result, err)
		}
		state, err := o.ClusterState(ctx)
		if err != nil || state.Receipts != want {
			t.Fatal("uncertain part advanced", state, err)
		}
		o.Close()
		o, err = OpenClusterOutbox(ctx, path)
		if err != nil {
			t.Fatal(err)
		}
	}
	result, err := o.DeliverClusterSeed(ctx, native.Path(), remote.StaticToken("fixture"), nil, deliveryLimits())
	if err != nil || !result.Complete || result.Receipts != 1 {
		t.Fatal("complete replay failed", result, err)
	}
	state, err := ReadClusterOutboxState(ctx, path)
	if err != nil || state.Status() != "complete" || state.BeginSequence != 1 {
		t.Fatal("completed state differs", state, err)
	}
	// Concurrent settlers may accept the same authenticated replay, never a
	// second effect or a divergent offset. Exercise the same transaction owner.
	receipt := h.receipt(seedPartBatch(id, len(bodies)-1, len(bodies)))
	receipt.Replayed = true
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- o.recordClusterReceipt(ctx, state, len(bodies)-1, receipt) }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal("idempotent receipt replay", err)
		}
	}
	o.Close()
	id2 := strings.Repeat("b", 32)
	ctx2, native2, o2, path2, bodies2 := clusterSenderFixture(t, server.URL, id2, true)
	h.register(id2, bodies2)
	limits := deliveryLimits()
	limits.MaxParts = 1
	result, err = o2.DeliverClusterSeed(ctx2, native2.Path(), remote.StaticToken("fixture"), nil, limits)
	if err != nil || result.Complete || result.Receipts != 1 {
		t.Fatal("bounded second begin", result, err)
	}
	second, err := ReadClusterOutboxState(ctx2, path2)
	if err != nil || second.BeginSequence != int64(len(bodies)+1) || second.Status() != "partial" {
		t.Fatal("second snapshot offset lost", second, err)
	}
	result, err = o2.DeliverClusterSeed(ctx2, native2.Path(), remote.StaticToken("fixture"), nil, deliveryLimits())
	if err != nil || !result.Complete {
		t.Fatal("second completion", result, err)
	}
}

func TestClusterDeliveryRejectsPersistedDamageBeforeToken(t *testing.T) {
	for _, kind := range []string{"body", "malformed-receipt", "non-prefix", "bounds"} {
		t.Run(kind, func(t *testing.T) {
			h := newClusterReceiverFixture()
			server := httptest.NewServer(h)
			defer server.Close()
			id := strings.Repeat("c", 32)
			ctx, native, o, _, bodies := clusterSenderFixture(t, server.URL, id, true)
			h.register(id, bodies)
			limits := deliveryLimits()
			limits.MaxParts = 3
			if _, err := o.DeliverClusterSeed(ctx, native.Path(), remote.StaticToken("fixture"), nil, limits); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "body":
				if _, err := o.base.DB().ExecContext(ctx, `update parts set body=? where part_index=1`, bytes.Repeat([]byte{'x'}, len(bodies[1]))); err != nil {
					t.Fatal(err)
				}
			case "malformed-receipt":
				if _, err := o.base.DB().ExecContext(ctx, `update parts set receipt_json='{}' where part_index=1`); err != nil {
					t.Fatal(err)
				}
			case "non-prefix":
				if _, err := o.base.DB().ExecContext(ctx, `update parts set receipt_json=null where part_index=1`); err != nil {
					t.Fatal(err)
				}
			case "bounds":
				limits.MaxRows = 1
			}
			tokens := 0
			token := deliveryToken(func(context.Context) (string, error) { tokens++; return "fixture", nil })
			before := h.callCount()
			result, err := o.DeliverClusterSeed(ctx, native.Path(), token, nil, limits)
			if err == nil || result.Receipts != 0 || result.Complete || tokens != 0 || h.callCount() != before {
				t.Fatal("damaged custody passed network preflight", result, err)
			}
		})
	}
}

func TestClusterDeliveryRechecksAuthorityAfterToken(t *testing.T) {
	for _, kind := range []string{"private", "unknown-public", "number", "global-node", "purged-node", "missing-node"} {
		t.Run(kind, func(t *testing.T) {
			h := newClusterReceiverFixture()
			server := httptest.NewServer(h)
			defer server.Close()
			id := strings.Repeat("d", 32)
			ctx, native, o, _, bodies := clusterSenderFixture(t, server.URL, id, kind != "missing-node")
			h.register(id, bodies)
			limits := deliveryLimits()
			limits.MaxParts = len(bodies) - 1
			if result, err := o.DeliverClusterSeed(ctx, native.Path(), remote.StaticToken("fixture"), nil, limits); err != nil || result.Complete {
				t.Fatal("prefix", result, err)
			}
			token := deliveryToken(func(context.Context) (string, error) {
				query := ""
				switch kind {
				case "private":
					query = `update repositories set raw_json='{"private":true}'`
				case "unknown-public":
					query = `update repositories set raw_json='{}'`
				case "number":
					query = `insert into thread_exclusions values('fixture/repo',10,'issue',1,'one','now','owner_requested','fixture')`
				default:
					if kind == "purged-node" {
						if _, err := native.DB().ExecContext(ctx, `delete from threads where id=1`); err != nil {
							return "", err
						}
					}
					query = `insert into thread_exclusions values('other/repo',90,'issue',90,'other','now','owner_requested','fixture'); insert into thread_excluded_nodes values('NODE_1','other/repo',90)`
				}
				_, err := native.DB().ExecContext(ctx, query)
				return "fixture", err
			})
			before := h.callCount()
			result, err := o.DeliverClusterSeed(ctx, native.Path(), token, nil, deliveryLimits())
			if err == nil || result.Receipts != 0 || result.Complete || h.callCount() != before {
				t.Fatal("post-token authority change escaped", result, err)
			}
			state, readErr := o.ClusterState(ctx)
			if readErr != nil || state.Receipts != len(bodies)-1 {
				t.Fatal("refusal advanced receipt")
			}
		})
	}
}

func TestClusterDeliveryWrongOffsetAndCancellation(t *testing.T) {
	h := newClusterReceiverFixture()
	server := httptest.NewServer(h)
	defer server.Close()
	id := strings.Repeat("e", 32)
	ctx, native, o, _, bodies := clusterSenderFixture(t, server.URL, id, true)
	h.register(id, bodies)
	batch := seedPartBatch(id, 1, len(bodies))
	h.bad[batch] = true
	result, err := o.DeliverClusterSeed(ctx, native.Path(), remote.StaticToken("fixture"), nil, deliveryLimits())
	if !errors.Is(err, ErrClusterOutbox) || result.Receipts != 1 {
		t.Fatal("wrong sequence accepted", result, err)
	}
	h.mu.Lock()
	delete(h.bad, batch)
	h.mu.Unlock()
	callCtx, cancel := context.WithCancel(ctx)
	token := deliveryToken(func(context.Context) (string, error) { cancel(); return "fixture", nil })
	before := h.callCount()
	result, err = o.DeliverClusterSeed(callCtx, native.Path(), token, nil, deliveryLimits())
	if !errors.Is(err, context.Canceled) || result.Receipts != 0 || h.callCount() != before {
		t.Fatal("cancellation not preserved", result, err)
	}
	result, err = o.DeliverClusterSeed(ctx, native.Path(), remote.StaticToken("fixture"), nil, deliveryLimits())
	if err != nil || !result.Complete {
		t.Fatal("corrected receipt replay", result, err)
	}
}

func TestClusterStatusRejectsInvalidReceiptBody(t *testing.T) {
	h := newClusterReceiverFixture()
	server := httptest.NewServer(h)
	defer server.Close()
	id := strings.Repeat("f", 32)
	ctx, native, o, path, bodies := clusterSenderFixture(t, server.URL, id, true)
	h.register(id, bodies)
	if result, err := o.DeliverClusterSeed(ctx, native.Path(), remote.StaticToken("fixture"), nil, deliveryLimits()); err != nil || !result.Complete {
		t.Fatal("completion", result, err)
	}
	var saved []byte
	if err := o.base.DB().QueryRowContext(ctx, `select receipt_json from parts where part_index=1`).Scan(&saved); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"malformed", "hash", "sequence"} {
		t.Run(kind, func(t *testing.T) {
			var receipt remote.SeedReceipt
			if json.Unmarshal(saved, &receipt) != nil {
				t.Fatal("bad fixture receipt")
			}
			var bad []byte
			switch kind {
			case "malformed":
				bad = []byte(`{}`)
			case "hash":
				receipt.PayloadSHA256 = strings.Repeat("0", 64)
				bad, _ = json.Marshal(receipt)
			case "sequence":
				receipt.Sequence = "999"
				bad, _ = json.Marshal(receipt)
			}
			if _, err := o.base.DB().ExecContext(ctx, `update parts set receipt_json=? where part_index=1`, bad); err != nil {
				t.Fatal(err)
			}
			if _, err := ReadClusterOutboxState(ctx, path); err == nil {
				t.Fatal("invalid receipt claimed complete")
			}
		})
	}
}

func (h *clusterTestReceiver) callCount() int { h.mu.Lock(); defer h.mu.Unlock(); return h.calls }
func (h *clusterTestReceiver) receipt(batch string) remote.SeedReceipt {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.receipts[batch]
}

func TestClusterDeliveryRedirectRechecksPublicAuthority(t *testing.T) {
	var native *store.Store
	var sourceCtx context.Context
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if _, err := native.DB().ExecContext(sourceCtx, `update repositories set raw_json='{"private":true}'`); err != nil {
			t.Error(err)
			w.WriteHeader(500)
			return
		}
		w.Header().Set("Location", r.URL.String()+"?again=1")
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	ctx, n, o, _, _ := clusterSenderFixture(t, server.URL, strings.Repeat("9", 32), true)
	native, sourceCtx = n, ctx
	result, err := o.DeliverClusterSeed(ctx, native.Path(), remote.StaticToken("fixture"), nil, deliveryLimits())
	if !errors.Is(err, store.ErrNativeClusterContext) || requests.Load() != 1 || result.Receipts != 0 || result.Complete {
		t.Fatal("redirect escaped refreshed authority", result, err)
	}
}
