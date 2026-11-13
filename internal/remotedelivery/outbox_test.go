//go:build darwin || linux

package remotedelivery

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"testing"

	"github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/gitcrawl/internal/store"
)

func seedRegistration(c store.NativeReportCapture) SeedRegistration {
	return SeedRegistration{Origin: "http://127.0.0.1:8123", Archive: "fixture/archive", BindingID: "fixture-binding", File: c.Anchor.File, Repository: c.Anchor.Repository}
}

func newSeedOutbox(t *testing.T, ctx context.Context, c store.NativeReportCapture) (*Outbox, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "outbox.sqlite")
	o, err := CreateSeedOutbox(ctx, path, seedRegistration(c), c, StageLimits{MaxEncodedBytes: 32 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { o.Close() })
	return o, path
}

func outboxState(t *testing.T, ctx context.Context, o *Outbox) SeedState {
	t.Helper()
	s, err := o.State(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func nextSeedPart(t *testing.T, ctx context.Context, o *Outbox) *PendingPart {
	t.Helper()
	p, err := o.NextPart(ctx)
	if err != nil || p == nil {
		t.Fatal("missing pending part", err)
	}
	return p
}

func seedReceipt(p *PendingPart) remote.SeedReceipt {
	suffix := strconv.Itoa(p.Ref.Index - 1)
	if p.Ref.Index == 0 {
		suffix = "begin"
	} else if p.Ref.Index == p.PartCount-1 {
		suffix = "complete"
	}
	batch := "seed1:" + p.Ref.SeedID + ":" + suffix
	return remote.SeedReceipt{App: "gitcrawl", Archive: "fixture/archive", BindingID: "fixture-binding", BatchID: batch, Cursor: batch, Sequence: strconv.Itoa(p.Ref.Index + 1), PayloadSHA256: p.SHA256}
}

func TestSeedOutboxNativeReopenExactBytesAndCompletion(t *testing.T) {
	ctx, native := nativeFixture(t)
	err := native.WithTx(ctx, func(tx *store.Store) error {
		for i := range 501 {
			if _, err := tx.UpsertComment(ctx, store.Comment{ThreadID: 1, GitHubID: strconv.Itoa(i + 1), CommentType: "issue_comment", Body: "private\x00☃", RawJSON: "{}"}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	c, report := projected(t, ctx, native)
	if err := native.Close(); err != nil {
		t.Fatal(err)
	}
	sourceBefore, err := os.ReadFile(native.Path())
	if err != nil {
		t.Fatal(err)
	}
	o, path := newSeedOutbox(t, ctx, c)
	initial := outboxState(t, ctx, o)
	if initial.Anchor != c.Anchor || initial.Registration != seedRegistration(c) || initial.NativeSHA256 != c.SHA256 || initial.NativeRows != c.Rows || initial.NativeBytes != c.Bytes || initial.BaselineState != BaselineUnqualified || initial.Receipts != 0 {
		t.Fatal("initial custody or baseline changed")
	}
	request := seedRequest()
	request.SeedID = initial.SeedID
	expected, err := PreparePrivateSeed(ctx, report, request)
	if err != nil {
		t.Fatal(err)
	}
	if len(expected.Plan.Manifest().Tables[2].Chunks) != 2 {
		t.Fatal("fixture did not cross chunk boundary")
	}
	bodies := append([][]byte{expected.Manifest}, expected.Chunks...)
	bodies = append(bodies, expected.Complete)
	if initial.PartCount != len(bodies) {
		t.Fatal("incomplete staged capture")
	}
	var first *PendingPart
	for i, body := range bodies {
		if err := o.Close(); err != nil {
			t.Fatal(err)
		}
		o, err = OpenSeedOutbox(ctx, path)
		if err != nil {
			t.Fatal(err)
		}
		p := nextSeedPart(t, ctx, o)
		if i == 0 {
			first = p
		}
		if p.Ref.Index != i || !bytes.Equal(p.Body, body) || p.SHA256 != privateSeedHash(body) {
			t.Fatal("reopen changed exact request bytes or order")
		}
		if err := o.RecordReceipt(ctx, p.Ref, seedReceipt(p)); err != nil {
			t.Fatal(err)
		}
		state := outboxState(t, ctx, o)
		if i < len(bodies)-1 && (state.BaselineState != BaselineUnqualified || state.BaselineNative != "" || state.ServerCursor != "") {
			t.Fatal("partial seed qualified")
		}
	}
	complete := outboxState(t, ctx, o)
	if complete.BaselineState != BaselineQualified || complete.BaselineNative != expected.Plan.Manifest().NativeThrough || complete.BaselineSHA256 != c.SHA256 || complete.Receipts != complete.PartCount {
		t.Fatal("completion lost baseline")
	}
	var receiptBefore, receiptAfter []byte
	if err := o.base.DB().QueryRowContext(ctx, `select receipt_json from parts where part_index=0`).Scan(&receiptBefore); err != nil {
		t.Fatal(err)
	}
	replay := seedReceipt(first)
	replay.Replayed = true
	if err := o.RecordReceipt(ctx, first.Ref, replay); err != nil {
		t.Fatal(err)
	}
	if err := o.base.DB().QueryRowContext(ctx, `select receipt_json from parts where part_index=0`).Scan(&receiptAfter); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(receiptBefore, receiptAfter) {
		t.Fatal("replay rewrote original receipt")
	}
	if p, err := o.NextPart(ctx); err != nil || p != nil {
		t.Fatal("completed seed still pending", err)
	}
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	o, err = OpenSeedOutbox(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	if got := outboxState(t, ctx, o); !reflect.DeepEqual(got, complete) {
		t.Fatal("completed state changed on reopen")
	}
	sourceAfter, err := os.ReadFile(native.Path())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(sourceBefore, sourceAfter) {
		t.Fatal("outbox changed native source")
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if _, err := os.Stat(native.Path() + suffix); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("outbox opened native sidecar", err)
		}
	}
}

func TestSeedOutboxH0ReceiptIdentityOrderAndConcurrency(t *testing.T) {
	ctx, native := nativeFixture(t)
	c, _ := projected(t, ctx, native)
	if c.Anchor.Cursor.Sequence != 0 {
		t.Fatal("fixture is not H0")
	}
	o, path := newSeedOutbox(t, ctx, c)
	p := nextSeedPart(t, ctx, o)
	plan, err := remote.DecodeGitHubSeedManifest(p.Body)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Manifest().Tables[2].Rows != "0" {
		t.Fatal("empty native table changed")
	}
	initial := outboxState(t, ctx, o)
	for _, mutate := range []func(*remote.SeedReceipt){
		func(r *remote.SeedReceipt) { r.App = "slacrawl" }, func(r *remote.SeedReceipt) { r.Archive = "other" },
		func(r *remote.SeedReceipt) { r.BindingID = "other" }, func(r *remote.SeedReceipt) { r.BatchID = "other" },
		func(r *remote.SeedReceipt) { r.Cursor = "other" }, func(r *remote.SeedReceipt) { r.PayloadSHA256 = privateSeedHash(nil) },
		func(r *remote.SeedReceipt) { r.Sequence = "0" }, func(r *remote.SeedReceipt) { r.Sequence = "01" }, func(r *remote.SeedReceipt) { r.Sequence = "2" },
	} {
		r := seedReceipt(p)
		mutate(&r)
		if err := o.RecordReceipt(ctx, p.Ref, r); !errors.Is(err, ErrSeedOutbox) {
			t.Fatal("mismatched receipt admitted", err)
		}
	}
	var later PendingPart
	later.Ref = PartRef{SeedID: p.Ref.SeedID, Index: 1}
	later.PartCount = p.PartCount
	if err := o.base.DB().QueryRowContext(ctx, `select payload_sha256 from parts where part_index=1`).Scan(&later.SHA256); err != nil {
		t.Fatal(err)
	}
	if err := o.RecordReceipt(ctx, later.Ref, seedReceipt(&later)); !errors.Is(err, ErrSeedOutbox) {
		t.Fatal("out of order receipt admitted", err)
	}
	if got := outboxState(t, ctx, o); !reflect.DeepEqual(got, initial) {
		t.Fatal("refusal progressed state")
	}
	other, err := OpenSeedOutbox(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if got := nextSeedPart(t, ctx, other); !reflect.DeepEqual(got, p) {
		t.Fatal("workers see different bytes")
	}
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for _, handle := range []*Outbox{o, other} {
		wg.Add(1)
		go func(o *Outbox) { defer wg.Done(); results <- o.RecordReceipt(ctx, p.Ref, seedReceipt(p)) }(handle)
	}
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := outboxState(t, ctx, o); got.Receipts != 1 || got.BaselineState != BaselineUnqualified {
		t.Fatal("concurrent replay advanced twice")
	}
	r := seedReceipt(p)
	r.Sequence = "2"
	if err := other.RecordReceipt(ctx, p.Ref, r); !errors.Is(err, ErrSeedOutbox) {
		t.Fatal("mismatched replay admitted", err)
	}
}

func TestSeedOutboxFinalReceiptRollbackAndCanceledWrite(t *testing.T) {
	ctx, native := nativeFixture(t)
	c, _ := projected(t, ctx, native)
	o, _ := newSeedOutbox(t, ctx, c)
	initial := outboxState(t, ctx, o)
	writeCtx, cancel := context.WithCancel(ctx)
	err := o.write(writeCtx, func(conn *sql.Conn) error {
		var synchronous int
		if err := conn.QueryRowContext(writeCtx, `pragma synchronous`).Scan(&synchronous); err != nil {
			return err
		}
		if synchronous != 2 {
			t.Fatal("write connection is not FULL")
		}
		if _, err := conn.ExecContext(writeCtx, `update binding set origin='https://changed.example.test'`); err != nil {
			return err
		}
		cancel()
		_, err := conn.ExecContext(writeCtx, `update binding set origin='https://canceled.example.test'`)
		return err
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation not propagated", err)
	}
	if got := outboxState(t, ctx, o); !reflect.DeepEqual(got, initial) {
		t.Fatal("canceled transaction committed")
	}
	var final *PendingPart
	for {
		p := nextSeedPart(t, ctx, o)
		if p.Ref.Index == p.PartCount-1 {
			final = p
			break
		}
		if err := o.RecordReceipt(ctx, p.Ref, seedReceipt(p)); err != nil {
			t.Fatal(err)
		}
	}
	before := outboxState(t, ctx, o)
	if _, err := o.base.DB().ExecContext(ctx, `create trigger reject_final before update of baseline_state on binding begin select raise(abort,'fixture final failure'); end`); err != nil {
		t.Fatal(err)
	}
	if err := o.RecordReceipt(ctx, final.Ref, seedReceipt(final)); err == nil {
		t.Fatal("final transaction should fail")
	}
	if got := outboxState(t, ctx, o); !reflect.DeepEqual(got, before) {
		t.Fatal("failed final receipt qualified or partially committed")
	}
	if got := nextSeedPart(t, ctx, o); !reflect.DeepEqual(got, final) {
		t.Fatal("failed final receipt lost retry bytes")
	}
	if _, err := o.base.DB().ExecContext(ctx, `drop trigger reject_final`); err != nil {
		t.Fatal(err)
	}
	if err := o.RecordReceipt(ctx, final.Ref, seedReceipt(final)); err != nil {
		t.Fatal(err)
	}
	if got := outboxState(t, ctx, o); got.BaselineState != BaselineQualified {
		t.Fatal("recovered final receipt failed to qualify")
	}
}

func TestSeedOutboxSameHeadInventorySurvivesReopen(t *testing.T) {
	ctx, native := nativeFixture(t)
	if _, err := native.ReserveThreadChildObservation(ctx, 1, store.ThreadChildComments, "2026-01-03", 1); err != nil {
		t.Fatal(err)
	}
	before, _ := projected(t, ctx, native)
	if err := native.ReplaceThreadChildObservationMembers(ctx, 1, store.ThreadChildComments, 1, []int64{}); err != nil {
		t.Fatal(err)
	}
	after, _ := projected(t, ctx, native)
	if before.Anchor != after.Anchor || before.SHA256 == after.SHA256 {
		t.Fatal("fixture did not change inventory at same native head")
	}
	var tokens []string
	for _, c := range []store.NativeReportCapture{before, after} {
		o, path := newSeedOutbox(t, ctx, c)
		o.Close()
		o, err := OpenSeedOutbox(ctx, path)
		if err != nil {
			t.Fatal(err)
		}
		s := outboxState(t, ctx, o)
		o.Close()
		if s.NativeSHA256 != c.SHA256 || s.Anchor != c.Anchor {
			t.Fatal("inventory provenance lost")
		}
		tokens = append(tokens, s.NativeThrough)
	}
	if tokens[0] == tokens[1] {
		t.Fatal("same-H inventory updates collapsed")
	}
}

func TestSeedOutboxCorruptPendingBytesRefused(t *testing.T) {
	ctx, native := nativeFixture(t)
	c, _ := projected(t, ctx, native)
	o, _ := newSeedOutbox(t, ctx, c)
	p := nextSeedPart(t, ctx, o)
	if err := o.RecordReceipt(ctx, p.Ref, seedReceipt(p)); err != nil {
		t.Fatal(err)
	}
	if _, err := o.base.DB().ExecContext(ctx, `update parts set body=? where part_index=1`, []byte(`{"changed":true}`)); err != nil {
		t.Fatal(err)
	}
	if p, err := o.NextPart(ctx); !errors.Is(err, ErrSeedOutbox) || p != nil {
		t.Fatal("corrupt bytes returned", err)
	}
	if s := outboxState(t, ctx, o); s.Receipts != 1 || s.BaselineState != BaselineUnqualified {
		t.Fatal("corruption progressed baseline")
	}
}
