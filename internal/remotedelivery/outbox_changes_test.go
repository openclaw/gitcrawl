//go:build darwin || linux

package remotedelivery

import (
	"bytes"
	"context"
	"database/sql"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/gitcrawl/internal/store"
)

var changeTestLimits = ChangeStageLimits{MaxRows: 2000, MaxBytes: 16 << 20, MaxParts: 10}

func changesFixture(t *testing.T) (context.Context, *store.Store, *Outbox, string, store.NativeReportCapture) {
	t.Helper()
	ctx, native := nativeFixture(t)
	if _, err := native.UpsertComment(ctx, store.Comment{ThreadID: 1, GitHubID: "comment", CommentType: "issue_comment", Body: "unchanged\x00body", RawJSON: `{"node_id":"comment-node"}`}); err != nil {
		t.Fatal(err)
	}
	if err := native.PrepareNativeReportIndexes(ctx); err != nil {
		t.Fatal(err)
	}
	capture, _ := projected(t, ctx, native)
	o, path := newSeedOutbox(t, ctx, capture)
	completeInventorySeed(t, ctx, o)
	if err := MigrateSeedOutboxV2(ctx, path, inventoryTestLimits); err != nil {
		t.Fatal(err)
	}
	before := inventoryRowsEvidence(t, ctx, o)
	seed := inventorySeedEvidence(t, ctx, o)
	state := reportState(t, ctx, path)
	if err := MigrateReportOutboxV3(ctx, path); err != nil {
		t.Fatal(err)
	}
	if before != inventoryRowsEvidence(t, ctx, o) || !reflect.DeepEqual(seed, inventorySeedEvidence(t, ctx, o)) || state != reportState(t, ctx, path) {
		t.Fatal("schema-only upgrade changed qualified custody")
	}
	return ctx, native, o, path, capture
}

func reportState(t *testing.T, ctx context.Context, path string) ReportOutboxState {
	t.Helper()
	s, err := ReadReportOutboxState(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func inventoryRowsEvidence(t *testing.T, ctx context.Context, o *Outbox) string {
	t.Helper()
	var value string
	err := o.base.DB().QueryRowContext(ctx, `select json_group_array(json_array(table_name,hex(encoded_key),hex(row_sha256),hex(key_json),repository_id,thread_id,thread_number,node_id)) from (select * from inventory_rows order by table_name,encoded_key)`).Scan(&value)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func captureChanges(t *testing.T, ctx context.Context, native *store.Store, o *Outbox) (ReportOutboxState, store.NativeReportDelta) {
	t.Helper()
	var state ReportOutboxState
	var delta store.NativeReportDelta
	err := o.WithQualifiedReportIndex(ctx, func(s ReportOutboxState, index store.NativeReportOldIndex) error {
		state = s
		var err error
		delta, err = store.CaptureNativeReportChanges(ctx, native.Path(), store.NativeReportDeltaRequest{Baseline: s.BaselineAnchor, Limits: store.NativeReportDeltaLimits{MaxJournalRecords: 2000, MaxAffectedKeys: 2000, MaxLookupRows: 10000, MaxRows: 2000, MaxNativeBytes: 16 << 20}}, index)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return state, delta
}

func pendingChange(t *testing.T, ctx context.Context, o *Outbox) *PendingChangePart {
	t.Helper()
	p, err := o.NextChangePart(ctx)
	if err != nil || p == nil {
		t.Fatal("missing change part", err)
	}
	return p
}

func changeReceipt(t *testing.T, p *PendingChangePart, base int64) remote.ChangeReceipt {
	t.Helper()
	b, err := remote.DecodeChangeBatch(p.Body)
	if err != nil {
		t.Fatal(err)
	}
	return remote.ChangeReceipt{App: b.App, Archive: b.Archive, BindingID: b.BindingID, BatchID: b.BatchID(), PayloadSHA256: p.SHA256, Sequence: strconv.FormatInt(base+int64(p.Ref.Index)+1, 10), Cursor: b.BatchID()}
}

func settleChanges(t *testing.T, ctx context.Context, o *Outbox, base int64) (*PendingChangePart, remote.ChangeReceipt) {
	t.Helper()
	var first *PendingChangePart
	var firstReceipt remote.ChangeReceipt
	for {
		p, err := o.NextChangePart(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if p == nil {
			return first, firstReceipt
		}
		receipt := changeReceipt(t, p, base)
		if first == nil {
			first, firstReceipt = p, receipt
		}
		if err := o.RecordChangeReceipt(ctx, p.Ref, receipt); err != nil {
			t.Fatal(err)
		}
	}
}

func TestOutboxChangesParentMetadataThenExclusionAndHistoricalReplay(t *testing.T) {
	ctx, native, o, path, _ := changesFixture(t)
	seed := inventorySeedEvidence(t, ctx, o)
	if _, err := native.DB().ExecContext(ctx, `update threads set number=15 where id=1`); err != nil {
		t.Fatal(err)
	}
	expected, delta := captureChanges(t, ctx, native, o)
	fresh, _ := projected(t, ctx, native)
	want := independentInventoryDigest(t, ctx, fresh)
	before := inventoryRowsEvidence(t, ctx, o)
	id, err := o.StageReportChanges(ctx, expected, delta, changeTestLimits)
	if err != nil {
		t.Fatal(err)
	}
	staged := reportState(t, ctx, path)
	if staged.ActiveChangeID != id {
		t.Fatal("active capture missing")
	}
	staged.ActiveChangeID = ""
	if staged != expected || before != inventoryRowsEvidence(t, ctx, o) {
		t.Fatal("staging advanced baseline")
	}
	var same bool
	var oldNumber, newNumber int
	if err := o.base.DB().QueryRowContext(ctx, `select m.row_sha256=i.row_sha256,i.thread_number,m.thread_number from change_mutations m join inventory_rows i using(table_name,encoded_key) where m.capture_id=? and m.table_name='comments'`, id).Scan(&same, &oldNumber, &newNumber); err != nil || !same || oldNumber != 10 || newNumber != 15 {
		t.Fatal("unchanged child hash lost metadata mutation", same, oldNumber, newNumber, err)
	}
	if _, err := o.StageReportChanges(ctx, expected, delta, changeTestLimits); err == nil {
		t.Fatal("second active capture accepted")
	}
	first, receipt := settleChanges(t, ctx, o, expected.ReceiverSequence)
	after := reportState(t, ctx, path)
	if after.ActiveChangeID != "" || after.InventorySHA256 != want || after.BaselineSHA256 != want || after.BaselineAnchor != fresh.Anchor {
		t.Fatal("first capture differs from fresh inventory")
	}
	if _, err := native.DB().ExecContext(ctx, `insert into thread_exclusions(repository,number,kind,original_thread_id,github_id,excluded_at,reason,plan_id) values('fixture/repo',15,'issue',1,'one','2026-01-01','owner_requested','fixture')`); err != nil {
		t.Fatal(err)
	}
	expected2, delta2 := captureChanges(t, ctx, native, o)
	if len(delta2.Removals) < 2 {
		t.Fatal("exclusion did not close parent/child scope")
	}
	for _, ref := range delta2.Removals {
		if ref.ThreadID == 1 && ref.ThreadNumber != 15 {
			t.Fatal("second capture used stale parent metadata")
		}
	}
	wrong := delta2
	wrong.Removals = slices.Clone(delta2.Removals)
	wrong.Removals[0].ThreadNumber++
	if _, err := o.StageReportChanges(ctx, expected2, wrong, changeTestLimits); err == nil {
		t.Fatal("wrong retained removal provenance accepted")
	}
	fresh2, _ := projected(t, ctx, native)
	if _, err := o.StageReportChanges(ctx, expected2, delta2, changeTestLimits); err != nil {
		t.Fatal(err)
	}
	settleChanges(t, ctx, o, expected2.ReceiverSequence)
	final := reportState(t, ctx, path)
	finalRows := inventoryRowsEvidence(t, ctx, o)
	if final.InventorySHA256 != independentInventoryDigest(t, ctx, fresh2) || final.BaselineAnchor != fresh2.Anchor {
		t.Fatal("second capture differs from full inventory")
	}
	receipt.Replayed = true
	if err := o.RecordChangeReceipt(ctx, first.Ref, receipt); err != nil {
		t.Fatal("historical replay refused", err)
	}
	if final != reportState(t, ctx, path) || finalRows != inventoryRowsEvidence(t, ctx, o) {
		t.Fatal("historical replay republished old baseline")
	}
	if !reflect.DeepEqual(seed, inventorySeedEvidence(t, ctx, o)) {
		t.Fatal("incremental settlement changed seed provenance")
	}
	if _, err := o.StageReportChanges(ctx, expected, delta, changeTestLimits); err == nil {
		t.Fatal("stale predecessor accepted")
	}
	reopened, err := OpenReportOutbox(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	reopened.Close()
}

func TestOutboxChangesEmptyVersionTransition(t *testing.T) {
	ctx, native, o, path, _ := changesFixture(t)
	for iteration := range 2 {
		expected, delta := captureChanges(t, ctx, native, o)
		if delta.Rows != 0 || delta.From != delta.Through {
			t.Fatal("fixture interval not empty")
		}
		before := inventoryRowsEvidence(t, ctx, o)
		if _, err := o.StageReportChanges(ctx, expected, delta, changeTestLimits); err != nil {
			t.Fatal(err)
		}
		p := pendingChange(t, ctx, o)
		b, err := remote.DecodeChangeBatch(p.Body)
		if err != nil {
			t.Fatal(err)
		}
		from, _ := remote.ParseGitHubNativeAnchor(b.NativeFrom)
		through, _ := remote.ParseGitHubNativeAnchor(b.NativeThrough)
		if len(b.Changes) != 0 || b.PartCount != 1 || through.Version != remote.GitHubNativeV2 || through.Sequence != from.Sequence || through.InventorySHA256 != expected.InventorySHA256 {
			t.Fatal("invalid empty transition")
		}
		if iteration == 0 && from.Version != remote.GitHubNativeV1 {
			t.Fatal("initial token reinterpreted")
		}
		if iteration == 0 {
			for _, field := range []string{"App", "Archive", "BindingID", "BatchID", "PayloadSHA256", "Sequence", "Cursor"} {
				r := changeReceipt(t, p, expected.ReceiverSequence)
				reflect.ValueOf(&r).Elem().FieldByName(field).SetString("wrong")
				if err := o.RecordChangeReceipt(ctx, p.Ref, r); err == nil {
					t.Fatal("bad receipt accepted", field)
				}
			}
		}
		if err := o.RecordChangeReceipt(ctx, p.Ref, changeReceipt(t, p, expected.ReceiverSequence)); err != nil {
			t.Fatal(err)
		}
		if before != inventoryRowsEvidence(t, ctx, o) {
			t.Fatal("empty interval mutated inventory")
		}
		if got := reportState(t, ctx, path); got.ReceiverSequence != expected.ReceiverSequence+1 || got.InventorySHA256 != expected.InventorySHA256 {
			t.Fatal("empty receipt checkpoint differs")
		}
	}
}

func manyCommentDelta(base ReportOutboxState, capture store.NativeReportCapture) store.NativeReportDelta {
	delta := store.NativeReportDelta{From: base.BaselineAnchor, Through: base.BaselineAnchor, Rows: 501, Bytes: 100000, JournalRecords: 501}
	delta.Through.Cursor.Sequence += 501
	for i := range 501 {
		values := slices.Clone(capture.Tables[2].Rows[0])
		values[0].Integer = int64(i + 100)
		delta.Upserts = append(delta.Upserts, store.NativeReportDeltaRow{Key: store.NativeReportKey{Table: "comments", Values: []store.NativeReportValue{values[0]}}, Values: values})
	}
	return delta
}

func TestOutboxChangesExactPartsOrderedReceiptsAndFinalRollback(t *testing.T) {
	ctx, _, o, path, capture := changesFixture(t)
	expected := reportState(t, ctx, path)
	id, err := o.StageReportChanges(ctx, expected, manyCommentDelta(expected, capture), changeTestLimits)
	if err != nil {
		t.Fatal(err)
	}
	first := pendingChange(t, ctx, o)
	if first.PartCount != 2 {
		t.Fatal("fixture did not cross part boundary", first.PartCount)
	}
	reopened, err := OpenReportOutbox(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	again := pendingChange(t, ctx, reopened)
	if !bytes.Equal(first.Body, again.Body) || first.SHA256 != again.SHA256 {
		t.Fatal("reopen changed exact request")
	}
	var lastBody []byte
	var lastHash string
	if err := o.base.DB().QueryRowContext(ctx, `select body,payload_sha256 from change_parts where capture_id=? and part_index=1`, id).Scan(&lastBody, &lastHash); err != nil {
		t.Fatal(err)
	}
	last := &PendingChangePart{Ref: ChangePartRef{CaptureID: id, Index: 1}, PartCount: 2, Body: lastBody, SHA256: lastHash}
	if err := o.RecordChangeReceipt(ctx, last.Ref, changeReceipt(t, last, expected.ReceiverSequence)); err == nil {
		t.Fatal("out-of-order receipt accepted")
	}

	rows := inventoryRowsEvidence(t, ctx, o)
	if err := o.RecordChangeReceipt(ctx, first.Ref, changeReceipt(t, first, expected.ReceiverSequence)); err != nil {
		t.Fatal(err)
	}
	partial := reportState(t, ctx, path)
	partial.ActiveChangeID = ""
	if partial != expected || rows != inventoryRowsEvidence(t, ctx, o) {
		t.Fatal("partial receipt promoted baseline")
	}
	if _, err := o.base.DB().ExecContext(ctx, `create trigger reject_change_completion before update of baseline_native on binding begin select raise(abort,'fixture failure');end`); err != nil {
		t.Fatal(err)
	}
	if err := o.RecordChangeReceipt(ctx, last.Ref, changeReceipt(t, last, expected.ReceiverSequence)); err == nil {
		t.Fatal("fixture did not abort final settlement")
	}
	if rows != inventoryRowsEvidence(t, ctx, o) || pendingChange(t, ctx, o).Ref != last.Ref {
		t.Fatal("failed final transaction changed inventory/receipt")
	}
	if _, err := o.base.DB().ExecContext(ctx, `drop trigger reject_change_completion`); err != nil {
		t.Fatal(err)
	}
	if err := o.RecordChangeReceipt(ctx, last.Ref, changeReceipt(t, last, expected.ReceiverSequence)); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := o.base.DB().QueryRowContext(ctx, `select count(*) from inventory_rows where table_name='comments'`).Scan(&count); err != nil || count != 502 {
		t.Fatal("final mutation count", count, err)
	}
	final := reportState(t, ctx, path)
	if final.ActiveChangeID != "" || final.ReceiverSequence != expected.ReceiverSequence+2 {
		t.Fatal("final checkpoint differs")
	}
	r := changeReceipt(t, first, expected.ReceiverSequence)
	r.Replayed = true
	if err := o.RecordChangeReceipt(ctx, first.Ref, r); err != nil || final != reportState(t, ctx, path) {
		t.Fatal("old part replay changed checkpoint", err)
	}
}

func TestOutboxChangesBoundsAndStageRollback(t *testing.T) {
	ctx, _, o, path, capture := changesFixture(t)
	expected := reportState(t, ctx, path)
	delta := manyCommentDelta(expected, capture)
	before := inventoryRowsEvidence(t, ctx, o)
	for _, limits := range []ChangeStageLimits{{}, {MaxRows: 1, MaxBytes: 16 << 20, MaxParts: 10}, {MaxRows: 2000, MaxBytes: 100, MaxParts: 10}, {MaxRows: 2000, MaxBytes: 16 << 20, MaxParts: 1}} {
		if _, err := o.StageReportChanges(ctx, expected, delta, limits); err == nil {
			t.Fatal("bound exceeded", limits)
		}
		var count int
		if err := o.base.DB().QueryRowContext(ctx, `select count(*) from change_captures`).Scan(&count); err != nil || count != 0 {
			t.Fatal("failed stage retained capture", count, err)
		}
		if before != inventoryRowsEvidence(t, ctx, o) || expected != reportState(t, ctx, path) {
			t.Fatal("failed stage changed baseline")
		}
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	for _, unbounded := range []context.Context{context.Background(), canceled} {
		if _, err := o.StageReportChanges(unbounded, expected, delta, changeTestLimits); err == nil {
			t.Fatal("deadline gate failed")
		}
	}
	duplicate := delta
	duplicate.Upserts = slices.Clone(delta.Upserts)
	duplicate.Upserts[1] = duplicate.Upserts[0]
	if _, err := o.StageReportChanges(ctx, expected, duplicate, changeTestLimits); err == nil {
		t.Fatal("duplicate candidate key accepted")
	}
	if err := MigrateReportOutboxV3(ctx, path); err == nil {
		t.Fatal("schema3 migration repeated")
	}
	if err := MigrateReportOutboxV3(context.Background(), path); err == nil {
		t.Fatal("unbounded migration accepted")
	}
	if _, err := OpenSeedOutbox(ctx, path); err == nil {
		t.Fatal("seed API accepted schema3")
	}
}

func TestOutboxChangesRejectsMissingMetadataClosure(t *testing.T) {
	ctx, native, o, path, _ := changesFixture(t)
	if _, err := native.DB().ExecContext(ctx, `update threads set number=15 where id=1`); err != nil {
		t.Fatal(err)
	}
	expected, delta := captureChanges(t, ctx, native, o)
	delta.Upserts = slices.DeleteFunc(delta.Upserts, func(row store.NativeReportDeltaRow) bool { return row.Key.Table == "comments" })
	delta.Rows = int64(len(delta.Upserts) + len(delta.Removals))
	if _, err := o.StageReportChanges(ctx, expected, delta, changeTestLimits); err == nil {
		t.Fatal("renumbered parent accepted stale child metadata")
	}
	if expected != reportState(t, ctx, path) {
		t.Fatal("closure refusal changed baseline")
	}
}

// Deadline cancellation while the write lock is held must not install schema3.
func TestOutboxChangesMigrationWriterDeadline(t *testing.T) {
	ctx, native := nativeFixture(t)
	capture, _ := projected(t, ctx, native)
	o, path := newSeedOutbox(t, ctx, capture)
	completeInventorySeed(t, ctx, o)
	if err := MigrateSeedOutboxV2(ctx, path, inventoryTestLimits); err != nil {
		t.Fatal(err)
	}
	conn, err := o.base.DB().Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `begin immediate`); err != nil {
		t.Fatal(err)
	}
	defer conn.ExecContext(context.Background(), `rollback`)
	short, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if err := MigrateReportOutboxV3(short, path); err == nil {
		t.Fatal("locked migration ignored deadline")
	}
	if _, err := conn.ExecContext(ctx, `rollback`); err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := o.base.WithTx(ctx, func(tx *sql.Tx) error { return inventoryVersion(ctx, tx, 2, 4) }); err != nil {
		t.Fatal("deadline changed schema", err)
	}
}

func TestOutboxChangesCandidateParentLookupPlan(t *testing.T) {
	ctx, _, o, _, capture := changesFixture(t)
	codec, err := privateInventoryCodec()
	if err != nil {
		t.Fatal(err)
	}
	// Ten thousand valid compact keys/row hashes exercise the actual parent
	// query plan without constructing another full native body archive.
	err = o.write(ctx, func(conn *sql.Conn) error {
		statement, err := conn.PrepareContext(ctx, `insert into inventory_rows(table_name,encoded_key,row_sha256,key_json,repository_id,thread_id,thread_number,node_id) values('comments',?,?,?,1,1,10,'comment-node')`)
		if err != nil {
			return err
		}
		defer statement.Close()
		for i := int64(2); i <= 10001; i++ {
			values := slices.Clone(capture.Tables[2].Rows[0])
			values[0].Integer = i
			delta := store.NativeReportDelta{Upserts: []store.NativeReportDeltaRow{{Key: store.NativeReportKey{Table: "comments", Values: []store.NativeReportValue{values[0]}}, Values: values}}}
			changes, err := projectOutboxChanges(ctx, delta, 1<<20)
			if err != nil {
				return err
			}
			row, err := deriveInventoryRow(ctx, conn, codec, capture.Anchor, changes[0])
			if err != nil {
				return err
			}
			if _, err := statement.ExecContext(ctx, row.Key, row.SHA256, row.KeyJSON); err != nil {
				return err
			}
		}
		for _, query := range []struct {
			sql  string
			args []any
		}{
			{candidateParentCheckSQL, []any{"none", "none", 1, "none", "none"}},
			{candidateChildNumbersSQL, []any{"none"}},
		} {
			rows, err := conn.QueryContext(ctx, "explain query plan "+query.sql, query.args...)
			if err != nil {
				return err
			}
			var plan []string
			for rows.Next() {
				var id, parent, unused int
				var detail string
				if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
					rows.Close()
					return err
				}
				plan = append(plan, detail)
			}
			if err := rows.Err(); err != nil {
				rows.Close()
				return err
			}
			rows.Close()
			joined := strings.Join(plan, "\n")
			if !strings.Contains(joined, "SEARCH candidate_parent USING INDEX change_thread_id") || !strings.Contains(joined, "SEARCH base_parent USING INDEX inventory_thread_id") {
				t.Fatalf("correlated parent lookup lost its retained indexes:\n%s", joined)
			}
			t.Log(joined)
		}

		return validateCandidateParents(ctx, conn, "none", 1)
	})
	if err != nil {
		t.Fatal(err)
	}
}
