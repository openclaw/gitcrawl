//go:build darwin || linux

package remotedelivery

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/openclaw/gitcrawl/internal/store"
)

func TestNativeChangesWithQualifiedOutboxMatchFullCapture(t *testing.T) {
	ctx, native := nativeFixture(t)
	third, err := native.UpsertThread(ctx, store.Thread{RepoID: 1, GitHubID: "third", Number: 99, Kind: "issue", State: "open", Title: "hidden", RawJSON: `{"node_id":"third-node"}`, LabelsJSON: "[]", AssigneesJSON: "[]", ContentHash: "third"})
	if err != nil {
		t.Fatal(err)
	}
	for i, parent := range []int64{1, 1, third} {
		if _, err := native.UpsertComment(ctx, store.Comment{ThreadID: parent, GitHubID: string(rune('a' + i)), CommentType: "issue_comment", RawJSON: "{}"}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := native.DB().ExecContext(ctx, `insert into thread_exclusions(repository,number,kind,original_thread_id,github_id,excluded_at,reason,plan_id) values('fixture/repo',99,'issue',3,'third','2026-01-01','owner_requested','fixture')`); err != nil {
		t.Fatal(err)
	}
	if err := native.PrepareNativeReportIndexes(ctx); err != nil {
		t.Fatal(err)
	}
	baseline, _ := projected(t, ctx, native)
	seed, path := newSeedOutbox(t, ctx, baseline)
	completeInventorySeed(t, ctx, seed)
	if err := MigrateSeedOutboxV2(ctx, path, inventoryTestLimits); err != nil {
		t.Fatal(err)
	}
	o, err := OpenReportOutbox(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	// Move a child, delete its old parent, renumber the new parent, and reveal
	// an unchanged previously excluded subtree in one native journal interval.
	if _, err := native.DB().ExecContext(ctx, `update comments set thread_id=2,body='moved' where id=1; delete from threads where id=1; update threads set number=30 where id=2; delete from thread_exclusions where repository='fixture/repo' and number=99;`); err != nil {
		t.Fatal(err)
	}
	var delta store.NativeReportDelta
	err = o.WithQualifiedReportIndex(ctx, func(state ReportOutboxState, index store.NativeReportOldIndex) error {
		anchor := state.BaselineAnchor
		var err error
		delta, err = store.CaptureNativeReportChanges(ctx, native.Path(), store.NativeReportDeltaRequest{Baseline: anchor, Limits: store.NativeReportDeltaLimits{MaxJournalRecords: 1000, MaxAffectedKeys: 1000, MaxLookupRows: 10000, MaxRows: 1000, MaxNativeBytes: 16 << 20}}, index)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := nativeCapture(ctx, native)
	if err != nil {
		t.Fatal(err)
	}
	codec, err := privateInventoryCodec()
	if err != nil {
		t.Fatal(err)
	}
	keyID := func(key store.NativeReportKey) string {
		t.Helper()
		wire, err := inventoryNativeKey(key)
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := codec.EncodeKey(key.Table, wire)
		if err != nil {
			t.Fatal(err)
		}
		return key.Table + "\x00" + string(encoded)
	}
	indexCapture := func(c store.NativeReportCapture) map[string][]store.NativeReportValue {
		rows := map[string][]store.NativeReportValue{}
		for _, table := range c.Tables {
			for _, row := range table.Rows {
				key := store.NativeReportKey{Table: table.Name}
				for _, column := range table.KeyColumns {
					key.Values = append(key.Values, row[slices.Index(table.Columns, column)])
				}
				rows[keyID(key)] = row
			}
		}
		return rows
	}
	applied := indexCapture(baseline)
	for _, ref := range delta.Removals {
		delete(applied, keyID(ref.Key))
	}
	for _, row := range delta.Upserts {
		applied[keyID(row.Key)] = row.Values
	}
	if !reflect.DeepEqual(applied, indexCapture(fresh)) {
		t.Fatal("journal delta plus old inventory differs from full native capture")
	}
	if delta.From != baseline.Anchor || delta.Through != fresh.Anchor || delta.Rows != int64(len(delta.Upserts)+len(delta.Removals)) {
		t.Fatal("delta anchor/count mismatch")
	}
	// A node exclusion owned by another repository must still hold this scope.
	if _, err := native.DB().ExecContext(ctx, `insert into thread_exclusions(repository,number,kind,original_thread_id,github_id,excluded_at,reason,plan_id) values('other/repo',1,'issue',1,'other','2026-01-01','owner_requested','fixture'); insert into thread_excluded_nodes(node_id,repository,number) values('third-node','other/repo',1)`); err != nil {
		t.Fatal(err)
	}
	err = o.WithQualifiedReportIndex(ctx, func(state ReportOutboxState, index store.NativeReportOldIndex) error {
		anchor := state.BaselineAnchor
		result, err := store.CaptureNativeReportChanges(ctx, native.Path(), store.NativeReportDeltaRequest{Baseline: anchor, Limits: store.NativeReportDeltaLimits{MaxJournalRecords: 1000, MaxAffectedKeys: 1000, MaxLookupRows: 10000, MaxRows: 1000, MaxNativeBytes: 16 << 20}}, index)
		if err == nil || !reflect.DeepEqual(result, store.NativeReportDelta{}) {
			t.Fatal("global policy conflict produced a partial capture", err)
		}
		return err
	})
	if !errors.Is(err, store.ErrNativeReportSource) {
		t.Fatal("cross-repository node exclusion was ignored", err)
	}
}

func TestQualifiedReportIndexRetainsDeletedParentAndPinsSnapshot(t *testing.T) {
	ctx, native := nativeFixture(t)
	for _, parent := range []int64{1, 2} {
		if _, err := native.UpsertComment(ctx, store.Comment{ThreadID: parent, GitHubID: string(rune('a' + parent)), CommentType: "issue_comment", RawJSON: `{"node_id":"shared-node"}`}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := native.ReserveThreadChildObservation(ctx, 1, store.ThreadChildComments, "2026-01-01", 1); err != nil {
		t.Fatal(err)
	}
	if err := native.ReplaceThreadChildObservationMembers(ctx, 1, store.ThreadChildComments, 1, []int64{1}); err != nil {
		t.Fatal(err)
	}
	capture, _ := projected(t, ctx, native)
	seed, path := newSeedOutbox(t, ctx, capture)
	completeInventorySeed(t, ctx, seed)
	if err := MigrateSeedOutboxV2(ctx, path, inventoryTestLimits); err != nil {
		t.Fatal(err)
	}
	o, err := OpenReportOutbox(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	other, err := OpenReportOutbox(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	// Source deletion does not erase the old qualified parent/child identities.
	if _, err := native.DB().ExecContext(ctx, "delete from threads where id=1"); err != nil {
		t.Fatal(err)
	}
	var retained store.NativeReportOldIndex
	err = o.WithQualifiedReportIndex(ctx, func(state ReportOutboxState, index store.NativeReportOldIndex) error {
		anchor := state.BaselineAnchor
		retained = index
		if anchor != capture.Anchor {
			t.Fatal("baseline anchor changed")
		}
		var subtree []store.NativeReportRef
		if err := index.ThreadSubtrees(ctx, []int64{1}, func(ref store.NativeReportRef) error { subtree = append(subtree, ref); return nil }); err != nil {
			return err
		}
		if len(subtree) != 4 {
			t.Fatalf("missing deleted-parent closure: %d", len(subtree))
		}
		for _, ref := range subtree {
			if ref.RepositoryID != capture.Anchor.RepositoryID || ref.ThreadID != 1 || ref.ThreadNumber != 10 {
				t.Fatalf("wrong retained ownership: %+v", ref)
			}
		}
		var numbered []store.NativeReportRef
		if err := index.ThreadNumbers(ctx, []int64{10}, func(ref store.NativeReportRef) error { numbered = append(numbered, ref); return nil }); err != nil {
			return err
		}
		slices.SortFunc(subtree, func(a, b store.NativeReportRef) int { return strings.Compare(a.Key.Table, b.Key.Table) })
		slices.SortFunc(numbered, func(a, b store.NativeReportRef) int { return strings.Compare(a.Key.Table, b.Key.Table) })
		if !reflect.DeepEqual(subtree, numbered) {
			t.Fatal("number lookup differs from parent closure")
		}
		keys := []store.NativeReportKey{{Table: "repositories", Values: []store.NativeReportValue{{Kind: store.NativeReportInteger, Integer: capture.Anchor.RepositoryID}}}, {Table: "thread_child_observation_reservations", Values: []store.NativeReportValue{{Kind: store.NativeReportInteger, Integer: 1}, {Kind: store.NativeReportText, Text: "comments"}}}}
		var found []store.NativeReportRef
		if err := index.Keys(ctx, keys, func(ref store.NativeReportRef) error { found = append(found, ref); return nil }); err != nil {
			return err
		}
		if len(found) != 2 || !reflect.DeepEqual(found[0].Key, keys[0]) || !reflect.DeepEqual(found[1].Key, keys[1]) {
			t.Fatal("typed key lookup differs", found)
		}
		// A later writer cannot change this callback's pinned inventory view.
		if _, err := other.base.DB().ExecContext(ctx, "delete from inventory_rows where table_name='comments'"); err != nil {
			return err
		}
		var nodes []store.NativeReportRef
		if err := index.Nodes(ctx, []string{"shared-node"}, func(ref store.NativeReportRef) error { nodes = append(nodes, ref); return nil }); err != nil {
			return err
		}
		if len(nodes) != 2 || nodes[0].NodeID == nil || *nodes[0].NodeID != "shared-node" {
			t.Fatal("snapshot lost retained nodes", nodes)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := retained.Nodes(ctx, []string{"shared-node"}, func(store.NativeReportRef) error { return nil }); err == nil {
		t.Fatal("view survived its transaction")
	}
	err = o.WithQualifiedReportIndex(ctx, func(_ ReportOutboxState, index store.NativeReportOldIndex) error {
		return index.Nodes(ctx, []string{"shared-node"}, func(store.NativeReportRef) error { t.Fatal("new view retained deleted index rows"); return nil })
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestQualifiedReportIndexBoundsErrorsAndKeyIntegrity(t *testing.T) {
	ctx, native := nativeFixture(t)
	capture, _ := projected(t, ctx, native)
	seed, path := newSeedOutbox(t, ctx, capture)
	completeInventorySeed(t, ctx, seed)
	if err := MigrateSeedOutboxV2(ctx, path, inventoryTestLimits); err != nil {
		t.Fatal(err)
	}
	o, err := OpenReportOutbox(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	if err := o.WithQualifiedReportIndex(context.Background(), func(ReportOutboxState, store.NativeReportOldIndex) error { return nil }); !errors.Is(err, ErrSeedOutbox) {
		t.Fatal("unbounded view accepted", err)
	}
	if err := o.WithQualifiedReportIndex(ctx, nil); !errors.Is(err, ErrSeedOutbox) {
		t.Fatal("nil callback accepted", err)
	}
	sentinel := errors.New("stop index walk")
	err = o.WithQualifiedReportIndex(ctx, func(_ ReportOutboxState, index store.NativeReportOldIndex) error {
		if err := index.ThreadSubtrees(ctx, make([]int64, 501), func(store.NativeReportRef) error { return nil }); !errors.Is(err, ErrSeedOutbox) {
			t.Fatal("oversized lookup accepted", err)
		}
		if err := index.Keys(ctx, []store.NativeReportKey{{Table: "unknown"}}, func(store.NativeReportRef) error { return nil }); err == nil {
			t.Fatal("unknown key accepted")
		}
		if err := index.ThreadNumbers(ctx, []int64{10}, func(store.NativeReportRef) error { return sentinel }); !errors.Is(err, sentinel) {
			t.Fatal("callback error lost", err)
		}
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		if err := index.Nodes(canceled, []string{"node"}, func(store.NativeReportRef) error { return nil }); !errors.Is(err, context.Canceled) {
			t.Fatal("cancellation lost", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := o.base.DB().ExecContext(ctx, `update inventory_rows set key_json=cast('[{"kind":"integer","value":"999"}]' as blob) where table_name='threads'`); err != nil {
		t.Fatal(err)
	}
	err = o.WithQualifiedReportIndex(ctx, func(_ ReportOutboxState, index store.NativeReportOldIndex) error {
		return index.ThreadNumbers(ctx, []int64{10}, func(store.NativeReportRef) error { return nil })
	})
	if !errors.Is(err, ErrSeedOutbox) {
		t.Fatal("key metadata mismatch accepted", err)
	}
}
