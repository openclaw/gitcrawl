package store

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"testing"
	"time"
)

func nativeJournalEntries(t *testing.T, s *Store, after int64, table string) []changeContractEntry {
	t.Helper()
	var out []changeContractEntry
	for _, row := range changeContractEntriesAfter(t, s.db, after) {
		if row.Table == table {
			out = append(out, row)
		}
	}
	return out
}

func TestChangeContractNativeReportWritersRollbackAndPurge(t *testing.T) {
	s := changeContractFixture(t, 0)
	ctx := context.Background()
	start := changeContractSequence(t, s.db)
	ids := make([]int64, 2)
	for i := range ids {
		id, err := s.UpsertComment(ctx, Comment{ThreadID: int64(i + 1), GitHubID: fmt.Sprint(i + 1), CommentType: "issue_comment", Body: "PRIVATE_BODY_NOT_A_KEY", RawJSON: fmt.Sprintf(`{"node_id":"comment-%d"}`, i+1)})
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = id
	}
	if _, err := s.UpsertComment(ctx, Comment{ThreadID: 1, GitHubID: "1", CommentType: "issue_comment", Body: "changed", RawJSON: `{"node_id":"comment-1"}`}); err != nil {
		t.Fatal(err)
	}
	// Preserve a later sibling revision as required by native purge's ID guard.
	if _, err := s.UpsertComment(ctx, Comment{ThreadID: 2, GitHubID: "2", CommentType: "issue_comment", Body: "later sibling", RawJSON: `{"node_id":"comment-2"}`}); err != nil {
		t.Fatal(err)
	}
	entries := nativeJournalEntries(t, s, start, "comments")
	if len(entries) < 3 {
		t.Fatal("comment mutations have no row keys")
	}
	for _, row := range entries {
		if row.Key != strconv.FormatInt(ids[0], 10) && row.Key != strconv.FormatInt(ids[1], 10) {
			t.Fatal("comment log contains something other than its native key")
		}
	}
	if _, err := s.ReserveThreadChildObservation(ctx, 1, ThreadChildComments, "2026-01-01", 1); err != nil {
		t.Fatal(err)
	}
	start = changeContractSequence(t, s.db)
	if err := s.ReplaceThreadChildObservationMembers(ctx, 1, ThreadChildComments, 1, []int64{ids[0]}); err != nil {
		t.Fatal(err)
	}
	if err := s.ReplaceThreadChildObservationMembers(ctx, 1, ThreadChildComments, 1, nil); err != nil {
		t.Fatal(err)
	}
	entries = nativeJournalEntries(t, s, start, "thread_child_observation_memberships")
	if len(entries) != 2 || entries[0].Op != ChangeContractOpInsert || entries[1].Op != ChangeContractOpUpdate || entries[0].Key != `[1,"comments"]` || entries[1].Key != entries[0].Key {
		t.Fatal("membership writer did not journal stable composite key")
	}
	start = changeContractSequence(t, s.db)
	rollback := errors.New("fixture rollback")
	err := s.WithTx(ctx, func(tx *Store) error {
		if _, err := tx.UpsertComment(ctx, Comment{ThreadID: 1, GitHubID: "1", CommentType: "issue_comment", Body: "rollback", RawJSON: "{}"}); err != nil {
			return err
		}
		if err := tx.ReplaceThreadChildObservationMembers(ctx, 1, ThreadChildComments, 1, []int64{ids[0]}); err != nil {
			return err
		}
		return rollback
	})
	if !errors.Is(err, rollback) || changeContractSequence(t, s.db) != start || len(changeContractEntriesAfter(t, s.db, start)) != 0 {
		t.Fatal("rolled back native writers advanced journal", err)
	}
	plan, err := s.PlanThreadPurge(ctx, "fixture/repo", []int{10})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.PurgeThreads(ctx, "fixture/repo", []int{10}, plan.PlanID); err != nil {
		t.Fatal(err)
	}
	for table, want := range map[string]string{"comments": strconv.FormatInt(ids[0], 10), "thread_child_observation_memberships": `[1,"comments"]`} {
		got := nativeJournalEntries(t, s, start, table)
		if !slices.ContainsFunc(got, func(e changeContractEntry) bool { return e.Op == ChangeContractOpDelete && e.Key == want }) {
			t.Fatalf("purge cascade lost %s old key", table)
		}
	}
	nodes := nativeJournalEntries(t, s, start, "thread_excluded_nodes")
	if len(nodes) != 1 || nodes[0].Op != ChangeContractOpInsert || nodes[0].Key != "comment-1" {
		t.Fatal("real purge did not journal global node exclusion")
	}
	start = changeContractSequence(t, s.db)
	if _, err := s.db.ExecContext(ctx, `update thread_excluded_nodes set node_id='replacement-node' where node_id='comment-1'`); err != nil {
		t.Fatal(err)
	}
	nodes = nativeJournalEntries(t, s, start, "thread_excluded_nodes")
	if len(nodes) != 2 || nodes[0].Op != ChangeContractOpDelete || nodes[0].Key != "comment-1" || nodes[1].Op != ChangeContractOpInsert || nodes[1].Key != "replacement-node" || nodes[1].Seq != nodes[0].Seq+1 {
		t.Fatal("node key change lost delete-old/insert-new")
	}
}

// Install the exact table-mode contract supported by the pre-upgrade owner,
// using its existing trigger generator. This reproduces old writable-rearm
// state without fetching/building an older executable or another journal.
func installLegacyReportTracking(t *testing.T, s *Store, omitNew bool) {
	t.Helper()
	ctx := context.Background()
	err := s.WithTx(ctx, func(tx *Store) error {
		q := tx.q()
		triggers := changeContractTriggers(changeContractTableSpec{15, "comments", ChangeContractModeTable}, []string{"id"})
		stored := make([]string, len(triggers))
		for i, tr := range triggers {
			if _, err := q.ExecContext(ctx, "drop trigger "+sqliteQuotedIdentifier(tr.name)); err != nil {
				return err
			}
			if _, err := q.ExecContext(ctx, tr.sql); err != nil {
				return err
			}
			if err := q.QueryRowContext(ctx, `select sql from sqlite_schema where type='trigger' and name=?`, tr.name).Scan(&stored[i]); err != nil {
				return err
			}
		}
		if _, err := q.ExecContext(ctx, `update change_contract_tables set mode='table',insert_trigger=?,update_trigger=?,delete_trigger=? where table_id=15`, stored[0], stored[1], stored[2]); err != nil {
			return err
		}
		if omitNew {
			for _, name := range []string{"thread_child_observation_memberships", "thread_excluded_nodes"} {
				for _, event := range []string{"insert", "update", "delete"} {
					if _, err := q.ExecContext(ctx, "drop trigger "+sqliteQuotedIdentifier(changeContractTriggerPrefix+name+"_"+event)); err != nil {
						return err
					}
				}
				if _, err := q.ExecContext(ctx, `delete from change_contract_tables where table_name=?`, name); err != nil {
					return err
				}
			}
		}
		return bumpChangeContractGeneration(ctx, q, "legacy writer rearm", time.Now())
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestChangeContractNativeReportLegacyModeAndRearm(t *testing.T) {
	s := changeContractFixture(t, 0)
	ctx := context.Background()
	installLegacyReportTracking(t, s, true)
	legacy, err := s.ChangeContractStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	start := changeContractSequence(t, s.db)
	if _, err := s.UpsertComment(ctx, Comment{ThreadID: 1, GitHubID: "one", CommentType: "issue_comment", Body: "legacy", RawJSON: "{}"}); err != nil {
		t.Fatal(err)
	}
	if changeContractSequence(t, s.db) <= start || len(nativeJournalEntries(t, s, start, "comments")) != 0 {
		t.Fatal("fixture does not represent old table-mode writer")
	}
	path := s.Path()
	s.Close()
	s, err = Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	upgraded, err := s.ChangeContractStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if upgraded.Epoch != legacy.Epoch || upgraded.Generation != legacy.Generation+1 || upgraded.SchemaVersion == legacy.SchemaVersion || upgraded.ContractVersion != legacy.ContractVersion || !upgraded.Armed {
		t.Fatal("upgrade did not preserve owner and invalidate old cursor")
	}
	for name, id := range map[string]int64{"comments": 15, "thread_child_observation_memberships": 38, "thread_excluded_nodes": 39} {
		if !slices.ContainsFunc(upgraded.Tables, func(row ChangeContractTableStatus) bool {
			return row.Table == name && row.TableID == id && row.Mode == ChangeContractModeRows && row.Armed
		}) {
			t.Fatal("upgrade missing stable row-mode table", name)
		}
	}
	start = changeContractSequence(t, s.db)
	if _, err := s.UpsertComment(ctx, Comment{ThreadID: 1, GitHubID: "one", CommentType: "issue_comment", Body: "upgraded", RawJSON: "{}"}); err != nil {
		t.Fatal(err)
	}
	if len(nativeJournalEntries(t, s, start, "comments")) == 0 {
		t.Fatal("upgraded writer has no comment keys")
	}
	// An old writable open can demote comments again while leaving the appended
	// tables intact. Status must report the stored mode, not this binary's spec.
	installLegacyReportTracking(t, s, false)
	downgraded, err := s.ChangeContractStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(downgraded.Tables, func(row ChangeContractTableStatus) bool {
		return row.Table == "comments" && row.Mode == ChangeContractModeTable && row.Armed
	}) {
		t.Fatal("status hides legacy mode downgrade")
	}
	s.Close()
	s, err = Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	repaired, err := s.ChangeContractStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if repaired.Generation != downgraded.Generation+1 || !slices.ContainsFunc(repaired.Tables, func(row ChangeContractTableStatus) bool {
		return row.Table == "comments" && row.Mode == ChangeContractModeRows && row.Armed
	}) {
		t.Fatal("writable reopen failed to repair valid legacy-mode triggers")
	}
	again, err := s.EnableChangeContract(ctx, ChangeContractEnableOptions{})
	if err != nil || len(again.Rearmed) != 0 || again.Generation != repaired.Generation {
		t.Fatal("healthy rearm is not idempotent", err)
	}
}
