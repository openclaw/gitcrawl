package store

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestNativeReportNodeIndexesMatchPolicy(t *testing.T) {
	s := changeContractFixture(t, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	raws := []string{`{"node_id":"123"}`, `{"node_id":123}`, `{"node_id":123.0}`,
		`{"node_id":1e2}`, `{"node_id":true}`, `{"node_id":null}`, `{}`, `invalid`,
		`{"node_id":"first","node_id":"last"}`, `{"node_id":{"id":1}}`, `{"node_id":""}`}
	for i, raw := range raws {
		id, err := s.UpsertThread(ctx, Thread{RepoID: 1, GitHubID: fmt.Sprint(i + 100), Number: i + 100,
			Kind: "issue", State: "open", Title: "fixture", RawJSON: raw, LabelsJSON: "[]", AssigneesJSON: "[]", ContentHash: "fixture"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.UpsertComment(ctx, Comment{ThreadID: id, GitHubID: fmt.Sprint(i + 100), CommentType: "issue_comment", Body: "fixture", RawJSON: raw}); err != nil {
			t.Fatal(err)
		}
	}
	query := "select id from threads where " + nativeReportNodeKeySQL + "=? and repo_id=?"
	before := topLevelQueryPlan(t, s, recordedQuery{query, []any{"123", 1}})
	if strings.Contains(before, "idx_threads_report_node") {
		t.Fatal("ordinary open unexpectedly prepared report indexes")
	}
	var cookie int
	if err := s.db.QueryRowContext(ctx, "pragma schema_version").Scan(&cookie); err != nil {
		t.Fatal(err)
	}
	if err := s.PrepareNativeReportIndexes(ctx); err != nil {
		t.Fatal(err)
	}
	var prepared int
	if err := s.db.QueryRowContext(ctx, "pragma schema_version").Scan(&prepared); err != nil || prepared == cookie {
		t.Fatal("index preparation did not invalidate old schema cookie", err)
	}
	if err := s.PrepareNativeReportIndexes(ctx); err != nil {
		t.Fatal(err)
	}
	var again int
	if err := s.db.QueryRowContext(ctx, "pragma schema_version").Scan(&again); err != nil || again != prepared {
		t.Fatal("prepared index operation is not idempotent", err)
	}
	ids := func(query string, args ...any) []int64 {
		t.Helper()
		rows, err := s.db.QueryContext(ctx, query, args...)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var out []int64
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				t.Fatal(err)
			}
			out = append(out, id)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return out
	}
	if _, err := s.db.ExecContext(ctx, `insert into thread_exclusions(repository,number,kind,original_thread_id,github_id,excluded_at,reason,plan_id) values('other/repo',1,'issue',1,'fixture','2026-01-01','owner_requested','fixture')`); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"threads", "comments"} {
		indexed := "select id from " + table + " where " + nativeReportNodeKeySQL + "=? order by id"
		plan := topLevelQueryPlan(t, s, recordedQuery{indexed, []any{"123"}})
		if !strings.Contains(plan, "SEARCH "+table+" USING ") || !strings.Contains(plan, "INDEX idx_"+table+"_report_node") {
			t.Fatal("reverse lookup does not use its index", table, plan)
		}
		for _, node := range []string{"123", "123.0", "100.0", "1", "true", "null", "first", "last", `{"id":1}`, "", "missing"} {
			if _, err := s.db.ExecContext(ctx, "insert or ignore into thread_excluded_nodes(node_id,repository,number) values(?, 'other/repo', 1)", node); err != nil {
				t.Fatal(err)
			}
			reference := "select x.id from " + table + ` x where exists(select 1 from thread_excluded_nodes e where e.node_id=? and e.node_id in (select json_extract(case when json_valid(x.raw_json) then x.raw_json else '{}' end,'$.node_id'))) order by x.id`
			if got, want := ids(indexed, node), ids(reference, node); !reflect.DeepEqual(got, want) {
				t.Fatalf("%s reverse node %q: got %v want %v", table, node, got, want)
			}
		}
	}
}

func TestNativeReportIndexPreparationAdmissionAndRollback(t *testing.T) {
	s := changeContractFixture(t, 0)
	if err := s.PrepareNativeReportIndexes(context.Background()); err == nil {
		t.Fatal("unbounded index preparation accepted")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	canceled, stop := context.WithCancel(ctx)
	stop()
	if err := s.PrepareNativeReportIndexes(canceled); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation lost", err)
	}
	// The second DDL fails after the first index is created in the transaction.
	if _, err := s.db.ExecContext(ctx, "create table idx_comments_report_node(x)"); err != nil {
		t.Fatal(err)
	}
	if err := s.PrepareNativeReportIndexes(ctx); err == nil {
		t.Fatal("conflicting schema object accepted")
	}
	var count int
	if err := s.db.QueryRowContext(ctx, "select count(*) from sqlite_schema where name='idx_threads_report_node'").Scan(&count); err != nil || count != 0 {
		t.Fatal("failed preparation retained its first index", err)
	}
	for _, ddl := range []string{"pragma user_version=15", "pragma user_version=16; create table portable_metadata(x)"} {
		if _, err := s.db.ExecContext(ctx, ddl); err != nil {
			t.Fatal(err)
		}
		if err := s.PrepareNativeReportIndexes(ctx); !errors.Is(err, ErrNativeReportSource) {
			t.Fatal("unsupported source accepted", err)
		}
	}
}
