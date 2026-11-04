package store

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestPartialIdentityLookupSkewedParity(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "lookup.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	// Isolated schema preserves the exact production index names and lookup columns.
	for _, q := range []string{"DROP TABLE IF EXISTS lookup_members", "CREATE TEMP TABLE lookup_groups(id INTEGER PRIMARY KEY,repo_id INTEGER,status TEXT,closed_at TEXT,cluster_type TEXT,stable_key TEXT,stable_slug TEXT,representative_thread_id INTEGER,title TEXT)", "CREATE INDEX temp.lookup_groups_repo ON lookup_groups(repo_id,status)", "CREATE TEMP TABLE lookup_members(cluster_id INTEGER,thread_id INTEGER,state TEXT,PRIMARY KEY(cluster_id,thread_id))", "CREATE INDEX temp.lookup_members_thread_state ON lookup_members(thread_id,state)"} {
		if _, err = s.DB().ExecContext(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	for i := 1; i <= 2000; i++ {
		if _, err = s.DB().ExecContext(ctx, "INSERT INTO lookup_groups VALUES(?,1,'active',NULL,'duplicate_candidate',?,?,?,?)", i, fmt.Sprint(i), fmt.Sprint(i), i, fmt.Sprint(i)); err != nil {
			t.Fatal(err)
		}
		if _, err = s.DB().ExecContext(ctx, "INSERT INTO lookup_members VALUES(?,?,'active')", i, 10000+i); err != nil {
			t.Fatal(err)
		}
	}
	for _, q := range []string{"INSERT INTO lookup_members VALUES(2,10,'active'),(2,11,'active'),(5,11,'active'),(7,12,'removed'),(8,13,'active'),(9,14,'active'),(10,15,'active'),(11,16,'active')", "UPDATE lookup_groups SET status='closed' WHERE id=8", "UPDATE lookup_groups SET closed_at='closed' WHERE id=9", "UPDATE lookup_groups SET cluster_type='similarity' WHERE id=10", "UPDATE lookup_groups SET repo_id=2 WHERE id=11"} {
		if _, err = s.DB().ExecContext(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	old := `SELECT DISTINCT cg.id,cg.stable_key,cg.stable_slug,coalesce(cg.cluster_type,''),coalesce(cg.representative_thread_id,0),coalesce(cg.title,'') FROM lookup_groups cg JOIN lookup_members cm ON cm.cluster_id=cg.id WHERE cg.repo_id=? AND cg.status='active' AND cg.closed_at IS NULL AND coalesce(cg.cluster_type,'')<>'similarity' AND cm.state='active' AND cm.thread_id IN (%s) ORDER BY cg.id`
	next := strings.Replace(old, "FROM lookup_groups cg JOIN lookup_members cm ON cm.cluster_id=cg.id", "FROM lookup_members cm INDEXED BY lookup_members_thread_state CROSS JOIN lookup_groups cg ON cg.id=cm.cluster_id", 1)
	read := func(q string, args []any) [][]any {
		rows, e := s.DB().QueryContext(ctx, q, args...)
		if e != nil {
			t.Fatal(e)
		}
		defer rows.Close()
		out := [][]any{}
		for rows.Next() {
			var id, rep int64
			var key, slug, kind, title string
			if e = rows.Scan(&id, &key, &slug, &kind, &rep, &title); e != nil {
				t.Fatal(e)
			}
			out = append(out, []any{id, key, slug, kind, rep, title})
		}
		if e = rows.Err(); e != nil {
			t.Fatal(e)
		}
		return out
	}
	for _, tc := range []struct {
		ids  []int
		want []int64
	}{{[]int{9}, nil}, {[]int{10}, []int64{2}}, {[]int{10, 10}, []int64{2}}, {[]int{10, 11}, []int64{2, 5}}, {[]int{12, 13, 14, 15, 16}, nil}, {[]int{10002, 10001}, []int64{1, 2}}} {
		args := []any{1}
		marks := []string{}
		for _, id := range tc.ids {
			args = append(args, id)
			marks = append(marks, "?")
		}
		a := read(fmt.Sprintf(old, strings.Join(marks, ",")), args)
		b := read(fmt.Sprintf(next, strings.Join(marks, ",")), args)
		if !reflect.DeepEqual(a, b) {
			t.Fatalf("row parity: %v != %v", a, b)
		}
		var ids []int64
		for _, row := range b {
			ids = append(ids, row[0].(int64))
		}
		if !reflect.DeepEqual(ids, tc.want) {
			t.Fatalf("distinct/ordered identities: %v want %v", ids, tc.want)
		}
	}
	rows, err := s.DB().QueryContext(ctx, "EXPLAIN QUERY PLAN "+fmt.Sprintf(next, "?"), 1, 10)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var plan []string
	for rows.Next() {
		var a, b, c int
		var detail string
		if err = rows.Scan(&a, &b, &c, &detail); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, detail)
	}
	if len(plan) < 2 || !strings.Contains(plan[0], "SEARCH cm USING INDEX lookup_members_thread_state") || !strings.Contains(plan[1], "SEARCH cg USING INTEGER PRIMARY KEY") {
		t.Fatalf("membership-first indexed plan: %v", plan)
	}
}
