package store

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

type recordedQuery struct {
	query string
	args  []any
}

type queryRecorder struct {
	dbQueries
	recorded []recordedQuery
}

func (r *queryRecorder) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	r.recorded = append(r.recorded, recordedQuery{query: query, args: args})
	return r.dbQueries.QueryContext(ctx, query, args...)
}

// Every candidate page must read the ordering index instead of scanning and
// sorting all of the repository's threads, which made collection quadratic.
func TestEmbeddingCandidatePagesSeekOrderingIndex(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	repoID, err := st.UpsertRepository(ctx, Repository{
		Owner: "fixture", Name: "repo", FullName: "fixture/repo", RawJSON: "{}",
		UpdatedAt: "2026-01-01T00:00:00Z",
	})
	if err != nil {
		t.Fatal(err)
	}
	for number := 1; number <= embeddingCandidatePageSize+2; number++ {
		state := "open"
		if number%2 == 0 {
			state = "closed"
		}
		_, err := st.UpsertThread(ctx, Thread{
			RepoID: repoID, GitHubID: fmt.Sprint(number), Number: number,
			Kind: "issue", State: state, Title: fmt.Sprintf("Thread %d", number),
			Body: "body", RawJSON: "{}", LabelsJSON: "[]", AssigneesJSON: "[]",
			ContentHash: "fixture", UpdatedAtGitHub: fmt.Sprintf("2026-01-01T00:%02d:%02dZ", number/60, number%60),
			UpdatedAt: "2026-01-01T00:00:00Z",
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	recorder := &queryRecorder{dbQueries: st.q()}
	recording := *st
	recording.queries = recorder
	tasks, err := recording.ListEmbeddingTasks(ctx, EmbeddingTaskOptions{
		RepoID: repoID, Model: "fixture", Force: true, IncludeClosed: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != embeddingCandidatePageSize+2 {
		t.Fatalf("tasks=%d", len(tasks))
	}

	pages := 0
	for _, recorded := range recorder.recorded {
		if !strings.Contains(recorded.query, "existing_hash") {
			continue
		}
		pages++
		plan := topLevelQueryPlan(t, st, recorded)
		if strings.Contains(plan, "TEMP B-TREE FOR ORDER BY") {
			t.Fatalf("candidate page %d sorts every thread:\n%s", pages, plan)
		}
		want := "USING INDEX idx_threads_repo_embed_order (repo_id=?)"
		if pages > 1 {
			want = "USING INDEX idx_threads_repo_embed_order (repo_id=? AND <expr><?)"
		}
		if !strings.Contains(plan, want) {
			t.Fatalf("candidate page %d plan lacks %q:\n%s", pages, want, plan)
		}
	}
	if pages != 2 {
		t.Fatalf("candidate pages=%d, want 2", pages)
	}
}

// The plan rows of the outer select; correlated subqueries keep their own sorts.
func topLevelQueryPlan(t *testing.T, st *Store, recorded recordedQuery) string {
	t.Helper()
	rows, err := st.DB().Query("explain query plan "+recorded.query, recorded.args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var plan strings.Builder
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		if parent == 0 {
			fmt.Fprintln(&plan, detail)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return plan.String()
}

func TestEmbeddingOrderingIndexAddedOnReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "archive.db")
	st, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.DB().Exec(`drop index if exists idx_threads_repo_embed_order`); err != nil {
		t.Fatal(err)
	}
	var before int
	if err := st.DB().QueryRow(`pragma schema_version`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	for pass := range 2 {
		st, err = Open(ctx, path)
		if err != nil {
			t.Fatal(err)
		}
		var indexes, schema, version int
		if err := st.DB().QueryRow(`select count(*) from sqlite_schema where type = 'index' and name = 'idx_threads_repo_embed_order'`).Scan(&indexes); err != nil {
			t.Fatal(err)
		}
		if err := st.DB().QueryRow(`pragma schema_version`).Scan(&schema); err != nil {
			t.Fatal(err)
		}
		if err := st.DB().QueryRow(`pragma user_version`).Scan(&version); err != nil {
			t.Fatal(err)
		}
		if err := st.Close(); err != nil {
			t.Fatal(err)
		}
		// Only CREATE INDEX changes the schema; neither open rebuilds a table.
		if indexes != 1 || schema != before+1 || version != schemaVersion {
			t.Fatalf("reopen %d: indexes=%d schema=%d (before=%d) version=%d", pass, indexes, schema, before, version)
		}
	}
}
