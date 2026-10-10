package store

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

// threadVectorWhereBeforeTimeFilters is threadVectorWhere as it was before the
// time and number filters. Callers that set none of them (search, cluster,
// refresh, the TUI, and neighbors without the new flags) must keep this SQL.
func threadVectorWhereBeforeTimeFilters(query ThreadVectorQuery) (string, []any) {
	where := `t.repo_id = ?`
	args := []any{query.RepoID}
	if !query.IncludeClosed {
		where += ` and t.state = 'open' and t.closed_at_local is null`
	}
	if query.Model != "" {
		where += ` and tv.model = ?`
		args = append(args, query.Model)
	}
	if query.Basis != "" {
		where += ` and tv.basis = ?`
		args = append(args, query.Basis)
	}
	if query.Dimensions > 0 {
		where += ` and tv.dimensions = ?`
		args = append(args, query.Dimensions)
	}
	return where, args
}

func TestThreadVectorWhereWithoutTimeFiltersIsUnchanged(t *testing.T) {
	for _, includeClosed := range []bool{false, true} {
		for _, model := range []string{"", "text-embedding-3-small"} {
			for _, basis := range []string{"", "title_original"} {
				for _, dimensions := range []int{0, 1024} {
					query := ThreadVectorQuery{RepoID: 7, Model: model, Basis: basis, Dimensions: dimensions, IncludeClosed: includeClosed}
					gotWhere, gotArgs := threadVectorWhere(query)
					wantWhere, wantArgs := threadVectorWhereBeforeTimeFilters(query)
					if gotWhere != wantWhere || !reflect.DeepEqual(gotArgs, wantArgs) {
						t.Fatalf("threadVectorWhere(%+v) = %q %v, want %q %v", query, gotWhere, gotArgs, wantWhere, wantArgs)
					}
				}
			}
		}
	}
}

func TestStateAsOfUsesNewestCompleteListSync(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	repoID, err := st.UpsertRepository(ctx, Repository{Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl", RawJSON: "{}", UpdatedAt: "2026-04-26T00:00:00Z"})
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	otherRepoID, err := st.UpsertRepository(ctx, Repository{Owner: "openclaw", Name: "openclaw", FullName: "openclaw/openclaw", RawJSON: "{}", UpdatedAt: "2026-04-26T00:00:00Z"})
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	record := func(repoID int64, scope, status, started, stats string) {
		t.Helper()
		if _, err := st.RecordRun(ctx, RunRecord{RepoID: repoID, Kind: "sync", Scope: scope, Status: status, StartedAt: started, FinishedAt: started, StatsJSON: stats}); err != nil {
			t.Fatalf("record run: %v", err)
		}
	}
	check := func(want string) {
		t.Helper()
		got, err := st.StateAsOf(ctx, repoID)
		if err != nil {
			t.Fatalf("state as of: %v", err)
		}
		if want == "" && !got.IsZero() || want != "" && got.UTC().Format(time.RFC3339) != want {
			t.Fatalf("state as of = %v, want %q", got, want)
		}
	}

	check("")
	// A limited or since-bounded open sync records no sweep.
	record(repoID, "open", "success", "2026-09-01T00:00:00Z", `{"repository":"openclaw/gitcrawl"}`)
	check("")
	record(repoID, "open", "success", "2026-09-02T00:00:00Z", `{"closed_sweep_through":"2026-09-02T00:00:00Z"}`)
	check("2026-09-02T00:00:00Z")
	record(repoID, "all", "success", "2026-09-03T00:00:00+09:00", `{"closed_sweep_through":"2026-09-03T00:00:00+09:00"}`)
	check("2026-09-02T15:00:00Z")
	// None of these certify every row's state.
	record(repoID, "numbers:1,2", "success", "2026-09-05T00:00:00Z", `{"closed_sweep_through":"2026-09-05T00:00:00Z"}`)
	record(repoID, "closed", "success", "2026-09-05T00:00:00Z", `{"closed_sweep_through":"2026-09-05T00:00:00Z"}`)
	record(repoID, "open", "checkpoint", "2026-09-05T00:00:00Z", `{"closed_sweep_through":"2026-09-05T00:00:00Z"}`)
	record(repoID, "open", "failed", "2026-09-05T00:00:00Z", `{"closed_sweep_through":"2026-09-05T00:00:00Z"}`)
	record(repoID, "open", "success", "2026-09-05T00:00:00Z", `not json`)
	record(otherRepoID, "open", "success", "2026-09-05T00:00:00Z", `{"closed_sweep_through":"2026-09-05T00:00:00Z"}`)
	check("2026-09-02T15:00:00Z")
	// Ordered by instant, not by row id or text.
	record(repoID, "open", "success", "2026-09-02T20:00:00-05:00", `{"closed_sweep_through":"2026-09-02T20:00:00-05:00"}`)
	check("2026-09-03T01:00:00Z")
}
