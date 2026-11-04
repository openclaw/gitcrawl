package syncer

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	gh "github.com/openclaw/gitcrawl/internal/github"
	"github.com/openclaw/gitcrawl/internal/store"
)

func contractWrites(t *testing.T, st *store.Store, table string) int64 {
	t.Helper()
	var n int64
	if err := st.DB().QueryRow(`select count(*) from change_contract_log l join change_contract_tables t using(table_id) where t.table_name = ?`, table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// An identical GraphQL recheck on a record-efficient archive writes no content
// row in threads, pull_request_details, pull_request_review_threads or
// repositories; it records the check in the check tables.
func TestRecordEfficientGraphQLRecheckWritesNoContentRows(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	legacy := &metadataGitHub{}
	options := Options{Owner: "openclaw", Repo: "gitcrawl", Numbers: []int{8}, State: "all", IncludePRMetadata: true, IncludeComments: true}
	if _, err := New(legacy, st).Sync(ctx, options); err != nil {
		t.Fatal(err)
	}
	repo, err := st.RepositoryByFullName(ctx, "openclaw/gitcrawl")
	if err != nil {
		t.Fatal(err)
	}
	threads, err := st.ListThreads(ctx, repo.ID, true)
	if err != nil || len(threads) != 1 {
		t.Fatalf("threads=%+v err=%v", threads, err)
	}
	var row map[string]any
	if err := json.Unmarshal([]byte(threads[0].RawJSON), &row); err != nil {
		t.Fatal(err)
	}
	row["_gitcrawl_source"] = "graphql"
	pull, _ := legacy.GetPull(ctx, "openclaw", "gitcrawl", 8, nil)
	comments, _ := legacy.ListIssueComments(ctx, "openclaw", "gitcrawl", 8, nil)
	rawRepo, _ := legacy.GetRepo(ctx, "openclaw", "gitcrawl", nil)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("REST fallback %s", r.URL)
		http.Error(w, "REST forbidden", 500)
	}))
	defer server.Close()
	client := historyFixtureClient{Client: gh.New(gh.Options{BaseURL: server.URL}), batch: gh.HistoryBatch{Repository: rawRepo, Items: []gh.HistoryItem{{Thread: row, Pull: pull, Comments: comments,
		ReviewThreads: []map[string]any{{"id": "RT_fixture", "isResolved": true, "isOutdated": false, "comments": map[string]any{"nodes": []any{}}}}}}}}
	options.GraphQLHistory = true
	if _, err := New(client, st).Sync(ctx, options); err != nil {
		t.Fatal(err)
	}
	if _, err := st.EnableRecordEfficiency(ctx, "2026-10-30T00:00:00Z"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.EnableChangeContract(ctx, store.ChangeContractEnableOptions{}); err != nil {
		t.Fatal(err)
	}
	// The first recheck after enabling may record the format transition; the
	// following identical rechecks must not write content rows.
	if _, err := New(client, st).Sync(ctx, options); err != nil {
		t.Fatal(err)
	}
	content := []string{"threads", "pull_request_details", "pull_request_review_threads", "repositories", "comments"}
	before := map[string]int64{}
	for _, table := range content {
		before[table] = contractWrites(t, st, table)
	}
	checks := contractWrites(t, st, "thread_checks")
	fetches := contractWrites(t, st, "pull_request_detail_fetches")
	for i := 0; i < 3; i++ {
		if _, err := New(client, st).Sync(ctx, options); err != nil {
			t.Fatal(err)
		}
	}
	for _, table := range content {
		if got := contractWrites(t, st, table) - before[table]; got != 0 {
			t.Errorf("identical rechecks wrote %d %s rows, want 0", got, table)
		}
	}
	if contractWrites(t, st, "thread_checks") <= checks || contractWrites(t, st, "pull_request_detail_fetches") <= fetches {
		t.Fatal("rechecks must be recorded in the check tables")
	}
}
