package syncer

import (
	"context"
	"encoding/json"
	"errors"
	gh "github.com/openclaw/gitcrawl/internal/github"
	"github.com/openclaw/gitcrawl/internal/store"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

type historyFixtureClient struct {
	*gh.Client
	batch gh.HistoryBatch
	err   error
}

func (f historyFixtureClient) FetchGraphQLHistory(context.Context, string, string, []int, gh.Reporter) (gh.HistoryBatch, error) {
	return f.batch, f.err
}
func TestGraphQLHistoryUsesNativeTransactionsAndPreservesLegacyIdentity(t *testing.T) {
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
	before, err := st.ListThreads(ctx, repo.ID, true)
	if err != nil || len(before) != 1 {
		t.Fatalf("before=%+v err=%v", before, err)
	}
	var row map[string]any
	if err := json.Unmarshal([]byte(before[0].RawJSON), &row); err != nil {
		t.Fatal(err)
	}
	row["id"] = "PR_new_namespace"
	row["_gitcrawl_source"] = "graphql"
	pull, _ := legacy.GetPull(ctx, "openclaw", "gitcrawl", 8, nil)
	comments, _ := legacy.ListIssueComments(ctx, "openclaw", "gitcrawl", 8, nil)
	reviews, _ := legacy.ListPullReviews(ctx, "openclaw", "gitcrawl", 8, nil)
	inline, _ := legacy.ListPullReviewComments(ctx, "openclaw", "gitcrawl", 8, nil)
	rawRepo, _ := legacy.GetRepo(ctx, "openclaw", "gitcrawl", nil)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("REST fallback %s", r.URL)
		http.Error(w, "REST forbidden", 500)
	}))
	defer server.Close()
	client := historyFixtureClient{Client: gh.New(gh.Options{BaseURL: server.URL}), batch: gh.HistoryBatch{Repository: rawRepo, Items: []gh.HistoryItem{{Thread: row, Pull: pull, Comments: comments, Reviews: reviews, ReviewComments: inline}}}}
	options.GraphQLHistory = true
	client.batch.Items[0].ReviewThreads = []map[string]any{{"id": "RT_fixture", "isResolved": true, "isOutdated": false, "comments": map[string]any{"nodes": []any{map[string]any{"id": "inline-node", "body": "reply", "replyTo": map[string]any{"id": "parent-node"}}}}}}
	stats, err := New(client, st).Sync(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	after, err := st.ListThreads(ctx, repo.ID, true)
	if err != nil || len(after) != 1 {
		t.Fatalf("after=%+v err=%v", after, err)
	}
	if after[0].ID != before[0].ID || after[0].GitHubID != before[0].GitHubID {
		t.Fatalf("identity changed before=%+v after=%+v", before, after)
	}
	if stats.ThreadsSynced != 1 || stats.PRDetailsSynced != 1 || stats.CommentsSynced == 0 {
		t.Fatalf("stats %+v", stats)
	}
	if stats.ReviewThreadsSynced != 1 {
		t.Fatalf("review states missing: %+v", stats)
	}
	var resolved int
	var members string
	st.DB().QueryRow("SELECT is_resolved FROM pull_request_review_threads WHERE review_thread_id='RT_fixture'").Scan(&resolved)
	st.DB().QueryRow("SELECT review_thread_ids_json FROM pull_request_review_thread_syncs WHERE thread_id=?", after[0].ID).Scan(&members)
	if resolved != 1 || members != `["RT_fixture"]` {
		t.Fatalf("review projection %d %s", resolved, members)
	}
	if _, err := New(client, st).Sync(ctx, options); err != nil {
		t.Fatal(err)
	}
	assertTableRowCount(t, st, "threads", 1)
	client.err = errors.New("partial GraphQL failure")
	if _, err := New(client, st).Sync(ctx, options); err == nil {
		t.Fatal("failed batch persisted")
	}
	assertTableRowCount(t, st, "threads", 1)
	var failed int
	if err := st.DB().QueryRow("SELECT count(*) FROM analytics_fetch_attempts WHERE status='failed'").Scan(&failed); err != nil || failed != 1 {
		t.Fatalf("missing durable fetch failure: %d %v", failed, err)
	}
	options.IncludePRDetails = true
	if _, err := New(client, st).Sync(ctx, options); err == nil {
		t.Fatal("unsupported hydration accepted")
	}
}
