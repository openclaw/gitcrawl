package syncer

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	gh "github.com/openclaw/gitcrawl/internal/github"
	"github.com/openclaw/gitcrawl/internal/store"
)

type fakeGitHub struct{}

func (fakeGitHub) GetRepo(ctx context.Context, owner, repo string, reporter gh.Reporter) (map[string]any, error) {
	return map[string]any{"id": 123}, nil
}

func (fakeGitHub) ListRepositoryIssues(ctx context.Context, owner, repo string, options gh.ListIssuesOptions, reporter gh.Reporter) ([]map[string]any, error) {
	return []map[string]any{
		{
			"id":         1,
			"number":     7,
			"state":      "open",
			"title":      "download stalls",
			"body":       "large artifact download stalls",
			"html_url":   "https://github.com/openclaw/gitcrawl/issues/7",
			"created_at": "2026-04-26T00:00:00Z",
			"updated_at": "2026-04-26T00:00:00Z",
			"labels":     []map[string]any{{"name": "bug"}},
			"assignees":  []map[string]any{},
			"user":       map[string]any{"login": "vincentkoc", "type": "User"},
		},
		{
			"id":           2,
			"number":       8,
			"state":        "open",
			"title":        "fix sync",
			"body":         "",
			"html_url":     "https://github.com/openclaw/gitcrawl/pull/8",
			"created_at":   "2026-04-26T00:00:00Z",
			"updated_at":   "2026-04-26T00:00:00Z",
			"labels":       []map[string]any{},
			"assignees":    []map[string]any{},
			"user":         map[string]any{"login": "vincentkoc", "type": "User"},
			"pull_request": map[string]any{"url": "https://api.github.com/repos/openclaw/gitcrawl/pulls/8"},
		},
	}, nil
}

func (fakeGitHub) ListIssueComments(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) ([]map[string]any, error) {
	if number != 7 {
		return nil, nil
	}
	return []map[string]any{{
		"id":         11,
		"body":       "same bug here",
		"created_at": "2026-04-26T00:00:00Z",
		"updated_at": "2026-04-26T00:00:00Z",
		"user":       map[string]any{"login": "vincentkoc", "type": "User"},
	}}, nil
}

func (fakeGitHub) ListPullReviews(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) ([]map[string]any, error) {
	return nil, nil
}

func (fakeGitHub) ListPullReviewComments(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) ([]map[string]any, error) {
	return nil, nil
}

func TestSyncPersistsIssuesAndPullRequests(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	s := New(fakeGitHub{}, st)
	s.now = func() time.Time { return time.Date(2026, 4, 26, 0, 0, 0, 0, time.UTC) }
	stats, err := s.Sync(ctx, Options{Owner: "openclaw", Repo: "gitcrawl", IncludeComments: true})
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if stats.ThreadsSynced != 2 || stats.IssuesSynced != 1 || stats.PullRequestsSynced != 1 {
		t.Fatalf("unexpected stats: %#v", stats)
	}
	if stats.CommentsSynced != 1 {
		t.Fatalf("comments synced: got %d want 1", stats.CommentsSynced)
	}

	repo, err := st.RepositoryByFullName(ctx, "openclaw/gitcrawl")
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	threads, err := st.ListThreads(ctx, repo.ID, false)
	if err != nil {
		t.Fatalf("threads: %v", err)
	}
	if len(threads) != 2 {
		t.Fatalf("threads: got %d want 2", len(threads))
	}
	if threads[1].Kind != "pull_request" {
		t.Fatalf("second thread kind: %s", threads[1].Kind)
	}
	var documentCount int
	if err := st.DB().QueryRowContext(ctx, `select count(*) from documents_fts where documents_fts match 'download'`).Scan(&documentCount); err != nil {
		t.Fatalf("query document index: %v", err)
	}
	if documentCount != 1 {
		t.Fatalf("document count: got %d want 1", documentCount)
	}
}
