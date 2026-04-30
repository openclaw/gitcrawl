package syncer

import (
	"context"
	"encoding/json"
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

func (fakeGitHub) GetIssue(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) (map[string]any, error) {
	if number == 8 {
		return map[string]any{
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
		}, nil
	}
	return map[string]any{
		"id":         1,
		"number":     7,
		"state":      "open",
		"title":      "download stalls",
		"body":       "large file download stalls",
		"html_url":   "https://github.com/openclaw/gitcrawl/issues/7",
		"created_at": "2026-04-26T00:00:00Z",
		"updated_at": "2026-04-26T00:00:00Z",
		"labels":     []map[string]any{{"name": "bug"}},
		"assignees":  []map[string]any{},
		"user":       map[string]any{"login": "vincentkoc", "type": "User"},
	}, nil
}

func (fakeGitHub) ListRepositoryIssues(ctx context.Context, owner, repo string, options gh.ListIssuesOptions, reporter gh.Reporter) ([]map[string]any, error) {
	if options.State == "closed" {
		return nil, nil
	}
	return []map[string]any{
		{
			"id":         1,
			"number":     7,
			"state":      "open",
			"title":      "download stalls",
			"body":       "large file download stalls",
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

type sinceCaptureGitHub struct {
	fakeGitHub
	since string
}

func (f *sinceCaptureGitHub) ListRepositoryIssues(ctx context.Context, owner, repo string, options gh.ListIssuesOptions, reporter gh.Reporter) ([]map[string]any, error) {
	f.since = options.Since
	return nil, nil
}

type stateCaptureGitHub struct {
	fakeGitHub
	state string
}

func (f *stateCaptureGitHub) ListRepositoryIssues(ctx context.Context, owner, repo string, options gh.ListIssuesOptions, reporter gh.Reporter) ([]map[string]any, error) {
	f.state = options.State
	return nil, nil
}

type closedSweepGitHub struct {
	fakeGitHub
}

func (closedSweepGitHub) ListRepositoryIssues(ctx context.Context, owner, repo string, options gh.ListIssuesOptions, reporter gh.Reporter) ([]map[string]any, error) {
	if options.State == "closed" {
		return []map[string]any{{
			"id":         1,
			"number":     7,
			"state":      "closed",
			"title":      "download stalls",
			"body":       "large file download stalls",
			"html_url":   "https://github.com/openclaw/gitcrawl/issues/7",
			"created_at": "2026-04-26T00:00:00Z",
			"updated_at": "2026-04-27T00:00:00Z",
			"closed_at":  "2026-04-27T00:00:00Z",
			"labels":     []map[string]any{{"name": "bug"}},
			"assignees":  []map[string]any{},
			"user":       map[string]any{"login": "vincentkoc", "type": "User"},
		}}, nil
	}
	return nil, nil
}

type targetedGitHub struct {
	fakeGitHub
	listCalled bool
	numbers    []int
}

func (f *targetedGitHub) GetIssue(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) (map[string]any, error) {
	f.numbers = append(f.numbers, number)
	return f.fakeGitHub.GetIssue(ctx, owner, repo, number, reporter)
}

func (f *targetedGitHub) ListRepositoryIssues(ctx context.Context, owner, repo string, options gh.ListIssuesOptions, reporter gh.Reporter) ([]map[string]any, error) {
	f.listCalled = true
	return nil, nil
}

type pullCommentGitHub struct {
	fakeGitHub
}

func (pullCommentGitHub) ListPullReviews(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) ([]map[string]any, error) {
	if number != 8 {
		return nil, nil
	}
	return []map[string]any{{
		"id":         81,
		"body":       "review body",
		"created_at": "2026-04-26T00:00:00Z",
		"updated_at": "2026-04-26T00:01:00Z",
		"user":       map[string]any{"login": "reviewbot[bot]", "type": "User"},
	}}, nil
}

func (pullCommentGitHub) ListPullReviewComments(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) ([]map[string]any, error) {
	if number != 8 {
		return nil, nil
	}
	return []map[string]any{{
		"id":         82,
		"body":       "line comment",
		"created_at": "2026-04-26T00:02:00Z",
		"updated_at": "2026-04-26T00:03:00Z",
		"user":       map[string]any{"login": "alice", "type": "Bot"},
	}}, nil
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
	if stats.MetadataOnly {
		t.Fatal("metadata only: got true want false")
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
	if err := st.DB().QueryRowContext(ctx, `select count(*) from documents_fts where documents_fts match 'failure OR bug'`).Scan(&documentCount); err != nil {
		t.Fatalf("query document index: %v", err)
	}
	if documentCount != 1 {
		t.Fatalf("document count: got %d want 1", documentCount)
	}
}

func TestSyncHydratesPullReviewComments(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	s := New(pullCommentGitHub{}, st)
	s.now = func() time.Time { return time.Date(2026, 4, 26, 0, 0, 0, 0, time.UTC) }
	stats, err := s.Sync(ctx, Options{Owner: "openclaw", Repo: "gitcrawl", Numbers: []int{8}, IncludeComments: true})
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if stats.CommentsSynced != 2 {
		t.Fatalf("comments synced = %d, want 2", stats.CommentsSynced)
	}
	repo, err := st.RepositoryByFullName(ctx, "openclaw/gitcrawl")
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	threads, err := st.ListThreads(ctx, repo.ID, true)
	if err != nil {
		t.Fatalf("threads: %v", err)
	}
	if len(threads) != 1 || threads[0].Kind != "pull_request" {
		t.Fatalf("threads = %+v", threads)
	}
}

func TestSyncCanTargetIssueNumbers(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	client := &targetedGitHub{}
	s := New(client, st)
	s.now = func() time.Time { return time.Date(2026, 4, 26, 0, 0, 0, 0, time.UTC) }
	stats, err := s.Sync(ctx, Options{Owner: "openclaw", Repo: "gitcrawl", Numbers: []int{7, 7, 8}, IncludeComments: true})
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if client.listCalled {
		t.Fatal("targeted sync should not call repository issue listing")
	}
	if got, want := client.numbers, []int{7, 8}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("targeted numbers: got %#v want %#v", got, want)
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
}

func TestSyncNormalizesRelativeSince(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	client := &sinceCaptureGitHub{}
	s := New(client, st)
	s.now = func() time.Time { return time.Date(2026, 4, 27, 8, 30, 0, 0, time.UTC) }
	stats, err := s.Sync(ctx, Options{Owner: "openclaw", Repo: "gitcrawl", Since: "15m"})
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	want := "2026-04-27T08:15:00Z"
	if client.since != want || stats.RequestedSince != want {
		t.Fatalf("since: client=%q stats=%q want %q", client.since, stats.RequestedSince, want)
	}
}

func TestSyncRejectsInvalidSince(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	s := New(fakeGitHub{}, st)
	if _, err := s.Sync(ctx, Options{Owner: "openclaw", Repo: "gitcrawl", Since: "yesterday"}); err == nil {
		t.Fatal("expected invalid since to fail")
	}
}

func TestSyncPassesRequestedState(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	client := &stateCaptureGitHub{}
	s := New(client, st)
	if _, err := s.Sync(ctx, Options{Owner: "openclaw", Repo: "gitcrawl", State: "all"}); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if client.state != "all" {
		t.Fatalf("state = %q, want all", client.state)
	}
}

func TestSyncDefaultsToOpenState(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	client := &stateCaptureGitHub{}
	s := New(client, st)
	if _, err := s.Sync(ctx, Options{Owner: "openclaw", Repo: "gitcrawl"}); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if client.state != "open" {
		t.Fatalf("default state = %q, want open", client.state)
	}
}

func TestSyncRejectsInvalidState(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	s := New(fakeGitHub{}, st)
	if _, err := s.Sync(ctx, Options{Owner: "openclaw", Repo: "gitcrawl", State: "merged"}); err == nil {
		t.Fatal("expected invalid state to fail")
	}
}

func TestSyncOpenSinceAppliesClosedOverlapSweep(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repoID, err := st.UpsertRepository(ctx, store.Repository{
		Owner:     "openclaw",
		Name:      "gitcrawl",
		FullName:  "openclaw/gitcrawl",
		RawJSON:   "{}",
		UpdatedAt: "2026-04-26T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("seed repo: %v", err)
	}
	if _, err := st.UpsertThread(ctx, store.Thread{
		RepoID:          repoID,
		GitHubID:        "1",
		Number:          7,
		Kind:            "issue",
		State:           "open",
		Title:           "download stalls",
		Body:            "large file download stalls",
		HTMLURL:         "https://github.com/openclaw/gitcrawl/issues/7",
		LabelsJSON:      "[]",
		AssigneesJSON:   "[]",
		RawJSON:         "{}",
		ContentHash:     "old",
		CreatedAtGitHub: "2026-04-26T00:00:00Z",
		UpdatedAtGitHub: "2026-04-26T00:00:00Z",
		FirstPulledAt:   "2026-04-26T00:00:00Z",
		LastPulledAt:    "2026-04-26T00:00:00Z",
		UpdatedAt:       "2026-04-26T00:00:00Z",
	}); err != nil {
		t.Fatalf("seed thread: %v", err)
	}

	s := New(closedSweepGitHub{}, st)
	s.now = func() time.Time { return time.Date(2026, 4, 27, 1, 0, 0, 0, time.UTC) }
	stats, err := s.Sync(ctx, Options{Owner: "openclaw", Repo: "gitcrawl", Since: "1h"})
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if stats.ThreadsClosed != 1 {
		t.Fatalf("threads closed = %d, want 1", stats.ThreadsClosed)
	}
	threads, err := st.ListThreads(ctx, repoID, true)
	if err != nil {
		t.Fatalf("threads: %v", err)
	}
	if len(threads) != 1 || threads[0].State != "closed" || threads[0].ClosedAtGitHub == "" {
		t.Fatalf("thread not closed from overlap sweep: %#v", threads)
	}
}

func TestMappingHelperBranches(t *testing.T) {
	if got := jsonID("abc"); got != "abc" {
		t.Fatalf("string json id = %q", got)
	}
	if got := jsonID(float64(12)); got != "12" {
		t.Fatalf("float json id = %q", got)
	}
	if got := jsonID(int64(13)); got != "13" {
		t.Fatalf("int64 json id = %q", got)
	}
	if got := jsonID(json.Number("14")); got != "14" {
		t.Fatalf("json number id = %q", got)
	}
	if got := jsonID(struct{}{}); got != "" {
		t.Fatalf("unknown json id = %q", got)
	}
	if got := intValue(float64(22)); got != 22 {
		t.Fatalf("float int value = %d", got)
	}
	if got := intValue(int64(23)); got != 23 {
		t.Fatalf("int64 int value = %d", got)
	}
	if got := intValue(json.Number("24")); got != 24 {
		t.Fatalf("json number int value = %d", got)
	}
	if got := intValue("bad"); got != 0 {
		t.Fatalf("bad int value = %d", got)
	}
	if got := stringValue(time.Unix(0, 0).UTC()); got == "" {
		t.Fatal("Stringer value should render")
	}
	if loginFromUser("not-user") != "" || typeFromUser("not-user") != "" {
		t.Fatal("non-map user should return empty fields")
	}
	comment := mapComment(77, "review", map[string]any{
		"id":         json.Number("88"),
		"body":       time.Unix(0, 0).UTC(),
		"created_at": "2026-04-30T00:00:00Z",
		"updated_at": "2026-04-30T00:01:00Z",
		"user":       map[string]any{"login": "dependabot[bot]", "type": "User"},
	})
	if comment.GitHubID != "88" || !comment.IsBot || comment.Body == "" {
		t.Fatalf("comment = %+v", comment)
	}
}
