package syncer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"path/filepath"
	"strings"
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

func (fakeGitHub) GetPull(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) (map[string]any, error) {
	return map[string]any{
		"number":          number,
		"head":            map[string]any{"sha": "head-sha", "ref": "feature", "repo": map[string]any{"full_name": "openclaw/gitcrawl"}},
		"base":            map[string]any{"sha": "base-sha"},
		"mergeable_state": "clean",
		"additions":       12,
		"deletions":       3,
		"changed_files":   2,
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

func (fakeGitHub) ListPullReviewThreads(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) ([]map[string]any, error) {
	return nil, nil
}

func (fakeGitHub) ListPullFiles(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) ([]map[string]any, error) {
	return nil, nil
}

func (fakeGitHub) ListPullCommits(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) ([]map[string]any, error) {
	return nil, nil
}

func (fakeGitHub) ListCommitCheckRuns(ctx context.Context, owner, repo, ref string, reporter gh.Reporter) ([]map[string]any, error) {
	return nil, nil
}

func (fakeGitHub) ListWorkflowRuns(ctx context.Context, owner, repo string, options gh.ListWorkflowRunsOptions, reporter gh.Reporter) ([]map[string]any, error) {
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
		"body":       "",
		"state":      "APPROVED",
		"commit_id":  "head-sha",
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

func (pullCommentGitHub) ListPullReviewThreads(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) ([]map[string]any, error) {
	if number != 8 {
		return nil, nil
	}
	return []map[string]any{{
		"id":                 "PRRT_8",
		"path":               "internal/cache.go",
		"line":               42,
		"isResolved":         false,
		"isOutdated":         false,
		"viewerCanResolve":   true,
		"viewerCanUnresolve": false,
		"viewerCanReply":     true,
		"comments": map[string]any{"nodes": []any{map[string]any{
			"id":         "PRRC_82",
			"databaseId": 82,
			"body":       "line comment",
			"author":     map[string]any{"login": "alice", "__typename": "Bot"},
			"path":       "internal/cache.go",
			"diffHunk":   "@@ cache",
			"createdAt":  "2026-04-26T00:02:00Z",
			"updatedAt":  "2026-04-26T00:03:00Z",
			"url":        "https://github.com/openclaw/gitcrawl/pull/8#discussion_r82",
		}}},
	}}, nil
}

type pullDetailsGitHub struct {
	fakeGitHub
}

type emptyHeadPullGitHub struct {
	fakeGitHub
	checksCalled bool
	runsCalled   bool
}

type failingReviewThreadsGitHub struct {
	pullDetailsGitHub
}

type txProbePullDetailsGitHub struct {
	fakeGitHub
	st                    *store.Store
	sawPersistedThread    bool
	sawPersistedThreadErr error
}

func (g *txProbePullDetailsGitHub) ListPullFiles(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) ([]map[string]any, error) {
	storedRepo, err := g.st.RepositoryByFullName(ctx, owner+"/"+repo)
	if err != nil {
		g.sawPersistedThreadErr = err
		return nil, nil
	}
	threads, err := g.st.ListThreads(ctx, storedRepo.ID, true)
	if err != nil {
		g.sawPersistedThreadErr = err
		return nil, nil
	}
	for _, thread := range threads {
		if thread.Number == number && thread.Kind == "pull_request" {
			g.sawPersistedThread = true
		}
	}
	return nil, nil
}

func (emptyHeadPullGitHub) GetPull(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) (map[string]any, error) {
	return map[string]any{
		"number":          number,
		"head":            map[string]any{"ref": "feature", "repo": map[string]any{"full_name": "openclaw/gitcrawl"}},
		"base":            map[string]any{"sha": "base-sha"},
		"mergeable_state": "unknown",
	}, nil
}

func (g *emptyHeadPullGitHub) ListCommitCheckRuns(ctx context.Context, owner, repo, ref string, reporter gh.Reporter) ([]map[string]any, error) {
	g.checksCalled = true
	return nil, nil
}

func (g *emptyHeadPullGitHub) ListWorkflowRuns(ctx context.Context, owner, repo string, options gh.ListWorkflowRunsOptions, reporter gh.Reporter) ([]map[string]any, error) {
	g.runsCalled = true
	return nil, nil
}

func (pullDetailsGitHub) ListPullReviewThreads(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) ([]map[string]any, error) {
	return pullCommentGitHub{}.ListPullReviewThreads(ctx, owner, repo, number, reporter)
}

func (failingReviewThreadsGitHub) ListPullReviewThreads(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) ([]map[string]any, error) {
	return nil, errors.New("graphql unavailable")
}

func (pullDetailsGitHub) ListPullFiles(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) ([]map[string]any, error) {
	return []map[string]any{{
		"filename":  "internal/cache.go",
		"status":    "modified",
		"additions": 10,
		"deletions": 2,
		"changes":   12,
		"patch":     "@@ cache",
	}}, nil
}

func (pullDetailsGitHub) ListPullCommits(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) ([]map[string]any, error) {
	return []map[string]any{{
		"sha":      "commit-sha",
		"html_url": "https://github.com/openclaw/gitcrawl/commit/commit-sha",
		"author":   map[string]any{"login": "alice"},
		"commit": map[string]any{
			"message": "feat: cache",
			"author":  map[string]any{"name": "Alice", "date": "2026-04-26T00:00:00Z"},
		},
	}}, nil
}

func (pullDetailsGitHub) ListCommitCheckRuns(ctx context.Context, owner, repo, ref string, reporter gh.Reporter) ([]map[string]any, error) {
	return []map[string]any{{
		"name":        "test",
		"status":      "completed",
		"conclusion":  "success",
		"details_url": "https://github.com/openclaw/gitcrawl/actions/runs/99",
		"check_suite": map[string]any{"app": map[string]any{"name": "GitHub Actions"}},
	}}, nil
}

func (pullDetailsGitHub) ListWorkflowRuns(ctx context.Context, owner, repo string, options gh.ListWorkflowRunsOptions, reporter gh.Reporter) ([]map[string]any, error) {
	return []map[string]any{{
		"id":          99,
		"run_number":  7,
		"head_branch": "feature",
		"head_sha":    options.HeadSHA,
		"status":      "completed",
		"conclusion":  "success",
		"name":        "CI",
		"event":       "pull_request",
		"html_url":    "https://github.com/openclaw/gitcrawl/actions/runs/99",
		"created_at":  "2026-04-26T00:00:00Z",
		"updated_at":  "2026-04-26T00:01:00Z",
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
	var progressLogs bytes.Buffer
	stats, err := s.Sync(ctx, Options{
		Owner:           "openclaw",
		Repo:            "gitcrawl",
		IncludeComments: true,
		Logger:          testProgressLogger(&progressLogs),
	})
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
	for _, want := range []string{
		`msg="sync progress"`,
		`state=finished`,
		`unit=threads`,
		`percent=100.0`,
		`completion=100.0%`,
		`repository=openclaw/gitcrawl`,
	} {
		if !strings.Contains(progressLogs.String(), want) {
			t.Fatalf("missing %q in progress logs:\n%s", want, progressLogs.String())
		}
	}
}

func TestMetadataOnlySyncPreservesCommentBackedDocumentText(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	s := New(fakeGitHub{}, st)
	s.now = func() time.Time { return time.Date(2026, 4, 26, 0, 0, 0, 0, time.UTC) }
	if _, err := s.Sync(ctx, Options{Owner: "openclaw", Repo: "gitcrawl", IncludeComments: true}); err != nil {
		t.Fatalf("sync with comments: %v", err)
	}
	assertDocumentFTSCount(t, st, "same", 1)
	stats, err := s.Sync(ctx, Options{Owner: "openclaw", Repo: "gitcrawl"})
	if err != nil {
		t.Fatalf("metadata sync: %v", err)
	}
	if !stats.MetadataOnly || stats.CommentsSynced != 0 {
		t.Fatalf("metadata stats = %#v", stats)
	}
	assertDocumentFTSCount(t, st, "same", 1)
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
	comments, err := st.ListComments(ctx, threads[0].ID)
	if err != nil {
		t.Fatalf("comments: %v", err)
	}
	var foundBodylessReview bool
	for _, comment := range comments {
		if comment.CommentType == "pull_review" && comment.GitHubID == "81" && comment.Body == "" {
			foundBodylessReview = true
		}
	}
	if !foundBodylessReview {
		t.Fatalf("bodyless pull review was not persisted: %+v", comments)
	}
}

func TestSyncHydratesPullRequestDetails(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	s := New(pullDetailsGitHub{}, st)
	s.now = func() time.Time { return time.Date(2026, 4, 26, 0, 0, 0, 0, time.UTC) }
	stats, err := s.Sync(ctx, Options{Owner: "openclaw", Repo: "gitcrawl", Numbers: []int{8}, IncludePRDetails: true})
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if stats.ReviewThreadsSynced != 1 || stats.PRDetailsSynced != 1 || stats.PRFilesSynced != 1 || stats.PRCommitsSynced != 1 || stats.PRChecksSynced != 1 || stats.WorkflowRunsSynced != 1 {
		t.Fatalf("stats = %#v", stats)
	}
	repo, err := st.RepositoryByFullName(ctx, "openclaw/gitcrawl")
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	cache, err := st.PullRequestCache(ctx, repo.ID, 8)
	if err != nil {
		t.Fatalf("pr cache: %v", err)
	}
	if cache.Detail.HeadSHA != "head-sha" || len(cache.Files) != 1 || len(cache.Commits) != 1 || len(cache.Checks) != 1 {
		t.Fatalf("cache = %+v", cache)
	}
	runs, err := st.ListWorkflowRuns(ctx, repo.ID, store.WorkflowRunListOptions{HeadSHA: "head-sha", Limit: 10})
	if err != nil {
		t.Fatalf("workflow runs: %v", err)
	}
	if len(runs) != 1 || runs[0].RunID != "99" {
		t.Fatalf("runs = %+v", runs)
	}
	threads, err := st.ListThreads(ctx, repo.ID, true)
	if err != nil {
		t.Fatalf("threads: %v", err)
	}
	if len(threads) != 1 {
		t.Fatalf("threads = %+v", threads)
	}
	reviewThreads, err := st.PullRequestReviewThreads(ctx, threads[0].ID)
	if err != nil {
		t.Fatalf("review threads: %v", err)
	}
	if len(reviewThreads) != 1 || reviewThreads[0].ReviewThreadID != "PRRT_8" {
		t.Fatalf("review threads = %+v", reviewThreads)
	}
	fetchedAt, err := st.PullRequestReviewThreadsFetchedAt(ctx, threads[0].ID)
	if err != nil {
		t.Fatalf("review thread marker: %v", err)
	}
	if fetchedAt == "" {
		t.Fatal("missing review thread sync marker")
	}
}

func TestSyncPullRequestDetailsFailsOnReviewThreadFetchError(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	s := New(failingReviewThreadsGitHub{}, st)
	s.now = func() time.Time { return time.Date(2026, 4, 26, 0, 0, 0, 0, time.UTC) }
	_, err = s.Sync(ctx, Options{Owner: "openclaw", Repo: "gitcrawl", Numbers: []int{8}, IncludePRDetails: true})
	if err == nil || !strings.Contains(err.Error(), "list pull request review threads for #8") {
		t.Fatalf("sync error = %v", err)
	}
	repo, err := st.RepositoryByFullName(ctx, "openclaw/gitcrawl")
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	threads, err := st.ListThreads(ctx, repo.ID, true)
	if err != nil {
		t.Fatalf("threads: %v", err)
	}
	if len(threads) != 1 {
		t.Fatalf("threads = %+v", threads)
	}
	fetchedAt, err := st.PullRequestReviewThreadsFetchedAt(ctx, threads[0].ID)
	if err != nil {
		t.Fatalf("review thread marker: %v", err)
	}
	if fetchedAt != "" {
		t.Fatalf("review thread marker should stay empty after failed fetch, got %q", fetchedAt)
	}
}

func TestSyncPullRequestDetailsSkipsCheckAndWorkflowFetchWithoutHeadSHA(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	client := &emptyHeadPullGitHub{}
	s := New(client, st)
	s.now = func() time.Time { return time.Date(2026, 4, 26, 0, 0, 0, 0, time.UTC) }
	stats, err := s.Sync(ctx, Options{Owner: "openclaw", Repo: "gitcrawl", Numbers: []int{8}, IncludePRDetails: true})
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if stats.PRDetailsSynced != 1 || stats.PRChecksSynced != 0 || stats.WorkflowRunsSynced != 0 {
		t.Fatalf("stats = %#v", stats)
	}
	if client.checksCalled {
		t.Fatal("check runs should not be fetched without head SHA")
	}
	if client.runsCalled {
		t.Fatal("workflow runs should not be fetched without head SHA")
	}
}

func TestSyncPullRequestDetailsDoesNotFetchInsideTransaction(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	client := &txProbePullDetailsGitHub{st: st}
	s := New(client, st)
	s.now = func() time.Time { return time.Date(2026, 4, 26, 0, 0, 0, 0, time.UTC) }
	if _, err := s.Sync(ctx, Options{Owner: "openclaw", Repo: "gitcrawl", Numbers: []int{8}, IncludePRDetails: true}); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if client.sawPersistedThreadErr != nil {
		t.Fatalf("probe persisted thread: %v", client.sawPersistedThreadErr)
	}
	if !client.sawPersistedThread {
		t.Fatal("PR detail fetch ran before the PR thread was committed")
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

func TestExpectedIssueTotal(t *testing.T) {
	cases := []struct {
		name  string
		repo  map[string]any
		state string
		since string
		limit int
		want  int
	}{
		{name: "open no filters", repo: map[string]any{"open_issues_count": float64(666)}, state: "open", want: 666},
		{name: "open with limit below count", repo: map[string]any{"open_issues_count": float64(666)}, state: "open", limit: 100, want: 100},
		{name: "open with limit above count", repo: map[string]any{"open_issues_count": float64(50)}, state: "open", limit: 200, want: 50},
		{name: "open with since", repo: map[string]any{"open_issues_count": float64(666)}, state: "open", since: "2026-04-26T00:00:00Z", want: 0},
		{name: "closed state", repo: map[string]any{"open_issues_count": float64(666)}, state: "closed", want: 0},
		{name: "all state", repo: map[string]any{"open_issues_count": float64(666)}, state: "all", want: 0},
		{name: "missing count", repo: map[string]any{}, state: "open", want: 0},
		{name: "zero count", repo: map[string]any{"open_issues_count": float64(0)}, state: "open", want: 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := expectedIssueTotal(tc.repo, tc.state, tc.since, tc.limit); got != tc.want {
				t.Fatalf("expectedIssueTotal = %d, want %d", got, tc.want)
			}
		})
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

func TestMappingFallbackBranches(t *testing.T) {
	now := time.Date(2026, 5, 5, 12, 0, 0, 123, time.UTC)
	normalized, err := normalizeSince("2026-05-05T12:00:00+02:00", now)
	if err != nil {
		t.Fatalf("normalize iso since: %v", err)
	}
	if normalized != "2026-05-05T10:00:00Z" {
		t.Fatalf("normalized iso since = %q", normalized)
	}
	if got, err := normalizeSince("2w", now); err != nil || got != "2026-04-21T12:00:00.000000123Z" {
		t.Fatalf("normalize weeks = %q, %v", got, err)
	}
	if got := mustJSON(map[string]any{"bad": make(chan int)}); got != "{}" {
		t.Fatalf("mustJSON marshal fallback = %q", got)
	}

	thread := mapIssueToThread(99, map[string]any{
		"id":         int64(123),
		"number":     456,
		"state":      "closed",
		"title":      "fallbacks",
		"body":       "body",
		"html_url":   "https://github.com/openclaw/gitcrawl/issues/456",
		"labels":     nil,
		"assignees":  nil,
		"created_at": "2026-05-05T10:00:00Z",
		"updated_at": "2026-05-05T11:00:00Z",
		"closed_at":  "2026-05-05T12:00:00Z",
	}, "2026-05-05T12:00:00Z")
	if thread.LabelsJSON != "[]" || thread.AssigneesJSON != "[]" {
		t.Fatalf("nullable label defaults: labels=%s assignees=%s", thread.LabelsJSON, thread.AssigneesJSON)
	}
	if thread.GitHubID != "123" || thread.Number != 456 || thread.AuthorLogin != "" || thread.ClosedAtGitHub == "" {
		t.Fatalf("thread = %+v", thread)
	}
}

func testProgressLogger(out *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewTextHandler(out, &slog.HandlerOptions{
		ReplaceAttr: func(_ []string, attr slog.Attr) slog.Attr {
			if attr.Key == slog.TimeKey {
				return slog.Attr{}
			}
			return attr
		},
	}))
}

func assertDocumentFTSCount(t *testing.T, st *store.Store, query string, want int) {
	t.Helper()
	var got int
	if err := st.DB().QueryRowContext(context.Background(), `select count(*) from documents_fts where documents_fts match ?`, query).Scan(&got); err != nil {
		t.Fatalf("query document index: %v", err)
	}
	if got != want {
		t.Fatalf("document FTS count for %q: got %d want %d", query, got, want)
	}
}
