package store

import (
	"context"
	"path/filepath"
	"testing"
)

func TestPullRequestCacheRoundTripAndWorkflowFilters(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	repoID, threadIDs := seedVectorThreads(t, ctx, st)
	threadID := threadIDs[1]
	fetchedAt := "2026-05-05T10:00:00Z"

	detail := PullRequestDetail{
		ThreadID: threadID, RepoID: repoID, Number: 302,
		BaseSHA: "base", HeadSHA: "head", HeadRef: "feature/cache", HeadRepoFullName: "openclaw/gitcrawl-fork",
		MergeableState: "clean", Additions: 12, Deletions: 3, ChangedFiles: 2,
		RawJSON: "{}", FetchedAt: fetchedAt, UpdatedAt: "2026-05-05T09:59:00Z",
	}
	files := []PullRequestFile{
		{Path: "z.go", Status: "modified", Additions: 2, Deletions: 1, Changes: 3, Patch: "@@", RawJSON: "{}", FetchedAt: fetchedAt},
		{Path: "a.go", Status: "renamed", Additions: 10, Changes: 10, PreviousPath: "old.go", RawJSON: "{}", FetchedAt: fetchedAt},
	}
	commits := []PullRequestCommit{
		{SHA: "abc", Message: "feat: cache", AuthorLogin: "alice", AuthorName: "Alice", CommittedAt: "2026-05-05T08:00:00Z", HTMLURL: "https://example.invalid/commit/abc", RawJSON: "{}", FetchedAt: fetchedAt},
	}
	checks := []PullRequestCheck{
		{Name: "z-check", Status: "completed", Conclusion: "success", DetailsURL: "https://example.invalid/z", WorkflowName: "CI", StartedAt: "2026-05-05T09:00:00Z", CompletedAt: "2026-05-05T09:05:00Z", RawJSON: "{}", FetchedAt: fetchedAt},
		{Name: "a-check", Status: "queued", RawJSON: "{}", FetchedAt: fetchedAt},
	}
	runs := []WorkflowRun{
		{RepoID: repoID, RunID: "100", RunNumber: 7, HeadBranch: "main", HeadSHA: "head", Status: "completed", Conclusion: "success", WorkflowName: "CI", Event: "push", HTMLURL: "https://example.invalid/run/100", CreatedAtGH: "2026-05-05T09:00:00Z", UpdatedAtGH: "2026-05-05T09:05:00Z", RawJSON: "{}", FetchedAt: fetchedAt},
		{RepoID: repoID, RunID: "101", RunNumber: 8, HeadBranch: "release", HeadSHA: "other", Status: "in_progress", WorkflowName: "release", Event: "workflow_dispatch", CreatedAtGH: "2026-05-05T09:10:00Z", UpdatedAtGH: "2026-05-05T09:11:00Z", RawJSON: "{}", FetchedAt: fetchedAt},
	}
	if err := st.UpsertPullRequestCache(ctx, detail, files, commits, checks, runs); err != nil {
		t.Fatalf("upsert pr cache: %v", err)
	}
	cache, err := st.PullRequestCache(ctx, repoID, 302)
	if err != nil {
		t.Fatalf("pull request cache: %v", err)
	}
	if cache.Detail.HeadSHA != "head" || cache.Detail.MergeableState != "clean" {
		t.Fatalf("detail = %+v", cache.Detail)
	}
	if len(cache.Files) != 2 || cache.Files[0].Path != "a.go" || cache.Files[0].PreviousPath != "old.go" {
		t.Fatalf("files = %+v", cache.Files)
	}
	if len(cache.Commits) != 1 || cache.Commits[0].SHA != "abc" || cache.Commits[0].AuthorName != "Alice" {
		t.Fatalf("commits = %+v", cache.Commits)
	}
	if len(cache.Checks) != 2 || cache.Checks[0].Name != "a-check" || cache.Checks[1].Conclusion != "success" {
		t.Fatalf("checks = %+v", cache.Checks)
	}
	apiChecks, err := st.PullRequestChecksAPIOrder(ctx, threadID)
	if err != nil {
		t.Fatalf("api-order checks: %v", err)
	}
	if len(apiChecks) != 2 || apiChecks[0].Name != "z-check" || apiChecks[1].Name != "a-check" {
		t.Fatalf("api-order checks = %+v", apiChecks)
	}

	mainRuns, err := st.ListWorkflowRuns(ctx, repoID, WorkflowRunListOptions{Branch: "main", HeadSHA: "head", Limit: 5})
	if err != nil {
		t.Fatalf("list filtered runs: %v", err)
	}
	if len(mainRuns) != 1 || mainRuns[0].RunID != "100" || mainRuns[0].HTMLURL == "" {
		t.Fatalf("main runs = %+v", mainRuns)
	}
	allRuns, err := st.ListWorkflowRuns(ctx, repoID, WorkflowRunListOptions{})
	if err != nil {
		t.Fatalf("list default runs: %v", err)
	}
	if len(allRuns) != 2 || allRuns[0].RunID != "101" {
		t.Fatalf("all runs = %+v", allRuns)
	}

	detail.HeadSHA = "head-v2"
	if err := st.UpsertPullRequestCache(ctx, detail, files[:1], nil, nil, []WorkflowRun{{RepoID: repoID, RunID: "100", RunNumber: 9, HeadBranch: "main", HeadSHA: "head-v2", Status: "completed", Conclusion: "failure", UpdatedAtGH: "2026-05-05T10:00:00Z", RawJSON: "{}", FetchedAt: fetchedAt}}); err != nil {
		t.Fatalf("update pr cache: %v", err)
	}
	cache, err = st.PullRequestCache(ctx, repoID, 302)
	if err != nil {
		t.Fatalf("updated pull request cache: %v", err)
	}
	if cache.Detail.HeadSHA != "head-v2" || len(cache.Files) != 1 || len(cache.Commits) != 0 || len(cache.Checks) != 0 {
		t.Fatalf("updated cache = %+v", cache)
	}
}

func TestPullRequestCacheAndDocumentRollsBackTogether(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	repoID, threadIDs := seedVectorThreads(t, ctx, st)
	threadID := threadIDs[1]
	err = st.UpsertPullRequestCacheAndDocument(ctx,
		PullRequestDetail{
			ThreadID: threadID, RepoID: repoID, Number: 302,
			RawJSON: "{}", FetchedAt: "2026-05-05T10:00:00Z", UpdatedAt: "2026-05-05T10:00:00Z",
		},
		[]PullRequestFile{{ThreadID: threadID, Path: "cache.go", RawJSON: "{}", FetchedAt: "2026-05-05T10:00:00Z"}},
		nil, nil, nil,
		Document{
			ThreadID: threadID + 999, Title: "invalid", RawText: "invalid",
			DedupeText: "invalid", UpdatedAt: "2026-05-05T10:00:00Z",
		},
	)
	if err == nil {
		t.Fatal("invalid document unexpectedly succeeded")
	}
	for _, table := range []string{"pull_request_details", "pull_request_files"} {
		var count int
		if err := st.DB().QueryRowContext(ctx, `select count(*) from `+table+` where thread_id = ?`, threadID).Scan(&count); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		if count != 0 {
			t.Fatalf("%s rows after rollback = %d", table, count)
		}
	}
}
