package store

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"
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

func TestMissingPullRequestDetailNumbersSelectsOnlyUnfilledPRs(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	now := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)
	repoID, err := st.UpsertRepository(ctx, Repository{Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl", RawJSON: "{}", UpdatedAt: now})
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	threads := []Thread{
		{RepoID: repoID, GitHubID: "11", Number: 11, Kind: "pull_request", State: "closed", Title: "old missing", HTMLURL: "https://github.com/openclaw/gitcrawl/pull/11", LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "h11", UpdatedAtGitHub: "2026-06-01T00:00:00Z", UpdatedAt: now},
		{RepoID: repoID, GitHubID: "12", Number: 12, Kind: "pull_request", State: "open", Title: "new missing", HTMLURL: "https://github.com/openclaw/gitcrawl/pull/12", LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "h12", UpdatedAtGitHub: "2026-06-03T00:00:00Z", UpdatedAt: now},
		{RepoID: repoID, GitHubID: "13", Number: 13, Kind: "pull_request", State: "open", Title: "already filled", HTMLURL: "https://github.com/openclaw/gitcrawl/pull/13", LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "h13", UpdatedAtGitHub: "2026-06-04T00:00:00Z", UpdatedAt: now},
		{RepoID: repoID, GitHubID: "14", Number: 14, Kind: "issue", State: "open", Title: "plain issue", HTMLURL: "https://github.com/openclaw/gitcrawl/issues/14", LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "h14", UpdatedAtGitHub: "2026-06-05T00:00:00Z", UpdatedAt: now},
	}
	var filledThreadID int64
	for _, thread := range threads {
		id, err := st.UpsertThread(ctx, thread)
		if err != nil {
			t.Fatalf("thread %d: %v", thread.Number, err)
		}
		if thread.Number == 13 {
			filledThreadID = id
		}
	}
	if err := st.UpsertPullRequestCache(ctx, PullRequestDetail{ThreadID: filledThreadID, RepoID: repoID, Number: 13, RawJSON: "{}", FetchedAt: now, UpdatedAt: now}, nil, nil, nil, nil); err != nil {
		t.Fatalf("fill pr detail: %v", err)
	}

	got, err := st.MissingPullRequestDetailNumbers(ctx, repoID, MissingPullRequestDetailOptions{Order: "newest-first"})
	if err != nil {
		t.Fatalf("missing newest: %v", err)
	}
	if len(got) != 2 || got[0] != 12 || got[1] != 11 {
		t.Fatalf("newest missing = %v", got)
	}
	got, err = st.MissingPullRequestDetailNumbers(ctx, repoID, MissingPullRequestDetailOptions{Order: "oldest-first", Limit: 1})
	if err != nil {
		t.Fatalf("missing oldest: %v", err)
	}
	if len(got) != 1 || got[0] != 11 {
		t.Fatalf("oldest limited missing = %v", got)
	}
	got, err = st.MissingPullRequestDetailNumbers(ctx, repoID, MissingPullRequestDetailOptions{Order: "open-first"})
	if err != nil {
		t.Fatalf("missing open-first: %v", err)
	}
	if len(got) != 2 || got[0] != 12 || got[1] != 11 {
		t.Fatalf("open-first missing = %v", got)
	}
}

func TestPullRequestCacheAllowsDuplicateFilePaths(t *testing.T) {
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
		ThreadID:  threadID,
		RepoID:    repoID,
		Number:    302,
		RawJSON:   "{}",
		FetchedAt: fetchedAt,
		UpdatedAt: fetchedAt,
	}
	files := []PullRequestFile{
		// Regression for GitHub PR file lists that contain separate removed and
		// added entries with the same filename, as seen in jj-vcs/jj#9355.
		{
			Path:      "docs/governance/GOVERNANCE.md",
			Status:    "removed",
			Deletions: 1,
			Changes:   1,
			RawJSON:   `{"filename":"docs/governance/GOVERNANCE.md","status":"removed"}`,
			FetchedAt: fetchedAt,
		},
		{
			Path:      "docs/governance/GOVERNANCE.md",
			Status:    "added",
			Additions: 161,
			Changes:   161,
			RawJSON:   `{"filename":"docs/governance/GOVERNANCE.md","status":"added"}`,
			FetchedAt: fetchedAt,
		},
	}

	if err := st.UpsertPullRequestCache(ctx, detail, files, nil, nil, nil); err != nil {
		t.Fatalf("upsert duplicate-path pr files: %v", err)
	}
	cache, err := st.PullRequestCache(ctx, repoID, 302)
	if err != nil {
		t.Fatalf("pull request cache: %v", err)
	}
	if len(cache.Files) != 2 {
		t.Fatalf("files len = %d, want 2: %+v", len(cache.Files), cache.Files)
	}
	if cache.Files[0].Status != "removed" || cache.Files[0].Position != 0 ||
		cache.Files[1].Status != "added" || cache.Files[1].Position != 1 {
		t.Fatalf("files = %+v", cache.Files)
	}

	if err := st.UpsertPullRequestCache(ctx, detail, files[:1], nil, nil, nil); err != nil {
		t.Fatalf("replace duplicate-path pr files: %v", err)
	}
	cache, err = st.PullRequestCache(ctx, repoID, 302)
	if err != nil {
		t.Fatalf("updated pull request cache: %v", err)
	}
	if len(cache.Files) != 1 || cache.Files[0].Status != "removed" || cache.Files[0].Position != 0 {
		t.Fatalf("updated files = %+v", cache.Files)
	}
}

func TestPullRequestCacheReplacesCompleteWorkflowRunSnapshot(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	repoID, threadIDs := seedVectorThreads(t, ctx, st)
	threadID := threadIDs[1]
	detail := PullRequestDetail{
		ThreadID: threadID, RepoID: repoID, Number: 302, HeadSHA: "head",
		RawJSON: "{}", FetchedAt: "2026-07-12T03:00:00Z", UpdatedAt: "2026-07-12T03:00:00Z",
	}
	initial := make([]WorkflowRun, 25)
	for index := range initial {
		initial[index] = WorkflowRun{
			RepoID: repoID, RunID: fmt.Sprintf("%d", index+1), RunNumber: index + 1, HeadSHA: "head",
			RawJSON: "{}", FetchedAt: "2026-07-12T03:00:00Z",
		}
	}
	if err := st.UpsertPullRequestCache(ctx, detail, nil, nil, nil, initial); err != nil {
		t.Fatalf("seed workflow runs: %v", err)
	}
	all, err := st.ListWorkflowRuns(ctx, repoID, WorkflowRunListOptions{HeadSHA: "head", Limit: -1})
	if err != nil {
		t.Fatalf("list complete workflow runs: %v", err)
	}
	if len(all) != 25 {
		t.Fatalf("complete workflow runs = %d, want 25", len(all))
	}
	state, err := st.ReadWorkflowRunSnapshotState(ctx, repoID, detail.HeadSHA)
	if err != nil {
		t.Fatalf("read normalized legacy workflow snapshot: %v", err)
	}
	if !state.ReservationFound ||
		state.SourceUpdatedAt != detail.FetchedAt ||
		len(state.Runs) != 25 ||
		state.Runs[0].UpdatedAtGH != detail.FetchedAt {
		t.Fatalf("normalized legacy workflow snapshot = %+v", state)
	}
	replacement := initial[:3]
	if err := st.UpsertPullRequestCache(ctx, detail, nil, nil, nil, replacement); err != nil {
		t.Fatalf("replace workflow runs: %v", err)
	}
	all, err = st.ListWorkflowRuns(ctx, repoID, WorkflowRunListOptions{HeadSHA: "head", Limit: -1})
	if err != nil {
		t.Fatalf("list replacement workflow runs: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("replacement workflow runs = %+v", all)
	}
}

func TestPullRequestCacheWorkflowWritesHonorOrderedSnapshotAcrossStores(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "gitcrawl.db")
	firstStore, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open first store: %v", err)
	}
	t.Cleanup(func() { _ = firstStore.Close() })
	repoID, threadIDs := seedVectorThreads(t, ctx, firstStore)
	detail := PullRequestDetail{
		ThreadID: threadIDs[1], RepoID: repoID, Number: 302, HeadSHA: "head",
		RawJSON: "{}", FetchedAt: "2026-07-12T03:00:00Z", UpdatedAt: "2026-07-12T03:00:00Z",
	}
	initial := []WorkflowRun{
		{
			RepoID: repoID, RunID: "100", RunNumber: 1, HeadSHA: "head",
			UpdatedAtGH: "2026-07-12T03:01:00Z", RawJSON: "{}", FetchedAt: detail.FetchedAt,
		},
		{
			RepoID: repoID, RunID: "101", RunNumber: 2, HeadSHA: "head",
			UpdatedAtGH: "2026-07-12T03:02:00Z", RawJSON: "{}", FetchedAt: detail.FetchedAt,
		},
	}
	if err := firstStore.UpsertPullRequestCache(ctx, detail, nil, nil, nil, initial); err != nil {
		t.Fatalf("seed legacy workflow cache: %v", err)
	}
	baseline, err := firstStore.ReadWorkflowRunSnapshotState(ctx, repoID, detail.HeadSHA)
	if err != nil {
		t.Fatalf("read legacy workflow snapshot: %v", err)
	}
	if !baseline.ReservationFound || len(baseline.Runs) != 2 {
		t.Fatalf("legacy workflow snapshot = %+v, want ordered two-run snapshot", baseline)
	}

	deletionSequence, err := firstStore.NextThreadObservationSequence(
		ctx,
		"2026-07-12T03:03:00Z",
	)
	if err != nil {
		t.Fatalf("allocate deletion observation: %v", err)
	}
	deleted, err := firstStore.ApplyWorkflowRunSnapshot(
		ctx,
		repoID,
		detail.HeadSHA,
		"2026-07-12T03:03:00Z",
		deletionSequence,
		baseline,
		initial[:1],
	)
	if err != nil {
		t.Fatalf("apply ordered deletion: %v", err)
	}
	if !deleted.Applied || deleted.RowsSynced != 1 {
		t.Fatalf("ordered deletion = %+v", deleted)
	}

	secondStore, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open second store: %v", err)
	}
	t.Cleanup(func() { _ = secondStore.Close() })
	if err := secondStore.UpsertPullRequestCacheFamilies(
		ctx,
		detail,
		nil,
		nil,
		nil,
		initial,
		PullRequestHydrationFamilies{WorkflowRuns: true},
	); err != nil {
		t.Fatalf("replay stale legacy workflow cache: %v", err)
	}

	if err := firstStore.Close(); err != nil {
		t.Fatalf("close first store: %v", err)
	}
	if err := secondStore.Close(); err != nil {
		t.Fatalf("close second store: %v", err)
	}
	reopened, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer reopened.Close()
	state, err := reopened.ReadWorkflowRunSnapshotState(ctx, repoID, detail.HeadSHA)
	if err != nil {
		t.Fatalf("read reopened workflow snapshot: %v", err)
	}
	if state.SourceUpdatedAt != "2026-07-12T03:03:00Z" ||
		state.ObservationSequence != deletionSequence ||
		len(state.Runs) != 1 ||
		state.Runs[0].RunID != "100" {
		t.Fatalf("stale legacy write resurrected deleted workflow run: %+v", state)
	}
}

func TestPullRequestCacheRejectsStaleEmptyWorkflowSnapshotAcrossStores(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "gitcrawl.db")
	firstStore, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open first store: %v", err)
	}
	t.Cleanup(func() { _ = firstStore.Close() })
	repoID, threadIDs := seedVectorThreads(t, ctx, firstStore)
	detail := PullRequestDetail{
		ThreadID: threadIDs[1], RepoID: repoID, Number: 302, HeadSHA: "head",
		RawJSON: "{}", FetchedAt: "2026-07-12T03:00:00Z", UpdatedAt: "2026-07-12T03:00:00Z",
	}
	initial := []WorkflowRun{{
		RepoID: repoID, RunID: "100", RunNumber: 1, HeadSHA: "head",
		UpdatedAtGH: "2026-07-12T03:00:00Z", RawJSON: "{}", FetchedAt: detail.FetchedAt,
	}}
	if err := firstStore.UpsertPullRequestCache(ctx, detail, nil, nil, nil, initial); err != nil {
		t.Fatalf("seed newer workflow cache: %v", err)
	}

	secondStore, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open second store: %v", err)
	}
	t.Cleanup(func() { _ = secondStore.Close() })
	staleDetail := detail
	staleDetail.FetchedAt = "2026-07-12T02:00:00Z"
	staleDetail.UpdatedAt = "2026-07-12T02:00:00Z"
	if err := secondStore.UpsertPullRequestCacheFamilies(
		ctx,
		staleDetail,
		nil,
		nil,
		nil,
		nil,
		PullRequestHydrationFamilies{WorkflowRuns: true},
	); err != nil {
		t.Fatalf("apply stale empty workflow cache: %v", err)
	}

	if err := firstStore.Close(); err != nil {
		t.Fatalf("close first store: %v", err)
	}
	if err := secondStore.Close(); err != nil {
		t.Fatalf("close second store: %v", err)
	}
	reopened, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer reopened.Close()
	state, err := reopened.ReadWorkflowRunSnapshotState(ctx, repoID, detail.HeadSHA)
	if err != nil {
		t.Fatalf("read reopened workflow snapshot: %v", err)
	}
	if state.SourceUpdatedAt != "2026-07-12T03:00:00Z" ||
		len(state.Runs) != 1 ||
		state.Runs[0].RunID != "100" {
		t.Fatalf("stale empty workflow cache deleted newer state: %+v", state)
	}
}

func TestPullRequestCacheRejectsClocklessEmptyWorkflowSnapshot(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	repoID, threadIDs := seedVectorThreads(t, ctx, st)
	err = st.UpsertPullRequestCacheFamilies(
		ctx,
		PullRequestDetail{
			ThreadID: threadIDs[1],
			RepoID:   repoID,
			Number:   302,
			HeadSHA:  "head",
			RawJSON:  "{}",
		},
		nil,
		nil,
		nil,
		nil,
		PullRequestHydrationFamilies{WorkflowRuns: true},
	)
	if err == nil || !strings.Contains(err.Error(), "empty workflow snapshot source") {
		t.Fatalf("clockless empty workflow snapshot error = %v", err)
	}
}

func TestOpenMigratesLegacyPullRequestFilesToPositionKey(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "gitcrawl.db")
	st, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	repoID, threadIDs := seedVectorThreads(t, ctx, st)
	threadID := threadIDs[1]
	fetchedAt := "2026-05-05T10:00:00Z"

	detail := PullRequestDetail{
		ThreadID:  threadID,
		RepoID:    repoID,
		Number:    302,
		RawJSON:   "{}",
		FetchedAt: fetchedAt,
		UpdatedAt: fetchedAt,
	}
	legacyFiles := []PullRequestFile{
		{Path: "z.go", Status: "modified", Additions: 2, Changes: 2, RawJSON: `{"filename":"z.go"}`, FetchedAt: fetchedAt},
		{Path: "a.go", Status: "added", Additions: 1, Changes: 1, RawJSON: `{"filename":"a.go"}`, FetchedAt: fetchedAt},
	}
	if err := st.UpsertPullRequestCache(ctx, detail, legacyFiles, nil, nil, nil); err != nil {
		t.Fatalf("seed pr cache: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open raw db: %v", err)
	}
	_, err = db.ExecContext(ctx, `
		drop index if exists idx_pull_request_files_path;
		drop index if exists idx_pull_request_files_thread_path;
		alter table pull_request_files rename to pull_request_files_current;
		create table pull_request_files (
			thread_id integer not null references threads(id) on delete cascade,
			path text not null,
			status text,
			additions integer not null default 0,
			deletions integer not null default 0,
			changes integer not null default 0,
			previous_path text,
			patch text,
			raw_json text not null,
			fetched_at text not null,
			primary key(thread_id, path)
		);
		insert into pull_request_files(thread_id, path, status, additions, deletions, changes, previous_path, patch, raw_json, fetched_at)
			select thread_id, path, status, additions, deletions, changes, previous_path, patch, raw_json, fetched_at
			from pull_request_files_current;
		drop table pull_request_files_current;
		create index if not exists idx_pull_request_files_path on pull_request_files(path);
		pragma user_version = 3;
	`)
	if err != nil {
		t.Fatalf("seed legacy pull_request_files table: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close raw db: %v", err)
	}

	st, err = Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open migrated store: %v", err)
	}
	defer st.Close()
	if !st.pullRequestFilesHavePositionKey(ctx) {
		t.Fatal("pull_request_files primary key was not migrated")
	}
	var version int
	if err := st.DB().QueryRowContext(ctx, `pragma user_version`).Scan(&version); err != nil {
		t.Fatalf("read user_version: %v", err)
	}
	if version != schemaVersion {
		t.Fatalf("user_version = %d, want %d", version, schemaVersion)
	}
	cache, err := st.PullRequestCache(ctx, repoID, 302)
	if err != nil {
		t.Fatalf("pull request cache: %v", err)
	}
	if len(cache.Files) != 2 ||
		cache.Files[0].Path != "a.go" || cache.Files[0].Position != 0 ||
		cache.Files[1].Path != "z.go" || cache.Files[1].Position != 1 {
		t.Fatalf("migrated files = %+v", cache.Files)
	}

	duplicateFiles := []PullRequestFile{
		{Path: "docs/governance/GOVERNANCE.md", Status: "removed", Deletions: 1, Changes: 1, RawJSON: `{"filename":"docs/governance/GOVERNANCE.md","status":"removed"}`, FetchedAt: fetchedAt},
		{Path: "docs/governance/GOVERNANCE.md", Status: "added", Additions: 161, Changes: 161, RawJSON: `{"filename":"docs/governance/GOVERNANCE.md","status":"added"}`, FetchedAt: fetchedAt},
	}
	if err := st.UpsertPullRequestCache(ctx, detail, duplicateFiles, nil, nil, nil); err != nil {
		t.Fatalf("upsert duplicate-path pr files after migration: %v", err)
	}
	cache, err = st.PullRequestCache(ctx, repoID, 302)
	if err != nil {
		t.Fatalf("updated pull request cache: %v", err)
	}
	if len(cache.Files) != 2 ||
		cache.Files[0].Status != "removed" || cache.Files[0].Position != 0 ||
		cache.Files[1].Status != "added" || cache.Files[1].Position != 1 {
		t.Fatalf("duplicate files after migration = %+v", cache.Files)
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
