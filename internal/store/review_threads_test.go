package store

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func TestUpsertPullRequestReviewThreadsRollsBackMergeOnInsertError(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repoID, err := st.UpsertRepository(ctx, Repository{Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl", RawJSON: "{}", UpdatedAt: "2026-05-15T00:00:00Z"})
	if err != nil {
		t.Fatalf("upsert repo: %v", err)
	}
	threadID, err := st.UpsertThread(ctx, Thread{
		RepoID: repoID, GitHubID: "1", Number: 1, Kind: "pull_request", State: "open",
		Title: "Atomic review threads", HTMLURL: "https://github.com/openclaw/gitcrawl/pull/1",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "hash", UpdatedAt: "2026-05-15T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("upsert thread: %v", err)
	}
	oldFetchedAt := "2026-05-15T00:00:00Z"
	if err := st.UpsertPullRequestReviewThreads(ctx, threadID, oldFetchedAt, []PullRequestReviewThread{{
		ReviewThreadID: "old", IsResolved: false, CommentsJSON: "[]", RawJSON: "{}", FetchedAt: oldFetchedAt,
	}}); err != nil {
		t.Fatalf("seed review threads: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `
		create trigger fail_review_thread_insert
		before insert on pull_request_review_threads
		begin
			select raise(fail, 'blocked review thread insert');
		end;
	`); err != nil {
		t.Fatalf("create fail trigger: %v", err)
	}
	defer st.DB().ExecContext(ctx, `drop trigger if exists fail_review_thread_insert`)

	err = st.UpsertPullRequestReviewThreads(ctx, threadID, "2026-05-15T00:01:00Z", []PullRequestReviewThread{{
		ReviewThreadID: "new", IsResolved: true, CommentsJSON: "[]", RawJSON: "{}", FetchedAt: "2026-05-15T00:01:00Z",
	}})
	if err == nil || !strings.Contains(err.Error(), "blocked review thread insert") {
		t.Fatalf("expected trigger error, got %v", err)
	}
	threads, err := st.PullRequestReviewThreads(ctx, threadID)
	if err != nil {
		t.Fatalf("list review threads: %v", err)
	}
	if len(threads) != 1 || threads[0].ReviewThreadID != "old" {
		t.Fatalf("review thread merge should roll back, got %+v", threads)
	}
	fetchedAt, err := st.PullRequestReviewThreadsFetchedAt(ctx, threadID)
	if err != nil {
		t.Fatalf("review thread fetched marker: %v", err)
	}
	if fetchedAt != oldFetchedAt {
		t.Fatalf("fetched marker = %q, want %q", fetchedAt, oldFetchedAt)
	}
}

func TestPullRequestReviewThreadMergeTombstoneRevisionAndRestore(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	repoID, err := st.UpsertRepository(ctx, Repository{Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl", RawJSON: "{}", UpdatedAt: "2026-05-15T00:00:00Z"})
	if err != nil {
		t.Fatalf("upsert repo: %v", err)
	}
	threadID, err := st.UpsertThread(ctx, Thread{
		RepoID: repoID, GitHubID: "2", Number: 2, Kind: "pull_request", State: "open",
		Title: "Review thread history", HTMLURL: "https://github.com/openclaw/gitcrawl/pull/2",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "hash", UpdatedAt: "2026-05-15T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("upsert thread: %v", err)
	}
	first := PullRequestReviewThread{
		ReviewThreadID: "rt-1", Path: "a.go", Line: 10, CommentsJSON: `[{"body":"first"}]`,
		RawJSON: `{"id":"rt-1","version":1}`, FetchedAt: "2026-05-15T00:01:00Z",
	}
	if err := st.UpsertPullRequestReviewThreads(ctx, threadID, first.FetchedAt, []PullRequestReviewThread{first}); err != nil {
		t.Fatalf("seed review thread: %v", err)
	}
	refetched := first
	refetched.FetchedAt = "2026-05-15T00:01:30Z"
	if err := st.UpsertPullRequestReviewThreads(ctx, threadID, refetched.FetchedAt, []PullRequestReviewThread{refetched}); err != nil {
		t.Fatalf("refetch unchanged review thread: %v", err)
	}
	var revisions int
	if err := st.DB().QueryRowContext(ctx, `select count(*) from pull_request_review_thread_revisions where thread_id = ? and review_thread_id = 'rt-1'`, threadID).Scan(&revisions); err != nil {
		t.Fatalf("unchanged review thread revisions: %v", err)
	}
	if revisions != 1 {
		t.Fatalf("unchanged refetch created %d review thread revisions", revisions)
	}
	if err := st.UpsertPullRequestReviewThreads(ctx, threadID, "2026-05-15T00:02:00Z", nil); err != nil {
		t.Fatalf("empty merge: %v", err)
	}
	threads, err := st.PullRequestReviewThreads(ctx, threadID)
	if err != nil || len(threads) != 1 || threads[0].ReviewThreadID != "rt-1" {
		t.Fatalf("not-seen review thread was removed: threads=%+v err=%v", threads, err)
	}
	first.IsResolved = true
	first.RawJSON = `{"id":"rt-1","version":2}`
	first.FetchedAt = "2026-05-15T00:03:00Z"
	if err := st.UpsertPullRequestReviewThreads(ctx, threadID, first.FetchedAt, []PullRequestReviewThread{first}); err != nil {
		t.Fatalf("edit review thread: %v", err)
	}
	if err := st.UpsertPullRequestReviewThreads(ctx, threadID, "2026-05-15T00:04:00Z", []PullRequestReviewThread{{
		ReviewThreadID: "rt-1", DeletedAt: "2026-05-15T00:04:00Z", DeletionReason: "explicit-source-delete",
	}}); err != nil {
		t.Fatalf("import sparse review thread tombstone: %v", err)
	}
	threads, err = st.PullRequestReviewThreads(ctx, threadID)
	if err != nil || len(threads) != 0 {
		t.Fatalf("tombstoned review thread remained visible: threads=%+v err=%v", threads, err)
	}
	if err := st.DB().QueryRowContext(ctx, `select count(*) from pull_request_review_thread_revisions where thread_id = ? and review_thread_id = 'rt-1'`, threadID).Scan(&revisions); err != nil {
		t.Fatalf("review thread revisions: %v", err)
	}
	if revisions != 3 {
		t.Fatalf("review thread revisions = %d, want create/edit/delete", revisions)
	}
	var retainedPath string
	if err := st.DB().QueryRowContext(ctx, `select path from pull_request_review_threads where thread_id = ? and review_thread_id = 'rt-1'`, threadID).Scan(&retainedPath); err != nil {
		t.Fatalf("retained review thread path: %v", err)
	}
	if retainedPath != "a.go" {
		t.Fatalf("sparse review thread tombstone replaced path with %q", retainedPath)
	}
	first.DeletedAt = ""
	first.DeletionReason = ""
	first.FetchedAt = "2026-05-15T00:05:00Z"
	if err := st.UpsertPullRequestReviewThreads(ctx, threadID, first.FetchedAt, []PullRequestReviewThread{first}); err != nil {
		t.Fatalf("restore review thread: %v", err)
	}
	threads, err = st.PullRequestReviewThreads(ctx, threadID)
	if err != nil || len(threads) != 1 || !threads[0].IsResolved {
		t.Fatalf("restored review thread = %+v err=%v", threads, err)
	}
	if err := st.DB().QueryRowContext(ctx, `select count(*) from pull_request_review_thread_revisions where thread_id = ? and review_thread_id = 'rt-1'`, threadID).Scan(&revisions); err != nil {
		t.Fatalf("restored review thread revisions: %v", err)
	}
	if revisions != 4 {
		t.Fatalf("review thread revisions after restore = %d, want 4", revisions)
	}
	applied, err := st.TombstonePullRequestReviewThread(ctx, threadID, "rt-1", "2026-05-15T00:06:00Z", "explicit-source-delete")
	if err != nil || !applied {
		t.Fatalf("direct review thread tombstone = %t, %v", applied, err)
	}
	applied, err = st.TombstonePullRequestReviewThread(ctx, threadID, "missing", "2026-05-15T00:06:00Z", "explicit-source-delete")
	if err != nil || applied {
		t.Fatalf("missing review thread tombstone = %t, %v", applied, err)
	}
	if _, err := st.TombstonePullRequestReviewThread(ctx, threadID, "rt-1", "", ""); err == nil || !strings.Contains(err.Error(), "deleted_at is required") {
		t.Fatalf("empty review thread tombstone error = %v", err)
	}
}
