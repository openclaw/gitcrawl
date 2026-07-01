package store

import (
	"context"
	"path/filepath"
	"testing"
)

func TestSyncAttemptFailureRetryAndResolve(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repoID, err := st.UpsertRepository(ctx, Repository{Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl", RawJSON: "{}", UpdatedAt: "2026-06-06T00:00:00Z"})
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	threadID, err := st.UpsertThread(ctx, Thread{
		RepoID: repoID, GitHubID: "84", Number: 84, Kind: "pull_request", State: "open",
		Title: "Track failed sync attempts", HTMLURL: "https://github.com/openclaw/gitcrawl/pull/84",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "h84", UpdatedAt: "2026-06-06T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("thread: %v", err)
	}
	first := SyncAttemptFailure{
		RepoID: repoID, ThreadID: threadID, Number: 84, Operation: "pull_request_details", ErrorClass: "rate_limit",
		ErrorMessage: "secondary rate limit", FirstSeenAt: "2026-06-06T00:00:00Z", LastSeenAt: "2026-06-06T00:00:00Z",
	}
	id, err := st.RecordSyncAttemptFailure(ctx, first)
	if err != nil {
		t.Fatalf("record first failure: %v", err)
	}
	if _, err := st.RecordSyncAttemptFailure(ctx, SyncAttemptFailure{
		RepoID: repoID, ThreadID: threadID, Number: 84, Operation: "pull_request_details", ErrorClass: "rate_limit",
		ErrorMessage: "secondary rate limit again", LastSeenAt: "2026-06-06T00:05:00Z",
	}); err != nil {
		t.Fatalf("record retry failure: %v", err)
	}

	failures, err := st.ListSyncAttemptFailures(ctx, SyncAttemptFailureListOptions{RepoID: repoID})
	if err != nil {
		t.Fatalf("list failures: %v", err)
	}
	if len(failures) != 1 {
		t.Fatalf("failure count = %d, want 1", len(failures))
	}
	if failures[0].ID != id || failures[0].RetryCount != 1 || failures[0].ResolvedAt != "" || failures[0].ErrorMessage != "secondary rate limit again" {
		t.Fatalf("failure = %+v", failures[0])
	}

	resolved, err := st.ResolveSyncAttemptFailures(ctx, repoID, 84, "2026-06-06T00:10:00Z")
	if err != nil {
		t.Fatalf("resolve failures: %v", err)
	}
	if resolved != 1 {
		t.Fatalf("resolved = %d, want 1", resolved)
	}
	unresolved, err := st.ListSyncAttemptFailures(ctx, SyncAttemptFailureListOptions{RepoID: repoID})
	if err != nil {
		t.Fatalf("list unresolved failures: %v", err)
	}
	if len(unresolved) != 0 {
		t.Fatalf("unresolved failures = %+v", unresolved)
	}
	history, err := st.ListSyncAttemptFailures(ctx, SyncAttemptFailureListOptions{RepoID: repoID, IncludeResolved: true})
	if err != nil {
		t.Fatalf("list history failures: %v", err)
	}
	if len(history) != 1 || history[0].ResolvedAt != "2026-06-06T00:10:00Z" {
		t.Fatalf("history = %+v", history)
	}

	if _, err := st.RecordSyncAttemptFailure(ctx, SyncAttemptFailure{
		RepoID: repoID, ThreadID: threadID, Number: 84, Operation: "pull_request_details", ErrorClass: "rate_limit",
		ErrorMessage: "secondary rate limit after resolve", LastSeenAt: "2026-06-06T00:15:00Z",
	}); err != nil {
		t.Fatalf("record unresolved retry: %v", err)
	}
	failures, err = st.ListSyncAttemptFailures(ctx, SyncAttemptFailureListOptions{RepoID: repoID})
	if err != nil {
		t.Fatalf("list reopened failures: %v", err)
	}
	if len(failures) != 1 || failures[0].RetryCount != 2 || failures[0].ResolvedAt != "" {
		t.Fatalf("reopened failure = %+v", failures)
	}
}
