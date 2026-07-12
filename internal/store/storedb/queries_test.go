package storedb_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/openclaw/gitcrawl/internal/store"
	"github.com/openclaw/gitcrawl/internal/store/storedb"
)

func TestGeneratedQueriesRoundTrip(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

	q := storedb.New(st.DB())
	tx, err := st.DB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if _, err := q.WithTx(tx).CountRepositories(ctx); err != nil {
		t.Fatalf("count repositories in tx: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit tx: %v", err)
	}

	now := "2026-05-15T10:00:00Z"
	later := "2026-05-15T10:01:00Z"
	ns := func(s string) sql.NullString { return sql.NullString{String: s, Valid: true} }

	repoID, err := q.UpsertRepository(ctx, storedb.UpsertRepositoryParams{
		Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl",
		GithubRepoID: ns("123"), RawJson: `{"repo":true}`, UpdatedAt: now,
	})
	if err != nil {
		t.Fatalf("upsert repository: %v", err)
	}
	threadID, err := q.UpsertThread(ctx, storedb.UpsertThreadParams{
		RepoID: repoID, GithubID: "T_1", Number: 19, Kind: "pull_request", State: "open", Title: "sqlc refactor",
		Body: ns("body"), AuthorLogin: ns("codex"), AuthorType: ns("Bot"), HtmlUrl: "https://github.com/openclaw/gitcrawl/pull/19",
		LabelsJson: `["refactor"]`, AssigneesJson: `[]`, RawJson: `{"thread":true}`, ContentHash: "hash-1", IsDraft: 0,
		CreatedAtGh: ns(now), UpdatedAtGh: ns(now), FirstPulledAt: ns(now), LastPulledAt: ns(now), UpdatedAt: now,
	})
	if err != nil {
		t.Fatalf("upsert thread: %v", err)
	}
	if updated, err := q.ReserveThreadEvidenceObservation(ctx, storedb.ReserveThreadEvidenceObservationParams{
		ID:                          threadID,
		EvidenceObservationSequence: 3,
	}); err != nil || updated != 1 {
		t.Fatalf("reserve thread evidence observation = %d, %v", updated, err)
	}
	if updated, err := q.ReserveThreadEvidenceObservation(ctx, storedb.ReserveThreadEvidenceObservationParams{
		ID:                          threadID,
		EvidenceObservationSequence: 2,
	}); err != nil || updated != 0 {
		t.Fatalf("reserve stale thread evidence observation = %d, %v", updated, err)
	}

	if got, err := q.CountRepositories(ctx); err != nil || got != 1 {
		t.Fatalf("count repositories = %d, %v", got, err)
	}
	if got, err := q.CountThreads(ctx); err != nil || got != 1 {
		t.Fatalf("count threads = %d, %v", got, err)
	}
	if got, err := q.CountOpenThreads(ctx); err != nil || got != 1 {
		t.Fatalf("count open threads = %d, %v", got, err)
	}
	if got, err := q.CountClusters(ctx); err != nil || got != 0 {
		t.Fatalf("count clusters = %d, %v", got, err)
	}
	if _, err := q.RepositoryByFullName(ctx, "openclaw/gitcrawl"); err != nil {
		t.Fatalf("repository by full name: %v", err)
	}
	if rows, err := q.ListRepositories(ctx); err != nil || len(rows) != 1 {
		t.Fatalf("list repositories len = %d, %v", len(rows), err)
	}
	if rows, err := q.ListThreadsCurrentSchema(ctx, storedb.ListThreadsCurrentSchemaParams{
		RepoID: repoID, IncludeClosed: 0, RowLimit: 10,
	}); err != nil || len(rows) != 1 {
		t.Fatalf("list threads len = %d, %v", len(rows), err)
	}

	commentID, err := q.UpsertComment(ctx, storedb.UpsertCommentParams{
		ThreadID: threadID, GithubID: "IC_1", CommentType: "issue_comment", AuthorLogin: ns("alice"),
		AuthorType: ns("User"), Body: "comment", IsBot: 0, RawJson: `{"comment":true}`, CreatedAtGh: ns(now), UpdatedAtGh: ns(now),
	})
	if err != nil || commentID == 0 {
		t.Fatalf("upsert comment id = %d, %v", commentID, err)
	}
	if rows, err := q.ListComments(ctx, threadID); err != nil || len(rows) != 1 {
		t.Fatalf("list comments len = %d, %v", len(rows), err)
	}
	documentID, err := q.UpsertDocument(ctx, storedb.UpsertDocumentParams{
		ThreadID: threadID, Title: "sqlc refactor", Body: ns("document body"), RawText: "raw", DedupeText: "dedupe", UpdatedAt: now,
	})
	if err != nil || documentID == 0 {
		t.Fatalf("upsert document id = %d, %v", documentID, err)
	}

	runParams := storedb.RecordSyncRunParams{
		RepoID: repoID, Scope: "all", Status: "success", StartedAt: now, FinishedAt: ns(later), StatsJson: ns(`{"ok":true}`),
	}
	if _, err := q.RecordSyncRun(ctx, runParams); err != nil {
		t.Fatalf("record sync run: %v", err)
	}
	if got, err := q.LastSuccessfulSyncAt(ctx, repoID); err != nil || got != later {
		t.Fatalf("last successful sync = %q, %v", got, err)
	}
	if got, err := q.LastSuccessfulListSyncAt(ctx, storedb.LastSuccessfulListSyncAtParams{RepoID: repoID, State: "open"}); err != nil || got != later {
		t.Fatalf("last successful list sync = %q, %v", got, err)
	}
	if got, err := q.MaxSuccessfulSyncFinishedAt(ctx); err != nil || got != later {
		t.Fatalf("max successful sync = %q, %v", got, err)
	}
	if rows, err := q.ListSyncRuns(ctx, storedb.ListSyncRunsParams{RepoID: repoID, RowLimit: 10}); err != nil || len(rows) != 1 {
		t.Fatalf("list sync runs len = %d, %v", len(rows), err)
	}
	if _, err := q.RecordSummaryRun(ctx, storedb.RecordSummaryRunParams(runParams)); err != nil {
		t.Fatalf("record summary run: %v", err)
	}
	if rows, err := q.ListSummaryRuns(ctx, storedb.ListSummaryRunsParams{RepoID: repoID, RowLimit: 10}); err != nil || len(rows) != 1 {
		t.Fatalf("list summary runs len = %d, %v", len(rows), err)
	}
	if _, err := q.RecordEmbeddingRun(ctx, storedb.RecordEmbeddingRunParams(runParams)); err != nil {
		t.Fatalf("record embedding run: %v", err)
	}
	if rows, err := q.ListEmbeddingRuns(ctx, storedb.ListEmbeddingRunsParams{RepoID: repoID, RowLimit: 10}); err != nil || len(rows) != 1 {
		t.Fatalf("list embedding runs len = %d, %v", len(rows), err)
	}
	if _, err := q.RecordClusterRun(ctx, storedb.RecordClusterRunParams(runParams)); err != nil {
		t.Fatalf("record cluster run: %v", err)
	}
	if rows, err := q.ListClusterRuns(ctx, storedb.ListClusterRunsParams{RepoID: repoID, RowLimit: 10}); err != nil || len(rows) != 1 {
		t.Fatalf("list cluster runs len = %d, %v", len(rows), err)
	}

	if err := q.UpsertPullRequestDetail(ctx, storedb.UpsertPullRequestDetailParams{
		ThreadID: threadID, RepoID: repoID, Number: 19, BaseSha: ns("base"), HeadSha: ns("head"), HeadRef: ns("feature/sqlc"),
		HeadRepoFullName: ns("openclaw/gitcrawl"), MergeableState: ns("clean"), Additions: 10, Deletions: 2, ChangedFiles: 3,
		RawJson: `{"detail":true}`, FetchedAt: now, UpdatedAt: later,
	}); err != nil {
		t.Fatalf("upsert pr detail: %v", err)
	}
	if _, err := q.PullRequestDetail(ctx, storedb.PullRequestDetailParams{RepoID: repoID, Number: 19}); err != nil {
		t.Fatalf("pull request detail: %v", err)
	}
	if err := q.InsertPullRequestFile(ctx, storedb.InsertPullRequestFileParams{
		ThreadID: threadID, Path: "internal/store/store.go", Status: ns("modified"), Additions: 4, Deletions: 1, Changes: 5,
		PreviousPath: ns("internal/store/db.go"), Patch: ns("@@"), RawJson: `{"file":true}`, FetchedAt: now,
	}); err != nil {
		t.Fatalf("insert pr file: %v", err)
	}
	if rows, err := q.PullRequestFiles(ctx, threadID); err != nil || len(rows) != 1 {
		t.Fatalf("pull request files len = %d, %v", len(rows), err)
	}
	if err := q.InsertPullRequestCommit(ctx, storedb.InsertPullRequestCommitParams{
		ThreadID: threadID, Sha: "abc123", Message: ns("commit"), AuthorLogin: ns("alice"), AuthorName: ns("Alice"),
		CommittedAt: ns(now), HtmlUrl: ns("https://github.com/openclaw/gitcrawl/commit/abc123"), RawJson: `{"commit":true}`, FetchedAt: now,
	}); err != nil {
		t.Fatalf("insert pr commit: %v", err)
	}
	if rows, err := q.PullRequestCommits(ctx, threadID); err != nil || len(rows) != 1 {
		t.Fatalf("pull request commits len = %d, %v", len(rows), err)
	}
	if err := q.InsertPullRequestCheck(ctx, storedb.InsertPullRequestCheckParams{
		ThreadID: threadID, Name: "Go", Status: ns("completed"), Conclusion: ns("success"), DetailsUrl: ns("https://example.test/check"),
		WorkflowName: ns("CI"), StartedAt: ns(now), CompletedAt: ns(later), RawJson: `{"check":true}`, FetchedAt: now,
	}); err != nil {
		t.Fatalf("insert pr check: %v", err)
	}
	if rows, err := q.PullRequestChecks(ctx, threadID); err != nil || len(rows) != 1 {
		t.Fatalf("pull request checks len = %d, %v", len(rows), err)
	}
	if err := q.UpsertPullRequestReviewThread(ctx, storedb.UpsertPullRequestReviewThreadParams{
		ThreadID: threadID, ReviewThreadID: "PRRT_1", Path: ns("internal/store/store.go"), Line: 10, StartLine: 9,
		ViewerCanResolve: 1, ViewerCanReply: 1, FirstAuthorLogin: ns("reviewer"), FirstAuthorType: ns("User"),
		FirstCommentBody: ns("nit"), FirstCommentUrl: ns("https://example.test/comment"), FirstCommentCreatedAt: ns(now),
		FirstCommentUpdatedAt: ns(now), CommentsJson: `[]`, RawJson: `{"review":true}`, FetchedAt: now,
	}); err != nil {
		t.Fatalf("upsert review thread: %v", err)
	}
	if rows, err := q.PullRequestReviewThreads(ctx, threadID); err != nil || len(rows) != 1 {
		t.Fatalf("pull request review threads len = %d, %v", len(rows), err)
	}
	if err := q.UpsertPullRequestReviewThreadSync(ctx, storedb.UpsertPullRequestReviewThreadSyncParams{ThreadID: threadID, FetchedAt: later}); err != nil {
		t.Fatalf("upsert review thread sync: %v", err)
	}
	if got, err := q.PullRequestReviewThreadsFetchedAt(ctx, threadID); err != nil || got != later {
		t.Fatalf("review threads fetched at = %q, %v", got, err)
	}

	if err := q.UpsertWorkflowRun(ctx, storedb.UpsertWorkflowRunParams{
		RepoID: repoID, RunID: "987", RunNumber: 1, HeadBranch: ns("feature/sqlc"), HeadSha: ns("head"),
		Status: ns("completed"), Conclusion: ns("success"), WorkflowName: ns("CI"), Event: ns("pull_request"),
		HtmlUrl: ns("https://github.com/openclaw/gitcrawl/actions/runs/987"), CreatedAtGh: ns(now), UpdatedAtGh: ns(later),
		RawJson: `{"workflow":true}`, FetchedAt: later,
	}); err != nil {
		t.Fatalf("upsert workflow run: %v", err)
	}
	if rows, err := q.ListWorkflowRuns(ctx, storedb.ListWorkflowRunsParams{RepoID: repoID, HeadBranch: nil, HeadSha: nil, RowLimit: 10}); err != nil || len(rows) != 1 {
		t.Fatalf("list workflow runs len = %d, %v", len(rows), err)
	}

	if _, err := st.DB().ExecContext(ctx, `insert into repo_sync_state(repo_id, last_open_close_reconciled_at, updated_at) values(?, ?, ?)`, repoID, later, now); err != nil {
		t.Fatalf("insert repo sync state: %v", err)
	}
	if got, err := q.RepoSyncStateLastSync(ctx); err != nil || got != later {
		t.Fatalf("repo sync state last sync = %q, %v", got, err)
	}
	if _, err := st.DB().ExecContext(ctx, `create table portable_metadata(key text primary key, value text not null)`); err != nil {
		t.Fatalf("create portable metadata: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `insert into portable_metadata(key, value) values('exported_at', ?)`, later); err != nil {
		t.Fatalf("insert portable metadata: %v", err)
	}
	if got, err := q.PortableExportedAt(ctx); err != nil || got != later {
		t.Fatalf("portable exported at = %q, %v", got, err)
	}

	if rows, err := q.CloseThreadLocally(ctx, storedb.CloseThreadLocallyParams{ClosedAt: ns(later), Reason: ns("done"), RepoID: repoID, Number: 19}); err != nil || rows != 1 {
		t.Fatalf("close thread locally rows = %d, %v", rows, err)
	}
	if rows, err := q.ReopenThreadLocally(ctx, storedb.ReopenThreadLocallyParams{UpdatedAt: later, RepoID: repoID, Number: 19}); err != nil || rows != 1 {
		t.Fatalf("reopen thread locally rows = %d, %v", rows, err)
	}
	if rows, err := q.MarkOpenThreadClosedFromGitHub(ctx, storedb.MarkOpenThreadClosedFromGitHubParams{
		GithubID: "T_1", State: "closed", Title: "sqlc refactor", Body: ns("body"), AuthorLogin: ns("codex"), AuthorType: ns("Bot"),
		HtmlUrl: "https://github.com/openclaw/gitcrawl/pull/19", LabelsJson: `["refactor"]`, AssigneesJson: `[]`, RawJson: `{"thread":true}`,
		ContentHash: "hash-2", CreatedAtGh: ns(now), UpdatedAtGh: ns(later), ClosedAtGh: ns(later), LastPulledAt: ns(later),
		UpdatedAt: later, RepoID: repoID, Kind: "pull_request", Number: 19,
	}); err != nil || rows != 1 {
		t.Fatalf("mark thread closed rows = %d, %v", rows, err)
	}

	if err := q.DeletePullRequestChecks(ctx, threadID); err != nil {
		t.Fatalf("delete pr checks: %v", err)
	}
	if err := q.DeletePullRequestCommits(ctx, threadID); err != nil {
		t.Fatalf("delete pr commits: %v", err)
	}
	if err := q.DeletePullRequestFiles(ctx, threadID); err != nil {
		t.Fatalf("delete pr files: %v", err)
	}
	if err := q.DeletePullRequestReviewThreads(ctx, threadID); err != nil {
		t.Fatalf("delete review threads: %v", err)
	}
}
