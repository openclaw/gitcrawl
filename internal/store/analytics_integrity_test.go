package store

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
)

func TestAnalyticsFailureRecoveryRetainsReceiptsAndFairRetryTimes(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "archive.db")
	s, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	attempt := AnalyticsAttempt{Repository: "fixture/repo", Number: 7, Operation: "graphql_history", StartedAt: "2026-01-01T00:00:00Z", FinishedAt: "2026-01-01T00:00:01Z", Status: "failed", ErrorClass: "validation", ErrorText: "incomplete connection", Evidence: json.RawMessage(`{"totalCount":2,"received":1}`)}
	if err = s.RecordAnalyticsAttempt(ctx, attempt); err != nil {
		t.Fatal(err)
	}
	if due, err := s.DueAnalyticsRetries(ctx, "fixture/repo", "2026-01-01T00:00:10Z", 5); err != nil || len(due) != 0 {
		t.Fatalf("early retry %v %v", due, err)
	}
	s.Close()
	s, err = Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if due, err := s.DueAnalyticsRetries(ctx, "fixture/repo", "2026-01-01T00:00:40Z", 5); err != nil || len(due) != 1 || due[0] != 7 {
		t.Fatalf("restart lost failure %v %v", due, err)
	}
	attempt.Number = 8
	attempt.FinishedAt = "2026-01-01T00:00:02Z"
	if err = s.RecordAnalyticsAttempt(ctx, attempt); err != nil {
		t.Fatal(err)
	}
	attempt.Number = 7
	attempt.FinishedAt = "2026-01-01T00:00:40Z"
	if err = s.RecordAnalyticsAttempt(ctx, attempt); err != nil {
		t.Fatal(err)
	}
	if due, err := s.DueAnalyticsRetries(ctx, "fixture/repo", "2026-01-01T00:00:41Z", 1); err != nil || len(due) != 1 || due[0] != 8 {
		t.Fatalf("poison item starved peer %v %v", due, err)
	}
	attempt.Status = "success"
	attempt.FinishedAt = "2026-01-01T00:02:00Z"
	attempt.ErrorClass = ""
	attempt.ErrorText = ""
	if err = s.RecordAnalyticsAttempt(ctx, attempt); err != nil {
		t.Fatal(err)
	}
	var receipts, resolved int
	s.DB().QueryRow("SELECT count(*) FROM analytics_fetch_attempts").Scan(&receipts)
	s.DB().QueryRow("SELECT count(*) FROM analytics_retries WHERE number=7 AND resolved_at IS NOT NULL").Scan(&resolved)
	if receipts != 4 || resolved != 1 {
		t.Fatalf("history lost receipts=%d resolved=%d", receipts, resolved)
	}
}

func TestReviewStateRecoveryIsBoundedResumableAndPreservesHistory(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	_, err = s.DB().Exec(`INSERT INTO repositories(id,owner,name,full_name,github_repo_id,raw_json,updated_at) VALUES(1,'fixture','repo','Fixture/Repo',1,'{}','2026-01-01');
INSERT INTO threads(id,repo_id,github_id,number,kind,state,title,html_url,labels_json,assignees_json,raw_json,content_hash,updated_at,last_pulled_at) VALUES
(1,1,'P1',1,'pull_request','open','','','[]','[]','{"_graphql":{"reviewThreads":{"totalCount":1}}}','h1','2026-01-01','2026-01-01T00:00:00Z'),
(2,1,'P2',2,'pull_request','open','','','[]','[]','{"_gitcrawl_source":"graphql","_graphql":{"reviewThreads":{"totalCount":0,"nodes":[],"pageInfo":{"hasNextPage":false}}}}','h2','2026-01-01','2026-01-01T00:00:00Z');`)
	if err != nil {
		t.Fatal(err)
	}
	p, err := s.SeedReviewStateRecovery(ctx, "fixture/repo", 1)
	if err != nil || p.Done || p.Queued != 1 || p.Cursor != 1 {
		t.Fatalf("first step %+v %v", p, err)
	}
	p, err = s.SeedReviewStateRecovery(ctx, "fixture/repo", 1)
	if err != nil || !p.Done || p.Queued != 1 || p.Scanned != 2 {
		t.Fatalf("resume %+v %v", p, err)
	}
	var emptyIDs, observed string
	if err = s.DB().QueryRow("SELECT review_thread_ids_json,fetched_at FROM pull_request_review_thread_syncs WHERE thread_id=2").Scan(&emptyIDs, &observed); err != nil || emptyIDs != "[]" || observed != "2026-01-01T00:00:00Z" {
		t.Fatalf("known empty evidence not materialized: %s %s %v", emptyIDs, observed, err)
	}
	threads := []PullRequestReviewThread{{ReviewThreadID: "T1", ThreadID: 1, IsResolved: true, IsOutdated: false, RawJSON: `{"id":"T1","isResolved":true}`, CommentsJSON: `[]`, FetchedAt: "2026-01-02T00:00:00Z"}}
	if err = s.UpsertPullRequestReviewThreads(ctx, 1, "2026-01-02T00:00:00Z", threads); err != nil {
		t.Fatal(err)
	}
	threads[0].IsResolved = false
	threads[0].RawJSON = `{"id":"T1","isResolved":false}`
	if err = s.UpsertPullRequestReviewThreads(ctx, 1, "2026-01-03T00:00:00Z", threads); err != nil {
		t.Fatal(err)
	}
	if err = s.UpsertPullRequestReviewThreads(ctx, 1, "2026-01-04T00:00:00Z", nil); err != nil {
		t.Fatal(err)
	}
	var revisions, retained int
	var membership string
	s.DB().QueryRow("SELECT count(*) FROM pull_request_review_thread_revisions WHERE thread_id=1").Scan(&revisions)
	s.DB().QueryRow("SELECT count(*) FROM pull_request_review_threads WHERE thread_id=1 AND deleted_at IS NULL").Scan(&retained)
	s.DB().QueryRow("SELECT review_thread_ids_json FROM pull_request_review_thread_syncs WHERE thread_id=1").Scan(&membership)
	if revisions != 2 || retained != 1 || membership != "[]" {
		t.Fatalf("absence became deletion or history lost: %d %d %s", revisions, retained, membership)
	}
	for _, invalid := range [][]PullRequestReviewThread{{{ReviewThreadID: ""}}, {{ReviewThreadID: "duplicate"}, {ReviewThreadID: "duplicate"}}} {
		if err = s.UpsertPullRequestReviewThreads(ctx, 1, "2026-01-05T00:00:00Z", invalid); err == nil {
			t.Fatal("invalid membership was certified")
		}
	}
	s.DB().QueryRow("SELECT review_thread_ids_json FROM pull_request_review_thread_syncs WHERE thread_id=1").Scan(&membership)
	if membership != "[]" {
		t.Fatal("rejected input changed prior membership")
	}
	if err = s.UpsertPullRequestReviewThreads(ctx, 1, "2026-01-01T00:00:00Z", threads); err != nil {
		t.Fatal(err)
	}
	s.DB().QueryRow("SELECT review_thread_ids_json,fetched_at FROM pull_request_review_thread_syncs WHERE thread_id=1").Scan(&membership, &observed)
	if membership != "[]" || observed != "2026-01-04T00:00:00Z" {
		t.Fatalf("older observation overwrote newer membership: %s %s", membership, observed)
	}
}

func TestAnalyticsV14MigrationPreservesReceiptsAndCoverageWatermark(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "archive.db")
	s, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.SaveAnalyticsCoverage(ctx, "fixture/repo", "2026-01-01T00:00:00Z", 1, 2); err != nil {
		t.Fatal(err)
	}
	if err = s.SetAnalyticsState(ctx, "updates:fixture/repo", `{"kind":1,"cursor":"provider-cursor"}`); err != nil {
		t.Fatal(err)
	}
	if _, err = s.DB().Exec(`DROP TABLE analytics_fetch_attempts; DROP TABLE analytics_retries; ALTER TABLE pull_request_review_thread_syncs DROP COLUMN review_thread_ids_json; PRAGMA user_version=14`); err != nil {
		t.Fatal(err)
	}
	if err = s.markObservationSchemaConverged(ctx); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, err = Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var version, complete int
	var through string
	s.DB().QueryRow("PRAGMA user_version").Scan(&version)
	s.DB().QueryRow("SELECT through,complete FROM analytics_coverage WHERE repository='fixture/repo'").Scan(&through, &complete)
	cp, err := s.AnalyticsState(ctx, "updates:fixture/repo")
	if err != nil || version != 15 || through != "2026-01-01T00:00:00Z" || complete != 1 || cp != `{"kind":1,"cursor":"provider-cursor"}` {
		t.Fatalf("migration changed evidence: %d %s %d %s %v", version, through, complete, cp, err)
	}
}

func TestReviewStateFailuresDoNotInvalidateVerifiedCoreCoverage(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.SaveAnalyticsCoverage(ctx, "fixture/repo", "2026-01-01T00:00:00Z", 1, 2); err != nil {
		t.Fatal(err)
	}
	a := AnalyticsAttempt{Repository: "fixture/repo", Number: 2, Operation: "review_state", StartedAt: "2026-01-02T00:00:00Z", FinishedAt: "2026-01-02T00:00:01Z", Status: "failed", ErrorClass: "validation", Evidence: json.RawMessage(`{}`)}
	if err = s.RecordAnalyticsAttempt(ctx, a); err != nil {
		t.Fatal(err)
	}
	if err = s.SaveReviewStateCoverage(ctx, "fixture/repo", ReviewStateRecovery{Done: true, Scanned: 2, Ceiling: 2, Cursor: 2, Queued: 1}); err != nil {
		t.Fatal(err)
	}
	var core, review int
	s.DB().QueryRow("SELECT complete FROM analytics_coverage WHERE repository='fixture/repo'").Scan(&core)
	s.DB().QueryRow("SELECT complete FROM analytics_review_state_coverage WHERE repository='fixture/repo'").Scan(&review)
	if core != 1 || review != 0 {
		t.Fatalf("enrichment invalidated core: core=%d review=%d", core, review)
	}
	if n, err := s.AnalyticsCoreOutstanding(ctx, "fixture/repo"); err != nil || n != 0 {
		t.Fatalf("wrong core queue %d %v", n, err)
	}
	a.Operation = "graphql_history"
	if err = s.RecordAnalyticsAttempt(ctx, a); err != nil {
		t.Fatal(err)
	}
	s.DB().QueryRow("SELECT complete FROM analytics_coverage WHERE repository='fixture/repo'").Scan(&core)
	if core != 0 {
		t.Fatal("core failure left core coverage complete")
	}
}

func TestAnalyticsStatusSelectsRepositoryBeforeLimit(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.SaveAnalyticsCoverage(ctx, "fixture/repo", "2026-01-01T00:00:00Z", 1, 2)
	a := AnalyticsAttempt{Repository: "fixture/repo", Number: 1, Operation: "graphql_history", StartedAt: "2026-01-01T00:00:00Z", FinishedAt: "2026-01-01T00:00:01Z", Status: "success", Evidence: json.RawMessage(`{}`)}
	if err = s.RecordAnalyticsAttempt(ctx, a); err != nil {
		t.Fatal(err)
	}
	a.Repository = "fixture/other"
	for i := 0; i < 60; i++ {
		if err = s.RecordAnalyticsAttempt(ctx, a); err != nil {
			t.Fatal(err)
		}
	}
	status, err := s.AnalyticsIntegrityStatus(ctx, "fixture/repo")
	if err != nil {
		t.Fatal(err)
	}
	if len(status["latest_attempts"].([]map[string]any)) != 1 {
		t.Fatal("another repository hid scoped history")
	}
}

func TestAnalyticsStatusBeforeCollectionIsUnknown(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	status, err := s.AnalyticsIntegrityStatus(ctx, "fixture/repo")
	if err != nil {
		t.Fatal(err)
	}
	coverage := status["coverage"].(map[string]any)
	if coverage["state"] != "not_started" || coverage["complete"] != false || coverage["issues"] != nil {
		t.Fatalf("invented coverage: %+v", coverage)
	}
	var n int
	s.DB().QueryRow("SELECT count(*) FROM analytics_coverage").Scan(&n)
	if n != 0 {
		t.Fatal("read-only status wrote a baseline")
	}
}

func TestAnalyticsV14WithoutExtensionTablesCanUpgrade(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "archive.db")
	s, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.DB().Exec(`DROP TABLE actor_profiles; DROP TABLE analytics_collection_state; PRAGMA user_version=14`); err != nil {
		t.Fatal(err)
	}
	if err = s.markObservationSchemaConverged(ctx); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, err = Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err = s.AnalyticsState(ctx, "missing"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.AnalyticsProfileNodes(ctx, 1); err != nil {
		t.Fatal(err)
	}
}

func TestAnalyticsCoreRetryOwnsBackoffWhenReviewRetryAlsoExists(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	a := AnalyticsAttempt{Repository: "fixture/repo", Number: 1, Operation: "graphql_history", StartedAt: "2026-01-01T00:00:00Z", FinishedAt: "2026-01-01T00:00:01Z", Status: "failed", Evidence: json.RawMessage(`{}`)}
	if err = s.RecordAnalyticsAttempt(ctx, a); err != nil {
		t.Fatal(err)
	}
	a.Operation = "review_state"
	if err = s.RecordAnalyticsAttempt(ctx, a); err != nil {
		t.Fatal(err)
	}
	if due, err := s.DueAnalyticsRetries(ctx, a.Repository, "2026-01-01T00:00:40Z", 8, "review_state"); err != nil || len(due) != 0 {
		t.Fatalf("review bypassed due core scheduler: %v %v", due, err)
	}
	a.Operation = "graphql_history"
	a.FinishedAt = "2026-01-01T00:00:50Z"
	if err = s.RecordAnalyticsAttempt(ctx, a); err != nil {
		t.Fatal(err)
	}
	if due, err := s.DueAnalyticsRetries(ctx, a.Repository, "2026-01-01T00:01:00Z", 8, "review_state"); err != nil || len(due) != 0 {
		t.Fatalf("review bypassed core backoff: %v %v", due, err)
	}
}

func TestAnalyticsRecoveryRequiresAnAcceptedMembershipObservation(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	a := AnalyticsAttempt{Repository: "fixture/repo", Number: 1, Operation: "review_state", StartedAt: "2026-01-01T00:00:00Z", FinishedAt: "2026-01-01T00:00:01Z", Status: "success", Evidence: json.RawMessage(`{"threads_skipped_stale":1}`)}
	if err = s.RecordAnalyticsAttempt(ctx, a); err != nil {
		t.Fatal(err)
	}
	var status, class string
	s.DB().QueryRow("SELECT status,error_class FROM analytics_fetch_attempts ORDER BY id DESC LIMIT 1").Scan(&status, &class)
	if status != "failed" || class != "unapplied_review_state" {
		t.Fatal("fetch success certified missing membership")
	}
	repo, err := s.UpsertRepository(ctx, Repository{Owner: "fixture", Name: "repo", FullName: "fixture/repo", RawJSON: "{}", UpdatedAt: "2026-01-02T00:00:00Z"})
	if err != nil {
		t.Fatal(err)
	}
	tid, err := s.UpsertThread(ctx, Thread{RepoID: repo, GitHubID: "P1", Number: 1, Kind: "pull_request", State: "open", Title: "fixture", HTMLURL: "https://github.com/fixture/repo/pull/1", LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "h", UpdatedAt: "2026-01-02T00:00:00Z"})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.UpsertPullRequestReviewThreads(ctx, tid, "2026-01-02T00:00:00Z", nil); err != nil {
		t.Fatal(err)
	}
	a.FinishedAt = "2026-01-02T00:00:01Z"
	if err = s.RecordAnalyticsAttempt(ctx, a); err != nil {
		t.Fatal(err)
	}
	if n, err := s.AnalyticsOutstanding(ctx, a.Repository); err != nil || n != 0 {
		t.Fatalf("proven empty membership did not reconcile: %d %v", n, err)
	}
}
