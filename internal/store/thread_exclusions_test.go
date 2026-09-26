package store

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

func exclusionFixture(t *testing.T) *Store {
	t.Helper()
	s, err := Open(context.Background(), filepath.Join(t.TempDir(), "archive.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	_, err = s.DB().Exec(`
INSERT INTO repositories(id,owner,name,full_name,github_repo_id,raw_json,updated_at) VALUES(1,'fixture','repo','fixture/repo','100','{}','2026-01-01');
INSERT INTO threads(id,repo_id,github_id,number,kind,state,title,body,html_url,labels_json,assignees_json,raw_json,content_hash,updated_at) VALUES
 (1,1,'gh10',10,'pull_request','closed','remove','OWNER_REMOVAL_SENTINEL','https://github.com/fixture/repo/pull/10','[]','[]','{"id":1010,"node_id":"node10","_graphql":{"id":"node10"}}','hash10','2026-01-01'),
 (2,1,'gh20',20,'pull_request','open','keep','UNCHANGED_BODY','https://github.com/fixture/repo/pull/20','[]','[]','{"node_id":"node20"}','hash20','2026-01-01');
INSERT INTO comments(id,thread_id,github_id,comment_type,body,raw_json) VALUES
 (1,1,'comment10','issue_comment','removed comment','{"node_id":"comment-node10"}'),
 (2,2,'comment20','issue_comment','kept comment','{"node_id":"comment-node20"}');
INSERT INTO comment_revisions(id,comment_id,body,raw_json,recorded_at) VALUES
 (1,1,'removed old comment','{"node_id":"comment-node10"}','2026-01-01'),
 (2,2,'kept old comment','{"node_id":"comment-node20"}','2026-01-01');
INSERT INTO documents(id,thread_id,title,body,raw_text,dedupe_text,updated_at) VALUES
 (1,1,'remove','OWNER_REMOVAL_SENTINEL','OWNER_REMOVAL_SENTINEL','remove','2026-01-01'),
 (2,2,'keep','UNCHANGED_BODY','UNCHANGED_BODY','keep','2026-01-01');
INSERT INTO pull_request_details(thread_id,repo_id,number,raw_json,fetched_at,updated_at) VALUES(1,1,10,'{"fixture":"removed"}','2026-01-01','2026-01-01'),(2,1,20,'{"fixture":"kept"}','2026-01-01','2026-01-01');
INSERT INTO actor_identity_evidence VALUES('node10','shared-actor','fixture','User','2026-01-01','{}'),('comment-node10','shared-actor','fixture','User','2026-01-01','{}');
INSERT INTO actor_profiles(node_id,login,actor_type,observed_at,raw_json) VALUES('shared-actor','fixture','User','2026-01-01','{}');
INSERT INTO analytics_pending_nodes VALUES('node10','identity'),('comment-node10','identity'),('shared-actor','profile');
`)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	at := "2026-01-01T00:00:00Z"
	if err = s.SaveAnalyticsCoverage(ctx, "fixture/repo", at, 0, 2); err != nil {
		t.Fatal(err)
	}
	for _, n := range []int{10, 20} {
		if err = s.RecordAnalyticsAttempt(ctx, AnalyticsAttempt{Repository: "fixture/repo", Number: n, Operation: "review_state", StartedAt: at, FinishedAt: at, Status: "failed", ErrorClass: "partial_response", Evidence: []byte(`{"numbers":[10,20],"body_evidence":{"sha256":"fixture-hash","bytes":10}}`)}); err != nil {
			t.Fatal(err)
		}
	}
	if err = s.SaveReviewStateCoverage(ctx, "fixture/repo", ReviewStateRecovery{Done: true, Cursor: 2, Ceiling: 2, Scanned: 2, Queued: 2}); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestOwnerPurgeRemovesExactClosureAndPreventsReintroduction(t *testing.T) {
	s := exclusionFixture(t)
	ctx := context.Background()
	plan, err := s.PlanThreadPurge(ctx, "fixture/repo", []int{10})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Applied || len(plan.Targets) != 1 || len(plan.Targets[0].Nodes) != 2 || plan.Counts["comments"] != 1 || plan.Counts["comment_revisions"] != 1 {
		t.Fatalf("plan=%+v", plan)
	}
	var retained string
	s.DB().QueryRow("SELECT raw_json FROM pull_request_details WHERE thread_id=2").Scan(&retained)
	result, err := s.PurgeThreads(ctx, "fixture/repo", []int{10}, "owner-request-fixture")
	if err != nil || !result.Applied {
		t.Fatalf("%+v %v", result, err)
	}
	for _, table := range []string{"threads", "comments", "comment_revisions", "documents", "pull_request_details"} {
		var n int
		if err = s.DB().QueryRow("SELECT count(*) FROM " + table).Scan(&n); err != nil || n != 1 {
			t.Fatalf("%s: %d %v", table, n, err)
		}
	}
	var after string
	s.DB().QueryRow("SELECT raw_json FROM pull_request_details WHERE thread_id=2").Scan(&after)
	if after != retained {
		t.Fatal("peer evidence changed")
	}
	var fts int
	if err = s.DB().QueryRow("SELECT count(*) FROM documents_fts WHERE documents_fts MATCH 'OWNER_REMOVAL_SENTINEL'").Scan(&fts); err != nil || fts != 0 {
		t.Fatalf("FTS leaked removed document: %d %v", fts, err)
	}
	var profiles int
	s.DB().QueryRow("SELECT count(*) FROM actor_profiles").Scan(&profiles)
	if profiles != 1 {
		t.Fatal("shared actor removed")
	}
	var remaining string
	s.DB().QueryRow("SELECT evidence_json FROM analytics_fetch_attempts WHERE number=20").Scan(&remaining)
	if !strings.Contains(remaining, `"numbers":[10,20]`) {
		t.Fatal("unrelated mixed receipt changed")
	}
	if excluded, err := s.ThreadExcluded(ctx, "FIXTURE/REPO", 10); err != nil || !excluded {
		t.Fatal("missing permanent policy")
	}
	if excluded, err := s.NodeExcluded(ctx, "1010"); err != nil || excluded {
		t.Fatal("REST database ID mistaken for GraphQL node", err)
	}
	if _, err = s.UpsertThread(ctx, Thread{RepoID: 1, Number: 10, Kind: "pull_request", GitHubID: "gh10"}); !errors.Is(err, ErrThreadExcluded) {
		t.Fatalf("reinsertion allowed: %v", err)
	}
	if err = s.SaveActorEvidence(ctx, []map[string]any{{"id": "node10", "author": map[string]any{"id": "shared-actor"}}}, "2026-01-02T00:00:00Z"); err != nil {
		t.Fatal(err)
	}
	if err = s.RecordAnalyticsAttempt(ctx, AnalyticsAttempt{Repository: "fixture/repo", Number: 10, Operation: "review_state", StartedAt: "2026-01-02T00:00:00Z", FinishedAt: "2026-01-02T00:00:00Z", Status: "success", Evidence: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{"SELECT count(*) FROM analytics_fetch_attempts WHERE number=10", "SELECT count(*) FROM analytics_retries WHERE number=10", "SELECT count(*) FROM actor_identity_evidence WHERE node_id='node10'", "SELECT count(*) FROM analytics_pending_nodes WHERE node_id='node10'"} {
		var n int
		s.DB().QueryRow(q).Scan(&n)
		if n != 0 {
			t.Fatal("excluded evidence recreated", q, n)
		}
	}
	if _, err = s.DB().Exec("INSERT INTO analytics_pending_nodes VALUES('node10','identity')"); err == nil {
		t.Fatal("SQL guard bypass")
	}
	// The remaining ordinary retry can resolve, but owner removal is not provider completeness.
	s.DB().Exec("UPDATE analytics_retries SET resolved_at='2026-01-02T00:00:00Z' WHERE number=20")
	if err = s.SaveReviewStateCoverage(ctx, "fixture/repo", ReviewStateRecovery{Done: true}); err != nil {
		t.Fatal(err)
	}
	var complete, pending, excluded, core int
	s.DB().QueryRow("SELECT complete,pending_items,owner_excluded_items FROM analytics_review_state_coverage").Scan(&complete, &pending, &excluded)
	s.DB().QueryRow("SELECT complete FROM analytics_coverage").Scan(&core)
	if complete != 0 || pending != 0 || excluded != 1 || core != 1 {
		t.Fatalf("false recovery: review=%d pending=%d excluded=%d core=%d", complete, pending, excluded, core)
	}
	again, err := s.PurgeThreads(ctx, "fixture/repo", []int{10}, "same-request")
	if err != nil || !again.Targets[0].AlreadyExcluded {
		t.Fatalf("idempotency: %+v %v", again, err)
	}
	var original string
	s.DB().QueryRow("SELECT request_id FROM thread_exclusions").Scan(&original)
	if original != "owner-request-fixture" {
		t.Fatal("original owner receipt rewritten")
	}
}

func TestOwnerPurgeRejectsTailAndRollsBack(t *testing.T) {
	s := exclusionFixture(t)
	ctx := context.Background()
	if _, err := s.PurgeThreads(ctx, "fixture/repo", []int{20}, "tail"); err == nil || !strings.Contains(err.Error(), "ID reuse") {
		t.Fatalf("tail admitted: %v", err)
	}
	var n int
	s.DB().QueryRow("SELECT count(*) FROM thread_exclusions").Scan(&n)
	if n != 0 {
		t.Fatal("tail failure wrote policy")
	}
	id, err := s.UpsertThread(ctx, Thread{RepoID: 1, Number: 30, Kind: "issue", GitHubID: "gh30", Title: "new", RawJSON: "{}", LabelsJSON: "[]", AssigneesJSON: "[]", UpdatedAt: "2026-01-02T00:00:00Z"})
	if err != nil || id <= 2 {
		t.Fatalf("native ID reused: %d %v", id, err)
	}
	if _, err = s.PlanThreadPurge(ctx, "fixture/repo", []int{20}); err == nil || !strings.Contains(err.Error(), "ID reuse") {
		t.Fatal("child tail admitted", err)
	}
	_, err = s.DB().Exec(`CREATE TRIGGER deny_fixture_delete BEFORE DELETE ON comments WHEN OLD.id=1 BEGIN SELECT RAISE(ABORT,'fixture interruption'); END`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.PurgeThreads(ctx, "fixture/repo", []int{10}, "rollback"); err == nil {
		t.Fatal("failure accepted")
	}
	for _, q := range []string{"SELECT count(*) FROM threads WHERE id=1", "SELECT count(*) FROM actor_identity_evidence WHERE node_id='node10'", "SELECT count(*) FROM analytics_retries WHERE number=10"} {
		s.DB().QueryRow(q).Scan(&n)
		if n != 1 {
			t.Fatal("partial purge", q, n)
		}
	}
	s.DB().QueryRow("SELECT count(*) FROM thread_exclusions").Scan(&n)
	if n != 0 {
		t.Fatal("rollback left exclusion")
	}
	for _, numbers := range [][]int{nil, {0}, {10, 10}, {999}} {
		if _, err = s.PlanThreadPurge(ctx, "fixture/repo", numbers); err == nil {
			t.Fatal("invalid selection accepted", numbers)
		}
	}
	if _, err = s.PurgeThreads(ctx, "fixture/repo", []int{10}, ""); err == nil {
		t.Fatal("missing owner request accepted")
	}
}

func TestOwnerExclusionV15MigrationIsAdditive(t *testing.T) {
	s := exclusionFixture(t)
	path := s.Path()
	_, err := s.DB().Exec(`DROP TABLE thread_excluded_nodes;DROP TABLE thread_exclusions;ALTER TABLE analytics_review_state_coverage DROP COLUMN owner_excluded_items;PRAGMA user_version=15;`)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.markObservationSchemaConverged(context.Background()); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, err = Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var version, count int
	s.DB().QueryRow("pragma user_version").Scan(&version)
	s.DB().QueryRow("select count(*) from comments").Scan(&count)
	if version != 16 || count != 2 {
		t.Fatalf("migration altered data: %d %d", version, count)
	}
}

func TestOwnerPurgeRefusesBlobBackedTargetsWithoutMutation(t *testing.T) {
	for _, mode := range []string{"shared", "unshared", "external", "tail"} {
		t.Run(mode, func(t *testing.T) {
			s := exclusionFixture(t)
			ctx := context.Background()
			_, err := s.DB().Exec(`INSERT INTO blobs(id,sha256,media_type,size_bytes,storage_kind,inline_text,created_at) VALUES
   (1,'one','application/json',20,'inline','removed-blob','2026-01-01'),
   (2,'two','application/json',20,'inline','peer-blob','2026-01-01');
   UPDATE comments SET raw_json_blob_id=1 WHERE id=1;`)
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "shared":
				_, err = s.DB().Exec("UPDATE comments SET raw_json_blob_id=1 WHERE id=2")
			case "external":
				_, err = s.DB().Exec("UPDATE blobs SET storage_kind='file',storage_path='/fixture/private' WHERE id=1")
			case "tail":
				_, err = s.DB().Exec("DELETE FROM blobs WHERE id=2")
			}
			if err != nil {
				t.Fatal(err)
			}
			result, err := s.PurgeThreads(ctx, "fixture/repo", []int{10}, "blob-fixture")
			var count, threads int
			s.DB().QueryRow("SELECT count(*) FROM blobs WHERE id=1").Scan(&count)
			s.DB().QueryRow("SELECT count(*) FROM threads WHERE id=1").Scan(&threads)
			if err == nil || result.Applied || !strings.Contains(err.Error(), "blob-backed") || count != 1 || threads != 1 {
				t.Fatalf("unsupported blob target was altered: %+v %v count=%d threads=%d", result, err, count, threads)
			}
			var policy int
			s.DB().QueryRow("SELECT count(*) FROM thread_exclusions").Scan(&policy)
			if policy != 0 {
				t.Fatal("refusal installed an exclusion")
			}

		})
	}
}

func TestOwnerPurgePortableMirrorKeepsSparseSchemaAndPeers(t *testing.T) {
	for _, sanitized := range []bool{true} {
		t.Run(fmt.Sprint(sanitized), func(t *testing.T) {
			s := exclusionFixture(t)
			ctx := context.Background()
			path := s.Path()
			_, err := s.PrunePortablePayloads(ctx, PortablePruneOptions{BodyChars: 32, RetainSanitizedPayloadColumns: sanitized})
			if err != nil {
				t.Fatal(err)
			}
			var before string
			if err = s.DB().QueryRow("SELECT title||body_excerpt||content_hash FROM threads WHERE id=2").Scan(&before); err != nil {
				t.Fatal(err)
			}
			s.Close()
			s, err = OpenThreadPurgeMirror(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			result, err := s.PurgeThreads(ctx, "fixture/repo", []int{10}, "mirror-fixture")
			if err != nil || !result.Applied {
				t.Fatalf("mirror purge: %+v %v", result, err)
			}
			var after string
			var version, n int
			s.DB().QueryRow("SELECT title||body_excerpt||content_hash FROM threads WHERE id=2").Scan(&after)
			s.DB().QueryRow("PRAGMA user_version").Scan(&version)
			s.DB().QueryRow("SELECT count(*) FROM comments WHERE thread_id=1").Scan(&n)
			if version != portableSchemaVersion || after != before || n != 0 || s.hasTable(ctx, "documents") {
				t.Fatalf("mirror migration or leak: %d %q %q %d", version, before, after, n)
			}
			if _, err = s.DB().Exec("UPDATE threads SET number=10 WHERE id=2"); err == nil {
				t.Fatal("mirror resurrection allowed")
			}
		})
	}
}

func TestOwnerPurgePublicationCannotDiscardPolicy(t *testing.T) {
	s := exclusionFixture(t)
	ctx := context.Background()
	if _, err := s.PurgeThreads(ctx, "fixture/repo", []int{10}, "private-owner-reference"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PrunePortablePayloads(ctx, PortablePruneOptions{BodyChars: 1}); err == nil || !strings.Contains(err.Error(), "local owner exclusions") {
		t.Fatal("private policy publication admitted", err)
	}
	var body string
	if err := s.DB().QueryRow("SELECT body FROM threads WHERE id=2").Scan(&body); err != nil || body != "UNCHANGED_BODY" {
		t.Fatal("refusal modified retained data", err)
	}
	if _, err := OpenThreadPurgeMirror(ctx, s.Path()); err == nil {
		t.Fatal("native archive accepted as portable mirror")
	}
}

func TestOwnerPurgeCanonicalRepositoryMatchesCaseVariantReceipts(t *testing.T) {
	s := exclusionFixture(t)
	ctx := context.Background()
	_, err := s.DB().Exec("UPDATE repositories SET full_name='Fixture/Repo';UPDATE analytics_fetch_attempts SET repository='Fixture/Repo' WHERE number=10;UPDATE analytics_retries SET repository='Fixture/Repo' WHERE number=10;UPDATE analytics_review_state_coverage SET repository='Fixture/Repo'")
	if err != nil {
		t.Fatal(err)
	}
	result, err := s.PurgeThreads(ctx, "FIXTURE/REPO", []int{10}, "case-fixture")
	if err != nil || result.Counts["analytics_fetch_attempts"] != 1 || result.Counts["analytics_retries"] != 1 {
		t.Fatalf("variant not planned: %+v %v", result, err)
	}
	for _, table := range []string{"analytics_fetch_attempts", "analytics_retries"} {
		var n int
		if err = s.DB().QueryRow("SELECT count(*) FROM " + table + " WHERE number=10").Scan(&n); err != nil || n != 0 {
			t.Fatal("variant retained", table, n, err)
		}
	}
	var complete, excluded int
	s.DB().QueryRow("SELECT complete,owner_excluded_items FROM analytics_review_state_coverage WHERE repository='Fixture/Repo'").Scan(&complete, &excluded)
	if complete != 0 || excluded != 1 {
		t.Fatal("variant coverage falsely complete", complete, excluded)
	}
	if err = s.RecordAnalyticsAttempt(ctx, AnalyticsAttempt{Repository: "Fixture/Repo", Number: 10, Operation: "review_state", StartedAt: "2026-01-01T00:00:00Z", FinishedAt: "2026-01-01T00:00:01Z", Status: "failed", ErrorClass: "partial_response", Evidence: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	var n int
	s.DB().QueryRow("SELECT count(*) FROM analytics_retries WHERE number=10").Scan(&n)
	if n != 0 {
		t.Fatal("variant requeued")
	}
}

func TestOwnerPurgeRefusesWorkflowEvidenceAndKeepsUnrelatedReservations(t *testing.T) {
	for _, mode := range []string{"run", "current-head", "historical-head", "unrelated"} {
		t.Run(mode, func(t *testing.T) {
			s := exclusionFixture(t)
			ctx := context.Background()
			var err error
			switch mode {
			case "run":
				_, err = s.DB().Exec("INSERT INTO github_workflow_runs(repo_id,run_id,head_sha,raw_json,fetched_at) VALUES(1,1,'older-sha','{}','2026-01-01')")
			case "current-head":
				_, err = s.DB().Exec("UPDATE pull_request_details SET head_sha='linked-sha' WHERE thread_id=1;INSERT INTO workflow_run_observation_reservations VALUES(1,'linked-sha','2026-01-01',1)")
			case "historical-head":
				_, err = s.DB().Exec(`UPDATE threads SET raw_json='{"node_id":"node10","_graphql":{"headRefOid":"older-sha"}}' WHERE id=1;INSERT INTO workflow_run_observation_reservations VALUES(1,'older-sha','2026-01-01',1)`)
			case "unrelated":
				_, err = s.DB().Exec("UPDATE pull_request_details SET head_sha='peer-sha' WHERE thread_id=2;INSERT INTO workflow_run_observation_reservations VALUES(1,'peer-sha','2026-01-01',1)")
			}
			if err != nil {
				t.Fatal(err)
			}
			result, err := s.PurgeThreads(ctx, "fixture/repo", []int{10}, "workflow-fixture")
			var n int
			s.DB().QueryRow("SELECT count(*) FROM threads WHERE id=1").Scan(&n)
			if mode == "unrelated" {
				if err != nil || !result.Applied || n != 0 {
					t.Fatalf("unrelated reservation blocked target: %+v %v", result, err)
				}
				s.DB().QueryRow("SELECT count(*) FROM workflow_run_observation_reservations WHERE head_sha='peer-sha'").Scan(&n)
				if n != 1 {
					t.Fatal("peer reservation lost")
				}
			} else {
				if err == nil || !strings.Contains(err.Error(), "workflow") || result.Applied || n != 1 {
					t.Fatalf("workflow evidence ignored: %+v %v count=%d", result, err, n)
				}
				s.DB().QueryRow("SELECT count(*) FROM thread_exclusions").Scan(&n)
				if n != 0 {
					t.Fatal("refusal wrote policy")
				}
			}
		})
	}
}
