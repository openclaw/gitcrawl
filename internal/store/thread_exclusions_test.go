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
INSERT INTO thread_revisions(id,thread_id,content_hash,title_hash,body_hash,labels_hash,created_at) VALUES
 (1,1,'remove','t','b','l','2026-01-01'),(2,2,'keep','t','b','l','2026-01-01');
INSERT INTO thread_key_summaries(id,thread_revision_id,summary_kind,prompt_version,provider,model,input_hash,output_hash,key_text,created_at) VALUES
 (1,1,'key','v1','fixture','fixture','i','o','removed summary','2026-01-01'),(2,2,'key','v1','fixture','fixture','i','o','kept summary','2026-01-01');
INSERT INTO sync_attempt_failures(id,repo_id,thread_id,number,operation,error_class,error_message,first_seen_at,last_seen_at) VALUES
 (1,1,1,10,'issue','fixture','removed error','2026-01-01','2026-01-01'),(2,1,2,20,'issue','fixture','kept error','2026-01-01','2026-01-01');
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
	result, err := applyPurge(t, s, ctx, "fixture/repo", []int{10}, "owner-request-fixture")
	if err != nil || !result.Applied {
		t.Fatalf("%+v %v", result, err)
	}
	for _, table := range []string{"threads", "comments", "comment_revisions", "documents", "pull_request_details", "thread_revisions", "thread_key_summaries", "sync_attempt_failures"} {
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
	if _, err := s.PlanThreadPurge(ctx, "fixture/repo", []int{10}); err == nil {
		t.Fatal("removed target should require a new selection")
	}
	var original string
	if err := s.DB().QueryRow("SELECT plan_id FROM thread_exclusions").Scan(&original); err != nil || original != plan.PlanID {
		t.Fatal("plan identity not retained", err)
	}

}

func TestOwnerPurgeRejectsTailAndRollsBack(t *testing.T) {
	s := exclusionFixture(t)
	ctx := context.Background()
	if _, err := applyPurge(t, s, ctx, "fixture/repo", []int{20}, "tail"); err == nil || !strings.Contains(err.Error(), "ID reuse") {
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
	if _, err = applyPurge(t, s, ctx, "fixture/repo", []int{10}, "rollback"); err == nil {
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
			result, err := applyPurge(t, s, ctx, "fixture/repo", []int{10}, "blob-fixture")
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

func TestOwnerPurgePublicationCannotDiscardPolicy(t *testing.T) {
	s := exclusionFixture(t)
	ctx := context.Background()
	if _, err := applyPurge(t, s, ctx, "fixture/repo", []int{10}, "private-owner-reference"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PrunePortablePayloads(ctx, PortablePruneOptions{BodyChars: 1}); err == nil || !strings.Contains(err.Error(), "local owner exclusions") {
		t.Fatal("private policy publication admitted", err)
	}
	var body string
	if err := s.DB().QueryRow("SELECT body FROM threads WHERE id=2").Scan(&body); err != nil || body != "UNCHANGED_BODY" {
		t.Fatal("refusal modified retained data", err)
	}
}

func TestOwnerPurgeCanonicalRepositoryMatchesCaseVariantReceipts(t *testing.T) {
	s := exclusionFixture(t)
	ctx := context.Background()
	_, err := s.DB().Exec("UPDATE repositories SET full_name='Fixture/Repo';UPDATE analytics_fetch_attempts SET repository='Fixture/Repo' WHERE number=10;UPDATE analytics_retries SET repository='Fixture/Repo' WHERE number=10;UPDATE analytics_review_state_coverage SET repository='Fixture/Repo'")
	if err != nil {
		t.Fatal(err)
	}
	result, err := applyPurge(t, s, ctx, "FIXTURE/REPO", []int{10}, "case-fixture")
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
			result, err := applyPurge(t, s, ctx, "fixture/repo", []int{10}, "workflow-fixture")
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

func applyPurge(t *testing.T, s *Store, ctx context.Context, repository string, numbers []int, _ string) (ThreadPurgePlan, error) {
	t.Helper()
	plan, err := s.PlanThreadPurge(ctx, repository, numbers)
	if err != nil {
		return plan, err
	}
	return s.PurgeThreads(ctx, repository, numbers, plan.PlanID)
}

func TestOwnerPurgePlanIdentityRejectsStaleContent(t *testing.T) {
	s := exclusionFixture(t)
	ctx := context.Background()
	plan, err := s.PlanThreadPurge(ctx, "fixture/repo", []int{10})
	if err != nil {
		t.Fatal(err)
	}
	again, err := s.PlanThreadPurge(ctx, "FIXTURE/REPO", []int{10})
	if err != nil || plan.PlanID != again.PlanID || len(plan.PlanID) != 64 {
		t.Fatal("unstable plan", err)
	}
	if _, err = s.DB().Exec("UPDATE comments SET body='changed after preview' WHERE id=1"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.PurgeThreads(ctx, "fixture/repo", []int{10}, plan.PlanID); err == nil || !strings.Contains(err.Error(), "plan changed") {
		t.Fatal("stale plan accepted", err)
	}
	var n int
	if err = s.DB().QueryRow("SELECT count(*) FROM thread_exclusions").Scan(&n); err != nil || n != 0 {
		t.Fatal("stale apply changed policy", err)
	}
	fresh, err := s.PlanThreadPurge(ctx, "fixture/repo", []int{10})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.DB().Exec("CREATE TRIGGER fail_mid_purge BEFORE DELETE ON comments BEGIN SELECT RAISE(ABORT,'injected mid-apply failure'); END"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.PurgeThreads(ctx, "fixture/repo", []int{10}, fresh.PlanID); err == nil {
		t.Fatal("failure injection ignored")
	}
	after, err := s.PlanThreadPurge(ctx, "fixture/repo", []int{10})
	if err != nil || after.PlanID != fresh.PlanID {
		t.Fatal("rollback changed selected rows", err)
	}
	if err = s.DB().QueryRow("SELECT count(*) FROM thread_exclusions").Scan(&n); err != nil || n != 0 {
		t.Fatal("rollback changed policy", err)
	}
}

func TestOwnerPurgeRefusesAmbiguousAndSharedTargets(t *testing.T) {
	for _, mutation := range []string{
		"UPDATE comments SET raw_json='{\"node_id\":\"node10\"}' WHERE id=2",
		"INSERT INTO repositories(owner,name,full_name,raw_json,updated_at) VALUES('Fixture','Repo','Fixture/Repo','{}','now')",
		"INSERT INTO threads(repo_id,github_id,number,kind,state,title,html_url,labels_json,assignees_json,raw_json,content_hash,updated_at) VALUES(1,'other',10,'issue','open','','','[]','[]','{}','h','now')",
		"INSERT INTO cluster_groups(repo_id,stable_key,stable_slug,status,representative_thread_id,created_at,updated_at) VALUES(1,'k','s','open',1,'now','now')",
		"CREATE TABLE portable_metadata(fixture TEXT)",
	} {
		t.Run(mutation, func(t *testing.T) {
			s := exclusionFixture(t)
			if _, err := s.DB().Exec(mutation); err != nil {
				t.Fatal(err)
			}
			if _, err := s.PlanThreadPurge(context.Background(), "fixture/repo", []int{10}); err == nil {
				t.Fatal("unsafe selection accepted")
			}
		})
	}
}

func TestOwnerPurgeMultipleTargetsAreOneTransaction(t *testing.T) {
	s := exclusionFixture(t)
	ctx := context.Background()
	for _, n := range []int{30, 40} {
		if _, err := s.UpsertThread(ctx, Thread{RepoID: 1, Number: n, GitHubID: fmt.Sprint(n), Kind: "issue", Title: "synthetic", RawJSON: "{}", LabelsJSON: "[]", AssigneesJSON: "[]", UpdatedAt: "2026-01-01T00:00:00Z"}); err != nil {
			t.Fatal(err)
		}
	}
	plan, err := s.PlanThreadPurge(ctx, "fixture/repo", []int{30, 10})
	if err != nil {
		t.Fatal(err)
	}
	ordered, err := s.PlanThreadPurge(ctx, "fixture/repo", []int{10, 30})
	if err != nil || ordered.PlanID != plan.PlanID || plan.Counts["threads"] != 2 || plan.Targets[0].Number != 10 || plan.Targets[1].Number != 30 {
		t.Fatal("selection or order changed plan", err)
	}
	if _, err = s.DB().Exec("CREATE TRIGGER fail_second_target BEFORE DELETE ON threads WHEN OLD.number=30 BEGIN SELECT RAISE(ABORT,'second target failed'); END"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.PurgeThreads(ctx, "fixture/repo", []int{10, 30}, plan.PlanID); err == nil {
		t.Fatal("second-target failure ignored")
	}
	after, err := s.PlanThreadPurge(ctx, "fixture/repo", []int{10, 30})
	if err != nil || after.PlanID != plan.PlanID {
		t.Fatal("transaction changed selected data", err)
	}
	if _, err = s.DB().Exec("DROP TRIGGER fail_second_target"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.PurgeThreads(ctx, "fixture/repo", []int{10, 30}, plan.PlanID); err != nil {
		t.Fatal(err)
	}
	var kept string
	if err = s.DB().QueryRow("SELECT group_concat(number,',') FROM (SELECT number FROM threads ORDER BY number)").Scan(&kept); err != nil || kept != "20,40" {
		t.Fatal("wrong retained set", kept, err)
	}
}

func TestOwnerPurgePreviewBeforeOptionalAnalyticsMigration(t *testing.T) {
	s := exclusionFixture(t)
	ctx := context.Background()
	path := s.Path()
	if _, err := s.DB().Exec(`DROP TABLE analytics_fetch_attempts;DROP TABLE analytics_retries;DROP TABLE actor_identity_evidence;DROP TABLE analytics_pending_nodes;PRAGMA user_version=14`); err != nil {
		t.Fatal(err)
	}
	s.Close()
	readonly, err := OpenReadOnly(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := readonly.PlanThreadPurge(ctx, "fixture/repo", []int{10})
	if err != nil {
		t.Fatal(err)
	}
	if readonly.hasTable(ctx, "analytics_retries") || plan.Counts["analytics_retries"] != 0 {
		t.Fatal("preview migrated archive")
	}
	readonly.Close()
	writable, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer writable.Close()
	if _, err := writable.PurgeThreads(ctx, "fixture/repo", []int{10}, plan.PlanID); err != nil {
		t.Fatal("empty optional tables changed selection", err)
	}
}
