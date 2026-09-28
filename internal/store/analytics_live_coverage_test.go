package store

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
)

func TestAnalyticsReviewCoverageTracksLaterAttempts(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "source.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	_, err = st.DB().Exec(`INSERT INTO repositories(id,owner,name,full_name,raw_json,updated_at) VALUES(1,'fixture','repo','fixture/repo','{}','2026-01-01');
 INSERT INTO threads(id,repo_id,github_id,number,kind,state,title,html_url,labels_json,assignees_json,raw_json,content_hash,updated_at) VALUES(1,1,'PR_fixture',1,'pull_request','open','','','[]','[]','{}','h','2026-01-01')`)
	if err != nil {
		t.Fatal(err)
	}
	if err = st.SaveReviewStateCoverage(ctx, "fixture/repo", ReviewStateRecovery{Done: true}); err != nil {
		t.Fatal(err)
	}
	check := func(pending, complete int) {
		t.Helper()
		var gotPending, gotComplete int
		if err := st.DB().QueryRow("SELECT pending_items,complete FROM analytics_review_state_coverage WHERE repository='fixture/repo'").Scan(&gotPending, &gotComplete); err != nil {
			t.Fatal(err)
		}
		if gotPending != pending || gotComplete != complete {
			t.Fatalf("coverage=%d,%d want %d,%d", gotPending, gotComplete, pending, complete)
		}
	}
	check(0, 1)
	a := AnalyticsAttempt{Repository: "fixture/repo", Number: 1, Operation: "review_state", StartedAt: "2026-01-01T00:00:00Z", FinishedAt: "2026-01-01T00:00:01Z", Status: "failed", Evidence: json.RawMessage(`{}`)}
	if err = st.RecordAnalyticsAttempt(ctx, a); err != nil {
		t.Fatal(err)
	}
	check(1, 0)
	if err = st.UpsertPullRequestReviewThreads(ctx, 1, "2026-01-01T00:01:00Z", nil); err != nil {
		t.Fatal(err)
	}
	a.Status = "success"
	a.Operation = "graphql_history"
	a.StartedAt = "2026-01-01T00:01:00Z"
	a.FinishedAt = "2026-01-01T00:01:01Z"
	if err = st.RecordAnalyticsAttempt(ctx, a); err != nil {
		t.Fatal(err)
	}
	check(0, 1)
	if err = st.SaveReviewStateCoverage(ctx, "fixture/repo", ReviewStateRecovery{Done: false}); err != nil {
		t.Fatal(err)
	}
	check(0, 0)
}

func TestAnalyticsRecoveryDoesNotQueueMembershipPublishedAfterScan(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "source.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	_, err = st.DB().Exec(`INSERT INTO repositories(id,owner,name,full_name,raw_json,updated_at) VALUES(1,'fixture','repo','fixture/repo','{}','2026-01-01');
 INSERT INTO threads(id,repo_id,github_id,number,kind,state,title,html_url,labels_json,assignees_json,raw_json,content_hash,updated_at) VALUES(1,1,'PR_fixture',1,'pull_request','open','','','[]','[]','{}','h','2026-01-01')`)
	if err != nil {
		t.Fatal(err)
	}
	// The scan had no membership. A later complete empty observation is still
	// authoritative when that old scan reaches its transaction's enqueue step.
	if err = st.UpsertPullRequestReviewThreads(ctx, 1, "2026-01-01T00:01:00Z", nil); err != nil {
		t.Fatal(err)
	}
	if err = st.WithTx(ctx, func(tx *Store) error {
		_, e := tx.queueReviewStateRecovery(ctx, "fixture/repo", 1, 1, "2026-01-01T00:00:00Z")
		return e
	}); err != nil {
		t.Fatal(err)
	}
	queued, err := st.AnalyticsItemQueued(ctx, "fixture/repo", 1, "review_state")
	if err != nil || queued {
		t.Fatalf("stale scan queued accepted membership: %t %v", queued, err)
	}
}
