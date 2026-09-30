package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
)

func recoveryContractStore(t *testing.T) (*Store, int64) {
	t.Helper()
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	repoID, err := s.UpsertRepository(ctx, Repository{Owner: "fixture", Name: "repo", FullName: "fixture/repo", GitHubRepoID: "101", RawJSON: `{"node_id":"R1"}`, UpdatedAt: "2026-01-01T00:00:00Z"})
	if err != nil {
		t.Fatal(err)
	}
	id, err := s.UpsertThread(ctx, Thread{RepoID: repoID, GitHubID: "201", Number: 7, Kind: "pull_request", State: "open", Title: "retained", HTMLURL: "https://github.com/fixture/repo/pull/7", LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: `{"node_id":"P7"}`, ContentHash: "retained-hash", UpdatedAt: "2026-01-01T00:00:00Z"})
	if err != nil {
		t.Fatal(err)
	}
	return s, id
}

func TestReviewStateParentBindsIdentityInOwningTransaction(t *testing.T) {
	ctx := context.Background()
	s, id := recoveryContractStore(t)
	parent, err := s.ReviewStateParent(ctx, "FIXTURE/REPO", 7, "101", "R1")
	if err != nil || parent.ID != id || parent.GitHubID != "201" || parent.RawJSON != `{"node_id":"P7"}` {
		t.Fatalf("parent=%+v err=%v", parent, err)
	}
	for _, tc := range []struct {
		repo           string
		number         int
		database, node string
	}{
		{"fixture/other", 7, "101", "R1"}, {"fixture/repo", 8, "101", "R1"}, {"fixture/repo", 7, "102", "R1"}, {"fixture/repo", 7, "101", "R2"},
	} {
		if _, err := s.ReviewStateParent(ctx, tc.repo, tc.number, tc.database, tc.node); err == nil {
			t.Fatalf("mismatched identity accepted: %+v", tc)
		}
	}
	rolledBack := errors.New("fixture rollback")
	err = s.WithTx(ctx, func(tx *Store) error {
		if _, err := tx.q().ExecContext(ctx, `UPDATE repositories SET raw_json='{"node_id":"R-new"}' WHERE id=?`, parent.RepoID); err != nil {
			return err
		}
		if _, err := tx.ReviewStateParent(ctx, "fixture/repo", 7, "101", "R1"); err == nil {
			t.Fatal("read stale identity outside owning transaction")
		}
		got, err := tx.ReviewStateParent(ctx, "fixture/repo", 7, "101", "R-new")
		if err != nil || got.ID != id {
			t.Fatalf("transaction-local binding failed: %+v %v", got, err)
		}
		return rolledBack
	})
	if !errors.Is(err, rolledBack) {
		t.Fatal(err)
	}
	if _, err = s.ReviewStateParent(ctx, "fixture/repo", 7, "101", "R1"); err != nil {
		t.Fatal("identity mutation escaped rollback", err)
	}
	if _, err = s.DB().ExecContext(ctx, "UPDATE threads SET kind='issue' WHERE id=?", id); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ReviewStateParent(ctx, "fixture/repo", 7, "101", "R1"); err == nil {
		t.Fatal("issue accepted as review-state parent")
	}
}

func TestReviewRecoveryCannotDischargeCoreRetryOrCertifyCoreCoverage(t *testing.T) {
	ctx := context.Background()
	s, id := recoveryContractStore(t)
	through := "2026-01-01T00:00:00Z"
	if err := s.SaveAnalyticsCoverage(ctx, "fixture/repo", through, 2, 1); err != nil {
		t.Fatal(err)
	}
	a := AnalyticsAttempt{Repository: "fixture/repo", Number: 7, Operation: "review_state", StartedAt: through, FinishedAt: "2026-01-01T00:00:01Z", Status: "failed", ErrorClass: "validation", Evidence: json.RawMessage(`{}`)}
	record := func() {
		t.Helper()
		if err := s.RecordAnalyticsAttempt(ctx, a); err != nil {
			t.Fatal(err)
		}
	}
	queued := func(want bool, operations ...string) {
		t.Helper()
		got, err := s.AnalyticsItemQueued(ctx, a.Repository, a.Number, operations...)
		if err != nil || got != want {
			t.Fatalf("queued(%v)=%v want%v err%v", operations, got, want, err)
		}
	}
	coverage := func(want int) {
		t.Helper()
		var complete int
		var watermark string
		if err := s.DB().QueryRowContext(ctx, "SELECT complete,through FROM analytics_coverage WHERE repository=?", a.Repository).Scan(&complete, &watermark); err != nil || complete != want || watermark != through {
			t.Fatalf("coverage=%d through=%s err=%v", complete, watermark, err)
		}
	}
	record()
	queued(false)
	queued(true, "review_state")
	coverage(1)
	a.Operation = "graphql_history"
	record()
	queued(true)
	coverage(0)
	if err := s.UpsertPullRequestReviewThreads(ctx, id, a.FinishedAt, nil); err != nil {
		t.Fatal(err)
	}
	a.Operation = "review_state"
	a.Status = "success"
	record()
	queued(false, "review_state")
	queued(true)
	coverage(0)
	a.Operation = "graphql_history"
	record()
	queued(false)
	coverage(0)
	// A successful item is not itself a completed traversal watermark.
	if err := s.SetAnalyticsCoverageComplete(ctx, a.Repository, true); err != nil {
		t.Fatal(err)
	}
	coverage(1)
	a.Status = "failed"
	a.FinishedAt = "2026-01-01T00:01:00Z"
	record()
	queued(true)
	coverage(0)
	var receipts int
	if err := s.DB().QueryRowContext(ctx, "SELECT count(*) FROM analytics_fetch_attempts WHERE repository=?", a.Repository).Scan(&receipts); err != nil || receipts != 5 {
		t.Fatalf("lost recovery history: %d %v", receipts, err)
	}
	var resolved sql.NullString
	if err := s.DB().QueryRowContext(ctx, "SELECT resolved_at FROM analytics_retries WHERE repository=? AND operation='review_state'", a.Repository).Scan(&resolved); err != nil || !resolved.Valid {
		t.Fatalf("independent review resolution lost: %v %v", resolved, err)
	}
}
