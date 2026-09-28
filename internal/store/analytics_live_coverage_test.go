package store

import (
	"context"
	"path/filepath"
	"testing"
)

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
