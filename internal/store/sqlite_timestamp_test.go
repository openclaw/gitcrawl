package store

import (
	"context"
	"path/filepath"
	"testing"
)

func TestTimestampOrderKeyPreservesRFC3339NanoPrecision(t *testing.T) {
	earlier, earlierOK := timestampOrderKey("2026-07-12T00:00:00.000000001Z")
	later, laterOK := timestampOrderKey("2026-07-12T00:00:00.000000002Z")
	if !earlierOK || !laterOK || earlier >= later {
		t.Fatalf("timestamp keys out of order: %q >= %q", earlier, later)
	}
	utc, utcOK := timestampOrderKey("2026-07-12T00:00:00Z")
	offset, offsetOK := timestampOrderKey("2026-07-12T02:00:00+02:00")
	if !utcOK || !offsetOK || utc != offset {
		t.Fatalf("equivalent instants have different keys: %q != %q", utc, offset)
	}
	if _, ok := timestampOrderKey("invalid"); ok {
		t.Fatal("invalid timestamp produced an ordering key")
	}
}

func TestSQLiteTimestampKeyOrdersSubMillisecondRevisions(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repoID, err := st.UpsertRepository(ctx, Repository{
		Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl",
		RawJSON: "{}", UpdatedAt: "2026-07-12T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("repository: %v", err)
	}
	thread := Thread{
		RepoID: repoID, GitHubID: "10", Number: 10, Kind: "issue", State: "open",
		Title: "newer", HTMLURL: "https://github.com/openclaw/gitcrawl/issues/10",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "thread",
		UpdatedAtGitHub: "2026-07-12T00:00:00.000000002Z",
		UpdatedAt:       "2026-07-12T00:00:00.000000002Z",
	}
	thread.ID, err = st.UpsertThread(ctx, thread)
	if err != nil {
		t.Fatalf("thread: %v", err)
	}
	newer, err := st.UpsertThreadRevisionAndFingerprint(ctx, ThreadEvidence{Thread: thread}, "2026-07-12T00:00:01Z")
	if err != nil {
		t.Fatalf("newer revision: %v", err)
	}

	thread.Title = "stale"
	thread.UpdatedAtGitHub = "2026-07-12T00:00:00.000000001Z"
	thread.UpdatedAt = thread.UpdatedAtGitHub
	stale, err := st.UpsertThreadRevisionAndFingerprint(ctx, ThreadEvidence{Thread: thread}, "2026-07-12T00:00:02Z")
	if err != nil {
		t.Fatalf("stale revision: %v", err)
	}

	var latestID int64
	if err := st.DB().QueryRowContext(ctx, `
		select id
		from thread_revisions
		where thread_id = ?
		order by gitcrawl_timestamp_key(coalesce(nullif(source_updated_at, ''), created_at)) desc,
			observation_sequence desc,
			id desc
		limit 1
	`, thread.ID).Scan(&latestID); err != nil {
		t.Fatalf("latest revision: %v", err)
	}
	if latestID != newer.RevisionID || latestID == stale.RevisionID {
		t.Fatalf("latest revision = %d, newer = %d, stale = %d", latestID, newer.RevisionID, stale.RevisionID)
	}
}
