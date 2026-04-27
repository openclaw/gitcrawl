package store

import (
	"context"
	"path/filepath"
	"testing"
)

func TestRecordAndListRuns(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repoID, err := st.UpsertRepository(ctx, Repository{
		Owner:     "openclaw",
		Name:      "gitcrawl",
		FullName:  "openclaw/gitcrawl",
		RawJSON:   "{}",
		UpdatedAt: "2026-04-26T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	if _, err := st.RecordRun(ctx, RunRecord{
		RepoID:     repoID,
		Kind:       "sync",
		Scope:      "open",
		Status:     "success",
		StartedAt:  "2026-04-26T00:00:00Z",
		FinishedAt: "2026-04-26T00:00:01Z",
		StatsJSON:  `{"threads_synced":1}`,
	}); err != nil {
		t.Fatalf("record run: %v", err)
	}

	runs, err := st.ListRuns(ctx, repoID, "sync", 10)
	if err != nil {
		t.Fatalf("list runs: %v", err)
	}
	if len(runs) != 1 || runs[0].Kind != "sync" || runs[0].Status != "success" {
		t.Fatalf("unexpected runs: %#v", runs)
	}
}

func TestStatusAcceptsCompletedSyncRuns(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repoID, err := st.UpsertRepository(ctx, Repository{
		Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl", RawJSON: "{}", UpdatedAt: "2026-04-26T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	if _, err := st.RecordRun(ctx, RunRecord{
		RepoID: repoID, Kind: "sync", Scope: "open", Status: "completed",
		StartedAt: "2026-04-26T00:00:00Z", FinishedAt: "2026-04-26T00:00:01Z",
	}); err != nil {
		t.Fatalf("record run: %v", err)
	}
	status, err := st.Status(ctx)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.LastSyncAt.IsZero() {
		t.Fatalf("expected last sync time, got %#v", status)
	}
}
