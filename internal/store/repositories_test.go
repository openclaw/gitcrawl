package store

import (
	"context"
	"path/filepath"
	"testing"
)

func TestUpsertRepositoryAndThread(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repoID, err := st.UpsertRepository(ctx, Repository{
		Owner:        "openclaw",
		Name:         "gitcrawl",
		FullName:     "openclaw/gitcrawl",
		GitHubRepoID: "123",
		RawJSON:      "{}",
		UpdatedAt:    "2026-04-26T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("upsert repo: %v", err)
	}
	threadID, err := st.UpsertThread(ctx, Thread{
		RepoID:        repoID,
		GitHubID:      "456",
		Number:        1,
		Kind:          "issue",
		State:         "open",
		Title:         "download stalls",
		HTMLURL:       "https://github.com/openclaw/gitcrawl/issues/1",
		LabelsJSON:    "[]",
		AssigneesJSON: "[]",
		RawJSON:       "{}",
		ContentHash:   "hash",
		UpdatedAt:     "2026-04-26T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("upsert thread: %v", err)
	}
	if threadID == 0 {
		t.Fatal("expected thread id")
	}

	rows, err := st.ListThreads(ctx, repoID, false)
	if err != nil {
		t.Fatalf("list threads: %v", err)
	}
	if len(rows) != 1 || rows[0].Title != "download stalls" {
		t.Fatalf("unexpected rows: %#v", rows)
	}
}
