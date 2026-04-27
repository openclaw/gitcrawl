package store

import (
	"context"
	"path/filepath"
	"testing"
)

func TestCloseThreadLocallyHidesThreadAndSurvivesUpsert(t *testing.T) {
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
		UpdatedAt: "2026-04-27T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("upsert repo: %v", err)
	}
	thread := Thread{
		RepoID:        repoID,
		GitHubID:      "42",
		Number:        42,
		Kind:          "issue",
		State:         "open",
		Title:         "Noisy duplicate",
		HTMLURL:       "https://github.com/openclaw/gitcrawl/issues/42",
		LabelsJSON:    "[]",
		AssigneesJSON: "[]",
		RawJSON:       "{}",
		ContentHash:   "hash-a",
		UpdatedAt:     "2026-04-27T00:00:00Z",
	}
	if _, err := st.UpsertThread(ctx, thread); err != nil {
		t.Fatalf("upsert thread: %v", err)
	}

	if err := st.CloseThreadLocally(ctx, repoID, 42, "TUI manual close"); err != nil {
		t.Fatalf("close thread locally: %v", err)
	}
	openRows, err := st.ListThreads(ctx, repoID, false)
	if err != nil {
		t.Fatalf("list open threads: %v", err)
	}
	if len(openRows) != 0 {
		t.Fatalf("locally closed thread should be hidden, got %#v", openRows)
	}
	closedRows, err := st.ListThreads(ctx, repoID, true)
	if err != nil {
		t.Fatalf("list closed threads: %v", err)
	}
	if len(closedRows) != 1 || closedRows[0].ClosedAtLocal == "" || closedRows[0].CloseReasonLocal != "TUI manual close" {
		t.Fatalf("closed thread not marked correctly: %#v", closedRows)
	}

	thread.Title = "Noisy duplicate updated upstream"
	thread.ContentHash = "hash-b"
	if _, err := st.UpsertThread(ctx, thread); err != nil {
		t.Fatalf("upsert closed thread: %v", err)
	}
	openRows, err = st.ListThreads(ctx, repoID, false)
	if err != nil {
		t.Fatalf("list open threads after upsert: %v", err)
	}
	if len(openRows) != 0 {
		t.Fatalf("upsert should preserve local close, got %#v", openRows)
	}
}

func TestCloseThreadLocallyRequiresExistingThread(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if err := st.CloseThreadLocally(ctx, 1, 404, "missing"); err == nil {
		t.Fatal("expected missing thread error")
	}
}
