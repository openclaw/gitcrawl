package store

import (
	"context"
	"path/filepath"
	"testing"
)

func TestUpsertDocumentIndexesFTS(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repoID, err := st.UpsertRepository(ctx, Repository{Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl", RawJSON: "{}", UpdatedAt: "2026-04-26T00:00:00Z"})
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	threadID, err := st.UpsertThread(ctx, Thread{
		RepoID: repoID, GitHubID: "1", Number: 1, Kind: "issue", State: "open",
		Title: "download stalls", HTMLURL: "https://github.com/openclaw/gitcrawl/issues/1",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "hash", UpdatedAt: "2026-04-26T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("thread: %v", err)
	}
	if _, err := st.UpsertDocument(ctx, Document{
		ThreadID:   threadID,
		Title:      "download stalls",
		RawText:    "download stalls on large artifacts",
		DedupeText: "download stalls large artifacts",
		UpdatedAt:  "2026-04-26T00:00:00Z",
	}); err != nil {
		t.Fatalf("document: %v", err)
	}

	var count int
	if err := st.DB().QueryRowContext(ctx, `select count(*) from documents_fts where documents_fts match 'artifacts'`).Scan(&count); err != nil {
		t.Fatalf("fts query: %v", err)
	}
	if count != 1 {
		t.Fatalf("fts count: got %d want 1", count)
	}
}
