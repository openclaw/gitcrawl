package store

import (
	"context"
	"path/filepath"
	"testing"
)

func TestSearchDocuments(t *testing.T) {
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
	if _, err := st.UpsertDocument(ctx, Document{ThreadID: threadID, Title: "download stalls", RawText: "artifact download stalls", DedupeText: "artifact download stalls", UpdatedAt: "2026-04-26T00:00:00Z"}); err != nil {
		t.Fatalf("document: %v", err)
	}

	hits, err := st.SearchDocuments(ctx, repoID, "artifact", 10)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(hits) != 1 || hits[0].Number != 1 {
		t.Fatalf("unexpected hits: %#v", hits)
	}
}
