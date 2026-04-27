package store

import (
	"context"
	"path/filepath"
	"testing"
)

func TestUpsertAndListThreadVectors(t *testing.T) {
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
	if err := st.UpsertThreadVector(ctx, ThreadVector{
		ThreadID: threadID, Basis: "title_original", Model: "test", Dimensions: 3,
		ContentHash: "hash", Vector: []float64{1, 0, 0}, CreatedAt: "2026-04-26T00:00:00Z", UpdatedAt: "2026-04-26T00:00:00Z",
	}); err != nil {
		t.Fatalf("vector: %v", err)
	}

	vectors, err := st.ListThreadVectors(ctx, repoID)
	if err != nil {
		t.Fatalf("list vectors: %v", err)
	}
	if len(vectors) != 1 || vectors[0].Vector[0] != 1 {
		t.Fatalf("unexpected vectors: %#v", vectors)
	}
}
