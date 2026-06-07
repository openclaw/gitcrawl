package store

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
)

func TestReplaceAndSearchCodeSnapshot(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	repoID, err := st.UpsertRepository(ctx, Repository{Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl", RawJSON: "{}", UpdatedAt: "2026-06-06T00:00:00Z"})
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	snapshotID, err := st.ReplaceCodeSnapshot(ctx, CodeSnapshot{
		RepoID: repoID, SourceRoot: "/tmp/gitcrawl", GitSHA: "abc", WorktreeDirty: true,
		FileCount: 2, ByteCount: 40, IndexedAt: "2026-06-06T00:00:00Z",
	}, []CodeDocument{
		{Path: "internal/cache/store.go", Language: "go", ContentHash: "h1", Text: "package cache\nfunc RefreshManifest() {}", ByteSize: 39, UpdatedAt: "2026-06-06T00:00:00Z"},
		{Path: "README.md", Language: "md", ContentHash: "h2", Text: "local archive", ByteSize: 13, UpdatedAt: "2026-06-06T00:00:00Z"},
	})
	if err != nil {
		t.Fatalf("replace snapshot: %v", err)
	}
	hits, err := st.SearchCodeDocuments(ctx, repoID, "RefreshManifest", 10)
	if err != nil {
		t.Fatalf("search code: %v", err)
	}
	if len(hits) != 1 || hits[0].Path != "internal/cache/store.go" || hits[0].GitSHA != "abc" || !hits[0].WorktreeDirty {
		t.Fatalf("hits = %#v", hits)
	}

	replacementID, err := st.ReplaceCodeSnapshot(ctx, CodeSnapshot{
		RepoID: repoID, SourceRoot: "/tmp/gitcrawl", GitSHA: "abc",
		FileCount: 1, ByteCount: 12, IndexedAt: "2026-06-06T01:00:00Z",
	}, []CodeDocument{{Path: "new.go", Language: "go", ContentHash: "h3", Text: "package fresh", ByteSize: 13, UpdatedAt: "2026-06-06T01:00:00Z"}})
	if err != nil {
		t.Fatalf("replace newer snapshot: %v", err)
	}
	if replacementID == snapshotID {
		t.Fatal("same-commit re-index reused old snapshot id")
	}
	oldHits, err := st.SearchCodeDocuments(ctx, repoID, "RefreshManifest", 10)
	if err != nil {
		t.Fatalf("search old code: %v", err)
	}
	if len(oldHits) != 0 {
		t.Fatalf("old snapshot still searchable: %#v", oldHits)
	}
	var snapshots int
	if err := st.DB().QueryRowContext(ctx, `select count(*) from code_snapshots where repo_id = ?`, repoID).Scan(&snapshots); err != nil {
		t.Fatalf("count snapshots: %v", err)
	}
	if snapshots != 1 {
		t.Fatalf("snapshot count = %d, want 1", snapshots)
	}

	if _, err := st.ReplaceCodeSnapshot(ctx, CodeSnapshot{
		RepoID: repoID, SourceRoot: "/tmp/gitcrawl", GitSHA: "abc",
		FileCount: 2, ByteCount: 20, IndexedAt: "2026-06-06T02:00:00Z",
	}, []CodeDocument{
		{Path: "duplicate.go", Language: "go", ContentHash: "h4", Text: "package duplicate", ByteSize: 17, UpdatedAt: "2026-06-06T02:00:00Z"},
		{Path: "duplicate.go", Language: "go", ContentHash: "h5", Text: "package broken", ByteSize: 14, UpdatedAt: "2026-06-06T02:00:00Z"},
	}); err == nil {
		t.Fatal("invalid replacement unexpectedly succeeded")
	}
	hits, err = st.SearchCodeDocuments(ctx, repoID, "package fresh", 10)
	if err != nil {
		t.Fatalf("search after failed replacement: %v", err)
	}
	if len(hits) != 1 || hits[0].Path != "new.go" {
		t.Fatalf("failed replacement discarded prior snapshot: %#v", hits)
	}
}

func TestPortablePruneDropsCodeCorpus(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	repoID, err := st.UpsertRepository(ctx, Repository{Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl", RawJSON: "{}", UpdatedAt: "2026-06-06T00:00:00Z"})
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	if _, err := st.ReplaceCodeSnapshot(ctx, CodeSnapshot{
		RepoID: repoID, SourceRoot: "/private/repo", GitSHA: "abc",
		FileCount: 1, ByteCount: 6, IndexedAt: "2026-06-06T00:00:00Z",
	}, []CodeDocument{{Path: "secret.txt", Language: "txt", ContentHash: "h", Text: "secret", ByteSize: 6, UpdatedAt: "2026-06-06T00:00:00Z"}}); err != nil {
		t.Fatalf("replace snapshot: %v", err)
	}
	if _, err := st.PrunePortablePayloads(ctx, PortablePruneOptions{BodyChars: 64}); err != nil {
		t.Fatalf("portable prune: %v", err)
	}
	for _, table := range []string{"code_snapshots", "code_documents", "code_documents_fts"} {
		if st.tableExists(ctx, table) {
			t.Fatalf("portable prune retained %s", table)
		}
	}
	hits, err := st.SearchCodeDocuments(ctx, repoID, "secret", 10)
	if err != nil {
		t.Fatalf("search pruned code corpus: %v", err)
	}
	if len(hits) != 0 {
		t.Fatalf("pruned code hits = %#v", hits)
	}
	if _, err := st.LatestCodeSnapshot(ctx, repoID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("latest pruned snapshot error = %v", err)
	}
}

func TestSearchCodeDocumentsHandlesEmptyStates(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	repoID, err := st.UpsertRepository(ctx, Repository{Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl", RawJSON: "{}", UpdatedAt: "2026-06-06T00:00:00Z"})
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	hits, err := st.SearchCodeDocuments(ctx, repoID, "missing", 0)
	if err != nil {
		t.Fatalf("search unindexed code: %v", err)
	}
	if len(hits) != 0 {
		t.Fatalf("unindexed code hits = %#v", hits)
	}
	if _, err := st.ReplaceCodeSnapshot(ctx, CodeSnapshot{
		RepoID: repoID, SourceRoot: "/tmp/gitcrawl", GitSHA: "abc",
		FileCount: 1, ByteCount: 12, IndexedAt: "2026-06-06T00:00:00Z",
	}, []CodeDocument{{Path: "README.md", Language: "md", ContentHash: "h", Text: "local archive", ByteSize: 13, UpdatedAt: "2026-06-06T00:00:00Z"}}); err != nil {
		t.Fatalf("replace snapshot: %v", err)
	}
	for _, query := range []string{"", "not-present"} {
		hits, err := st.SearchCodeDocuments(ctx, repoID, query, 0)
		if err != nil {
			t.Fatalf("search %q: %v", query, err)
		}
		if len(hits) != 0 {
			t.Fatalf("search %q hits = %#v", query, hits)
		}
	}
}
