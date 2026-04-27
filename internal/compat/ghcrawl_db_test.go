package compat

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/openclaw/gitcrawl/internal/store"
)

func TestGHCrawlDatabaseCompatibility(t *testing.T) {
	source := os.Getenv("GITCRAWL_GHCRAWL_COMPAT_DB")
	if source == "" {
		t.Skip("set GITCRAWL_GHCRAWL_COMPAT_DB to a copied ghcrawl.db to run this compatibility test")
	}
	copyPath := filepath.Join(t.TempDir(), "ghcrawl-copy.db")
	copyFile(t, source, copyPath)

	ctx := context.Background()
	st, err := store.Open(ctx, copyPath)
	if err != nil {
		t.Fatalf("open copied ghcrawl db: %v", err)
	}
	defer st.Close()

	status, err := st.Status(ctx)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.RepositoryCount == 0 || status.ThreadCount == 0 {
		t.Fatalf("expected populated ghcrawl db, got %#v", status)
	}

	repo, err := st.RepositoryByFullName(ctx, "openclaw/openclaw")
	if err != nil {
		t.Fatalf("repository lookup: %v", err)
	}
	if _, err := st.ListThreadsFiltered(ctx, store.ThreadListOptions{RepoID: repo.ID, Limit: 2}); err != nil {
		t.Fatalf("list threads: %v", err)
	}
	if _, err := st.SearchDocuments(ctx, repo.ID, "download", 2); err != nil {
		t.Fatalf("search documents: %v", err)
	}
	if _, err := st.ListClusterSummaries(ctx, store.ClusterSummaryOptions{RepoID: repo.ID, IncludeClosed: true, Limit: 2, Sort: "size"}); err != nil {
		t.Fatalf("list clusters: %v", err)
	}
}

func copyFile(t *testing.T, source, destination string) {
	t.Helper()
	in, err := os.Open(source)
	if err != nil {
		t.Fatalf("open source db: %v", err)
	}
	defer in.Close()
	out, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("create destination db: %v", err)
	}
	defer out.Close()
	if _, err := io.Copy(out, in); err != nil {
		t.Fatalf("copy db: %v", err)
	}
}
