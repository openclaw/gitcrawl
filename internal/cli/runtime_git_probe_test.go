//go:build !windows

package cli

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestPortableStoreRootDoesNotRequireGitForUninitializedMetadata(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("PATH", dir)

	candidate := filepath.Join(dir, "data")
	if err := os.MkdirAll(filepath.Join(candidate, ".git"), 0o755); err != nil {
		t.Fatalf("mkdir stray Git metadata: %v", err)
	}
	root, ok, err := portableStoreRoot(context.Background(), filepath.Join(candidate, "gitcrawl.db"))
	if err != nil {
		t.Fatalf("portable store root: %v", err)
	}
	if ok || root != "" {
		t.Fatalf("portable store root = %q, ok=%v, want local database", root, ok)
	}
}
