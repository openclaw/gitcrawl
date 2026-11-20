package cli

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestAcquireAPIValidationDoesNotOpenArchiveOrResolveGitHubToken(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "native.sqlite")
	t.Setenv("GITCRAWL_DB", target)
	t.Setenv("EMPTY_PUBLISHER", "")
	t.Setenv("EMPTY_READER", "")
	a := New()
	a.Stdout = io.Discard
	a.Stderr = io.Discard
	a.githubAuthTokenLookup = func(context.Context) (string, error) { t.Fatal("looked up personal GitHub token"); return "", nil }
	if releaseNotificationAllowed([]string{"acquire-api"}) {
		t.Fatal("unrelated GitHub release lookup enabled")
	}
	err := a.Run(context.Background(), []string{"acquire-api", "--endpoint", "https://archive.example", "--source", "fixture", "--repo", "example/project", "--publisher-token-env", "EMPTY_PUBLISHER", "--reader-token-env", "EMPTY_READER", "--since", "2026-01-01T00:00:00Z"})
	if err == nil {
		t.Fatal("missing explicit credentials accepted")
	}
	if _, err = os.Stat(target); !os.IsNotExist(err) {
		t.Fatal("archive touched", err)
	}
}

func TestAcquireAPIParentSelectionRejectsAmbiguousModeWithoutOpeningArchive(t *testing.T) {
	root := t.TempDir()
	db := filepath.Join(root, "native.sqlite")
	t.Setenv("GITCRAWL_DB", db)
	a := New()
	a.Stdout = io.Discard
	a.Stderr = io.Discard
	a.githubAuthTokenLookup = func(context.Context) (string, error) { t.Fatal("personal credential lookup"); return "", nil }
	err := a.Run(context.Background(), []string{"acquire-api", "--endpoint", "https://archive.example", "--source", "fixture", "--repo", "example/project", "--publisher-token-env", "PUB", "--reader-token-env", "READER", "--parent-selection-file", "unused.json", "--since", "2026-01-01T00:00:00Z"})
	if err == nil {
		t.Fatal("accepted mixed parent and incremental mode")
	}
	if _, err = os.Stat(db); !os.IsNotExist(err) {
		t.Fatal("opened SQLite", err)
	}
}
