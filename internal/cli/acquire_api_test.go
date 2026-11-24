package cli

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
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

func TestAcquireAPILifecycleDispatchWithoutSQLite(t *testing.T) {
	root := t.TempDir()
	db := filepath.Join(root, "archive", "native.sqlite")
	selection := filepath.Join(root, "numbers.txt")
	if err := os.WriteFile(selection, []byte("7\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GITCRAWL_DB", db)
	t.Setenv("LIFECYCLE_PUBLISHER", "publisher")
	t.Setenv("LIFECYCLE_READER", "reader")
	t.Setenv("OCTOPOOL_TOKEN", "relay")
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("other active mode must stop before provider call")
		w.WriteHeader(500)
	}))
	defer provider.Close()
	t.Setenv("OCTOPOOL_URL", provider.URL)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || !strings.HasSuffix(r.URL.Path, "/acquisition/state") || r.Header.Get("Authorization") != "Bearer publisher" {
			t.Error("wrong acquisition route")
		}
		fmt.Fprint(w, `{"revision":1,"sequence":20,"checkpoint":{},"completedThrough":"2026-01-04T00:00:00Z","activeRun":{"id":"other","mode":"historical-parents","expectedRevision":1,"horizon":"2026-01-04T00:00:00Z","scope":{}}}`)
	}))
	defer api.Close()
	a := New()
	a.Stdout = io.Discard
	a.Stderr = io.Discard
	a.githubAuthTokenLookup = func(context.Context) (string, error) { t.Fatal("personal GitHub credential lookup"); return "", nil }
	err := a.Run(context.Background(), []string{"acquire-api", "--endpoint", api.URL, "--source", "fixture", "--repo", "example/project", "--publisher-token-env", "LIFECYCLE_PUBLISHER", "--reader-token-env", "LIFECYCLE_READER", "--lifecycle-numbers-file", selection, "--since", "2026-01-01T00:00:00Z", "--lifecycle-max-pages", "1"})
	if err == nil || !strings.Contains(err.Error(), "another acquisition mode is active") {
		t.Fatal("lifecycle selector not dispatched", err)
	}
	err = a.Run(context.Background(), []string{"acquire-api", "--cycle", "--numbers-file", selection, "--lifecycle-max-pages", "1", "--lifecycle-reader-endpoint", api.URL, "--endpoint", api.URL, "--source", "fixture", "--repo", "example/project", "--publisher-token-env", "LIFECYCLE_PUBLISHER", "--reader-token-env", "LIFECYCLE_READER"})
	if err == nil || !strings.Contains(err.Error(), "outside the admitted cycle") {
		t.Fatal("scheduled window did not reach the active-run owner", err)
	}
	if _, err = os.Stat(filepath.Dir(db)); !os.IsNotExist(err) {
		t.Fatal("SQLite path touched", err)
	}
}

func TestAcquireAPILifecycleRejectsMixedSelectors(t *testing.T) {
	a := New()
	a.Stdout = io.Discard
	a.Stderr = io.Discard
	base := []string{"acquire-api", "--endpoint", "https://archive.example", "--source", "fixture", "--repo", "example/project", "--publisher-token-env", "PUB", "--reader-token-env", "READ"}
	for _, args := range [][]string{{"--commits", "--numbers-file", "unused"}, {"--commits", "--parent-selection-file", "unused"}, {"--commits", "--lifecycle-numbers-file", "unused"}, {"--commit-max-pages", "1"}, {"--lifecycle-numbers-file", "unused", "--numbers-file", "unused"}, {"--lifecycle-numbers-file", "unused", "--parent-selection-file", "unused"}, {"--lifecycle-max-pages", "1"}} {
		if err := a.Run(context.Background(), append(append([]string{}, base...), args...)); err == nil {
			t.Fatal("mixed or unselected lifecycle flags accepted", args)
		}
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
