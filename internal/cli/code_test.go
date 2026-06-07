package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/openclaw/gitcrawl/internal/store"
)

func TestCodeIndexAndScopedSearch(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "gitcrawl.db")
	if err := New().Run(ctx, []string{"--config", configPath, "init", "--db", dbPath}); err != nil {
		t.Fatalf("init: %v", err)
	}
	st, err := store.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	repoID, err := st.UpsertRepository(ctx, store.Repository{
		Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl",
		RawJSON: "{}", UpdatedAt: "2026-06-06T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	threadID, err := st.UpsertThread(ctx, store.Thread{
		RepoID: repoID, GitHubID: "1", Number: 1, Kind: "issue", State: "open",
		Title: "Manifest refresh issue", Body: "cache misses", HTMLURL: "https://github.com/openclaw/gitcrawl/issues/1",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "h", UpdatedAt: "2026-06-06T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("thread: %v", err)
	}
	if _, err := st.UpsertDocument(ctx, store.Document{
		ThreadID: threadID, Title: "Manifest refresh issue", RawText: "manifest refresh cache misses",
		DedupeText: "manifest refresh cache misses", UpdatedAt: "2026-06-06T00:00:00Z",
	}); err != nil {
		t.Fatalf("thread document: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	source := filepath.Join(dir, "source")
	if err := os.MkdirAll(filepath.Join(source, "internal", "cache"), 0o755); err != nil {
		t.Fatalf("mkdir source: %v", err)
	}
	if err := os.WriteFile(filepath.Join(source, "internal", "cache", "store.go"), []byte("package cache\n// manifest refresh\nfunc RefreshManifest() {}\n"), 0o600); err != nil {
		t.Fatalf("write source: %v", err)
	}
	if err := runGit(ctx, source, "init", "-b", "main"); err != nil {
		t.Fatalf("git init: %v", err)
	}
	if err := runGit(ctx, source, "add", "."); err != nil {
		t.Fatalf("git add: %v", err)
	}
	if err := runGit(ctx, source, "-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "-m", "seed"); err != nil {
		t.Fatalf("git commit: %v", err)
	}

	app := New()
	var stdout bytes.Buffer
	app.Stdout = &stdout
	if err := app.Run(ctx, []string{"--config", configPath, "--json", "code", "index", "openclaw/gitcrawl", "--path", source}); err != nil {
		t.Fatalf("code index: %v", err)
	}
	var indexed codeIndexResult
	if err := json.Unmarshal(stdout.Bytes(), &indexed); err != nil {
		t.Fatalf("decode code index: %v\n%s", err, stdout.String())
	}
	expectedRoot, err := filepath.EvalSymlinks(source)
	if err != nil {
		t.Fatalf("resolve source root: %v", err)
	}
	if indexed.FilesIndexed != 1 || indexed.GitSHA == "" || indexed.SourceRoot != expectedRoot {
		t.Fatalf("index result = %#v", indexed)
	}

	stdout.Reset()
	search := New()
	search.Stdout = &stdout
	if err := search.Run(ctx, []string{"--config", configPath, "--json", "search", "openclaw/gitcrawl", "--query", "RefreshManifest", "--scope", "code"}); err != nil {
		t.Fatalf("code search: %v", err)
	}
	if !strings.Contains(stdout.String(), `"path": "internal/cache/store.go"`) || !strings.Contains(stdout.String(), `"scope": "code"`) {
		t.Fatalf("code search output = %s", stdout.String())
	}

	stdout.Reset()
	allSearch := New()
	allSearch.Stdout = &stdout
	if err := allSearch.Run(ctx, []string{"--config", configPath, "--json", "search", "openclaw/gitcrawl", "--query", "manifest", "--scope", "all"}); err != nil {
		t.Fatalf("all search: %v", err)
	}
	if !strings.Contains(stdout.String(), `"scope": "threads"`) || !strings.Contains(stdout.String(), `"scope": "code"`) {
		t.Fatalf("all search output = %s", stdout.String())
	}
}

func TestCodeCommandValidation(t *testing.T) {
	ctx := context.Background()
	for _, args := range [][]string{
		{"code"},
		{"code", "unknown"},
		{"code", "index"},
		{"code", "index", "badrepo"},
		{"code", "index", "openclaw/gitcrawl", "--max-files", "0"},
		{"code", "index", "openclaw/gitcrawl", "--max-total-bytes", "0"},
		{"search", "openclaw/gitcrawl", "--query", "x", "--scope", "bad"},
		{"search", "openclaw/gitcrawl", "--query", "x", "--scope", "code", "--mode", "semantic"},
	} {
		if err := New().Run(ctx, args); err == nil {
			t.Fatalf("args unexpectedly succeeded: %#v", args)
		}
	}
}

func TestCodeIndexRejectsPortableStoreRuntime(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	checkout := filepath.Join(dir, "portable")
	configPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(checkout, "data", "gitcrawl.db")
	if err := New().Run(ctx, []string{"--config", configPath, "init", "--db", dbPath}); err != nil {
		t.Fatalf("init: %v", err)
	}
	st, err := store.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if _, err := st.UpsertRepository(ctx, store.Repository{
		Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl",
		RawJSON: "{}", UpdatedAt: "2026-06-06T00:00:00Z",
	}); err != nil {
		t.Fatalf("repo: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	if err := runGit(ctx, checkout, "init", "-b", "main"); err != nil {
		t.Fatalf("git init: %v", err)
	}
	if err := runGit(ctx, checkout, "add", "."); err != nil {
		t.Fatalf("git add: %v", err)
	}
	if err := runGit(ctx, checkout, "-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "-m", "seed"); err != nil {
		t.Fatalf("git commit: %v", err)
	}

	err = New().Run(ctx, []string{"--config", configPath, "code", "index", "openclaw/gitcrawl", "--path", checkout})
	if err == nil || !strings.Contains(err.Error(), "requires a local database") {
		t.Fatalf("code index portable error = %v", err)
	}
}
