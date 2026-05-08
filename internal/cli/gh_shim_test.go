package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/openclaw/gitcrawl/internal/config"
	"github.com/openclaw/gitcrawl/internal/store"
)

func TestGHShimSearchAcceptsGHFlags(t *testing.T) {
	ctx := context.Background()
	configPath := seedGHShimRepo(t, ctx)

	run := New()
	var stdout bytes.Buffer
	run.Stdout = &stdout
	if err := run.Run(ctx, []string{
		"--config", configPath,
		"gh", "search", "issues", "hot loop",
		"-R", "openclaw/openclaw",
		"--state", "open",
		"--match", "comments",
		"--sort", "updated",
		"--order", "desc",
		"--json", "number,title,state,url",
		"--limit", "10",
	}); err != nil {
		t.Fatalf("gh shim search: %v", err)
	}
	var rows []map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &rows); err != nil {
		t.Fatalf("decode search: %v\n%s", err, stdout.String())
	}
	if len(rows) != 1 || int(rows[0]["number"].(float64)) != 10 {
		t.Fatalf("rows = %#v", rows)
	}
}

func TestGHShimFallsBackForUnsupportedRead(t *testing.T) {
	ctx := context.Background()
	configPath := seedGHShimRepo(t, ctx)
	dir := t.TempDir()
	ghPath := filepath.Join(dir, "gh")
	if err := os.WriteFile(ghPath, []byte("#!/bin/sh\necho fallback:$*\n"), 0o755); err != nil {
		t.Fatalf("write fake gh: %v", err)
	}
	t.Setenv("GITCRAWL_GH_PATH", ghPath)

	run := New()
	var stdout bytes.Buffer
	run.Stdout = &stdout
	if err := run.Run(ctx, []string{"--config", configPath, "gh", "pr", "view", "12", "-R", "openclaw/openclaw", "--json", "unsupportedField"}); err != nil {
		t.Fatalf("fallback: %v", err)
	}
	if got := strings.TrimSpace(stdout.String()); got != "fallback:pr view 12 -R openclaw/openclaw --json unsupportedField" {
		t.Fatalf("fallback output = %q", got)
	}
}

func TestGHShimPassThroughUsesConfigEnvToken(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	if err := os.WriteFile(configPath, []byte(`
[env]
GITHUB_TOKEN = "config-token"
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	ghPath := filepath.Join(dir, "gh")
	if err := os.WriteFile(ghPath, []byte("#!/bin/sh\necho token:$GITHUB_TOKEN args:$*\n"), 0o755); err != nil {
		t.Fatalf("write fake gh: %v", err)
	}
	t.Setenv("GITHUB_TOKEN", "")
	t.Setenv("GITCRAWL_GH_PATH", ghPath)

	run := New()
	var stdout bytes.Buffer
	run.Stdout = &stdout
	if err := run.Run(ctx, []string{"--config", configPath, "gh", "auth", "status"}); err != nil {
		t.Fatalf("pass-through: %v", err)
	}
	if got := strings.TrimSpace(stdout.String()); got != "token:config-token args:auth status" {
		t.Fatalf("pass-through output = %q", got)
	}
}

func TestGHShimCachedFallbackUsesConfigEnvToken(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	if err := os.WriteFile(configPath, []byte(`
[env]
GITHUB_TOKEN = "config-token"
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	ghPath := filepath.Join(dir, "gh")
	if err := os.WriteFile(ghPath, []byte("#!/bin/sh\necho token:$GITHUB_TOKEN args:$*\n"), 0o755); err != nil {
		t.Fatalf("write fake gh: %v", err)
	}
	t.Setenv("GITHUB_TOKEN", "")
	t.Setenv("GITCRAWL_GH_PATH", ghPath)

	run := New()
	var stdout bytes.Buffer
	run.Stdout = &stdout
	if err := run.Run(ctx, []string{"--config", configPath, "gh", "repo", "view", "openclaw/gitcrawl"}); err != nil {
		t.Fatalf("cached fallback: %v", err)
	}
	if got := strings.TrimSpace(stdout.String()); got != "token:config-token args:repo view openclaw/gitcrawl" {
		t.Fatalf("cached fallback output = %q", got)
	}
}

func TestGHShimFallsBackForEmptyOpenIssueListWithoutBroadSync(t *testing.T) {
	ctx := context.Background()
	configPath := seedGHShimEmptyRepo(t, ctx)
	dir := t.TempDir()
	ghPath := filepath.Join(dir, "gh")
	if err := os.WriteFile(ghPath, []byte("#!/bin/sh\necho fallback:$*\n"), 0o755); err != nil {
		t.Fatalf("write fake gh: %v", err)
	}
	t.Setenv("GITCRAWL_GH_PATH", ghPath)

	run := New()
	var stdout bytes.Buffer
	run.Stdout = &stdout
	if err := run.Run(ctx, []string{"--config", configPath, "gh", "issue", "list", "-R", "openclaw/openclaw", "--state", "open", "--json", "number"}); err != nil {
		t.Fatalf("fallback: %v", err)
	}
	if got := strings.TrimSpace(stdout.String()); got != "fallback:issue list -R openclaw/openclaw --state open --json number" {
		t.Fatalf("fallback output = %q", got)
	}
}

func TestGHShimSearchFallsBackForEmptyOpenRepoWithoutBroadSync(t *testing.T) {
	ctx := context.Background()
	configPath := seedGHShimEmptyRepo(t, ctx)
	dir := t.TempDir()
	ghPath := filepath.Join(dir, "gh")
	if err := os.WriteFile(ghPath, []byte("#!/bin/sh\necho fallback:$*\n"), 0o755); err != nil {
		t.Fatalf("write fake gh: %v", err)
	}
	t.Setenv("GITCRAWL_GH_PATH", ghPath)

	run := New()
	var stdout bytes.Buffer
	run.Stdout = &stdout
	if err := run.Run(ctx, []string{"--config", configPath, "gh", "search", "issues", "-R", "openclaw/openclaw", "--state", "open", "--json", "number"}); err != nil {
		t.Fatalf("fallback: %v", err)
	}
	if got := strings.TrimSpace(stdout.String()); got != "fallback:search issues -R openclaw/openclaw --state open --json number" {
		t.Fatalf("fallback output = %q", got)
	}
}

func TestGHShimAutoHydratePortableStoreWritesRuntimeMirror(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	remoteDir := filepath.Join(dir, "remote")
	checkoutDir := filepath.Join(dir, "checkout")
	dbRel := filepath.Join("data", "openclaw__openclaw.sync.db")
	if err := os.MkdirAll(filepath.Join(remoteDir, "data"), 0o755); err != nil {
		t.Fatalf("mkdir remote data: %v", err)
	}
	if err := runGit(ctx, remoteDir, "init", "-b", "main"); err != nil {
		t.Fatalf("git init: %v", err)
	}
	seedPortableThread(t, filepath.Join(remoteDir, dbRel), 1, "portable issue")
	if err := runGit(ctx, remoteDir, "add", dbRel); err != nil {
		t.Fatalf("git add seed: %v", err)
	}
	if err := runGit(ctx, remoteDir, "-c", "user.email=test@example.com", "-c", "user.name=Test", "commit", "-m", "seed store"); err != nil {
		t.Fatalf("git commit seed: %v", err)
	}
	if _, err := syncPortableStore(ctx, remoteDir, checkoutDir); err != nil {
		t.Fatalf("clone portable store: %v", err)
	}

	configPath := filepath.Join(dir, "config.toml")
	app := New()
	if err := app.Run(ctx, []string{"--config", configPath, "init", "--db", filepath.Join(checkoutDir, dbRel)}); err != nil {
		t.Fatalf("init config: %v", err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/repos/openclaw/openclaw":
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 101, "full_name": "openclaw/openclaw"})
		case "/repos/openclaw/openclaw/issues/2":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id":         502,
				"number":     2,
				"state":      "open",
				"title":      "runtime-only issue",
				"body":       "hydrate into runtime mirror",
				"html_url":   "https://github.com/openclaw/openclaw/issues/2",
				"created_at": "2026-05-08T00:00:00Z",
				"updated_at": "2026-05-08T00:00:00Z",
				"labels":     []map[string]any{},
				"assignees":  []map[string]any{},
				"user":       map[string]any{"login": "alice", "type": "User"},
			})
		default:
			t.Fatalf("unexpected path: %s", r.URL.String())
		}
	}))
	defer server.Close()
	t.Setenv("GITHUB_TOKEN", "test-token")
	t.Setenv("GITCRAWL_GITHUB_BASE_URL", server.URL)

	run := New()
	var stdout bytes.Buffer
	run.Stdout = &stdout
	if err := run.Run(ctx, []string{"--config", configPath, "gh", "issue", "view", "2", "-R", "openclaw/openclaw", "--json", "number,title"}); err != nil {
		t.Fatalf("gh issue view: %v", err)
	}
	if !strings.Contains(stdout.String(), `"number": 2`) || !strings.Contains(stdout.String(), "runtime-only issue") {
		t.Fatalf("view output = %q", stdout.String())
	}
	if !gitWorktreeClean(ctx, checkoutDir) {
		t.Fatal("auto-hydrate dirtied portable checkout")
	}
	assertPortableThreadPresence(t, ctx, filepath.Join(checkoutDir, dbRel), 2, false)
	mirrorPath, err := run.portableRuntimeDBPath(filepath.Join(checkoutDir, dbRel))
	if err != nil {
		t.Fatalf("runtime db path: %v", err)
	}
	assertPortableThreadPresence(t, ctx, mirrorPath, 2, true)
}

func TestGHShimViewAcceptsFullGitHubURL(t *testing.T) {
	ctx := context.Background()
	configPath := seedGHShimRepo(t, ctx)

	run := New()
	var stdout bytes.Buffer
	run.Stdout = &stdout
	if err := run.Run(ctx, []string{
		"--config", configPath,
		"gh", "issue", "view", "https://github.com/openclaw/openclaw/issues/10",
		"--json", "number,title,url",
	}); err != nil {
		t.Fatalf("gh issue view URL: %v", err)
	}
	var row map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &row); err != nil {
		t.Fatalf("decode issue view: %v\n%s", err, stdout.String())
	}
	if int(row["number"].(float64)) != 10 || row["url"] != "https://github.com/openclaw/openclaw/issues/10" {
		t.Fatalf("row = %#v", row)
	}
}

func seedGHShimEmptyRepo(t *testing.T, ctx context.Context) string {
	t.Helper()
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "gitcrawl.db")
	app := New()
	if err := app.Run(ctx, []string{"--config", configPath, "init", "--db", dbPath}); err != nil {
		t.Fatalf("init: %v", err)
	}
	cfg, err := config.Load(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	cfg.CacheDir = filepath.Join(dir, "cache")
	if err := config.Save(configPath, cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}
	st, err := store.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	repoID, err := st.UpsertRepository(ctx, store.Repository{
		Owner:     "openclaw",
		Name:      "openclaw",
		FullName:  "openclaw/openclaw",
		RawJSON:   "{}",
		UpdatedAt: "2026-05-08T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("seed repository: %v", err)
	}
	if _, err := st.RecordRun(ctx, store.RunRecord{
		RepoID:     repoID,
		Kind:       "sync",
		Scope:      "numbers:13",
		Status:     "success",
		StartedAt:  "2026-05-08T00:00:00Z",
		FinishedAt: "2026-05-08T00:00:01Z",
	}); err != nil {
		t.Fatalf("record targeted sync: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	return configPath
}

func assertPortableThreadPresence(t *testing.T, ctx context.Context, dbPath string, number int, want bool) {
	t.Helper()
	st, err := store.OpenReadOnly(ctx, dbPath)
	if err != nil {
		t.Fatalf("open store %s: %v", dbPath, err)
	}
	defer st.Close()
	repo, err := st.RepositoryByFullName(ctx, "openclaw/openclaw")
	if err != nil {
		t.Fatalf("repository %s: %v", dbPath, err)
	}
	threads, err := st.ListThreadsFiltered(ctx, store.ThreadListOptions{RepoID: repo.ID, IncludeClosed: true, Numbers: []int{number}})
	if err != nil {
		t.Fatalf("list threads %s: %v", dbPath, err)
	}
	got := len(threads) > 0
	if got != want {
		t.Fatalf("thread %d presence in %s = %v, want %v", number, dbPath, got, want)
	}
}

func seedGHShimRepo(t *testing.T, ctx context.Context) string {
	t.Helper()
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "gitcrawl.db")
	app := New()
	if err := app.Run(ctx, []string{"--config", configPath, "init", "--db", dbPath}); err != nil {
		t.Fatalf("init: %v", err)
	}
	cfg, err := config.Load(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	cfg.CacheDir = filepath.Join(dir, "cache")
	if err := config.Save(configPath, cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}
	st, err := store.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	repoID, err := st.UpsertRepository(ctx, store.Repository{
		Owner:     "openclaw",
		Name:      "openclaw",
		FullName:  "openclaw/openclaw",
		RawJSON:   "{}",
		UpdatedAt: "2026-04-27T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("seed repository: %v", err)
	}
	issueID, err := st.UpsertThread(ctx, store.Thread{
		RepoID:          repoID,
		GitHubID:        "10",
		Number:          10,
		Kind:            "issue",
		State:           "open",
		Title:           "Hot loop burns CPU",
		Body:            "the runtime has a hot loop",
		AuthorLogin:     "alice",
		AuthorType:      "User",
		HTMLURL:         "https://github.com/openclaw/openclaw/issues/10",
		LabelsJSON:      `[{"name":"bug","color":"d73a4a"}]`,
		AssigneesJSON:   `[{"login":"peter"}]`,
		RawJSON:         "{}",
		ContentHash:     "issue-10",
		UpdatedAtGitHub: "2026-04-27T01:00:00Z",
		UpdatedAt:       "2026-04-27T01:00:00Z",
	})
	if err != nil {
		t.Fatalf("seed issue: %v", err)
	}
	if _, err := st.UpsertDocument(ctx, store.Document{ThreadID: issueID, Title: "Hot loop burns CPU", RawText: "runtime hot loop burns CPU", DedupeText: "runtime hot loop burns cpu", UpdatedAt: "2026-04-27T01:00:00Z"}); err != nil {
		t.Fatalf("seed issue document: %v", err)
	}
	if _, err := st.UpsertComment(ctx, store.Comment{
		ThreadID:        issueID,
		GitHubID:        "1001",
		CommentType:     "issue_comment",
		AuthorLogin:     "carol",
		AuthorType:      "User",
		Body:            "same hot loop here",
		RawJSON:         "{}",
		CreatedAtGitHub: "2026-04-27T01:10:00Z",
		UpdatedAtGitHub: "2026-04-27T01:10:00Z",
	}); err != nil {
		t.Fatalf("seed issue comment: %v", err)
	}
	prID, err := st.UpsertThread(ctx, store.Thread{
		RepoID:          repoID,
		GitHubID:        "12",
		Number:          12,
		Kind:            "pull_request",
		State:           "open",
		Title:           "Manifest cache update",
		AuthorLogin:     "bob",
		AuthorType:      "User",
		HTMLURL:         "https://github.com/openclaw/openclaw/pull/12",
		LabelsJSON:      "[]",
		AssigneesJSON:   "[]",
		RawJSON:         `{"head":{"sha":"abc123"}}`,
		ContentHash:     "pr-12",
		IsDraft:         true,
		UpdatedAtGitHub: "2026-04-27T02:00:00Z",
		UpdatedAt:       "2026-04-27T02:00:00Z",
	})
	if err != nil {
		t.Fatalf("seed pr: %v", err)
	}
	if _, err := st.UpsertDocument(ctx, store.Document{ThreadID: prID, Title: "Manifest cache update", RawText: "manifest cache refresh", DedupeText: "manifest cache refresh", UpdatedAt: "2026-04-27T02:00:00Z"}); err != nil {
		t.Fatalf("seed pr document: %v", err)
	}
	if _, err := st.UpsertComment(ctx, store.Comment{
		ThreadID:        prID,
		GitHubID:        "1201",
		CommentType:     "review_comment",
		AuthorLogin:     "dana",
		AuthorType:      "User",
		Body:            "cache path looks good",
		RawJSON:         "{}",
		CreatedAtGitHub: "2026-04-27T02:10:00Z",
		UpdatedAtGitHub: "2026-04-27T02:10:00Z",
	}); err != nil {
		t.Fatalf("seed pr comment: %v", err)
	}
	fetchedAt := time.Now().UTC().Format(time.RFC3339Nano)
	if err := st.UpsertPullRequestCache(ctx, store.PullRequestDetail{
		ThreadID:         prID,
		RepoID:           repoID,
		Number:           12,
		BaseSHA:          "base123",
		HeadSHA:          "abc123",
		HeadRef:          "manifest-cache",
		HeadRepoFullName: "openclaw/openclaw",
		MergeableState:   "clean",
		Additions:        10,
		Deletions:        2,
		ChangedFiles:     1,
		RawJSON:          `{"head":{"sha":"abc123"}}`,
		FetchedAt:        fetchedAt,
		UpdatedAt:        fetchedAt,
	}, []store.PullRequestFile{{
		ThreadID:  prID,
		Path:      "internal/cache.go",
		Status:    "modified",
		Additions: 10,
		Deletions: 2,
		Changes:   12,
		RawJSON:   "{}",
		FetchedAt: fetchedAt,
	}}, []store.PullRequestCommit{{
		ThreadID:    prID,
		SHA:         "commit123",
		Message:     "feat: cache",
		AuthorLogin: "alice",
		AuthorName:  "Alice",
		CommittedAt: "2026-04-27T01:00:00Z",
		HTMLURL:     "https://github.com/openclaw/openclaw/commit/commit123",
		RawJSON:     "{}",
		FetchedAt:   fetchedAt,
	}}, []store.PullRequestCheck{{
		ThreadID:     prID,
		Name:         "test",
		Status:       "completed",
		Conclusion:   "success",
		DetailsURL:   "https://github.com/openclaw/openclaw/actions/runs/99",
		WorkflowName: "CI",
		RawJSON:      "{}",
		FetchedAt:    fetchedAt,
	}}, []store.WorkflowRun{{
		RepoID:       repoID,
		RunID:        "99",
		RunNumber:    7,
		HeadBranch:   "manifest-cache",
		HeadSHA:      "abc123",
		Status:       "completed",
		Conclusion:   "success",
		WorkflowName: "CI",
		Event:        "pull_request",
		HTMLURL:      "https://github.com/openclaw/openclaw/actions/runs/99",
		CreatedAtGH:  "2026-04-27T01:00:00Z",
		UpdatedAtGH:  "2026-04-27T02:00:00Z",
		RawJSON:      "{}",
		FetchedAt:    fetchedAt,
	}}); err != nil {
		t.Fatalf("seed pr cache: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	return configPath
}
