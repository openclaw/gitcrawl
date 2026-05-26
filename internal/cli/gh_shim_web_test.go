package cli

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gh "github.com/openclaw/gitcrawl/internal/github"
)

func TestGHShimWebFallbackServesContentsFromRawGitHub(t *testing.T) {
	ctx := context.Background()
	configPath := seedGHShimRepo(t, ctx)
	rawServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/openclaw/openclaw/abc123/src/app.go" {
			t.Fatalf("raw path = %s", r.URL.Path)
		}
		_, _ = w.Write([]byte("package main\n"))
	}))
	defer rawServer.Close()
	dir := t.TempDir()
	countPath := filepath.Join(dir, "count")
	ghPath := filepath.Join(dir, "gh")
	if err := os.WriteFile(ghPath, []byte("#!/bin/sh\necho called > \"$GH_SHIM_COUNT\"\necho fallback:$*\n"), 0o755); err != nil {
		t.Fatalf("write fake gh: %v", err)
	}
	t.Setenv("GITCRAWL_GH_PATH", ghPath)
	t.Setenv("GH_SHIM_COUNT", countPath)
	t.Setenv("GITCRAWL_GH_RAW_BASE_URL", rawServer.URL)

	run := New()
	var stdout bytes.Buffer
	run.Stdout = &stdout
	err := run.Run(ctx, []string{"--config", configPath, "gh", "--web-fallback", "api", "repos/openclaw/openclaw/contents/src/app.go?ref=abc123"})
	if err != nil {
		t.Fatalf("web contents: %v", err)
	}
	if _, err := os.Stat(countPath); !os.IsNotExist(err) {
		t.Fatalf("fake gh should not be called: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
		t.Fatalf("decode payload: %v\n%s", err, stdout.String())
	}
	decoded, err := base64.StdEncoding.DecodeString(payload["content"].(string))
	if err != nil {
		t.Fatalf("decode content: %v", err)
	}
	if string(decoded) != "package main\n" || payload["path"] != "src/app.go" {
		t.Fatalf("payload = %#v decoded=%q", payload, decoded)
	}
	if payload["sha"] != testGitBlobSHA([]byte("package main\n")) {
		t.Fatalf("sha = %v", payload["sha"])
	}
	if payload["git_url"] != "https://api.github.com/repos/openclaw/openclaw/git/blobs/"+testGitBlobSHA([]byte("package main\n")) {
		t.Fatalf("git_url = %v", payload["git_url"])
	}
	links := payload["_links"].(map[string]any)
	if links["git"] != payload["git_url"] || links["html"] != payload["html_url"] || links["self"] != payload["url"] {
		t.Fatalf("links = %#v payload=%#v", links, payload)
	}
}

func TestGHShimWebFallbackServesPRDiff(t *testing.T) {
	ctx := context.Background()
	configPath := seedGHShimRepo(t, ctx)
	webServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/openclaw/openclaw/pull/12.diff":
			http.Redirect(w, r, "/raw/pull/12.diff", http.StatusFound)
		case "/raw/pull/12.diff":
			_, _ = w.Write([]byte("diff --git a/a b/a\n"))
		default:
			t.Fatalf("web path = %s", r.URL.Path)
		}
	}))
	defer webServer.Close()
	dir := t.TempDir()
	countPath := filepath.Join(dir, "count")
	ghPath := filepath.Join(dir, "gh")
	if err := os.WriteFile(ghPath, []byte("#!/bin/sh\necho called > \"$GH_SHIM_COUNT\"\necho fallback:$*\n"), 0o755); err != nil {
		t.Fatalf("write fake gh: %v", err)
	}
	t.Setenv("GITCRAWL_GH_PATH", ghPath)
	t.Setenv("GH_SHIM_COUNT", countPath)
	t.Setenv("GITCRAWL_GH_WEB_BASE_URL", webServer.URL)

	run := New()
	var stdout bytes.Buffer
	run.Stdout = &stdout
	err := run.Run(ctx, []string{"--config", configPath, "gh", "--web-fallback", "pr", "diff", "12", "-R", "openclaw/openclaw", "--color=never"})
	if err != nil {
		t.Fatalf("web pr diff: %v", err)
	}
	if got := stdout.String(); got != "diff --git a/a b/a\n" {
		t.Fatalf("diff = %q", got)
	}
	if _, err := os.Stat(countPath); !os.IsNotExist(err) {
		t.Fatalf("fake gh should not be called: %v", err)
	}
}

func TestGHShimWebFallbackServesAPICommitMedia(t *testing.T) {
	ctx := context.Background()
	configPath := seedGHShimRepo(t, ctx)
	webServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/openclaw/openclaw/commit/abcdef1.diff":
			_, _ = w.Write([]byte("diff --git a/a b/a\n"))
		case "/openclaw/openclaw/commit/abcdef1.patch":
			_, _ = w.Write([]byte("From abcdef1 Mon Sep 17 00:00:00 2001\n"))
		default:
			t.Fatalf("web path = %s", r.URL.Path)
		}
	}))
	defer webServer.Close()
	dir := t.TempDir()
	countPath := filepath.Join(dir, "count")
	ghPath := filepath.Join(dir, "gh")
	if err := os.WriteFile(ghPath, []byte("#!/bin/sh\necho called > \"$GH_SHIM_COUNT\"\necho fallback:$*\n"), 0o755); err != nil {
		t.Fatalf("write fake gh: %v", err)
	}
	t.Setenv("GITCRAWL_GH_PATH", ghPath)
	t.Setenv("GH_SHIM_COUNT", countPath)
	t.Setenv("GITCRAWL_GH_WEB_BASE_URL", webServer.URL)

	run := New()
	var stdout bytes.Buffer
	run.Stdout = &stdout
	err := run.Run(ctx, []string{"--config", configPath, "gh", "--web-fallback", "api", "repos/openclaw/openclaw/commits/abcdef1", "-H", "Accept: application/vnd.github.v3.diff"})
	if err != nil {
		t.Fatalf("web commit diff: %v", err)
	}
	if got := stdout.String(); got != "diff --git a/a b/a\n" {
		t.Fatalf("diff = %q", got)
	}
	stdout.Reset()
	err = run.Run(ctx, []string{"--config", configPath, "gh", "--web-fallback", "api", "repos/openclaw/openclaw/commits/abcdef1", "--header=Accept: application/vnd.github.v3.patch"})
	if err != nil {
		t.Fatalf("web commit patch: %v", err)
	}
	if got := stdout.String(); got != "From abcdef1 Mon Sep 17 00:00:00 2001\n" {
		t.Fatalf("patch = %q", got)
	}
	if _, err := os.Stat(countPath); !os.IsNotExist(err) {
		t.Fatalf("fake gh should not be called: %v", err)
	}
}

func TestGHShimWebFallbackServesAPICompareMedia(t *testing.T) {
	ctx := context.Background()
	configPath := seedGHShimRepo(t, ctx)
	webServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.EscapedPath() {
		case "/openclaw/openclaw/compare/main...feature%2Fcache.diff":
			_, _ = w.Write([]byte("diff --git a/b b/b\n"))
		case "/openclaw/openclaw/compare/main...feature%2Fcache.patch":
			_, _ = w.Write([]byte("From 1234567 Mon Sep 17 00:00:00 2001\n"))
		default:
			t.Fatalf("web path = %s", r.URL.EscapedPath())
		}
	}))
	defer webServer.Close()
	dir := t.TempDir()
	countPath := filepath.Join(dir, "count")
	ghPath := filepath.Join(dir, "gh")
	if err := os.WriteFile(ghPath, []byte("#!/bin/sh\necho called > \"$GH_SHIM_COUNT\"\necho fallback:$*\n"), 0o755); err != nil {
		t.Fatalf("write fake gh: %v", err)
	}
	t.Setenv("GITCRAWL_GH_PATH", ghPath)
	t.Setenv("GH_SHIM_COUNT", countPath)
	t.Setenv("GITCRAWL_GH_WEB_BASE_URL", webServer.URL)

	run := New()
	var stdout bytes.Buffer
	run.Stdout = &stdout
	err := run.Run(ctx, []string{"--config", configPath, "gh", "--web-fallback", "api", "repos/openclaw/openclaw/compare/main...feature%2Fcache", "-H", "Accept: application/vnd.github.diff"})
	if err != nil {
		t.Fatalf("web compare diff: %v", err)
	}
	if got := stdout.String(); got != "diff --git a/b b/b\n" {
		t.Fatalf("diff = %q", got)
	}
	stdout.Reset()
	err = run.Run(ctx, []string{"--config", configPath, "gh", "--web-fallback", "api", "repos/openclaw/openclaw/compare/main...feature%2Fcache", "-H", "Accept: application/vnd.github.patch"})
	if err != nil {
		t.Fatalf("web compare patch: %v", err)
	}
	if got := stdout.String(); got != "From 1234567 Mon Sep 17 00:00:00 2001\n" {
		t.Fatalf("patch = %q", got)
	}
	if _, err := os.Stat(countPath); !os.IsNotExist(err) {
		t.Fatalf("fake gh should not be called: %v", err)
	}
}

func TestGHShimWebFallbackDefaultsWhenBelowHalfLimit(t *testing.T) {
	ctx := context.Background()
	configPath := seedGHShimRepo(t, ctx)
	rawServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("low budget\n"))
	}))
	defer rawServer.Close()
	dir := t.TempDir()
	countPath := filepath.Join(dir, "count")
	ghPath := filepath.Join(dir, "gh")
	if err := os.WriteFile(ghPath, []byte("#!/bin/sh\ncount=0\n[ -f \"$GH_SHIM_COUNT\" ] && count=$(cat \"$GH_SHIM_COUNT\")\ncount=$((count + 1))\nprintf \"%s\" \"$count\" > \"$GH_SHIM_COUNT\"\necho fallback:$*\n"), 0o755); err != nil {
		t.Fatalf("write fake gh: %v", err)
	}
	t.Setenv("GITCRAWL_GH_PATH", ghPath)
	t.Setenv("GH_SHIM_COUNT", countPath)
	t.Setenv("GITCRAWL_GH_RAW_BASE_URL", rawServer.URL)
	t.Setenv("GITHUB_TOKEN", "test-token")
	app := New()
	app.configPath = configPath
	if err := app.writeSharedRateLimit(ctx, "test-token", gh.RateLimitSnapshot{
		Host: "github.com", Limit: 5000, Remaining: 2499, ResetAt: time.Now().Add(time.Hour), Resource: "core",
	}, "test"); err != nil {
		t.Fatalf("write rate limit: %v", err)
	}

	run := New()
	var stdout bytes.Buffer
	run.Stdout = &stdout
	err := run.Run(ctx, []string{"--config", configPath, "gh", "api", "repos/openclaw/openclaw/contents/README.md?ref=abc123"})
	if err != nil {
		t.Fatalf("web low budget default: %v", err)
	}
	if strings.Contains(stdout.String(), "fallback:") {
		t.Fatalf("used backend gh instead of web: %q", stdout.String())
	}
	stdout.Reset()
	if err := run.Run(ctx, []string{"--config", configPath, "gh", "xcache", "stats", "--json"}); err != nil {
		t.Fatalf("stats: %v", err)
	}
	var stats ghCommandCacheStats
	if err := json.Unmarshal(stdout.Bytes(), &stats); err != nil {
		t.Fatalf("decode stats: %v\n%s", err, stdout.String())
	}
	if stats.Counters.WebHits != 1 || stats.Counters.BackendMisses != 0 {
		t.Fatalf("counters = %+v", stats.Counters)
	}
}

func TestGHShimWebFallbackDefaultsWithGHAuthTokenBudget(t *testing.T) {
	ctx := context.Background()
	configPath := seedGHShimRepo(t, ctx)
	rawServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("gh auth budget\n"))
	}))
	defer rawServer.Close()
	dir := t.TempDir()
	countPath := filepath.Join(dir, "count")
	ghPath := filepath.Join(dir, "gh")
	if err := os.WriteFile(ghPath, []byte("#!/bin/sh\nif [ \"$1\" = auth ] && [ \"$2\" = token ]; then echo gh-auth-token; exit 0; fi\necho called > \"$GH_SHIM_COUNT\"\necho fallback:$*\n"), 0o755); err != nil {
		t.Fatalf("write fake gh: %v", err)
	}
	t.Setenv("GITCRAWL_GH_PATH", ghPath)
	t.Setenv("GH_SHIM_COUNT", countPath)
	t.Setenv("GITCRAWL_GH_RAW_BASE_URL", rawServer.URL)
	t.Setenv("GITHUB_TOKEN", "")
	app := New()
	app.configPath = configPath
	if err := app.writeSharedRateLimit(ctx, "gh-auth-token", gh.RateLimitSnapshot{
		Host: "github.com", Limit: 5000, Remaining: 2499, ResetAt: time.Now().Add(time.Hour), Resource: "core",
	}, "test"); err != nil {
		t.Fatalf("write rate limit: %v", err)
	}

	run := New()
	var stdout bytes.Buffer
	run.Stdout = &stdout
	err := run.Run(ctx, []string{"--config", configPath, "gh", "api", "repos/openclaw/openclaw/contents/README.md?ref=abc123"})
	if err != nil {
		t.Fatalf("web low budget default with gh auth token: %v", err)
	}
	if strings.Contains(stdout.String(), "fallback:") {
		t.Fatalf("used backend gh instead of web: %q", stdout.String())
	}
	if _, err := os.Stat(countPath); !os.IsNotExist(err) {
		t.Fatalf("fake gh fallback should not be called: %v", err)
	}
}

func TestGHShimWebFallbackSkipsUnsupportedAPIMedia(t *testing.T) {
	ctx := context.Background()
	configPath := seedGHShimRepo(t, ctx)
	webServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("web fallback should not hit media endpoint")
	}))
	defer webServer.Close()
	dir := t.TempDir()
	ghPath := filepath.Join(dir, "gh")
	if err := os.WriteFile(ghPath, []byte("#!/bin/sh\necho fallback:$*\n"), 0o755); err != nil {
		t.Fatalf("write fake gh: %v", err)
	}
	t.Setenv("GITCRAWL_GH_PATH", ghPath)
	t.Setenv("GITCRAWL_GH_WEB_BASE_URL", webServer.URL)

	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "json accept",
			args: []string{"api", "repos/openclaw/openclaw/commits/abcdef1", "-H", "Accept: application/vnd.github+json"},
			want: "fallback:api repos/openclaw/openclaw/commits/abcdef1 -H Accept: application/vnd.github+json",
		},
		{
			name: "conflicting accepts",
			args: []string{"api", "repos/openclaw/openclaw/commits/abcdef1", "-H", "Accept: application/vnd.github.v3.diff", "-H", "Accept: application/vnd.github.v3.patch"},
			want: "fallback:api repos/openclaw/openclaw/commits/abcdef1 -H Accept: application/vnd.github.v3.diff -H Accept: application/vnd.github.v3.patch",
		},
		{
			name: "branch commit ref",
			args: []string{"api", "repos/openclaw/openclaw/commits/main", "-H", "Accept: application/vnd.github.v3.diff"},
			want: "fallback:api repos/openclaw/openclaw/commits/main -H Accept: application/vnd.github.v3.diff",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			run := New()
			var stdout bytes.Buffer
			run.Stdout = &stdout
			args := append([]string{"--config", configPath, "gh", "--web-fallback"}, tt.args...)
			err := run.Run(ctx, args)
			if err != nil {
				t.Fatalf("unsupported media fallback: %v", err)
			}
			if got := strings.TrimSpace(stdout.String()); got != tt.want {
				t.Fatalf("stdout = %q", got)
			}
		})
	}
}

func TestGHShimWebFallbackSkipsNonGitHubHostname(t *testing.T) {
	ctx := context.Background()
	configPath := seedGHShimRepo(t, ctx)
	rawServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("web fallback should not hit raw server for GHES host")
	}))
	defer rawServer.Close()
	dir := t.TempDir()
	ghPath := filepath.Join(dir, "gh")
	if err := os.WriteFile(ghPath, []byte("#!/bin/sh\necho fallback:$*\n"), 0o755); err != nil {
		t.Fatalf("write fake gh: %v", err)
	}
	t.Setenv("GITCRAWL_GH_PATH", ghPath)
	t.Setenv("GITCRAWL_GH_RAW_BASE_URL", rawServer.URL)

	run := New()
	var stdout bytes.Buffer
	run.Stdout = &stdout
	err := run.Run(ctx, []string{"--config", configPath, "gh", "--web-fallback", "api", "--hostname", "ghe.example", "repos/openclaw/openclaw/contents/README.md?ref=abc123"})
	if err != nil {
		t.Fatalf("ghes fallback: %v", err)
	}
	if got := strings.TrimSpace(stdout.String()); got != "fallback:api --hostname ghe.example repos/openclaw/openclaw/contents/README.md?ref=abc123" {
		t.Fatalf("stdout = %q", got)
	}
}

func TestGHShimWebFallbackSkipsCustomGitHubBaseURL(t *testing.T) {
	ctx := context.Background()
	configPath := seedGHShimRepo(t, ctx)
	rawServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("web fallback should not hit raw server for custom GitHub base URL")
	}))
	defer rawServer.Close()
	dir := t.TempDir()
	ghPath := filepath.Join(dir, "gh")
	if err := os.WriteFile(ghPath, []byte("#!/bin/sh\necho fallback:$*\n"), 0o755); err != nil {
		t.Fatalf("write fake gh: %v", err)
	}
	t.Setenv("GITCRAWL_GH_PATH", ghPath)
	t.Setenv("GITCRAWL_GH_RAW_BASE_URL", rawServer.URL)
	t.Setenv("GITCRAWL_GITHUB_BASE_URL", "https://ghe.example/api/v3")

	run := New()
	var stdout bytes.Buffer
	run.Stdout = &stdout
	err := run.Run(ctx, []string{"--config", configPath, "gh", "--web-fallback", "api", "repos/openclaw/openclaw/contents/README.md?ref=abc123"})
	if err != nil {
		t.Fatalf("custom base fallback: %v", err)
	}
	if got := strings.TrimSpace(stdout.String()); got != "fallback:api repos/openclaw/openclaw/contents/README.md?ref=abc123" {
		t.Fatalf("stdout = %q", got)
	}
}

func TestGHShimWebFallbackAllowsExplicitGitHubHostname(t *testing.T) {
	ctx := context.Background()
	configPath := seedGHShimRepo(t, ctx)
	rawServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/openclaw/openclaw/abc123/README.md" {
			t.Fatalf("raw path = %s", r.URL.Path)
		}
		_, _ = w.Write([]byte("readme\n"))
	}))
	defer rawServer.Close()
	dir := t.TempDir()
	countPath := filepath.Join(dir, "count")
	ghPath := filepath.Join(dir, "gh")
	if err := os.WriteFile(ghPath, []byte("#!/bin/sh\necho called > \"$GH_SHIM_COUNT\"\necho fallback:$*\n"), 0o755); err != nil {
		t.Fatalf("write fake gh: %v", err)
	}
	t.Setenv("GITCRAWL_GH_PATH", ghPath)
	t.Setenv("GH_SHIM_COUNT", countPath)
	t.Setenv("GITCRAWL_GH_RAW_BASE_URL", rawServer.URL)
	t.Setenv("GITCRAWL_GITHUB_BASE_URL", "https://ghe.example/api/v3")

	run := New()
	var stdout bytes.Buffer
	run.Stdout = &stdout
	err := run.Run(ctx, []string{"--config", configPath, "gh", "--web-fallback", "api", "--hostname", "github.com", "repos/openclaw/openclaw/contents/README.md?ref=abc123"})
	if err != nil {
		t.Fatalf("explicit github hostname: %v", err)
	}
	if strings.Contains(stdout.String(), "fallback:") {
		t.Fatalf("used backend gh instead of web: %q", stdout.String())
	}
	if _, err := os.Stat(countPath); !os.IsNotExist(err) {
		t.Fatalf("fake gh should not be called: %v", err)
	}
}

func TestGHShimWebFallbackSkipsAPIOutputModifiers(t *testing.T) {
	ctx := context.Background()
	configPath := seedGHShimRepo(t, ctx)
	rawServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("web fallback should not hit raw server for --silent")
	}))
	defer rawServer.Close()
	dir := t.TempDir()
	ghPath := filepath.Join(dir, "gh")
	if err := os.WriteFile(ghPath, []byte("#!/bin/sh\necho fallback:$*\n"), 0o755); err != nil {
		t.Fatalf("write fake gh: %v", err)
	}
	t.Setenv("GITCRAWL_GH_PATH", ghPath)
	t.Setenv("GITCRAWL_GH_RAW_BASE_URL", rawServer.URL)

	run := New()
	var stdout bytes.Buffer
	run.Stdout = &stdout
	err := run.Run(ctx, []string{"--config", configPath, "gh", "--web-fallback", "api", "--silent", "repos/openclaw/openclaw/contents/README.md?ref=abc123"})
	if err != nil {
		t.Fatalf("silent fallback: %v", err)
	}
	if got := strings.TrimSpace(stdout.String()); got != "fallback:api --silent repos/openclaw/openclaw/contents/README.md?ref=abc123" {
		t.Fatalf("stdout = %q", got)
	}
}

func TestGHShimWebFallbackSkipsExplicitColoredPRDiff(t *testing.T) {
	ctx := context.Background()
	configPath := seedGHShimRepo(t, ctx)
	webServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("web fallback should not hit diff endpoint for --color=always")
	}))
	defer webServer.Close()
	dir := t.TempDir()
	ghPath := filepath.Join(dir, "gh")
	if err := os.WriteFile(ghPath, []byte("#!/bin/sh\necho fallback:$*\n"), 0o755); err != nil {
		t.Fatalf("write fake gh: %v", err)
	}
	t.Setenv("GITCRAWL_GH_PATH", ghPath)
	t.Setenv("GITCRAWL_GH_WEB_BASE_URL", webServer.URL)

	run := New()
	var stdout bytes.Buffer
	run.Stdout = &stdout
	err := run.Run(ctx, []string{"--config", configPath, "gh", "--web-fallback", "pr", "diff", "12", "-R", "openclaw/openclaw", "--color=always"})
	if err != nil {
		t.Fatalf("colored diff fallback: %v", err)
	}
	if got := strings.TrimSpace(stdout.String()); got != "fallback:pr diff 12 -R openclaw/openclaw --color=always" {
		t.Fatalf("stdout = %q", got)
	}
}

func TestGHShimWebFallbackSkipsImplicitGHESRemotePRDiff(t *testing.T) {
	ctx := context.Background()
	configPath := seedGHShimRepo(t, ctx)
	webServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("web fallback should not hit diff endpoint for GHES remote")
	}))
	defer webServer.Close()
	dir := t.TempDir()
	if err := runGit(ctx, dir, "init"); err != nil {
		t.Fatalf("git init: %v", err)
	}
	if err := runGit(ctx, dir, "remote", "add", "origin", "https://ghe.example/openclaw/openclaw.git"); err != nil {
		t.Fatalf("git remote add: %v", err)
	}
	oldwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	defer func() {
		if err := os.Chdir(oldwd); err != nil {
			t.Fatalf("restore cwd: %v", err)
		}
	}()
	ghPath := filepath.Join(dir, "gh")
	if err := os.WriteFile(ghPath, []byte("#!/bin/sh\necho fallback:$*\n"), 0o755); err != nil {
		t.Fatalf("write fake gh: %v", err)
	}
	t.Setenv("GITCRAWL_GH_PATH", ghPath)
	t.Setenv("GITCRAWL_GH_WEB_BASE_URL", webServer.URL)

	run := New()
	var stdout bytes.Buffer
	run.Stdout = &stdout
	err = run.Run(ctx, []string{"--config", configPath, "gh", "--web-fallback", "pr", "diff", "12", "--color=never"})
	if err != nil {
		t.Fatalf("ghes remote fallback: %v", err)
	}
	if got := strings.TrimSpace(stdout.String()); got != "fallback:pr diff 12 --color=never" {
		t.Fatalf("stdout = %q", got)
	}
}

func TestGHShimWebFallbackSkipsRedirectedPRDiff(t *testing.T) {
	ctx := context.Background()
	configPath := seedGHShimRepo(t, ctx)
	webServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/openclaw/openclaw/pull/12.diff":
			http.Redirect(w, r, "/login", http.StatusFound)
		case "/login":
			t.Fatalf("web fallback followed a login redirect")
		default:
			t.Fatalf("web path = %s", r.URL.Path)
		}
	}))
	defer webServer.Close()
	dir := t.TempDir()
	ghPath := filepath.Join(dir, "gh")
	if err := os.WriteFile(ghPath, []byte("#!/bin/sh\necho fallback:$*\n"), 0o755); err != nil {
		t.Fatalf("write fake gh: %v", err)
	}
	t.Setenv("GITCRAWL_GH_PATH", ghPath)
	t.Setenv("GITCRAWL_GH_WEB_BASE_URL", webServer.URL)

	run := New()
	var stdout bytes.Buffer
	run.Stdout = &stdout
	err := run.Run(ctx, []string{"--config", configPath, "gh", "--web-fallback", "pr", "diff", "12", "-R", "openclaw/openclaw", "--color=never"})
	if err != nil {
		t.Fatalf("redirect diff fallback: %v", err)
	}
	if got := strings.TrimSpace(stdout.String()); got != "fallback:pr diff 12 -R openclaw/openclaw --color=never" {
		t.Fatalf("stdout = %q", got)
	}
}

func TestGHShimWebFallbackSkipsOversizedResponses(t *testing.T) {
	ctx := context.Background()
	configPath := seedGHShimRepo(t, ctx)
	rawServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(bytes.Repeat([]byte("x"), 64*1024*1024+1))
	}))
	defer rawServer.Close()
	dir := t.TempDir()
	ghPath := filepath.Join(dir, "gh")
	if err := os.WriteFile(ghPath, []byte("#!/bin/sh\necho fallback:$*\n"), 0o755); err != nil {
		t.Fatalf("write fake gh: %v", err)
	}
	t.Setenv("GITCRAWL_GH_PATH", ghPath)
	t.Setenv("GITCRAWL_GH_RAW_BASE_URL", rawServer.URL)

	run := New()
	var stdout bytes.Buffer
	run.Stdout = &stdout
	err := run.Run(ctx, []string{"--config", configPath, "gh", "--web-fallback", "api", "repos/openclaw/openclaw/contents/huge.bin?ref=abc123"})
	if err != nil {
		t.Fatalf("oversized fallback: %v", err)
	}
	if got := strings.TrimSpace(stdout.String()); got != "fallback:api repos/openclaw/openclaw/contents/huge.bin?ref=abc123" {
		t.Fatalf("stdout = %q", got)
	}
}

func testGitBlobSHA(body []byte) string {
	hash := sha1.New()
	_, _ = fmt.Fprintf(hash, "blob %d\x00", len(body))
	_, _ = hash.Write(body)
	return fmt.Sprintf("%x", hash.Sum(nil))
}
