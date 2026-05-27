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
	"os/exec"
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

func TestGHShimWebFallbackServesRunViewAndActionsAPI(t *testing.T) {
	ctx := context.Background()
	configPath := seedGHShimRepo(t, ctx)
	webServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/openclaw/openclaw/actions/runs/99":
			_, _ = w.Write([]byte(testGHWebRunHTML()))
		case "/openclaw/openclaw/actions/runs/99/job_groups_batch":
			_, _ = w.Write([]byte(`{"totalCount":3}`))
		case "/openclaw/openclaw/actions/runs/99/job/111":
			_, _ = w.Write([]byte(testGHWebJobHTML("Setup", "success")))
		case "/openclaw/openclaw/actions/runs/99/job/222":
			_, _ = w.Write([]byte(testGHWebJobHTML("Release", "failure")))
		case "/openclaw/openclaw/actions/runs/99/job/333":
			_, _ = w.Write([]byte(`<html><body></body></html>`))
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
	err := run.Run(ctx, []string{"--config", configPath, "gh", "--web-fallback", "run", "view", "99", "-R", "openclaw/openclaw", "--json", "databaseId,number,workflowName,displayTitle,status,conclusion,url,event,headBranch,headSha,createdAt,jobs"})
	if err != nil {
		t.Fatalf("web run view: %v", err)
	}
	var view map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &view); err != nil {
		t.Fatalf("decode run view: %v\n%s", err, stdout.String())
	}
	if view["databaseId"].(float64) != 99 || view["number"].(float64) != 1247 || view["workflowName"] != "Full Release Validation" || view["displayTitle"] != "Release main" {
		t.Fatalf("view basics = %#v", view)
	}
	if view["status"] != "completed" || view["conclusion"] != "failure" || view["event"] != "workflow_dispatch" {
		t.Fatalf("view state = %#v", view)
	}
	if view["headBranch"] != "main" || view["headSha"] != "989d449404b8f6c85c3bca54abb336a0870c60f7" || view["createdAt"] != "2026-05-26T22:26:15Z" {
		t.Fatalf("view ref = %#v", view)
	}
	jobs := view["jobs"].([]any)
	if len(jobs) != 3 {
		t.Fatalf("jobs = %#v", jobs)
	}
	second := jobs[1].(map[string]any)
	if second["databaseId"].(float64) != 222 || second["name"] != "Run release validation" || second["conclusion"] != "failure" {
		t.Fatalf("second job = %#v", second)
	}
	steps := second["steps"].([]any)
	if len(steps) != 1 || steps[0].(map[string]any)["name"] != "Release" || steps[0].(map[string]any)["conclusion"] != "failure" {
		t.Fatalf("steps = %#v", steps)
	}
	third := jobs[2].(map[string]any)
	if third["status"] != "queued" || third["conclusion"] != "" {
		t.Fatalf("third job = %#v", third)
	}

	stdout.Reset()
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq not installed")
	}
	err = run.Run(ctx, []string{"--config", configPath, "gh", "--web-fallback", "api", "repos/openclaw/openclaw/actions/runs/99/jobs", "--jq", "{total_count, jobs: [.jobs[] | {id,name,status,conclusion,html_url}]}"})
	if err != nil {
		t.Fatalf("web api jobs: %v", err)
	}
	var jobsPayload map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &jobsPayload); err != nil {
		t.Fatalf("decode jobs: %v\n%s", err, stdout.String())
	}
	if jobsPayload["total_count"].(float64) != 3 {
		t.Fatalf("jobs payload = %#v", jobsPayload)
	}
	apiJobs := jobsPayload["jobs"].([]any)
	apiFirst := apiJobs[0].(map[string]any)
	if apiFirst["id"].(float64) != 111 || apiFirst["html_url"] != webServer.URL+"/openclaw/openclaw/actions/runs/99/job/111" {
		t.Fatalf("api first job = %#v", apiFirst)
	}

	stdout.Reset()
	err = run.Run(ctx, []string{"--config", configPath, "gh", "--web-fallback", "api", "repos/openclaw/openclaw/actions/runs/99", "--jq", "{id:.id,html_url:.html_url,jobs_url:.jobs_url,status:.status}"})
	if err != nil {
		t.Fatalf("web api run: %v", err)
	}
	var apiRun map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &apiRun); err != nil {
		t.Fatalf("decode api run: %v\n%s", err, stdout.String())
	}
	if apiRun["id"].(float64) != 99 || apiRun["html_url"] != webServer.URL+"/openclaw/openclaw/actions/runs/99" || apiRun["jobs_url"] != "https://api.github.com/repos/openclaw/openclaw/actions/runs/99/jobs" || apiRun["status"] != "completed" {
		t.Fatalf("api run = %#v", apiRun)
	}
	if _, err := os.Stat(countPath); !os.IsNotExist(err) {
		t.Fatalf("fake gh should not be called: %v", err)
	}
}

func TestGHShimWebFallbackRunMetadataIgnoresCollapsedJobs(t *testing.T) {
	ctx := context.Background()
	configPath := seedGHShimRepo(t, ctx)
	webServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/openclaw/openclaw/actions/runs/123456":
			_, _ = w.Write([]byte(strings.ReplaceAll(testGHWebRunHTML(), "runs/99", "runs/123456")))
		case "/openclaw/openclaw/actions/runs/123456/job_groups_batch":
			_, _ = w.Write([]byte(`{"totalCount":4}`))
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
	err := run.Run(ctx, []string{"--config", configPath, "gh", "--web-fallback", "run", "view", "123456", "-R", "openclaw/openclaw", "--json", "status,conclusion"})
	if err != nil {
		t.Fatalf("web run view: %v", err)
	}
	var view map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &view); err != nil {
		t.Fatalf("decode run view: %v\n%s", err, stdout.String())
	}
	if view["status"] != "completed" || view["conclusion"] != "failure" {
		t.Fatalf("view state = %#v", view)
	}
	if _, err := os.Stat(countPath); !os.IsNotExist(err) {
		t.Fatalf("fake gh should not be called: %v", err)
	}
}

func TestGHShimWebFallbackJobCardsDoNotRequireJobPages(t *testing.T) {
	ctx := context.Background()
	webServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/openclaw/openclaw/actions/runs/123459":
			_, _ = w.Write([]byte(strings.ReplaceAll(testGHWebRunHTML(), "runs/99", "runs/123459")))
		case "/openclaw/openclaw/actions/runs/123459/job_groups_batch":
			_, _ = w.Write([]byte(`{"totalCount":3}`))
		default:
			t.Fatalf("unexpected job-detail fetch path = %s", r.URL.Path)
		}
	}))
	defer webServer.Close()
	t.Setenv("GITCRAWL_GH_WEB_BASE_URL", webServer.URL)

	run, _, err := fetchGHWebRunSnapshot(ctx, "openclaw", "openclaw", "123459", true, false)
	if err != nil {
		t.Fatalf("fetch run snapshot: %v", err)
	}
	if len(run.Jobs) != 3 || run.Jobs[1].Name != "Run release validation" || run.Jobs[1].Conclusion != "failure" {
		t.Fatalf("jobs = %#v", run.Jobs)
	}
}

func TestGHShimWebFallbackUsesRunPageJobRefsWhenGraphIsLazy(t *testing.T) {
	ctx := context.Background()
	webServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/openclaw/openclaw/actions/runs/123460":
			_, _ = w.Write([]byte(`<!doctype html><html><body>
<span class="PageHeader-parentLink-label">CI</span>
<script type="application/json" data-target="react-partial.embeddedData">{"props":{"jobGroupsFetchUrl":"/openclaw/openclaw/actions/runs/123460/job_groups_batch?attempt=1"}}</script>
<h1 class="PageHeader-title lh-default"><span><span class="markdown-title">ci: add package smoke test</span><span class="color-fg-muted" style="font-weight: 400">#30</span></span></h1>
<span class="h4 color-fg-default">Success</span>
<a href="/openclaw/openclaw/actions/runs/123460/job/111#step:27:2"><strong>Node 24</strong></a>
<a href="/openclaw/openclaw/actions/runs/123460/job/222#step:27:2"><strong>Node 22</strong></a>
</body></html>`))
		case "/openclaw/openclaw/actions/runs/123460/job_groups_batch":
			_, _ = w.Write([]byte(`{"totalCount":2,"hasMore":true,"jobGroups":[{"name":"Node 22","nonNested":{"jobs":[{"id":222,"displayName":"Node 22","status":"completed","conclusion":"success","href":"/openclaw/openclaw/actions/runs/123460/job/222"}]}}]}`))
		case "/openclaw/openclaw/actions/runs/123460/job/111":
			_, _ = w.Write([]byte(testGHWebJobPageHTML("Node 24", "success")))
		case "/openclaw/openclaw/actions/runs/123460/job/222":
			_, _ = w.Write([]byte(testGHWebJobPageHTML("Node 22", "success")))
		default:
			t.Fatalf("web path = %s", r.URL.Path)
		}
	}))
	defer webServer.Close()
	t.Setenv("GITCRAWL_GH_WEB_BASE_URL", webServer.URL)

	run, _, err := fetchGHWebRunSnapshot(ctx, "openclaw", "openclaw", "123460", true, false)
	if err != nil {
		t.Fatalf("fetch run snapshot: %v", err)
	}
	if len(run.Jobs) != 2 {
		t.Fatalf("jobs = %#v", run.Jobs)
	}
	if run.Jobs[0].Name != "Node 24" || run.Jobs[0].Status != "completed" || run.Jobs[0].Conclusion != "success" {
		t.Fatalf("first job = %#v", run.Jobs[0])
	}
	if run.Jobs[1].Name != "Node 22" || run.Jobs[1].Status != "completed" || run.Jobs[1].Conclusion != "success" {
		t.Fatalf("second job = %#v", run.Jobs[1])
	}
}

func TestGHShimWebFallbackLazyCompletedJobsRequireSteps(t *testing.T) {
	ctx := context.Background()
	configPath := seedGHShimRepo(t, ctx)
	webServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/openclaw/openclaw/actions/runs/123461":
			_, _ = w.Write([]byte(`<!doctype html><html><body>
<span class="PageHeader-parentLink-label">CI</span>
<script type="application/json" data-target="react-partial.embeddedData">{"props":{"jobGroupsFetchUrl":"/openclaw/openclaw/actions/runs/123461/job_groups_batch?attempt=1"}}</script>
<h1 class="PageHeader-title lh-default"><span><span class="markdown-title">ci</span><span class="color-fg-muted" style="font-weight: 400">#30</span></span></h1>
<span class="h4 color-fg-default">Success</span>
<a href="/openclaw/openclaw/actions/runs/123461/job/111"><strong>Node 24</strong></a>
</body></html>`))
		case "/openclaw/openclaw/actions/runs/123461/job_groups_batch":
			_, _ = w.Write([]byte(`{"totalCount":1,"hasMore":false,"jobGroups":[]}`))
		case "/openclaw/openclaw/actions/runs/123461/job/111":
			_, _ = w.Write([]byte(`<html><body><span class="two-line-wrapping">Node 24</span><svg aria-label="completed successfully: "></svg></body></html>`))
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
	err := run.Run(ctx, []string{"--config", configPath, "gh", "--web-fallback", "run", "view", "123461", "-R", "openclaw/openclaw", "--json", "jobs"})
	if err != nil {
		t.Fatalf("fallback run view: %v", err)
	}
	if !strings.Contains(stdout.String(), "fallback:run view 123461") {
		t.Fatalf("expected fake gh fallback, got %q", stdout.String())
	}
	if _, err := os.Stat(countPath); err != nil {
		t.Fatalf("fake gh was not called: %v", err)
	}
}

func TestGHShimWebFallbackJobsFallThroughWithoutJobMarkup(t *testing.T) {
	ctx := context.Background()
	configPath := seedGHShimRepo(t, ctx)
	webServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/openclaw/openclaw/actions/runs/123457" {
			t.Fatalf("web path = %s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`<!doctype html><html><body>
<span class="PageHeader-parentLink-label">Full Release Validation</span>
<h1 class="PageHeader-title lh-default"><span><span class="markdown-title">Release main</span><span class="color-fg-muted" style="font-weight: 400">#1247</span></span></h1>
<span class="h4 color-fg-default">Failure</span>
</body></html>`))
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
	err := run.Run(ctx, []string{"--config", configPath, "gh", "--web-fallback", "run", "view", "123457", "-R", "openclaw/openclaw", "--json", "jobs"})
	if err != nil {
		t.Fatalf("fallback run view: %v", err)
	}
	if !strings.Contains(stdout.String(), "fallback:run view 123457") {
		t.Fatalf("expected fake gh fallback, got %q", stdout.String())
	}
	if _, err := os.Stat(countPath); err != nil {
		t.Fatalf("fake gh was not called: %v", err)
	}
}

func TestGHShimWebFallbackJobsFallThroughWithoutCompletedJobSteps(t *testing.T) {
	ctx := context.Background()
	configPath := seedGHShimRepo(t, ctx)
	webServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/openclaw/openclaw/actions/runs/123458":
			_, _ = w.Write([]byte(strings.ReplaceAll(testGHWebRunHTML(), "runs/99", "runs/123458")))
		case "/openclaw/openclaw/actions/runs/123458/job_groups_batch":
			_, _ = w.Write([]byte(`{"totalCount":3}`))
		case "/openclaw/openclaw/actions/runs/123458/job/111":
			_, _ = w.Write([]byte(`<html><body></body></html>`))
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
	err := run.Run(ctx, []string{"--config", configPath, "gh", "--web-fallback", "run", "view", "123458", "-R", "openclaw/openclaw", "--json", "jobs"})
	if err != nil {
		t.Fatalf("fallback run view: %v", err)
	}
	if !strings.Contains(stdout.String(), "fallback:run view 123458") {
		t.Fatalf("expected fake gh fallback, got %q", stdout.String())
	}
	if _, err := os.Stat(countPath); err != nil {
		t.Fatalf("fake gh was not called: %v", err)
	}
}

func TestGHShimWebFallbackSkipsUnsupportedRunViewFields(t *testing.T) {
	ctx := context.Background()
	app := New()
	if _, ok := app.parseGHWebRunViewArgs(ctx, []string{"99", "-R", "openclaw/openclaw", "--json", "updatedAt"}); ok {
		t.Fatal("updatedAt should fall through until web can prove an equivalent value")
	}
	if _, ok := app.parseGHWebRunViewArgs(ctx, []string{"99", "-R", "openclaw/openclaw", "--json", "id"}); ok {
		t.Fatal("id should fall through because gh run view only supports databaseId")
	}
	if _, ok := app.parseGHWebRunViewArgs(ctx, []string{"99", "-R", "openclaw/openclaw", "--job", "111", "--json", "status"}); ok {
		t.Fatal("--job logs should fall through to gh")
	}
	if _, ok := parseGHWebAPIJobsArgs([]string{"repos/openclaw/openclaw/actions/runs/99/jobs?per_page=1&page=2"}); ok {
		t.Fatal("paginated jobs API queries should fall through to gh")
	}
	if ghWebJQFieldsSupported(`.jobs[] | {id,workflow_name}`, map[string]bool{"jobs": true, "id": true}) {
		t.Fatal("unsupported jq shorthand fields should fall through to gh")
	}
	if ghWebJQFieldsSupported(`{id:.id, workflow_id:.["workflow_id"]}`, map[string]bool{"id": true}) {
		t.Fatal("unsupported jq string-index fields should fall through to gh")
	}
	if ghWebJQFieldsSupported(`.jobs[]["runner_name"]`, map[string]bool{"jobs": true}) {
		t.Fatal("unsupported nested jq string-index fields should fall through to gh")
	}
	if ghWebJQFieldsSupported(`.database_id`, map[string]bool{"id": true}) {
		t.Fatal("unsupported REST fields should fall through to gh")
	}
	if ghWebJQFieldsSupported(`{workflow_id, run:{id,status}}`, map[string]bool{"id": true, "status": true}) {
		t.Fatal("unsupported nested jq shorthand fields should fall through to gh")
	}
	if ghWebJQFieldsSupported(`{status, hasWorkflow: has("workflow_id")}`, map[string]bool{"status": true}) {
		t.Fatal("jq object introspection should fall through to gh")
	}
	if ghWebJQFieldsSupported(`.`, map[string]bool{"status": true}) {
		t.Fatal("jq identity should fall through to gh")
	}
	if ghWebJQJobsProjectionSupported(`.jobs[]`) {
		t.Fatal("whole job object projections should fall through to gh")
	}
	if ghWebJQJobsProjectionSupported(`{total_count, jobs}`) {
		t.Fatal("whole jobs array shorthand should fall through to gh")
	}
	if ghWebJQJobsProjectionSupported(`.jobs[] | length`) {
		t.Fatal("job object length should fall through to gh")
	}
	if ghWebJQJobsProjectionSupported(`.jobs[] as $j | $j`) {
		t.Fatal("job variable re-emits should fall through to gh")
	}
	if ghWebJQJobsProjectionSupported(`.["jobs"][]`) {
		t.Fatal("bracket whole job projections should fall through to gh")
	}
	if ghWebJQJobsProjectionSupported(`{jobs: .["jobs"]}`) {
		t.Fatal("bracket whole jobs array projections should fall through to gh")
	}
	if ghWebJQJobsProjectionSupported(`.jobs | sort_by(.name)`) {
		t.Fatal("job transforms should fall through to gh")
	}
	if ghWebJQJobsProjectionSupported(`.jobs[]?`) {
		t.Fatal("optional whole job object projections should fall through to gh")
	}
	if ghWebJQFieldsSupported(`.jobs | map(select(.conclusion=="failure"))`, map[string]bool{"jobs": true, "conclusion": true}) {
		t.Fatal("jq whole-object transforms should fall through to gh")
	}
	if !ghWebJQJobsProjectionSupported(`.jobs[] | {id,name,status}`) {
		t.Fatal("projected job fields should be supported")
	}
	if !ghWebJQJobsProjectionSupported(`.jobs[] | [.status,.conclusion] | @tsv`) {
		t.Fatal("projected job arrays should be supported")
	}
	if !ghWebJQNeedsJobDetails(`.jobs[] | {name,started_at,completed_at}`) {
		t.Fatal("shorthand time fields should require job details")
	}
	if !ghWebJQNeedsJobDetails(`.jobs[] | {name,steps}`) {
		t.Fatal("shorthand steps should require job details")
	}
}

func TestGHShimWebFallbackSkipsUnknownCompletedJobConclusion(t *testing.T) {
	if _, _, ok := parseGHWebJobState(`<streaming-graph-job data-concluded="true"><svg aria-label="completed mysteriously: "></svg></streaming-graph-job>`); ok {
		t.Fatal("unknown completed job conclusion should fall through to gh")
	}
	status, conclusion, ok := parseGHWebJobState(`<streaming-graph-job data-concluded="true"><svg aria-label="action required: "></svg></streaming-graph-job>`)
	if !ok || status != "completed" || conclusion != "action_required" {
		t.Fatalf("action required conclusion = %q %q %v", status, conclusion, ok)
	}
	status, conclusion, ok = parseGHWebJobState(`<streaming-graph-job data-concluded="true"><svg aria-label="startup failure: "></svg></streaming-graph-job>`)
	if !ok || status != "completed" || conclusion != "startup_failure" {
		t.Fatalf("startup failure conclusion = %q %q %v", status, conclusion, ok)
	}
	status, conclusion, ok = parseGHWebJobState(`<streaming-graph-job><svg aria-label="Pending: "></svg></streaming-graph-job>`)
	if !ok || status != "pending" || conclusion != "" {
		t.Fatalf("pending status = %q %q %v", status, conclusion, ok)
	}
	status, conclusion, ok = parseGHWebJobState(`<streaming-graph-job><svg aria-label="Requested: "></svg></streaming-graph-job>`)
	if !ok || status != "requested" || conclusion != "" {
		t.Fatalf("requested status = %q %q %v", status, conclusion, ok)
	}
}

func TestGHShimWebFallbackRunStateConclusions(t *testing.T) {
	status, conclusion := ghWebStatusConclusionFromLabel("Startup failure")
	if status != "completed" || conclusion != "startup_failure" {
		t.Fatalf("startup failure run state = %q %q", status, conclusion)
	}
	status, conclusion = ghWebStatusConclusionFromLabel("Action required")
	if status != "completed" || conclusion != "action_required" {
		t.Fatalf("action required run state = %q %q", status, conclusion)
	}
}

func TestGHShimWebFallbackJobStateFromSteps(t *testing.T) {
	status, conclusion := ghWebJobStateFromSteps([]ghWebRunStep{{Status: "completed", Conclusion: "success"}})
	if status != "completed" || conclusion != "success" {
		t.Fatalf("success job state = %q %q", status, conclusion)
	}
	status, conclusion = ghWebJobStateFromSteps([]ghWebRunStep{{Status: "completed", Conclusion: "success"}, {Status: "completed", Conclusion: "skipped"}})
	if status != "completed" || conclusion != "success" {
		t.Fatalf("success with skipped step state = %q %q", status, conclusion)
	}
	status, conclusion = ghWebJobStateFromSteps([]ghWebRunStep{{Status: "completed", Conclusion: "success"}, {Status: "completed", Conclusion: "failure"}})
	if status != "completed" || conclusion != "failure" {
		t.Fatalf("failure job state = %q %q", status, conclusion)
	}
	status, conclusion = ghWebJobStateFromSteps([]ghWebRunStep{{Status: "in_progress"}})
	if status != "in_progress" || conclusion != "" {
		t.Fatalf("running job state = %q %q", status, conclusion)
	}
}

func TestGHShimWebFallbackJobPageStateUsesHeader(t *testing.T) {
	status, conclusion, ok := parseGHWebJobPageState(`<html><body>
<button aria-label="Subscribe"></button>
<span class="PageHeader-leadingVisual actions-workflow-runs-status">
<svg aria-label="completed successfully: "></svg>
</span>
</body></html>`)
	if !ok || status != "completed" || conclusion != "success" {
		t.Fatalf("job page state = %q %q %v", status, conclusion, ok)
	}
}

func TestGHShimWebFallbackRESTJobsUseNullForMissingTimes(t *testing.T) {
	run := ghWebRunSnapshot{
		Owner: "openclaw",
		Repo:  "openclaw",
		Jobs: []ghWebRunJob{{
			ID:     1,
			Name:   "Queued",
			Status: "queued",
			Steps: []ghWebRunStep{{
				Name:   "Pending",
				Number: 1,
				Status: "queued",
			}},
		}},
	}
	job := run.ghAPIJobsPayload()[0]
	if job["started_at"] != nil || job["completed_at"] != nil {
		t.Fatalf("job times = %#v %#v", job["started_at"], job["completed_at"])
	}
	step := job["steps"].([]map[string]any)[0]
	if step["started_at"] != nil || step["completed_at"] != nil {
		t.Fatalf("step times = %#v %#v", step["started_at"], step["completed_at"])
	}
}

func TestGHShimWebFallbackRESTJobsUsesDefaultPageSize(t *testing.T) {
	run := ghWebRunSnapshot{Owner: "openclaw", Repo: "openclaw"}
	for index := 0; index < 31; index++ {
		run.Jobs = append(run.Jobs, ghWebRunJob{ID: int64(index + 1), Name: fmt.Sprintf("Job %d", index+1), Status: "completed", Conclusion: "success"})
	}
	if got := len(run.ghAPIJobsPagePayload(30)); got != 30 {
		t.Fatalf("default page jobs = %d", got)
	}
	if got := len(run.ghAPIJobsPayload()); got != 31 {
		t.Fatalf("full jobs = %d", got)
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

func testGHWebRunHTML() string {
	return `<!doctype html>
<html>
<head><title>Release main Fix #12 · openclaw/openclaw@989d449 · GitHub</title></head>
<body>
<span class="PageHeader-parentLink-label"> Full Release Validation</span>
<script type="application/json" data-target="react-partial.embeddedData">{"props":{"jobGroupsFetchUrl":"/openclaw/openclaw/actions/runs/99/job_groups_batch"}}</script>
<h1 class="PageHeader-title lh-default"><span><span class="markdown-title">Release main</span><span class="color-fg-muted" style="font-weight: 400">#1247</span></span></h1>
<span class="h4 color-fg-default">Failure</span>
<relative-time datetime="2026-05-26T22:26:15Z">May 26, 2026 22:26</relative-time>
<a href="/openclaw/openclaw/commit/989d449404b8f6c85c3bca54abb336a0870c60f7">989d449</a>
<a class="d-inline-block branch-name" title="main" href="/some-fork/openclaw/tree/refs/heads/main">main</a>
<div class="text-small color-fg-muted">on: workflow_dispatch</div>
<streaming-graph-job data-concluded="true">
<a href="/openclaw/openclaw/actions/runs/99/job/111">
<svg aria-label="completed successfully: "></svg>
<span data-target="streaming-graph-job.name">Resolve target ref</span>
</a>
<tool-tip>Resolve target ref</tool-tip>
</streaming-graph-job>
<streaming-graph-job data-concluded="true">
<a href="/openclaw/openclaw/actions/runs/99/job/222">
<svg aria-label="failed: "></svg>
<span data-target="streaming-graph-job.name">Run release validation</span>
</a>
<tool-tip>Run release validation</tool-tip>
</streaming-graph-job>
<streaming-graph-job>
<a href="/openclaw/openclaw/actions/runs/99/job/333">
<svg aria-label="Queued: "></svg>
<span data-target="streaming-graph-job.name">Queued release summary</span>
</a>
<tool-tip>Queued release summary</tool-tip>
</streaming-graph-job>
</body>
</html>`
}

func testGHWebJobHTML(stepName, conclusion string) string {
	return `<html><body>
<check-step data-name="` + stepName + `" data-number="1" data-conclusion="` + conclusion + `" data-started-at="2026-05-26T22:26:18Z" data-completed-at="2026-05-26T22:26:29Z"></check-step>
</body></html>`
}

func testGHWebJobPageHTML(name, conclusion string) string {
	return `<html><body>
	<span class="h4 color-fg-default">Success</span>
	<span class="two-line-wrapping">` + name + `</span>
	<span class="PageHeader-leadingVisual actions-workflow-runs-status">
	<svg aria-label="completed successfully: "></svg>
	</span>
	<check-step data-name="Set up job" data-number="1" data-conclusion="` + conclusion + `" data-started-at="2026-05-26T23:54:09Z" data-completed-at="2026-05-26T23:54:12Z"></check-step>
	</body></html>`
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
