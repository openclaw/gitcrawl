package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/openclaw/gitcrawl/internal/config"
	gh "github.com/openclaw/gitcrawl/internal/github"
	"github.com/openclaw/gitcrawl/internal/store"
)

func TestGHShimCachesReadOnlyFallbackCommands(t *testing.T) {
	ctx := context.Background()
	configPath := seedGHShimRepo(t, ctx)
	dir := t.TempDir()
	countPath := filepath.Join(dir, "count")
	ghPath := filepath.Join(dir, "gh")
	script := `#!/bin/sh
count=0
if [ -f "$GH_SHIM_COUNT" ]; then
  count=$(cat "$GH_SHIM_COUNT")
fi
count=$((count + 1))
printf "%s" "$count" > "$GH_SHIM_COUNT"
echo "call-$count:$*"
`
	if err := os.WriteFile(ghPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake gh: %v", err)
	}
	t.Setenv("GITCRAWL_GH_PATH", ghPath)
	t.Setenv("GH_SHIM_COUNT", countPath)
	t.Setenv("GH_REPO", "cache-test/"+filepath.Base(dir))
	t.Setenv("GITCRAWL_GH_CACHE_TTL", "1m")

	run := New()
	var stdout bytes.Buffer
	run.Stdout = &stdout
	args := []string{"--config", configPath, "gh", "run", "view", "123", "-R", "openclaw/openclaw", "--json", "status"}
	if err := run.Run(ctx, args); err != nil {
		t.Fatalf("first cached read: %v", err)
	}
	first := stdout.String()
	stdout.Reset()
	if err := run.Run(ctx, args); err != nil {
		t.Fatalf("second cached read: %v", err)
	}
	if second := stdout.String(); second != first {
		t.Fatalf("cached output changed: first=%q second=%q", first, second)
	}
	countData, err := os.ReadFile(countPath)
	if err != nil {
		t.Fatalf("read count: %v", err)
	}
	if strings.TrimSpace(string(countData)) != "1" {
		t.Fatalf("fake gh call count = %q, want 1", countData)
	}

	stdout.Reset()
	if err := run.Run(ctx, []string{"--config", configPath, "gh", "xcache", "stats", "--json"}); err != nil {
		t.Fatalf("xcache stats: %v", err)
	}
	var stats map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &stats); err != nil {
		t.Fatalf("decode stats: %v\n%s", err, stdout.String())
	}
	if int(stats["entries"].(float64)) != 1 {
		t.Fatalf("stats = %#v", stats)
	}
	counters := stats["counters"].(map[string]any)
	if int(counters["backend_misses"].(float64)) != 1 || int(counters["fallback_hits"].(float64)) != 1 {
		t.Fatalf("counters = %#v", counters)
	}
	if stats["hit_rate_percent"].(float64) != 50 {
		t.Fatalf("hit rate = %#v", stats["hit_rate_percent"])
	}

	stdout.Reset()
	if err := run.Run(ctx, []string{"--config", configPath, "gh", "xcache", "reset", "--json"}); err != nil {
		t.Fatalf("xcache reset: %v", err)
	}
	stdout.Reset()
	if err := run.Run(ctx, []string{"--config", configPath, "gh", "xcache", "stats", "--json"}); err != nil {
		t.Fatalf("xcache stats after reset: %v", err)
	}
	if err := json.Unmarshal(stdout.Bytes(), &stats); err != nil {
		t.Fatalf("decode reset stats: %v\n%s", err, stdout.String())
	}
	counters = stats["counters"].(map[string]any)
	if int(counters["backend_misses"].(float64)) != 0 || int(counters["fallback_hits"].(float64)) != 0 {
		t.Fatalf("reset counters = %#v", counters)
	}

	stdout.Reset()
	if err := run.Run(ctx, []string{"--config", configPath, "gh", "xcache", "keys", "--json"}); err != nil {
		t.Fatalf("xcache keys: %v", err)
	}
	var keys []map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &keys); err != nil {
		t.Fatalf("decode keys: %v\n%s", err, stdout.String())
	}
	if len(keys) != 1 || keys[0]["command"] != "run view" {
		t.Fatalf("keys = %#v", keys)
	}

	stdout.Reset()
	if err := run.Run(ctx, []string{"--config", configPath, "gh", "xcache", "flush", "--json"}); err != nil {
		t.Fatalf("xcache flush: %v", err)
	}
	var flushed map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &flushed); err != nil {
		t.Fatalf("decode flush: %v\n%s", err, stdout.String())
	}
	if int(flushed["removed"].(float64)) != 1 {
		t.Fatalf("flushed = %#v", flushed)
	}
}

func TestGHShimLiveControlsAndLivenessTombstones(t *testing.T) {
	ctx := context.Background()
	configPath := seedGHShimRepo(t, ctx)
	dir := t.TempDir()
	countPath := filepath.Join(dir, "count")
	ghPath := filepath.Join(dir, "gh")
	script := `#!/bin/sh
count=0
if [ -f "$GH_SHIM_COUNT" ]; then
  count=$(cat "$GH_SHIM_COUNT")
fi
count=$((count + 1))
printf "%s" "$count" > "$GH_SHIM_COUNT"
echo "live-$count:$*"
`
	if err := os.WriteFile(ghPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake gh: %v", err)
	}
	t.Setenv("GITCRAWL_GH_PATH", ghPath)
	t.Setenv("GH_SHIM_COUNT", countPath)

	app := New()
	app.configPath = configPath
	readArgs := []string{"release", "view", "v1.0.0", "-R", "openclaw/openclaw"}
	cacheDir, err := app.ghCommandCacheDir()
	if err != nil {
		t.Fatalf("cache dir: %v", err)
	}
	entryPath := filepath.Join(cacheDir, app.ghCommandCacheKey(ctx, readArgs)+".json")
	if err := writeGHCommandCache(entryPath, ghCommandCacheEntry{
		CreatedAt: time.Now().UTC(),
		Args:      readArgs,
		Tags:      app.ghCommandCacheTags(ctx, readArgs),
		ExitCode:  0,
		Stdout:    "cached-release\n",
	}); err != nil {
		t.Fatalf("write cache: %v", err)
	}
	if err := app.recordGHLivenessTombstone(ctx, []string{"release", "create", "v1.0.0", "-R", "openclaw/openclaw"}); err != nil {
		t.Fatalf("record tombstone: %v", err)
	}

	run := New()
	var stdout, stderr bytes.Buffer
	run.Stdout = &stdout
	run.Stderr = &stderr
	if err := run.Run(ctx, []string{"--config", configPath, "gh", "release", "view", "v1.0.0", "-R", "openclaw/openclaw"}); err != nil {
		t.Fatalf("liveness read: %v\nstderr=%s", err, stderr.String())
	}
	if !strings.Contains(stdout.String(), "live-1:release view") || !strings.Contains(stderr.String(), "bypassing gh cache") {
		t.Fatalf("stdout=%q stderr=%q", stdout.String(), stderr.String())
	}

	stdout.Reset()
	stderr.Reset()
	if err := run.Run(ctx, []string{"--config", configPath, "gh", "--cached", "release", "view", "v1.0.0", "-R", "openclaw/openclaw"}); err != nil {
		t.Fatalf("cached read: %v\nstderr=%s", err, stderr.String())
	}
	if !strings.Contains(stdout.String(), "live-1:release view") || !strings.Contains(stderr.String(), "serving cached gh release view") {
		t.Fatalf("cached stdout=%q stderr=%q", stdout.String(), stderr.String())
	}

	stdout.Reset()
	stderr.Reset()
	if err := run.Run(ctx, []string{"--config", configPath, "gh", "--live", "release", "view", "v1.0.0", "-R", "openclaw/openclaw"}); err != nil {
		t.Fatalf("live read: %v\nstderr=%s", err, stderr.String())
	}
	if !strings.Contains(stdout.String(), "live-2:release view") {
		t.Fatalf("live stdout=%q stderr=%q", stdout.String(), stderr.String())
	}

	stdout.Reset()
	stderr.Reset()
	if err := run.Run(ctx, []string{"--config", configPath, "gh", "xcache", "stats", "--json"}); err != nil {
		t.Fatalf("stats: %v", err)
	}
	var stats map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &stats); err != nil {
		t.Fatalf("decode stats: %v\n%s", err, stdout.String())
	}
	if len(stats["liveness"].([]any)) == 0 {
		t.Fatalf("missing liveness stats: %#v", stats)
	}
	counters := stats["counters"].(map[string]any)
	if int(counters["live_bypasses"].(float64)) < 2 {
		t.Fatalf("counters = %#v", counters)
	}
}

func TestGHRunListBroadBranchUsesFallbackCache(t *testing.T) {
	ctx := context.Background()
	configPath := seedGHShimRepo(t, ctx)
	dir := t.TempDir()
	countPath := filepath.Join(dir, "count")
	ghPath := filepath.Join(dir, "gh")
	script := `#!/bin/sh
count=0
if [ -f "$GH_SHIM_COUNT" ]; then
  count=$(cat "$GH_SHIM_COUNT")
fi
count=$((count + 1))
printf "%s" "$count" > "$GH_SHIM_COUNT"
echo "real-gh:$*"
`
	if err := os.WriteFile(ghPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake gh: %v", err)
	}
	t.Setenv("GITCRAWL_GH_PATH", ghPath)
	t.Setenv("GH_SHIM_COUNT", countPath)
	t.Setenv("GITCRAWL_GH_CACHE_TTL", "1m")

	app := New()
	app.configPath = configPath
	cacheArgs := []string{"run", "list", "-R", "openclaw/openclaw", "--branch", "main"}
	cacheDir, err := app.ghCommandCacheDir()
	if err != nil {
		t.Fatalf("cache dir: %v", err)
	}
	entryPath := filepath.Join(cacheDir, app.ghCommandCacheKey(ctx, cacheArgs)+".json")
	if err := writeGHCommandCache(entryPath, ghCommandCacheEntry{
		CreatedAt: time.Now().UTC(),
		Args:      cacheArgs,
		Tags:      app.ghCommandCacheTags(ctx, cacheArgs),
		ExitCode:  0,
		Stdout:    "cached-run-list\n",
	}); err != nil {
		t.Fatalf("write cached run list: %v", err)
	}

	run := New()
	var stdout, stderr bytes.Buffer
	run.Stdout = &stdout
	run.Stderr = &stderr
	if err := run.Run(ctx, []string{"--config", configPath, "gh", "run", "list", "-R", "openclaw/openclaw", "--branch", "main"}); err != nil {
		t.Fatalf("broad run list: %v\nstderr=%s", err, stderr.String())
	}
	if !strings.Contains(stdout.String(), "cached-run-list") {
		t.Fatalf("stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
	if _, err := os.Stat(countPath); !os.IsNotExist(err) {
		t.Fatalf("fake gh should not be called, count stat err=%v", err)
	}
}

func TestGHAPIProjectionCacheReusesRawResponse(t *testing.T) {
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq not installed")
	}
	ctx := context.Background()
	configPath := seedGHShimRepo(t, ctx)
	dir := t.TempDir()
	countPath := filepath.Join(dir, "count")
	ghPath := filepath.Join(dir, "gh")
	script := `#!/bin/sh
count=0
if [ -f "$GH_SHIM_COUNT" ]; then
  count=$(cat "$GH_SHIM_COUNT")
fi
count=$((count + 1))
printf "%s" "$count" > "$GH_SHIM_COUNT"
printf '{"number":12,"title":"Manifest cache update"}\n'
`
	if err := os.WriteFile(ghPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake gh: %v", err)
	}
	t.Setenv("GITCRAWL_GH_PATH", ghPath)
	t.Setenv("GH_SHIM_COUNT", countPath)
	t.Setenv("GITCRAWL_GH_CACHE_TTL", "1m")

	for _, tc := range []struct {
		jq   string
		want string
	}{
		{".number", "12"},
		{".title", "Manifest cache update"},
	} {
		run := New()
		var stdout, stderr bytes.Buffer
		run.Stdout = &stdout
		run.Stderr = &stderr
		if err := run.Run(ctx, []string{"--config", configPath, "gh", "api", "repos/openclaw/openclaw/pulls/12", "--jq", tc.jq}); err != nil {
			t.Fatalf("gh api --jq %s: %v\nstderr=%s", tc.jq, err, stderr.String())
		}
		if got := strings.TrimSpace(stdout.String()); got != tc.want {
			t.Fatalf("projection %s = %q, want %q", tc.jq, got, tc.want)
		}
	}
	countData, err := os.ReadFile(countPath)
	if err != nil {
		t.Fatalf("read count: %v", err)
	}
	if strings.TrimSpace(string(countData)) != "1" {
		t.Fatalf("fake gh call count = %q, want 1", countData)
	}
}

func TestGHRunReadsHonorLivenessBeforeLocalCache(t *testing.T) {
	ctx := context.Background()
	configPath := seedGHShimRepo(t, ctx)
	dir := t.TempDir()
	countPath := filepath.Join(dir, "count")
	ghPath := filepath.Join(dir, "gh")
	script := `#!/bin/sh
count=0
if [ -f "$GH_SHIM_COUNT" ]; then
  count=$(cat "$GH_SHIM_COUNT")
fi
count=$((count + 1))
printf "%s" "$count" > "$GH_SHIM_COUNT"
echo "real-gh-$count:$*"
`
	if err := os.WriteFile(ghPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake gh: %v", err)
	}
	t.Setenv("GITCRAWL_GH_PATH", ghPath)
	t.Setenv("GH_SHIM_COUNT", countPath)

	app := New()
	app.configPath = configPath
	if err := app.recordGHLivenessTombstone(ctx, []string{"workflow", "run", "ci.yml", "-R", "openclaw/openclaw"}); err != nil {
		t.Fatalf("record workflow tombstone: %v", err)
	}

	run := New()
	var stdout, stderr bytes.Buffer
	run.Stdout = &stdout
	run.Stderr = &stderr
	if err := run.Run(ctx, []string{"--config", configPath, "gh", "run", "view", "99", "-R", "openclaw/openclaw"}); err != nil {
		t.Fatalf("run view: %v\nstderr=%s", err, stderr.String())
	}
	if !strings.Contains(stdout.String(), "real-gh-1:run view") || !strings.Contains(stderr.String(), "bypassing gh cache") {
		t.Fatalf("stdout=%q stderr=%q", stdout.String(), stderr.String())
	}

	stdout.Reset()
	stderr.Reset()
	if err := run.Run(ctx, []string{"--config", configPath, "gh", "run", "list", "-R", "openclaw/openclaw", "--commit", "abc123"}); err != nil {
		t.Fatalf("run list commit: %v\nstderr=%s", err, stderr.String())
	}
	if !strings.Contains(stdout.String(), "real-gh-2:run list") || !strings.Contains(stderr.String(), "bypassing gh cache") {
		t.Fatalf("stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
}

func TestGHShimPreservesGHJQWithoutExternalJQ(t *testing.T) {
	ctx := context.Background()
	configPath := seedGHShimRepo(t, ctx)
	dir := t.TempDir()
	countPath := filepath.Join(dir, "count")
	ghPath := filepath.Join(dir, "gh")
	script := `#!/bin/sh
count=0
if [ -f "$GH_SHIM_COUNT" ]; then
  IFS= read -r count < "$GH_SHIM_COUNT"
fi
count=$((count + 1))
printf "%s" "$count" > "$GH_SHIM_COUNT"
jq_expr=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --jq|-q)
      shift
      jq_expr="$1"
      ;;
    --jq=*)
      jq_expr="${1#--jq=}"
      ;;
    -q=*)
      jq_expr="${1#-q=}"
      ;;
  esac
  shift
done
case "$jq_expr" in
  .nameWithOwner)
    printf 'openclaw/gitcrawl\n'
    ;;
  .id)
    printf '123\n'
    ;;
  *)
    printf '{"nameWithOwner":"openclaw/gitcrawl","id":123}\n'
    ;;
esac
`
	if err := os.WriteFile(ghPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake gh: %v", err)
	}
	t.Setenv("GITCRAWL_GH_PATH", ghPath)
	t.Setenv("GH_SHIM_COUNT", countPath)
	t.Setenv("GITCRAWL_GH_CACHE_TTL", "1m")
	t.Setenv("PATH", "")

	run := New()
	var stdout bytes.Buffer
	run.Stdout = &stdout
	base := []string{"--config", configPath, "gh", "repo", "view", "openclaw/gitcrawl", "--json", "id,nameWithOwner"}
	if err := run.Run(ctx, append(append([]string{}, base...), "--jq", ".nameWithOwner")); err != nil {
		t.Fatalf("first jq read: %v", err)
	}
	if strings.TrimSpace(stdout.String()) != "openclaw/gitcrawl" {
		t.Fatalf("first jq output = %q", stdout.String())
	}
	stdout.Reset()
	if err := run.Run(ctx, append(append([]string{}, base...), "--jq", ".nameWithOwner")); err != nil {
		t.Fatalf("second jq read: %v", err)
	}
	if strings.TrimSpace(stdout.String()) != "openclaw/gitcrawl" {
		t.Fatalf("second jq output = %q", stdout.String())
	}
	countData, err := os.ReadFile(countPath)
	if err != nil {
		t.Fatalf("read count: %v", err)
	}
	if strings.TrimSpace(string(countData)) != "1" {
		t.Fatalf("fake gh call count = %q, want 1", countData)
	}
}

func TestGHRateLimitStateUsesObservedAPIHost(t *testing.T) {
	ctx := context.Background()
	configPath := seedGHShimRepo(t, ctx)
	t.Setenv("GITCRAWL_GITHUB_BASE_URL", "https://ghe.example/api/v3")
	t.Setenv("GITHUB_TOKEN", "custom-host-token")

	app := New()
	app.configPath = configPath
	if err := app.writeSharedRateLimit(ctx, "custom-host-token", gh.RateLimitSnapshot{
		Host:      "ghe.example",
		Limit:     5000,
		Remaining: 0,
		ResetAt:   time.Now().Add(time.Hour),
		Resource:  "core",
	}, "test"); err != nil {
		t.Fatalf("write rate limit state: %v", err)
	}
	if _, ok := app.sharedRateLimitStateForTokenHost("custom-host-token", "ghe.example"); !ok {
		t.Fatal("missing custom-host rate limit state")
	}
	if _, ok := app.sharedRateLimitStateForTokenHost("custom-host-token", "github.com"); ok {
		t.Fatal("custom-host rate limit state should not be stored under github.com")
	}
	if _, ok := app.sharedRateLimitStateForToken("custom-host-token"); ok {
		t.Fatal("default shim host should not read custom-host rate limit state")
	}
	if _, low := app.sharedRateLimitLowForArgs(ctx, []string{"api", "--hostname", "ghe.example", "rate_limit"}); !low {
		t.Fatal("command hostname should read custom-host rate limit state")
	}
}

func TestGHShimLowBudgetStaleUsesCommandHostname(t *testing.T) {
	ctx := context.Background()
	configPath := seedGHShimRepo(t, ctx)
	dir := t.TempDir()
	countPath := filepath.Join(dir, "count")
	ghPath := filepath.Join(dir, "gh")
	script := `#!/bin/sh
count=0
if [ -f "$GH_SHIM_COUNT" ]; then
  count=$(cat "$GH_SHIM_COUNT")
fi
count=$((count + 1))
printf "%s" "$count" > "$GH_SHIM_COUNT"
printf '{"name":"fresh"}\n'
`
	if err := os.WriteFile(ghPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake gh: %v", err)
	}
	t.Setenv("GITCRAWL_GH_PATH", ghPath)
	t.Setenv("GH_SHIM_COUNT", countPath)
	t.Setenv("GITHUB_TOKEN", "hostname-token")

	app := New()
	app.configPath = configPath
	args := []string{"api", "--hostname", "ghe.example", "repos/openclaw/gitcrawl"}
	cacheDir, err := app.ghCommandCacheDir()
	if err != nil {
		t.Fatalf("cache dir: %v", err)
	}
	entryPath := filepath.Join(cacheDir, app.ghCommandCacheKey(ctx, args)+".json")
	if err := writeGHCommandCache(entryPath, ghCommandCacheEntry{
		CreatedAt: time.Now().UTC().Add(-2 * time.Hour),
		Args:      args,
		ExitCode:  0,
		Stdout:    `{"name":"stale"}` + "\n",
	}); err != nil {
		t.Fatalf("write stale cache: %v", err)
	}
	if err := app.writeSharedRateLimit(ctx, "hostname-token", gh.RateLimitSnapshot{
		Host:      "github.com",
		Limit:     5000,
		Remaining: 0,
		ResetAt:   time.Now().Add(time.Hour),
		Resource:  "core",
	}, "test"); err != nil {
		t.Fatalf("write default rate limit state: %v", err)
	}

	run := New()
	var stdout, stderr bytes.Buffer
	run.Stdout = &stdout
	run.Stderr = &stderr
	if err := run.Run(ctx, []string{"--config", configPath, "gh", "api", "--hostname", "ghe.example", "repos/openclaw/gitcrawl"}); err != nil {
		t.Fatalf("hostname read: %v\nstderr=%s", err, stderr.String())
	}
	if !strings.Contains(stdout.String(), `"fresh"`) || strings.Contains(stdout.String(), `"stale"`) {
		t.Fatalf("stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
	countData, err := os.ReadFile(countPath)
	if err != nil {
		t.Fatalf("read count: %v", err)
	}
	if strings.TrimSpace(string(countData)) != "1" {
		t.Fatalf("fake gh call count = %q, want 1", countData)
	}
}

func TestGHShimServesLowBudgetStaleBeforeBackend(t *testing.T) {
	ctx := context.Background()
	configPath := seedGHShimRepo(t, ctx)
	dir := t.TempDir()
	countPath := filepath.Join(dir, "count")
	ghPath := filepath.Join(dir, "gh")
	script := `#!/bin/sh
count=0
if [ -f "$GH_SHIM_COUNT" ]; then
  count=$(cat "$GH_SHIM_COUNT")
fi
count=$((count + 1))
printf "%s" "$count" > "$GH_SHIM_COUNT"
echo "backend should not run" >&2
exit 9
`
	if err := os.WriteFile(ghPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake gh: %v", err)
	}
	t.Setenv("GITCRAWL_GH_PATH", ghPath)
	t.Setenv("GH_SHIM_COUNT", countPath)
	t.Setenv("GITHUB_TOKEN", "low-budget-token")

	app := New()
	app.configPath = configPath
	cacheArgs := []string{"repo", "view", "openclaw/gitcrawl", "--json", "nameWithOwner"}
	cacheDir, err := app.ghCommandCacheDir()
	if err != nil {
		t.Fatalf("cache dir: %v", err)
	}
	entryPath := filepath.Join(cacheDir, app.ghCommandCacheKey(ctx, cacheArgs)+".json")
	if err := writeGHCommandCache(entryPath, ghCommandCacheEntry{
		CreatedAt: time.Now().UTC().Add(-2 * time.Hour),
		Args:      cacheArgs,
		ExitCode:  0,
		Stdout:    `{"nameWithOwner":"openclaw/gitcrawl"}` + "\n",
	}); err != nil {
		t.Fatalf("write stale cache: %v", err)
	}
	if err := app.writeSharedRateLimit(ctx, "low-budget-token", gh.RateLimitSnapshot{
		Limit:     5000,
		Remaining: 0,
		ResetAt:   time.Now().Add(time.Hour),
		Resource:  "core",
	}, "test"); err != nil {
		t.Fatalf("write rate limit state: %v", err)
	}

	run := New()
	var stdout, stderr bytes.Buffer
	run.Stdout = &stdout
	run.Stderr = &stderr
	if err := run.Run(ctx, []string{"--config", configPath, "gh", "repo", "view", "openclaw/gitcrawl", "--json", "nameWithOwner"}); err != nil {
		t.Fatalf("low-budget stale read: %v\nstderr=%s", err, stderr.String())
	}
	if !strings.Contains(stdout.String(), "openclaw/gitcrawl") || !strings.Contains(stderr.String(), "shared GitHub rate limit low") {
		t.Fatalf("stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
	if data, err := os.ReadFile(countPath); err == nil && strings.TrimSpace(string(data)) != "" {
		t.Fatalf("backend ran unexpectedly: %q", data)
	}
	stdout.Reset()
	stderr.Reset()
	if err := run.Run(ctx, []string{"--config", configPath, "gh", "xcache", "stats", "--json"}); err != nil {
		t.Fatalf("stats: %v", err)
	}
	var stats map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &stats); err != nil {
		t.Fatalf("decode stats: %v\n%s", err, stdout.String())
	}
	counters := stats["counters"].(map[string]any)
	if int(counters["low_budget_stale_hits"].(float64)) != 1 {
		t.Fatalf("counters = %#v", counters)
	}
	if stats["rate_limit"] == nil {
		t.Fatalf("missing rate_limit: %#v", stats)
	}
}

func TestGHXCacheCommandsReportAndCleanCacheState(t *testing.T) {
	ctx := context.Background()
	configPath := seedGHShimRepo(t, ctx)
	app := New()
	app.configPath = configPath
	var stdout bytes.Buffer
	app.Stdout = &stdout
	dir, err := app.ghCommandCacheDir()
	if err != nil {
		t.Fatalf("cache dir: %v", err)
	}
	now := time.Now()
	freshPath := filepath.Join(dir, "fresh.json")
	expiredPath := filepath.Join(dir, "expired.json")
	if err := writeGHCommandCache(freshPath, ghCommandCacheEntry{CreatedAt: now.Add(-time.Minute), Args: []string{"api", "users/octocat"}, ExitCode: 0, Stdout: "{}"}); err != nil {
		t.Fatalf("write fresh cache: %v", err)
	}
	if err := writeGHCommandCache(expiredPath, ghCommandCacheEntry{CreatedAt: now.Add(-8 * 24 * time.Hour), Args: []string{"api", "users/octocat"}, ExitCode: 0, Stdout: "{}"}); err != nil {
		t.Fatalf("write expired cache: %v", err)
	}
	lockPath := filepath.Join(dir, "stale.lock")
	if err := os.WriteFile(lockPath, []byte("123\n"), 0o600); err != nil {
		t.Fatalf("write lock: %v", err)
	}
	old := now.Add(-3 * time.Minute)
	if err := os.Chtimes(lockPath, old, old); err != nil {
		t.Fatalf("age lock: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "broken.json"), []byte("{"), 0o600); err != nil {
		t.Fatalf("write broken entry: %v", err)
	}
	if _, ok := ghCommandCacheKeyInfoFromDirEntry(dir, mustDirEntry(t, dir, "broken.json")); ok {
		t.Fatal("broken cache entry should be ignored")
	}
	if err := app.incrementGHXCacheCounter("local_hits"); err != nil {
		t.Fatalf("increment hit: %v", err)
	}
	if err := app.incrementGHXCacheBackendMiss([]string{"api", "repos/openclaw/gitcrawl/actions/runs/1/jobs"}); err != nil {
		t.Fatalf("increment miss: %v", err)
	}
	if err := app.runGHXCache([]string{"stats", "--since", "2h"}); err != nil {
		t.Fatalf("stats: %v", err)
	}
	statsText := stdout.String()
	if !strings.Contains(statsText, "hit rate") || !strings.Contains(statsText, "Backend Misses by Route") {
		t.Fatalf("stats output = %q", statsText)
	}
	stdout.Reset()
	if err := app.runGHXCache([]string{"keys"}); err != nil {
		t.Fatalf("keys: %v", err)
	}
	if !strings.Contains(stdout.String(), "api users/octocat") {
		t.Fatalf("keys output = %q", stdout.String())
	}
	stdout.Reset()
	if err := app.runGHXCache([]string{"snapshot", "--reset"}); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if !strings.Contains(stdout.String(), "Reset xcache counters") {
		t.Fatalf("snapshot output = %q", stdout.String())
	}
	stdout.Reset()
	if err := app.runGHXCache([]string{"gc"}); err != nil {
		t.Fatalf("gc: %v", err)
	}
	if !strings.Contains(stdout.String(), "Removed 1 expired entrie(s), 1 stale lock(s)") {
		t.Fatalf("gc output = %q", stdout.String())
	}
	stdout.Reset()
	if err := app.runGHXCache([]string{"flush"}); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if !strings.Contains(stdout.String(), "Flushed") {
		t.Fatalf("flush output = %q", stdout.String())
	}
	if err := app.clearGHCommandCache(); err != nil {
		t.Fatalf("clear cache: %v", err)
	}
	stdout.Reset()
	if err := app.runGHXCache([]string{"--help"}); err != nil {
		t.Fatalf("xcache help: %v", err)
	}
	if !strings.Contains(stdout.String(), "Usage:") || !strings.Contains(stdout.String(), "snapshot") {
		t.Fatalf("help output = %q", stdout.String())
	}
	if err := app.runGHXCache([]string{"stats", "--since", "nope"}); err == nil {
		t.Fatal("invalid since should fail")
	}
	if err := app.runGHXCache([]string{"mystery"}); err == nil {
		t.Fatal("unknown xcache command should fail")
	}
}

func mustDirEntry(t *testing.T, dir, name string) os.DirEntry {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	for _, entry := range entries {
		if entry.Name() == name {
			return entry
		}
	}
	t.Fatalf("missing dir entry %s", name)
	return nil
}

func TestGHShimCachesGHXStyleReadOnlyFallbackCommands(t *testing.T) {
	for _, args := range [][]string{
		{"gh", "release", "view", "v1.2.3", "-R", "openclaw/openclaw"},
		{"gh", "workflow", "view", "ci.yml", "-R", "openclaw/openclaw"},
		{"gh", "secret", "list", "-R", "openclaw/openclaw"},
		{"gh", "variable", "list", "-R", "openclaw/openclaw"},
		{"gh", "ruleset", "list", "-R", "openclaw/openclaw"},
	} {
		if !cacheableGHRead(args[1:]) {
			t.Fatalf("%v should be cacheable", args)
		}
	}
}

func TestGHShimCommandAwareCacheTTLs(t *testing.T) {
	t.Setenv("GITCRAWL_GH_CACHE_TTL", "")
	if got := ghCommandCacheTTL([]string{"api", "users/octocat"}); got != 7*24*time.Hour {
		t.Fatalf("user api ttl = %s, want 7d", got)
	}
	if got := ghCommandCacheTTL([]string{"api", "graphql", "-f", "query={ viewer { login } }"}); got != 6*time.Hour {
		t.Fatalf("graphql api ttl = %s, want 6h", got)
	}
	if got := ghCommandCacheTTL([]string{"run", "view", "123", "--log"}); got != 12*time.Hour {
		t.Fatalf("run log ttl = %s, want 12h", got)
	}
	if got := ghCommandCacheTTL([]string{"run", "view", "123", "--job", "456"}); got != time.Minute {
		t.Fatalf("run job ttl = %s, want 1m", got)
	}
	if got := ghCommandCacheTTL([]string{"run", "list", "-R", "openclaw/openclaw"}); got != 30*time.Second {
		t.Fatalf("run list ttl = %s, want 30s", got)
	}
	if got := ghCommandCacheTTL([]string{"search", "issues", "cache"}); got != 15*time.Minute {
		t.Fatalf("search ttl = %s, want 15m", got)
	}
	if got := ghCommandCacheTTL([]string{"api", "-i", "repos/openclaw/openclaw/actions/runs/123/logs"}); got != 12*time.Hour {
		t.Fatalf("actions log api ttl = %s, want 12h", got)
	}
	if got := ghCommandCacheTTL([]string{"api", "repos/openclaw/openclaw/actions/runs/123"}); got != 30*time.Second {
		t.Fatalf("actions run api ttl = %s, want 30s", got)
	}
	if got := normalizeGHAPIRoute([]string{"--cache", "30s", "repos/openclaw/openclaw/actions/runs/123/jobs"}); got != "api repos/:owner/:repo/actions/runs/:id/jobs" {
		t.Fatalf("--cache route = %q", got)
	}
	if got := ghCommandCacheTTL([]string{"api", "--cache", "45s", "repos/openclaw/openclaw/actions/runs/123/jobs"}); got != 45*time.Second {
		t.Fatalf("--cache ttl = %s, want 45s", got)
	}
	if got := ghCommandCacheTTL([]string{"api", "--method", "GET", "search/issues", "-f", "q=repo:openclaw/openclaw created:2020-01-01..2020-01-31"}); got != 7*24*time.Hour {
		t.Fatalf("stable search ttl = %s, want 7d", got)
	}
	if got := ghCommandCacheTTL([]string{"api", "--method", "GET", "search/issues", "-f", "q=repo:openclaw/openclaw updated:2020-01-01..2020-01-31"}); got != 15*time.Minute {
		t.Fatalf("updated search ttl = %s, want 15m", got)
	}
	if got := ghCommandCacheTTL([]string{"api", "repos/openclaw/openclaw/pages"}); got != 30*time.Minute {
		t.Fatalf("pages api ttl = %s, want 30m", got)
	}
	if got := ghCommandCacheTTL([]string{"api", "repos/openclaw/openclaw/contents/README.md?ref=v0.2.0"}); got != 7*24*time.Hour {
		t.Fatalf("tagged contents api ttl = %s, want 7d", got)
	}
	if got := ghCommandCacheTTL([]string{"api", "repos/openclaw/openclaw/contents/README.md?ref=refs%2Ftags%2Fv0.2.0"}); got != 7*24*time.Hour {
		t.Fatalf("refs/tags contents api ttl = %s, want 7d", got)
	}
	if got := ghCommandCacheTTL([]string{"api", "repos/openclaw/openclaw/contents/README.md?ref=0123456789abcdef0123456789abcdef01234567"}); got != 7*24*time.Hour {
		t.Fatalf("sha contents api ttl = %s, want 7d", got)
	}
	if got := ghCommandCacheTTL([]string{"api", "repos/openclaw/openclaw/contents/README.md?ref=vnext"}); got != 30*time.Minute {
		t.Fatalf("mutable vnext contents api ttl = %s, want 30m", got)
	}
	if got := ghCommandCacheTTL([]string{"api", "repos/openclaw/openclaw/contents/README.md?ref=refs%2Fheads%2Fv0.2.0"}); got != 30*time.Minute {
		t.Fatalf("v-prefixed branch contents api ttl = %s, want 30m", got)
	}
	if got := normalizeGHAPIRoute([]string{"repos/openclaw/openclaw/actions/runs?per_page=1"}); got != "api repos/:owner/:repo/actions/runs" {
		t.Fatalf("normalized actions route = %q", got)
	}
	if got := normalizeGHAPIRoute([]string{"--paginate", "repos/openclaw/openclaw/issues?state=all&creator=octocat", "--jq", ".[].number"}); got != "api repos/:owner/:repo/issues" {
		t.Fatalf("normalized paginated issues route = %q", got)
	}
	if got := normalizeGHAPIRoute([]string{"repos/openclaw/openclaw/contents/.github/workflows/ci.yml?ref=main"}); got != "api repos/:owner/:repo/contents/:path" {
		t.Fatalf("normalized contents route = %q", got)
	}
	entry := ghCommandCacheEntry{CreatedAt: time.Now().Add(-3 * time.Minute), ExitCode: 1, Stderr: "HTTP 403: API rate limit exceeded"}
	if ttl := ghCommandCacheEntryTTL(entry, 12*time.Hour); ttl != 2*time.Minute {
		t.Fatalf("rate-limit error ttl = %s, want 2m", ttl)
	}
	completedRun := ghCommandCacheEntry{
		Args:     []string{"run", "view", "123", "-R", "openclaw/openclaw", "--json", "status,conclusion"},
		ExitCode: 0,
		Stdout:   `{"status":"completed","conclusion":"success"}`,
	}
	if ttl := ghCommandCacheEntryTTL(completedRun, 2*time.Minute); ttl != 12*time.Hour {
		t.Fatalf("completed run ttl = %s, want 12h", ttl)
	}
	completedRuns := ghCommandCacheEntry{
		Args:     []string{"run", "list", "-R", "openclaw/openclaw", "--json", "status,conclusion"},
		ExitCode: 0,
		Stdout:   `[{"status":"completed","conclusion":"success"}]`,
	}
	if ttl := ghCommandCacheEntryTTL(completedRuns, 2*time.Minute); ttl != 30*time.Minute {
		t.Fatalf("completed run list ttl = %s, want 30m", ttl)
	}
	completedJobs := ghCommandCacheEntry{
		Args:     []string{"api", "repos/openclaw/openclaw/actions/runs/123/jobs"},
		ExitCode: 0,
		Stdout:   `{"jobs":[{"status":"completed","conclusion":"success"}]}`,
	}
	if ttl := ghCommandCacheEntryTTL(completedJobs, time.Minute); ttl != 12*time.Hour {
		t.Fatalf("completed jobs ttl = %s, want 12h", ttl)
	}
}

func TestGHShimCanonicalizesEquivalentCacheKeys(t *testing.T) {
	ctx := context.Background()
	configPath := seedGHShimRepo(t, ctx)
	a := New()
	a.configPath = configPath
	t.Setenv("GH_HOST", "")
	t.Setenv("GH_REPO", "")

	first := a.ghCommandCacheKey(ctx, []string{"run", "view", "123", "-R", "openclaw/openclaw", "--json", "status,conclusion"})
	second := a.ghCommandCacheKey(ctx, []string{"run", "view", "123", "--json", "conclusion,status", "--repo", "openclaw/openclaw"})
	if first != second {
		t.Fatalf("equivalent command keys differ: %s != %s", first, second)
	}
}

func TestGHShimGraphQLReadOnlyDetection(t *testing.T) {
	if !cacheableGHRead([]string{"api", "graphql", "-f", "login=octocat", "-f", "query=query { viewer { login } }"}) {
		t.Fatalf("graphql query should be cacheable")
	}
	if !cacheableGHRead([]string{"api", "graphql", "-f", "query={ viewer { login } }"}) {
		t.Fatalf("anonymous graphql query should be cacheable")
	}
	if cacheableGHRead([]string{"api", "graphql", "-f", "query=mutation { addStar(input:{starrableId:\"x\"}) { clientMutationId } }"}) {
		t.Fatalf("graphql mutation should not be cacheable")
	}
	if cacheableGHRead([]string{"api", "graphql", "-X", "PATCH", "-f", "query={ viewer { login } }"}) {
		t.Fatalf("graphql non-read method should not be cacheable")
	}
	if cacheableGHRead([]string{"api", "graphql", "-f", "query=@query.graphql"}) {
		t.Fatalf("graphql file-backed query should not be cacheable")
	}
	if cacheableGHRead([]string{"api", "repos/openclaw/openclaw/issues", "-f", "title=x"}) {
		t.Fatalf("REST API fields should not be cacheable")
	}
}

func TestGHShimExplicitCacheKeysAreCwdIndependent(t *testing.T) {
	ctx := context.Background()
	configPath := seedGHShimRepo(t, ctx)
	a := New()
	a.configPath = configPath
	t.Setenv("GH_REPO", "")
	t.Setenv("GH_HOST", "")

	original, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	defer func() { _ = os.Chdir(original) }()
	firstDir := t.TempDir()
	secondDir := t.TempDir()

	if err := os.Chdir(firstDir); err != nil {
		t.Fatalf("chdir first: %v", err)
	}
	apiFirst := a.ghCommandCacheKey(ctx, []string{"api", "users/octocat"})
	repoFirst := a.ghCommandCacheKey(ctx, []string{"repo", "view", "openclaw/gitcrawl", "--json", "nameWithOwner"})
	runFirst := a.ghCommandCacheKey(ctx, []string{"run", "view", "123", "-R", "openclaw/gitcrawl", "--json", "status"})
	implicitFirst := a.ghCommandCacheKey(ctx, []string{"repo", "view", "--json", "nameWithOwner"})

	if err := os.Chdir(secondDir); err != nil {
		t.Fatalf("chdir second: %v", err)
	}
	if apiSecond := a.ghCommandCacheKey(ctx, []string{"api", "users/octocat"}); apiSecond != apiFirst {
		t.Fatalf("explicit api key changed across cwd: %s != %s", apiSecond, apiFirst)
	}
	if repoSecond := a.ghCommandCacheKey(ctx, []string{"repo", "view", "openclaw/gitcrawl", "--json", "nameWithOwner"}); repoSecond != repoFirst {
		t.Fatalf("explicit repo key changed across cwd: %s != %s", repoSecond, repoFirst)
	}
	if runSecond := a.ghCommandCacheKey(ctx, []string{"run", "view", "123", "-R", "openclaw/gitcrawl", "--json", "status"}); runSecond != runFirst {
		t.Fatalf("explicit -R key changed across cwd: %s != %s", runSecond, runFirst)
	}
	if implicitSecond := a.ghCommandCacheKey(ctx, []string{"repo", "view", "--json", "nameWithOwner"}); implicitSecond == implicitFirst {
		t.Fatalf("implicit repo key did not include cwd")
	}

	if err := os.Setenv("GH_REPO", "openclaw/other"); err != nil {
		t.Fatalf("set GH_REPO: %v", err)
	}
	if apiWithEnv := a.ghCommandCacheKey(ctx, []string{"api", "users/octocat"}); apiWithEnv != apiFirst {
		t.Fatalf("explicit api key changed across GH_REPO: %s != %s", apiWithEnv, apiFirst)
	}
	if repoWithEnv := a.ghCommandCacheKey(ctx, []string{"repo", "view", "openclaw/gitcrawl", "--json", "nameWithOwner"}); repoWithEnv != repoFirst {
		t.Fatalf("explicit repo key changed across GH_REPO: %s != %s", repoWithEnv, repoFirst)
	}
	if runWithEnv := a.ghCommandCacheKey(ctx, []string{"run", "view", "123", "-R", "openclaw/gitcrawl", "--json", "status"}); runWithEnv != runFirst {
		t.Fatalf("explicit -R key changed across GH_REPO: %s != %s", runWithEnv, runFirst)
	}
}

func TestGHShimGHRepoScopedCacheKeysAreCwdIndependent(t *testing.T) {
	ctx := context.Background()
	configPath := seedGHShimRepo(t, ctx)
	a := New()
	a.configPath = configPath
	t.Setenv("GH_HOST", "")
	t.Setenv("GH_REPO", "")

	original, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	defer func() { _ = os.Chdir(original) }()

	if err := os.Chdir(t.TempDir()); err != nil {
		t.Fatalf("chdir first: %v", err)
	}
	if err := os.Setenv("GH_REPO", "openclaw/one"); err != nil {
		t.Fatalf("set GH_REPO one: %v", err)
	}
	first := a.ghCommandCacheKey(ctx, []string{"repo", "view", "--json", "nameWithOwner"})

	if err := os.Chdir(t.TempDir()); err != nil {
		t.Fatalf("chdir second: %v", err)
	}
	second := a.ghCommandCacheKey(ctx, []string{"repo", "view", "--json", "nameWithOwner"})
	if second != first {
		t.Fatalf("GH_REPO-scoped key changed across cwd: %s != %s", second, first)
	}

	if err := os.Setenv("GH_REPO", "openclaw/two"); err != nil {
		t.Fatalf("set GH_REPO two: %v", err)
	}
	if otherRepo := a.ghCommandCacheKey(ctx, []string{"repo", "view", "--json", "nameWithOwner"}); otherRepo == first {
		t.Fatalf("GH_REPO-scoped key ignored GH_REPO change")
	}
}

func TestGHShimTracksBackendMissesByCommandAndRoute(t *testing.T) {
	ctx := context.Background()
	configPath := seedGHShimRepo(t, ctx)
	dir := t.TempDir()
	ghPath := filepath.Join(dir, "gh")
	if err := os.WriteFile(ghPath, []byte("#!/bin/sh\necho api:$*\n"), 0o755); err != nil {
		t.Fatalf("write fake gh: %v", err)
	}
	t.Setenv("GITCRAWL_GH_PATH", ghPath)
	t.Setenv("GH_REPO", "miss-test/"+filepath.Base(dir))
	t.Setenv("GITCRAWL_GH_CACHE_TTL", "1m")

	run := New()
	var stdout bytes.Buffer
	run.Stdout = &stdout
	args := []string{"--config", configPath, "gh", "api", "-i", "repos/openclaw/openclaw/actions/runs/123/logs"}
	if err := run.Run(ctx, args); err != nil {
		t.Fatalf("api read: %v", err)
	}
	stdout.Reset()
	if err := run.Run(ctx, []string{"--config", configPath, "gh", "xcache", "stats", "--json"}); err != nil {
		t.Fatalf("xcache stats: %v", err)
	}
	var stats ghCommandCacheStats
	if err := json.Unmarshal(stdout.Bytes(), &stats); err != nil {
		t.Fatalf("decode stats: %v\n%s", err, stdout.String())
	}
	if stats.Counters.BackendMissesByCommand["api"] != 1 {
		t.Fatalf("backend misses by command = %#v", stats.Counters.BackendMissesByCommand)
	}
	if stats.Counters.BackendMissesByRoute["api repos/:owner/:repo/actions/runs/:id/logs"] != 1 {
		t.Fatalf("backend misses by route = %#v", stats.Counters.BackendMissesByRoute)
	}
	if stats.Counters.BackendMissesByKey["api repos/openclaw/openclaw/actions/runs/123/logs -i"] != 1 {
		t.Fatalf("backend misses by key = %#v", stats.Counters.BackendMissesByKey)
	}
}

func TestGHShimXCacheStatsSinceAndSnapshot(t *testing.T) {
	ctx := context.Background()
	configPath := seedGHShimRepo(t, ctx)
	dir := t.TempDir()
	ghPath := filepath.Join(dir, "gh")
	if err := os.WriteFile(ghPath, []byte("#!/bin/sh\necho repo:$*\n"), 0o755); err != nil {
		t.Fatalf("write fake gh: %v", err)
	}
	t.Setenv("GITCRAWL_GH_PATH", ghPath)
	t.Setenv("GH_REPO", "stats-since/"+filepath.Base(dir))
	t.Setenv("GITCRAWL_GH_CACHE_TTL", "1m")

	run := New()
	var stdout bytes.Buffer
	run.Stdout = &stdout
	args := []string{"--config", configPath, "gh", "repo", "view", "openclaw/gitcrawl", "--json", "nameWithOwner"}
	if err := run.Run(ctx, args); err != nil {
		t.Fatalf("repo view: %v", err)
	}
	stdout.Reset()
	if err := run.Run(ctx, []string{"--config", configPath, "gh", "xcache", "stats", "--since", "1h", "--json"}); err != nil {
		t.Fatalf("xcache stats --since: %v", err)
	}
	var stats ghCommandCacheStats
	if err := json.Unmarshal(stdout.Bytes(), &stats); err != nil {
		t.Fatalf("decode stats: %v\n%s", err, stdout.String())
	}
	if stats.Since != "1h0m0s" || stats.CumulativeCounters == nil || stats.Counters.BackendMisses != 1 {
		t.Fatalf("since stats = %+v", stats)
	}

	stdout.Reset()
	if err := run.Run(ctx, []string{"--config", configPath, "gh", "xcache", "snapshot", "--reset", "--json"}); err != nil {
		t.Fatalf("xcache snapshot: %v", err)
	}
	var snap ghCommandCacheSnapshotResult
	if err := json.Unmarshal(stdout.Bytes(), &snap); err != nil {
		t.Fatalf("decode snapshot: %v\n%s", err, stdout.String())
	}
	if snap.SnapshotPath == "" || !snap.Reset {
		t.Fatalf("snapshot result = %+v", snap)
	}
	if _, err := os.Stat(snap.SnapshotPath); err != nil {
		t.Fatalf("snapshot file: %v", err)
	}
	stdout.Reset()
	if err := run.Run(ctx, []string{"--config", configPath, "gh", "xcache", "stats", "--json"}); err != nil {
		t.Fatalf("xcache stats after snapshot reset: %v", err)
	}
	if err := json.Unmarshal(stdout.Bytes(), &stats); err != nil {
		t.Fatalf("decode reset stats: %v\n%s", err, stdout.String())
	}
	if stats.Counters.BackendMisses != 0 {
		t.Fatalf("snapshot reset counters = %+v", stats.Counters)
	}
}

func TestGHShimCachesReadOnlyFallbackErrors(t *testing.T) {
	ctx := context.Background()
	configPath := seedGHShimRepo(t, ctx)
	dir := t.TempDir()
	countPath := filepath.Join(dir, "count")
	ghPath := filepath.Join(dir, "gh")
	script := `#!/bin/sh
count=0
if [ -f "$GH_SHIM_COUNT" ]; then
  count=$(cat "$GH_SHIM_COUNT")
fi
count=$((count + 1))
printf "%s" "$count" > "$GH_SHIM_COUNT"
echo "missing-$count:$*" >&2
exit 42
`
	if err := os.WriteFile(ghPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake gh: %v", err)
	}
	t.Setenv("GITCRAWL_GH_PATH", ghPath)
	t.Setenv("GH_SHIM_COUNT", countPath)
	t.Setenv("GH_REPO", "error-cache/"+filepath.Base(dir))
	t.Setenv("GITCRAWL_GH_CACHE_TTL", "1m")

	args := []string{"--config", configPath, "gh", "release", "view", "missing", "-R", "openclaw/openclaw"}
	for i := 0; i < 2; i++ {
		run := New()
		var stderr bytes.Buffer
		run.Stderr = &stderr
		err := run.Run(ctx, args)
		if err == nil {
			t.Fatalf("run %d unexpectedly succeeded", i)
		}
		if !strings.Contains(stderr.String(), "missing-1:release view missing") {
			t.Fatalf("stderr %d = %q", i, stderr.String())
		}
	}
	countData, err := os.ReadFile(countPath)
	if err != nil {
		t.Fatalf("read count: %v", err)
	}
	if strings.TrimSpace(string(countData)) != "1" {
		t.Fatalf("fake gh call count = %q, want 1", countData)
	}
}

func TestGHShimServesExpiredSuccessOnRateLimit(t *testing.T) {
	ctx := context.Background()
	configPath := seedGHShimRepo(t, ctx)
	dir := t.TempDir()
	countPath := filepath.Join(dir, "count")
	ghPath := filepath.Join(dir, "gh")
	script := `#!/bin/sh
count=0
if [ -f "$GH_SHIM_COUNT" ]; then
  count=$(cat "$GH_SHIM_COUNT")
fi
count=$((count + 1))
printf "%s" "$count" > "$GH_SHIM_COUNT"
if [ "$count" = "1" ]; then
  echo "release-ok"
  exit 0
fi
echo "HTTP 403: API rate limit exceeded" >&2
exit 1
`
	if err := os.WriteFile(ghPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake gh: %v", err)
	}
	t.Setenv("GITCRAWL_GH_PATH", ghPath)
	t.Setenv("GH_SHIM_COUNT", countPath)
	t.Setenv("GITCRAWL_GH_CACHE_TTL", "1ns")

	args := []string{"--config", configPath, "gh", "release", "view", "v1", "-R", "openclaw/openclaw"}
	run := New()
	var stdout, stderr bytes.Buffer
	run.Stdout = &stdout
	run.Stderr = &stderr
	if err := run.Run(ctx, args); err != nil {
		t.Fatalf("first read: %v", err)
	}
	stdout.Reset()
	stderr.Reset()
	if err := run.Run(ctx, args); err != nil {
		t.Fatalf("stale read should succeed: %v", err)
	}
	if strings.TrimSpace(stdout.String()) != "release-ok" {
		t.Fatalf("stale stdout = %q", stdout.String())
	}
	if !strings.Contains(stderr.String(), "serving stale cached gh response") {
		t.Fatalf("stderr missing stale warning: %q", stderr.String())
	}
	countData, err := os.ReadFile(countPath)
	if err != nil {
		t.Fatalf("read count: %v", err)
	}
	if strings.TrimSpace(string(countData)) != "2" {
		t.Fatalf("fake gh call count = %q, want 2", countData)
	}

	stdout.Reset()
	stderr.Reset()
	if err := run.Run(ctx, []string{"--config", configPath, "gh", "xcache", "stats", "--json"}); err != nil {
		t.Fatalf("xcache stats: %v", err)
	}
	var stats ghCommandCacheStats
	if err := json.Unmarshal(stdout.Bytes(), &stats); err != nil {
		t.Fatalf("decode stats: %v\n%s", err, stdout.String())
	}
	if stats.Counters.StaleHits != 1 || stats.Counters.BackendMisses != 2 || stats.CacheHits != 1 || stats.TotalReads != 3 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestGHShimServesStaleWhileAnotherProcessRefreshes(t *testing.T) {
	ctx := context.Background()
	configPath := seedGHShimRepo(t, ctx)
	dir := t.TempDir()
	countPath := filepath.Join(dir, "count")
	ghPath := filepath.Join(dir, "gh")
	script := `#!/bin/sh
count=0
if [ -f "$GH_SHIM_COUNT" ]; then
  count=$(cat "$GH_SHIM_COUNT")
fi
count=$((count + 1))
printf "%s" "$count" > "$GH_SHIM_COUNT"
if [ "$count" != "1" ]; then
  sleep 1
fi
echo "release-$count"
`
	if err := os.WriteFile(ghPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake gh: %v", err)
	}
	t.Setenv("GITCRAWL_GH_PATH", ghPath)
	t.Setenv("GH_SHIM_COUNT", countPath)
	t.Setenv("GITCRAWL_GH_CACHE_TTL", "1ns")
	t.Setenv("GITCRAWL_GH_STALE_GRACE", "1h")

	args := []string{"--config", configPath, "gh", "release", "view", "v1", "-R", "openclaw/openclaw"}
	run := New()
	var stdout bytes.Buffer
	run.Stdout = &stdout
	if err := run.Run(ctx, args); err != nil {
		t.Fatalf("seed read: %v", err)
	}
	stdout.Reset()

	var wg sync.WaitGroup
	outputs := make(chan string, 2)
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			run := New()
			var out bytes.Buffer
			run.Stdout = &out
			if err := run.Run(ctx, args); err != nil {
				errs <- err
				return
			}
			outputs <- strings.TrimSpace(out.String())
		}()
	}
	wg.Wait()
	close(errs)
	close(outputs)
	for err := range errs {
		t.Fatalf("stale while refresh run: %v", err)
	}
	seen := map[string]int{}
	for out := range outputs {
		seen[out]++
	}
	if seen["release-1"] != 1 || seen["release-2"] != 1 {
		t.Fatalf("outputs = %#v, want one stale and one refresh", seen)
	}
	countData, err := os.ReadFile(countPath)
	if err != nil {
		t.Fatalf("read count: %v", err)
	}
	if strings.TrimSpace(string(countData)) != "2" {
		t.Fatalf("fake gh call count = %q, want 2", countData)
	}
}

func TestGHShimMutatingFallbackClearsMatchingCacheForGHXStyleMutations(t *testing.T) {
	ctx := context.Background()
	configPath := seedGHShimRepo(t, ctx)
	dir := t.TempDir()
	countPath := filepath.Join(dir, "count")
	ghPath := filepath.Join(dir, "gh")
	script := `#!/bin/sh
count=0
if [ -f "$GH_SHIM_COUNT" ]; then
  count=$(cat "$GH_SHIM_COUNT")
fi
count=$((count + 1))
printf "%s" "$count" > "$GH_SHIM_COUNT"
echo "call-$count:$*"
`
	if err := os.WriteFile(ghPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake gh: %v", err)
	}
	t.Setenv("GITCRAWL_GH_PATH", ghPath)
	t.Setenv("GH_SHIM_COUNT", countPath)
	t.Setenv("GH_REPO", "mutation-cache/"+filepath.Base(dir))
	t.Setenv("GITCRAWL_GH_CACHE_TTL", "1m")

	run := New()
	var stdout bytes.Buffer
	run.Stdout = &stdout
	readArgs := []string{"--config", configPath, "gh", "run", "view", "123", "-R", "openclaw/openclaw"}
	if err := run.Run(ctx, readArgs); err != nil {
		t.Fatalf("first read: %v", err)
	}
	stdout.Reset()
	if err := run.Run(ctx, readArgs); err != nil {
		t.Fatalf("second read: %v", err)
	}
	stdout.Reset()
	if err := run.Run(ctx, []string{"--config", configPath, "gh", "run", "rerun", "123", "-R", "openclaw/openclaw"}); err != nil {
		t.Fatalf("mutation: %v", err)
	}
	stdout.Reset()
	if err := run.Run(ctx, readArgs); err != nil {
		t.Fatalf("third read: %v", err)
	}
	countData, err := os.ReadFile(countPath)
	if err != nil {
		t.Fatalf("read count: %v", err)
	}
	if strings.TrimSpace(string(countData)) != "3" {
		t.Fatalf("fake gh call count = %q, want 3", countData)
	}
}

func TestGHShimMutatingFallbackInvalidatesTargetedTags(t *testing.T) {
	ctx := context.Background()
	configPath := seedGHShimRepo(t, ctx)
	dir := t.TempDir()
	countPath := filepath.Join(dir, "count")
	ghPath := filepath.Join(dir, "gh")
	script := `#!/bin/sh
count=0
if [ -f "$GH_SHIM_COUNT" ]; then
  count=$(cat "$GH_SHIM_COUNT")
fi
count=$((count + 1))
printf "%s" "$count" > "$GH_SHIM_COUNT"
echo "call-$count:$*"
`
	if err := os.WriteFile(ghPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake gh: %v", err)
	}
	t.Setenv("GITCRAWL_GH_PATH", ghPath)
	t.Setenv("GH_SHIM_COUNT", countPath)
	t.Setenv("GITCRAWL_GH_CACHE_TTL", "1m")

	run := New()
	var stdout bytes.Buffer
	run.Stdout = &stdout
	releaseArgs := []string{"--config", configPath, "gh", "release", "view", "v1", "-R", "openclaw/openclaw"}
	issueArgs := []string{"--config", configPath, "gh", "api", "repos/openclaw/openclaw/issues/12"}
	for _, args := range [][]string{releaseArgs, issueArgs} {
		stdout.Reset()
		if err := run.Run(ctx, args); err != nil {
			t.Fatalf("seed read %v: %v", args, err)
		}
	}
	stdout.Reset()
	if err := run.Run(ctx, []string{"--config", configPath, "gh", "issue", "comment", "12", "-R", "openclaw/openclaw", "--body", "fixed"}); err != nil {
		t.Fatalf("mutation: %v", err)
	}
	stdout.Reset()
	if err := run.Run(ctx, releaseArgs); err != nil {
		t.Fatalf("release should remain cached: %v", err)
	}
	if !strings.Contains(stdout.String(), "call-1:") {
		t.Fatalf("release cache was invalidated: %q", stdout.String())
	}
	stdout.Reset()
	if err := run.Run(ctx, issueArgs); err != nil {
		t.Fatalf("issue should be refetched: %v", err)
	}
	if !strings.Contains(stdout.String(), "call-4:") {
		t.Fatalf("issue cache was not invalidated: %q", stdout.String())
	}
}

func TestGHShimCachesPRDiffByHeadSHA(t *testing.T) {
	ctx := context.Background()
	configPath := seedGHShimRepo(t, ctx)
	dir := t.TempDir()
	countPath := filepath.Join(dir, "count")
	ghPath := filepath.Join(dir, "gh")
	script := `#!/bin/sh
count=0
if [ -f "$GH_SHIM_COUNT" ]; then
  count=$(cat "$GH_SHIM_COUNT")
fi
count=$((count + 1))
printf "%s" "$count" > "$GH_SHIM_COUNT"
echo "diff-$count:$*"
`
	if err := os.WriteFile(ghPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake gh: %v", err)
	}
	t.Setenv("GITCRAWL_GH_PATH", ghPath)
	t.Setenv("GH_SHIM_COUNT", countPath)

	run := New()
	var stdout bytes.Buffer
	run.Stdout = &stdout
	args := []string{"--config", configPath, "gh", "pr", "diff", "12", "-R", "openclaw/openclaw"}
	if err := run.Run(ctx, args); err != nil {
		t.Fatalf("first pr diff: %v", err)
	}
	stdout.Reset()
	if err := run.Run(ctx, args); err != nil {
		t.Fatalf("second pr diff: %v", err)
	}
	countData, err := os.ReadFile(countPath)
	if err != nil {
		t.Fatalf("read count: %v", err)
	}
	if strings.TrimSpace(string(countData)) != "1" {
		t.Fatalf("fake gh call count = %q, want 1", countData)
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	st, err := store.Open(ctx, cfg.DBPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	repo, err := st.RepositoryByFullName(ctx, "openclaw/openclaw")
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	if _, err := st.UpsertThread(ctx, store.Thread{
		RepoID: repo.ID, GitHubID: "12", Number: 12, Kind: "pull_request", State: "open",
		Title: "Manifest cache update", AuthorLogin: "bob", AuthorType: "User",
		HTMLURL: "https://github.com/openclaw/openclaw/pull/12", LabelsJSON: "[]", AssigneesJSON: "[]",
		RawJSON: `{"head":{"sha":"def456"}}`, ContentHash: "pr-12-new", IsDraft: true,
		UpdatedAtGitHub: "2026-04-27T03:00:00Z", UpdatedAt: "2026-04-27T03:00:00Z",
	}); err != nil {
		t.Fatalf("update pr head: %v", err)
	}
	if err := st.UpsertPullRequestCache(ctx, store.PullRequestDetail{
		ThreadID:  prIDForTest(t, ctx, st, repo.ID, 12),
		RepoID:    repo.ID,
		Number:    12,
		HeadSHA:   "def456",
		RawJSON:   `{"head":{"sha":"def456"}}`,
		FetchedAt: "2026-04-27T03:00:00Z",
		UpdatedAt: "2026-04-27T03:00:00Z",
	}, nil, nil, nil, nil); err != nil {
		t.Fatalf("update pr cache head: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	stdout.Reset()
	if err := run.Run(ctx, args); err != nil {
		t.Fatalf("third pr diff after head change: %v", err)
	}
	countData, err = os.ReadFile(countPath)
	if err != nil {
		t.Fatalf("read count after update: %v", err)
	}
	if strings.TrimSpace(string(countData)) != "2" {
		t.Fatalf("fake gh call count after head update = %q, want 2", countData)
	}
}

func TestGHShimXCacheGCRemovesExpiredEntries(t *testing.T) {
	ctx := context.Background()
	configPath := seedGHShimRepo(t, ctx)
	dir := t.TempDir()
	ghPath := filepath.Join(dir, "gh")
	if err := os.WriteFile(ghPath, []byte("#!/bin/sh\necho cached:$*\n"), 0o755); err != nil {
		t.Fatalf("write fake gh: %v", err)
	}
	t.Setenv("GITCRAWL_GH_PATH", ghPath)
	t.Setenv("GITCRAWL_GH_CACHE_TTL", "1ns")

	run := New()
	var stdout bytes.Buffer
	run.Stdout = &stdout
	if err := run.Run(ctx, []string{"--config", configPath, "gh", "run", "view", "789", "-R", "openclaw/openclaw"}); err != nil {
		t.Fatalf("cached read: %v", err)
	}
	stdout.Reset()
	if err := run.Run(ctx, []string{"--config", configPath, "gh", "xcache", "gc", "--json"}); err != nil {
		t.Fatalf("xcache gc: %v", err)
	}
	var result map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("decode gc: %v\n%s", err, stdout.String())
	}
	if int(result["removed"].(float64)) != 1 {
		t.Fatalf("gc = %#v", result)
	}
}

func TestGHShimXCacheGCKeepsStablePRDiffEntries(t *testing.T) {
	ctx := context.Background()
	configPath := seedGHShimRepo(t, ctx)
	ghPath := filepath.Join(t.TempDir(), "gh")
	if err := os.WriteFile(ghPath, []byte("#!/bin/sh\necho diff:$*\n"), 0o755); err != nil {
		t.Fatalf("write fake gh: %v", err)
	}
	t.Setenv("GITCRAWL_GH_PATH", ghPath)
	t.Setenv("GH_REPO", "openclaw/openclaw")

	run := New()
	var stdout bytes.Buffer
	run.Stdout = &stdout
	args := []string{"--config", configPath, "gh", "pr", "diff", "12"}
	if err := run.Run(ctx, args); err != nil {
		t.Fatalf("cache pr diff: %v", err)
	}
	t.Setenv("GH_REPO", "")
	app := New()
	app.configPath = configPath
	cacheDir, err := app.ghCommandCacheDir()
	if err != nil {
		t.Fatalf("cache dir: %v", err)
	}
	entries, err := os.ReadDir(cacheDir)
	if err != nil {
		t.Fatalf("read cache dir: %v", err)
	}
	var entryPath string
	for _, entry := range entries {
		if entry.Type().IsRegular() && isGHCommandCacheEntryFile(entry.Name()) {
			entryPath = filepath.Join(cacheDir, entry.Name())
			break
		}
	}
	if entryPath == "" {
		t.Fatal("cached pr diff entry not found")
	}
	cached, ok := readGHCommandCacheEntry(entryPath)
	if !ok {
		t.Fatalf("read cached entry %s", entryPath)
	}
	if cached.StableIdentity == "" {
		t.Fatalf("stable identity was not persisted: %+v", cached)
	}
	cached.CreatedAt = time.Now().Add(-10 * time.Minute)
	data, err := json.Marshal(cached)
	if err != nil {
		t.Fatalf("marshal cache entry: %v", err)
	}
	if err := os.WriteFile(entryPath, data, 0o644); err != nil {
		t.Fatalf("age cache entry: %v", err)
	}

	stdout.Reset()
	if err := run.Run(ctx, []string{"--config", configPath, "gh", "xcache", "gc", "--json"}); err != nil {
		t.Fatalf("xcache gc unchanged head: %v", err)
	}
	var result map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("decode gc unchanged: %v\n%s", err, stdout.String())
	}
	if int(result["removed"].(float64)) != 0 {
		t.Fatalf("unchanged head gc = %#v", result)
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	st, err := store.Open(ctx, cfg.DBPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	repo, err := st.RepositoryByFullName(ctx, "openclaw/openclaw")
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	if err := st.UpsertPullRequestCache(ctx, store.PullRequestDetail{
		ThreadID:  prIDForTest(t, ctx, st, repo.ID, 12),
		RepoID:    repo.ID,
		Number:    12,
		HeadSHA:   "def456",
		RawJSON:   `{"head":{"sha":"def456"}}`,
		FetchedAt: "2026-04-27T03:00:00Z",
		UpdatedAt: "2026-04-27T03:00:00Z",
	}, nil, nil, nil, nil); err != nil {
		t.Fatalf("update pr cache head: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	stdout.Reset()
	if err := run.Run(ctx, []string{"--config", configPath, "gh", "xcache", "gc", "--json"}); err != nil {
		t.Fatalf("xcache gc changed head: %v", err)
	}
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("decode gc changed: %v\n%s", err, stdout.String())
	}
	if int(result["removed"].(float64)) != 1 {
		t.Fatalf("changed head gc = %#v", result)
	}
}

func TestGHShimCoalescesConcurrentReadOnlyFallbacks(t *testing.T) {
	ctx := context.Background()
	configPath := seedGHShimRepo(t, ctx)
	dir := t.TempDir()
	countPath := filepath.Join(dir, "count")
	ghPath := filepath.Join(dir, "gh")
	script := `#!/bin/sh
count=0
if [ -f "$GH_SHIM_COUNT" ]; then
  count=$(cat "$GH_SHIM_COUNT")
fi
count=$((count + 1))
printf "%s" "$count" > "$GH_SHIM_COUNT"
sleep 0.2
echo "call-$count:$*"
`
	if err := os.WriteFile(ghPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake gh: %v", err)
	}
	t.Setenv("GITCRAWL_GH_PATH", ghPath)
	t.Setenv("GH_SHIM_COUNT", countPath)
	t.Setenv("GH_REPO", "coalesce-test/"+filepath.Base(dir))
	t.Setenv("GITCRAWL_GH_CACHE_TTL", "1m")

	args := []string{"--config", configPath, "gh", "run", "view", "456", "-R", "openclaw/openclaw", "--json", "status"}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	outputs := make(chan string, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			run := New()
			var stdout bytes.Buffer
			run.Stdout = &stdout
			if err := run.Run(ctx, args); err != nil {
				errs <- err
				return
			}
			outputs <- stdout.String()
		}()
	}
	wg.Wait()
	close(errs)
	close(outputs)
	for err := range errs {
		t.Fatalf("coalesced run: %v", err)
	}
	if len(outputs) != 2 {
		t.Fatalf("outputs = %d, want 2", len(outputs))
	}
	var first string
	for out := range outputs {
		if first == "" {
			first = out
		} else if out != first {
			t.Fatalf("coalesced outputs differ: %q vs %q", first, out)
		}
	}
	countData, err := os.ReadFile(countPath)
	if err != nil {
		t.Fatalf("read count: %v", err)
	}
	if strings.TrimSpace(string(countData)) != "1" {
		t.Fatalf("fake gh call count = %q, want 1", countData)
	}
}
