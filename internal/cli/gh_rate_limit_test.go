package cli

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/openclaw/gitcrawl/internal/config"
	gh "github.com/openclaw/gitcrawl/internal/github"
)

func TestSharedRateLimitStateRoundTrip(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	cfg := config.Default()
	cfg.CacheDir = filepath.Join(dir, "cache")
	cfg.Env = map[string]string{"GITHUB_TOKEN": "gh-token"}
	if err := config.Save(cfgPath, cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}
	t.Setenv("GITCRAWL_GH_RATE_LIMIT_LOW_REMAINING", "10")
	t.Setenv("GITCRAWL_GH_RATE_LIMIT_MAX_AGE", "1h")

	app := New()
	app.configPath = cfgPath
	resetAt := time.Now().Add(time.Hour).UTC()
	if err := app.writeSharedRateLimit(ctx, "gh-token", gh.RateLimitSnapshot{
		Host:      "github.com",
		Limit:     5000,
		Remaining: 5,
		ResetAt:   resetAt,
		Resource:  "core",
	}, "test"); err != nil {
		t.Fatalf("write rate limit: %v", err)
	}

	state, ok := app.sharedRateLimitStateForTokenHost("gh-token", "github.com")
	if !ok {
		t.Fatalf("missing shared rate limit state")
	}
	if !state.Low || state.Remaining != 5 || state.Threshold != 10 || state.Resource != "core" {
		t.Fatalf("state = %#v", state)
	}
	if !app.hasSharedRateLimitStateForHost("github.com") {
		t.Fatalf("expected shared state file")
	}
	low, ok := app.sharedRateLimitLowForHost(ctx, "github.com")
	if !ok || low.Remaining != 5 {
		t.Fatalf("low state = %#v ok=%v", low, ok)
	}
	if state, ok := app.sharedRateLimitState(ctx); !ok || state.Remaining != 5 {
		t.Fatalf("shared state = %#v ok=%v", state, ok)
	}
	if state, ok := app.sharedRateLimitStateForToken("gh-token"); !ok || state.Remaining != 5 {
		t.Fatalf("shared state for token = %#v ok=%v", state, ok)
	}
	if state, ok := app.sharedRateLimitLow(ctx); !ok || state.Remaining != 5 {
		t.Fatalf("shared low = %#v ok=%v", state, ok)
	}
	observer := app.observeGitHubRateLimit(ctx, "gh-token")
	observer(gh.RateLimitSnapshot{Host: "github.com", Limit: 5000, Remaining: 6, ResetAt: resetAt, Resource: "core"})
	if state, ok := app.sharedRateLimitStateForTokenHost("gh-token", "github.com"); !ok || state.Remaining != 6 {
		t.Fatalf("observed state = %#v ok=%v", state, ok)
	}
	if notice := low.staleNotice(2 * time.Minute); !strings.Contains(notice, "5 remaining") || !strings.Contains(notice, "2m0s ago") {
		t.Fatalf("notice = %q", notice)
	}
}

func TestRecordGHRateLimitFromOutputAndHostHelpers(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	cfg := config.Default()
	cfg.CacheDir = filepath.Join(dir, "cache")
	cfg.Env = map[string]string{"GITHUB_TOKEN": "gh-token"}
	if err := config.Save(cfgPath, cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}
	t.Setenv("GH_HOST", "")
	t.Setenv("GITCRAWL_GH_RATE_LIMIT_LOW_REMAINING", "50")

	app := New()
	app.configPath = cfgPath
	raw := `{"resources":{"core":{"limit":5000,"remaining":40,"reset":1893456000}}}`
	if err := app.recordGHRateLimitFromOutput(ctx, []string{"api", "rate_limit", "--hostname", "enterprise.example"}, raw); err != nil {
		t.Fatalf("record rate limit: %v", err)
	}
	state, ok := app.sharedRateLimitLowForArgs(ctx, []string{"api", "rate_limit", "--hostname=enterprise.example"})
	if !ok {
		t.Fatalf("missing low state")
	}
	if state.Host != "enterprise.example" || state.Remaining != 40 || !state.Low {
		t.Fatalf("recorded state = %#v", state)
	}

	if got := ghRateLimitHostForAPIBaseURL("https://api.github.com"); got != "github.com" {
		t.Fatalf("api host = %q", got)
	}
	if got := ghRateLimitHostForAPIBaseURL("https://ghe.example/api/v3"); got != "ghe.example" {
		t.Fatalf("ghe host = %q", got)
	}
	if got := ghRateLimitHostForArgs([]string{"api", "rate_limit", "--hostname", " ghe.example "}); got != "ghe.example" {
		t.Fatalf("arg host = %q", got)
	}
	if got := ghRateLimitSnapshotHost(gh.RateLimitSnapshot{Host: "api.github.com"}); got != "api.github.com" {
		t.Fatalf("snapshot host = %q", got)
	}
	t.Setenv("GH_HOST", "enterprise.example")
	if got := ghRateLimitSnapshotHost(gh.RateLimitSnapshot{}); got != "enterprise.example" {
		t.Fatalf("env snapshot host = %q", got)
	}
	t.Setenv("GH_HOST", "")
	t.Setenv("GITCRAWL_GITHUB_BASE_URL", "https://api.github.com")
	if got := ghRateLimitSnapshotHost(gh.RateLimitSnapshot{}); got != "github.com" {
		t.Fatalf("base url snapshot host = %q", got)
	}
	if got := safeFileToken(" host/name:value "); got != "host_name_value" {
		t.Fatalf("safe token = %q", got)
	}
	if got := ghRateLimitLowRemaining(); got != 50 {
		t.Fatalf("low remaining = %d", got)
	}
	if hash := ghRateLimitTokenHash("gh-token"); len(hash) != 16 {
		t.Fatalf("hash = %q", hash)
	}
}
