package cli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/openclaw/gitcrawl/internal/config"
	gh "github.com/openclaw/gitcrawl/internal/github"
)

const ghSharedRateLimitFilePrefix = "_rate_limit_"

type ghSharedRateLimitState struct {
	Host      string    `json:"host"`
	TokenHash string    `json:"token_hash"`
	Limit     int       `json:"limit,omitempty"`
	Remaining int       `json:"remaining"`
	ResetAt   time.Time `json:"reset_at,omitempty"`
	Resource  string    `json:"resource,omitempty"`
	UpdatedAt time.Time `json:"updated_at"`
	Source    string    `json:"source,omitempty"`
	Low       bool      `json:"low"`
	Threshold int       `json:"threshold"`
}

func (a *App) observeGitHubRateLimit(ctx context.Context, token string) gh.RateLimitObserver {
	return func(snapshot gh.RateLimitSnapshot) {
		_ = a.writeSharedRateLimit(ctx, token, snapshot, "syncer")
	}
}

func (a *App) writeSharedRateLimit(ctx context.Context, token string, snapshot gh.RateLimitSnapshot, source string) error {
	if strings.TrimSpace(token) == "" || snapshot.Remaining < 0 {
		return nil
	}
	dir, err := a.ghCommandCacheDir()
	if err != nil {
		return err
	}
	state := ghSharedRateLimitState{
		Host:      ghRateLimitSnapshotHost(snapshot),
		TokenHash: ghRateLimitTokenHash(token),
		Limit:     snapshot.Limit,
		Remaining: snapshot.Remaining,
		ResetAt:   snapshot.ResetAt,
		Resource:  snapshot.Resource,
		UpdatedAt: time.Now().UTC(),
		Source:    source,
		Threshold: ghRateLimitLowRemaining(),
	}
	state.Low = state.Remaining <= state.Threshold && (state.ResetAt.IsZero() || time.Now().Before(state.ResetAt))
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return writeAtomicFile(filepath.Join(dir, ghSharedRateLimitFilePrefix+state.Host+"_"+state.TokenHash+".json"), data, 0o600)
}

func (a *App) sharedRateLimitState(ctx context.Context) (ghSharedRateLimitState, bool) {
	cfg, err := config.LoadRuntime(a.configPath)
	if err != nil {
		return ghSharedRateLimitState{}, false
	}
	token := a.resolveGitHubToken(ctx, cfg)
	if token.Value == "" {
		return ghSharedRateLimitState{}, false
	}
	return a.sharedRateLimitStateForToken(token.Value)
}

func (a *App) sharedRateLimitStateForToken(token string) (ghSharedRateLimitState, bool) {
	return a.sharedRateLimitStateForTokenHost(token, ghRateLimitHost())
}

func (a *App) sharedRateLimitStateForTokenHost(token, host string) (ghSharedRateLimitState, bool) {
	dir, err := a.ghCommandCacheDir()
	if err != nil {
		return ghSharedRateLimitState{}, false
	}
	path := filepath.Join(dir, ghSharedRateLimitFilePrefix+safeFileToken(host)+"_"+ghRateLimitTokenHash(token)+".json")
	data, err := os.ReadFile(path)
	if err != nil {
		return ghSharedRateLimitState{}, false
	}
	var state ghSharedRateLimitState
	if err := json.Unmarshal(data, &state); err != nil {
		return ghSharedRateLimitState{}, false
	}
	state.Threshold = ghRateLimitLowRemaining()
	state.Low = state.Remaining <= state.Threshold && (state.ResetAt.IsZero() || time.Now().Before(state.ResetAt))
	return state, true
}

func (a *App) hasSharedRateLimitStateForHost(host string) bool {
	dir, err := a.ghCommandCacheDir()
	if err != nil {
		return false
	}
	prefix := ghSharedRateLimitFilePrefix + safeFileToken(host) + "_"
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), prefix) && strings.HasSuffix(entry.Name(), ".json") {
			return true
		}
	}
	return false
}

func (a *App) sharedRateLimitLow(ctx context.Context) (ghSharedRateLimitState, bool) {
	return a.sharedRateLimitLowForHost(ctx, ghRateLimitHost())
}

func (a *App) sharedRateLimitLowForArgs(ctx context.Context, args []string) (ghSharedRateLimitState, bool) {
	return a.sharedRateLimitLowForHost(ctx, ghRateLimitHostForArgs(args))
}

func (a *App) sharedRateLimitLowForHost(ctx context.Context, host string) (ghSharedRateLimitState, bool) {
	cfg, err := config.LoadRuntime(a.configPath)
	if err != nil {
		return ghSharedRateLimitState{}, false
	}
	token := config.ResolveGitHubToken(cfg)
	if token.Value == "" {
		return ghSharedRateLimitState{}, false
	}
	state, ok := a.sharedRateLimitStateForTokenHost(token.Value, host)
	if !ok || !state.Low {
		return ghSharedRateLimitState{}, false
	}
	if !state.UpdatedAt.IsZero() && time.Since(state.UpdatedAt) > ghRateLimitStateMaxAge() {
		return ghSharedRateLimitState{}, false
	}
	return state, true
}

func (a *App) recordGHRateLimitFromOutput(ctx context.Context, args []string, raw string) error {
	if len(args) < 2 || args[0] != "api" || normalizeGHAPIRoute(args[1:]) != "api rate_limit" {
		return nil
	}
	var payload struct {
		Resources map[string]struct {
			Limit     int   `json:"limit"`
			Remaining int   `json:"remaining"`
			Reset     int64 `json:"reset"`
			Used      int   `json:"used"`
		} `json:"resources"`
		Limit     int   `json:"limit"`
		Remaining int   `json:"remaining"`
		Reset     int64 `json:"reset"`
	}
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return nil
	}
	core, ok := payload.Resources["core"]
	if !ok {
		core.Limit = payload.Limit
		core.Remaining = payload.Remaining
		core.Reset = payload.Reset
	}
	if core.Limit == 0 && core.Remaining == 0 && core.Reset == 0 {
		return nil
	}
	cfg, err := config.LoadRuntime(a.configPath)
	if err != nil {
		return nil
	}
	token := a.resolveGitHubToken(ctx, cfg)
	if token.Value == "" {
		return nil
	}
	var resetAt time.Time
	if core.Reset > 0 {
		resetAt = time.Unix(core.Reset, 0).UTC()
	}
	return a.writeSharedRateLimit(ctx, token.Value, gh.RateLimitSnapshot{
		Host:      ghRateLimitHostForArgs(args),
		Limit:     core.Limit,
		Remaining: core.Remaining,
		ResetAt:   resetAt,
		Resource:  "core",
	}, "gh api rate_limit")
}

func ghRateLimitHost() string {
	if host := strings.TrimSpace(os.Getenv("GH_HOST")); host != "" {
		return safeFileToken(host)
	}
	return "github.com"
}

func ghRateLimitHostForArgs(args []string) string {
	for index := 0; index < len(args); index++ {
		arg := args[index]
		switch arg {
		case "--hostname":
			if index+1 < len(args) {
				if host := strings.TrimSpace(args[index+1]); host != "" {
					return safeFileToken(host)
				}
				index++
			}
		default:
			if strings.HasPrefix(arg, "--hostname=") {
				if host := strings.TrimSpace(strings.TrimPrefix(arg, "--hostname=")); host != "" {
					return safeFileToken(host)
				}
			}
		}
	}
	return ghRateLimitHost()
}

func ghRateLimitSnapshotHost(snapshot gh.RateLimitSnapshot) string {
	if host := strings.TrimSpace(snapshot.Host); host != "" {
		return safeFileToken(host)
	}
	if strings.TrimSpace(os.Getenv("GH_HOST")) != "" {
		return ghRateLimitHost()
	}
	if host := ghRateLimitHostForAPIBaseURL(githubBaseURL()); host != "" {
		return host
	}
	return ghRateLimitHost()
}

func ghRateLimitHostForAPIBaseURL(baseURL string) string {
	parsed, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil || parsed.Host == "" {
		return ""
	}
	if strings.EqualFold(parsed.Hostname(), "api.github.com") {
		return "github.com"
	}
	return safeFileToken(parsed.Host)
}

func ghRateLimitTokenHash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])[:16]
}

func ghRateLimitLowRemaining() int {
	if raw := strings.TrimSpace(os.Getenv("GITCRAWL_GH_RATE_LIMIT_LOW_REMAINING")); raw != "" {
		if value, err := strconv.Atoi(raw); err == nil && value >= 0 {
			return value
		}
	}
	return 250
}

func ghRateLimitStateMaxAge() time.Duration {
	if raw := strings.TrimSpace(os.Getenv("GITCRAWL_GH_RATE_LIMIT_MAX_AGE")); raw != "" {
		if duration, err := time.ParseDuration(raw); err == nil && duration > 0 {
			return duration
		}
	}
	return 30 * time.Minute
}

func safeFileToken(value string) string {
	replacer := strings.NewReplacer("/", "_", "\\", "_", ":", "_", " ", "_")
	return strings.Trim(replacer.Replace(value), "._")
}

func (state ghSharedRateLimitState) staleNotice(age time.Duration) string {
	reset := ""
	if !state.ResetAt.IsZero() {
		reset = " reset=" + state.ResetAt.Format(time.RFC3339)
	}
	return fmt.Sprintf("gitcrawl: shared GitHub rate limit low (%d remaining, threshold %d%s); serving stale cached gh response from %s ago\n",
		state.Remaining, state.Threshold, reset, age.Round(time.Second))
}
