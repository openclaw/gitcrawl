package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/openclaw/gitcrawl/internal/config"
)

func (a *App) execRealGHWithMutationTracking(ctx context.Context, args []string) error {
	err := a.execRealGH(ctx, args)
	if err == nil && mutatingGHCommand(args) {
		_ = a.incrementGHXCacheCounter("pass_through_writes")
		_ = a.clearGHCommandCacheForMutation(ctx, args)
		_ = a.recordGHLivenessTombstone(ctx, args)
	}
	return err
}

func (a *App) execRealGHMaybeCached(ctx context.Context, args []string, controls ghShimControls) error {
	if !cacheableGHRead(args) {
		return a.execRealGHWithMutationTracking(ctx, args)
	}
	cacheDir, err := a.ghCommandCacheDir()
	if err != nil {
		return a.execRealGH(ctx, args)
	}
	if rawArgs, jqExpr, ok := ghAPIProjectionCacheArgs(args); ok {
		return a.execRealGHAPIProjectionMaybeCached(ctx, args, rawArgs, jqExpr, cacheDir, controls)
	}
	stableIdentity := a.ghCommandStableIdentity(ctx, args)
	ttl := ghCommandCacheTTLBase(args, stableIdentity != "")
	entryPath := filepath.Join(cacheDir, a.ghCommandCacheKeyWithStableIdentity(ctx, args, stableIdentity)+".json")
	staleEntry, hasStaleEntry := readGHCommandCacheEntry(entryPath)
	bypassCache := false
	if a.shouldBypassGHCacheForLiveness(ctx, args, controls) {
		bypassCache = true
	}
	if !bypassCache {
		if entry, ok := readGHCommandCache(entryPath, ttl); ok {
			_ = a.incrementGHXCacheCounter("fallback_hits")
			a.writeGHCommandCacheLivenessNotice(entry)
			return a.writeGHCommandCacheEntry(entry)
		}
		if hasStaleEntry {
			if state, low := a.sharedRateLimitLowForArgs(ctx, args); low && ghCommandCacheEntryCanServeLowBudgetStale(staleEntry, ttl) {
				_ = a.incrementGHXCacheCounter("stale_hits")
				_ = a.incrementGHXCacheCounter("low_budget_stale_hits")
				_, _ = io.WriteString(a.Stderr, state.staleNotice(time.Since(staleEntry.CreatedAt)))
				return a.writeGHCommandCacheEntry(staleEntry)
			}
		}
		lockPath := entryPath + ".lock"
		lock, locked := tryGHCommandCacheLock(lockPath)
		if !locked {
			if entry, hit, ok := waitGHCommandCache(entryPath, lockPath, ttl, staleEntry, hasStaleEntry); ok {
				_ = a.incrementGHXCacheCounter(hit)
				a.writeGHCommandCacheLivenessNotice(entry)
				return a.writeGHCommandCacheEntry(entry)
			}
			lock, locked = tryGHCommandCacheLock(lockPath)
		}
		if locked {
			defer func() {
				_ = lock.Close()
				_ = os.Remove(lockPath)
			}()
			if entry, ok := readGHCommandCache(entryPath, ttl); ok {
				_ = a.incrementGHXCacheCounter("fallback_hits")
				a.writeGHCommandCacheLivenessNotice(entry)
				return a.writeGHCommandCacheEntry(entry)
			}
		}
	}

	stdout, stderr, exitCode, err := a.captureRealGH(ctx, args)
	_ = a.incrementGHXCacheBackendMiss(args)
	if err != nil && hasStaleEntry && ghCommandCacheEntryCanServeStale(staleEntry, ttl) && ghCommandOutputLooksRateLimited(stdout, stderr) {
		_ = a.incrementGHXCacheCounter("stale_hits")
		_, _ = fmt.Fprintf(a.Stderr, "gitcrawl: GitHub rate limited; serving stale cached gh response from %s ago\n", time.Since(staleEntry.CreatedAt).Round(time.Second))
		return a.writeGHCommandCacheEntry(staleEntry)
	}
	if err == nil || cacheGHReadErrors() {
		if err == nil {
			_ = a.recordGHRateLimitFromOutput(ctx, args, stdout)
		}
		_ = writeGHCommandCache(entryPath, ghCommandCacheEntry{
			CreatedAt:      time.Now().UTC(),
			Args:           append([]string(nil), args...),
			Tags:           a.ghCommandCacheTags(ctx, args),
			StableIdentity: stableIdentity,
			ExitCode:       exitCode,
			Stdout:         stdout,
			Stderr:         stderr,
		})
	}
	_, _ = io.WriteString(a.Stdout, stdout)
	_, _ = io.WriteString(a.Stderr, stderr)
	return err
}

func (a *App) execRealGHAPIProjectionMaybeCached(ctx context.Context, originalArgs, rawArgs []string, jqExpr, cacheDir string, controls ghShimControls) error {
	if _, err := exec.LookPath("jq"); err != nil {
		return a.execRealGHMaybeCachedWithoutProjection(ctx, originalArgs, cacheDir, controls)
	}
	ttl := ghCommandCacheTTLBase(rawArgs, false)
	entryPath := filepath.Join(cacheDir, a.ghCommandCacheKeyWithStableIdentity(ctx, rawArgs, "")+".json")
	staleEntry, hasStaleEntry := readGHCommandCacheEntry(entryPath)
	if !a.shouldBypassGHCacheForLiveness(ctx, originalArgs, controls) {
		if entry, ok := readGHCommandCache(entryPath, ttl); ok {
			_ = a.incrementGHXCacheCounter("fallback_hits")
			a.writeGHCommandCacheLivenessNotice(entry)
			return a.writeOrFallbackGHAPIProjection(ctx, originalArgs, cacheDir, controls, entry.Stdout, jqExpr)
		}
		if hasStaleEntry {
			if state, low := a.sharedRateLimitLowForArgs(ctx, rawArgs); low && ghCommandCacheEntryCanServeLowBudgetStale(staleEntry, ttl) {
				_ = a.incrementGHXCacheCounter("stale_hits")
				_ = a.incrementGHXCacheCounter("low_budget_stale_hits")
				_, _ = io.WriteString(a.Stderr, state.staleNotice(time.Since(staleEntry.CreatedAt)))
				return a.writeOrFallbackGHAPIProjection(ctx, originalArgs, cacheDir, controls, staleEntry.Stdout, jqExpr)
			}
		}
		lockPath := entryPath + ".lock"
		lock, locked := tryGHCommandCacheLock(lockPath)
		if !locked {
			if entry, hit, ok := waitGHCommandCache(entryPath, lockPath, ttl, staleEntry, hasStaleEntry); ok {
				_ = a.incrementGHXCacheCounter(hit)
				a.writeGHCommandCacheLivenessNotice(entry)
				return a.writeOrFallbackGHAPIProjection(ctx, originalArgs, cacheDir, controls, entry.Stdout, jqExpr)
			}
			lock, locked = tryGHCommandCacheLock(lockPath)
		}
		if locked {
			defer func() {
				_ = lock.Close()
				_ = os.Remove(lockPath)
			}()
			if entry, ok := readGHCommandCache(entryPath, ttl); ok {
				_ = a.incrementGHXCacheCounter("fallback_hits")
				a.writeGHCommandCacheLivenessNotice(entry)
				return a.writeOrFallbackGHAPIProjection(ctx, originalArgs, cacheDir, controls, entry.Stdout, jqExpr)
			}
		}
	}

	stdout, stderr, exitCode, err := a.captureRealGH(ctx, rawArgs)
	_ = a.incrementGHXCacheBackendMiss(rawArgs)
	if err != nil && hasStaleEntry && ghCommandCacheEntryCanServeStale(staleEntry, ttl) && ghCommandOutputLooksRateLimited(stdout, stderr) {
		_ = a.incrementGHXCacheCounter("stale_hits")
		_, _ = fmt.Fprintf(a.Stderr, "gitcrawl: GitHub rate limited; serving stale cached gh response from %s ago\n", time.Since(staleEntry.CreatedAt).Round(time.Second))
		return a.writeOrFallbackGHAPIProjection(ctx, originalArgs, cacheDir, controls, staleEntry.Stdout, jqExpr)
	}
	var projectedOut, projectedErr string
	if err == nil {
		var projectErr error
		projectedOut, projectedErr, projectErr = runGHAPIProjection(stdout, jqExpr)
		if projectErr != nil {
			return a.execRealGHMaybeCachedWithoutProjection(ctx, originalArgs, cacheDir, controls)
		}
	}
	if err == nil || cacheGHReadErrors() {
		if err == nil {
			_ = a.recordGHRateLimitFromOutput(ctx, rawArgs, stdout)
		}
		_ = writeGHCommandCache(entryPath, ghCommandCacheEntry{
			CreatedAt: time.Now().UTC(),
			Args:      append([]string(nil), rawArgs...),
			Tags:      a.ghCommandCacheTags(ctx, rawArgs),
			ExitCode:  exitCode,
			Stdout:    stdout,
			Stderr:    stderr,
		})
	}
	if err != nil {
		_, _ = io.WriteString(a.Stdout, stdout)
		_, _ = io.WriteString(a.Stderr, stderr)
		return err
	}
	if stderr != "" {
		_, _ = io.WriteString(a.Stderr, stderr)
	}
	_, _ = io.WriteString(a.Stdout, projectedOut)
	_, _ = io.WriteString(a.Stderr, projectedErr)
	return nil
}

func (a *App) execRealGHMaybeCachedWithoutProjection(ctx context.Context, args []string, cacheDir string, controls ghShimControls) error {
	stableIdentity := a.ghCommandStableIdentity(ctx, args)
	ttl := ghCommandCacheTTLBase(args, stableIdentity != "")
	entryPath := filepath.Join(cacheDir, a.ghCommandCacheKeyWithStableIdentity(ctx, args, stableIdentity)+".json")
	staleEntry, hasStaleEntry := readGHCommandCacheEntry(entryPath)
	if !a.shouldBypassGHCacheForLiveness(ctx, args, controls) {
		if entry, ok := readGHCommandCache(entryPath, ttl); ok {
			_ = a.incrementGHXCacheCounter("fallback_hits")
			a.writeGHCommandCacheLivenessNotice(entry)
			return a.writeGHCommandCacheEntry(entry)
		}
		if hasStaleEntry {
			if state, low := a.sharedRateLimitLowForArgs(ctx, args); low && ghCommandCacheEntryCanServeLowBudgetStale(staleEntry, ttl) {
				_ = a.incrementGHXCacheCounter("stale_hits")
				_ = a.incrementGHXCacheCounter("low_budget_stale_hits")
				_, _ = io.WriteString(a.Stderr, state.staleNotice(time.Since(staleEntry.CreatedAt)))
				return a.writeGHCommandCacheEntry(staleEntry)
			}
		}
	}
	stdout, stderr, exitCode, err := a.captureRealGH(ctx, args)
	_ = a.incrementGHXCacheBackendMiss(args)
	if err != nil && hasStaleEntry && ghCommandCacheEntryCanServeStale(staleEntry, ttl) && ghCommandOutputLooksRateLimited(stdout, stderr) {
		_ = a.incrementGHXCacheCounter("stale_hits")
		_, _ = fmt.Fprintf(a.Stderr, "gitcrawl: GitHub rate limited; serving stale cached gh response from %s ago\n", time.Since(staleEntry.CreatedAt).Round(time.Second))
		return a.writeGHCommandCacheEntry(staleEntry)
	}
	if err == nil || cacheGHReadErrors() {
		if err == nil {
			_ = a.recordGHRateLimitFromOutput(ctx, args, stdout)
		}
		_ = writeGHCommandCache(entryPath, ghCommandCacheEntry{
			CreatedAt:      time.Now().UTC(),
			Args:           append([]string(nil), args...),
			Tags:           a.ghCommandCacheTags(ctx, args),
			StableIdentity: stableIdentity,
			ExitCode:       exitCode,
			Stdout:         stdout,
			Stderr:         stderr,
		})
	}
	_, _ = io.WriteString(a.Stdout, stdout)
	_, _ = io.WriteString(a.Stderr, stderr)
	return err
}

func ghAPIProjectionCacheArgs(args []string) ([]string, string, bool) {
	if len(args) == 0 || args[0] != "api" {
		return nil, "", false
	}
	raw := make([]string, 0, len(args))
	raw = append(raw, "api")
	jqExpr := ""
	for index := 1; index < len(args); index++ {
		arg := args[index]
		switch arg {
		case "--template", "-t", "--input":
			return nil, "", false
		case "--jq", "-q":
			if index+1 >= len(args) {
				return nil, "", false
			}
			jqExpr = args[index+1]
			index++
			continue
		default:
			if strings.HasPrefix(arg, "--jq=") {
				jqExpr = strings.TrimPrefix(arg, "--jq=")
				continue
			}
			if strings.HasPrefix(arg, "--template=") || strings.HasPrefix(arg, "--input=") {
				return nil, "", false
			}
			raw = append(raw, arg)
		}
	}
	if strings.TrimSpace(jqExpr) == "" || !cacheableGHRead(raw) {
		return nil, "", false
	}
	return raw, jqExpr, true
}

func (a *App) writeOrFallbackGHAPIProjection(ctx context.Context, args []string, cacheDir string, controls ghShimControls, stdout string, jqExpr string) error {
	projectedOut, projectedErr, err := runGHAPIProjection(stdout, jqExpr)
	if err != nil {
		return a.execRealGHMaybeCachedWithoutProjection(ctx, args, cacheDir, controls)
	}
	_, _ = io.WriteString(a.Stdout, projectedOut)
	_, _ = io.WriteString(a.Stderr, projectedErr)
	return nil
}

func runGHAPIProjection(stdout string, jqExpr string) (string, string, error) {
	cmd := exec.Command("jq", "-r", jqExpr)
	cmd.Stdin = strings.NewReader(stdout)
	var projectedOut, projectedErr bytes.Buffer
	cmd.Stdout = &projectedOut
	cmd.Stderr = &projectedErr
	err := cmd.Run()
	return projectedOut.String(), projectedErr.String(), err
}

func cacheGHReadErrors() bool {
	return !strings.EqualFold(strings.TrimSpace(os.Getenv("GITCRAWL_GH_CACHE_ERRORS")), "0")
}

func (a *App) realGHEnv() []string {
	env := os.Environ()
	cfg, err := config.LoadRuntime(a.configPath)
	if err != nil {
		return env
	}
	token := config.ResolveGitHubToken(cfg)
	if token.Value == "" {
		return env
	}
	tokenEnv := strings.TrimSpace(cfg.GitHub.TokenEnv)
	if tokenEnv == "" {
		tokenEnv = config.DefaultTokenEnv
	}
	env = setEnvValue(env, tokenEnv, token.Value)
	if tokenEnv != config.DefaultTokenEnv && !envValueNonEmpty(env, config.DefaultTokenEnv) {
		env = setEnvValue(env, config.DefaultTokenEnv, token.Value)
	}
	return env
}

func setEnvValue(env []string, key, value string) []string {
	key = strings.TrimSpace(key)
	if key == "" {
		return env
	}
	entry := key + "=" + value
	prefix := key + "="
	out := append([]string(nil), env...)
	for index, existing := range out {
		if strings.HasPrefix(existing, prefix) {
			out[index] = entry
			return out
		}
	}
	return append(out, entry)
}

func envValueNonEmpty(env []string, key string) bool {
	key = strings.TrimSpace(key)
	if key == "" {
		return false
	}
	prefix := key + "="
	for _, existing := range env {
		if strings.HasPrefix(existing, prefix) {
			return strings.TrimSpace(strings.TrimPrefix(existing, prefix)) != ""
		}
	}
	return false
}

func (a *App) captureRealGH(ctx context.Context, args []string) (string, string, int, error) {
	ghPath, err := resolveRealGHPath()
	if err != nil {
		return "", "", 127, err
	}
	var stdout, stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, ghPath, args...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	cmd.Env = a.realGHEnv()
	err = cmd.Run()
	exitCode := 0
	if err != nil {
		exitCode = 1
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			exitCode = exitErr.ExitCode()
		}
	}
	return stdout.String(), stderr.String(), exitCode, err
}

func (a *App) ghCommandCacheDir() (string, error) {
	cfg, err := config.Load(a.configPath)
	if err != nil {
		cfg = config.Default()
	}
	dir := filepath.Join(cfg.CacheDir, "gh-shim")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	return dir, nil
}

func (a *App) clearGHCommandCache() error {
	_, err := a.clearGHCommandCacheCount()
	return err
}

func (a *App) clearGHCommandCacheCount() (int, error) {
	dir, err := a.ghCommandCacheDir()
	if err != nil {
		return 0, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, err
	}
	removed := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() && name == "_liveness" {
			if err := os.RemoveAll(filepath.Join(dir, name)); err == nil {
				removed++
			}
			continue
		}
		if strings.HasSuffix(name, ".lock") || isGHCommandCacheEntryFile(name) {
			if err := os.Remove(filepath.Join(dir, entry.Name())); err == nil {
				removed++
			}
		}
	}
	return removed, nil
}

const ghXCacheStatsFile = "_stats.json"

func isGHCommandCacheEntryFile(name string) bool {
	return strings.HasSuffix(name, ".json") && !strings.HasPrefix(name, "_")
}

type ghCommandCacheEntry struct {
	CreatedAt      time.Time `json:"created_at"`
	Args           []string  `json:"args"`
	Tags           []string  `json:"tags,omitempty"`
	StableIdentity string    `json:"stable_identity,omitempty"`
	ExitCode       int       `json:"exit_code"`
	Stdout         string    `json:"stdout"`
	Stderr         string    `json:"stderr"`
}

func (a *App) writeGHCommandCacheEntry(entry ghCommandCacheEntry) error {
	_, _ = io.WriteString(a.Stdout, entry.Stdout)
	_, _ = io.WriteString(a.Stderr, entry.Stderr)
	if entry.ExitCode != 0 {
		return fmt.Errorf("cached gh command failed with exit code %d", entry.ExitCode)
	}
	return nil
}

func (a *App) writeGHCommandCacheLivenessNotice(entry ghCommandCacheEntry) {
	if !ghCommandNeedsLivenessNotice(entry.Args) || entry.CreatedAt.IsZero() {
		return
	}
	_, _ = fmt.Fprintf(a.Stderr, "gitcrawl: serving cached gh %s from %s ago; use --live for CI/release liveness\n",
		ghCommandName(entry.Args), time.Since(entry.CreatedAt).Round(time.Second))
}

func readGHCommandCache(path string, ttl time.Duration) (ghCommandCacheEntry, bool) {
	entry, ok := readGHCommandCacheEntry(path)
	if !ok {
		return ghCommandCacheEntry{}, false
	}
	if entry.CreatedAt.IsZero() || time.Since(entry.CreatedAt) > ghCommandCacheEntryTTL(entry, ttl) {
		return ghCommandCacheEntry{}, false
	}
	return entry, true
}

func readGHCommandCacheEntry(path string) (ghCommandCacheEntry, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return ghCommandCacheEntry{}, false
	}
	var entry ghCommandCacheEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return ghCommandCacheEntry{}, false
	}
	return entry, true
}

func ghCommandCacheEntryTTL(entry ghCommandCacheEntry, ttl time.Duration) time.Duration {
	if entry.ExitCode == 0 {
		if completedTTL := ghCompletedRunCacheTTL(entry); completedTTL > ttl {
			return completedTTL
		}
		if closedTTL := ghClosedThreadCacheTTL(entry); closedTTL > ttl {
			return closedTTL
		}
		return ttl
	}
	errorTTL := 5 * time.Minute
	if ghCommandCacheEntryLooksRateLimited(entry) {
		errorTTL = 2 * time.Minute
	}
	if ttl > errorTTL {
		return errorTTL
	}
	return ttl
}

func ghCommandCacheEntryCanServeStale(entry ghCommandCacheEntry, ttl time.Duration) bool {
	if entry.ExitCode != 0 || entry.CreatedAt.IsZero() {
		return false
	}
	age := time.Since(entry.CreatedAt)
	if age <= ghCommandCacheEntryTTL(entry, ttl) {
		return true
	}
	return age <= ghCommandCacheEntryTTL(entry, ttl)+ghCommandCacheStaleGrace(entry.Args)
}

func ghCommandCacheEntryCanServeLowBudgetStale(entry ghCommandCacheEntry, ttl time.Duration) bool {
	if entry.ExitCode != 0 || entry.CreatedAt.IsZero() {
		return false
	}
	age := time.Since(entry.CreatedAt)
	if age <= ghCommandCacheEntryTTL(entry, ttl) {
		return true
	}
	return age <= ghCommandCacheEntryTTL(entry, ttl)+ghCommandCacheLowBudgetStaleGrace(entry.Args)
}

func ghCommandCacheLowBudgetStaleGrace(args []string) time.Duration {
	if raw := strings.TrimSpace(os.Getenv("GITCRAWL_GH_LOW_BUDGET_STALE_GRACE")); raw != "" {
		if duration, err := time.ParseDuration(raw); err == nil && duration >= 0 {
			return duration
		}
	}
	if len(args) == 0 {
		return time.Hour
	}
	switch args[0] {
	case "run":
		return 10 * time.Minute
	case "api":
		route := normalizeGHAPIRoute(args[1:])
		switch {
		case strings.Contains(route, "/actions/runs"):
			return 10 * time.Minute
		case strings.Contains(route, "/contents"), strings.HasPrefix(route, "api users/"):
			return 24 * time.Hour
		case strings.Contains(route, "/issues"), strings.Contains(route, "/pulls"):
			return 6 * time.Hour
		}
	case "issue", "pr", "repo", "release", "workflow":
		return 6 * time.Hour
	}
	return time.Hour
}

func ghCommandCacheStaleGrace(args []string) time.Duration {
	if raw := strings.TrimSpace(os.Getenv("GITCRAWL_GH_STALE_GRACE")); raw != "" {
		if duration, err := time.ParseDuration(raw); err == nil && duration >= 0 {
			return duration
		}
	}
	if len(args) == 0 {
		return 5 * time.Minute
	}
	switch args[0] {
	case "run":
		return 2 * time.Minute
	case "api":
		route := normalizeGHAPIRoute(args[1:])
		switch {
		case strings.Contains(route, "/actions/runs"):
			return 2 * time.Minute
		case strings.Contains(route, "/pages"):
			return 30 * time.Minute
		case strings.Contains(route, "/contents"):
			return 6 * time.Hour
		case strings.HasPrefix(route, "api users/"):
			return 24 * time.Hour
		}
	case "release", "workflow", "repo":
		return 30 * time.Minute
	}
	return 10 * time.Minute
}

func ghCommandCacheEntryLooksRateLimited(entry ghCommandCacheEntry) bool {
	return ghCommandOutputLooksRateLimited(entry.Stdout, entry.Stderr)
}

func ghCommandOutputLooksRateLimited(stdout, stderr string) bool {
	text := strings.ToLower(stdout + "\n" + stderr)
	return strings.Contains(text, "api rate limit") ||
		strings.Contains(text, "secondary rate limit") ||
		strings.Contains(text, "rate limit exceeded") ||
		strings.Contains(text, "x-ratelimit-remaining")
}

func writeGHCommandCache(path string, entry ghCommandCacheEntry) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	temp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tempPath)
		}
	}()
	if _, err := temp.Write(data); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tempPath, path); err != nil {
		return err
	}
	cleanup = false
	return nil
}

func tryGHCommandCacheLock(path string) (*os.File, bool) {
	lock, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, false
	}
	_, _ = fmt.Fprintf(lock, "%d\n", os.Getpid())
	return lock, true
}

func waitGHCommandCache(entryPath, lockPath string, ttl time.Duration, staleEntry ghCommandCacheEntry, hasStaleEntry bool) (ghCommandCacheEntry, string, bool) {
	if hasStaleEntry && ghCommandCacheEntryCanServeStale(staleEntry, ttl) {
		time.Sleep(250 * time.Millisecond)
		if entry, ok := readGHCommandCache(entryPath, ttl); ok {
			return entry, "fallback_hits", true
		}
		if _, err := os.Stat(lockPath); err == nil {
			return staleEntry, "stale_hits", true
		}
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
		if entry, ok := readGHCommandCache(entryPath, ttl); ok {
			return entry, "fallback_hits", true
		}
		if _, err := os.Stat(lockPath); os.IsNotExist(err) {
			return ghCommandCacheEntry{}, "", false
		}
	}
	_ = os.Remove(lockPath)
	return ghCommandCacheEntry{}, "", false
}

func (a *App) ghCommandCacheKey(ctx context.Context, args []string) string {
	return a.ghCommandCacheKeyWithStableIdentity(ctx, args, a.ghCommandStableIdentity(ctx, args))
}

func (a *App) ghCommandCacheKeyWithStableIdentity(ctx context.Context, args []string, stableIdentity string) string {
	material := strings.Join([]string{
		"v4",
		config.ResolvePath(a.configPath),
		ghCommandCacheScope(args),
		os.Getenv("GH_HOST"),
		ghCommandCacheRepoEnv(args),
		stableIdentity,
		strings.Join(canonicalGHCommandArgs(args), "\x00"),
	}, "\x00")
	sum := sha256.Sum256([]byte(material))
	return hex.EncodeToString(sum[:])
}

func ghCommandCacheScope(args []string) string {
	if ghCommandHasOwnExplicitIdentity(args) {
		return "explicit"
	}
	if os.Getenv("GH_REPO") != "" {
		return "env-repo"
	}
	cwd, _ := os.Getwd()
	return "cwd:" + cwd
}

func ghCommandCacheRepoEnv(args []string) string {
	if ghCommandHasOwnExplicitIdentity(args) {
		return ""
	}
	return os.Getenv("GH_REPO")
}

func ghCommandHasOwnExplicitIdentity(args []string) bool {
	if len(args) == 0 {
		return false
	}
	if args[0] == "api" {
		return ghAPIPathArg(args[1:]) != ""
	}
	if hasGHExplicitRepoFlag(args) {
		return true
	}
	if len(args) >= 3 && args[0] == "repo" && args[1] == "view" {
		return firstGHPositionalArg(args[2:]) != ""
	}
	return false
}

func hasGHExplicitRepoFlag(args []string) bool {
	for index := 0; index < len(args); index++ {
		arg := args[index]
		switch arg {
		case "-R", "--repo":
			return index+1 < len(args) && strings.TrimSpace(args[index+1]) != ""
		default:
			if strings.HasPrefix(arg, "--repo=") && strings.TrimSpace(strings.TrimPrefix(arg, "--repo=")) != "" {
				return true
			}
		}
	}
	return false
}

func firstGHPositionalArg(args []string) string {
	for index := 0; index < len(args); index++ {
		arg := args[index]
		if strings.HasPrefix(arg, "-") {
			if !strings.Contains(arg, "=") && index+1 < len(args) {
				index++
			}
			continue
		}
		return strings.TrimSpace(arg)
	}
	return ""
}

func (a *App) ghCommandStableIdentity(ctx context.Context, args []string) string {
	if !isGHPRDiff(args) {
		return ""
	}
	repo, number, ok := parseGHPRDiffIdentityArgs(args)
	if !ok {
		return ""
	}
	return a.ghStablePRDiffIdentity(ctx, repo, number)
}

func (a *App) ghStablePRDiffIdentity(ctx context.Context, repo string, number int) string {
	thread, err := a.localGHThread(ctx, repo, "pull_request", number)
	if err != nil {
		return ""
	}
	sha := ""
	owner, repoName, err := parseOwnerRepo(repo)
	if err == nil {
		if rt, openErr := a.openLocalRuntimeReadOnly(ctx); openErr == nil {
			if localRepo, repoErr := rt.repository(ctx, owner, repoName); repoErr == nil {
				if cache, cacheErr := rt.Store.PullRequestCache(ctx, localRepo.ID, number); cacheErr == nil {
					sha = cache.Detail.HeadSHA
				}
			}
			_ = rt.Store.Close()
		}
	}
	if sha == "" {
		sha = ghPRHeadSHAFromRawJSON(thread.RawJSON)
	}
	if sha == "" {
		return ""
	}
	return fmt.Sprintf("pr-diff:%s:%d:%s", repo, number, sha)
}

func parseStablePRDiffIdentity(value string) (repo string, number int, sha string, ok bool) {
	rest, ok := strings.CutPrefix(value, "pr-diff:")
	if !ok {
		return "", 0, "", false
	}
	repo, rest, ok = strings.Cut(rest, ":")
	if !ok {
		return "", 0, "", false
	}
	numberRaw, sha, ok := strings.Cut(rest, ":")
	if !ok {
		return "", 0, "", false
	}
	parsed, err := strconv.Atoi(numberRaw)
	if err != nil || repo == "" || parsed <= 0 || sha == "" {
		return "", 0, "", false
	}
	return repo, parsed, sha, true
}
