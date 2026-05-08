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
	"strings"
	"time"

	"github.com/openclaw/gitcrawl/internal/config"
)

func (a *App) execRealGHMaybeCached(ctx context.Context, args []string) error {
	if !cacheableGHRead(args) {
		err := a.execRealGH(ctx, args)
		if err == nil && mutatingGHCommand(args) {
			_ = a.incrementGHXCacheCounter("pass_through_writes")
			_ = a.clearGHCommandCacheForMutation(ctx, args)
		}
		return err
	}
	cacheDir, err := a.ghCommandCacheDir()
	if err != nil {
		return a.execRealGH(ctx, args)
	}
	ttl := a.ghCommandCacheTTL(ctx, args)
	entryPath := filepath.Join(cacheDir, a.ghCommandCacheKey(ctx, args)+".json")
	staleEntry, hasStaleEntry := readGHCommandCacheEntry(entryPath)
	if entry, ok := readGHCommandCache(entryPath, ttl); ok {
		_ = a.incrementGHXCacheCounter("fallback_hits")
		return a.writeGHCommandCacheEntry(entry)
	}
	lockPath := entryPath + ".lock"
	lock, locked := tryGHCommandCacheLock(lockPath)
	if !locked {
		if entry, hit, ok := waitGHCommandCache(entryPath, lockPath, ttl, staleEntry, hasStaleEntry); ok {
			_ = a.incrementGHXCacheCounter(hit)
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
			return a.writeGHCommandCacheEntry(entry)
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
		_ = writeGHCommandCache(entryPath, ghCommandCacheEntry{
			CreatedAt: time.Now().UTC(),
			Args:      append([]string(nil), args...),
			Tags:      a.ghCommandCacheTags(ctx, args),
			ExitCode:  exitCode,
			Stdout:    stdout,
			Stderr:    stderr,
		})
	}
	_, _ = io.WriteString(a.Stdout, stdout)
	_, _ = io.WriteString(a.Stderr, stderr)
	return err
}

func cacheGHReadErrors() bool {
	return !strings.EqualFold(strings.TrimSpace(os.Getenv("GITCRAWL_GH_CACHE_ERRORS")), "0")
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
	CreatedAt time.Time `json:"created_at"`
	Args      []string  `json:"args"`
	Tags      []string  `json:"tags,omitempty"`
	ExitCode  int       `json:"exit_code"`
	Stdout    string    `json:"stdout"`
	Stderr    string    `json:"stderr"`
}

func (a *App) writeGHCommandCacheEntry(entry ghCommandCacheEntry) error {
	_, _ = io.WriteString(a.Stdout, entry.Stdout)
	_, _ = io.WriteString(a.Stderr, entry.Stderr)
	if entry.ExitCode != 0 {
		return fmt.Errorf("cached gh command failed with exit code %d", entry.ExitCode)
	}
	return nil
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
	material := strings.Join([]string{
		"v4",
		config.ResolvePath(a.configPath),
		ghCommandCacheScope(args),
		os.Getenv("GH_HOST"),
		ghCommandCacheRepoEnv(args),
		a.ghCommandStableIdentity(ctx, args),
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
