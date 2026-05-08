package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/openclaw/gitcrawl/internal/config"
	"github.com/openclaw/gitcrawl/internal/store"
)

type localRuntime struct {
	Config       config.Config
	Store        *store.Store
	SourceDBPath string
	RemoteSource bool
}

const portableStoreRefreshTimeout = 15 * time.Second
const portableStoreRefreshTTL = 2 * time.Minute
const portableStoreRefreshFailureBackoff = time.Minute

var errPortableStoreDirty = errors.New("portable store checkout has local changes")

func (a *App) openLocalRuntime(ctx context.Context) (localRuntime, error) {
	cfg, err := config.LoadRuntime(a.configPath)
	if err != nil {
		return localRuntime{}, err
	}
	sourceDBPath := cfg.DBPath
	remoteSource := false
	if _, ok := portableStoreRoot(cfg.DBPath); ok {
		mirrorPath, _, err := a.ensurePortableRuntimeDB(ctx, cfg.DBPath, false)
		if err != nil {
			return localRuntime{}, err
		}
		cfg.DBPath = mirrorPath
		remoteSource = true
	}
	st, err := store.Open(ctx, cfg.DBPath)
	if err != nil {
		return localRuntime{}, err
	}
	return localRuntime{Config: cfg, Store: st, SourceDBPath: sourceDBPath, RemoteSource: remoteSource}, nil
}

func (a *App) openLocalRuntimeReadOnly(ctx context.Context) (localRuntime, error) {
	cfg, err := config.LoadRuntime(a.configPath)
	if err != nil {
		return localRuntime{}, err
	}
	sourceDBPath := cfg.DBPath
	remoteSource := false
	if _, ok := portableStoreRoot(cfg.DBPath); ok {
		mirrorPath, _, err := a.ensurePortableRuntimeDB(ctx, cfg.DBPath, true)
		if err != nil {
			return localRuntime{}, err
		}
		cfg.DBPath = mirrorPath
		remoteSource = true
	}
	st, err := store.OpenReadOnly(ctx, cfg.DBPath)
	if err != nil {
		return localRuntime{}, err
	}
	return localRuntime{Config: cfg, Store: st, SourceDBPath: sourceDBPath, RemoteSource: remoteSource}, nil
}

func (rt localRuntime) repository(ctx context.Context, owner, repo string) (store.Repository, error) {
	return rt.Store.RepositoryByFullName(ctx, owner+"/"+repo)
}

func (rt localRuntime) defaultRepository(ctx context.Context) (store.Repository, error) {
	repos, err := rt.Store.ListRepositories(ctx)
	if err != nil {
		return store.Repository{}, err
	}
	if len(repos) == 0 {
		return store.Repository{}, fmt.Errorf("no local repositories found")
	}
	return repos[0], nil
}

func refreshPortableStoreForDB(ctx context.Context, dbPath string) error {
	root, ok := portableStoreRoot(dbPath)
	if !ok {
		return nil
	}
	if !gitWorktreeClean(ctx, root) {
		return errPortableStoreDirty
	}
	pullCtx, cancel := context.WithTimeout(ctx, portableStoreRefreshTimeout)
	defer cancel()
	if err := fastForwardGitCheckout(pullCtx, root, true); err != nil {
		return err
	}
	return removePortableSQLiteSidecars(root)
}

var portableRuntimeMu sync.Mutex

func (a *App) ensurePortableRuntimeDB(ctx context.Context, sourceDBPath string, refresh bool) (string, bool, error) {
	mirrorPath, err := a.portableRuntimeDBPath(sourceDBPath)
	if err != nil {
		return "", false, err
	}
	changed, err := refreshPortableRuntimeDB(ctx, sourceDBPath, mirrorPath, refresh)
	return mirrorPath, changed, err
}

func (a *App) portableRuntimeDBPath(sourceDBPath string) (string, error) {
	root, ok := portableStoreRoot(sourceDBPath)
	if !ok {
		return "", fmt.Errorf("portable store root not found for %s", sourceDBPath)
	}
	rel, err := filepath.Rel(root, sourceDBPath)
	if err != nil || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) || rel == ".." || filepath.IsAbs(rel) {
		return "", fmt.Errorf("portable database %s is outside store root %s", sourceDBPath, root)
	}
	name := safePathName(filepath.Base(root))
	if name == "" {
		name = "portable-store"
	}
	return filepath.Join(filepath.Dir(config.ResolvePath(a.configPath)), "runtime", name, rel), nil
}

func refreshPortableRuntimeDB(ctx context.Context, sourceDBPath, mirrorPath string, refresh bool) (bool, error) {
	portableRuntimeMu.Lock()
	defer portableRuntimeMu.Unlock()
	if refresh {
		_ = refreshPortableStoreForDBIfDue(ctx, sourceDBPath, mirrorPath)
	}
	needsCopy, err := portableRuntimeNeedsCopy(sourceDBPath, mirrorPath)
	if err != nil {
		return false, err
	}
	if !needsCopy {
		return false, nil
	}
	if err := copyFileAtomic(sourceDBPath, mirrorPath); err != nil {
		return false, err
	}
	return true, nil
}

type portableStoreRefreshState struct {
	LastAttempt string `json:"last_attempt,omitempty"`
	LastSuccess string `json:"last_success,omitempty"`
	LastFailure string `json:"last_failure,omitempty"`
	Error       string `json:"error,omitempty"`
}

func refreshPortableStoreForDBIfDue(ctx context.Context, sourceDBPath, mirrorPath string) error {
	ttl := portableStoreRefreshInterval()
	statePath := portableStoreRefreshStatePath(mirrorPath)
	state := readPortableStoreRefreshState(statePath)
	now := time.Now().UTC()
	if ttl > 0 && recentPortableRefresh(state.LastSuccess, now, ttl) {
		return nil
	}
	if ttl > 0 && recentPortableRefresh(state.LastFailure, now, portableStoreRefreshFailureBackoff) {
		return nil
	}
	lockPath := statePath + ".lock"
	if err := os.MkdirAll(filepath.Dir(statePath), 0o755); err != nil {
		return err
	}
	removeStalePortableRefreshLock(lockPath, now)
	lock, locked := tryGHCommandCacheLock(lockPath)
	if !locked {
		return nil
	}
	defer func() {
		_ = lock.Close()
		_ = os.Remove(lockPath)
	}()
	state = readPortableStoreRefreshState(statePath)
	now = time.Now().UTC()
	if ttl > 0 && recentPortableRefresh(state.LastSuccess, now, ttl) {
		return nil
	}
	state.LastAttempt = now.Format(time.RFC3339Nano)
	err := refreshPortableStoreForDB(ctx, sourceDBPath)
	if err != nil {
		state.LastFailure = time.Now().UTC().Format(time.RFC3339Nano)
		state.Error = err.Error()
		_ = writePortableStoreRefreshState(statePath, state)
		return err
	}
	state.LastSuccess = time.Now().UTC().Format(time.RFC3339Nano)
	state.LastFailure = ""
	state.Error = ""
	return writePortableStoreRefreshState(statePath, state)
}

func removeStalePortableRefreshLock(path string, now time.Time) {
	info, err := os.Stat(path)
	if err != nil {
		return
	}
	if now.Sub(info.ModTime()) <= 2*portableStoreRefreshTimeout {
		return
	}
	_ = os.Remove(path)
}

func portableStoreRefreshInterval() time.Duration {
	if raw := strings.TrimSpace(os.Getenv("GITCRAWL_PORTABLE_REFRESH_TTL")); raw != "" {
		if duration, err := time.ParseDuration(raw); err == nil && duration >= 0 {
			return duration
		}
	}
	return portableStoreRefreshTTL
}

func portableStoreRefreshStatePath(mirrorPath string) string {
	return filepath.Join(filepath.Dir(mirrorPath), ".portable-refresh.json")
}

func readPortableStoreRefreshState(path string) portableStoreRefreshState {
	data, err := os.ReadFile(path)
	if err != nil {
		return portableStoreRefreshState{}
	}
	var state portableStoreRefreshState
	if err := json.Unmarshal(data, &state); err != nil {
		return portableStoreRefreshState{}
	}
	return state
}

func writePortableStoreRefreshState(path string, state portableStoreRefreshState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return writeAtomicFile(path, data, 0o600)
}

func recentPortableRefresh(value string, now time.Time, maxAge time.Duration) bool {
	if strings.TrimSpace(value) == "" {
		return false
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return false
	}
	return now.Sub(parsed) <= maxAge
}

func portableRuntimeNeedsCopy(sourceDBPath, mirrorPath string) (bool, error) {
	sourceInfo, err := os.Stat(sourceDBPath)
	if err != nil {
		return false, fmt.Errorf("stat portable source db: %w", err)
	}
	mirrorInfo, err := os.Stat(mirrorPath)
	if err != nil {
		if os.IsNotExist(err) {
			return true, nil
		}
		return false, fmt.Errorf("stat portable runtime db: %w", err)
	}
	return sourceInfo.ModTime().After(mirrorInfo.ModTime()), nil
}

func copyFileAtomic(sourcePath, targetPath string) error {
	if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
		return fmt.Errorf("create portable runtime dir: %w", err)
	}
	source, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("open portable source db: %w", err)
	}
	defer source.Close()
	temp, err := os.CreateTemp(filepath.Dir(targetPath), "."+filepath.Base(targetPath)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create portable runtime temp db: %w", err)
	}
	tempPath := temp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tempPath)
		}
	}()
	if _, err := io.Copy(temp, source); err != nil {
		_ = temp.Close()
		return fmt.Errorf("copy portable runtime db: %w", err)
	}
	if err := temp.Chmod(0o600); err != nil {
		_ = temp.Close()
		return fmt.Errorf("chmod portable runtime db: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close portable runtime db: %w", err)
	}
	if err := os.Rename(tempPath, targetPath); err != nil {
		return fmt.Errorf("replace portable runtime db: %w", err)
	}
	cleanup = false
	_ = os.Remove(targetPath + "-wal")
	_ = os.Remove(targetPath + "-shm")
	return nil
}

func portableStoreRoot(dbPath string) (string, bool) {
	dir := filepath.Clean(filepath.Dir(dbPath))
	for {
		if info, err := os.Stat(filepath.Join(dir, ".git")); err == nil && info.IsDir() {
			return dir, true
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", false
		}
		dir = parent
	}
}

func gitWorktreeClean(ctx context.Context, dir string) bool {
	if err := runGit(ctx, "", "-C", dir, "update-index", "-q", "--refresh"); err != nil {
		return false
	}
	if err := runGit(ctx, "", "-C", dir, "diff", "--quiet", "--"); err != nil {
		return false
	}
	if err := runGit(ctx, "", "-C", dir, "diff", "--cached", "--quiet", "--"); err != nil {
		return false
	}
	return true
}
