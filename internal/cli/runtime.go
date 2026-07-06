package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	crawlremote "github.com/openclaw/crawlkit/remote"
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
const portableStoreRepairTimeout = 90 * time.Second
const portableStoreRefreshTTL = 2 * time.Minute
const portableStoreRefreshFailureBackoff = time.Minute
const portableStoreMarkerFile = "gitcrawl-portable-store"
const staleGitIndexLockAge = 2 * time.Second

var errPortableStoreDirty = errors.New("portable store checkout has local changes")

func (a *App) openLocalRuntime(ctx context.Context) (localRuntime, error) {
	cfg, err := config.LoadRuntime(a.configPath)
	if err != nil {
		return localRuntime{}, err
	}
	if cfg.Remote.Enabled() && cfg.Remote.Mode == crawlremote.ModeCloud {
		return localRuntime{}, fmt.Errorf("command requires a local gitcrawl database; config is remote cloud mode")
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
	return a.openLocalRuntimeReadOnlyWithConfig(ctx, cfg)
}

func (a *App) openLocalRuntimeReadOnlyWithConfig(ctx context.Context, cfg config.Config) (localRuntime, error) {
	if cfg.Remote.Enabled() && cfg.Remote.Mode == crawlremote.ModeCloud {
		return localRuntime{}, fmt.Errorf("command requires a local gitcrawl database; config is remote cloud mode")
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
	if !portableStoreIsGitWorktree(ctx, root) {
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

type portableRepairResult struct {
	Action           string
	DBBackupPath     string
	StoreBackupPath  string
	RemovedIndexLock bool
}

func repairMalformedPortableStoreForDB(ctx context.Context, dbPath, configPath string) (portableRepairResult, error) {
	result := portableRepairResult{Action: "reset-pulled"}
	root, ok := portableStoreRoot(dbPath)
	if !ok {
		return result, nil
	}
	if !portableStoreIsGitWorktree(ctx, root) {
		return result, nil
	}
	if !portableStoreRepairAllowed(root, configPath) {
		return result, fmt.Errorf("refuse destructive repair for unmarked portable store checkout %s", root)
	}
	backupPath, err := preserveMalformedPortableDB(root, dbPath)
	if err != nil {
		return result, err
	}
	result.DBBackupPath = backupPath
	pullCtx, cancel := context.WithTimeout(ctx, portableStoreRepairTimeout)
	defer cancel()
	if !gitWorktreeClean(pullCtx, root) {
		removed, err := runGitWithStaleIndexLockRetry(pullCtx, root, "-C", root, "reset", "--hard", "HEAD")
		result.RemovedIndexLock = result.RemovedIndexLock || removed
		if err != nil {
			return result, err
		}
	}
	removed, err := fastForwardGitCheckoutWithStaleIndexLockRetry(pullCtx, root, true)
	result.RemovedIndexLock = result.RemovedIndexLock || removed
	if err != nil {
		return result, err
	}
	return result, removePortableSQLiteSidecars(root)
}

func recloneMalformedPortableStoreForDB(ctx context.Context, dbPath, configPath string) (portableRepairResult, error) {
	result := portableRepairResult{Action: "recloned"}
	root, ok := portableStoreRoot(dbPath)
	if !ok {
		return result, nil
	}
	if !portableStoreIsGitWorktree(ctx, root) {
		return result, nil
	}
	if !portableStoreRepairAllowed(root, configPath) {
		return result, fmt.Errorf("refuse reclone for unmarked portable store checkout %s", root)
	}
	remote := portableStoreRemoteURL(ctx, root)
	if strings.TrimSpace(remote) == "" {
		return result, fmt.Errorf("portable store remote not found for %s", root)
	}
	branch := currentGitBranch(ctx, root)
	timestamp := time.Now().UTC().Format("20060102T150405Z")
	backupPath := filepath.Join(filepath.Dir(root), "backups", "checkout-malformed-"+timestamp)
	if err := os.MkdirAll(filepath.Dir(backupPath), 0o755); err != nil {
		return result, fmt.Errorf("create portable checkout backup parent: %w", err)
	}
	if err := os.Rename(root, backupPath); err != nil {
		return result, fmt.Errorf("preserve malformed portable checkout: %w", err)
	}
	result.StoreBackupPath = backupPath
	cloneCtx, cancel := context.WithTimeout(ctx, portableStoreRepairTimeout)
	defer cancel()
	cloneArgs := []string{"clone", "--depth", "1"}
	if strings.TrimSpace(branch) != "" {
		cloneArgs = append(cloneArgs, "--branch", branch)
	}
	cloneArgs = append(cloneArgs, remote, root)
	if err := runGit(cloneCtx, "", cloneArgs...); err != nil {
		_ = os.RemoveAll(root)
		_ = os.Rename(backupPath, root)
		return result, err
	}
	if err := markPortableStoreCheckout(root); err != nil {
		return result, err
	}
	return result, removePortableSQLiteSidecars(root)
}

var portableRuntimeMu sync.Mutex

func (a *App) ensurePortableRuntimeDB(ctx context.Context, sourceDBPath string, refresh bool) (string, bool, error) {
	mirrorPath, err := a.portableRuntimeDBPath(sourceDBPath)
	if err != nil {
		return "", false, err
	}
	changed, err := refreshPortableRuntimeDB(ctx, sourceDBPath, mirrorPath, refresh, a.configPath)
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

func refreshPortableRuntimeDB(ctx context.Context, sourceDBPath, mirrorPath string, refresh bool, configPath string) (bool, error) {
	portableRuntimeMu.Lock()
	defer portableRuntimeMu.Unlock()
	portableRoot, isPortableSource := portableStoreRoot(sourceDBPath)
	isRepairablePortableSource := isPortableSource && portableStoreIsGitWorktree(ctx, portableRoot)
	if refresh {
		_ = refreshPortableStoreForDBIfDue(ctx, sourceDBPath, mirrorPath)
	}
	needsCopy, err := portableRuntimeNeedsCopy(sourceDBPath, mirrorPath)
	if err != nil {
		return false, err
	}
	statePath := portableStoreRefreshStatePath(mirrorPath)
	mirrorCorrupt := false
	if isRepairablePortableSource && !needsCopy {
		mirrorHealthErr := portableMirrorCachedHealth(ctx, mirrorPath, sourceDBPath, statePath)
		if mirrorHealthErr != nil {
			if isSQLiteCorruption(mirrorHealthErr) {
				mirrorCorrupt = true
				needsCopy = true
			} else if isPortableManifestMismatch(mirrorHealthErr) {
				needsCopy = true
			} else {
				return false, fmt.Errorf("check portable runtime db: %w", mirrorHealthErr)
			}
		}
	}
	if needsCopy && isRepairablePortableSource {
		sourceHealthErr := validatePortableSQLiteFile(ctx, sourceDBPath, sourceDBPath)
		if sourceHealthErr != nil && isPortableSourceRepairableHealthError(sourceHealthErr) {
			repair, err := repairMalformedPortableStoreForDB(ctx, sourceDBPath, configPath)
			recordPortableRepairState(statePath, repair, err)
			if err != nil {
				if !mirrorCorrupt {
					if mirrorHealthErr := sqliteStoreHealth(ctx, mirrorPath); mirrorHealthErr == nil {
						return false, nil
					}
				}
				return false, fmt.Errorf("repair malformed portable store db: %w", err)
			}
			sourceHealthErr = validatePortableSQLiteFile(ctx, sourceDBPath, sourceDBPath)
			if sourceHealthErr != nil && isPortableSourceRepairableHealthError(sourceHealthErr) {
				reclone, err := recloneMalformedPortableStoreForDB(ctx, sourceDBPath, configPath)
				recordPortableRepairState(statePath, reclone, err)
				if err != nil {
					return false, fmt.Errorf("reclone malformed portable store db: %w", err)
				}
				sourceHealthErr = validatePortableSQLiteFile(ctx, sourceDBPath, sourceDBPath)
			}
		}
		if sourceHealthErr != nil {
			return false, fmt.Errorf("check portable source db: %w", sourceHealthErr)
		}
	}
	if !needsCopy {
		return false, nil
	}
	if err := copySQLiteFileAtomicVerified(ctx, sourceDBPath, mirrorPath); err != nil {
		return false, err
	}
	if isRepairablePortableSource {
		_ = markPortableMirrorHealthVerified(mirrorPath, statePath, sourceDBPath)
	}
	return true, nil
}

type portableStoreRefreshState struct {
	LastAttempt                 string `json:"last_attempt,omitempty"`
	LastSuccess                 string `json:"last_success,omitempty"`
	LastFailure                 string `json:"last_failure,omitempty"`
	Error                       string `json:"error,omitempty"`
	MirrorHealthModTime         string `json:"mirror_health_mod_time,omitempty"`
	MirrorHealthSize            int64  `json:"mirror_health_size,omitempty"`
	MirrorHealthManifestModTime string `json:"mirror_health_manifest_mod_time,omitempty"`
	MirrorHealthManifestSize    int64  `json:"mirror_health_manifest_size,omitempty"`
	LastRepair                  string `json:"last_repair,omitempty"`
	LastRepairBackup            string `json:"last_repair_backup,omitempty"`
	LastRepairAt                string `json:"last_repair_at,omitempty"`
	LastRepairError             string `json:"last_repair_error,omitempty"`
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

func recordPortableRepairState(path string, result portableRepairResult, repairErr error) {
	if strings.TrimSpace(path) == "" || strings.TrimSpace(result.Action) == "" {
		return
	}
	state := readPortableStoreRefreshState(path)
	state.LastRepair = result.Action
	state.LastRepairAt = time.Now().UTC().Format(time.RFC3339Nano)
	state.LastRepairBackup = result.DBBackupPath
	if result.StoreBackupPath != "" {
		state.LastRepairBackup = result.StoreBackupPath
	}
	if repairErr != nil {
		state.LastRepairError = repairErr.Error()
	} else {
		state.LastRepairError = ""
	}
	_ = writePortableStoreRefreshState(path, state)
}

func sqliteStoreOpenHealth(ctx context.Context, path string) error {
	if strings.TrimSpace(path) == "" {
		return os.ErrNotExist
	}
	if _, err := os.Stat(path); err != nil {
		return err
	}
	st, err := store.OpenReadOnly(ctx, path)
	if err != nil {
		return err
	}
	return st.Close()
}

func portableMirrorCachedHealth(ctx context.Context, mirrorPath, sourceDBPath, statePath string) error {
	manifestModTime, manifestSize, err := portableDBManifestStamp(sourceDBPath)
	if err != nil {
		return err
	}
	if err := sqliteStoreCachedHealthWithManifest(ctx, mirrorPath, sourceDBPath, statePath, manifestModTime, manifestSize); err != nil {
		return err
	}
	return nil
}

func sqliteStoreCachedHealthWithManifest(ctx context.Context, path, sourceDBPath, statePath, manifestModTime string, manifestSize int64) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	state := readPortableStoreRefreshState(statePath)
	modTime := info.ModTime().UTC().Format(time.RFC3339Nano)
	if state.MirrorHealthSize == info.Size() &&
		state.MirrorHealthModTime == modTime &&
		state.MirrorHealthManifestSize == manifestSize &&
		state.MirrorHealthManifestModTime == manifestModTime {
		return sqliteStoreOpenHealth(ctx, path)
	}
	if manifestModTime == "" {
		if err := sqliteStoreHealth(ctx, path); err != nil {
			return err
		}
		return markPortableMirrorHealthVerified(path, statePath, "")
	}
	if err := validatePortableSQLiteFile(ctx, path, sourceDBPath); err != nil {
		return err
	}
	return markSQLiteStoreHealthVerifiedWithManifest(path, statePath, manifestModTime, manifestSize)
}

func markPortableMirrorHealthVerified(path, statePath, sourceDBPath string) error {
	manifestModTime, manifestSize, err := portableDBManifestStamp(sourceDBPath)
	if err != nil {
		return err
	}
	return markSQLiteStoreHealthVerifiedWithManifest(path, statePath, manifestModTime, manifestSize)
}

func markSQLiteStoreHealthVerifiedWithManifest(path, statePath, manifestModTime string, manifestSize int64) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	state := readPortableStoreRefreshState(statePath)
	state.MirrorHealthSize = info.Size()
	state.MirrorHealthModTime = info.ModTime().UTC().Format(time.RFC3339Nano)
	state.MirrorHealthManifestSize = manifestSize
	state.MirrorHealthManifestModTime = manifestModTime
	return writePortableStoreRefreshState(statePath, state)
}

func portableDBManifestStamp(dbPath string) (string, int64, error) {
	if strings.TrimSpace(dbPath) == "" {
		return "", 0, nil
	}
	info, err := os.Stat(portableDBManifestPath(dbPath))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", 0, nil
		}
		return "", 0, err
	}
	return info.ModTime().UTC().Format(time.RFC3339Nano), info.Size(), nil
}

func sqliteStoreHealth(ctx context.Context, path string) error {
	st, err := store.OpenReadOnly(ctx, path)
	if err != nil {
		return err
	}
	defer st.Close()
	rows, err := st.DB().QueryContext(ctx, `pragma quick_check`)
	if err != nil {
		return err
	}
	defer rows.Close()
	var problems []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return err
		}
		if strings.TrimSpace(line) != "ok" {
			problems = append(problems, line)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(problems) > 0 {
		return fmt.Errorf("sqlite quick_check failed: %s", strings.Join(problems, "; "))
	}
	return nil
}

type portableDBManifest struct {
	Schema      string `json:"schema,omitempty"`
	ExportedAt  string `json:"exportedAt,omitempty"`
	OutputPath  string `json:"outputPath,omitempty"`
	OutputBytes int64  `json:"outputBytes,omitempty"`
	SHA256      string `json:"sha256,omitempty"`
	QuickCheck  string `json:"quickCheck,omitempty"`
}

func portableDBManifestPath(dbPath string) string {
	return dbPath + ".manifest.json"
}

func validatePortableSQLiteFile(ctx context.Context, dbPath, manifestDBPath string) error {
	if err := sqliteStoreHealth(ctx, dbPath); err != nil {
		return err
	}
	return validatePortableDBManifest(dbPath, portableDBManifestPath(manifestDBPath))
}

func validatePortableDBManifest(dbPath, manifestPath string) error {
	manifest, ok, err := readPortableDBManifest(manifestPath)
	if err != nil {
		return fmt.Errorf("portable manifest mismatch: %w", err)
	}
	if !ok {
		return nil
	}
	info, err := os.Stat(dbPath)
	if err != nil {
		return err
	}
	if strings.TrimSpace(manifest.Schema) == "" {
		return fmt.Errorf("portable manifest mismatch: schema missing")
	}
	if manifest.OutputBytes <= 0 {
		return fmt.Errorf("portable manifest mismatch: outputBytes missing")
	}
	if strings.TrimSpace(manifest.SHA256) == "" {
		return fmt.Errorf("portable manifest mismatch: sha256 missing")
	}
	if strings.TrimSpace(manifest.QuickCheck) != "" && strings.TrimSpace(manifest.QuickCheck) != "ok" {
		return fmt.Errorf("portable manifest mismatch: quickCheck %q", manifest.QuickCheck)
	}
	if manifest.OutputBytes > 0 && info.Size() != manifest.OutputBytes {
		return fmt.Errorf("portable manifest mismatch: size %d != %d", info.Size(), manifest.OutputBytes)
	}
	sum, err := fileSHA256(dbPath)
	if err != nil {
		return err
	}
	sumText := fmt.Sprintf("%x", sum)
	if !strings.EqualFold(sumText, strings.TrimSpace(manifest.SHA256)) {
		return fmt.Errorf("portable manifest mismatch: sha256 %s != %s", sumText, manifest.SHA256)
	}
	return nil
}

func readPortableDBManifest(path string) (portableDBManifest, bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return portableDBManifest{}, false, nil
		}
		return portableDBManifest{}, false, err
	}
	var manifest portableDBManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return portableDBManifest{}, true, fmt.Errorf("read portable manifest: %w", err)
	}
	return manifest, true, nil
}

func isPortableSourceRepairableHealthError(err error) bool {
	return isSQLiteCorruption(err) || isPortableManifestMismatch(err)
}

func isSQLiteCorruption(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "database disk image is malformed") ||
		strings.Contains(message, "file is not a database") ||
		strings.Contains(message, "sqlite quick_check failed") ||
		strings.Contains(message, "sqlite_corrupt") ||
		strings.Contains(message, "error code 11") ||
		strings.Contains(message, "(11)")
}

func isPortableManifestMismatch(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "portable manifest mismatch")
}

func preserveMalformedPortableDB(root, dbPath string) (string, error) {
	timestamp := time.Now().UTC().Format("20060102T150405Z")
	backupDir := filepath.Join(filepath.Dir(root), "backups", "malformed-"+timestamp)
	if err := os.MkdirAll(backupDir, 0o755); err != nil {
		return "", fmt.Errorf("create malformed db backup: %w", err)
	}
	for _, path := range []string{
		dbPath,
		dbPath + "-wal",
		dbPath + "-shm",
		dbPath + ".manifest.json",
	} {
		if _, err := os.Stat(path); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return "", err
		}
		target := filepath.Join(backupDir, filepath.Base(path)+".malformed")
		if strings.HasSuffix(path, ".manifest.json") {
			target = filepath.Join(backupDir, filepath.Base(path))
		}
		if err := copyFileAtomic(path, target); err != nil {
			return "", fmt.Errorf("preserve malformed db evidence: %w", err)
		}
	}
	return backupDir, nil
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

func copySQLiteFileAtomicVerified(ctx context.Context, sourcePath, targetPath string) error {
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
	if err := validatePortableSQLiteFile(ctx, tempPath, sourcePath); err != nil {
		return fmt.Errorf("validate portable runtime temp db: %w", err)
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

func portableStoreIsGitWorktree(ctx context.Context, dir string) bool {
	out, err := gitOutput(ctx, "", "-C", dir, "rev-parse", "--is-inside-work-tree")
	return err == nil && strings.TrimSpace(out) == "true"
}

func portableStoreRemoteURL(ctx context.Context, root string) string {
	branch := currentGitBranch(ctx, root)
	remoteName := gitBranchRemote(ctx, root, branch)
	if remoteName != "" {
		remote, err := gitConfigValue(ctx, root, "remote."+remoteName+".url")
		if err == nil && strings.TrimSpace(remote) != "" {
			return strings.TrimSpace(remote)
		}
	}
	return gitRemoteURL(ctx, root)
}

func portableStoreRepairAllowed(root, configPath string) bool {
	if strings.TrimSpace(root) == "" {
		return false
	}
	if info, err := os.Stat(filepath.Join(root, ".git", "info", portableStoreMarkerFile)); err == nil && !info.IsDir() {
		return true
	}
	defaultStoresDir := filepath.Join(filepath.Dir(config.ResolvePath(configPath)), "stores")
	rel, err := filepath.Rel(defaultStoresDir, root)
	return err == nil && rel != "." && rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator)) && !filepath.IsAbs(rel)
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

func fastForwardGitCheckoutWithStaleIndexLockRetry(ctx context.Context, root string, quiet bool) (bool, error) {
	err := fastForwardGitCheckout(ctx, root, quiet)
	if err == nil {
		return false, nil
	}
	if !isGitIndexLockError(err) {
		return false, err
	}
	removed, cleanupErr := removeStaleGitIndexLock(ctx, root, staleGitIndexLockAge)
	if cleanupErr != nil || !removed {
		if cleanupErr != nil {
			return false, fmt.Errorf("%w; cleanup stale index lock: %v", err, cleanupErr)
		}
		return false, err
	}
	return true, fastForwardGitCheckout(ctx, root, quiet)
}

func runGitWithStaleIndexLockRetry(ctx context.Context, root string, args ...string) (bool, error) {
	err := runGit(ctx, "", args...)
	if err == nil {
		return false, nil
	}
	if !isGitIndexLockError(err) {
		return false, err
	}
	removed, cleanupErr := removeStaleGitIndexLock(ctx, root, staleGitIndexLockAge)
	if cleanupErr != nil || !removed {
		if cleanupErr != nil {
			return false, fmt.Errorf("%w; cleanup stale index lock: %v", err, cleanupErr)
		}
		return false, err
	}
	return true, runGit(ctx, "", args...)
}

func isGitIndexLockError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "index.lock") && strings.Contains(message, "file exists")
}

func removeStaleGitIndexLock(ctx context.Context, root string, minAge time.Duration) (bool, error) {
	lockPath := filepath.Join(root, ".git", "index.lock")
	info, err := os.Stat(lockPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	if minAge > 0 && time.Since(info.ModTime()) < minAge {
		return false, nil
	}
	lsofPath, err := exec.LookPath("lsof")
	if err != nil {
		return false, nil
	}
	cmd := exec.CommandContext(ctx, lsofPath, lockPath)
	out, err := cmd.CombinedOutput()
	if strings.TrimSpace(string(out)) != "" {
		return false, nil
	}
	if err != nil {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) || exitErr.ExitCode() != 1 {
			return false, nil
		}
	}
	if err := os.Remove(lockPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	return true, nil
}
