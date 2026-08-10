package cli

import (
	"compress/gzip"
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
	portableexport "github.com/openclaw/gitcrawl/internal/portable"
	"github.com/openclaw/gitcrawl/internal/store"
)

type localRuntime struct {
	Config       config.Config
	Store        *store.Store
	SourceDBPath string
	RemoteSource bool
}

type dbTargetInfo struct {
	DBTarget         string `json:"db_target,omitempty"`
	DBTargetPath     string `json:"db_target_path,omitempty"`
	PortableSourceDB string `json:"portable_source_db,omitempty"`
}

func (rt localRuntime) dbTarget() dbTargetInfo {
	if rt.RemoteSource {
		return dbTargetInfo{
			DBTarget:         "runtime-mirror",
			DBTargetPath:     rt.Config.DBPath,
			PortableSourceDB: rt.SourceDBPath,
		}
	}
	return dbTargetInfo{DBTarget: "direct", DBTargetPath: rt.Config.DBPath}
}

const portableStoreRefreshTimeout = 15 * time.Second
const portableStoreRepairTimeout = 90 * time.Second
const portableStoreRefreshTTL = 2 * time.Minute
const portableStoreRefreshFailureBackoff = time.Minute
const portableRuntimeTempMaxAge = time.Hour
const portableSourceRecoveryBackoff = 15 * time.Minute
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
	if _, ok, err := portableStoreRoot(ctx, cfg.DBPath); err != nil {
		return localRuntime{}, err
	} else if ok {
		mirrorPath, _, err := a.ensurePortableRuntimeDB(ctx, cfg.DBPath, false)
		if err != nil {
			return localRuntime{}, err
		}
		cfg.DBPath = mirrorPath
		remoteSource = true
		a.dbTargetNoticeOnce.Do(func() {
			fmt.Fprintf(a.Stderr, "gitcrawl: portable store checkout detected; writes go to the runtime mirror at %s, not the checkout database %s. Run 'gitcrawl portable prune' to publish.\n", mirrorPath, sourceDBPath)
		})
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
	if _, ok, err := portableStoreRoot(ctx, cfg.DBPath); err != nil {
		return localRuntime{}, err
	} else if ok {
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
	root, ok, err := portableStoreRoot(ctx, dbPath)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	clean := gitWorktreeClean(ctx, root)
	if !clean {
		removed, _ := removeStaleGitIndexLock(ctx, root, staleGitIndexLockAge)
		if removed {
			clean = gitWorktreeClean(ctx, root)
		}
	}
	if !clean {
		return errPortableStoreDirty
	}
	pullCtx, cancel := context.WithTimeout(ctx, portableStoreRefreshTimeout)
	defer cancel()
	if _, err := fastForwardGitCheckoutWithStaleIndexLockRetry(pullCtx, root, true); err != nil {
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
	root, ok, err := portableStoreRoot(ctx, dbPath)
	if err != nil {
		return result, err
	}
	if !ok {
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
	root, ok, err := portableStoreRoot(ctx, dbPath)
	if err != nil {
		return result, err
	}
	if !ok {
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
	mirrorPath, err := a.portableRuntimeDBPath(ctx, sourceDBPath)
	if err != nil {
		return "", false, err
	}
	changed, err := refreshPortableRuntimeDB(ctx, sourceDBPath, mirrorPath, refresh, a.configPath)
	return mirrorPath, changed, err
}

func (a *App) portableRuntimeDBPath(ctx context.Context, sourceDBPath string) (string, error) {
	root, ok, err := portableStoreRoot(ctx, sourceDBPath)
	if err != nil {
		return "", err
	}
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
	sweepOrphanPortableRuntimeTempFiles(mirrorPath, portableRuntimeTempMaxAge)
	_, isPortableSource, err := portableStoreRoot(ctx, sourceDBPath)
	if err != nil {
		return false, err
	}
	isRepairablePortableSource := isPortableSource
	if refresh {
		_ = refreshPortableStoreForDBIfDue(ctx, sourceDBPath, mirrorPath)
	}
	needsCopy, err := portableRuntimeNeedsCopy(sourceDBPath, mirrorPath)
	statePath := portableStoreRefreshStatePath(mirrorPath)
	if err != nil {
		if !isRepairablePortableSource || !errors.Is(err, os.ErrNotExist) {
			return false, err
		}
		// A recovered source is preferred, but any failure along the
		// repair/reclone chain degrades to serving a healthy mirror so a
		// missing source cannot take reads down. Recovery runs reset/pull and
		// potentially a full reclone, so attempts are backed off via the
		// recorded repair timestamp instead of repeating on every read. The
		// backoff only gates this stat-failure branch: an externally restored
		// source makes the stat succeed and skips the gate entirely.
		recoverErr := err
		state := readPortableStoreRefreshState(statePath)
		if !recentPortableRefresh(state.LastRepairAt, time.Now().UTC(), portableSourceRecoveryBackoff) {
			recoverErr = recoverMissingPortableSource(ctx, sourceDBPath, configPath, statePath)
			if recoverErr == nil {
				needsCopy, recoverErr = portableRuntimeNeedsCopy(sourceDBPath, mirrorPath)
			}
		}
		if recoverErr != nil {
			if mirrorHealthErr := sqliteStoreHealth(ctx, mirrorPath); mirrorHealthErr == nil {
				return false, nil
			}
			return false, recoverErr
		}
	}
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
		sourceHealthErr := validatePortableSQLiteSourceFile(ctx, sourceDBPath, sourceDBPath)
		if sourceHealthErr != nil && isPortableSourceRepairableHealthError(sourceHealthErr) {
			repair, err := repairMalformedPortableStoreForDB(ctx, sourceDBPath, configPath)
			recordPortableRepairState(statePath, repair, err)
			if err != nil {
				state := readPortableStoreRefreshState(statePath)
				if !recentPortableRefresh(state.LastRecloneAttempt, time.Now().UTC(), portableSourceRecoveryBackoff) {
					reclone, recloneErr := recloneMalformedPortableStoreForDB(ctx, sourceDBPath, configPath)
					recordPortableRepairState(statePath, reclone, recloneErr)
					if recloneErr == nil {
						err = nil
					}
				}
			}
			if err != nil {
				if !mirrorCorrupt {
					if mirrorHealthErr := sqliteStoreHealth(ctx, mirrorPath); mirrorHealthErr == nil {
						return false, nil
					}
				}
				return false, fmt.Errorf("repair malformed portable store db: %w", err)
			}
			sourceHealthErr = validatePortableSQLiteSourceFile(ctx, sourceDBPath, sourceDBPath)
			if sourceHealthErr != nil && isPortableSourceRepairableHealthError(sourceHealthErr) {
				reclone, err := recloneMalformedPortableStoreForDB(ctx, sourceDBPath, configPath)
				recordPortableRepairState(statePath, reclone, err)
				if err != nil {
					return false, fmt.Errorf("reclone malformed portable store db: %w", err)
				}
				sourceHealthErr = validatePortableSQLiteSourceFile(ctx, sourceDBPath, sourceDBPath)
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
	MirrorHealthSourceSHA256    string `json:"mirror_health_source_sha256,omitempty"`
	LastRepair                  string `json:"last_repair,omitempty"`
	LastRepairBackup            string `json:"last_repair_backup,omitempty"`
	LastRepairAt                string `json:"last_repair_at,omitempty"`
	LastRepairError             string `json:"last_repair_error,omitempty"`
	LastRecloneAttempt          string `json:"last_reclone_attempt,omitempty"`
}

func recoverMissingPortableSource(ctx context.Context, sourceDBPath, configPath, statePath string) error {
	repair, err := repairMalformedPortableStoreForDB(ctx, sourceDBPath, configPath)
	recordPortableRepairState(statePath, repair, err)
	if err != nil {
		state := readPortableStoreRefreshState(statePath)
		if !recentPortableRefresh(state.LastRecloneAttempt, time.Now().UTC(), portableSourceRecoveryBackoff) {
			reclone, recloneErr := recloneMalformedPortableStoreForDB(ctx, sourceDBPath, configPath)
			recordPortableRepairState(statePath, reclone, recloneErr)
			if recloneErr == nil {
				// The caller re-stats the source and falls back to a healthy
				// mirror if the recloned store still lacks the database;
				// falling through here would reclone a second time.
				return nil
			}
			return fmt.Errorf("repair malformed portable store db: %w; reclone fallback: %v", err, recloneErr)
		}
		return fmt.Errorf("repair malformed portable store db: %w", err)
	}
	if _, statErr := os.Stat(sourceDBPath); errors.Is(statErr, os.ErrNotExist) {
		reclone, recloneErr := recloneMalformedPortableStoreForDB(ctx, sourceDBPath, configPath)
		recordPortableRepairState(statePath, reclone, recloneErr)
		if recloneErr != nil {
			return fmt.Errorf("reclone malformed portable store db: %w", recloneErr)
		}
	}
	return nil
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
	now := time.Now().UTC().Format(time.RFC3339Nano)
	state.LastRepair = result.Action
	state.LastRepairAt = now
	if result.Action == "recloned" {
		state.LastRecloneAttempt = now
	}
	state.LastRepairBackup = result.DBBackupPath
	if result.StoreBackupPath != "" {
		state.LastRepairBackup = result.StoreBackupPath
	}
	if repairErr != nil {
		state.LastRepairError = repairErr.Error()
	} else {
		state.LastRepairError = ""
	}
	// The state file gates repair/reclone backoffs; without its parent
	// directory (fresh install, mirror never created) a silent write failure
	// would leave recovery attempts unbounded.
	_ = os.MkdirAll(filepath.Dir(path), 0o755)
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
	manifestModTime, manifestSize, sourceSHA256, err := portableDBManifestStamp(sourceDBPath)
	if err != nil {
		return err
	}
	if err := sqliteStoreCachedHealthWithManifest(ctx, mirrorPath, sourceDBPath, statePath, manifestModTime, manifestSize, sourceSHA256); err != nil {
		return err
	}
	return nil
}

func sqliteStoreCachedHealthWithManifest(ctx context.Context, path, sourceDBPath, statePath, manifestModTime string, manifestSize int64, sourceSHA256 string) error {
	return sqliteStoreCachedHealthWithManifestChecks(
		ctx,
		path,
		sourceDBPath,
		statePath,
		manifestModTime,
		manifestSize,
		sourceSHA256,
		sqliteStoreOpenHealth,
		sqliteStoreHealth,
	)
}

func sqliteStoreCachedHealthWithManifestChecks(ctx context.Context, path, sourceDBPath, statePath, manifestModTime string, manifestSize int64, sourceSHA256 string, openHealthCheck, fullHealthCheck func(context.Context, string) error) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	state := readPortableStoreRefreshState(statePath)
	modTime := info.ModTime().UTC().Format(time.RFC3339Nano)
	manifestGenerationUnchanged := portableManifestGenerationUnchanged(
		state,
		manifestModTime,
		manifestSize,
		sourceSHA256,
	)
	manifestGenerationRecorded := sourceSHA256 == "" || state.MirrorHealthSourceSHA256 != ""
	if state.MirrorHealthSize == info.Size() &&
		state.MirrorHealthModTime == modTime &&
		manifestGenerationUnchanged &&
		manifestGenerationRecorded {
		return openHealthCheck(ctx, path)
	}
	if manifestModTime == "" || manifestGenerationUnchanged {
		if err := fullHealthCheck(ctx, path); err != nil {
			return err
		}
		return markSQLiteStoreHealthVerifiedWithManifest(path, statePath, manifestModTime, manifestSize, sourceSHA256)
	}
	if err := validatePortableSQLiteFile(ctx, path, sourceDBPath); err != nil {
		return err
	}
	return markSQLiteStoreHealthVerifiedWithManifest(path, statePath, manifestModTime, manifestSize, sourceSHA256)
}

func portableManifestGenerationUnchanged(state portableStoreRefreshState, manifestModTime string, manifestSize int64, sourceSHA256 string) bool {
	if sourceSHA256 != "" {
		if state.MirrorHealthSourceSHA256 != "" {
			return strings.EqualFold(state.MirrorHealthSourceSHA256, sourceSHA256)
		}
	}
	return state.MirrorHealthManifestSize == manifestSize &&
		state.MirrorHealthManifestModTime == manifestModTime
}

func markPortableMirrorHealthVerified(path, statePath, sourceDBPath string) error {
	manifestModTime, manifestSize, sourceSHA256, err := portableDBManifestStamp(sourceDBPath)
	if err != nil {
		return err
	}
	return markSQLiteStoreHealthVerifiedWithManifest(path, statePath, manifestModTime, manifestSize, sourceSHA256)
}

func markSQLiteStoreHealthVerifiedWithManifest(path, statePath, manifestModTime string, manifestSize int64, sourceSHA256 string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	state := readPortableStoreRefreshState(statePath)
	state.MirrorHealthSize = info.Size()
	state.MirrorHealthModTime = info.ModTime().UTC().Format(time.RFC3339Nano)
	state.MirrorHealthManifestSize = manifestSize
	state.MirrorHealthManifestModTime = manifestModTime
	state.MirrorHealthSourceSHA256 = sourceSHA256
	return writePortableStoreRefreshState(statePath, state)
}

func portableDBManifestStamp(dbPath string) (string, int64, string, error) {
	if strings.TrimSpace(dbPath) == "" {
		return "", 0, "", nil
	}
	manifestPath := portableDBManifestPath(dbPath)
	info, err := os.Stat(manifestPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", 0, "", nil
		}
		return "", 0, "", err
	}
	manifest, ok, err := readPortableDBManifest(manifestPath)
	if err != nil {
		return "", 0, "", fmt.Errorf("portable manifest mismatch: %w", err)
	}
	if !ok {
		return "", 0, "", nil
	}
	return info.ModTime().UTC().Format(time.RFC3339Nano), info.Size(), strings.ToLower(strings.TrimSpace(manifest.SHA256)), nil
}

func sqliteStoreHealth(ctx context.Context, path string) error {
	return sqliteStoreHealthWithOpen(ctx, path, store.OpenReadOnly)
}

func sqliteStoreImmutableHealth(ctx context.Context, path string) error {
	return sqliteStoreHealthWithOpen(ctx, path, store.OpenReadOnlyImmutable)
}

func sqliteStoreHealthWithOpen(ctx context.Context, path string, open func(context.Context, string) (*store.Store, error)) error {
	st, err := open(ctx, path)
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
	Schema            string `json:"schema,omitempty"`
	Profile           string `json:"profile,omitempty"`
	ExportedAt        string `json:"exportedAt,omitempty"`
	OutputPath        string `json:"outputPath,omitempty"`
	OutputBytes       int64  `json:"outputBytes,omitempty"`
	SHA256            string `json:"sha256,omitempty"`
	ArtifactID        string `json:"artifactId,omitempty"`
	ArtifactIDProfile string `json:"artifactIdProfile,omitempty"`
	QuickCheck        string `json:"quickCheck,omitempty"`
	Compression       string `json:"compression,omitempty"`
	ArchivePath       string `json:"archivePath,omitempty"`
	ArchiveBytes      int64  `json:"archiveBytes,omitempty"`
	ArchiveSHA256     string `json:"archiveSha256,omitempty"`
}

func portableDBManifestPath(dbPath string) string {
	return dbPath + ".manifest.json"
}

func validatePortableSQLiteFile(ctx context.Context, dbPath, manifestDBPath string) error {
	if err := sqliteStoreHealth(ctx, dbPath); err != nil {
		return err
	}
	return validatePortableDBManifest(ctx, dbPath, portableDBManifestPath(manifestDBPath))
}

func validatePortableSQLiteSourceFile(ctx context.Context, dbPath, manifestDBPath string) error {
	_, _, compressed, err := portableSourceArtifact(dbPath)
	if err != nil {
		return err
	}
	if !compressed {
		if err := sqliteStoreImmutableHealth(ctx, dbPath); err != nil {
			return err
		}
		return validatePortableDBManifest(ctx, dbPath, portableDBManifestPath(manifestDBPath))
	}
	tempDir, err := os.MkdirTemp("", "gitcrawl-portable-source-*")
	if err != nil {
		return fmt.Errorf("create portable source validation dir: %w", err)
	}
	defer os.RemoveAll(tempDir)
	tempPath, err := stagePortableSQLiteSourceTemp(dbPath, filepath.Join(tempDir, filepath.Base(dbPath)), 0o600)
	if err != nil {
		return err
	}
	defer func() {
		_ = os.Remove(tempPath)
		removeSQLiteTempSidecars(tempPath)
	}()
	if err := sqliteStoreImmutableHealth(ctx, tempPath); err != nil {
		return err
	}
	return validatePortableDBManifest(ctx, tempPath, portableDBManifestPath(manifestDBPath))
}

func validatePortableDBManifest(ctx context.Context, dbPath, manifestPath string) error {
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
	artifactID := strings.TrimSpace(manifest.ArtifactID)
	artifactIDProfile := strings.TrimSpace(manifest.ArtifactIDProfile)
	if artifactIDProfile == "" {
		// Derived manifests published before semantic identity used artifactId as
		// an exact-SHA alias. Keep that additive manifest evolution readable.
		if artifactID != "" && !strings.EqualFold(artifactID, strings.TrimSpace(manifest.SHA256)) {
			return fmt.Errorf("portable manifest mismatch: legacy artifactId %s != sha256 %s", manifest.ArtifactID, manifest.SHA256)
		}
	} else {
		if manifest.Profile != portableexport.CurrentStateV1 {
			return fmt.Errorf("portable manifest mismatch: profile %q does not support semantic artifact identity", manifest.Profile)
		}
		if artifactIDProfile != portableexport.CurrentStateSemanticV1 {
			return fmt.Errorf("portable manifest mismatch: unsupported artifactIdProfile %q", manifest.ArtifactIDProfile)
		}
		computedArtifactID, err := portableexport.ComputeArtifactID(ctx, dbPath, artifactIDProfile)
		if err != nil {
			return fmt.Errorf("portable manifest mismatch: recompute artifactId: %w", err)
		}
		if artifactID == "" || !strings.EqualFold(computedArtifactID, artifactID) {
			return fmt.Errorf("portable manifest mismatch: artifactId %s != %s", computedArtifactID, manifest.ArtifactID)
		}
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

func portableSourceArtifact(dbPath string) (string, portableDBManifest, bool, error) {
	manifestPath := portableDBManifestPath(dbPath)
	manifest, ok, err := readPortableDBManifest(manifestPath)
	if err != nil {
		return "", portableDBManifest{}, false, fmt.Errorf("portable manifest mismatch: %w", err)
	}
	if !ok || strings.TrimSpace(manifest.Compression) == "" {
		return dbPath, manifest, false, nil
	}
	if strings.TrimSpace(manifest.Compression) != "gzip" {
		return "", portableDBManifest{}, false, fmt.Errorf(
			"portable manifest mismatch: unsupported compression %q",
			manifest.Compression,
		)
	}
	archiveRaw := strings.TrimSpace(manifest.ArchivePath)
	if archiveRaw == "" ||
		strings.HasPrefix(archiveRaw, "/") ||
		strings.HasPrefix(archiveRaw, "\\") ||
		(len(archiveRaw) >= 2 && archiveRaw[1] == ':') {
		return "", portableDBManifest{}, false, fmt.Errorf("portable manifest mismatch: archivePath must be relative")
	}
	for _, component := range strings.FieldsFunc(archiveRaw, func(r rune) bool {
		return r == '/' || r == '\\'
	}) {
		if component == ".." {
			return "", portableDBManifest{}, false, fmt.Errorf("portable manifest mismatch: archivePath escapes the store")
		}
	}
	archivePath := filepath.FromSlash(archiveRaw)
	archivePath = filepath.Clean(archivePath)
	if archivePath == "." || archivePath == ".." || strings.HasPrefix(archivePath, ".."+string(os.PathSeparator)) {
		return "", portableDBManifest{}, false, fmt.Errorf("portable manifest mismatch: archivePath escapes the store")
	}
	manifestDir := filepath.Dir(manifestPath)
	resolved := filepath.Join(manifestDir, archivePath)
	relative, err := filepath.Rel(manifestDir, resolved)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(os.PathSeparator)) {
		return "", portableDBManifest{}, false, fmt.Errorf("portable manifest mismatch: archivePath escapes the store")
	}
	if manifest.ArchiveBytes <= 0 {
		return "", portableDBManifest{}, false, fmt.Errorf("portable manifest mismatch: archiveBytes missing")
	}
	if strings.TrimSpace(manifest.ArchiveSHA256) == "" {
		return "", portableDBManifest{}, false, fmt.Errorf("portable manifest mismatch: archiveSha256 missing")
	}
	return resolved, manifest, true, nil
}

func validatePortableArchive(path string, manifest portableDBManifest) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if info.Size() != manifest.ArchiveBytes {
		return fmt.Errorf(
			"portable manifest mismatch: archive size %d != %d",
			info.Size(),
			manifest.ArchiveBytes,
		)
	}
	sum, err := fileSHA256(path)
	if err != nil {
		return err
	}
	sumText := fmt.Sprintf("%x", sum)
	if !strings.EqualFold(sumText, strings.TrimSpace(manifest.ArchiveSHA256)) {
		return fmt.Errorf(
			"portable manifest mismatch: archive sha256 %s != %s",
			sumText,
			manifest.ArchiveSHA256,
		)
	}
	return nil
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
	paths := []string{
		dbPath,
		dbPath + "-wal",
		dbPath + "-shm",
		dbPath + ".manifest.json",
	}
	if archivePath, _, compressed, err := portableSourceArtifact(dbPath); err == nil && compressed {
		paths = append(paths, archivePath)
	}
	for _, path := range paths {
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
	sourcePath, _, _, err := portableSourceArtifact(sourceDBPath)
	if err != nil {
		return false, err
	}
	sourceInfo, err := os.Stat(sourcePath)
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
	tempPath, err := stageFileCopyTemp(sourcePath, targetPath, 0o600)
	if err != nil {
		return err
	}
	if err := os.Rename(tempPath, targetPath); err != nil {
		_ = os.Remove(tempPath)
		removeSQLiteTempSidecars(tempPath)
		return fmt.Errorf("replace portable runtime db: %w", err)
	}
	removeSQLiteTempSidecars(tempPath)
	removeSQLiteTempSidecars(targetPath)
	return nil
}

func stageFileCopyTemp(sourcePath, targetPath string, mode os.FileMode) (string, error) {
	if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
		return "", fmt.Errorf("create portable runtime dir: %w", err)
	}
	source, err := os.Open(sourcePath)
	if err != nil {
		return "", fmt.Errorf("open portable source db: %w", err)
	}
	defer source.Close()
	temp, err := os.CreateTemp(filepath.Dir(targetPath), "."+filepath.Base(targetPath)+".tmp-*")
	if err != nil {
		return "", fmt.Errorf("create portable runtime temp db: %w", err)
	}
	tempPath := temp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tempPath)
			removeSQLiteTempSidecars(tempPath)
		}
	}()
	if _, err := io.Copy(temp, source); err != nil {
		_ = temp.Close()
		return "", fmt.Errorf("copy portable runtime db: %w", err)
	}
	if err := temp.Chmod(mode); err != nil {
		_ = temp.Close()
		return "", fmt.Errorf("chmod portable runtime db: %w", err)
	}
	if err := temp.Close(); err != nil {
		return "", fmt.Errorf("close portable runtime db: %w", err)
	}
	cleanup = false
	return tempPath, nil
}

func copySQLiteFileAtomicVerified(ctx context.Context, sourcePath, targetPath string) error {
	tempPath, err := stagePortableSQLiteSourceTemp(sourcePath, targetPath, 0o600)
	if err != nil {
		return err
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tempPath)
			removeSQLiteTempSidecars(tempPath)
		}
	}()
	if err := validatePortableSQLiteFile(ctx, tempPath, sourcePath); err != nil {
		return fmt.Errorf("validate portable runtime temp db: %w", err)
	}
	if err := os.Rename(tempPath, targetPath); err != nil {
		return fmt.Errorf("replace portable runtime db: %w", err)
	}
	cleanup = false
	removeSQLiteTempSidecars(tempPath)
	removeSQLiteTempSidecars(targetPath)
	return nil
}

func stagePortableSQLiteSourceTemp(sourceDBPath, targetPath string, mode os.FileMode) (string, error) {
	sourcePath, manifest, compressed, err := portableSourceArtifact(sourceDBPath)
	if err != nil {
		return "", err
	}
	if !compressed {
		return stageFileCopyTemp(sourcePath, targetPath, mode)
	}
	if err := validatePortableArchive(sourcePath, manifest); err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
		return "", fmt.Errorf("create portable runtime dir: %w", err)
	}
	source, err := os.Open(sourcePath)
	if err != nil {
		return "", fmt.Errorf("open portable source archive: %w", err)
	}
	defer source.Close()
	reader, err := gzip.NewReader(source)
	if err != nil {
		return "", fmt.Errorf("open portable gzip archive: %w", err)
	}
	defer reader.Close()
	temp, err := os.CreateTemp(filepath.Dir(targetPath), "."+filepath.Base(targetPath)+".tmp-*")
	if err != nil {
		return "", fmt.Errorf("create portable runtime temp db: %w", err)
	}
	tempPath := temp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tempPath)
			removeSQLiteTempSidecars(tempPath)
		}
	}()
	written, copyErr := io.Copy(temp, io.LimitReader(reader, manifest.OutputBytes+1))
	if copyErr != nil {
		_ = temp.Close()
		return "", fmt.Errorf("inflate portable runtime db: %w", copyErr)
	}
	if written != manifest.OutputBytes {
		_ = temp.Close()
		return "", fmt.Errorf(
			"portable manifest mismatch: inflated size %d != %d",
			written,
			manifest.OutputBytes,
		)
	}
	if err := temp.Chmod(mode); err != nil {
		_ = temp.Close()
		return "", fmt.Errorf("chmod portable runtime db: %w", err)
	}
	if err := temp.Close(); err != nil {
		return "", fmt.Errorf("close portable runtime db: %w", err)
	}
	cleanup = false
	return tempPath, nil
}

// publishPortableCheckoutPair replaces the checkout database and manifest with
// the pruned mirror pair. Both files are staged and validated inside the
// checkout before the first rename so a staging or validation failure leaves
// the previously published pair untouched.
func publishPortableCheckoutPair(ctx context.Context, mirrorDBPath, mirrorManifestPath, checkoutDBPath, checkoutManifestPath string) error {
	mode := os.FileMode(0o644)
	if info, err := os.Stat(checkoutDBPath); err == nil {
		mode = info.Mode().Perm()
	}
	manifestMode := mode
	if info, err := os.Stat(checkoutManifestPath); err == nil {
		manifestMode = info.Mode().Perm()
	}
	tempDB, err := stageFileCopyTemp(mirrorDBPath, checkoutDBPath, mode)
	if err != nil {
		return fmt.Errorf("stage published portable db: %w", err)
	}
	stagedDB := tempDB
	defer func() {
		if tempDB != "" {
			_ = os.Remove(tempDB)
		}
		removeSQLiteTempSidecars(stagedDB)
	}()
	tempManifest, err := stageFileCopyTemp(mirrorManifestPath, checkoutManifestPath, manifestMode)
	if err != nil {
		return fmt.Errorf("stage published portable manifest: %w", err)
	}
	defer func() {
		if tempManifest != "" {
			_ = os.Remove(tempManifest)
		}
	}()
	if err := sqliteStoreImmutableHealth(ctx, tempDB); err != nil {
		return fmt.Errorf("validate staged portable db: %w", err)
	}
	if err := validatePortableDBManifest(ctx, tempDB, tempManifest); err != nil {
		return fmt.Errorf("validate staged portable manifest: %w", err)
	}
	// The pair is replaced with two adjacent renames; a crash between them is
	// the same manifest-mismatch state a consumer's interrupted `git pull` can
	// produce, and manifest validation plus the git-based repair path recover
	// it. Concurrent publishers are out of contract (see the portable-store
	// caveats) the same way concurrent `git push` publishers are. A failed
	// manifest rename rolls the database back from the backup so an error
	// return leaves a consistent previous pair.
	rollbackDB := ""
	if _, err := os.Stat(checkoutDBPath); err == nil {
		backup, err := stageRollbackBackup(checkoutDBPath)
		if err != nil {
			return fmt.Errorf("back up published portable db: %w", err)
		}
		rollbackDB = backup
	}
	defer func() {
		if rollbackDB != "" {
			_ = os.Remove(rollbackDB)
		}
	}()
	if err := os.Rename(tempDB, checkoutDBPath); err != nil {
		return fmt.Errorf("replace published portable db: %w", err)
	}
	tempDB = ""
	removeSQLiteTempSidecars(checkoutDBPath)
	if err := os.Rename(tempManifest, checkoutManifestPath); err != nil {
		if rollbackDB != "" {
			backupPath := rollbackDB
			rollbackDB = ""
			if restoreErr := os.Rename(backupPath, checkoutDBPath); restoreErr != nil {
				// Keep the backup on disk for manual recovery.
				return fmt.Errorf("replace published portable manifest: %w; restoring the previous database from %s also failed: %v", err, backupPath, restoreErr)
			}
		}
		return fmt.Errorf("replace published portable manifest: %w", err)
	}
	tempManifest = ""
	return nil
}

// stageRollbackBackup snapshots path under a unique sibling name, preferring a
// hard link and falling back to a byte copy on filesystems without link
// support. The caller owns the returned file.
func stageRollbackBackup(path string) (string, error) {
	placeholder, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".publish-rollback-*")
	if err != nil {
		return "", err
	}
	backupPath := placeholder.Name()
	if err := placeholder.Close(); err != nil {
		_ = os.Remove(backupPath)
		return "", err
	}
	if err := os.Remove(backupPath); err != nil {
		return "", err
	}
	if err := os.Link(path, backupPath); err == nil {
		return backupPath, nil
	}
	mode := os.FileMode(0o600)
	if info, err := os.Stat(path); err == nil {
		mode = info.Mode().Perm()
	}
	temp, err := stageFileCopyTemp(path, backupPath, mode)
	if err != nil {
		return "", err
	}
	if err := os.Rename(temp, backupPath); err != nil {
		_ = os.Remove(temp)
		removeSQLiteTempSidecars(temp)
		return "", err
	}
	return backupPath, nil
}

func removeSQLiteTempSidecars(path string) {
	_ = os.Remove(path + "-wal")
	_ = os.Remove(path + "-shm")
}

func sweepOrphanPortableRuntimeTempFiles(mirrorPath string, maxAge time.Duration) {
	dir := filepath.Dir(mirrorPath)
	prefix := "." + filepath.Base(mirrorPath) + ".tmp-"
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	cutoff := time.Now().Add(-maxAge)
	for _, entry := range entries {
		name := entry.Name()
		if entry.Type().IsRegular() && strings.HasPrefix(name, prefix) {
			info, err := entry.Info()
			if err == nil && info.ModTime().Before(cutoff) {
				_ = os.Remove(filepath.Join(dir, name))
			}
		}
	}
}

func portableStoreRoot(ctx context.Context, dbPath string) (string, bool, error) {
	dir := filepath.Clean(filepath.Dir(dbPath))
	for {
		info, statErr := os.Stat(filepath.Join(dir, ".git"))
		if statErr == nil && info.IsDir() {
			isWorktree, err := probePortableStoreGitWorktree(ctx, dir)
			if err != nil {
				return "", false, fmt.Errorf("verify portable store candidate %s: %w", dir, err)
			}
			if isWorktree {
				return dir, true, nil
			}
		} else if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
			return "", false, fmt.Errorf("inspect portable store candidate %s: %w", dir, statErr)
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", false, nil
		}
		dir = parent
	}
}

func probePortableStoreGitWorktree(ctx context.Context, dir string) (bool, error) {
	initialized, err := gitMetadataLooksInitialized(filepath.Join(dir, ".git"))
	if err != nil {
		return false, err
	}
	if !initialized {
		return false, nil
	}

	topLevel, stderr, err := runGitCommandOutputWithEnvSeparate(ctx, "", portableStoreGitProbeEnv(), "-C", dir, "rev-parse", "--show-toplevel")
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return false, ctxErr
		}
		return false, fmt.Errorf("resolve portable store worktree: %w\n%s", err, strings.TrimSpace(stderr))
	}
	if !sameExistingPath(strings.TrimSpace(topLevel), dir) {
		return false, fmt.Errorf("Git resolved portable store candidate %s to worktree %s", dir, strings.TrimSpace(topLevel))
	}

	gitDir, stderr, err := runGitCommandOutputWithEnvSeparate(ctx, "", portableStoreGitProbeEnv(), "-C", dir, "rev-parse", "--absolute-git-dir")
	if err != nil {
		return false, fmt.Errorf("resolve portable store Git directory: %w\n%s", err, strings.TrimSpace(stderr))
	}
	if !sameExistingPath(strings.TrimSpace(gitDir), filepath.Join(dir, ".git")) {
		return false, fmt.Errorf("Git resolved portable store candidate %s to Git directory %s", dir, strings.TrimSpace(gitDir))
	}
	return true, nil
}

func gitMetadataLooksInitialized(gitDir string) (bool, error) {
	for _, name := range []string{"HEAD", "config", "objects", "refs"} {
		_, err := os.Lstat(filepath.Join(gitDir, name))
		switch {
		case err == nil:
			return true, nil
		case errors.Is(err, os.ErrNotExist):
			continue
		default:
			return false, fmt.Errorf("inspect Git metadata %s: %w", gitDir, err)
		}
	}
	return false, nil
}

func sameExistingPath(left, right string) bool {
	leftInfo, err := os.Stat(left)
	if err != nil {
		return false
	}
	rightInfo, err := os.Stat(right)
	return err == nil && os.SameFile(leftInfo, rightInfo)
}

func portableStoreGitProbeEnv() []string {
	repositoryEnv := map[string]struct{}{
		"GIT_ALTERNATE_OBJECT_DIRECTORIES": {},
		"GIT_CEILING_DIRECTORIES":          {},
		"GIT_COMMON_DIR":                   {},
		"GIT_DIR":                          {},
		"GIT_DISCOVERY_ACROSS_FILESYSTEM":  {},
		"GIT_GRAFT_FILE":                   {},
		"GIT_INDEX_FILE":                   {},
		"GIT_NAMESPACE":                    {},
		"GIT_OBJECT_DIRECTORY":             {},
		"GIT_PREFIX":                       {},
		"GIT_QUARANTINE_PATH":              {},
		"GIT_SHALLOW_FILE":                 {},
		"GIT_WORK_TREE":                    {},
	}
	env := make([]string, 0, len(os.Environ()))
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		upperName := strings.ToUpper(name)
		_, excluded := repositoryEnv[upperName]
		if excluded || upperName == "GIT_CONFIG_COUNT" || upperName == "GIT_CONFIG_PARAMETERS" ||
			strings.HasPrefix(upperName, "GIT_CONFIG_KEY_") || strings.HasPrefix(upperName, "GIT_CONFIG_VALUE_") {
			continue
		}
		env = append(env, entry)
	}
	return env
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
