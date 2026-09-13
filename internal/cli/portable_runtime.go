package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/openclaw/gitcrawl/internal/config"
)

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
	root, isPortableSource, err := portableStoreRoot(ctx, sourceDBPath)
	if err != nil {
		return false, err
	}
	if isPortableSource {
		var release func()
		ctx, release, err = acquirePortableOwner(ctx, root)
		if err != nil {
			return false, err
		}
		defer release()
	}
	statePath := portableStoreRefreshStatePath(mirrorPath)
	state := readPortableStoreRefreshState(statePath)
	local, err := portableRuntimeHasLocalChanges(ctx, sourceDBPath, mirrorPath, state)
	if err != nil {
		return false, err
	}
	if local {
		if err := sqliteStoreHealth(ctx, mirrorPath); err == nil {
			// Keep the original source digest/stamp: healthy local work does
			// not become a copy of the publisher's latest generation.
			changed := !state.MirrorWritable
			if state.MirrorHealthSourceSHA256 == "" {
				modTime, size, sha, stampErr := portableDBManifestStamp(sourceDBPath)
				if stampErr == nil && modTime != "" && portableManifestGenerationUnchanged(state, modTime, size, sha) {
					state.MirrorHealthSourceSHA256 = sha
					changed = true
				}
			}
			if changed {
				state.MirrorWritable = true
				if err := writePortableStoreRefreshState(statePath, state); err != nil {
					return false, err
				}
			}
			return false, nil
		} else if state.MirrorWritable || !isSQLiteCorruption(err) {
			return false, fmt.Errorf("check locally modified portable runtime (preserved): %w", err)
		}
		// A corrupt, never-writable replica still follows normal recovery.
	}
	sweepOrphanPortableRuntimeTempFiles(mirrorPath, portableRuntimeTempMaxAge)
	isRepairablePortableSource := isPortableSource
	if refresh {
		_ = refreshPortableStoreForDBIfDue(ctx, sourceDBPath, mirrorPath)
	}
	needsCopy, err := portableRuntimeNeedsCopy(sourceDBPath, mirrorPath)
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
	digest, err := copySQLiteFileAtomicVerified(ctx, sourceDBPath, mirrorPath)
	if err != nil {
		return false, err
	}
	if isRepairablePortableSource {
		if err := markPortableMirrorHealthVerified(mirrorPath, statePath, sourceDBPath, fmt.Sprintf("%x", digest)); err != nil {
			return false, err
		}
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
	MirrorWritable              bool   `json:"mirror_writable,omitempty"`
	LastRepair                  string `json:"last_repair,omitempty"`
	LastRepairBackup            string `json:"last_repair_backup,omitempty"`
	LastRepairAt                string `json:"last_repair_at,omitempty"`
	LastRepairError             string `json:"last_repair_error,omitempty"`
	LastRecloneAttempt          string `json:"last_reclone_attempt,omitempty"`
}

func portableRuntimeHasLocalChanges(ctx context.Context, source, path string, state portableStoreRefreshState) (bool, error) {
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if state.MirrorWritable {
		return true, nil
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if _, err := os.Stat(path + suffix); err == nil {
			return true, nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return false, err
		}
	}
	if state.MirrorHealthSourceSHA256 == "" {
		return false, nil
	}
	// A legacy health stamp can describe local bytes, not a pristine replica.
	// Recheck the digest whenever the source or runtime may have changed.
	if state.MirrorHealthSize == info.Size() && state.MirrorHealthModTime == info.ModTime().UTC().Format(time.RFC3339Nano) {
		modTime, size, sha, err := portableDBManifestStamp(source)
		needsCopy, copyErr := portableRuntimeNeedsCopy(source, path)
		if err == nil && copyErr == nil && !needsCopy && portableManifestGenerationUnchanged(state, modTime, size, sha) {
			return false, nil
		}
	}
	digest, err := portableFileSHA256(ctx, path)
	return !strings.EqualFold(fmt.Sprintf("%x", digest), state.MirrorHealthSourceSHA256), err
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
	root, ok, err := portableStoreRoot(ctx, sourceDBPath)
	if err != nil || !ok {
		return err
	}
	ctx, release, err := acquirePortableOwner(ctx, root)
	if err != nil {
		return err
	}
	defer release()
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
	if err := os.MkdirAll(filepath.Dir(statePath), 0o755); err != nil {
		return err
	}
	state = readPortableStoreRefreshState(statePath)
	now = time.Now().UTC()
	if ttl > 0 && recentPortableRefresh(state.LastSuccess, now, ttl) {
		return nil
	}
	state.LastAttempt = now.Format(time.RFC3339Nano)
	err = refreshPortableStoreForDB(ctx, sourceDBPath)
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
