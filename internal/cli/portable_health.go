package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/openclaw/gitcrawl/internal/store"
)

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
	open := func(ctx context.Context, path string) (*store.Store, error) {
		return openPortableMirrorReadOnly(ctx, path, sourceDBPath)
	}
	return sqliteStoreCachedHealthWithManifestChecks(
		ctx,
		path,
		sourceDBPath,
		statePath,
		manifestModTime,
		manifestSize,
		sourceSHA256,
		func(ctx context.Context, path string) error {
			st, err := open(ctx, path)
			if err != nil {
				return err
			}
			return st.Close()
		},
		func(ctx context.Context, path string) error {
			return sqliteStoreHealthWithOpen(ctx, path, open)
		},
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
	if err := fullHealthCheck(ctx, path); err != nil {
		return err
	}
	if err := validatePortableDBManifest(ctx, path, portableDBManifestPath(sourceDBPath)); err != nil {
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

func markPortableMirrorHealthVerified(path, statePath, sourceDBPath, replicaSHA256 string) error {
	manifestModTime, manifestSize, sourceSHA256, err := portableDBManifestStamp(sourceDBPath)
	if err != nil {
		return err
	}
	if sourceSHA256 != "" && !strings.EqualFold(sourceSHA256, replicaSHA256) {
		return fmt.Errorf("portable manifest changed after runtime validation")
	}
	state := readPortableStoreRefreshState(statePath)
	if state.MirrorWritable {
		state.MirrorWritable = false
		if err := writePortableStoreRefreshState(statePath, state); err != nil {
			return err
		}
	}
	// The caller just promoted validated bytes. A raw source without a
	// manifest still has a replica identity; health-only checks cannot invent it.
	return markSQLiteStoreHealthVerifiedWithManifest(path, statePath, manifestModTime, manifestSize, replicaSHA256)
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
	if sourceSHA256 != "" {
		state.MirrorHealthSourceSHA256 = sourceSHA256
	}
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
