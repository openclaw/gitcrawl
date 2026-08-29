package cli

import (
	"context"
	"errors"
	"os"
	"path/filepath"

	"github.com/openclaw/crawlkit/control"
	"github.com/openclaw/gitcrawl/internal/config"
	"github.com/openclaw/gitcrawl/internal/store"
)

// Status resolves the same artifact/runtime paths as readers, but never calls
// their refresh or repair path. A gzip-only source is inspected in private temp.
func (a *App) localArchiveStatus(ctx context.Context, cfg config.Config) (control.Status, error) {
	path, reportedPath := cfg.DBPath, cfg.DBPath
	var warnings []string
	stale := false
	immutable := false
	runtimeMirror := false
	_, portable, err := portableStoreRoot(ctx, cfg.DBPath)
	if err != nil {
		return control.Status{}, err
	}
	if portable {
		mirror, err := a.portableRuntimeDBPath(ctx, cfg.DBPath)
		if err != nil {
			return control.Status{}, err
		}
		if _, err := os.Stat(mirror); err == nil {
			path, reportedPath = mirror, mirror
			runtimeMirror = true
			state := readPortableStoreRefreshState(portableStoreRefreshStatePath(mirror))
			modTime, size, sha, stampErr := portableDBManifestStamp(cfg.DBPath)
			if stampErr != nil || !portableManifestGenerationUnchanged(state, modTime, size, sha) {
				stale = true
				warnings = append(warnings, "Runtime source generation differs from the checkout or cannot be verified; status did not refresh it.")
			}
			if state.MirrorWritable {
				warnings = append(warnings, "Runtime is writable local state; publisher updates do not replace it.")
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return control.Status{}, err
		} else {
			artifact, _, compressed, err := portableSourceArtifact(cfg.DBPath)
			if err != nil {
				return control.Status{}, err
			}
			if compressed {
				dir, err := os.MkdirTemp("", "gitcrawl-status-*")
				if err != nil {
					return control.Status{}, err
				}
				defer os.RemoveAll(dir)
				path, err = stagePortableSQLiteSourceTempContext(ctx, cfg.DBPath, filepath.Join(dir, "archive.db"), 0o600)
				if err != nil {
					return control.Status{}, err
				}
				if err := validatePortableSQLiteFile(ctx, path, cfg.DBPath); err != nil {
					return control.Status{}, err
				}
				reportedPath, immutable = artifact, true
				warnings = append(warnings, "No runtime mirror exists; counts describe the validated gzip artifact and database bytes are its compressed on-disk size.")
			}
		}
	}
	status := store.Status{DBPath: reportedPath}
	if _, err := os.Stat(path); err == nil {
		open := store.OpenReadOnly
		if immutable {
			open = store.OpenReadOnlyImmutable
		} else if runtimeMirror {
			open = func(ctx context.Context, path string) (*store.Store, error) {
				return openPortableMirrorReadOnly(ctx, path, cfg.DBPath)
			}
		}
		st, err := open(ctx, path)
		if err != nil {
			return control.Status{}, err
		}
		defer st.Close()
		status, err = st.Status(ctx)
		if err != nil {
			return control.Status{}, err
		}
	} else if !errors.Is(err, os.ErrNotExist) || portable {
		return control.Status{}, err
	}
	status.DBPath = reportedPath
	out := controlStatus(config.ResolvePath(a.configPath), cfg, status)
	if stale {
		out.State = "stale"
	}
	out.Warnings = append(out.Warnings, warnings...)
	if immutable {
		out.Databases[0].Kind = "sqlite-gzip"
	}
	return out, nil
}
