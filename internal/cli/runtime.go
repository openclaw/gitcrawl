package cli

import (
	"context"
	"errors"
	"fmt"
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
	if session, ok := ctx.Value(portableCommandKey{}).(*portableCommandSession); ok {
		session.mu.Lock()
		session.retain = true
		session.mu.Unlock()
	}
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
		// Writable opens can migrate schema as well as change user data. Record
		// ownership before either can happen, without changing source identity.
		statePath := portableStoreRefreshStatePath(mirrorPath)
		state := readPortableStoreRefreshState(statePath)
		state.MirrorWritable = true
		if err := writePortableStoreRefreshState(statePath, state); err != nil {
			return localRuntime{}, err
		}
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
	if root, ok, err := portableStoreRoot(ctx, cfg.DBPath); err != nil {
		return localRuntime{}, err
	} else if ok {
		var release func()
		ctx, release, err = acquirePortableOwner(ctx, root)
		if err != nil {
			return localRuntime{}, err
		}
		defer release()
		mirrorPath, _, err := a.ensurePortableRuntimeDB(ctx, cfg.DBPath, true)
		if err != nil {
			return localRuntime{}, err
		}
		cfg.DBPath = mirrorPath
		remoteSource = true
	}
	open := store.OpenReadOnly
	if remoteSource {
		open = func(ctx context.Context, path string) (*store.Store, error) {
			return openPortableMirrorReadOnly(ctx, path, sourceDBPath)
		}
	}
	st, err := open(ctx, cfg.DBPath)
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
