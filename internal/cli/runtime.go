package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/openclaw/gitcrawl/internal/config"
	"github.com/openclaw/gitcrawl/internal/store"
)

type localRuntime struct {
	Config config.Config
	Store  *store.Store
}

func (a *App) openLocalRuntime(ctx context.Context) (localRuntime, error) {
	cfg, err := config.Load(a.configPath)
	if err != nil {
		return localRuntime{}, err
	}
	st, err := store.Open(ctx, cfg.DBPath)
	if err != nil {
		return localRuntime{}, err
	}
	return localRuntime{Config: cfg, Store: st}, nil
}

func (a *App) openLocalRuntimeReadOnly(ctx context.Context) (localRuntime, error) {
	cfg, err := config.Load(a.configPath)
	if err != nil {
		return localRuntime{}, err
	}
	_ = refreshPortableStoreForDB(ctx, cfg.DBPath)
	st, err := store.OpenReadOnly(ctx, cfg.DBPath)
	if err != nil {
		return localRuntime{}, err
	}
	return localRuntime{Config: cfg, Store: st}, nil
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
		return nil
	}
	return runGit(ctx, "", "-C", root, "pull", "--ff-only", "--quiet")
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
