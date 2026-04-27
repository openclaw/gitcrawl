package cli

import (
	"context"
	"fmt"

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
