package cli

import (
	"context"

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
