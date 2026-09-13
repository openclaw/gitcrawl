package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/openclaw/gitcrawl/internal/config"
	"github.com/openclaw/gitcrawl/internal/store"
)

func (a *App) runTUI(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("tui", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	minSizeRaw := fs.String("min-size", "", "minimum active member count")
	limitRaw := fs.String("limit", "", "maximum cluster rows")
	sortMode := fs.String("sort", "", "sort mode: recent|oldest|size")
	layoutMode := fs.String("layout", "", "layout mode: focus|columns|right-stack")
	includeClosed := fs.Bool("include-closed", false, "deprecated; closed clusters are shown by default")
	hideClosed := fs.Bool("hide-closed", false, "hide locally closed clusters")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"min-size": true, "limit": true, "sort": true, "layout": true})); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return a.printCommandUsage("tui")
		}
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() > 1 {
		return usageErr(fmt.Errorf("tui accepts at most one owner/repo"))
	}

	minSize, err := parseOptionalPositiveInt(*minSizeRaw)
	if err != nil {
		return usageErr(err)
	}
	if strings.TrimSpace(*minSizeRaw) == "" {
		minSize = defaultTUIMinSize
	}
	limit, err := parseOptionalPositiveInt(*limitRaw)
	if err != nil {
		return usageErr(err)
	}

	interactive := a.format == FormatText && a.canRunInteractiveTUI()
	var rt localRuntime
	if interactive {
		rt, err = a.openLocalRuntime(ctx)
	} else {
		rt, err = a.openLocalRuntimeReadOnly(ctx)
	}
	if err != nil {
		if !interactive && errors.Is(err, os.ErrNotExist) {
			cfg := config.Default()
			if cfgErr := cfg.Normalize(); cfgErr != nil {
				return cfgErr
			}
			if cfgErr := cfg.ApplyRuntimeEnv(); cfgErr != nil {
				return cfgErr
			}
			sort, sortErr := resolveTUISort(*sortMode, cfg)
			if sortErr != nil {
				return sortErr
			}
			layout, layoutErr := resolveTUILayout(*layoutMode, cfg)
			if layoutErr != nil {
				return layoutErr
			}
			return a.writeOutput("tui", emptyClusterBrowserPayload(ctx, cfg, cfg.DBPath, sort, layout, minSize, limit, *hideClosed), true)
		}
		return err
	}
	defer rt.Store.Close()

	repo, inferred, err := a.resolveOptionalRepository(ctx, rt, fs.Args())
	if err != nil {
		if !interactive && len(fs.Args()) == 0 && strings.Contains(err.Error(), "no local repositories found") {
			sort, sortErr := resolveTUISort(*sortMode, rt.Config)
			if sortErr != nil {
				return sortErr
			}
			layout, layoutErr := resolveTUILayout(*layoutMode, rt.Config)
			if layoutErr != nil {
				return layoutErr
			}
			return a.writeOutput("tui", emptyClusterBrowserPayload(ctx, rt.Config, rt.SourceDBPath, sort, layout, minSize, limit, *hideClosed), true)
		}
		return err
	}
	sort, err := resolveTUISort(*sortMode, rt.Config)
	if err != nil {
		return err
	}
	layout, err := resolveTUILayout(*layoutMode, rt.Config)
	if err != nil {
		return err
	}
	showClosed := !*hideClosed || *includeClosed

	clusters, err := rt.Store.ListDisplayClusterSummaries(ctx, store.ClusterSummaryOptions{
		RepoID:        repo.ID,
		IncludeClosed: showClosed,
		MinSize:       minSize,
		Limit:         limit,
		Sort:          sort,
	})
	if err != nil {
		return err
	}
	if interactive {
		workingSet, err := rt.Store.ListDisplayClusterSummaries(ctx, store.ClusterSummaryOptions{
			RepoID:        repo.ID,
			IncludeClosed: showClosed,
			MinSize:       1,
			Limit:         maxInt(defaultTUIWorkingSetLimit, limit),
			Sort:          sort,
		})
		if err != nil {
			return err
		}
		clusters = mergeClusterSummaries(clusters, workingSet)
	}
	if clusters == nil {
		clusters = []store.ClusterSummary{}
	}
	payload := clusterBrowserPayload{
		Repository:         repo.FullName,
		InferredRepository: inferred,
		Mode:               "cluster-browser",
		DBSource:           databaseSourceKind(ctx, rt.SourceDBPath),
		DBLocation:         databaseSourceLocation(ctx, rt.SourceDBPath),
		DBRefreshSource:    remoteRefreshSource(rt),
		DBRuntimePath:      remoteRuntimePath(rt),
		ConfigPath:         a.configPath,
		Sort:               sort,
		Layout:             layout,
		MinSize:            minSize,
		Limit:              limit,
		HideClosed:         !showClosed,
		EmbedModel:         rt.Config.OpenAI.EmbedModel,
		EmbeddingBasis:     rt.Config.EmbeddingBasis,
		VectorBackend:      rt.Config.VectorBackend,
		Clusters:           clusters,
	}
	if !interactive {
		if a.format == FormatText {
			return usageErr(fmt.Errorf("tui requires an interactive terminal; run it from a TTY or pass --json for machine-readable cluster data"))
		}
		return a.writeOutput("tui", payload, true)
	}
	return a.runInteractiveTUI(ctx, rt.Store, repo.ID, payload)
}

func resolveTUISort(raw string, cfg config.Config) (string, error) {
	sort := strings.TrimSpace(raw)
	if sort == "" {
		sort = strings.TrimSpace(cfg.TUI.DefaultSort)
	}
	if sort == "" {
		sort = "size"
	}
	if sort != "recent" && sort != "oldest" && sort != "size" {
		return "", usageErr(fmt.Errorf("unsupported sort %q", sort))
	}
	return sort, nil
}

func resolveTUILayout(raw string, cfg config.Config) (string, error) {
	layout := strings.TrimSpace(raw)
	if layout == "" {
		layout = strings.TrimSpace(cfg.TUI.DefaultLayout)
	}
	if !isSupportedTUILayout(layout) {
		return "", usageErr(fmt.Errorf("unsupported layout %q", layout))
	}
	return string(normalizeTUILayout(layout)), nil
}

func emptyClusterBrowserPayload(ctx context.Context, cfg config.Config, sourceDBPath, sort, layout string, minSize, limit int, hideClosed bool) clusterBrowserPayload {
	if strings.TrimSpace(sourceDBPath) == "" {
		sourceDBPath = cfg.DBPath
	}
	return clusterBrowserPayload{
		Mode:           "cluster-browser",
		DBSource:       databaseSourceKind(ctx, sourceDBPath),
		DBLocation:     databaseSourceLocation(ctx, sourceDBPath),
		Sort:           sort,
		Layout:         layout,
		MinSize:        minSize,
		Limit:          limit,
		HideClosed:     hideClosed,
		EmbedModel:     cfg.OpenAI.EmbedModel,
		EmbeddingBasis: cfg.EmbeddingBasis,
		VectorBackend:  cfg.VectorBackend,
		Clusters:       []store.ClusterSummary{},
	}
}

func databaseSourceKind(ctx context.Context, dbPath string) string {
	if _, ok, _ := portableStoreRoot(ctx, dbPath); ok {
		return "remote"
	}
	return "local"
}

func remoteRefreshSource(rt localRuntime) string {
	if rt.RemoteSource {
		return rt.SourceDBPath
	}
	return ""
}

func remoteRuntimePath(rt localRuntime) string {
	if rt.RemoteSource {
		return rt.Config.DBPath
	}
	return ""
}

func databaseSourceLocation(ctx context.Context, dbPath string) string {
	filename := filepath.Base(dbPath)
	root, ok, _ := portableStoreRoot(ctx, dbPath)
	if !ok {
		return filename
	}
	if repo := githubRepoFromRemote(gitRemoteURL(ctx, root)); repo != "" {
		return repo + ":" + filename
	}
	return filepath.Base(root) + ":" + filename
}

func gitRemoteURL(ctx context.Context, dir string) string {
	cmd := exec.CommandContext(ctx, "git", "-C", dir, "remote", "get-url", "origin")
	out, err := cmd.Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func githubRepoFromRemote(remote string) string {
	value := strings.TrimSuffix(strings.TrimSpace(remote), ".git")
	switch {
	case strings.HasPrefix(value, "git@github.com:"):
		value = strings.TrimPrefix(value, "git@github.com:")
	case strings.Contains(value, "github.com/"):
		idx := strings.Index(value, "github.com/")
		value = value[idx+len("github.com/"):]
	default:
		return ""
	}
	value = strings.Trim(value, "/")
	parts := strings.Split(value, "/")
	if len(parts) < 2 {
		return ""
	}
	return parts[len(parts)-2] + "/" + parts[len(parts)-1]
}

func (a *App) resolveOptionalRepository(ctx context.Context, rt localRuntime, args []string) (store.Repository, bool, error) {
	if len(args) == 0 {
		repo, err := rt.defaultRepository(ctx)
		if err != nil {
			return store.Repository{}, false, usageErr(fmt.Errorf("tui could not infer a repository: %w; run gitcrawl sync owner/repo or pass owner/repo explicitly", err))
		}
		return repo, true, nil
	}
	owner, repoName, err := parseOwnerRepo(args[0])
	if err != nil {
		return store.Repository{}, false, usageErr(err)
	}
	repo, err := rt.repository(ctx, owner, repoName)
	if err != nil {
		return store.Repository{}, false, err
	}
	return repo, false, nil
}
