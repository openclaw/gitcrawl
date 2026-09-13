package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/openclaw/crawlkit/control"
	crawlremote "github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/gitcrawl/internal/config"
	"github.com/openclaw/gitcrawl/internal/store"
)

func (a *App) runMetadata(args []string) error {
	fs := flag.NewFlagSet("metadata", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, nil)); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 0 {
		return usageErr(fmt.Errorf("metadata takes flags only"))
	}
	cfg := config.Default()
	manifest := control.NewManifest("gitcrawl", "Git Crawl", "gitcrawl")
	manifest.Description = "Local-first GitHub issue and pull request crawler."
	manifest.Branding = control.Branding{SymbolName: "point.3.connected.trianglepath.dotted", AccentColor: "#2da44e"}
	manifest.Paths = control.Paths{
		DefaultConfig:   config.ResolvePath(""),
		ConfigEnv:       config.DefaultConfigEnv,
		DefaultDatabase: cfg.DBPath,
		DefaultCache:    cfg.CacheDir,
		DefaultLogs:     cfg.LogDir,
	}
	manifest.Capabilities = []string{"metadata", "status", "doctor", "sync", "capture", "coverage", "search", "code-index", "tui", "portable", "remote", "cloud-publish", "clusters", "summaries", "embeddings"}
	manifest.Privacy = control.Privacy{ContainsPrivateMessages: true, ExportsSecrets: false, LocalOnlyScopes: []string{"github", "git", "sqlite", "portable"}}
	manifest.Commands = map[string]control.Command{
		"status":           {Title: "Status", Argv: []string{"gitcrawl", "status", "--json"}, JSON: true},
		"remote-status":    {Title: "Remote archive status", Argv: []string{"gitcrawl", "remote", "status", "--json"}, JSON: true},
		"remote-archives":  {Title: "Remote archive list", Argv: []string{"gitcrawl", "remote", "archives", "--json"}, JSON: true},
		"remote-login":     {Title: "Remote GitHub login", Argv: []string{"gitcrawl", "remote", "login", "--json"}, JSON: true, Mutates: true},
		"cloud-publish":    {Title: "Publish cloud archive", Argv: []string{"gitcrawl", "cloud", "publish", "--json"}, JSON: true, Mutates: true},
		"whoami":           {Title: "Remote identity", Argv: []string{"gitcrawl", "whoami", "--json"}, JSON: true},
		"check-update":     {Title: "Check for updates", Argv: []string{"gitcrawl", "check-update", "--json"}, JSON: true},
		"doctor":           {Title: "Doctor", Argv: []string{"gitcrawl", "doctor", "--json"}, JSON: true},
		"coverage":         {Title: "Archive coverage", Argv: []string{"gitcrawl", "coverage", "--json"}, JSON: true},
		"sync":             {Title: "Sync repository", Argv: []string{"gitcrawl", "sync", "--json"}, JSON: true, Mutates: true},
		"capture":          {Title: "Export conversation capture", Argv: []string{"gitcrawl", "capture", "--json"}, JSON: true},
		"search":           {Title: "Search", Argv: []string{"gitcrawl", "search", "--json"}, JSON: true},
		"code-index":       {Title: "Code index", Argv: []string{"gitcrawl", "code", "index", "--json"}, JSON: true, Mutates: true},
		"tui":              {Title: "Terminal cluster browser", Argv: []string{"gitcrawl", "tui"}},
		"tui-json":         {Title: "Terminal cluster data", Argv: []string{"gitcrawl", "tui", "--json"}, JSON: true},
		"portable":         {Title: "Portable store tools", Argv: []string{"gitcrawl", "portable", "prune", "--json"}, JSON: true, Mutates: true},
		"portable-refresh": {Title: "Refresh portable subscriber", Argv: []string{"gitcrawl", "portable", "refresh", "--expected-remote", "URL", "--json"}, JSON: true, Mutates: true},
		"clusters":         {Title: "Clusters", Argv: []string{"gitcrawl", "clusters", "--json"}, JSON: true},
		"legacy-sync-api":  {Title: "Legacy sync-status alias", Argv: []string{"gitcrawl", "sync-status"}, Legacy: true, Deprecated: true},
	}
	return a.writeOutput("metadata", manifest, false)
}

func (a *App) runStatus(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, nil)); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 0 {
		return usageErr(fmt.Errorf("status takes flags only"))
	}
	cfg, err := config.LoadRuntime(a.configPath)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		cfg = config.Default()
		if err := cfg.Normalize(); err != nil {
			return err
		}
		if err := cfg.ApplyRuntimeEnv(); err != nil {
			return err
		}
	}
	if cfg.Remote.Enabled() && cfg.Remote.Mode == crawlremote.ModeCloud {
		return a.runRemoteStatusWithConfig(ctx, cfg)
	}
	status, err := a.localArchiveStatus(ctx, cfg)
	if err != nil {
		return err
	}
	return a.writeOutput("status", status, false)
}

func (a *App) runRemoteStatusWithConfig(ctx context.Context, cfg config.Config) error {
	client, err := a.remoteClient(cfg)
	if err != nil {
		return err
	}
	status, err := client.Status(ctx, "gitcrawl", cfg.Remote.Archive)
	if err != nil {
		return err
	}
	return a.writeOutput("status", remoteControlStatus(config.ResolvePath(a.configPath), cfg, status), false)
}

func controlStatus(configPath string, cfg config.Config, status store.Status) control.Status {
	counts := []control.Count{
		control.NewCount("repositories", "Repositories", int64(status.RepositoryCount)),
		control.NewCount("threads", "Threads", int64(status.ThreadCount)),
		control.NewCount("open_threads", "Open threads", int64(status.OpenThreadCount)),
		control.NewCount("clusters", "Clusters", int64(status.ClusterCount)),
	}
	out := control.NewStatus("gitcrawl", fmt.Sprintf("%d threads across %d repositories", status.ThreadCount, status.RepositoryCount))
	out.State = "current"
	out.ConfigPath = configPath
	out.DatabasePath = status.DBPath
	out.Counts = counts
	if !status.LastSyncAt.IsZero() {
		out.LastSyncAt = status.LastSyncAt.UTC().Format(time.RFC3339)
	}
	db := control.SQLiteDatabase("primary", "GitHub archive", "archive", status.DBPath, true, counts)
	out.DatabaseBytes = db.Bytes
	out.WALBytes = fileSize(status.DBPath + "-wal")
	out.Databases = []control.Database{db}
	return out
}

func remoteControlStatus(configPath string, cfg config.Config, status crawlremote.Status) control.Status {
	counts := append([]control.Count(nil), status.Counts...)
	summary := fmt.Sprintf("remote archive %s", firstNonEmpty(status.Archive, cfg.Remote.Archive))
	threadCount := countValue(counts, "threads")
	repoCount := countValue(counts, "repositories")
	if threadCount > 0 || repoCount > 0 {
		summary = fmt.Sprintf("%d threads across %d repositories", threadCount, repoCount)
	}
	out := control.NewStatus("gitcrawl", summary)
	out.State = "current"
	out.ConfigPath = configPath
	out.Counts = counts
	out.LastSyncAt = firstNonEmpty(status.LastSyncAt, status.LastIngestAt)
	out.Remote = &control.Remote{
		Enabled:      true,
		Mode:         firstNonEmpty(status.Mode, cfg.Remote.Mode),
		Endpoint:     cfg.Remote.Endpoint,
		Archive:      firstNonEmpty(status.Archive, cfg.Remote.Archive),
		LastIngestAt: status.LastIngestAt,
		LastSyncAt:   status.LastSyncAt,
	}
	if len(status.Warnings) > 0 {
		out.Warnings = append(out.Warnings, status.Warnings...)
	}
	out.Databases = []control.Database{
		control.RemoteDatabase("primary", "GitHub archive", "archive", "d1", cfg.Remote.Endpoint, firstNonEmpty(status.Archive, cfg.Remote.Archive), true, counts),
	}
	return out
}

func fileSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}
