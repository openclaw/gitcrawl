package cli

import (
	"context"
	"flag"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	crawlconfig "github.com/openclaw/crawlkit/config"
	crawlremote "github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/gitcrawl/internal/config"
)

func (a *App) runInit(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("init", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", "", "database path")
	runtimeDir := fs.String("runtime-dir", "", "root for an isolated database, cache, vectors, and logs")
	portableStore := fs.String("portable-store", "", "HTTPS git URL for a portable gitcrawl store")
	portableDB := fs.String("portable-db", "data/openclaw__openclaw.sync.db", "database path inside portable store")
	storeDir := fs.String("store-dir", "", "local portable store checkout directory")
	remoteEndpoint := fs.String("remote", "", "remote archive endpoint")
	remoteArchive := fs.String("archive", "", "remote archive id")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"db": true, "runtime-dir": true, "portable-store": true, "portable-db": true, "store-dir": true, "remote": true, "archive": true})); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 0 {
		return usageErr(fmt.Errorf("init does not take positional arguments"))
	}
	localDBPath := strings.TrimSpace(*dbPath)
	isolatedRuntimeDir := strings.TrimSpace(*runtimeDir)
	portableStoreURL := strings.TrimSpace(*portableStore)
	remoteEndpointValue := strings.TrimSpace(*remoteEndpoint)
	if nonEmptyCount(localDBPath, isolatedRuntimeDir, portableStoreURL, remoteEndpointValue) > 1 {
		return usageErr(fmt.Errorf("use only one of --db, --runtime-dir, --portable-store, or --remote"))
	}
	if localDBPath == ":memory:" || strings.HasPrefix(localDBPath, "file:") {
		return usageErr(fmt.Errorf("--db requires a filesystem path; SQLite URIs and :memory: are unsupported"))
	}
	if localDBPath != "" {
		var err error
		localDBPath, err = a.absoluteInitPath(localDBPath)
		if err != nil {
			return fmt.Errorf("resolve --db path: %w", err)
		}
	}
	var err error
	isolatedRuntimeDir, err = a.absoluteInitPath(isolatedRuntimeDir)
	if err != nil {
		return fmt.Errorf("resolve --runtime-dir path: %w", err)
	}

	cfg := config.Default()
	portableStoreDir := ""
	portableStoreAction := ""
	if remoteEndpointValue != "" {
		archive := strings.TrimSpace(*remoteArchive)
		if archive == "" {
			return usageErr(fmt.Errorf("--remote requires --archive"))
		}
		cfg.Remote = crawlremote.Config{
			Mode:     crawlremote.ModeCloud,
			Endpoint: remoteEndpointValue,
			Archive:  archive,
			TokenEnv: crawlremote.DefaultTokenEnv,
		}
	} else if portableStoreURL != "" {
		if err := validatePortableRemote(portableStoreURL); err != nil {
			return usageErr(err)
		}
		if err := validatePortableRelativePath(*portableDB); err != nil {
			return usageErr(err)
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, portableOperationTimeout)
		defer cancel()
		portableStoreDir = strings.TrimSpace(*storeDir)
		if portableStoreDir == "" {
			portableStoreDir = defaultPortableStoreDir(config.ResolvePath(a.configPath), portableStoreURL)
		}
		portableStoreDir, err = a.absoluteInitPath(portableStoreDir)
		if err != nil {
			return fmt.Errorf("resolve --store-dir path: %w", err)
		}
		var release func()
		ctx, release, err = acquirePortableOwner(ctx, portableStoreDir)
		if err != nil {
			return err
		}
		defer release()
		action, err := syncPortableStore(ctx, portableStoreURL, portableStoreDir)
		if err != nil {
			return err
		}
		portableStoreAction = action
		cfg.DBPath = filepath.Join(portableStoreDir, filepath.FromSlash(*portableDB))
		if err := validatePortableSQLiteSourceFile(ctx, cfg.DBPath, cfg.DBPath); err != nil {
			return fmt.Errorf("validate portable database: %w", err)
		}
	}
	if localDBPath != "" {
		cfg.DBPath = localDBPath
	}
	if isolatedRuntimeDir != "" {
		cfg.DBPath = filepath.Join(isolatedRuntimeDir, "gitcrawl.db")
		cfg.CacheDir = filepath.Join(isolatedRuntimeDir, "cache")
		cfg.VectorDir = filepath.Join(isolatedRuntimeDir, "vectors")
		cfg.LogDir = filepath.Join(isolatedRuntimeDir, "logs")
	}
	if err := cfg.Normalize(); err != nil {
		return err
	}
	if err := config.Save(a.configPath, cfg); err != nil {
		return err
	}
	if !(cfg.Remote.Enabled() && cfg.Remote.Mode == crawlremote.ModeCloud) {
		if err := config.EnsureRuntimeDirs(cfg); err != nil {
			return err
		}
	}
	result := initResult{
		ConfigPath:       config.ResolvePath(a.configPath),
		RuntimeDir:       isolatedRuntimeDir,
		DBPath:           cfg.DBPath,
		CacheDir:         cfg.CacheDir,
		VectorDir:        cfg.VectorDir,
		LogDir:           cfg.LogDir,
		PortableStoreURL: portableStoreURL,
		PortableStoreDir: portableStoreDir,
		PortableStore:    portableStoreAction,
	}
	if cfg.Remote.Enabled() && cfg.Remote.Mode == crawlremote.ModeCloud {
		result.RemoteMode = cfg.Remote.Mode
		result.RemoteEndpoint = cfg.Remote.Endpoint
		result.RemoteArchive = cfg.Remote.Archive
	}
	return a.writeInitOutput(result)
}

func (a *App) absoluteInitPath(path string) (string, error) {
	originalPath := strings.TrimSpace(path)
	path = crawlconfig.ExpandHome(path)
	if (originalPath == "~" || strings.HasPrefix(originalPath, "~/")) && path == originalPath {
		return "", fmt.Errorf("expand home-relative path %q: home directory unavailable", originalPath)
	}
	if path == "" || filepath.IsAbs(path) {
		return path, nil
	}
	workingDir, err := a.getWorkingDirectory()
	if err != nil {
		return "", err
	}
	return filepath.Join(workingDir, path), nil
}

type initResult struct {
	ConfigPath       string `json:"config_path"`
	RuntimeDir       string `json:"runtime_dir,omitempty"`
	DBPath           string `json:"db_path"`
	CacheDir         string `json:"cache_dir"`
	VectorDir        string `json:"vector_dir"`
	LogDir           string `json:"log_dir"`
	PortableStoreURL string `json:"portable_store_url,omitempty"`
	PortableStoreDir string `json:"portable_store_dir,omitempty"`
	PortableStore    string `json:"portable_store,omitempty"`
	RemoteMode       string `json:"remote_mode,omitempty"`
	RemoteEndpoint   string `json:"remote_endpoint,omitempty"`
	RemoteArchive    string `json:"remote_archive,omitempty"`
}
