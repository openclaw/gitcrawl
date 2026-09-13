package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	crawlremote "github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/gitcrawl/internal/config"
	"github.com/openclaw/gitcrawl/internal/store"
)

func (a *App) runDoctor(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("doctor", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	jsonOut := fs.Bool("json", false, "write JSON output")
	locks := fs.Bool("locks", false, "include SQLite lock and process diagnostics")
	if err := fs.Parse(normalizeCommandArgs(args, nil)); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	_ = ctx

	cfg, err := config.LoadRuntime(a.configPath)
	configExists := true
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		configExists = false
		cfg = config.Default()
		if err := cfg.Normalize(); err != nil {
			return err
		}
		if err := cfg.ApplyRuntimeEnv(); err != nil {
			return err
		}
	}
	if cfg.Remote.Enabled() && cfg.Remote.Mode == crawlremote.ModeCloud {
		return a.runRemoteDoctor(ctx, cfg, configExists)
	}
	if err := config.EnsureRuntimeDirs(cfg); err != nil {
		return err
	}
	storeStatus := store.Status{DBPath: cfg.DBPath}
	_, portableSource, portableProbeErr := portableStoreRoot(ctx, cfg.DBPath)
	if portableProbeErr != nil {
		return portableProbeErr
	}
	sourceHealth := sqliteDBHealth(ctx, cfg.DBPath, cfg.DBPath, portableSource)
	sourceSchema := store.InspectSchema(ctx, cfg.DBPath)
	if portableSource {
		sourceSchema = store.InspectPortableSourceSchema(ctx, cfg.DBPath)
	}
	dbSchema := sourceSchema
	runtimeHealth := map[string]any{}
	var runtimeSchema store.SchemaDiagnostics
	runtimeSchemaAvailable := false
	portableStoreStatus := map[string]any{}
	portableRefreshState := map[string]any{}
	repairAction := ""
	lockDBPath := cfg.DBPath
	runtimeOpenError := ""
	var runtimeOpenFailure error
	runtimeStatusError := ""
	var runtimeStatusFailure error
	// Doctor accepts a missing config file and applies runtime environment overrides above.
	// Reuse that resolved config so the runtime check inspects the same database we report.
	rt, err := a.openLocalRuntimeReadOnlyWithConfig(ctx, cfg)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			runtimeOpenError = err.Error()
			runtimeOpenFailure = err
		}
	} else {
		defer rt.Store.Close()
		lockDBPath = rt.Config.DBPath
		sourceHealth = sqliteDBHealth(ctx, rt.SourceDBPath, rt.SourceDBPath, rt.RemoteSource)
		if rt.RemoteSource {
			sourceSchema = store.InspectPortableSourceSchema(ctx, rt.SourceDBPath)
			runtimeHealth = sqliteDBHealth(ctx, rt.Config.DBPath, "", false)
			runtimeSchema = store.InspectSchema(ctx, rt.Config.DBPath)
			runtimeSchemaAvailable = true
			dbSchema = runtimeSchema
			portableStoreStatus = portableStoreGitStatus(ctx, rt.SourceDBPath)
			state := readPortableStoreRefreshState(portableStoreRefreshStatePath(rt.Config.DBPath))
			portableRefreshState = portableRefreshStatePayload(state)
			repairAction = state.LastRepair
			if repairAction == "" {
				repairAction = "none"
			}
		} else {
			sourceSchema = store.InspectSchema(ctx, rt.SourceDBPath)
			dbSchema = sourceSchema
		}
		storeStatus, err = rt.Store.Status(ctx)
		if err == nil && rt.RemoteSource {
			err = applyPortableExportTime(&storeStatus, rt.SourceDBPath, rt.Config.DBPath)
		}
		if err != nil {
			runtimeStatusError = err.Error()
			runtimeStatusFailure = err
		}
	}

	githubToken := a.resolveGitHubToken(ctx, cfg)
	openAIKey := config.ResolveOpenAIKey(cfg)
	runtimeIdentity := runtimeIdentityPayload()
	payload := map[string]any{
		"version":               version,
		"config_path":           config.ResolvePath(a.configPath),
		"config_exists":         configExists,
		"db_path":               cfg.DBPath,
		"source_db_health":      sourceHealth,
		"runtime_db_health":     runtimeHealth,
		"db_schema":             dbSchema,
		"source_db_schema":      sourceSchema,
		"portable_store_status": portableStoreStatus,
		"portable_refresh":      portableRefreshState,
		"repair_action":         repairAction,
		"runtime":               runtimeIdentity,
		"github_token_present":  githubToken.Value != "",
		"github_token_source":   githubToken.Source,
		"openai_key_present":    openAIKey.Value != "",
		"openai_key_source":     openAIKey.Source,
		"repository_count":      storeStatus.RepositoryCount,
		"thread_count":          storeStatus.ThreadCount,
		"open_thread_count":     storeStatus.OpenThreadCount,
		"cluster_count":         storeStatus.ClusterCount,
		"last_sync_at":          formatOptionalTime(storeStatus.LastSyncAt),
		"last_export_at":        formatOptionalTime(storeStatus.LastExportAt),
		"summary_model":         cfg.OpenAI.SummaryModel,
		"embed_model":           cfg.OpenAI.EmbedModel,
		"embed_base_url":        embedBaseURL(cfg),
		"embedding_basis":       cfg.EmbeddingBasis,
		"api_supported":         false,
	}
	if executablePath, ok := runtimeIdentity["executable_path"]; ok {
		payload["executable_path"] = executablePath
	}
	if runtimeSchemaAvailable {
		payload["runtime_db_schema"] = runtimeSchema
	}
	if runtimeOpenError != "" {
		payload["runtime_open_error"] = runtimeOpenError
	}
	if runtimeStatusError != "" {
		payload["runtime_status_error"] = runtimeStatusError
	}
	if *locks {
		payload["locks"] = sqliteLockDiagnostic(ctx, lockDBPath)
	}
	if err := a.writeOutput("doctor", payload, true); err != nil {
		return err
	}
	if runtimeOpenFailure != nil {
		return runtimeOpenFailure
	}
	if runtimeStatusFailure != nil {
		return runtimeStatusFailure
	}
	return nil
}

func portableRefreshStatePayload(state portableStoreRefreshState) map[string]any {
	out := map[string]any{}
	if state.LastAttempt != "" {
		out["last_attempt"] = state.LastAttempt
	}
	if state.LastSuccess != "" {
		out["last_success"] = state.LastSuccess
	}
	if state.LastFailure != "" {
		out["last_failure"] = state.LastFailure
	}
	if state.Error != "" {
		out["error"] = state.Error
	}
	if state.LastRepair != "" {
		out["last_repair"] = state.LastRepair
	}
	if state.LastRepairAt != "" {
		out["last_repair_at"] = state.LastRepairAt
	}
	if state.LastRepairBackup != "" {
		out["last_repair_backup"] = state.LastRepairBackup
	}
	if state.LastRepairError != "" {
		out["last_repair_error"] = state.LastRepairError
	}
	return out
}

func (a *App) runRemoteDoctor(ctx context.Context, cfg config.Config, configExists bool) error {
	remoteToken := config.ResolveRemoteToken(cfg)
	remoteStatus := map[string]any{
		"endpoint": cfg.Remote.Endpoint,
		"archive":  cfg.Remote.Archive,
		"mode":     cfg.Remote.Mode,
		"health":   "unchecked",
	}
	if strings.TrimSpace(cfg.Remote.Endpoint) == "" {
		remoteStatus["health"] = "missing_endpoint"
	} else if strings.TrimSpace(cfg.Remote.Archive) == "" {
		remoteStatus["health"] = "missing_archive"
	} else if remoteToken.Value == "" {
		remoteStatus["health"] = "missing_token"
	} else {
		client, err := a.remoteClient(cfg)
		if err != nil {
			remoteStatus["health"] = "error"
			remoteStatus["error"] = err.Error()
		} else if status, err := client.Status(ctx, "gitcrawl", cfg.Remote.Archive); err != nil {
			remoteStatus["health"] = "error"
			remoteStatus["error"] = err.Error()
		} else {
			remoteStatus["health"] = "ok"
			remoteStatus["last_sync_at"] = status.LastSyncAt
			remoteStatus["last_ingest_at"] = status.LastIngestAt
			remoteStatus["counts"] = status.Counts
		}
	}
	openAIKey := config.ResolveOpenAIKey(cfg)
	runtimeIdentity := runtimeIdentityPayload()
	payload := map[string]any{
		"version":              version,
		"config_path":          config.ResolvePath(a.configPath),
		"config_exists":        configExists,
		"remote":               remoteStatus,
		"runtime":              runtimeIdentity,
		"remote_token_present": remoteToken.Value != "",
		"remote_token_source":  remoteToken.Source,
		"openai_key_present":   openAIKey.Value != "",
		"openai_key_source":    openAIKey.Source,
		"summary_model":        cfg.OpenAI.SummaryModel,
		"embed_model":          cfg.OpenAI.EmbedModel,
		"embed_base_url":       embedBaseURL(cfg),
		"embedding_basis":      cfg.EmbeddingBasis,
		"api_supported":        false,
	}
	if executablePath, ok := runtimeIdentity["executable_path"]; ok {
		payload["executable_path"] = executablePath
	}
	return a.writeOutput("doctor", payload, true)
}

func sqliteDBHealth(ctx context.Context, dbPath, manifestDBPath string, immutable bool) map[string]any {
	result := map[string]any{
		"path":   dbPath,
		"exists": false,
		"health": "missing",
	}
	if strings.TrimSpace(dbPath) == "" {
		return result
	}
	info, err := os.Stat(dbPath)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			result["health"] = "error"
			result["error"] = err.Error()
		}
		return result
	}
	result["exists"] = true
	result["size"] = info.Size()
	if sum, err := fileSHA256(dbPath); err == nil {
		result["sha256"] = fmt.Sprintf("%x", sum)
	}
	if strings.TrimSpace(manifestDBPath) == "" {
		if err := sqliteStoreHealth(ctx, dbPath); err != nil {
			result["health"] = "error"
			result["error"] = err.Error()
		} else {
			result["health"] = "ok"
		}
		return result
	}
	manifestPath := portableDBManifestPath(manifestDBPath)
	result["manifest_path"] = manifestPath
	if manifest, ok, err := readPortableDBManifest(manifestPath); err != nil {
		result["manifest"] = "error"
		result["manifest_error"] = err.Error()
	} else if ok {
		result["manifest"] = "present"
		if manifest.OutputBytes > 0 {
			result["manifest_bytes"] = manifest.OutputBytes
		}
		if manifest.SHA256 != "" {
			result["manifest_sha256"] = manifest.SHA256
		}
	} else {
		result["manifest"] = "missing"
	}
	validate := validatePortableSQLiteFile
	if immutable {
		validate = validatePortableSQLiteSourceFile
	}
	if err := validate(ctx, dbPath, manifestDBPath); err != nil {
		result["health"] = "error"
		result["error"] = err.Error()
	} else {
		result["health"] = "ok"
	}
	return result
}

func portableStoreGitStatus(ctx context.Context, dbPath string) map[string]any {
	result := map[string]any{}
	root, ok, err := portableStoreRoot(ctx, dbPath)
	if err != nil {
		result["state"] = "error"
		result["error"] = err.Error()
		return result
	}
	if !ok {
		result["state"] = "not_portable"
		return result
	}
	result["root"] = root
	if gitWorktreeClean(ctx, root) {
		result["state"] = "clean"
	} else {
		result["state"] = "dirty"
	}
	if counts, err := gitOutput(ctx, "", "-C", root, "rev-list", "--left-right", "--count", "HEAD...@{upstream}"); err == nil {
		parts := strings.Fields(counts)
		if len(parts) == 2 {
			result["ahead"] = parts[0]
			result["behind"] = parts[1]
		}
	}
	return result
}
