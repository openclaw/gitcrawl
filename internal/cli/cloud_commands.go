package cli

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	crawlremote "github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/gitcrawl/internal/config"
)

const (
	gitcrawlCloudBatchSize             = 250
	gitcrawlCloudSQLiteBundleChunkSize = int64(64 * 1024 * 1024)

	gitcrawlSnapshotAtomicCapability     = "gitcrawl.snapshot.atomic"
	gitcrawlSnapshotCutoverCapability    = "gitcrawl.snapshot.cutover"
	gitcrawlSnapshotProvenanceCapability = "gitcrawl.snapshot.provenance.v1"
	sqliteBundleGzipUploadCapability     = "sqlite.bundle.gzip.upload"
)

var gitcrawlCloudCoverageColumns = []string{
	"dataset", "row_count", "eligible_count", "covered_count",
	"max_source_at", "dataset_generated_at", "complete", "mutation_token",
}

func gitcrawlCloudReaderQuerySpecs() []crawlremote.QuerySpec {
	return []crawlremote.QuerySpec{
		{Name: "gitcrawl.threads.search", Args: []string{"owner", "repo", "query", "kind", "state", "mode", "limit"}},
		{Name: "gitcrawl.clusters.related", Args: []string{"owner", "repo", "number"}},
		{Name: "gitcrawl.clusters.list", Args: []string{"owner", "repo", "status", "min_size"}},
		{Name: "gitcrawl.clusters.members", Args: []string{"owner", "repo", "cluster_id"}},
		{Name: "gitcrawl.pull_requests.review_context", Args: []string{"owner", "repo", "number"}},
		{Name: "gitcrawl.coverage", Args: []string{"dataset"}},
	}
}

func (a *App) runCloud(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return usageErr(fmt.Errorf("cloud requires a subcommand"))
	}
	switch args[0] {
	case "publish":
		return a.runCloudPublish(ctx, args[1:])
	default:
		return usageErr(fmt.Errorf("unknown cloud subcommand %q", args[0]))
	}
}

func (a *App) runCloudPublish(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("cloud publish", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	remoteEndpoint := fs.String("remote", "", "remote archive endpoint")
	archive := fs.String("archive", "", "remote archive id")
	tokenEnv := fs.String("token-env", "", "remote token environment variable")
	allowIncomplete := fs.Bool("allow-incomplete", false, "publish even when local enrichment coverage is incomplete")
	observationOrder := fs.Bool("observation-order", false, "publish durable observation ordering when the remote fence is enabled")
	stageOnly := fs.Bool("stage-only", false, "stage the immutable snapshot without moving unpinned reads")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"remote": true, "archive": true, "token-env": true})); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 0 {
		return usageErr(fmt.Errorf("cloud publish takes flags only"))
	}
	cutover := !*stageOnly

	cfg, err := config.LoadRuntime(a.configPath)
	if err != nil {
		return err
	}
	endpoint := firstNonEmpty(*remoteEndpoint, cfg.Remote.Endpoint)
	archiveID := firstNonEmpty(*archive, cfg.Remote.Archive)
	if endpoint == "" {
		return usageErr(fmt.Errorf("cloud publish requires --remote or remote.endpoint"))
	}
	if archiveID == "" {
		return usageErr(fmt.Errorf("cloud publish requires --archive or remote.archive"))
	}

	rt, err := a.openLocalRuntimeReadOnly(ctx)
	if err != nil {
		return err
	}
	defer rt.Store.Close()

	remoteCfg := crawlremote.Config{
		Mode:     crawlremote.ModePublisher,
		Endpoint: endpoint,
		Archive:  archiveID,
		TokenEnv: firstNonEmpty(*tokenEnv, cfg.Remote.TokenEnv, crawlremote.DefaultTokenEnv),
	}
	client, err := crawlremote.NewClientFromConfig(remoteCfg, crawlremote.Options{
		UserAgent:  "gitcrawl/" + version,
		HTTPClient: &http.Client{Timeout: 10 * time.Minute},
	})
	if err != nil {
		return err
	}
	snapshotPath, cleanupSnapshot, err := cloudSQLiteSnapshotPath(ctx, rt.Store.DB(), rt.Store.Path())
	if err != nil {
		return err
	}
	defer cleanupSnapshot()
	snapshotDB, err := sql.Open("sqlite", snapshotPath)
	if err != nil {
		return fmt.Errorf("open frozen cloud snapshot: %w", err)
	}
	defer snapshotDB.Close()
	snapshot, err := buildGitcrawlCloudSnapshot(
		ctx,
		snapshotDB,
		snapshotPath,
		*allowIncomplete,
		*observationOrder,
	)
	if err != nil {
		return err
	}
	manifest := gitcrawlCloudManifest(archiveID, snapshot)
	counts := gitcrawlCloudDatasetCounts(snapshot)
	if err := requireGitcrawlSnapshotPublishContract(
		ctx,
		client,
		snapshot,
		cutover,
	); err != nil {
		return err
	}
	sqliteBundle, err := uploadSQLiteSnapshotArchive(
		ctx,
		client,
		"gitcrawl",
		archiveID,
		snapshotPath,
		counts,
	)
	if err != nil {
		return err
	}

	alreadyStaged := false
	status, statusErr := client.PublishStatus(ctx, "gitcrawl", archiveID)
	if statusErr == nil {
		alreadyStaged = gitcrawlPublisherStatusMatches(status, manifest)
	} else if !remoteNotFound(statusErr) {
		return statusErr
	}
	var mutationToken string
	if !alreadyStaged {
		for _, dataset := range snapshot.Datasets {
			progress, err := sendSnapshotIngestRows(
				ctx,
				client,
				"gitcrawl",
				archiveID,
				manifest,
				dataset.Name,
				dataset.Columns,
				dataset.Rows,
				mutationToken,
				false,
			)
			if err != nil {
				return fmt.Errorf("publish cloud dataset %s: %w", dataset.Name, err)
			}
			counts[dataset.Name] = progress.RowsAccepted
			mutationToken = progress.MutationToken
		}
		coverageRows := gitcrawlCloudCoverageRows(snapshot, mutationToken)
		progress, err := sendSnapshotIngestRows(
			ctx,
			client,
			"gitcrawl",
			archiveID,
			manifest,
			"dataset_coverage",
			gitcrawlCloudCoverageColumns,
			coverageRows,
			mutationToken,
			true,
		)
		if err != nil {
			return fmt.Errorf("activate cloud snapshot: %w", err)
		}
		mutationToken = progress.MutationToken
	}
	var cutoverResult *crawlremote.CutoverResult
	if cutover {
		result, err := client.Cutover(ctx, "gitcrawl", archiveID, snapshot.ID)
		if err != nil {
			return fmt.Errorf("cut over cloud snapshot: %w", err)
		}
		cutoverResult = &result
	}
	sqliteBundlePrivacy := gitcrawlCloudSQLiteBundlePrivacy()
	return a.writeOutput("cloud publish", map[string]any{
		"remote":                strings.TrimRight(endpoint, "/"),
		"archive":               archiveID,
		"snapshot_id":           snapshot.ID,
		"source_sha256":         snapshot.ID,
		"source_sync_at":        snapshot.SourceSyncAt,
		"dataset_generated_at":  snapshot.DatasetGeneratedAt,
		"capabilities":          snapshot.Capabilities,
		"datasets":              counts,
		"hydration":             snapshot.Hydration,
		"already_staged":        alreadyStaged,
		"mutation_token":        mutationToken,
		"cutover":               cutoverResult,
		"sqlite_bundle":         sqliteBundle,
		"sqlite_bundle_privacy": sqliteBundlePrivacy,
	}, true)
}

func publishRows(ctx context.Context, db *sql.DB, query string, mapRow func([]any) []any) ([][]any, error) {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	out := make([][]any, 0)
	for rows.Next() {
		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		for i, value := range values {
			if bytes, ok := value.([]byte); ok {
				values[i] = string(bytes)
			}
		}
		out = append(out, mapRow(values))
	}
	return out, rows.Err()
}

type ingestProgress struct {
	RowsAccepted  int64
	MutationToken string
}

func sendIngestRows(
	ctx context.Context,
	client *crawlremote.Client,
	app, archive string,
	manifest crawlremote.IngestManifest,
	table string,
	columns []string,
	rows [][]any,
	final bool,
) (int64, error) {
	progress, err := sendSnapshotIngestRows(
		ctx,
		client,
		app,
		archive,
		manifest,
		table,
		columns,
		rows,
		"",
		final,
	)
	return progress.RowsAccepted, err
}

func sendSnapshotIngestRows(
	ctx context.Context,
	client *crawlremote.Client,
	app, archive string,
	manifest crawlremote.IngestManifest,
	table string,
	columns []string,
	rows [][]any,
	mutationToken string,
	final bool,
) (ingestProgress, error) {
	var total int64
	if len(rows) == 0 {
		result, err := sendIngestBatch(
			ctx,
			client,
			app,
			archive,
			manifest,
			table,
			columns,
			[][]any{},
			0,
			mutationToken,
			final,
		)
		return ingestProgress{RowsAccepted: result.RowsAccepted, MutationToken: result.MutationToken}, err
	}
	for start := 0; start < len(rows); start += gitcrawlCloudBatchSize {
		end := start + gitcrawlCloudBatchSize
		if end > len(rows) {
			end = len(rows)
		}
		result, err := sendIngestBatch(
			ctx,
			client,
			app,
			archive,
			manifest,
			table,
			columns,
			rows[start:end],
			start,
			mutationToken,
			final && end == len(rows),
		)
		if err != nil {
			return ingestProgress{RowsAccepted: total, MutationToken: mutationToken}, err
		}
		total += result.RowsAccepted
		mutationToken = result.MutationToken
	}
	return ingestProgress{RowsAccepted: total, MutationToken: mutationToken}, nil
}

func sendIngestBatch(
	ctx context.Context,
	client *crawlremote.Client,
	app, archive string,
	manifest crawlremote.IngestManifest,
	table string,
	columns []string,
	rows [][]any,
	cursor int,
	mutationToken string,
	final bool,
) (crawlremote.IngestResult, error) {
	for {
		result, err := client.Ingest(ctx, app, archive, crawlremote.IngestRequest{
			Manifest:      manifest,
			Table:         table,
			Columns:       columns,
			Rows:          rows,
			Cursor:        cursorFor(cursor),
			MutationToken: mutationToken,
			Final:         final,
		})
		if err == nil {
			if result.ResetIncomplete {
				if err := drainIngestReset(
					ctx,
					client,
					app,
					archive,
					manifest,
					table,
					columns,
					mutationToken,
				); err != nil {
					return crawlremote.IngestResult{}, err
				}
				continue
			}
			return result, nil
		}
		if !isResetIncomplete(err) {
			return crawlremote.IngestResult{}, err
		}
		if err := drainIngestReset(
			ctx,
			client,
			app,
			archive,
			manifest,
			table,
			columns,
			mutationToken,
		); err != nil {
			return crawlremote.IngestResult{}, err
		}
	}
}

func drainIngestReset(
	ctx context.Context,
	client *crawlremote.Client,
	app, archive string,
	manifest crawlremote.IngestManifest,
	table string,
	columns []string,
	mutationToken string,
) error {
	for {
		result, err := client.Ingest(ctx, app, archive, crawlremote.IngestRequest{
			Manifest:      manifest,
			Table:         table,
			Columns:       columns,
			Rows:          [][]any{},
			MutationToken: mutationToken,
		})
		if err != nil {
			return err
		}
		if !result.ResetIncomplete {
			return nil
		}
	}
}

func isResetIncomplete(err error) bool {
	var remoteErr *crawlremote.Error
	return errors.As(err, &remoteErr) && remoteErr.Code == "reset_incomplete"
}

func cursorFor(start int) string {
	if start == 0 {
		return ""
	}
	return fmt.Sprintf("%d", start)
}

func uploadSQLiteArchive(ctx context.Context, client *crawlremote.Client, app, archive string, db *sql.DB, dbPath string, manifest crawlremote.IngestManifest, counts map[string]int64) (*crawlremote.SQLiteBundle, error) {
	snapshotPath, cleanup, err := cloudSQLiteSnapshotPath(ctx, db, dbPath)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	return uploadSQLiteSnapshotArchive(ctx, client, app, archive, snapshotPath, counts)
}

func uploadSQLiteSnapshotArchive(
	ctx context.Context,
	client *crawlremote.Client,
	app, archive, snapshotPath string,
	counts map[string]int64,
) (*crawlremote.SQLiteBundle, error) {
	bundle, err := crawlremote.BuildSnapshotGzipSQLiteBundle(ctx, crawlremote.SQLiteBundleBuildOptions{
		App:        app,
		Archive:    archive,
		SourcePath: snapshotPath,
		ChunkSize:  gitcrawlCloudSQLiteBundleChunkSize,
		Counts:     counts,
		Privacy:    gitcrawlCloudSQLiteBundlePrivacy(),
	})
	if err != nil {
		return nil, err
	}
	defer bundle.Cleanup()
	result, err := client.UploadSQLiteBundleFiles(ctx, app, archive, bundle.Manifest, bundle.Parts)
	if err != nil {
		return nil, err
	}
	return result.Bundle, nil
}

func remoteNotFound(err error) bool {
	var remoteErr *crawlremote.Error
	return errors.As(err, &remoteErr) && remoteErr.Status == http.StatusNotFound
}

func requireGitcrawlSnapshotPublishContract(
	ctx context.Context,
	client *crawlremote.Client,
	snapshot gitcrawlCloudSnapshot,
	cutover bool,
) error {
	contract, err := client.Contract(ctx)
	if err != nil {
		return fmt.Errorf("read remote snapshot publish contract: %w", err)
	}
	if err := contract.Validate(); err != nil {
		return fmt.Errorf("validate remote snapshot publish contract: %w", err)
	}
	var appSpec *crawlremote.AppSpec
	for index := range contract.Apps {
		app := &contract.Apps[index]
		if app.App == "gitcrawl" {
			appSpec = app
			break
		}
	}
	if appSpec == nil {
		return fmt.Errorf("remote contract does not advertise the gitcrawl app")
	}
	requiredCapabilities := []string{
		gitcrawlSnapshotAtomicCapability,
		gitcrawlSnapshotProvenanceCapability,
		sqliteBundleGzipUploadCapability,
	}
	requiredCapabilities = append(requiredCapabilities, snapshot.Capabilities...)
	if cutover {
		requiredCapabilities = append(
			requiredCapabilities,
			gitcrawlSnapshotCutoverCapability,
		)
	}
	for _, capability := range requiredCapabilities {
		if !slices.Contains(appSpec.Capabilities, capability) {
			return fmt.Errorf(
				"remote does not advertise required snapshot publish capability %s",
				capability,
			)
		}
	}
	requiredRoutes := []crawlremote.RouteSpec{
		{
			Method: http.MethodGet,
			Path:   "/v1/apps/:app/archives/:archive/publish-status",
			Auth:   crawlremote.AuthPublisher,
		},
		{
			Method: http.MethodPost,
			Path:   "/v1/apps/:app/archives/:archive/query",
			Auth:   crawlremote.AuthReader,
		},
		{
			Method: http.MethodPost,
			Path:   "/v1/apps/:app/archives/:archive/ingest",
			Auth:   crawlremote.AuthPublisher,
		},
		{
			Method: http.MethodPut,
			Path:   "/v1/apps/:app/archives/:archive/sqlite",
			Auth:   crawlremote.AuthPublisher,
		},
	}
	if cutover {
		requiredRoutes = append(requiredRoutes, crawlremote.RouteSpec{
			Method: http.MethodPost,
			Path:   "/v1/apps/:app/archives/:archive/cutover",
			Auth:   crawlremote.AuthPublisher,
		})
	}
	for _, required := range requiredRoutes {
		if !slices.ContainsFunc(contract.Routes, func(route crawlremote.RouteSpec) bool {
			return route == required
		}) {
			return fmt.Errorf(
				"remote contract does not advertise required snapshot publish route %s %s with %s auth",
				required.Method,
				required.Path,
				required.Auth,
			)
		}
	}
	for _, required := range gitcrawlCloudReaderQuerySpecs() {
		queryIndex := -1
		for index, query := range appSpec.Queries {
			if query.Name != required.Name {
				continue
			}
			if queryIndex >= 0 {
				return fmt.Errorf(
					"remote contract advertises required reader query %s more than once",
					required.Name,
				)
			}
			queryIndex = index
		}
		if queryIndex < 0 {
			return fmt.Errorf(
				"remote contract does not advertise required reader query %s",
				required.Name,
			)
		}
		remoteArgs := appSpec.Queries[queryIndex].Args
		if !equalUniqueStringSet(remoteArgs, required.Args) {
			return fmt.Errorf(
				"remote contract reader query %s has arguments %v, want %v",
				required.Name,
				remoteArgs,
				required.Args,
			)
		}
	}
	requiredTables := make([]crawlremote.IngestTableSpec, 0, len(snapshot.Datasets)+1)
	for _, dataset := range snapshot.Datasets {
		requiredTables = append(requiredTables, crawlremote.IngestTableSpec{
			Name:    dataset.Name,
			Columns: dataset.Columns,
		})
	}
	requiredTables = append(requiredTables, crawlremote.IngestTableSpec{
		Name:    "dataset_coverage",
		Columns: gitcrawlCloudCoverageColumns,
	})
	for _, required := range requiredTables {
		tableIndex := slices.IndexFunc(appSpec.IngestTables, func(table crawlremote.IngestTableSpec) bool {
			return table.Name == required.Name
		})
		if tableIndex < 0 {
			return fmt.Errorf(
				"remote contract does not advertise required snapshot ingest table %s",
				required.Name,
			)
		}
		remoteColumns := appSpec.IngestTables[tableIndex].Columns
		for _, column := range required.Columns {
			if !slices.Contains(remoteColumns, column) {
				return fmt.Errorf(
					"remote contract snapshot ingest table %s is missing required column %s",
					required.Name,
					column,
				)
			}
		}
	}
	return nil
}

func equalUniqueStringSet(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	seen := make(map[string]struct{}, len(left))
	for _, value := range left {
		if _, duplicate := seen[value]; duplicate {
			return false
		}
		seen[value] = struct{}{}
	}
	for _, value := range right {
		if _, duplicate := seen[value]; !duplicate {
			return false
		}
		delete(seen, value)
	}
	return len(seen) == 0
}

func gitcrawlPublisherStatusMatches(
	status crawlremote.PublisherStatus,
	manifest crawlremote.IngestManifest,
) bool {
	snapshot := status.Snapshot
	if status.App != manifest.App ||
		status.Archive != manifest.Archive ||
		snapshot == nil ||
		snapshot.ID != manifest.SnapshotID ||
		snapshot.SourceSHA256 != manifest.SourceSHA256 ||
		snapshot.SchemaName != manifest.SchemaName ||
		snapshot.SchemaVersion != manifest.SchemaVersion ||
		snapshot.SchemaHash != manifest.SchemaHash {
		return false
	}
	if !status.CoverageComplete {
		return false
	}
	if !equalUniqueStringSet(snapshot.Capabilities, manifest.Capabilities) {
		return false
	}
	return true
}

func gitcrawlCloudSQLiteBundlePrivacy() map[string]any {
	return map[string]any{
		"includes_private_messages": true,
		"includes_raw_json":         false,
		"includes_source_code":      false,
	}
}

func cloudSQLiteSnapshotPath(ctx context.Context, db *sql.DB, dbPath string) (string, func(), error) {
	snapshotPath, cleanup, err := sqliteSnapshotPath(ctx, db, "")
	if err != nil {
		source := strings.TrimSpace(dbPath)
		if source == "" {
			return "", func() {}, err
		}
		if _, statErr := os.Stat(source); statErr != nil {
			return "", func() {}, fmt.Errorf("stat cloud SQLite source: %w", statErr)
		}
		reopened, openErr := sql.Open("sqlite", source)
		if openErr != nil {
			return "", func() {}, fmt.Errorf("reopen cloud SQLite source: %w", openErr)
		}
		snapshotPath, cleanup, err = sqliteSnapshotPath(ctx, reopened, "")
		closeErr := reopened.Close()
		if err != nil {
			return "", func() {}, err
		}
		if closeErr != nil {
			cleanup()
			return "", func() {}, fmt.Errorf("close reopened cloud SQLite source: %w", closeErr)
		}
	}
	snapshotDB, err := sql.Open("sqlite", snapshotPath)
	if err != nil {
		cleanup()
		return "", func() {}, fmt.Errorf("open cloud SQLite snapshot: %w", err)
	}
	if err := sanitizeCloudSQLiteSnapshot(ctx, snapshotDB); err != nil {
		_ = snapshotDB.Close()
		cleanup()
		return "", func() {}, err
	}
	if _, err := snapshotDB.ExecContext(ctx, `vacuum`); err != nil {
		_ = snapshotDB.Close()
		cleanup()
		return "", func() {}, fmt.Errorf("compact cloud SQLite snapshot: %w", err)
	}
	if err := snapshotDB.Close(); err != nil {
		cleanup()
		return "", func() {}, fmt.Errorf("close cloud SQLite snapshot: %w", err)
	}
	return snapshotPath, cleanup, nil
}

func sanitizeCloudSQLiteSnapshot(ctx context.Context, db *sql.DB) error {
	for _, column := range []struct {
		table string
		name  string
	}{
		{table: "repositories", name: "raw_json"},
		{table: "threads", name: "raw_json"},
		{table: "comments", name: "raw_json"},
		{table: "pull_request_details", name: "raw_json"},
		{table: "pull_request_files", name: "raw_json"},
		{table: "pull_request_commits", name: "raw_json"},
		{table: "pull_request_checks", name: "raw_json"},
		{table: "pull_request_review_threads", name: "raw_json"},
		{table: "pull_request_review_threads", name: "comments_json"},
		{table: "github_workflow_runs", name: "raw_json"},
		{table: "sync_runs", name: "error_text"},
		{table: "sync_runs", name: "stats_json"},
		{table: "summary_runs", name: "error_text"},
		{table: "summary_runs", name: "stats_json"},
		{table: "embedding_runs", name: "error_text"},
		{table: "embedding_runs", name: "stats_json"},
		{table: "cluster_runs", name: "error_text"},
		{table: "cluster_runs", name: "stats_json"},
	} {
		exists, err := sqliteColumnExists(ctx, db, column.table, column.name)
		if err != nil {
			return fmt.Errorf("inspect cloud snapshot %s.%s: %w", column.table, column.name, err)
		}
		if !exists {
			continue
		}
		if _, err := db.ExecContext(
			ctx,
			`update `+column.table+` set `+column.name+` = '' where `+column.name+` is not null and `+column.name+` != ''`,
		); err != nil {
			return fmt.Errorf("clear cloud snapshot %s.%s: %w", column.table, column.name, err)
		}
	}
	for _, column := range []struct {
		table string
		name  string
	}{
		{table: "comments", name: "raw_json_blob_id"},
		{table: "thread_revisions", name: "raw_json_blob_id"},
		{table: "pull_request_files", name: "patch"},
	} {
		exists, err := sqliteColumnExists(ctx, db, column.table, column.name)
		if err != nil {
			return fmt.Errorf("inspect cloud snapshot %s.%s: %w", column.table, column.name, err)
		}
		if !exists {
			continue
		}
		value := "null"
		if column.name == "patch" {
			value = "''"
		}
		if _, err := db.ExecContext(
			ctx,
			`update `+column.table+` set `+column.name+` = `+value+` where `+column.name+` is not null`,
		); err != nil {
			return fmt.Errorf("clear cloud snapshot %s.%s: %w", column.table, column.name, err)
		}
	}
	for _, table := range []string{
		"thread_changed_files",
		"thread_hunk_signatures",
		"thread_code_snapshots",
		"code_documents_fts",
		"code_documents",
		"code_snapshots",
	} {
		if _, err := db.ExecContext(ctx, `drop table if exists `+table); err != nil {
			return fmt.Errorf("drop local-only cloud snapshot table %s: %w", table, err)
		}
	}
	if exists, err := sqliteTableExists(ctx, db, "blobs"); err != nil {
		return err
	} else if exists {
		if _, err := db.ExecContext(ctx, `delete from blobs`); err != nil {
			return fmt.Errorf("clear cloud snapshot blobs: %w", err)
		}
	}
	if exists, err := sqliteTableExists(ctx, db, "portable_metadata"); err != nil {
		return err
	} else if exists {
		if _, err := db.ExecContext(
			ctx,
			`update portable_metadata set value = '' where key = 'source_path'`,
		); err != nil {
			return fmt.Errorf("clear cloud snapshot portable source path: %w", err)
		}
	}
	return nil
}

func sqliteSnapshotPath(ctx context.Context, db *sql.DB, dbPath string) (string, func(), error) {
	tmpDir, err := os.MkdirTemp("", "gitcrawl-cloud-sqlite-*")
	if err != nil {
		return "", func() {}, fmt.Errorf("create sqlite snapshot dir: %w", err)
	}
	cleanup := func() { _ = os.RemoveAll(tmpDir) }
	snapshotPath := filepath.Join(tmpDir, "archive.db")
	if _, err := db.ExecContext(ctx, "vacuum main into ?", snapshotPath); err == nil {
		return snapshotPath, cleanup, nil
	}
	source := strings.TrimSpace(dbPath)
	if source == "" {
		cleanup()
		return "", func() {}, fmt.Errorf("sqlite snapshot failed and no source db path is available")
	}
	return source, cleanup, nil
}

func cloudFileSHA256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open sqlite snapshot for hash: %w", err)
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", fmt.Errorf("hash sqlite snapshot: %w", err)
	}
	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}

var gitcrawlRepositoryColumns = []string{"id", "full_name", "owner", "name", "html_url", "default_branch", "updated_at"}

const gitcrawlRepositoryExportSQL = `
select id, full_name, owner, name, '' as html_url, '' as default_branch, updated_at
from repositories
order by id`

var gitcrawlThreadColumns = []string{"id", "repo_id", "github_id", "number", "kind", "state", "title", "body", "author_login", "author_type", "html_url", "labels_json", "assignees_json", "is_draft", "created_at_gh", "updated_at_gh", "closed_at_gh", "merged_at_gh", "updated_at"}

func gitcrawlThreadExportSQL(ctx context.Context, db *sql.DB) (string, error) {
	bodyExpr, err := gitcrawlThreadBodyExpr(ctx, db)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`
select id, repo_id, github_id, number, kind, state, title, %s,
       coalesce(author_login, ''), coalesce(author_type, ''), html_url,
       labels_json, assignees_json, is_draft, coalesce(created_at_gh, ''),
       coalesce(updated_at_gh, ''), coalesce(closed_at_gh, ''),
       coalesce(merged_at_gh, ''), updated_at
from threads
order by repo_id, number`, bodyExpr), nil
}

func gitcrawlThreadBodyExpr(ctx context.Context, db *sql.DB) (string, error) {
	hasBody, err := sqliteColumnExists(ctx, db, "threads", "body")
	if err != nil {
		return "", err
	}
	hasExcerpt, err := sqliteColumnExists(ctx, db, "threads", "body_excerpt")
	if err != nil {
		return "", err
	}
	switch {
	case hasBody && hasExcerpt:
		return "coalesce(body, body_excerpt, '')", nil
	case hasBody:
		return "coalesce(body, '')", nil
	case hasExcerpt:
		return "coalesce(body_excerpt, '')", nil
	default:
		return "''", nil
	}
}

func sqliteColumnExists(ctx context.Context, db *sql.DB, table, column string) (bool, error) {
	rows, err := db.QueryContext(ctx, `pragma table_info(`+table+`)`)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, typ string
		var notNull int
		var defaultValue any
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk); err != nil {
			return false, err
		}
		if name == column {
			return true, nil
		}
	}
	return false, rows.Err()
}

func sqliteTableExists(ctx context.Context, db *sql.DB, table string) (bool, error) {
	var name string
	err := db.QueryRowContext(
		ctx,
		`select name from sqlite_schema where type in ('table', 'view') and name = ?`,
		table,
	).Scan(&name)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("inspect cloud snapshot table %s: %w", table, err)
	}
	return name == table, nil
}
