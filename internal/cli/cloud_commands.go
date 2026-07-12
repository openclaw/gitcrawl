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
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	crawlremote "github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/gitcrawl/internal/config"
)

const (
	gitcrawlCloudBatchSize               = 250
	gitcrawlCloudSQLiteBundleChunkSize   = int64(64 * 1024 * 1024)
	gitcrawlCloudPublishPreflightTimeout = 30 * time.Second
	gitcrawlCloudHydrationTimeout        = 10 * time.Minute

	gitcrawlSnapshotAtomicCapability     = "gitcrawl.snapshot.atomic"
	gitcrawlSnapshotCutoverCapability    = "gitcrawl.snapshot.cutover"
	gitcrawlSnapshotProvenanceCapability = "gitcrawl.snapshot.provenance.v1"
	gitcrawlSnapshotStagingCapability    = "gitcrawl.snapshot.staging.v1"
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

func gitcrawlCloudPublicationCapabilities(requested []string) []string {
	capabilities := make([]string, 0, len(gitcrawlCloudReaderQuerySpecs())+len(requested))
	for _, query := range gitcrawlCloudReaderQuerySpecs() {
		capabilities = append(capabilities, query.Name)
	}
	return append(capabilities, requested...)
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
	httpClient := &http.Client{Timeout: 10 * time.Minute}
	tokenProvider := crawlremote.EnvTokenProvider{Name: remoteCfg.TokenEnv}
	client, err := crawlremote.NewClientFromConfig(remoteCfg, crawlremote.Options{
		UserAgent:     "gitcrawl/" + version,
		HTTPClient:    httpClient,
		TokenProvider: tokenProvider,
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
	publicationCapabilities := gitcrawlCloudPublicationCapabilities(snapshot.Capabilities)
	counts := gitcrawlCloudDatasetCounts(snapshot)
	if err := requireGitcrawlSnapshotPublishContract(
		ctx,
		client,
		snapshot,
		cutover,
	); err != nil {
		return err
	}
	if err := requireGitcrawlCloudPublishRoles(ctx, client); err != nil {
		return err
	}
	snapshotInfo, err := os.Stat(snapshotPath)
	if err != nil {
		return fmt.Errorf("stat frozen cloud snapshot: %w", err)
	}
	sqliteSourceSize := snapshotInfo.Size()
	if sqliteSourceSize <= 0 {
		return fmt.Errorf("frozen cloud snapshot has invalid size %d", sqliteSourceSize)
	}

	alreadyStaged := false
	status, statusErr := client.PublishStatus(ctx, "gitcrawl", archiveID)
	if statusErr == nil {
		alreadyStaged = gitcrawlPublisherStatusMatches(
			status,
			manifest,
			publicationCapabilities,
		)
		if alreadyStaged {
			snapshot.DatasetGeneratedAt = status.Snapshot.DatasetGeneratedAt
		}
	} else if !remoteNotFound(statusErr) {
		return statusErr
	}
	var sqliteBundle *crawlremote.SQLiteBundle
	var mutationToken string
	if !alreadyStaged {
		uploadedBundle, uploadedSourceSize, err := uploadSQLiteSnapshotArchive(
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
		if uploadedSourceSize != sqliteSourceSize {
			return fmt.Errorf(
				"SQLite bundle source size changed from %d to %d",
				sqliteSourceSize,
				uploadedSourceSize,
			)
		}
		sqliteBundle = uploadedBundle
		for _, dataset := range snapshot.Datasets {
			progress, err := sendSnapshotIngestDataset(
				ctx,
				snapshotDB,
				client,
				"gitcrawl",
				archiveID,
				manifest,
				dataset,
				mutationToken,
			)
			if err != nil {
				return fmt.Errorf("publish cloud dataset %s: %w", dataset.Name, err)
			}
			counts[dataset.Name] = progress.RowsAccepted
			mutationToken = progress.MutationToken
		}
		progress, err := completeGitcrawlSnapshotStaging(
			ctx,
			client,
			"gitcrawl",
			archiveID,
			manifest,
			snapshot,
			mutationToken,
		)
		if err != nil {
			return fmt.Errorf("complete cloud snapshot staging: %w", err)
		}
		mutationToken = progress.MutationToken
	}
	var cutoverResult *crawlremote.CutoverResult
	alreadyCutOver := false
	if cutover {
		readerStatus, err := client.Status(ctx, "gitcrawl", archiveID)
		if err != nil {
			return fmt.Errorf("read serving cloud snapshot status: %w", err)
		}
		if readerStatus.App != "gitcrawl" || readerStatus.Archive != archiveID {
			return fmt.Errorf(
				"serving cloud snapshot status returned app=%q archive=%q",
				readerStatus.App,
				readerStatus.Archive,
			)
		}
		alreadyCutOver = gitcrawlReaderStatusMatches(
			readerStatus,
			snapshot,
			manifest,
			publicationCapabilities,
		)
		if !alreadyCutOver {
			result, err := client.Cutover(ctx, "gitcrawl", archiveID, snapshot.ID)
			if err != nil {
				return fmt.Errorf("cut over cloud snapshot: %w", err)
			}
			cutoverResult = &result
		}
		if err := verifyGitcrawlSnapshotPublication(
			ctx,
			client,
			httpClient,
			tokenProvider,
			endpoint,
			archiveID,
			snapshot,
			manifest,
			publicationCapabilities,
			sqliteSourceSize,
		); err != nil {
			return fmt.Errorf("verify published cloud snapshot: %w", err)
		}
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
		"already_cut_over":      alreadyCutOver,
		"mutation_token":        mutationToken,
		"cutover":               cutoverResult,
		"sqlite_bundle":         sqliteBundle,
		"sqlite_bundle_privacy": sqliteBundlePrivacy,
	}, true)
}

type ingestProgress struct {
	RowsAccepted  int64
	MutationToken string
	Result        crawlremote.IngestResult
}

func sendSnapshotIngestDataset(
	ctx context.Context,
	db *sql.DB,
	client *crawlremote.Client,
	app, archive string,
	manifest crawlremote.IngestManifest,
	dataset gitcrawlCloudDataset,
	mutationToken string,
) (ingestProgress, error) {
	rows, err := db.QueryContext(ctx, dataset.Query)
	if err != nil {
		return ingestProgress{}, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return ingestProgress{}, err
	}
	if len(columns) != len(dataset.Columns) {
		return ingestProgress{}, fmt.Errorf(
			"dataset query returned %d columns, want %d",
			len(columns),
			len(dataset.Columns),
		)
	}

	batch := make([][]any, 0, gitcrawlCloudBatchSize)
	var scanned int64
	var accepted int64
	flush := func() error {
		result, err := sendIngestBatch(
			ctx,
			client,
			app,
			archive,
			manifest,
			dataset.Name,
			dataset.Columns,
			batch,
			scanned-int64(len(batch)),
			mutationToken,
			false,
		)
		if err != nil {
			return err
		}
		if result.RowsAccepted != int64(len(batch)) {
			return fmt.Errorf(
				"remote accepted %d rows from a %d-row batch",
				result.RowsAccepted,
				len(batch),
			)
		}
		accepted += result.RowsAccepted
		mutationToken = result.MutationToken
		batch = batch[:0]
		return nil
	}

	for rows.Next() {
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for index := range values {
			pointers[index] = &values[index]
		}
		if err := rows.Scan(pointers...); err != nil {
			return ingestProgress{RowsAccepted: accepted, MutationToken: mutationToken}, err
		}
		for index, value := range values {
			if bytes, ok := value.([]byte); ok {
				values[index] = string(bytes)
			}
		}
		batch = append(batch, values)
		scanned++
		if len(batch) == gitcrawlCloudBatchSize {
			if err := flush(); err != nil {
				return ingestProgress{RowsAccepted: accepted, MutationToken: mutationToken}, err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return ingestProgress{RowsAccepted: accepted, MutationToken: mutationToken}, err
	}
	if len(batch) > 0 || scanned == 0 {
		if err := flush(); err != nil {
			return ingestProgress{RowsAccepted: accepted, MutationToken: mutationToken}, err
		}
	}
	if scanned != dataset.RowCount {
		return ingestProgress{RowsAccepted: accepted, MutationToken: mutationToken}, fmt.Errorf(
			"dataset row count changed from preflight %d to stream %d",
			dataset.RowCount,
			scanned,
		)
	}
	return ingestProgress{RowsAccepted: accepted, MutationToken: mutationToken}, nil
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
		return ingestProgress{
			RowsAccepted:  result.RowsAccepted,
			MutationToken: result.MutationToken,
			Result:        result,
		}, err
	}
	var lastResult crawlremote.IngestResult
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
			int64(start),
			mutationToken,
			final && end == len(rows),
		)
		if err != nil {
			return ingestProgress{
				RowsAccepted:  total,
				MutationToken: mutationToken,
				Result:        lastResult,
			}, err
		}
		total += result.RowsAccepted
		mutationToken = result.MutationToken
		lastResult = result
	}
	return ingestProgress{
		RowsAccepted:  total,
		MutationToken: mutationToken,
		Result:        lastResult,
	}, nil
}

func completeGitcrawlSnapshotStaging(
	ctx context.Context,
	client *crawlremote.Client,
	app, archive string,
	manifest crawlremote.IngestManifest,
	snapshot gitcrawlCloudSnapshot,
	mutationToken string,
) (ingestProgress, error) {
	progress, err := sendSnapshotIngestRows(
		ctx,
		client,
		app,
		archive,
		manifest,
		"dataset_coverage",
		gitcrawlCloudCoverageColumns,
		gitcrawlCloudCoverageRows(snapshot, mutationToken),
		mutationToken,
		true,
	)
	if err != nil {
		return progress, err
	}
	result := progress.Result
	expectedRows := int64(len(snapshot.Datasets))
	if result.Table != "dataset_coverage" {
		return progress, fmt.Errorf(
			"remote completed table %q, want dataset_coverage",
			result.Table,
		)
	}
	if result.SnapshotID != snapshot.ID {
		return progress, fmt.Errorf(
			"remote completed snapshot %q, want %q",
			result.SnapshotID,
			snapshot.ID,
		)
	}
	if progress.RowsAccepted != expectedRows || result.RowsAccepted != expectedRows {
		return progress, fmt.Errorf(
			"remote accepted %d coverage rows with final batch count %d, want %d datasets",
			progress.RowsAccepted,
			result.RowsAccepted,
			expectedRows,
		)
	}
	if result.MutationToken != mutationToken {
		return progress, fmt.Errorf(
			"remote completed mutation token %q, want %q",
			result.MutationToken,
			mutationToken,
		)
	}
	if !result.Complete {
		return progress, fmt.Errorf("remote did not complete snapshot %s", snapshot.ID)
	}
	return progress, nil
}

func sendIngestBatch(
	ctx context.Context,
	client *crawlremote.Client,
	app, archive string,
	manifest crawlremote.IngestManifest,
	table string,
	columns []string,
	rows [][]any,
	cursor int64,
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

func cursorFor(start int64) string {
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
	bundle, _, err := uploadSQLiteSnapshotArchive(ctx, client, app, archive, snapshotPath, counts)
	return bundle, err
}

func uploadSQLiteSnapshotArchive(
	ctx context.Context,
	client *crawlremote.Client,
	app, archive, snapshotPath string,
	counts map[string]int64,
) (*crawlremote.SQLiteBundle, int64, error) {
	bundle, err := crawlremote.BuildSnapshotGzipSQLiteBundle(ctx, crawlremote.SQLiteBundleBuildOptions{
		App:        app,
		Archive:    archive,
		SourcePath: snapshotPath,
		ChunkSize:  gitcrawlCloudSQLiteBundleChunkSize,
		Counts:     counts,
		Privacy:    gitcrawlCloudSQLiteBundlePrivacy(),
	})
	if err != nil {
		return nil, 0, err
	}
	defer bundle.Cleanup()
	sourceSize := bundle.Manifest.Object.Size
	if sourceSize <= 0 {
		return nil, 0, fmt.Errorf("SQLite bundle manifest has invalid source size %d", sourceSize)
	}
	result, err := client.UploadSQLiteBundleFiles(ctx, app, archive, bundle.Manifest, bundle.Parts)
	if err != nil {
		return nil, 0, err
	}
	uploadedBundle, err := validateGitcrawlSQLiteBundleUpload(
		result,
		app,
		archive,
		bundle.Manifest,
	)
	if err != nil {
		return nil, 0, err
	}
	return uploadedBundle, sourceSize, nil
}

func validateGitcrawlSQLiteBundleUpload(
	result crawlremote.SQLiteBundleUploadResult,
	app, archive string,
	expected crawlremote.SQLiteBundleManifest,
) (*crawlremote.SQLiteBundle, error) {
	if result.App != app || result.Archive != archive {
		return nil, fmt.Errorf(
			"SQLite bundle upload returned app=%q archive=%q, want app=%q archive=%q",
			result.App,
			result.Archive,
			app,
			archive,
		)
	}
	if !result.Complete {
		return nil, fmt.Errorf("SQLite bundle upload was not finalized")
	}
	if result.Bundle == nil || result.Bundle.Manifest == nil {
		return nil, fmt.Errorf("SQLite bundle upload omitted the finalized manifest")
	}
	actual := result.Bundle.Manifest
	if actual.SnapshotID != expected.SnapshotID {
		return nil, fmt.Errorf(
			"SQLite bundle upload acknowledged snapshot %q, want %q",
			actual.SnapshotID,
			expected.SnapshotID,
		)
	}
	if actual.Object.SHA256 != expected.Object.SHA256 {
		return nil, fmt.Errorf(
			"SQLite bundle upload acknowledged digest %q, want %q",
			actual.Object.SHA256,
			expected.Object.SHA256,
		)
	}
	if actual.Object.Size != expected.Object.Size {
		return nil, fmt.Errorf(
			"SQLite bundle upload acknowledged source size %d, want %d",
			actual.Object.Size,
			expected.Object.Size,
		)
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
		gitcrawlSnapshotStagingCapability,
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
			Path:   "/v1/whoami",
			Auth:   crawlremote.AuthReader,
		},
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
		requiredRoutes = append(
			requiredRoutes,
			crawlremote.RouteSpec{
				Method: http.MethodGet,
				Path:   "/v1/apps/:app/archives/:archive/status",
				Auth:   crawlremote.AuthReader,
			},
			crawlremote.RouteSpec{
				Method: http.MethodPost,
				Path:   "/v1/apps/:app/archives/:archive/cutover",
				Auth:   crawlremote.AuthPublisher,
			},
			crawlremote.RouteSpec{
				Method: http.MethodGet,
				Path:   "/v1/apps/:app/archives/:archive/sqlite",
				Auth:   crawlremote.AuthReader,
			},
		)
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
		if !uniqueStringSuperset(remoteArgs, required.Args) {
			return fmt.Errorf(
				"remote contract reader query %s has arguments %v, missing required arguments from %v",
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

func requireGitcrawlCloudPublishRoles(ctx context.Context, client *crawlremote.Client) error {
	preflightCtx, cancel := context.WithTimeout(ctx, gitcrawlCloudPublishPreflightTimeout)
	defer cancel()
	identity, err := client.Whoami(preflightCtx)
	if err != nil {
		return fmt.Errorf("read remote identity before snapshot publication: %w", err)
	}
	if slices.Contains(identity.Roles, "admin") {
		return nil
	}
	missing := make([]string, 0, 2)
	for _, role := range []string{crawlremote.AuthPublisher, crawlremote.AuthReader} {
		if !slices.Contains(identity.Roles, role) {
			missing = append(missing, role)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf(
			"remote token must have publisher and reader roles before snapshot publication; missing %s",
			strings.Join(missing, ", "),
		)
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

func uniqueStringSuperset(values, required []string) bool {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if _, duplicate := seen[value]; duplicate {
			return false
		}
		seen[value] = struct{}{}
	}
	for _, value := range required {
		if _, ok := seen[value]; !ok {
			return false
		}
	}
	return true
}

func gitcrawlPublisherStatusMatches(
	status crawlremote.PublisherStatus,
	manifest crawlremote.IngestManifest,
	publicationCapabilities []string,
) bool {
	snapshot := status.Snapshot
	if status.App != manifest.App ||
		status.Archive != manifest.Archive ||
		status.ActiveSnapshotID != manifest.SnapshotID ||
		snapshot == nil ||
		snapshot.ID != manifest.SnapshotID ||
		snapshot.SourceSHA256 != manifest.SourceSHA256 ||
		snapshot.SourceSyncAt != manifest.SourceSyncAt ||
		snapshot.SchemaName != manifest.SchemaName ||
		snapshot.SchemaVersion != manifest.SchemaVersion ||
		snapshot.SchemaHash != manifest.SchemaHash ||
		strings.TrimSpace(snapshot.DatasetGeneratedAt) == "" {
		return false
	}
	if status.CoverageComplete != snapshot.CoverageComplete ||
		!status.CoverageComplete {
		return false
	}
	if !equalUniqueStringSet(snapshot.Capabilities, publicationCapabilities) {
		return false
	}
	return true
}

func gitcrawlReaderStatusMatches(
	status crawlremote.Status,
	snapshot gitcrawlCloudSnapshot,
	manifest crawlremote.IngestManifest,
	publicationCapabilities []string,
) bool {
	if status.App != manifest.App ||
		status.Archive != manifest.Archive ||
		status.ActiveSnapshotID != snapshot.ID ||
		status.SchemaName != manifest.SchemaName ||
		status.SchemaVersion != manifest.SchemaVersion ||
		status.SchemaHash != manifest.SchemaHash ||
		status.SourceSyncAt != snapshot.SourceSyncAt ||
		status.DatasetGeneratedAt != snapshot.DatasetGeneratedAt ||
		!status.CoverageComplete ||
		!equalUniqueStringSet(status.Capabilities, publicationCapabilities) {
		return false
	}
	if !gitcrawlPublisherStatusMatches(crawlremote.PublisherStatus{
		App:              status.App,
		Archive:          status.Archive,
		ActiveSnapshotID: status.ActiveSnapshotID,
		CoverageComplete: status.CoverageComplete,
		Snapshot:         status.Snapshot,
	}, manifest, publicationCapabilities) ||
		status.Snapshot.DatasetGeneratedAt != snapshot.DatasetGeneratedAt {
		return false
	}
	return gitcrawlDatasetCoverageMatches(status.Datasets, snapshot)
}

func gitcrawlDatasetCoverageMatches(
	actual []crawlremote.DatasetCoverage,
	snapshot gitcrawlCloudSnapshot,
) bool {
	if len(actual) != len(snapshot.Datasets) {
		return false
	}
	expected := make(map[string]gitcrawlCloudDataset, len(snapshot.Datasets))
	for _, dataset := range snapshot.Datasets {
		if _, duplicate := expected[dataset.Name]; duplicate {
			return false
		}
		expected[dataset.Name] = dataset
	}
	for _, dataset := range actual {
		want, ok := expected[dataset.Dataset]
		if !ok ||
			dataset.RowCount != want.RowCount ||
			dataset.EligibleCount != want.EligibleCount ||
			dataset.CoveredCount != want.CoveredCount ||
			dataset.FreshCount != want.CoveredCount ||
			dataset.MaxSourceAt != want.MaxSourceAt ||
			dataset.DatasetGeneratedAt != snapshot.DatasetGeneratedAt ||
			dataset.Complete != want.Complete {
			return false
		}
		delete(expected, dataset.Dataset)
	}
	return len(expected) == 0
}

func verifyGitcrawlSnapshotPublication(
	ctx context.Context,
	client *crawlremote.Client,
	httpClient *http.Client,
	tokenProvider crawlremote.TokenProvider,
	endpoint, archive string,
	snapshot gitcrawlCloudSnapshot,
	manifest crawlremote.IngestManifest,
	publicationCapabilities []string,
	sourceSize int64,
) error {
	status, err := client.PublishStatus(ctx, "gitcrawl", archive)
	if err != nil {
		return fmt.Errorf("read post-cutover publisher status: %w", err)
	}
	if !gitcrawlPublisherStatusMatches(status, manifest, publicationCapabilities) ||
		status.Snapshot.DatasetGeneratedAt != snapshot.DatasetGeneratedAt {
		return fmt.Errorf(
			"post-cutover publisher status does not match snapshot %s digest, profile, generation, and coverage",
			snapshot.ID,
		)
	}

	token, err := tokenProvider.Token(ctx)
	if err != nil {
		return fmt.Errorf("read remote token for snapshot hydration: %w", err)
	}
	sqliteURL := strings.TrimRight(endpoint, "/") +
		"/v1/apps/" + url.PathEscape("gitcrawl") +
		"/archives/" + url.PathEscape(archive) +
		"/sqlite"
	hydrationCtx, cancel := context.WithTimeout(ctx, gitcrawlCloudHydrationTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(hydrationCtx, http.MethodGet, sqliteURL, nil)
	if err != nil {
		return fmt.Errorf("build snapshot hydration request: %w", err)
	}
	request.Header.Set("accept", "application/vnd.sqlite3, application/octet-stream")
	request.Header.Set("authorization", "Bearer "+token)
	request.Header.Set("user-agent", "gitcrawl/"+version)
	response, err := httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("download bound SQLite snapshot: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		return fmt.Errorf(
			"download bound SQLite snapshot: status=%d body=%s",
			response.StatusCode,
			strings.TrimSpace(string(body)),
		)
	}
	return verifyGitcrawlSQLiteHydration(response, snapshot.ID, sourceSize)
}

func verifyGitcrawlSQLiteHydration(
	response *http.Response,
	expectedDigest string,
	expectedSize int64,
) error {
	advertised := strings.TrimSpace(response.Header.Get("x-crawl-content-sha256"))
	if advertised == "" {
		return fmt.Errorf("downloaded SQLite snapshot is missing x-crawl-content-sha256")
	}
	if !strings.EqualFold(advertised, expectedDigest) {
		return fmt.Errorf(
			"downloaded SQLite snapshot advertises digest %s, want %s",
			advertised,
			expectedDigest,
		)
	}
	if expectedSize <= 0 {
		return fmt.Errorf("uploaded SQLite manifest source size must be positive, got %d", expectedSize)
	}
	contentLength := response.ContentLength
	if contentLength <= 0 {
		header := strings.TrimSpace(response.Header.Get("content-length"))
		if header == "" {
			return fmt.Errorf("downloaded SQLite snapshot is missing a positive Content-Length")
		}
		parsed, err := strconv.ParseInt(header, 10, 64)
		if err != nil || parsed <= 0 {
			return fmt.Errorf("downloaded SQLite snapshot has invalid Content-Length %q", header)
		}
		contentLength = parsed
	}
	if contentLength != expectedSize {
		return fmt.Errorf(
			"downloaded SQLite snapshot Content-Length %d does not match uploaded source size %d",
			contentLength,
			expectedSize,
		)
	}
	hash := sha256.New()
	written, err := io.CopyN(hash, response.Body, expectedSize)
	if err != nil {
		return fmt.Errorf(
			"downloaded SQLite snapshot truncated after %d of %d bytes: %w",
			written,
			expectedSize,
			err,
		)
	}
	var extra [1]byte
	n, err := response.Body.Read(extra[:])
	if n > 0 {
		return fmt.Errorf(
			"downloaded SQLite snapshot exceeds Content-Length %d",
			expectedSize,
		)
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("check downloaded SQLite snapshot boundary: %w", err)
	}
	actual := fmt.Sprintf("%x", hash.Sum(nil))
	if actual != expectedDigest {
		return fmt.Errorf(
			"downloaded SQLite snapshot digest %s does not match source %s",
			actual,
			expectedDigest,
		)
	}
	return nil
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
