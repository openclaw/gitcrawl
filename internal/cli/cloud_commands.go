package cli

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	crawlremote "github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/gitcrawl/internal/config"
)

const (
	gitcrawlCloudBatchSize                   = 250
	gitcrawlCloudIngestRequestMaxBytes       = int64(4 * 1024 * 1024)
	gitcrawlCloudSQLiteBundleChunkSize       = int64(64 * 1024 * 1024)
	gitcrawlCloudPublishPreflightTimeout     = 30 * time.Second
	gitcrawlCloudPostCutoverStatusAttempts   = 5
	gitcrawlCloudPostCutoverStatusRetryDelay = 100 * time.Millisecond
	gitcrawlCloudHydrationTimeout            = 10 * time.Minute

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
	publicationCapabilities := gitcrawlCloudPublicationCapabilities(manifest.Capabilities)
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
	status, statusErr := client.PublishStatusForSnapshot(
		ctx,
		"gitcrawl",
		archiveID,
		snapshot.ID,
	)
	if statusErr == nil {
		alreadyStaged = gitcrawlPublisherStatusMatches(
			status,
			manifest,
			publicationCapabilities,
		)
		if alreadyStaged {
			snapshot.DatasetGeneratedAt = status.Snapshot.DatasetGeneratedAt
		}
	} else if !remoteNotFound(statusErr) && !remoteSnapshotIncomplete(statusErr) {
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
			snapshot.ID,
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
		completedConcurrently := false
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
				recoveredGeneration, recoveryErr := recoverConcurrentGitcrawlSnapshot(
					ctx,
					client,
					archiveID,
					snapshot,
					manifest,
					publicationCapabilities,
					err,
				)
				if recoveryErr != nil {
					return fmt.Errorf(
						"publish cloud dataset %s: %w",
						dataset.Name,
						recoveryErr,
					)
				}
				if recoveredGeneration != "" {
					snapshot.DatasetGeneratedAt = recoveredGeneration
					mutationToken = ""
					alreadyStaged = true
					completedConcurrently = true
					break
				}
				return fmt.Errorf("publish cloud dataset %s: %w", dataset.Name, err)
			}
			counts[dataset.Name] = progress.RowsAccepted
			mutationToken = progress.MutationToken
		}
		if !completedConcurrently {
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
				recoveredGeneration, recoveryErr := recoverConcurrentGitcrawlSnapshot(
					ctx,
					client,
					archiveID,
					snapshot,
					manifest,
					publicationCapabilities,
					err,
				)
				if recoveryErr != nil {
					return fmt.Errorf("complete cloud snapshot staging: %w", recoveryErr)
				}
				if recoveredGeneration == "" {
					return fmt.Errorf("complete cloud snapshot staging: %w", err)
				}
				snapshot.DatasetGeneratedAt = recoveredGeneration
				mutationToken = ""
				alreadyStaged = true
			} else {
				mutationToken = progress.MutationToken
			}
		}
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
			"",
		)
		expectedCutoverAt := ""
		if !alreadyCutOver {
			result, err := client.Cutover(ctx, "gitcrawl", archiveID, snapshot.ID)
			if err != nil {
				return fmt.Errorf("cut over cloud snapshot: %w", err)
			}
			if err := validateGitcrawlCutoverResult(result, archiveID, snapshot.ID); err != nil {
				return fmt.Errorf("validate cloud snapshot cutover: %w", err)
			}
			cutoverResult = &result
			expectedCutoverAt = result.CutoverAt
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
			expectedCutoverAt,
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
		"capabilities":          manifest.Capabilities,
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
