package cli

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	portableexport "github.com/openclaw/gitcrawl/internal/portable"
	"github.com/openclaw/gitcrawl/internal/store"
)

func (a *App) runPortable(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return usageErr(fmt.Errorf("portable requires a subcommand"))
	}
	switch args[0] {
	case "help", "--help", "-h":
		return a.printCommandUsage("portable")
	case "prune":
		return a.runPortablePrune(ctx, args[1:])
	case "export":
		return a.runPortableExport(ctx, args[1:])
	case "refresh":
		return a.runPortableRefresh(ctx, args[1:])
	default:
		return usageErr(fmt.Errorf("unknown portable subcommand %q", args[0]))
	}
}

func (a *App) runPortableExport(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("portable export", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	profile := fs.String("profile", "", "portable export profile")
	bodyCharsRaw := fs.String("body-chars", "256", "maximum thread body characters to keep")
	outputDir := fs.String("output-dir", "", "new artifact directory")
	databaseName := fs.String("database-name", "gitcrawl.db", "portable database basename")
	publicPath := fs.String("public-path", "", "logical portable database path")
	repositoryRaw := fs.String("repository", "", "restrict the artifact to one owner/repo")
	maxBytesRaw := fs.String("max-bytes", "", "maximum finalized database bytes")
	compression := fs.String("compression", "", "artifact compression (gzip)")
	maxArchiveBytesRaw := fs.String("max-archive-bytes", "", "maximum finalized archive bytes")
	jsonOut := fs.Bool("json", false, "write JSON output")
	valueFlags := map[string]bool{
		"profile": true, "body-chars": true, "output-dir": true,
		"database-name": true, "public-path": true, "repository": true, "max-bytes": true,
		"compression": true, "max-archive-bytes": true,
	}
	if err := fs.Parse(normalizeCommandArgs(args, valueFlags)); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 0 {
		return usageErr(fmt.Errorf("portable export does not take positional arguments"))
	}
	if strings.TrimSpace(*profile) == "" {
		return usageErr(fmt.Errorf("--profile is required"))
	}
	if strings.TrimSpace(*outputDir) == "" {
		return usageErr(fmt.Errorf("--output-dir is required"))
	}
	bodyChars, err := parseOptionalPositiveInt(*bodyCharsRaw)
	if err != nil {
		return usageErr(err)
	}
	if bodyChars == 0 {
		bodyChars = 256
	}
	var maxBytes *int64
	if strings.TrimSpace(*maxBytesRaw) != "" {
		parsed, err := strconv.ParseInt(strings.TrimSpace(*maxBytesRaw), 10, 64)
		if err != nil || parsed <= 0 {
			return usageErr(fmt.Errorf("--max-bytes must be a positive integer"))
		}
		maxBytes = &parsed
	}
	var maxArchiveBytes *int64
	if strings.TrimSpace(*maxArchiveBytesRaw) != "" {
		parsed, err := strconv.ParseInt(strings.TrimSpace(*maxArchiveBytesRaw), 10, 64)
		if err != nil || parsed <= 0 {
			return usageErr(fmt.Errorf("--max-archive-bytes must be a positive integer"))
		}
		maxArchiveBytes = &parsed
	}
	if *compression != "" && *compression != portableexport.CompressionGzip {
		return usageErr(fmt.Errorf("--compression must be %q", portableexport.CompressionGzip))
	}
	if maxArchiveBytes != nil && *compression == "" {
		return usageErr(fmt.Errorf("--max-archive-bytes requires --compression"))
	}
	logicalPath := *publicPath
	if logicalPath == "" {
		logicalPath = *databaseName
	}
	if _, err := portableexport.ResolveProfile(*profile); err != nil {
		return usageErr(err)
	}
	if err := portableexport.ValidateDatabaseName(*databaseName); err != nil {
		return usageErr(err)
	}
	if err := portableexport.ValidatePublicPath(logicalPath); err != nil {
		return usageErr(err)
	}
	repository := ""
	if strings.TrimSpace(*repositoryRaw) != "" {
		owner, name, err := parseOwnerRepo(*repositoryRaw)
		if err != nil {
			return usageErr(fmt.Errorf("repository: %w", err))
		}
		repository = owner + "/" + name
		if strings.TrimSpace(*repositoryRaw) != repository {
			return usageErr(fmt.Errorf("repository: expected owner/repo, got %q", *repositoryRaw))
		}
	}
	rt, err := a.openLocalRuntimeReadOnly(ctx)
	if err != nil {
		return err
	}
	sourceDBPath := rt.Store.Path()
	if err := rt.Store.Close(); err != nil {
		return err
	}
	progressStarted := time.Now()
	progressPrevious := progressStarted
	result, err := portableexport.Export(ctx, portableexport.ExportOptions{
		SourceDBPath:    sourceDBPath,
		OutputDir:       *outputDir,
		DatabaseName:    *databaseName,
		PublicPath:      logicalPath,
		Profile:         *profile,
		Repository:      repository,
		BodyChars:       bodyChars,
		MaxBytes:        maxBytes,
		Compression:     *compression,
		MaxArchiveBytes: maxArchiveBytes,
		Progress: func(stage portableexport.Stage) {
			now := time.Now()
			fmt.Fprintf(
				a.Stderr,
				"gitcrawl: portable export: stage=%s after=%s total=%s\n",
				stage,
				formatPortableProgressDuration(now.Sub(progressPrevious)),
				formatPortableProgressDuration(now.Sub(progressStarted)),
			)
			progressPrevious = now
		},
	})
	if err != nil {
		return err
	}
	return a.writeOutput("portable export", result, true)
}

func formatPortableProgressDuration(value time.Duration) string {
	if value < 0 {
		value = 0
	}
	return value.Round(100 * time.Millisecond).String()
}

func (a *App) runPortablePrune(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("portable prune", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	bodyCharsRaw := fs.String("body-chars", "256", "maximum thread body characters to keep")
	noVacuum := fs.Bool("no-vacuum", false, "skip size-reclaim vacuum unless needed to scrub failure history")
	includeSyncFailures := fs.Bool("include-sync-failures", false, "include the sync failure ledger with redacted error messages")
	noPublish := fs.Bool("no-publish", false, "do not publish the pruned runtime mirror back to the portable checkout")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"body-chars": true})); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 0 {
		return usageErr(fmt.Errorf("portable prune does not take positional arguments"))
	}
	bodyChars, err := parseOptionalPositiveInt(*bodyCharsRaw)
	if err != nil {
		return usageErr(err)
	}
	if bodyChars == 0 {
		bodyChars = 256
	}

	rt, err := a.openLocalRuntime(ctx)
	if err != nil {
		return err
	}
	target := rt.dbTarget()
	remoteSource := rt.RemoteSource
	sourceDBPath := rt.SourceDBPath
	stats, err := rt.Store.PrunePortablePayloads(ctx, store.PortablePruneOptions{
		BodyChars:           bodyChars,
		Vacuum:              !*noVacuum,
		IncludeSyncFailures: *includeSyncFailures,
	})
	closeErr := rt.Store.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err := sqliteStoreHealth(ctx, stats.DBPath); err != nil {
		return fmt.Errorf("validate portable db after prune: %w", err)
	}
	manifestPath, sha, err := writePortableDBManifest(stats)
	if err != nil {
		return err
	}
	stats.ManifestPath = manifestPath
	stats.SHA256 = sha
	stats.QuickCheck = "ok"
	result := struct {
		store.PortablePruneStats
		dbTargetInfo
		Published             bool   `json:"published"`
		PublishedDBPath       string `json:"published_db_path,omitempty"`
		PublishedManifestPath string `json:"published_manifest_path,omitempty"`
	}{PortablePruneStats: stats, dbTargetInfo: target}
	if remoteSource && !*noPublish {
		publishedManifestPath := portableDBManifestPath(sourceDBPath)
		if err := publishPortableCheckoutPair(ctx, stats.DBPath, manifestPath, sourceDBPath, publishedManifestPath); err != nil {
			return fmt.Errorf("publish portable store: %w", err)
		}
		result.Published = true
		result.PublishedDBPath = sourceDBPath
		result.PublishedManifestPath = publishedManifestPath
		fmt.Fprintf(a.Stderr, "gitcrawl: published pruned database to %s; commit both the database and %s.\n", sourceDBPath, publishedManifestPath)
	}
	return a.writeOutput("portable prune", result, true)
}

func writePortableDBManifest(stats store.PortablePruneStats) (string, string, error) {
	info, err := os.Stat(stats.DBPath)
	if err != nil {
		return "", "", fmt.Errorf("stat portable db for manifest: %w", err)
	}
	sum, err := fileSHA256(stats.DBPath)
	if err != nil {
		return "", "", fmt.Errorf("hash portable db for manifest: %w", err)
	}
	sumText := fmt.Sprintf("%x", sum)
	manifest := portableDBManifest{
		Schema:      "gitcrawl-portable-sync-v2",
		ExportedAt:  time.Now().UTC().Format(time.RFC3339Nano),
		OutputPath:  filepath.Base(stats.DBPath),
		OutputBytes: info.Size(),
		SHA256:      sumText,
		QuickCheck:  "ok",
	}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return "", "", err
	}
	manifestPath := portableDBManifestPath(stats.DBPath)
	if err := writeAtomicFile(manifestPath, append(data, '\n'), 0o644); err != nil {
		return "", "", fmt.Errorf("write portable db manifest: %w", err)
	}
	return manifestPath, sumText, nil
}
