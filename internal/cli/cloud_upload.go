package cli

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	crawlremote "github.com/openclaw/crawlkit/remote"
)

func uploadSQLiteArchive(ctx context.Context, client *crawlremote.Client, app, archive string, db *sql.DB, dbPath string, manifest crawlremote.IngestManifest, counts map[string]int64) (*crawlremote.SQLiteBundle, error) {
	snapshotPath, cleanup, err := cloudSQLiteSnapshotPath(ctx, db, dbPath)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	snapshotID, err := cloudFileSHA256(snapshotPath)
	if err != nil {
		return nil, err
	}
	bundle, _, err := uploadSQLiteSnapshotArchive(
		ctx,
		client,
		app,
		archive,
		snapshotPath,
		snapshotID,
		counts,
	)
	return bundle, err
}

func uploadSQLiteSnapshotArchive(
	ctx context.Context,
	client *crawlremote.Client,
	app, archive, snapshotPath, expectedSnapshotID string,
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
	expectedSnapshotID = strings.TrimSpace(expectedSnapshotID)
	if expectedSnapshotID == "" {
		return nil, 0, fmt.Errorf("selected SQLite snapshot digest is required")
	}
	if bundle.Manifest.SnapshotID != expectedSnapshotID ||
		bundle.Manifest.Object.SHA256 != expectedSnapshotID {
		return nil, 0, fmt.Errorf(
			"SQLite bundle source digest changed from selected snapshot %s to snapshot %s with object digest %s",
			expectedSnapshotID,
			bundle.Manifest.SnapshotID,
			bundle.Manifest.Object.SHA256,
		)
	}
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
