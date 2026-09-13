package cli

import (
	"context"
	"errors"
	"fmt"

	crawlremote "github.com/openclaw/crawlkit/remote"
)

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
