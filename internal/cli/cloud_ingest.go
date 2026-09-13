package cli

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	crawlremote "github.com/openclaw/crawlkit/remote"
)

type ingestProgress struct {
	RowsAccepted  int64
	MutationToken string
	Result        crawlremote.IngestResult
}

type gitcrawlIngestBatchSizer struct {
	baseBytes      int64
	finalBaseBytes int64
	rowBytes       int64
	rowCount       int
}

func newGitcrawlIngestBatchSizer(
	app, archive string,
	manifest crawlremote.IngestManifest,
	table string,
	columns []string,
	cursor, mutationToken string,
) (gitcrawlIngestBatchSizer, error) {
	manifest.App = strings.TrimSpace(app)
	manifest.Archive = strings.TrimSpace(archive)
	request := crawlremote.IngestRequest{
		Manifest:      manifest,
		Table:         table,
		Columns:       columns,
		Rows:          [][]any{},
		Cursor:        cursor,
		MutationToken: mutationToken,
	}
	baseBytes, err := encodedGitcrawlIngestRequestBytes(request)
	if err != nil {
		return gitcrawlIngestBatchSizer{}, err
	}
	request.Final = true
	finalBaseBytes, err := encodedGitcrawlIngestRequestBytes(request)
	if err != nil {
		return gitcrawlIngestBatchSizer{}, err
	}
	return gitcrawlIngestBatchSizer{
		baseBytes:      baseBytes,
		finalBaseBytes: finalBaseBytes,
	}, nil
}

func encodedGitcrawlIngestRequestBytes(request crawlremote.IngestRequest) (int64, error) {
	encoded, err := json.Marshal(request)
	if err != nil {
		return 0, fmt.Errorf("encode ingest request envelope: %w", err)
	}
	// crawlremote uses json.Encoder, which appends one newline byte.
	return int64(len(encoded)) + 1, nil
}

func (s *gitcrawlIngestBatchSizer) add(row []any, final bool) (bool, int64, error) {
	encoded, err := json.Marshal(row)
	if err != nil {
		return false, 0, fmt.Errorf("encode ingest row: %w", err)
	}
	separatorBytes := int64(0)
	if s.rowCount > 0 {
		separatorBytes = 1
	}
	baseBytes := s.baseBytes
	if final {
		baseBytes = s.finalBaseBytes
	}
	encodedBytes := baseBytes + s.rowBytes + separatorBytes + int64(len(encoded))
	if encodedBytes > gitcrawlCloudIngestRequestMaxBytes {
		return false, encodedBytes, nil
	}
	s.rowBytes += separatorBytes + int64(len(encoded))
	s.rowCount++
	return true, encodedBytes, nil
}

func (s gitcrawlIngestBatchSizer) encodedBytes(final bool) int64 {
	baseBytes := s.baseBytes
	if final {
		baseBytes = s.finalBaseBytes
	}
	return baseBytes + s.rowBytes
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
	var batchStart int64
	var batchSizer *gitcrawlIngestBatchSizer
	ensureBatchSizer := func() error {
		if batchSizer != nil {
			return nil
		}
		sizer, err := newGitcrawlIngestBatchSizer(
			app,
			archive,
			manifest,
			dataset.Name,
			dataset.Columns,
			cursorFor(batchStart),
			mutationToken,
		)
		if err != nil {
			return err
		}
		batchSizer = &sizer
		return nil
	}
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
			batchStart,
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
		batchStart = scanned
		batchSizer = nil
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
		if err := ensureBatchSizer(); err != nil {
			return ingestProgress{RowsAccepted: accepted, MutationToken: mutationToken}, err
		}
		fits, encodedBytes, err := batchSizer.add(values, false)
		if err != nil {
			return ingestProgress{RowsAccepted: accepted, MutationToken: mutationToken}, err
		}
		if !fits && len(batch) > 0 {
			if err := flush(); err != nil {
				return ingestProgress{RowsAccepted: accepted, MutationToken: mutationToken}, err
			}
			if err := ensureBatchSizer(); err != nil {
				return ingestProgress{RowsAccepted: accepted, MutationToken: mutationToken}, err
			}
			fits, encodedBytes, err = batchSizer.add(values, false)
			if err != nil {
				return ingestProgress{RowsAccepted: accepted, MutationToken: mutationToken}, err
			}
		}
		if !fits {
			return ingestProgress{RowsAccepted: accepted, MutationToken: mutationToken}, fmt.Errorf(
				"dataset %s row %d encoded ingest request is %d bytes, limit %d",
				dataset.Name,
				scanned,
				encodedBytes,
				gitcrawlCloudIngestRequestMaxBytes,
			)
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
	if len(batch) > 0 {
		if err := flush(); err != nil {
			return ingestProgress{RowsAccepted: accepted, MutationToken: mutationToken}, err
		}
	}
	if scanned == 0 {
		if err := ensureBatchSizer(); err != nil {
			return ingestProgress{RowsAccepted: accepted, MutationToken: mutationToken}, err
		}
		if encodedBytes := batchSizer.encodedBytes(false); encodedBytes > gitcrawlCloudIngestRequestMaxBytes {
			return ingestProgress{}, fmt.Errorf(
				"dataset %s empty encoded ingest request is %d bytes, limit %d",
				dataset.Name,
				encodedBytes,
				gitcrawlCloudIngestRequestMaxBytes,
			)
		}
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
		sizer, sizeErr := newGitcrawlIngestBatchSizer(
			app,
			archive,
			manifest,
			table,
			columns,
			"",
			mutationToken,
		)
		if sizeErr != nil {
			return ingestProgress{}, sizeErr
		}
		if encodedBytes := sizer.encodedBytes(final); encodedBytes > gitcrawlCloudIngestRequestMaxBytes {
			return ingestProgress{}, fmt.Errorf(
				"ingest table %s empty encoded request is %d bytes, limit %d",
				table,
				encodedBytes,
				gitcrawlCloudIngestRequestMaxBytes,
			)
		}
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
	for start := 0; start < len(rows); {
		sizer, err := newGitcrawlIngestBatchSizer(
			app,
			archive,
			manifest,
			table,
			columns,
			cursorFor(int64(start)),
			mutationToken,
		)
		if err != nil {
			return ingestProgress{
				RowsAccepted:  total,
				MutationToken: mutationToken,
				Result:        lastResult,
			}, err
		}
		end := start
		for end < len(rows) && end-start < gitcrawlCloudBatchSize {
			fits, encodedBytes, err := sizer.add(rows[end], final && end == len(rows)-1)
			if err != nil {
				return ingestProgress{
					RowsAccepted:  total,
					MutationToken: mutationToken,
					Result:        lastResult,
				}, err
			}
			if !fits {
				if end == start {
					return ingestProgress{
						RowsAccepted:  total,
						MutationToken: mutationToken,
						Result:        lastResult,
					}, fmt.Errorf(
						"ingest table %s row %d encoded request is %d bytes, limit %d",
						table,
						start,
						encodedBytes,
						gitcrawlCloudIngestRequestMaxBytes,
					)
				}
				break
			}
			end++
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
		start = end
	}
	return ingestProgress{
		RowsAccepted:  total,
		MutationToken: mutationToken,
		Result:        lastResult,
	}, nil
}
