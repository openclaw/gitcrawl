package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"
)

// NativeReportMeasurement contains scalar sizes, not payloads, wire sizes or
// an RSS estimate. TextBytes counts stored text bytes, including invalid UTF-8.
type NativeReportMeasurement struct {
	File             NativeReportFile           `json:"file"`
	SourceBytes      int64                      `json:"source_bytes"`
	SourceModifiedAt string                     `json:"source_modified_at"`
	Rows             int64                      `json:"rows"`
	Bytes            int64                      `json:"native_bytes"`
	Tables           []NativeReportTableMeasure `json:"tables"`
}

type NativeReportTableMeasure struct {
	Name            string `json:"name"`
	Rows            int64  `json:"rows"`
	Cells           int64  `json:"cells"`
	NullCells       int64  `json:"null_cells"`
	IntegerCells    int64  `json:"integer_cells"`
	TextCells       int64  `json:"text_cells"`
	TextBytes       int64  `json:"text_bytes"`
	MaxTextBytes    int64  `json:"max_text_bytes"`
	MaxRowBytes     int64  `json:"max_row_bytes"`
	ScalarBytes     int64  `json:"scalar_bytes"`
	ColumnNameBytes int64  `json:"column_name_bytes"`
}

// MeasureNativeReport uses capture's exact admission, scope and scalar cost in
// one read transaction. It does not materialize rows or qualify a seed. The
// caller must keep the source quiescent when using these sizes for admission.
func MeasureNativeReport(ctx context.Context, path string, request NativeReportRequest) (NativeReportMeasurement, error) {
	var result NativeReportMeasurement
	before, err := os.Stat(path)
	if err != nil {
		return result, ErrNativeReportSource
	}
	err = withNativeReportSource(ctx, path, request, func(tx *sql.Tx) error {
		anchor, err := nativeReportAnchor(ctx, tx, request)
		if err != nil {
			return err
		}
		for _, spec := range nativeReportTables {
			table, err := nativeReportTableForRead(ctx, tx, spec, anchor.RepositoryID)
			if err != nil {
				return err
			}
			measure, err := measureNativeReportTable(ctx, tx, spec, table, anchor.RepositoryID)
			if err != nil {
				return fmt.Errorf("measure %s: %w", spec.name, err)
			}
			if measure.Rows > request.Limits.MaxRows-result.Rows || measure.ColumnNameBytes > request.Limits.MaxBytes-result.Bytes {
				return ErrNativeReportLimit
			}
			result.Bytes += measure.ColumnNameBytes
			if measure.ScalarBytes > request.Limits.MaxBytes-result.Bytes {
				return ErrNativeReportLimit
			}
			result.Rows += measure.Rows
			result.Bytes += measure.ScalarBytes
			result.Tables = append(result.Tables, measure)
		}
		return nil
	})
	if err != nil {
		return NativeReportMeasurement{}, err
	}
	after, err := os.Stat(path)
	if err != nil || before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) {
		return NativeReportMeasurement{}, ErrNativeReportSource
	}
	result.File = request.File
	result.SourceBytes = after.Size()
	result.SourceModifiedAt = after.ModTime().UTC().Format(time.RFC3339Nano)
	return result, nil
}

func measureNativeReportTable(ctx context.Context, tx *sql.Tx, spec nativeReportTableSpec, table NativeReportTable, repoID int64) (NativeReportTableMeasure, error) {
	result := NativeReportTableMeasure{Name: spec.name}
	columns, sizes := nativeReportScalarExpressions(table)
	nulls, integers, texts, textBytes, badTypes := []string{}, []string{}, []string{}, []string{}, []string{}
	for i, column := range columns {
		typeof := "typeof(" + column + ")"
		nulls = append(nulls, "("+typeof+"='null')")
		integers = append(integers, "("+typeof+"='integer')")
		texts = append(texts, "("+typeof+"='text')")
		textBytes = append(textBytes, "case when "+typeof+"='text' then length(cast("+column+" as blob)) else 0 end")
		badTypes = append(badTypes, "("+typeof+" not in ('null','integer','text'))")
		result.ColumnNameBytes += int64(len(table.Columns[i]))
	}
	keyNulls := make([]string, len(table.KeyColumns))
	for i, key := range table.KeyColumns {
		keyNulls[i] = "x." + sqliteQuotedIdentifier(key) + " is null"
	}
	sum := func(parts []string) string { return "coalesce(sum(" + strings.Join(parts, "+") + "),0)" }
	query := "select count(*)," + sum(sizes) + "," + sum(nulls) + "," + sum(integers) + "," + sum(texts) + "," + sum(textBytes) +
		",coalesce(max(max(" + strings.Join(textBytes, ",") + ")),0),coalesce(max(" + strings.Join(sizes, "+") + "),0)," + sum(badTypes) +
		",coalesce(sum(" + strings.Join(keyNulls, " or ") + "),0)"
	if spec.name == "comments" {
		query += ",coalesce(sum(x.raw_json_blob_id is not null),0)"
	} else {
		query += ",0"
	}
	var bad, nullKeys, blobs int64
	err := tx.QueryRowContext(ctx, query+" from "+spec.from, repoID).Scan(&result.Rows, &result.ScalarBytes,
		&result.NullCells, &result.IntegerCells, &result.TextCells, &result.TextBytes, &result.MaxTextBytes,
		&result.MaxRowBytes, &bad, &nullKeys, &blobs)
	if err != nil {
		return result, err
	}
	if bad != 0 || nullKeys != 0 {
		return result, ErrNativeReportSource
	}
	if blobs != 0 {
		return result, ErrNativeReportBlob
	}
	result.Cells = result.NullCells + result.IntegerCells + result.TextCells
	return result, nil
}
