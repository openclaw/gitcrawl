//go:build darwin || linux

package store

import (
	"bytes"
	"errors"
	"os"
	"reflect"
	"testing"
	"time"
)

func checkNativeReportMeasurement(t *testing.T, s *Store) NativeReportMeasurement {
	t.Helper()
	ctx, req := nativeReportRequest(t, s)
	capture := takeNativeReport(t, ctx, s, req)
	got, err := MeasureNativeReport(ctx, s.Path(), req)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(s.Path())
	if err != nil {
		t.Fatal(err)
	}
	want := NativeReportMeasurement{Rows: capture.Rows, Bytes: capture.Bytes, File: req.File, SourceBytes: info.Size(), SourceModifiedAt: info.ModTime().UTC().Format(time.RFC3339Nano)}
	for _, table := range capture.Tables {
		m := NativeReportTableMeasure{Name: table.Name, Rows: int64(len(table.Rows))}
		for _, name := range table.Columns {
			m.ColumnNameBytes += int64(len(name))
		}
		for _, row := range table.Rows {
			var size int64
			for _, cell := range row {
				m.Cells++
				switch cell.Kind {
				case NativeReportNull:
					m.NullCells++
					size++
				case NativeReportInteger:
					m.IntegerCells++
					size += 9
				case NativeReportText:
					m.TextCells++
					n := int64(len(cell.Text))
					m.TextBytes += n
					m.MaxTextBytes = max(m.MaxTextBytes, n)
					size += 9 + n
				}
			}
			m.ScalarBytes += size
			m.MaxRowBytes = max(m.MaxRowBytes, size)
		}
		want.Tables = append(want.Tables, m)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("aggregate differs from full typed capture:\ngot %+v\nwant %+v", got, want)
	}
	return got
}

func TestNativeReportMeasurementMatchesCaptureWithoutChangingSource(t *testing.T) {
	s := changeContractFixture(t, 0)
	ctx, _ := nativeReportRequest(t, s)
	if _, err := s.UpsertComment(ctx, Comment{ThreadID: 1, GitHubID: "one", CommentType: "issue_comment", Body: "native\x00☃\xff", RawJSON: "{}"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(s.Path())
	if err != nil {
		t.Fatal(err)
	}
	got := checkNativeReportMeasurement(t, s)
	if len(got.Tables) != 7 || got.Tables[2].Rows != 1 || got.Tables[5].Rows != 0 || got.Tables[6].Rows != 0 {
		t.Fatal("missing selected or empty policy tables")
	}
	after, err := os.ReadFile(s.Path())
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("measurement changed native source", err)
	}
}

func TestNativeReportMeasurementRejectsUnsupportedScalars(t *testing.T) {
	for _, statement := range []string{
		"update comments set body=x'0102'",
		"update threads set observation_sequence=1.5",
	} {
		t.Run(statement, func(t *testing.T) {
			s := changeContractFixture(t, 0)
			ctx, req := nativeReportRequest(t, s)
			if _, err := s.UpsertComment(ctx, Comment{ThreadID: 1, GitHubID: "one", CommentType: "issue_comment", RawJSON: "{}"}); err != nil {
				t.Fatal(err)
			}
			if _, err := s.db.ExecContext(ctx, "PRAGMA ignore_check_constraints=ON"); err != nil {
				t.Fatal(err)
			}
			if _, err := s.db.ExecContext(ctx, statement); err != nil {
				t.Fatal(err)
			}
			if _, err := CaptureNativeReport(ctx, s.Path(), req); !errors.Is(err, ErrNativeReportSource) {
				t.Fatal("fixture did not violate capture's scalar contract", err)
			}
			got, err := MeasureNativeReport(ctx, s.Path(), req)
			if !errors.Is(err, ErrNativeReportSource) || !reflect.DeepEqual(got, NativeReportMeasurement{}) {
				t.Fatal("invalid scalar accepted or partial measurement returned", err)
			}
		})
	}
}

func TestNativeReportMeasurementRejectsNullKey(t *testing.T) {
	s := changeContractFixture(t, 0)
	ctx, req := nativeReportRequest(t, s)
	if _, err := s.db.ExecContext(ctx, `insert into thread_exclusions values('fixture/repo',99,'issue',99,'gh99','2026-01-01','owner_requested','fixture');
 insert into thread_excluded_nodes values(NULL,'fixture/repo',99)`); err != nil {
		t.Fatal(err)
	}
	if _, err := CaptureNativeReport(ctx, s.Path(), req); !errors.Is(err, ErrNativeReportSource) {
		t.Fatal("fixture must fail capture", err)
	}
	got, err := MeasureNativeReport(ctx, s.Path(), req)
	if !errors.Is(err, ErrNativeReportSource) || !reflect.DeepEqual(got, NativeReportMeasurement{}) {
		t.Fatal("null key accepted", err)
	}
}
