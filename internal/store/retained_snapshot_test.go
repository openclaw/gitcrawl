//go:build darwin || linux

package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func retainedFixture(t *testing.T) (*Store, context.Context, RetainedSnapshotRequest) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	s, err := Open(ctx, filepath.Join(t.TempDir(), "native.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	repo, err := s.UpsertRepository(ctx, Repository{Owner: "fixture", Name: "repo", FullName: "fixture/repo", RawJSON: "{}"})
	if err != nil {
		t.Fatal(err)
	}
	id, err := s.UpsertThread(ctx, Thread{RepoID: repo, GitHubID: "100", Number: 10, Kind: "pull_request", State: "open", Title: "title\x00☃", Body: "full body", RawJSON: "{}", LabelsJSON: "[]", AssigneesJSON: "[]", ContentHash: "one"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.UpsertComment(ctx, Comment{ThreadID: id, GitHubID: "9223372036854775808", CommentType: "pull_review", Body: "full comment\x00☃", RawJSON: `{"state":"APPROVED","submitted_at":"2026-01-03T12:34:56Z"}`}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.ExecContext(ctx, "update threads set evidence_observation_sequence=9223372036854775807; update comments set is_bot=-9223372036854775808"); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	file, err := NativeReportFileIdentity(s.Path())
	if err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(s.Path())
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(body)
	return s, ctx, RetainedSnapshotRequest{NativeReportRequest: NativeReportRequest{Repository: "fixture/repo", File: file, Limits: NativeReportLimits{MaxRows: 100, MaxBytes: 1 << 20}}, SHA256: hex.EncodeToString(hash[:])}
}

func TestRetainedSnapshotNoJournalFullRowsAndMeasurement(t *testing.T) {
	s, ctx, req := retainedFixture(t)
	before, _ := os.ReadFile(s.Path())
	tables := map[string]NativeReportTable{}
	got, err := StreamRetainedSnapshot(ctx, s.Path(), req, func(t NativeReportTable, row []NativeReportValue) error {
		saved := tables[t.Name]
		saved.Rows = append(saved.Rows, row)
		tables[t.Name] = saved
		return nil
	}, func(t NativeReportTable) error {
		saved := tables[t.Name]
		t.Rows = saved.Rows
		tables[t.Name] = t
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Capture.Anchor.Cursor != (NativeReportCursor{}) || got.FileSHA256 != req.SHA256 || len(got.Capture.Tables) != 7 {
		t.Fatal("invented cursor or incomplete tables")
	}
	comments := tables["comments"]
	for column, want := range map[string]string{"github_id": "9223372036854775808", "body": "full comment\x00☃", "submitted_at_gh": "2026-01-03T12:34:56Z"} {
		if v := nativeReportValue(t, comments, 0, column); v.Kind != NativeReportText || v.Text != want {
			t.Fatalf("lost %s", column)
		}
	}
	if nativeReportValue(t, comments, 0, "raw_json_blob_id").Kind != NativeReportNull || nativeReportValue(t, comments, 0, "is_bot").Integer != -9223372036854775808 || nativeReportValue(t, tables["threads"], 0, "evidence_observation_sequence").Integer != 9223372036854775807 {
		t.Fatal("lost null/int64")
	}
	measurement, err := MeasureRetainedSnapshot(ctx, s.Path(), req)
	if err != nil {
		t.Fatal(err)
	}
	if measurement.Rows != got.Capture.Rows || measurement.Bytes != got.Capture.Bytes {
		t.Fatal("measurement differs from stream")
	}
	var count int
	ro, err := OpenReadOnlyImmutable(ctx, s.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	if err = ro.db.QueryRowContext(ctx, "select count(*) from sqlite_master where name like 'change_contract_%'").Scan(&count); err != nil || count != 0 {
		t.Fatal("created journal", count, err)
	}
	after, _ := os.ReadFile(s.Path())
	if !bytes.Equal(before, after) {
		t.Fatal("changed source")
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if _, err = os.Stat(s.Path() + suffix); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("created sidecar", suffix, err)
		}
	}
	if _, err := CaptureNativeReport(ctx, s.Path(), req.NativeReportRequest); !errors.Is(err, ErrNativeReportSource) {
		t.Fatal("live capture must still require journal", err)
	}
}

func TestRetainedSnapshotRejectsDriftLimitsAndUncheckpointedWAL(t *testing.T) {
	for _, kind := range []string{"hash", "limit", "wal", "drift", "cancel"} {
		t.Run(kind, func(t *testing.T) {
			s, ctx, req := retainedFixture(t)
			switch kind {
			case "hash":
				req.SHA256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
			case "limit":
				req.Limits.MaxRows = 1
			case "wal":
				if err := os.WriteFile(s.Path()+"-wal", []byte("pending"), 0600); err != nil {
					t.Fatal(err)
				}
			case "cancel":
				c, cancel := context.WithCancel(ctx)
				cancel()
				ctx = c
			}
			changed := false
			got, err := StreamRetainedSnapshot(ctx, s.Path(), req, func(_ NativeReportTable, _ []NativeReportValue) error {
				if kind == "drift" && !changed {
					changed = true
					f, e := os.OpenFile(s.Path(), os.O_APPEND|os.O_WRONLY, 0)
					if e != nil {
						return e
					}
					_, e = f.Write([]byte{0})
					f.Close()
					return e
				}
				return nil
			}, func(NativeReportTable) error { return nil })
			if err == nil || !reflect.DeepEqual(got, RetainedSnapshot{}) {
				t.Fatal("accepted partial or changed snapshot", err)
			}
		})
	}
}

func TestRetainedSnapshotSchemaPolicyAndScalarRefusals(t *testing.T) {
	for _, test := range []struct {
		name, sql string
		want      error
	}{
		{"schema", "alter table comments add column new_fact text", ErrNativeReportSource},
		{"blob", "PRAGMA ignore_check_constraints=ON; update comments set body=x'0102'", ErrNativeReportSource},
		{"raw-reference", "PRAGMA foreign_keys=OFF; update comments set raw_json_blob_id=99", ErrNativeReportBlob},
		{"excluded-node", `insert into thread_exclusions values('fixture/repo',999,'issue',999,'gone','2026-01-01','owner_requested','plan'); insert into thread_excluded_nodes(node_id,repository,number) values('gone','fixture/repo',999); update comments set raw_json='{"node_id":"gone"}'`, ErrNativeReportSource},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, ctx, req := retainedFixture(t)
			writer, err := Open(ctx, s.Path())
			if err != nil {
				t.Fatal(err)
			}
			if _, err = writer.db.ExecContext(ctx, test.sql); err != nil {
				t.Fatal(err)
			}
			if err = writer.Close(); err != nil {
				t.Fatal(err)
			}
			body, _ := os.ReadFile(s.Path())
			sum := sha256.Sum256(body)
			req.SHA256 = hex.EncodeToString(sum[:])
			_, err = StreamRetainedSnapshot(ctx, s.Path(), req, func(NativeReportTable, []NativeReportValue) error { return nil }, func(NativeReportTable) error { return nil })
			if !errors.Is(err, test.want) {
				t.Fatalf("got %v, want %v", err, test.want)
			}
			if _, err = MeasureRetainedSnapshot(ctx, s.Path(), req); !errors.Is(err, test.want) {
				t.Fatalf("measure got %v, want %v", err, test.want)
			}
		})
	}
}
