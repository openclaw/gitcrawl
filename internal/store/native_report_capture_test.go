//go:build darwin || linux

package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"slices"
	"testing"
	"time"
)

func nativeReportRequest(t *testing.T, s *Store) (context.Context, NativeReportRequest) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	file, err := NativeReportFileIdentity(s.Path())
	if err != nil {
		t.Fatal(err)
	}
	return ctx, NativeReportRequest{Repository: "fixture/repo", File: file, Limits: NativeReportLimits{MaxRows: 10000, MaxBytes: 8 << 20}}
}
func nativeReportTable(t *testing.T, c NativeReportCapture, name string) NativeReportTable {
	t.Helper()
	for _, table := range c.Tables {
		if table.Name == name {
			return table
		}
	}
	t.Fatalf("missing table %s", name)
	return NativeReportTable{}
}
func nativeReportValue(t *testing.T, table NativeReportTable, row int, name string) NativeReportValue {
	t.Helper()
	index := slices.Index(table.Columns, name)
	if index < 0 || row >= len(table.Rows) {
		t.Fatalf("missing %s row %d column %s", table.Name, row, name)
	}
	return table.Rows[row][index]
}
func takeNativeReport(t *testing.T, ctx context.Context, s *Store, req NativeReportRequest) NativeReportCapture {
	t.Helper()
	got, err := CaptureNativeReport(ctx, s.Path(), req)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func TestNativeReportFidelityAndSourceUnchanged(t *testing.T) {
	s := changeContractFixture(t, 0)
	ctx, req := nativeReportRequest(t, s)
	raw := `{"state":"APPROVED","submitted_at":"2026-01-03T12:34:56Z","node_id":"review-one"}`
	body := "native\x00snowman ☃\xff"
	id, err := s.UpsertComment(ctx, Comment{ThreadID: 1, GitHubID: "9223372036854775808", CommentType: "pull_review", Body: body, RawJSON: raw, CreatedAtGitHub: "2026-01-01T00:00:00Z"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.ExecContext(ctx, "update threads set observation_sequence=? where id=1", int64(-9223372036854775807)); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(s.Path())
	if err != nil {
		t.Fatal(err)
	}
	got := takeNativeReport(t, ctx, s, req)
	comments := nativeReportTable(t, got, "comments")
	for name, want := range map[string]string{"github_id": "9223372036854775808", "body": body, "raw_json": raw, "submitted_at_gh": "2026-01-03T12:34:56Z", "publication_at_gh": "2026-01-03T12:34:56Z", "created_at_gh": "2026-01-01T00:00:00Z"} {
		if v := nativeReportValue(t, comments, 0, name); v.Kind != NativeReportText || v.Text != want {
			t.Fatalf("%s lost native text", name)
		}
	}
	if nativeReportValue(t, comments, 0, "id").Integer != id || nativeReportValue(t, comments, 0, "raw_json_blob_id").Kind != NativeReportNull || nativeReportValue(t, comments, 0, "updated_at_gh").Kind != NativeReportNull {
		t.Fatal("lost integer/reference/null")
	}
	if nativeReportValue(t, nativeReportTable(t, got, "threads"), 0, "observation_sequence").Integer != -9223372036854775807 {
		t.Fatal("integer precision lost")
	}
	again := takeNativeReport(t, ctx, s, req)
	if !reflect.DeepEqual(got, again) {
		t.Fatal("unchanged snapshot is not deterministic")
	}
	req.Limits = NativeReportLimits{MaxRows: got.Rows, MaxBytes: got.Bytes}
	takeNativeReport(t, ctx, s, req)
	for _, limits := range []NativeReportLimits{{MaxRows: got.Rows - 1, MaxBytes: got.Bytes}, {MaxRows: got.Rows, MaxBytes: got.Bytes - 1}} {
		req.Limits = limits
		c, err := CaptureNativeReport(ctx, s.Path(), req)
		if !errors.Is(err, ErrNativeReportLimit) || !reflect.DeepEqual(c, NativeReportCapture{}) {
			t.Fatal("bound returned partial success", err)
		}
	}
	after, err := os.ReadFile(s.Path())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("capture mutated source database")
	}
}

func TestNativeReportTableGapsAndTrackedInventoryChanges(t *testing.T) {
	s := changeContractFixture(t, 0)
	ctx, req := nativeReportRequest(t, s)
	initial := takeNativeReport(t, ctx, s, req)
	if initial.Anchor.Cursor.Sequence != 0 || len(initial.Tables) != 7 || len(nativeReportTable(t, initial, "comments").Rows) != 0 {
		t.Fatal("empty tables or genuine H0 lost")
	}
	if _, err := s.UpsertComment(ctx, Comment{ThreadID: 1, GitHubID: "comment", CommentType: "issue_comment", Body: "one", RawJSON: "{}"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, `insert into pull_request_files(thread_id,path,raw_json,fetched_at) values(2,'fixture.go','{}','2026-01-01')`); err != nil {
		t.Fatal(err)
	}
	first := takeNativeReport(t, ctx, s, req)
	if entries := changeContractEntriesAfter(t, s.db, 0); int64(len(entries)) >= first.Anchor.Cursor.Sequence {
		t.Fatal("fixture did not produce legitimate table-mode gaps")
	}
	if _, err := s.ReserveThreadChildObservation(ctx, 1, ThreadChildComments, "", 1); err != nil {
		t.Fatal(err)
	}
	if err := s.ReplaceThreadChildObservationMembers(ctx, 1, ThreadChildComments, 1, []int64{1}); err != nil {
		t.Fatal(err)
	}
	before := takeNativeReport(t, ctx, s, req)
	if err := s.ReplaceThreadChildObservationMembers(ctx, 1, ThreadChildComments, 1, nil); err != nil {
		t.Fatal(err)
	}
	after := takeNativeReport(t, ctx, s, req)
	if after.Anchor.Cursor.Sequence <= before.Anchor.Cursor.Sequence || before.SHA256 == after.SHA256 {
		t.Fatal("tracked membership change lost")
	}
	if _, err := s.UpsertComment(ctx, Comment{ThreadID: 1, GitHubID: "comment", CommentType: "issue_comment", Body: "changed", RawJSON: "{}"}); err != nil {
		t.Fatal(err)
	}
	changed := takeNativeReport(t, ctx, s, req)
	if nativeReportTable(t, changed, "comments").SHA256 == nativeReportTable(t, after, "comments").SHA256 {
		t.Fatal("comment content update lost")
	}
}

func TestNativeReportExclusionsAndLocalAbsence(t *testing.T) {
	s := changeContractFixture(t, 0)
	ctx, req := nativeReportRequest(t, s)
	// Native purge retains identity while cascading child deletion. Keep a later
	// sibling in each native ID table, as required by the purge owner.
	for _, thread := range []int64{1, 2} {
		if _, err := s.UpsertComment(ctx, Comment{ThreadID: thread, GitHubID: fmt.Sprint(thread), CommentType: "issue_comment", Body: "retained", RawJSON: fmt.Sprintf(`{"node_id":"comment-%d"}`, thread)}); err != nil {
			t.Fatal(err)
		}
	}
	before := takeNativeReport(t, ctx, s, req)
	plan, err := s.PlanThreadPurge(ctx, req.Repository, []int{10})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.PurgeThreads(ctx, req.Repository, []int{10}, plan.PlanID); err != nil {
		t.Fatal(err)
	}
	after := takeNativeReport(t, ctx, s, req)
	if len(nativeReportTable(t, before, "threads").Rows) != 2 || len(nativeReportTable(t, after, "threads").Rows) != 1 || len(nativeReportTable(t, after, "comments").Rows) != 1 {
		t.Fatal("incomplete scoped inventory")
	}
	if len(nativeReportTable(t, after, "thread_exclusions").Rows) != 1 || len(nativeReportTable(t, after, "thread_excluded_nodes").Rows) != 1 {
		t.Fatal("lost owner exclusion evidence")
	}
	// Policy changes now advance the same journal as content and change the
	// captured inventory digest, including direct SQLite writers.
	if _, err = s.db.ExecContext(ctx, "insert into thread_excluded_nodes values('later-node','fixture/repo',10)"); err != nil {
		t.Fatal(err)
	}
	policy := takeNativeReport(t, ctx, s, req)
	if policy.Anchor.Cursor.Sequence <= after.Anchor.Cursor.Sequence || policy.SHA256 == after.SHA256 {
		t.Fatal("tracked policy change lost")
	}
	// The native writer retains comments even when their node is excluded;
	// queueAnalyticsActor skips enrichment but does not reject the content.
	if _, err = s.UpsertComment(ctx, Comment{ThreadID: 2, GitHubID: "2", CommentType: "issue_comment", Body: "retained conflict", RawJSON: `{"node_id":"comment-1"}`}); err != nil {
		t.Fatal(err)
	}
	conflict, err := CaptureNativeReport(ctx, s.Path(), req)
	if !errors.Is(err, ErrNativeReportSource) || !reflect.DeepEqual(conflict, NativeReportCapture{}) {
		t.Fatal("retained excluded node did not refuse the entire capture", err)
	}
	// A raw SQL client can retain excluded content despite the ingestion guard.
	// Capture must filter it, not rely on the producer having purged already.
	if _, err = s.db.ExecContext(ctx, `insert into thread_exclusions values('fixture/repo',20,'pull_request',2,'gh20','2026-01-01','owner_requested','fixture')`); err != nil {
		t.Fatal(err)
	}
	filtered := takeNativeReport(t, ctx, s, req)
	if len(nativeReportTable(t, filtered, "threads").Rows) != 0 || len(nativeReportTable(t, filtered, "comments").Rows) != 0 {
		t.Fatal("owner-excluded content leaked")
	}
}

func TestNativeReportRefusesUnsupportedSourceWithoutPartialResult(t *testing.T) {
	for _, test := range []struct {
		name, sql string
		want      error
	}{
		{"blob", `insert into blobs(id,sha256,media_type,size_bytes,storage_kind,created_at) values(1,'hash','application/json',2,'inline','now'); update comments set raw_json_blob_id=1`, ErrNativeReportBlob},
		{"missing publication", `alter table comments drop column submitted_at_gh`, ErrNativeReportSource},
		{"extra column", `alter table comments add column future_evidence text`, ErrNativeReportSource},
		{"unarmed", `drop trigger change_contract_comments_insert`, ErrNativeReportSource},
		{"unarmed membership", `drop trigger change_contract_thread_child_observation_memberships_update`, ErrNativeReportSource},
		{"unarmed global nodes", `drop trigger change_contract_thread_excluded_nodes_delete`, ErrNativeReportSource},
		{"disabled", `drop table change_contract_state`, ErrNativeReportSource},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := changeContractFixture(t, 0)
			ctx, req := nativeReportRequest(t, s)
			if test.name == "blob" {
				if _, err := s.UpsertComment(ctx, Comment{ThreadID: 1, GitHubID: "one", CommentType: "issue_comment", RawJSON: "{}"}); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := s.db.ExecContext(ctx, test.sql); err != nil {
				t.Fatal(err)
			}
			c, err := CaptureNativeReport(ctx, s.Path(), req)
			if !errors.Is(err, test.want) || !reflect.DeepEqual(c, NativeReportCapture{}) {
				t.Fatal("unsupported source accepted or partial result returned", err)
			}
		})
	}
	s := changeContractFixture(t, 0)
	ctx, req := nativeReportRequest(t, s)
	for _, bad := range []NativeReportRequest{{Repository: "other/repo", File: req.File, Limits: req.Limits}, {Repository: req.Repository, Limits: req.Limits}} {
		if _, err := CaptureNativeReport(ctx, s.Path(), bad); !errors.Is(err, ErrNativeReportSource) {
			t.Fatal("scope/file mismatch accepted", err)
		}
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := CaptureNativeReport(canceled, s.Path(), req); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation ignored", err)
	}
	if _, err := CaptureNativeReport(context.Background(), s.Path(), req); err == nil {
		t.Fatal("unbounded deadline accepted")
	}
}

func TestNativeReportRequiresCurrentJournalCoverage(t *testing.T) {
	for _, omitNew := range []bool{true, false} {
		s := changeContractFixture(t, 0)
		ctx, req := nativeReportRequest(t, s)
		installLegacyReportTracking(t, s, omitNew)
		// This is a fresh snapshot request, not rejection of an old schema cookie.
		// Even armed legacy comment triggers cannot satisfy the row-key contract.
		if got, err := CaptureNativeReport(ctx, s.Path(), req); !errors.Is(err, ErrNativeReportSource) || !reflect.DeepEqual(got, NativeReportCapture{}) {
			t.Fatal("legacy row coverage accepted", err)
		}
		if _, err := s.EnableChangeContract(ctx, ChangeContractEnableOptions{}); err != nil {
			t.Fatal(err)
		}
		takeNativeReport(t, ctx, s, req)
	}
}

func TestNativeReportSnapshotWithConcurrentWriter(t *testing.T) {
	s := changeContractFixture(t, 0)
	ctx, req := nativeReportRequest(t, s)
	if _, err := s.UpsertComment(ctx, Comment{ThreadID: 1, GitHubID: "one", CommentType: "issue_comment", Body: "generation-0", RawJSON: "{}"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, "update threads set title='generation-0' where id=1"); err != nil {
		t.Fatal(err)
	}
	writerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		for i := 1; ; i++ {
			select {
			case <-stop:
				done <- nil
				return
			default:
			}
			err := s.WithTx(writerCtx, func(tx *Store) error {
				text := fmt.Sprintf("generation-%d", i)
				if _, err := tx.q().ExecContext(writerCtx, "update threads set title=? where id=1", text); err != nil {
					return err
				}
				_, err := tx.UpsertComment(writerCtx, Comment{ThreadID: 1, GitHubID: "one", CommentType: "issue_comment", Body: text, RawJSON: "{}"})
				return err
			})
			if err != nil {
				done <- err
				return
			}
		}
	}()
	defer func() {
		// Finish the in-flight commit before canceling its context.
		close(stop)
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	}()
	for i := 0; i < 8; i++ {
		got := takeNativeReport(t, ctx, s, req)
		title := nativeReportValue(t, nativeReportTable(t, got, "threads"), 0, "title")
		body := nativeReportValue(t, nativeReportTable(t, got, "comments"), 0, "body")
		if title.Text != body.Text {
			t.Fatal("capture mixed native generations")
		}
	}
}
