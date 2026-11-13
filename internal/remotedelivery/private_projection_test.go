//go:build darwin || linux

package remotedelivery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/openclaw/gitcrawl/internal/store"
)

func nativeFixture(t *testing.T) (context.Context, *store.Store) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "native.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	repo, err := s.UpsertRepository(ctx, store.Repository{Owner: "fixture", Name: "repo", FullName: "fixture/repo", GitHubRepoID: "9223372036854775808", RawJSON: `{"private":"repository"}`, UpdatedAt: "2026-01-01"})
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct {
		id     string
		number int
	}{{"one", 10}, {"two", 20}} {
		_, err := s.UpsertThread(ctx, store.Thread{RepoID: repo, GitHubID: item.id, Number: item.number, Kind: "pull_request", State: "open", Title: "retained", Body: "body", HTMLURL: "https://github.com/fixture/repo/pull/10", LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: item.id, UpdatedAt: "2026-01-01"})
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err = s.EnableChangeContract(ctx, store.ChangeContractEnableOptions{}); err != nil {
		t.Fatal(err)
	}
	return ctx, s
}

func nativeCapture(ctx context.Context, s *store.Store) (store.NativeReportCapture, error) {
	file, err := store.NativeReportFileIdentity(s.Path())
	if err != nil {
		return store.NativeReportCapture{}, err
	}
	return store.CaptureNativeReport(ctx, s.Path(), store.NativeReportRequest{Repository: "fixture/repo", File: file,
		Limits: store.NativeReportLimits{MaxRows: 10000, MaxBytes: 8 << 20}})
}

func projected(t *testing.T, ctx context.Context, s *store.Store) (store.NativeReportCapture, PrivateReport) {
	t.Helper()
	capture, err := nativeCapture(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	projection, err := ProjectPrivateReport(capture)
	if err != nil {
		t.Fatal(err)
	}
	return capture, projection
}

func value(t *testing.T, report PrivateReport, table string, row int, column string) PrivateValue {
	t.Helper()
	for _, item := range report.Tables {
		if item.Name == table {
			index := slices.Index(item.Columns, column)
			if index < 0 || row >= len(item.Rows) {
				t.Fatal("missing projected cell")
			}
			return item.Rows[row][index]
		}
	}
	t.Fatal("missing projected table")
	return PrivateValue{}
}

func TestPrivateReportNativeFidelity(t *testing.T) {
	ctx, s := nativeFixture(t)
	raw := `{"state":"APPROVED","submitted_at":"2026-01-03T12:34:56.123456789Z","node_id":"review-one","private":"raw\u0000snow☃"}`
	body := "full private\x00text ☃"
	if _, err := s.UpsertComment(ctx, store.Comment{ThreadID: 1, GitHubID: "9223372036854775808", CommentType: "pull_review", Body: body, RawJSON: raw, CreatedAtGitHub: "2026-01-01T00:00:00Z"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB().ExecContext(ctx, `update threads set title='',body=NULL,observation_sequence=-9223372036854775807,evidence_observation_sequence=9223372036854775807 where id=1; update comments set is_bot=-9223372036854775808`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReserveThreadChildObservation(ctx, 1, store.ThreadChildComments, "2026-01-03T12:34:56.123456789Z", 1); err != nil {
		t.Fatal(err)
	}
	if err := s.ReplaceThreadChildObservationMembers(ctx, 1, store.ThreadChildComments, 1, []int64{1}); err != nil {
		t.Fatal(err)
	}
	capture, got := projected(t, ctx, s)
	for _, check := range []struct{ table, column, kind, text string }{
		{"repositories", "github_repo_id", "text", "9223372036854775808"},
		{"threads", "title", "text", ""},
		{"threads", "observation_sequence", "integer", "-9223372036854775807"},
		{"threads", "evidence_observation_sequence", "integer", "9223372036854775807"},
		{"comments", "body", "text", body}, {"comments", "raw_json", "text", raw},
		{"comments", "is_bot", "integer", "-9223372036854775808"},
		{"comments", "submitted_at_gh", "text", "2026-01-03T12:34:56.123456789Z"},
		{"comments", "publication_at_gh", "text", "2026-01-03T12:34:56.123456789Z"},
		{"comments", "created_at_gh", "text", "2026-01-01T00:00:00Z"},
		{"thread_child_observation_memberships", "member_ids_json", "text", "[1]"},
	} {
		cell := value(t, got, check.table, 0, check.column)
		if cell.Kind != check.kind || cell.Value == nil || *cell.Value != check.text {
			t.Fatalf("lost %s.%s scalar fidelity", check.table, check.column)
		}
	}
	for _, check := range [][2]string{{"threads", "body"}, {"comments", "updated_at_gh"}, {"comments", "raw_json_blob_id"}} {
		if cell := value(t, got, check[0], 0, check[1]); cell.Kind != "null" || cell.Value != nil {
			t.Fatal("NULL collapsed")
		}
	}
	if got.Profile != PrivateReportProfile || got.SHA256 != capture.SHA256 || got.ScopeSHA256 != capture.Anchor.ScopeSHA256 || got.Cursor.Epoch != capture.Anchor.Cursor.Epoch {
		t.Fatal("logical provenance changed")
	}
	// Physical custody never enters the projected object or its JSON shape.
	capture.Anchor.File = store.NativeReportFile{Platform: "PRIVATE_LOCAL_FILE", Device: "PRIVATE_DEVICE", Inode: "PRIVATE_INODE"}
	again, err := ProjectPrivateReport(capture)
	if err != nil || !reflect.DeepEqual(got, again) {
		t.Fatal("physical identity affected projection", err)
	}
	encoded, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(encoded, []byte("PRIVATE_LOCAL")) || bytes.Contains(encoded, []byte("PRIVATE_DEVICE")) || bytes.Contains(encoded, []byte("PRIVATE_INODE")) {
		t.Fatal("physical identity leaked")
	}
	var decoded PrivateReport
	if err = json.Unmarshal(encoded, &decoded); err != nil || !reflect.DeepEqual(got, decoded) {
		t.Fatal("JSON lost private scalar fidelity", err)
	}
}

func TestPrivateReportEmptyLayoutsAndUnsupportedCapture(t *testing.T) {
	ctx, s := nativeFixture(t)
	capture, got := projected(t, ctx, s)
	if got.Cursor.Sequence != "0" || len(got.Tables) != 7 || len(got.Tables[2].Rows) != 0 || got.Tables[2].Rows == nil {
		t.Fatal("H0 or empty layout lost")
	}
	for _, mutate := range []func(*store.NativeReportCapture){
		func(c *store.NativeReportCapture) { c.Tables = c.Tables[:6] },
		func(c *store.NativeReportCapture) { c.Tables[2].Columns = c.Tables[2].Columns[:1] },
		func(c *store.NativeReportCapture) { c.Tables[2].KeyColumns = []string{"github_id"} },
		func(c *store.NativeReportCapture) { c.Tables[2].Name = "future_table" },
		func(c *store.NativeReportCapture) { c.Tables[0].Rows[0] = c.Tables[0].Rows[0][:1] },
		func(c *store.NativeReportCapture) {
			c.Tables[0].Rows[0][0] = store.NativeReportValue{Kind: store.NativeReportText, Text: "1"}
		},
		func(c *store.NativeReportCapture) { c.Tables[0].Rows[0][0] = store.NativeReportValue{} },
		func(c *store.NativeReportCapture) { c.Tables[0].Rows[0][1].Text = "PRIVATE_INVALID\xff" },
		func(c *store.NativeReportCapture) { c.Rows++ },
	} {
		// Copy a valid capture, then introduce invalid text before projection; never
		// rely on encoding/json's replacement of malformed native strings.
		encoded, err := json.Marshal(capture)
		if err != nil {
			t.Fatal(err)
		}
		var bad store.NativeReportCapture
		if err = json.Unmarshal(encoded, &bad); err != nil {
			t.Fatal(err)
		}
		mutate(&bad)
		result, err := ProjectPrivateReport(bad)
		if !errors.Is(err, ErrPrivateProjection) || !reflect.DeepEqual(result, PrivateReport{}) || strings.Contains(err.Error(), "PRIVATE_INVALID") {
			t.Fatal("unsupported capture exposed a result or private diagnostic", err)
		}
	}
	if result, err := ProjectPrivateReport(store.NativeReportCapture{}); !errors.Is(err, ErrPrivateProjection) || !reflect.DeepEqual(result, PrivateReport{}) {
		t.Fatal("failed capture became a report")
	}
	if _, err := s.UpsertComment(ctx, store.Comment{ThreadID: 1, GitHubID: "bad-utf8", CommentType: "issue_comment", Body: "private\xff", RawJSON: "{}"}); err != nil {
		t.Fatal(err)
	}
	bad, err := nativeCapture(ctx, s)
	if err != nil {
		t.Fatal("native owner must preserve invalid UTF-8 bytes", err)
	}
	if result, err := ProjectPrivateReport(bad); !errors.Is(err, ErrPrivateProjection) || !reflect.DeepEqual(result, PrivateReport{}) {
		t.Fatal("invalid native UTF-8 was replaced")
	}
}

func TestPrivateReportPreservesNativeExclusionAndBlobHolds(t *testing.T) {
	ctx, s := nativeFixture(t)
	for _, thread := range []int64{1, 2} {
		if _, err := s.UpsertComment(ctx, store.Comment{ThreadID: thread, GitHubID: "comment", CommentType: "issue_comment", Body: "private", RawJSON: `{"node_id":"node-` + strconv.FormatInt(thread, 10) + `"}`}); err != nil {
			t.Fatal(err)
		}
	}
	plan, err := s.PlanThreadPurge(ctx, "fixture/repo", []int{10})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.PurgeThreads(ctx, "fixture/repo", []int{10}, plan.PlanID); err != nil {
		t.Fatal(err)
	}
	_, report := projected(t, ctx, s)
	if len(report.Tables[1].Rows) != 1 || len(report.Tables[2].Rows) != 1 || len(report.Tables[5].Rows) != 1 || len(report.Tables[6].Rows) != 1 {
		t.Fatal("exclusion inventory or retained content changed")
	}
	if _, err = s.UpsertComment(ctx, store.Comment{ThreadID: 2, GitHubID: "comment", CommentType: "issue_comment", Body: "held", RawJSON: `{"node_id":"node-1"}`}); err != nil {
		t.Fatal(err)
	}
	if capture, err := nativeCapture(ctx, s); !errors.Is(err, store.ErrNativeReportSource) || !reflect.DeepEqual(capture, store.NativeReportCapture{}) {
		t.Fatal("excluded node no longer holds capture", err)
	}
	if _, err = s.UpsertComment(ctx, store.Comment{ThreadID: 2, GitHubID: "comment", CommentType: "issue_comment", Body: "retained", RawJSON: "{}"}); err != nil {
		t.Fatal(err)
	}
	capture, _ := projected(t, ctx, s)
	index := slices.Index(capture.Tables[2].Columns, "raw_json_blob_id")
	capture.Tables[2].Rows[0][index] = store.NativeReportValue{Kind: store.NativeReportInteger, Integer: 1}
	if got, err := ProjectPrivateReport(capture); !errors.Is(err, store.ErrNativeReportBlob) || !reflect.DeepEqual(got, PrivateReport{}) {
		t.Fatal("blob reference was silently projected", err)
	}
	if _, err = s.DB().ExecContext(ctx, `insert into blobs(id,sha256,media_type,size_bytes,storage_kind,created_at) values(1,'hash','application/json',2,'inline','now'); update comments set raw_json_blob_id=1`); err != nil {
		t.Fatal(err)
	}
	if capture, err := nativeCapture(ctx, s); !errors.Is(err, store.ErrNativeReportBlob) || !reflect.DeepEqual(capture, store.NativeReportCapture{}) {
		t.Fatal("native blob hold changed", err)
	}
}
