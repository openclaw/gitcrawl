package portable

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestExportRecentClosureAndSourcePreservation(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	source := filepath.Join(dir, "source.db")
	st := seedExportSource(t, ctx, source)
	defer st.Close()
	// The existing fixture covers every retained child family on open thread 1.
	// Thread 2 is provably old; its child rows must disappear with it.
	_, err := st.DB().Exec(`insert into threads(id,repo_id,github_id,number,kind,state,title,html_url,labels_json,assignees_json,raw_json,content_hash,created_at_gh,updated_at_gh,updated_at)
		values(2,1,'old',8,'issue','closed','old','','[]','[]','{}','old','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','2027-01-01T00:00:00Z');
		insert into comments(id,thread_id,github_id,comment_type,body,raw_json,created_at_gh) values(2,2,'old','issue_comment','old child','{}','2026-01-01T00:00:00Z');
		insert into thread_child_observation_reservations(thread_id,family,source_updated_at,observation_sequence) values(2,'comments','2026-01-01T00:00:00Z',7);
		insert into thread_child_observation_memberships(thread_id,family,observation_sequence,member_ids_json) values(2,'comments',7,'["old"]');`)
	if err != nil {
		t.Fatal(err)
	}
	before, wal := readFile(t, source), readFile(t, source+"-wal")
	options := testExportOptions(source, filepath.Join(dir, "recent"))
	options.Since = "2026-11-01T01:00:00+01:00"
	clock := time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC)
	result, err := (exporter{now: func() time.Time { return clock }}).export(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	manifest := readManifest(t, result.ManifestPath)
	if manifest.Subset == nil || manifest.Subset.Since != "2026-11-01T00:00:00Z" || manifest.Subset.CapturedAt != clock.Format(time.RFC3339Nano) {
		t.Fatalf("subset: %+v", manifest.Subset)
	}
	db := openRawDB(t, result.DatabasePath)
	defer db.Close()
	for _, table := range []string{"threads", "comments", "thread_revisions", "thread_fingerprints", "pull_request_details", "pull_request_files", "pull_request_commits", "pull_request_checks", "pull_request_review_threads", "pull_request_review_thread_revisions", "pull_request_review_thread_syncs", "thread_child_observation_memberships", "thread_child_observation_reservations", "github_workflow_runs", "workflow_run_observation_reservations"} {
		if n := rowCount(t, db, table); n != 1 {
			t.Errorf("%s retained=%d want 1", table, n)
		}
	}
	var membership string
	if err := db.QueryRow(`select member_ids_json from thread_child_observation_memberships`).Scan(&membership); err != nil || membership != `["C1"]` {
		t.Fatalf("membership %q %v", membership, err)
	}
	var allocator int64
	if err := db.QueryRow(`select value from thread_observation_sequence where id=1`).Scan(&allocator); err != nil || allocator != 7 {
		t.Fatalf("allocator %d %v", allocator, err)
	}
	if rowCount(t, db, "repo_sync_state") != 0 || tableExists(t, db, "sync_runs") {
		t.Fatal("subset retained repository acquisition checkpoints")
	}
	if n, err := testForeignKeyViolationCount(ctx, db); err != nil || n != 0 {
		t.Fatalf("FK %d %v", n, err)
	}
	if !bytes.Equal(before, readFile(t, source)) || !bytes.Equal(wal, readFile(t, source+"-wal")) {
		t.Fatal("recent export mutated source or WAL")
	}
	// Capture time is manifest-only: an unchanged later export is a semantic no-op.
	options.OutputDir = filepath.Join(dir, "again")
	again, err := (exporter{now: func() time.Time { return clock.Add(time.Hour) }}).export(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	if again.ArtifactID != result.ArtifactID || again.SHA256 != result.SHA256 {
		t.Fatal("capture time changed artifact identity")
	}
	// Scope claims cannot be removed or changed while retaining the database.
	for _, altered := range []Manifest{func() Manifest { m := manifest; m.Subset = nil; return m }(), func() Manifest {
		m := manifest
		subset := *m.Subset
		subset.Since = "2020-01-01T00:00:00Z"
		m.Subset = &subset
		return m
	}()} {
		data, err := json.Marshal(altered)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, "tampered.json")
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		if err := validateManifestPair(ctx, result.DatabasePath, path, altered); err == nil {
			t.Fatal("accepted altered subset claim")
		}
	}
	for _, since := range []string{"", "2020-01-01T00:00:00Z", "2026-12-01T00:00:00Z"} {
		next := testExportOptions(result.DatabasePath, filepath.Join(t.TempDir(), "reexport"))
		next.Since = since
		if _, err := Export(ctx, next); err == nil || !strings.Contains(err.Error(), "re-export") {
			t.Fatalf("inherited subset: %v", err)
		}
		if _, err := os.Stat(next.OutputDir); !os.IsNotExist(err) {
			t.Fatal("failed re-export published output")
		}
	}
	consume := testExportOptions(result.DatabasePath, filepath.Join(dir, "consume-subset"))
	consume.ConsumeSource = true
	if _, err := Export(ctx, consume); err == nil || !strings.Contains(err.Error(), "re-export") {
		t.Fatalf("consuming subset refusal: %v", err)
	}
	if _, err := os.Stat(result.DatabasePath); err != nil {
		t.Fatalf("subset was consumed before refusal: %v", err)
	}
	// A smaller final byte budget must fail atomically, never trim extra children.
	limit := result.BytesAfter - 1
	options.MaxBytes = &limit
	options.OutputDir = filepath.Join(dir, "over-budget")
	if _, err := Export(ctx, options); err == nil || !strings.Contains(err.Error(), "exceeds max-bytes") {
		t.Fatalf("budget: %v", err)
	}
	if _, err := os.Stat(options.OutputDir); !os.IsNotExist(err) {
		t.Fatal("budget failure published output")
	}
}

func TestExportRecentRefusesExclusionsBeforeRepositoryScope(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	source := filepath.Join(dir, "source.db")
	st := seedExportSource(t, ctx, source)
	defer st.Close()
	_, err := st.DB().Exec(`insert into repositories(id,owner,name,full_name,raw_json,updated_at) values(2,'example','other','example/other','{}','old');
		insert into thread_exclusions(repository,number,kind,original_thread_id,github_id,excluded_at,reason,plan_id)
		values('example/other',1,'issue',99,'excluded','2026-01-01T00:00:00Z','owner_requested','fixture');`)
	if err != nil {
		t.Fatal(err)
	}
	before, wal := readFile(t, source), readFile(t, source+"-wal")
	options := testExportOptions(source, filepath.Join(dir, "blocked"))
	options.Since = "2026-11-01T00:00:00Z"
	options.Repository = "openclaw/gitcrawl"
	if _, err := Export(ctx, options); err == nil || !strings.Contains(err.Error(), "local owner exclusions") {
		t.Fatalf("exclusions: %v", err)
	}
	if !bytes.Equal(before, readFile(t, source)) || !bytes.Equal(wal, readFile(t, source+"-wal")) {
		t.Fatal("refusal mutated source")
	}
	if _, err := os.Stat(options.OutputDir); !os.IsNotExist(err) {
		t.Fatal("refusal published output")
	}
}

func TestExportRecentRejectsUnsafeOptionsBeforeMutation(t *testing.T) {
	for _, tc := range []struct {
		since   string
		consume bool
		want    string
	}{
		{"2026-11-01T00:00:00Z", true, "copy-only"}, {"yesterday", false, "RFC3339"},
	} {
		dir := t.TempDir()
		source := filepath.Join(dir, "not-a-database")
		if err := os.WriteFile(source, []byte("unchanged"), 0600); err != nil {
			t.Fatal(err)
		}
		options := testExportOptions(source, filepath.Join(dir, "missing", "artifact"))
		options.Since = tc.since
		options.ConsumeSource = tc.consume
		if _, err := Export(context.Background(), options); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("invalid options: %v", err)
		}
		if string(readFile(t, source)) != "unchanged" {
			t.Fatal("source changed")
		}
		if _, err := os.Stat(filepath.Dir(options.OutputDir)); !os.IsNotExist(err) {
			t.Fatal("invalid options created staging parent")
		}
	}
}
