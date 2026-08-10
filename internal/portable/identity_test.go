package portable

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"
)

func TestSemanticArtifactIdentityIgnoresExplicitLocalBookkeeping(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	st := seedExportSource(t, ctx, sourcePath)
	defer st.Close()
	baseline, err := Export(ctx, testExportOptions(sourcePath, filepath.Join(dir, "baseline")))
	if err != nil {
		t.Fatalf("export baseline: %v", err)
	}
	baselineDB := openRawDB(t, baseline.DatabasePath)
	if _, err := baselineDB.ExecContext(ctx, `
		update threads set closed_at_local = '2026-08-08T00:00:00Z', close_reason_local = 'not returned by source';
		insert into comments(id, thread_id, github_id, comment_type, body, raw_json, deleted_at, deletion_reason)
		values(2, 1, 'C2', 'issue_comment', 'tombstoned comment', '', '2026-08-08T00:00:00Z', 'not returned by source');
	`); err != nil {
		t.Fatalf("seed local timestamp presence: %v", err)
	}
	if err := baselineDB.Close(); err != nil {
		t.Fatal(err)
	}
	baselineExactSHA := hashFile(t, baseline.DatabasePath)
	baselineArtifactID, err := ComputeArtifactID(ctx, baseline.DatabasePath, CurrentStateSemanticV1)
	if err != nil {
		t.Fatalf("compute local policy baseline: %v", err)
	}

	mutations := []struct {
		name string
		sql  string
	}{
		{name: "observation convergence", sql: `update observation_schema_convergence set checked_observation_sequence = 42`},
		{name: "pipeline state", sql: `create table repo_pipeline_state(repo_id integer, phase text, updated_at text); insert into repo_pipeline_state values(1, 'sync', 'later')`},
		{name: "repository sync state", sql: `update repo_sync_state set updated_at = 'later'`},
		{name: "observation allocator", sql: `update thread_observation_sequence set value = 42, last_started_at = 'later'`},
		{name: "child reservation", sql: `update thread_child_observation_reservations set source_updated_at = 'later', observation_sequence = 42`},
		{name: "workflow reservation", sql: `update workflow_run_observation_reservations set source_updated_at = 'later', observation_sequence = 42`},
		{name: "review sync marker", sql: `update pull_request_review_thread_syncs set fetched_at = 'later'`},
		{name: "SQLite planner statistics", sql: `analyze`},
		{name: "repository updated at", sql: `update repositories set updated_at = 'later'`},
		{name: "thread first pulled", sql: `update threads set first_pulled_at = 'later'`},
		{name: "thread last pulled", sql: `update threads set last_pulled_at = 'later'`},
		{name: "thread updated at", sql: `update threads set updated_at = 'later'`},
		{name: "thread observation sequence", sql: `update threads set observation_sequence = 42`},
		{name: "thread evidence sequence", sql: `update threads set evidence_observation_sequence = 42`},
		{name: "thread evidence source time", sql: `update threads set evidence_source_updated_at = 'later'`},
		{name: "thread local closure time", sql: `update threads set closed_at_local = 'later'`},
		{name: "comment tombstone time", sql: `update comments set deleted_at = 'later' where id = 2`},
		{name: "revision observation sequence", sql: `update thread_revisions set observation_sequence = 42`},
		{name: "revision record time", sql: `update thread_revisions set created_at = 'later'`},
		{name: "membership observation sequence", sql: `update thread_child_observation_memberships set observation_sequence = 42`},
		{name: "fingerprint record time", sql: `update thread_fingerprints set created_at = 'later'`},
		{name: "PR detail fetch time", sql: `update pull_request_details set fetched_at = 'later'`},
		{name: "PR detail local update time", sql: `update pull_request_details set updated_at = 'later'`},
		{name: "PR file fetch time", sql: `update pull_request_files set fetched_at = 'later'`},
		{name: "PR commit fetch time", sql: `update pull_request_commits set fetched_at = 'later'`},
		{name: "PR commit tombstone time", sql: `update pull_request_commits set deleted_at = 'later'`},
		{name: "PR check fetch time", sql: `update pull_request_checks set fetched_at = 'later'`},
		{name: "review thread fetch time", sql: `update pull_request_review_threads set fetched_at = 'later'`},
		{name: "review thread tombstone time", sql: `update pull_request_review_threads set deleted_at = 'later'`},
		{name: "review revision fetch time", sql: `update pull_request_review_thread_revisions set fetched_at = 'later'`},
		{name: "review revision record time", sql: `update pull_request_review_thread_revisions set recorded_at = 'later'`},
		{name: "review revision tombstone time", sql: `update pull_request_review_thread_revisions set deleted_at = 'later'`},
		{name: "workflow fetch time", sql: `update github_workflow_runs set fetched_at = 'later'`},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			mutatedPath := copyIdentityTestDatabase(t, baseline.DatabasePath, filepath.Join(dir, "local-"+strings.ReplaceAll(mutation.name, " ", "-")+".db"))
			db := openRawDB(t, mutatedPath)
			if _, err := db.ExecContext(ctx, mutation.sql); err != nil {
				t.Fatalf("apply local mutation: %v", err)
			}
			if err := db.Close(); err != nil {
				t.Fatalf("close locally mutated database: %v", err)
			}
			if got := hashFile(t, mutatedPath); got == baselineExactSHA {
				t.Fatal("local mutation did not change exact file SHA")
			}
			got, err := ComputeArtifactID(ctx, mutatedPath, CurrentStateSemanticV1)
			if err != nil {
				t.Fatalf("compute local mutation identity: %v", err)
			}
			if got != baselineArtifactID {
				t.Fatalf("local mutation identity = %s, want %s", got, baselineArtifactID)
			}
		})
	}
}

func TestSemanticArtifactIdentityIncludesMeaningfulState(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	st := seedExportSource(t, ctx, sourcePath)
	defer st.Close()
	baseline, err := Export(ctx, testExportOptions(sourcePath, filepath.Join(dir, "baseline")))
	if err != nil {
		t.Fatalf("export baseline: %v", err)
	}

	mutations := []struct {
		name string
		sql  string
	}{
		{name: "thread title", sql: `update threads set title = 'meaningfully changed'`},
		{name: "thread body excerpt and length", sql: `update threads set body = 'changed body', body_excerpt = 'changed body', body_length = 12`},
		{name: "thread labels", sql: `update threads set labels_json = '["changed"]'`},
		{name: "thread assignees", sql: `update threads set assignees_json = '["octocat"]'`},
		{name: "thread state", sql: `update threads set state = 'closed'`},
		{name: "thread URL", sql: `update threads set html_url = 'https://github.com/openclaw/gitcrawl/pull/700'`},
		{name: "thread content hash", sql: `update threads set content_hash = 'meaningfully changed'`},
		{name: "comment body", sql: `update comments set body = 'meaningfully changed'`},
		{name: "new row", sql: `insert into comments(id, thread_id, github_id, comment_type, body, raw_json) values(2, 1, 'C2', 'issue_comment', 'new comment', '')`},
		{name: "membership IDs", sql: `update thread_child_observation_memberships set member_ids_json = '["C1","C2"]'`},
		{name: "revision content", sql: `update thread_revisions set content_hash = 'meaningfully changed'`},
		{name: "fingerprint content", sql: `update thread_fingerprints set fingerprint_hash = 'meaningfully changed'`},
		{name: "PR field", sql: `update pull_request_details set base_sha = 'meaningfully-changed'`},
		{name: "workflow public state", sql: `update github_workflow_runs set status = 'in_progress'`},
		{name: "public timestamp", sql: `update threads set updated_at_gh = '2026-08-09T00:00:00Z'`},
		{name: "repository identity", sql: `update repositories set full_name = 'openclaw/renamed'`},
		{name: "unknown future column", sql: `alter table threads add column future_public_value text; update threads set future_public_value = 'retained'`},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			mutatedPath := copyIdentityTestDatabase(t, baseline.DatabasePath, filepath.Join(dir, "public-"+strings.ReplaceAll(mutation.name, " ", "-")+".db"))
			db := openRawDB(t, mutatedPath)
			if _, err := db.ExecContext(ctx, mutation.sql); err != nil {
				t.Fatalf("apply meaningful mutation: %v", err)
			}
			if err := db.Close(); err != nil {
				t.Fatalf("close meaningfully mutated database: %v", err)
			}
			got, err := ComputeArtifactID(ctx, mutatedPath, CurrentStateSemanticV1)
			if err != nil {
				t.Fatalf("compute meaningful mutation identity: %v", err)
			}
			if got == baseline.ArtifactID {
				t.Fatalf("meaningful mutation retained artifact identity %s", got)
			}
		})
	}
}

func TestSemanticArtifactIdentityCanonicalizesCompositeKeyRowids(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	create := func(name string, order []int) string {
		t.Helper()
		path := filepath.Join(dir, name+".db")
		db := openRawDB(t, path)
		if _, err := db.Exec(`
			create table pull_request_files(
				thread_id integer not null,
				position integer not null,
				path text not null,
				fetched_at text not null,
				primary key(thread_id, position)
			)
		`); err != nil {
			t.Fatal(err)
		}
		for _, position := range order {
			if _, err := db.Exec(`insert into pull_request_files values(1, ?, ?, ?)`, position, strings.Repeat("x", position+1), "local-time"); err != nil {
				t.Fatal(err)
			}
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		return path
	}
	forward := create("forward", []int{0, 1})
	reverse := create("reverse", []int{1, 0})
	if hashFile(t, forward) == hashFile(t, reverse) {
		t.Fatal("fixture insertion order did not change SQLite bytes")
	}
	forwardID, err := ComputeArtifactID(ctx, forward, CurrentStateSemanticV1)
	if err != nil {
		t.Fatal(err)
	}
	reverseID, err := ComputeArtifactID(ctx, reverse, CurrentStateSemanticV1)
	if err != nil {
		t.Fatal(err)
	}
	if forwardID != reverseID {
		t.Fatalf("composite-key insertion order changed semantic identity: forward=%s reverse=%s", forwardID, reverseID)
	}
}

func TestArtifactIdentityHashCanonicalizesSQLiteWriterHeader(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	basePath := filepath.Join(dir, "base.db")
	db := openRawDB(t, basePath)
	if _, err := db.Exec(`create table payload(id integer primary key, value text); insert into payload values(1, 'public')`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	firstPath := copyIdentityTestDatabase(t, basePath, filepath.Join(dir, "first.db"))
	secondPath := copyIdentityTestDatabase(t, basePath, filepath.Join(dir, "second.db"))
	writeHeader := func(path string, changeCounter, validFor, writerVersion uint32) {
		t.Helper()
		file, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		header := make([]byte, 100)
		if _, err := file.ReadAt(header, 0); err != nil {
			_ = file.Close()
			t.Fatal(err)
		}
		binary.BigEndian.PutUint32(header[24:28], changeCounter)
		binary.BigEndian.PutUint32(header[92:96], validFor)
		binary.BigEndian.PutUint32(header[96:100], writerVersion)
		if _, err := file.WriteAt(header, 0); err != nil {
			_ = file.Close()
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	}
	writeHeader(firstPath, 1, 1, 3049000)
	writeHeader(secondPath, 99, 99, 3050000)
	if hashFile(t, firstPath) == hashFile(t, secondPath) {
		t.Fatal("fixture writer headers did not change exact file hashes")
	}
	firstHash, err := hashArtifactIdentityFile(firstPath)
	if err != nil {
		t.Fatal(err)
	}
	secondHash, err := hashArtifactIdentityFile(secondPath)
	if err != nil {
		t.Fatal(err)
	}
	if firstHash != secondHash {
		t.Fatalf("SQLite writer header changed semantic file hash: first=%s second=%s", firstHash, secondHash)
	}
	for _, path := range []string{firstPath, secondPath} {
		db := openRawDB(t, path)
		if got, err := checkPragma(ctx, db, "quick_check"); err != nil || got != "ok" {
			_ = db.Close()
			t.Fatalf("canonical identity header quick_check = %q, %v", got, err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
	invalidPath := filepath.Join(dir, "invalid.db")
	if err := os.WriteFile(invalidPath, make([]byte, 100), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := hashArtifactIdentityFile(invalidPath); err == nil || !strings.Contains(err.Error(), "invalid SQLite header") {
		t.Fatalf("invalid identity header error = %v", err)
	}
}

func TestSemanticArtifactIdentityOptionalSchemaAndTypeSafety(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	minimalPath := filepath.Join(dir, "minimal.db")
	db := openRawDB(t, minimalPath)
	if _, err := db.Exec(`create table repositories(id integer primary key, future_public text); insert into repositories values(1, 'one')`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	first, err := ComputeArtifactID(ctx, minimalPath, CurrentStateSemanticV1)
	if err != nil {
		t.Fatalf("compute minimal optional-schema identity: %v", err)
	}
	db = openRawDB(t, minimalPath)
	if _, err := db.Exec(`update repositories set future_public = 'two'`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := ComputeArtifactID(ctx, minimalPath, CurrentStateSemanticV1)
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("unknown future column was excluded from semantic identity")
	}

	wrongTypePath := filepath.Join(dir, "wrong-type.db")
	db = openRawDB(t, wrongTypePath)
	if _, err := db.Exec(`create table repositories(id integer primary key, updated_at integer); insert into repositories values(1, 7)`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := ComputeArtifactID(ctx, wrongTypePath, CurrentStateSemanticV1); err == nil || !strings.Contains(err.Error(), `repositories.updated_at has type "INTEGER", want TEXT`) {
		t.Fatalf("wrong-type identity error = %v", err)
	}
}

func TestSemanticArtifactIdentityCleanupCancellationAndFailure(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	st := seedExportSource(t, ctx, sourcePath)
	defer st.Close()
	baseline, err := Export(ctx, testExportOptions(sourcePath, filepath.Join(dir, "baseline")))
	if err != nil {
		t.Fatal(err)
	}
	exactBefore := hashFile(t, baseline.DatabasePath)

	t.Run("cancel backup", func(t *testing.T) {
		tempParent := t.TempDir()
		cancelCtx, cancel := context.WithCancel(context.Background())
		_, err := computeArtifactIDWithOptions(cancelCtx, baseline.DatabasePath, currentStateSemanticPolicy, artifactIdentityOptions{
			TempParent: tempParent,
			Backup: onlineBackupOptions{PagesPerStep: 1, AfterStep: func(remaining, _ int) {
				if remaining > 0 {
					cancel()
				}
			}},
		})
		if err == nil || !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled artifact identity error = %v", err)
		}
		assertNoIdentityTemps(t, tempParent)
	})

	t.Run("cancel after normalization", func(t *testing.T) {
		tempParent := t.TempDir()
		cancelCtx, cancel := context.WithCancel(context.Background())
		_, err := computeArtifactIDWithOptions(cancelCtx, baseline.DatabasePath, currentStateSemanticPolicy, artifactIdentityOptions{
			TempParent: tempParent,
			AfterNormalize: func(*sql.DB) error {
				cancel()
				return nil
			},
		})
		if err == nil || !errors.Is(err, context.Canceled) {
			t.Fatalf("post-normalization cancellation error = %v", err)
		}
		assertNoIdentityTemps(t, tempParent)
	})

	t.Run("injected failure", func(t *testing.T) {
		tempParent := t.TempDir()
		marker := errors.New("identity hook failed")
		_, err := computeArtifactIDWithOptions(ctx, baseline.DatabasePath, currentStateSemanticPolicy, artifactIdentityOptions{
			TempParent: tempParent,
			AfterSnapshot: func(string) error {
				return marker
			},
		})
		if !errors.Is(err, marker) {
			t.Fatalf("injected artifact identity error = %v", err)
		}
		assertNoIdentityTemps(t, tempParent)
	})

	if exactAfter := hashFile(t, baseline.DatabasePath); exactAfter != exactBefore {
		t.Fatalf("artifact identity computation mutated finalized database: before=%s after=%s", exactBefore, exactAfter)
	}
	if _, err := ComputeArtifactID(ctx, baseline.DatabasePath, "future-semantic-v2"); err == nil || !strings.Contains(err.Error(), "unsupported artifact identity profile") {
		t.Fatalf("unknown artifact identity profile error = %v", err)
	}
}

func TestCurrentStateSemanticPolicyIsExplicitAndValid(t *testing.T) {
	if err := validateArtifactIdentityPolicy(currentStateSemanticPolicy); err != nil {
		t.Fatalf("current semantic identity policy: %v", err)
	}
	wantDropped := []string{
		"observation_schema_convergence",
		"pull_request_review_thread_syncs",
		"repo_pipeline_state",
		"repo_sync_state",
		"sqlite_stat1",
		"sqlite_stat4",
		"thread_child_observation_reservations",
		"thread_observation_sequence",
		"workflow_run_observation_reservations",
	}
	got := append([]string(nil), currentStateSemanticPolicy.DroppedTables...)
	sort.Strings(got)
	if !slices.Equal(got, wantDropped) {
		t.Fatalf("dropped identity tables = %v, want %v", got, wantDropped)
	}
	bad := currentStateSemanticPolicy
	bad.Tables = append([]identityTablePolicy(nil), bad.Tables...)
	bad.Tables = append(bad.Tables, bad.Tables[0])
	if err := validateArtifactIdentityPolicy(bad); err == nil || !strings.Contains(err.Error(), "repeats") {
		t.Fatalf("duplicate identity policy error = %v", err)
	}
}

func BenchmarkSemanticArtifactIdentity92MB(b *testing.B) {
	ctx := context.Background()
	dbPath := filepath.Join(b.TempDir(), "artifact.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		b.Fatal(err)
	}
	if _, err := db.Exec(`create table public_payload(id integer primary key, payload blob not null); insert into public_payload values(1, zeroblob(?))`, 92*1024*1024); err != nil {
		b.Fatal(err)
	}
	if err := db.Close(); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for range b.N {
		if _, err := ComputeArtifactID(ctx, dbPath, CurrentStateSemanticV1); err != nil {
			b.Fatal(err)
		}
	}
}

func copyIdentityTestDatabase(t *testing.T, source, target string) string {
	t.Helper()
	data, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return target
}

func assertNoIdentityTemps(t *testing.T, parent string) {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(parent, "gitcrawl-artifact-identity-*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 0 {
		t.Fatalf("artifact identity temporary files remain: %v", matches)
	}
}
