package portable

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/openclaw/gitcrawl/internal/store"
)

func TestExportCurrentStateV1SnapshotsLiveWALWithoutMutatingSource(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	st := seedExportSource(t, ctx, sourcePath)
	defer st.Close()

	sourceBefore := readFile(t, sourcePath)
	walBefore := readFile(t, sourcePath+"-wal")
	outputDir := filepath.Join(dir, "artifact.next")
	result, err := Export(ctx, ExportOptions{
		SourceDBPath: sourcePath,
		OutputDir:    outputDir,
		DatabaseName: "openclaw__openclaw.sync.db",
		PublicPath:   "data/openclaw__openclaw.sync.db",
		Profile:      CurrentStateV1,
		BodyChars:    8,
	})
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if !result.ArtifactCommitted || !result.ByteBudgetOK || result.QuickCheck != "ok" || result.IntegrityCheck != "ok" || result.ForeignKeyViolations != 0 {
		t.Fatalf("export result = %+v", result)
	}
	if !bytes.Equal(sourceBefore, readFile(t, sourcePath)) {
		t.Fatal("export changed source database bytes")
	}
	if !bytes.Equal(walBefore, readFile(t, sourcePath+"-wal")) {
		t.Fatal("export changed source WAL bytes")
	}
	for _, suffix := range []string{"-wal", "-shm"} {
		if _, err := os.Stat(result.DatabasePath + suffix); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("artifact sidecar %s exists: %v", suffix, err)
		}
	}

	db := openRawDB(t, result.DatabasePath)
	defer db.Close()
	for _, table := range currentStateProfile.DroppedTables {
		if tableExists(t, db, table) {
			t.Fatalf("omitted table %s still exists", table)
		}
	}
	for _, table := range []string{
		"repositories", "threads", "comments", "thread_revisions", "thread_fingerprints",
		"pull_request_details", "pull_request_checks", "thread_child_observation_memberships",
	} {
		if !tableExists(t, db, table) {
			t.Fatalf("preserved table %s is missing", table)
		}
		if got := rowCount(t, db, table); got != 1 {
			t.Fatalf("preserved table %s row count = %d, want 1", table, got)
		}
	}
	var body, excerpt string
	var bodyLength int
	if err := db.QueryRow(`select body_excerpt, body_excerpt, body_length from threads where id = 1`).Scan(&body, &excerpt, &bodyLength); err != nil {
		t.Fatalf("read compact thread: %v", err)
	}
	if body != "abcdefgh" || excerpt != "abcdefgh" || bodyLength != 26 {
		t.Fatalf("compact thread = body %q excerpt %q length %d", body, excerpt, bodyLength)
	}
	var sourceMetadata, profileMetadata, includes, capabilities, excluded, indexProfile string
	for key, target := range map[string]*string{
		"source_path": &sourceMetadata, "profile": &profileMetadata, "includes": &includes,
		"capabilities": &capabilities, "excluded": &excluded, "index_profile": &indexProfile,
	} {
		if err := db.QueryRow(`select value from portable_metadata where key = ?`, key).Scan(target); err != nil {
			t.Fatalf("read metadata %s: %v", key, err)
		}
	}
	if sourceMetadata != "data/openclaw__openclaw.sync.db" || strings.Contains(sourceMetadata, dir) {
		t.Fatalf("portable source_path = %q", sourceMetadata)
	}
	if profileMetadata != CurrentStateV1 || indexProfile != "constraints-only" {
		t.Fatalf("profile metadata = %q / %q", profileMetadata, indexProfile)
	}
	if !csvContains(includes, "comments") || !csvContains(includes, "thread_child_observation_memberships") ||
		!csvContains(capabilities, "current_comments") || csvContains(excluded, "comments") {
		t.Fatalf("inaccurate metadata includes=%q capabilities=%q excluded=%q", includes, capabilities, excluded)
	}
	if indexExists(t, db, "custom_comments_author") || !indexExists(t, db, "unique_comments_github") {
		t.Fatalf("index policy not applied: dropped=%v unique=%v", indexExists(t, db, "custom_comments_author"), indexExists(t, db, "unique_comments_github"))
	}
	if !slices.Contains(result.DroppedIndexes, "custom_comments_author") || slices.Contains(result.DroppedIndexes, "unique_comments_github") {
		t.Fatalf("dropped indexes = %v", result.DroppedIndexes)
	}
	manifest := readManifest(t, result.ManifestPath)
	if manifest.OutputPath != result.PublicPath || manifest.OutputBytes != result.BytesAfter ||
		manifest.SHA256 != result.SHA256 || manifest.ArtifactID != result.ArtifactID ||
		manifest.ForeignKeyViolations != 0 || !manifest.ValidationOK {
		t.Fatalf("manifest/result mismatch: manifest=%+v result=%+v", manifest, result)
	}
	if got := hashFile(t, result.DatabasePath); got != result.SHA256 {
		t.Fatalf("database hash = %s, want %s", got, result.SHA256)
	}
}

func TestExportByteBudgetExactAndOneByteOver(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	st := seedExportSource(t, ctx, sourcePath)
	defer st.Close()
	baseline, err := Export(ctx, testExportOptions(sourcePath, filepath.Join(dir, "baseline")))
	if err != nil {
		t.Fatalf("baseline export: %v", err)
	}
	exactOptions := testExportOptions(sourcePath, filepath.Join(dir, "exact"))
	exactOptions.MaxBytes = &baseline.BytesAfter
	exact, err := Export(ctx, exactOptions)
	if err != nil {
		t.Fatalf("exact byte budget: %v", err)
	}
	if exact.BytesAfter != baseline.BytesAfter || !exact.ByteBudgetOK {
		t.Fatalf("exact result = %+v, baseline bytes = %d", exact, baseline.BytesAfter)
	}
	oneUnder := baseline.BytesAfter - 1
	failingDir := filepath.Join(dir, "too-large")
	failingOptions := testExportOptions(sourcePath, failingDir)
	failingOptions.MaxBytes = &oneUnder
	failed, err := Export(ctx, failingOptions)
	if err == nil || !strings.Contains(err.Error(), "exceeds max-bytes") || failed.ByteBudgetOK {
		t.Fatalf("one-byte-over result=%+v err=%v", failed, err)
	}
	if _, err := os.Stat(failingDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed budget left target: %v", err)
	}
	assertNoExportTemps(t, dir)
}

func TestExportRejectsUnsafeInputsAndExistingOutput(t *testing.T) {
	for _, name := range []string{"", ".", "..", "a/b.db", `a\b.db`, "bad\x00.db", "gitcrawl.db-wal", "gitcrawl.db-shm", "gitcrawl.db.manifest.json"} {
		if err := ValidateDatabaseName(name); err == nil {
			t.Errorf("ValidateDatabaseName(%q) succeeded", name)
		}
	}
	for _, publicPath := range []string{"", ".", "/data/a.db", "../a.db", "data/../a.db", "data//a.db", `data\a.db`, "data/bad\x00.db", "C:/a.db"} {
		if err := ValidatePublicPath(publicPath); err == nil {
			t.Errorf("ValidatePublicPath(%q) succeeded", publicPath)
		}
	}
	if err := ValidateDatabaseName("export-2026.db"); err != nil {
		t.Fatalf("valid database name rejected: %v", err)
	}
	if err := ValidatePublicPath("data/export-2026.db"); err != nil {
		t.Fatalf("valid public path rejected: %v", err)
	}
	if _, err := ResolveProfile("current-state-v2"); err == nil || !strings.Contains(err.Error(), CurrentStateV1) {
		t.Fatalf("unknown profile error = %v", err)
	}
	ctx := context.Background()
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	st := seedExportSource(t, ctx, sourcePath)
	defer st.Close()
	existing := filepath.Join(dir, "existing")
	if err := os.Mkdir(existing, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := Export(ctx, testExportOptions(sourcePath, existing)); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("existing output error = %v", err)
	}
}

func TestExportFailureHooksCleanStagingAndNeverCommitPartialPair(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	st := seedExportSource(t, ctx, sourcePath)
	defer st.Close()
	for _, tc := range []struct {
		name     string
		exporter exporter
	}{
		{name: "manifest", exporter: exporter{beforeManifest: func() error { return errors.New("manifest fault") }}},
		{name: "commit", exporter: exporter{beforeCommit: func() error { return errors.New("commit fault") }}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			outputDir := filepath.Join(dir, tc.name)
			if _, err := tc.exporter.export(ctx, testExportOptions(sourcePath, outputDir)); err == nil || !strings.Contains(err.Error(), "fault") {
				t.Fatalf("fault error = %v", err)
			}
			if _, err := os.Stat(outputDir); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("fault left target: %v", err)
			}
			assertNoExportTemps(t, dir)
		})
	}
}

func TestRemoveSQLiteSidecarsFailsClosed(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "artifact.db")
	walDir := dbPath + "-wal"
	if err := os.Mkdir(walDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(walDir, "locked"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := removeSQLiteSidecars(dbPath); err == nil || !strings.Contains(err.Error(), "remove portable SQLite sidecar") {
		t.Fatalf("sidecar cleanup error = %v", err)
	}
}

func TestExportedDatabaseReopensWritableAndRecreatesOmittedSchema(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	st := seedExportSource(t, ctx, sourcePath)
	defer st.Close()
	result, err := Export(ctx, testExportOptions(sourcePath, filepath.Join(dir, "artifact")))
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	writable, err := store.Open(ctx, result.DatabasePath)
	if err != nil {
		t.Fatalf("writable reopen/migrate: %v", err)
	}
	defer writable.Close()
	for _, table := range currentStateProfile.DroppedTables {
		if !tableExists(t, writable.DB(), table) {
			t.Fatalf("migration did not recreate %s", table)
		}
		if table != "comment_revisions" && rowCount(t, writable.DB(), table) != 0 {
			got := rowCount(t, writable.DB(), table)
			t.Fatalf("recreated history table %s has %d rows", table, got)
		}
	}
	var historicalComments int
	if err := writable.DB().QueryRow(`select count(*) from comment_revisions where body = 'historical-comment'`).Scan(&historicalComments); err != nil {
		t.Fatalf("inspect recreated comment revisions: %v", err)
	}
	if historicalComments != 0 {
		t.Fatal("migration restored omitted historical comment data")
	}
	if !indexExists(t, writable.DB(), "idx_comments_thread_type") {
		t.Fatal("migration did not recreate ordinary schema indexes")
	}
}

func seedExportSource(t *testing.T, ctx context.Context, dbPath string) *store.Store {
	t.Helper()
	st, err := store.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	st.DB().SetMaxOpenConns(1)
	if _, err := st.DB().ExecContext(ctx, `pragma wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatalf("checkpoint source: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `pragma wal_autocheckpoint = 0`); err != nil {
		t.Fatalf("disable WAL autocheckpoint: %v", err)
	}
	statements := []string{
		`insert into repositories(id, owner, name, full_name, raw_json, updated_at) values(1, 'openclaw', 'gitcrawl', 'openclaw/gitcrawl', '{"private":"repo"}', '2026-08-08T00:00:00Z')`,
		`insert into threads(id, repo_id, github_id, number, kind, state, title, body, html_url, labels_json, assignees_json, raw_json, content_hash, updated_at) values(1, 1, 'T1', 7, 'pull_request', 'open', 'portable export', 'abcdefghijklmnopqrstuvwxyz', 'https://github.com/openclaw/gitcrawl/pull/7', '[]', '[]', '{"private":"thread"}', 'hash', '2026-08-08T00:00:00Z')`,
		`insert into comments(id, thread_id, github_id, comment_type, body, raw_json) values(1, 1, 'C1', 'issue_comment', 'comment-current-body', '{"private":"comment"}')`,
		`insert into comment_revisions(id, comment_id, body, raw_json, recorded_at) values(1, 1, 'historical-comment', '{}', '2026-08-08T00:00:00Z')`,
		`insert into thread_revisions(id, thread_id, content_hash, title_hash, body_hash, labels_hash, created_at) values(1, 1, 'content', 'title', 'body', 'labels', '2026-08-08T00:00:00Z')`,
		`insert into thread_fingerprints(id, thread_revision_id, algorithm_version, fingerprint_hash, fingerprint_slug, title_tokens_json, body_token_hash, linked_refs_json, file_set_hash, module_buckets_json, simhash64, feature_json, created_at) values(1, 1, 'v1', 'fingerprint', 'portable-export', '["portable"]', 'bodyhash', '[]', 'files', '[]', '1', '{}', '2026-08-08T00:00:00Z')`,
		`insert into thread_key_summaries(id, thread_revision_id, summary_kind, prompt_version, provider, model, input_hash, output_hash, key_text, created_at) values(1, 1, 'llm_key', 'v1', 'openai', 'test', 'in', 'out', 'historical enrichment', '2026-08-08T00:00:00Z')`,
		`insert into pull_request_details(thread_id, repo_id, number, raw_json, fetched_at, updated_at) values(1, 1, 7, '{}', '2026-08-08T00:00:00Z', '2026-08-08T00:00:00Z')`,
		`insert into pull_request_checks(id, thread_id, name, status, raw_json, fetched_at) values(1, 1, 'test', 'completed', '{}', '2026-08-08T00:00:00Z')`,
		`insert into thread_child_observation_memberships(thread_id, family, observation_sequence, member_ids_json) values(1, 'comments', 1, '["C1"]')`,
		`insert into cluster_groups(id, repo_id, stable_key, stable_slug, status, created_at, updated_at) values(1, 1, 'key', 'slug', 'open', '2026-08-08T00:00:00Z', '2026-08-08T00:00:00Z')`,
		`insert into cluster_memberships(cluster_id, thread_id, role, state, added_by, added_reason_json, created_at, updated_at) values(1, 1, 'member', 'active', 'system', '{}', '2026-08-08T00:00:00Z', '2026-08-08T00:00:00Z')`,
		`insert into cluster_overrides(id, repo_id, cluster_id, thread_id, action, created_at) values(1, 1, 1, 1, 'exclude', '2026-08-08T00:00:00Z')`,
		`insert into cluster_aliases(cluster_id, alias_slug, reason, created_at) values(1, 'old-slug', 'lineage', '2026-08-08T00:00:00Z')`,
		`insert into cluster_closures(cluster_id, reason, actor_kind, created_at, updated_at) values(1, 'local', 'human', '2026-08-08T00:00:00Z', '2026-08-08T00:00:00Z')`,
		`create index custom_comments_author on comments(author_login)`,
		`create unique index unique_comments_github on comments(github_id)`,
	}
	tx, err := st.DB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin seed: %v", err)
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			_ = tx.Rollback()
			t.Fatalf("seed source with %q: %v", statement, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit seed: %v", err)
	}
	if _, err := os.Stat(dbPath + "-wal"); err != nil {
		t.Fatalf("source WAL missing: %v", err)
	}
	return st
}

func testExportOptions(sourcePath, outputDir string) ExportOptions {
	return ExportOptions{
		SourceDBPath: sourcePath,
		OutputDir:    outputDir,
		DatabaseName: "gitcrawl.db",
		PublicPath:   "data/gitcrawl.db",
		Profile:      CurrentStateV1,
		BodyChars:    32,
	}
}

func openRawDB(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open sqlite %s: %v", path, err)
	}
	return db
}

func tableExists(t *testing.T, db *sql.DB, name string) bool {
	t.Helper()
	var exists int
	if err := db.QueryRow(`select exists(select 1 from sqlite_schema where type = 'table' and name = ?)`, name).Scan(&exists); err != nil {
		t.Fatalf("inspect table %s: %v", name, err)
	}
	return exists == 1
}

func indexExists(t *testing.T, db *sql.DB, name string) bool {
	t.Helper()
	var exists int
	if err := db.QueryRow(`select exists(select 1 from sqlite_schema where type = 'index' and name = ?)`, name).Scan(&exists); err != nil {
		t.Fatalf("inspect index %s: %v", name, err)
	}
	return exists == 1
}

func rowCount(t *testing.T, db *sql.DB, table string) int {
	t.Helper()
	var count int
	if err := db.QueryRow(`select count(*) from ` + quoteIdentifier(table)).Scan(&count); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return count
}

func csvContains(csv, value string) bool {
	return slices.Contains(strings.Split(csv, ","), value)
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return data
}

func hashFile(t *testing.T, path string) string {
	t.Helper()
	sum := sha256.Sum256(readFile(t, path))
	return hex.EncodeToString(sum[:])
}

func readManifest(t *testing.T, path string) Manifest {
	t.Helper()
	var manifest Manifest
	if err := json.Unmarshal(readFile(t, path), &manifest); err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	return manifest
}

func assertNoExportTemps(t *testing.T, parent string) {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(parent, ".gitcrawl-portable-export-*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 0 {
		t.Fatalf("temporary exports remain: %v", matches)
	}
}
