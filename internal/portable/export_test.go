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
	"time"

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
	for _, column := range []struct{ table, name string }{
		{table: "comments", name: "raw_json_blob_id"},
		{table: "thread_revisions", name: "raw_json_blob_id"},
	} {
		if columnExists(t, db, column.table, column.name) {
			t.Fatalf("derived artifact retained dangling column %s.%s", column.table, column.name)
		}
	}
	var journalMode string
	if err := db.QueryRow(`pragma journal_mode`).Scan(&journalMode); err != nil {
		t.Fatalf("read artifact journal mode: %v", err)
	}
	if journalMode != "delete" {
		t.Fatalf("artifact journal mode = %q, want delete", journalMode)
	}
	for _, table := range currentStateProfile.DroppedTables {
		if tableExists(t, db, table) {
			t.Fatalf("omitted table %s still exists", table)
		}
	}
	for _, table := range []string{
		"repositories", "threads", "comments", "thread_revisions", "thread_fingerprints",
		"pull_request_details", "pull_request_files", "pull_request_checks", "thread_child_observation_memberships",
	} {
		if !tableExists(t, db, table) {
			t.Fatalf("preserved table %s is missing", table)
		}
		if got := rowCount(t, db, table); got != 1 {
			t.Fatalf("preserved table %s row count = %d, want 1", table, got)
		}
	}
	var body, excerpt, threadRawJSON, repositoryRawJSON string
	var bodyLength int
	if err := db.QueryRow(`select body, body_excerpt, body_length, raw_json from threads where id = 1`).Scan(&body, &excerpt, &bodyLength, &threadRawJSON); err != nil {
		t.Fatalf("read compact thread: %v", err)
	}
	if err := db.QueryRow(`select raw_json from repositories where id = 1`).Scan(&repositoryRawJSON); err != nil {
		t.Fatalf("read compact repository: %v", err)
	}
	if body != "abcdefgh" || excerpt != "abcdefgh" || bodyLength != 26 || threadRawJSON != "" || repositoryRawJSON != "" {
		t.Fatalf("compact thread = body %q excerpt %q length %d thread_raw=%q repository_raw=%q", body, excerpt, bodyLength, threadRawJSON, repositoryRawJSON)
	}
	var sourceMetadata, profileMetadata, includes, capabilities, excluded, indexProfile, columnProfile string
	for key, target := range map[string]*string{
		"source_path": &sourceMetadata, "profile": &profileMetadata, "includes": &includes,
		"capabilities": &capabilities, "excluded": &excluded, "index_profile": &indexProfile, "column_profile": &columnProfile,
	} {
		if err := db.QueryRow(`select value from portable_metadata where key = ?`, key).Scan(target); err != nil {
			t.Fatalf("read metadata %s: %v", key, err)
		}
	}
	if sourceMetadata != "data/openclaw__openclaw.sync.db" || strings.Contains(sourceMetadata, dir) {
		t.Fatalf("portable source_path = %q", sourceMetadata)
	}
	if profileMetadata != CurrentStateV1 || indexProfile != "constraints-only" || columnProfile != store.PortableColumnProfileSanitizedCompatibility {
		t.Fatalf("profile metadata = %q / %q / %q", profileMetadata, indexProfile, columnProfile)
	}
	if !csvContains(includes, "comments") || !csvContains(includes, "thread_child_observation_memberships") ||
		!csvContains(capabilities, "current_comments") || !csvContains(excluded, "pull_request_file_patches") || csvContains(excluded, "comments") {
		t.Fatalf("inaccurate metadata includes=%q capabilities=%q excluded=%q", includes, capabilities, excluded)
	}
	if indexExists(t, db, "custom_comments_author") || !indexExists(t, db, "unique_comments_github") {
		t.Fatalf("index policy not applied: dropped=%v unique=%v", indexExists(t, db, "custom_comments_author"), indexExists(t, db, "unique_comments_github"))
	}
	if !slices.Contains(result.DroppedIndexes, "custom_comments_author") || slices.Contains(result.DroppedIndexes, "unique_comments_github") {
		t.Fatalf("dropped indexes = %v", result.DroppedIndexes)
	}
	for _, index := range []string{"custom_threads_title", "idx_threads_repo_number", "idx_threads_repo_state_closed", "idx_threads_repo_updated"} {
		if !slices.Contains(result.DroppedIndexes, index) {
			t.Fatalf("dropped indexes = %v, missing rebuilt threads index %s", result.DroppedIndexes, index)
		}
	}
	if slices.Contains(result.DroppedIndexes, "unique_threads_github_id") || !indexExists(t, db, "unique_threads_github_id") {
		t.Fatalf("explicit unique threads index was not preserved: dropped=%v exists=%v", result.DroppedIndexes, indexExists(t, db, "unique_threads_github_id"))
	}
	if slices.Contains(result.DroppedIndexes, "idx_comment_revisions_comment") || slices.Contains(result.DroppedIndexes, "custom_comment_revisions_body") {
		t.Fatalf("implicitly removed indexes were reported as explicitly dropped: %v", result.DroppedIndexes)
	}
	if len(result.DroppedTables) < len(currentStateProfile.DroppedTables) || !slices.Equal(result.DroppedTables[:len(currentStateProfile.DroppedTables)], currentStateProfile.DroppedTables) {
		t.Fatalf("profile tables were not dropped first: %v", result.DroppedTables)
	}
	seenDroppedTables := make(map[string]bool)
	for _, table := range result.DroppedTables {
		if seenDroppedTables[table] {
			t.Fatalf("dropped table %s reported more than once: %v", table, result.DroppedTables)
		}
		seenDroppedTables[table] = true
	}
	manifest := readManifest(t, result.ManifestPath)
	if manifest.OutputPath != result.PublicPath || manifest.OutputBytes != result.BytesAfter ||
		manifest.SHA256 != result.SHA256 || manifest.ArtifactID != result.ArtifactID ||
		manifest.ArtifactIDProfile != CurrentStateSemanticV1 || result.ArtifactIDProfile != CurrentStateSemanticV1 ||
		manifest.ForeignKeyViolations != 0 || !manifest.ValidationOK ||
		manifest.ColumnProfile != store.PortableColumnProfileSanitizedCompatibility || result.ColumnProfile != manifest.ColumnProfile {
		t.Fatalf("manifest/result mismatch: manifest=%+v result=%+v", manifest, result)
	}
	if got := hashFile(t, result.DatabasePath); got != result.SHA256 {
		t.Fatalf("database hash = %s, want %s", got, result.SHA256)
	}
	assertPortablePRFilePatchStripped(t, db)
	if !slices.Contains(manifest.Excluded, "pull_request_file_patches") {
		t.Fatalf("manifest excluded = %v", manifest.Excluded)
	}
	if manifest.Repository == nil || manifest.Repository.FullName != "openclaw/gitcrawl" || result.Repository == nil || *manifest.Repository != *result.Repository {
		t.Fatalf("single-repository metadata manifest=%+v result=%+v", manifest.Repository, result.Repository)
	}
	assertManifestTableCounts(t, db, manifest.Tables)
	if violations, err := testForeignKeyViolationCount(ctx, db); err != nil || violations != 0 {
		t.Fatalf("independent final artifact FK violations=%d err=%v", violations, err)
	}
}

func TestExportArtifactIdentityIsDeterministicAcrossExportTimes(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	st := seedExportSource(t, ctx, sourcePath)
	defer st.Close()

	exportAt := func(name, timestamp string) (ExportResult, Manifest) {
		t.Helper()
		instant, err := time.Parse(time.RFC3339Nano, timestamp)
		if err != nil {
			t.Fatalf("parse export time: %v", err)
		}
		result, err := (exporter{now: func() time.Time { return instant }}).export(
			ctx,
			testExportOptions(sourcePath, filepath.Join(dir, name)),
		)
		if err != nil {
			t.Fatalf("export %s: %v", name, err)
		}
		manifest := readManifest(t, result.ManifestPath)
		if manifest.ExportedAt != timestamp {
			t.Fatalf("export %s manifest exportedAt = %q, want %q", name, manifest.ExportedAt, timestamp)
		}
		if err := validateManifestPair(ctx, result.DatabasePath, result.ManifestPath, manifest); err != nil {
			t.Fatalf("validate export %s pair: %v", name, err)
		}
		db := openRawDB(t, result.DatabasePath)
		defer db.Close()
		var exportedAtCount int
		if err := db.QueryRow(`select count(*) from portable_metadata where key = 'exported_at'`).Scan(&exportedAtCount); err != nil {
			t.Fatalf("inspect export %s metadata: %v", name, err)
		}
		if exportedAtCount != 0 {
			t.Fatalf("export %s retained portable_metadata.exported_at", name)
		}
		return result, manifest
	}

	first, firstManifest := exportAt("first", "2026-08-09T10:00:00Z")
	second, secondManifest := exportAt("second", "2026-08-09T10:05:00Z")
	if firstManifest.ExportedAt == secondManifest.ExportedAt {
		t.Fatalf("manifest exportedAt values are equal: %q", firstManifest.ExportedAt)
	}
	if first.SHA256 != second.SHA256 || first.ArtifactID != second.ArtifactID || first.BytesAfter != second.BytesAfter {
		t.Fatalf("unchanged export identity differs: first=%+v second=%+v", first, second)
	}
	if !bytes.Equal(readFile(t, first.DatabasePath), readFile(t, second.DatabasePath)) {
		t.Fatal("unchanged exports produced different SQLite file bytes")
	}

	if _, err := st.DB().ExecContext(ctx, `update threads set title = 'portable export changed' where id = 1`); err != nil {
		t.Fatalf("change retained source content: %v", err)
	}
	changed, _ := exportAt("changed", "2026-08-09T10:10:00Z")
	if changed.ArtifactID == first.ArtifactID {
		t.Fatalf("artifactId did not change with source content: %s", changed.ArtifactID)
	}
}

func TestExportSemanticIdentityIgnoresLocalSourceChurnWhileExactSHAChanges(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	st := seedExportSource(t, ctx, sourcePath)
	defer st.Close()

	first, err := Export(ctx, testExportOptions(sourcePath, filepath.Join(dir, "first")))
	if err != nil {
		t.Fatalf("first export: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `
		update repositories set updated_at = '2026-08-10T00:00:00Z';
		update threads set
			first_pulled_at = '2026-08-10T00:00:00Z',
			last_pulled_at = '2026-08-10T00:01:00Z',
			updated_at = '2026-08-10T00:02:00Z',
			observation_sequence = 99,
			evidence_observation_sequence = 99,
			evidence_source_updated_at = '2026-08-10T00:03:00Z';
		update thread_revisions set observation_sequence = 99;
		update thread_child_observation_memberships set observation_sequence = 99;
	`); err != nil {
		t.Fatalf("mutate local source bookkeeping: %v", err)
	}
	second, err := Export(ctx, testExportOptions(sourcePath, filepath.Join(dir, "second")))
	if err != nil {
		t.Fatalf("second export: %v", err)
	}
	if first.SHA256 == second.SHA256 {
		t.Fatalf("local source churn retained exact SHA %s", first.SHA256)
	}
	if first.ArtifactID != second.ArtifactID {
		t.Fatalf("local source churn changed semantic identity: first=%s second=%s", first.ArtifactID, second.ArtifactID)
	}
	for _, result := range []ExportResult{first, second} {
		manifest := readManifest(t, result.ManifestPath)
		if manifest.ArtifactIDProfile != CurrentStateSemanticV1 || result.ArtifactIDProfile != CurrentStateSemanticV1 {
			t.Fatalf("artifact identity profile manifest=%q result=%q", manifest.ArtifactIDProfile, result.ArtifactIDProfile)
		}
		if manifest.SHA256 == manifest.ArtifactID {
			t.Fatalf("exact and semantic digests unexpectedly match: %s", manifest.SHA256)
		}
		if err := validateManifestPair(ctx, result.DatabasePath, result.ManifestPath, manifest); err != nil {
			t.Fatalf("validate semantic manifest pair: %v", err)
		}
	}
}

func TestOnlineBackupSnapshotsLiveWALInChunksWithoutSourceMutation(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	st := seedExportSource(t, ctx, sourcePath)
	defer st.Close()
	sourceBefore := readFile(t, sourcePath)
	walBefore := readFile(t, sourcePath+"-wal")
	targetPath := filepath.Join(dir, "snapshot.db")
	steps := 0
	if err := snapshotSQLiteWithOptions(ctx, sourcePath, targetPath, onlineBackupOptions{
		PagesPerStep: 1,
		AfterStep: func(remaining, pageCount int) {
			steps++
			if pageCount <= 0 || remaining < 0 {
				t.Errorf("backup progress remaining=%d page_count=%d", remaining, pageCount)
			}
		},
	}); err != nil {
		t.Fatalf("online backup: %v", err)
	}
	if steps <= 1 {
		t.Fatalf("online backup steps = %d, want multiple bounded chunks", steps)
	}
	db := openRawDB(t, targetPath)
	defer db.Close()
	if got := rowCount(t, db, "threads"); got != 1 {
		t.Fatalf("snapshot threads = %d, want committed WAL row", got)
	}
	if !bytes.Equal(sourceBefore, readFile(t, sourcePath)) || !bytes.Equal(walBefore, readFile(t, sourcePath+"-wal")) {
		t.Fatal("online backup changed source database or WAL bytes")
	}
}

func TestOnlineBackupCancellationRemovesPartialTarget(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	st := seedExportSource(t, context.Background(), sourcePath)
	defer st.Close()
	sourceBefore := readFile(t, sourcePath)
	walBefore := readFile(t, sourcePath+"-wal")
	ctx, cancel := context.WithCancel(context.Background())
	targetPath := filepath.Join(dir, "partial.db")
	sawPartial := false
	err := snapshotSQLiteWithOptions(ctx, sourcePath, targetPath, onlineBackupOptions{
		PagesPerStep: 1,
		AfterStep: func(remaining, pageCount int) {
			if remaining > 0 {
				_, statErr := os.Stat(targetPath)
				sawPartial = statErr == nil
				cancel()
			}
		},
	})
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled online backup error = %v", err)
	}
	if !sawPartial {
		t.Fatal("online backup cancellation did not occur after a partial target was created")
	}
	for _, path := range []string{targetPath, targetPath + "-wal", targetPath + "-shm"} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("partial backup artifact remains at %s: %v", path, err)
		}
	}
	if !bytes.Equal(sourceBefore, readFile(t, sourcePath)) || !bytes.Equal(walBefore, readFile(t, sourcePath+"-wal")) {
		t.Fatal("canceled online backup changed source database or WAL bytes")
	}
}

func TestExportScopedRepositoryKeepsOnlyRequestedDependentData(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	st := seedExportSource(t, ctx, sourcePath)
	defer st.Close()
	seedSecondExportRepository(t, ctx, st)
	options := testExportOptions(sourcePath, filepath.Join(dir, "scoped"))
	options.Repository = "openclaw/gitcrawl"
	result, err := Export(ctx, options)
	if err != nil {
		t.Fatalf("scoped export: %v", err)
	}
	if result.Repository == nil || *result.Repository != (Repository{ID: 1, Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl"}) {
		t.Fatalf("scoped repository = %+v", result.Repository)
	}
	db := openRawDB(t, result.DatabasePath)
	defer db.Close()
	for _, table := range []string{
		"repositories", "threads", "comments", "thread_revisions", "thread_fingerprints",
		"pull_request_details", "pull_request_files", "pull_request_checks", "thread_child_observation_memberships",
	} {
		if got := rowCount(t, db, table); got != 1 {
			t.Fatalf("scoped table %s rows = %d, want 1", table, got)
		}
	}
	var fullName, title, commentBody string
	if err := db.QueryRow(`select full_name from repositories`).Scan(&fullName); err != nil {
		t.Fatalf("read scoped repository: %v", err)
	}
	if err := db.QueryRow(`select title from threads`).Scan(&title); err != nil {
		t.Fatalf("read scoped thread: %v", err)
	}
	if err := db.QueryRow(`select body from comments`).Scan(&commentBody); err != nil {
		t.Fatalf("read scoped comment: %v", err)
	}
	if fullName != "openclaw/gitcrawl" || title != "portable export" || commentBody != "comment-current-body" {
		t.Fatalf("scoped data = %q / %q / %q", fullName, title, commentBody)
	}
	assertPortablePRFilePatchStripped(t, db)
	manifest := readManifest(t, result.ManifestPath)
	if manifest.Repository == nil || *manifest.Repository != *result.Repository {
		t.Fatalf("scoped manifest repository = %+v, result = %+v", manifest.Repository, result.Repository)
	}
	assertManifestTableCounts(t, db, manifest.Tables)
	if slices.Contains(result.DroppedIndexes, "idx_comment_revisions_comment") || slices.Contains(result.DroppedIndexes, "custom_comment_revisions_body") {
		t.Fatalf("dropped indexes include indexes removed with a table: %v", result.DroppedIndexes)
	}
}

func TestExportMissingRepositoryCleansStaging(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	st := seedExportSource(t, ctx, sourcePath)
	defer st.Close()
	outputDir := filepath.Join(dir, "missing")
	options := testExportOptions(sourcePath, outputDir)
	options.Repository = "openclaw/missing"
	if _, err := Export(ctx, options); err == nil || !strings.Contains(err.Error(), `target repository "openclaw/missing" count is 0`) {
		t.Fatalf("missing repository error = %v", err)
	}
	if _, err := os.Stat(outputDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing repository left target: %v", err)
	}
	assertNoExportTemps(t, dir)
}

func TestExportUnscopedMultiRepositoryOmitsSingularMetadata(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	st := seedExportSource(t, ctx, sourcePath)
	defer st.Close()
	seedSecondExportRepository(t, ctx, st)
	result, err := Export(ctx, testExportOptions(sourcePath, filepath.Join(dir, "multi")))
	if err != nil {
		t.Fatalf("unscoped multi-repository export: %v", err)
	}
	manifest := readManifest(t, result.ManifestPath)
	if result.Repository != nil || manifest.Repository != nil {
		t.Fatalf("multi-repository metadata result=%+v manifest=%+v", result.Repository, manifest.Repository)
	}
	db := openRawDB(t, result.DatabasePath)
	defer db.Close()
	if got := rowCount(t, db, "repositories"); got != 2 {
		t.Fatalf("unscoped repositories = %d, want 2", got)
	}
	assertManifestTableCounts(t, db, manifest.Tables)
}

func TestExportMigratesPhysicallyPrunedSourceBeforeSanitizedRebuild(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "physically-pruned.db")
	st := seedExportSource(t, ctx, sourcePath)
	if _, err := st.PrunePortablePayloads(ctx, store.PortablePruneOptions{BodyChars: 16}); err != nil {
		t.Fatalf("physically prune source: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close physically pruned source: %v", err)
	}
	result, err := Export(ctx, testExportOptions(sourcePath, filepath.Join(dir, "artifact")))
	if err != nil {
		t.Fatalf("export physically pruned source: %v", err)
	}
	db := openRawDB(t, result.DatabasePath)
	defer db.Close()
	var body, excerpt, rawJSON, columnProfile string
	if err := db.QueryRow(`select body, body_excerpt, raw_json from threads where id = 1`).Scan(&body, &excerpt, &rawJSON); err != nil {
		t.Fatalf("read migrated compatibility columns: %v", err)
	}
	if err := db.QueryRow(`select value from portable_metadata where key = 'column_profile'`).Scan(&columnProfile); err != nil {
		t.Fatal(err)
	}
	if body != "abcdefghijklmnop" || excerpt != body || rawJSON != "" || columnProfile != store.PortableColumnProfileSanitizedCompatibility {
		t.Fatalf("migrated physical source body=%q excerpt=%q raw=%q profile=%q", body, excerpt, rawJSON, columnProfile)
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
	for _, name := range []string{"", ".", "..", "a/b.db", `a\b.db`, "bad\x00.db", "bad?.db", "gitcrawl.db-wal", "gitcrawl.db-shm", "gitcrawl.db.manifest.json"} {
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

func TestExportCancellationDuringStageCleansStaging(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	st := seedExportSource(t, context.Background(), sourcePath)
	defer st.Close()
	ctx, cancel := context.WithCancel(context.Background())
	outputDir := filepath.Join(dir, "canceled")
	options := testExportOptions(sourcePath, outputDir)
	options.Progress = func(stage Stage) {
		if stage == StageCanonicalShaping {
			cancel()
		}
	}
	if _, err := Export(ctx, options); err == nil || !strings.Contains(err.Error(), "canceled during canonical shaping") {
		t.Fatalf("canceled export error = %v", err)
	}
	if _, err := os.Stat(outputDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("canceled export left target: %v", err)
	}
	assertNoExportTemps(t, dir)
}

func TestExportRejectsForeignKeyViolationBeforeIndexRemoval(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	st := seedExportSource(t, ctx, sourcePath)
	defer st.Close()
	if _, err := st.DB().ExecContext(ctx, `pragma foreign_keys = off`); err != nil {
		t.Fatal(err)
	}
	if _, err := st.DB().ExecContext(ctx, `insert into pull_request_checks(id, thread_id, name, status, raw_json, fetched_at) values(99, 999, 'orphan', 'completed', '{}', '2026-08-09T00:00:00Z')`); err != nil {
		t.Fatalf("seed FK violation: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `pragma foreign_keys = on`); err != nil {
		t.Fatal(err)
	}
	outputDir := filepath.Join(dir, "invalid-fk")
	if _, err := Export(ctx, testExportOptions(sourcePath, outputDir)); err == nil || !strings.Contains(err.Error(), "foreign-key violations") {
		t.Fatalf("foreign key export error = %v", err)
	}
	if _, err := os.Stat(outputDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("foreign key failure left target: %v", err)
	}
	assertNoExportTemps(t, dir)
}

func TestExportRejectsRetainedForeignKeyToDisposableTable(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	st := seedExportSource(t, ctx, sourcePath)
	defer st.Close()
	if _, err := st.DB().ExecContext(ctx, `
		insert into documents(id, thread_id, title, body, raw_text, dedupe_text, updated_at)
		values(1, 1, 'derived', 'body', 'raw', 'dedupe', '2026-08-09T00:00:00Z');
		create table retained_document_links(
			id integer primary key,
			document_id integer not null references documents(id)
		);
	`); err != nil {
		t.Fatalf("seed retained FK to disposable table: %v", err)
	}
	outputDir := filepath.Join(dir, "invalid-retained-fk")
	options := testExportOptions(sourcePath, outputDir)
	var stages []Stage
	options.Progress = func(stage Stage) { stages = append(stages, stage) }
	_, err := Export(ctx, options)
	if err == nil || !strings.Contains(err.Error(), "foreign") {
		t.Fatalf("retained FK export error = %v", err)
	}
	if slices.Contains(stages, Stage("canonical shaping: threads rebuild: preflight")) {
		t.Fatalf("retained FK reached threads rebuild preflight: %v", stages)
	}
	if slices.Contains(stages, Stage("canonical shaping: threads rebuild: compact copy")) {
		t.Fatalf("retained FK reached threads copy: %v", stages)
	}
	if _, err := os.Stat(outputDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("retained FK failure left target: %v", err)
	}
	assertNoExportTemps(t, dir)
}

func TestConfigureDisposableStoreDisablesJournalDurabilityAndSecureDelete(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "staging.db"))
	if err != nil {
		t.Fatalf("open staging store: %v", err)
	}
	defer st.Close()
	if err := configureDisposableStore(ctx, st.DB()); err != nil {
		t.Fatalf("configure disposable store: %v", err)
	}
	var journalMode string
	var synchronous, secureDelete, tempStore int
	if err := st.DB().QueryRowContext(ctx, `pragma journal_mode`).Scan(&journalMode); err != nil {
		t.Fatal(err)
	}
	if err := st.DB().QueryRowContext(ctx, `pragma synchronous`).Scan(&synchronous); err != nil {
		t.Fatal(err)
	}
	if err := st.DB().QueryRowContext(ctx, `pragma secure_delete`).Scan(&secureDelete); err != nil {
		t.Fatal(err)
	}
	if err := st.DB().QueryRowContext(ctx, `pragma temp_store`).Scan(&tempStore); err != nil {
		t.Fatal(err)
	}
	if journalMode != "off" || synchronous != 0 || secureDelete != 0 || tempStore != 2 || st.DB().Stats().MaxOpenConnections != 1 {
		t.Fatalf("disposable settings journal=%q synchronous=%d secure_delete=%d temp_store=%d max_open=%d", journalMode, synchronous, secureDelete, tempStore, st.DB().Stats().MaxOpenConnections)
	}
}

func TestFinalCompactArtifactExcludesDeletedPayloadSentinels(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	st := seedExportSource(t, ctx, sourcePath)
	defer st.Close()
	sentinels := []string{
		"GITCRAWL_REMOVED_HISTORY_SENTINEL_" + strings.Repeat("h", 96),
		"GITCRAWL_REMOVED_RAW_JSON_SENTINEL_" + strings.Repeat("r", 2<<20),
		"GITCRAWL_REMOVED_PR_PATCH_SENTINEL_" + strings.Repeat("p", 96),
		"GITCRAWL_REMOVED_SYNC_FAILURE_SENTINEL_" + strings.Repeat("s", 96),
		"GITCRAWL_REMOVED_FULL_THREAD_BODY_SENTINEL_" + strings.Repeat("b", 2<<20),
	}
	if _, err := st.DB().ExecContext(ctx, `update comment_revisions set body = ? where id = 1`, sentinels[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := st.DB().ExecContext(ctx, `update threads set raw_json = ? where id = 1`, sentinels[1]); err != nil {
		t.Fatal(err)
	}
	if _, err := st.DB().ExecContext(ctx, `update pull_request_files set patch = ? where thread_id = 1`, sentinels[2]); err != nil {
		t.Fatal(err)
	}
	if _, err := st.DB().ExecContext(ctx, `
		insert into sync_attempt_failures(
			id, repo_id, thread_id, number, operation, error_class, error_message,
			first_seen_at, last_seen_at
		) values(1, 1, 1, 7, 'pull_request_details', 'test', ?,
			'2026-08-09T00:00:00Z', '2026-08-09T00:00:00Z')
	`, sentinels[3]); err != nil {
		t.Fatal(err)
	}
	if _, err := st.DB().ExecContext(ctx, `update threads set body = ? where id = 1`, sentinels[4]); err != nil {
		t.Fatal(err)
	}
	result, err := Export(ctx, testExportOptions(sourcePath, filepath.Join(dir, "artifact")))
	if err != nil {
		t.Fatalf("export sentinel fixture: %v", err)
	}
	artifactBytes := readFile(t, result.DatabasePath)
	for index, sentinel := range sentinels {
		if bytes.Contains(artifactBytes, []byte(sentinel)) {
			t.Fatalf("final compact artifact retained deleted sentinel %d", index)
		}
	}
	db := openRawDB(t, result.DatabasePath)
	defer db.Close()
	var title, path, status, body, excerpt, rawJSON string
	var bodyLength int
	var additions int
	var patch sql.NullString
	if err := db.QueryRow(`select title from threads where id = 1`).Scan(&title); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`select body, body_excerpt, body_length, raw_json from threads where id = 1`).Scan(&body, &excerpt, &bodyLength, &rawJSON); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`select path, status, additions, patch from pull_request_files where thread_id = 1`).Scan(&path, &status, &additions, &patch); err != nil {
		t.Fatal(err)
	}
	wantExcerpt := sentinels[4][:32]
	if title != "portable export" || body != wantExcerpt || excerpt != wantExcerpt || bodyLength != len(sentinels[4]) || rawJSON != "" || path != "internal/portable/export.go" || status != "modified" || additions != 10 || patch.Valid {
		t.Fatalf("retained metadata title=%q body=%q excerpt=%q length=%d raw=%q file=%q/%q/%d patch=%#v", title, body, excerpt, bodyLength, rawJSON, path, status, additions, patch)
	}
}

func TestCompactGenerationReplacesUncompactWorkingDatabase(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	workingPath := filepath.Join(dir, "working.db")
	st, err := store.Open(ctx, workingPath)
	if err != nil {
		t.Fatalf("open working store: %v", err)
	}
	if err := configureDisposableStore(ctx, st.DB()); err != nil {
		t.Fatalf("configure working store: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `create table compact_payload(value blob); insert into compact_payload values(zeroblob(2097152)); delete from compact_payload`); err != nil {
		t.Fatalf("seed free space: %v", err)
	}
	workingInfo, err := os.Stat(workingPath)
	if err != nil {
		t.Fatal(err)
	}
	compactPath, err := createCompactDatabase(ctx, st.DB(), workingPath)
	if err != nil {
		t.Fatalf("create compact generation: %v", err)
	}
	compactInfo, err := os.Stat(compactPath)
	if err != nil {
		t.Fatal(err)
	}
	if compactInfo.Size() >= workingInfo.Size() {
		t.Fatalf("compact bytes = %d, uncompact bytes = %d", compactInfo.Size(), workingInfo.Size())
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close working store: %v", err)
	}
	if err := replaceWithCompactDatabase(ctx, workingPath, compactPath); err != nil {
		t.Fatalf("replace with compact generation: %v", err)
	}
	if _, err := os.Stat(compactPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("compact temp still exists: %v", err)
	}
	db := openRawDB(t, workingPath)
	defer db.Close()
	if check, err := checkPragma(ctx, db, "quick_check"); err != nil || check != "ok" {
		t.Fatalf("promoted compact quick_check=%q err=%v", check, err)
	}
}

func TestCompactGenerationCancellationCleansPartialFile(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	workingPath := filepath.Join(dir, "working.db")
	st, err := store.Open(ctx, workingPath)
	if err != nil {
		t.Fatalf("open working store: %v", err)
	}
	defer st.Close()
	if err := configureDisposableStore(ctx, st.DB()); err != nil {
		t.Fatalf("configure working store: %v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := createCompactDatabase(canceled, st.DB(), workingPath); err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled compact generation error = %v", err)
	}
	matches, err := filepath.Glob(filepath.Join(dir, ".working.db.compact-*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 0 {
		t.Fatalf("partial compact files remain: %v", matches)
	}
	if _, err := os.Stat(workingPath); err != nil {
		t.Fatalf("working database removed on compact cancellation: %v", err)
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
	for _, column := range []struct{ table, name string }{
		{table: "comments", name: "raw_json_blob_id"},
		{table: "thread_revisions", name: "raw_json_blob_id"},
	} {
		if !columnExists(t, writable.DB(), column.table, column.name) {
			t.Fatalf("writable reopen did not restore %s.%s", column.table, column.name)
		}
	}
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
	if _, err := writable.DB().ExecContext(ctx, `update threads set body = 'writable body', raw_json = '{"writable":true}' where id = 1`); err != nil {
		t.Fatalf("write retained thread columns: %v", err)
	}
	if _, err := writable.DB().ExecContext(ctx, `update repositories set raw_json = '{"writable":true}' where id = 1`); err != nil {
		t.Fatalf("write retained repository column: %v", err)
	}
	var body, threadRaw, repositoryRaw string
	if err := writable.DB().QueryRowContext(ctx, `select body, raw_json from threads where id = 1`).Scan(&body, &threadRaw); err != nil {
		t.Fatalf("read retained thread columns: %v", err)
	}
	if err := writable.DB().QueryRowContext(ctx, `select raw_json from repositories where id = 1`).Scan(&repositoryRaw); err != nil {
		t.Fatalf("read retained repository column: %v", err)
	}
	if body != "writable body" || threadRaw != `{"writable":true}` || repositoryRaw != `{"writable":true}` {
		t.Fatalf("retained writable columns body=%q thread_raw=%q repository_raw=%q", body, threadRaw, repositoryRaw)
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
		`insert into pull_request_files(thread_id, position, path, status, additions, deletions, changes, previous_path, patch, raw_json, fetched_at) values(1, 0, 'internal/portable/export.go', 'modified', 10, 2, 12, 'internal/export.go', '@@ retained repository patch', '{}', '2026-08-08T00:00:00Z')`,
		`insert into pull_request_commits(thread_id, sha, message, author_login, author_name, committed_at, html_url, raw_json, fetched_at, deleted_at, deletion_reason) values(1, 'abc123', 'portable commit', 'octocat', 'Octo Cat', '2026-08-07T00:00:00Z', 'https://github.com/openclaw/gitcrawl/commit/abc123', '{}', '2026-08-08T00:00:00Z', '2026-08-08T01:00:00Z', 'not returned by source')`,
		`insert into pull_request_checks(id, thread_id, name, status, raw_json, fetched_at) values(1, 1, 'test', 'completed', '{}', '2026-08-08T00:00:00Z')`,
		`insert into pull_request_review_threads(thread_id, review_thread_id, path, first_comment_body, comments_json, raw_json, fetched_at, deleted_at, deletion_reason) values(1, 'RT1', 'internal/portable/export.go', 'review body', '[{"body":"review body"}]', '{}', '2026-08-08T00:00:00Z', '2026-08-08T01:00:00Z', 'not returned by source')`,
		`insert into pull_request_review_thread_revisions(id, thread_id, review_thread_id, path, first_comment_body, comments_json, raw_json, fetched_at, deleted_at, deletion_reason, recorded_at) values(1, 1, 'RT1', 'internal/portable/export.go', 'review body', '[{"body":"review body"}]', '{}', '2026-08-08T00:00:00Z', '2026-08-08T01:00:00Z', 'not returned by source', '2026-08-08T02:00:00Z')`,
		`insert into pull_request_review_thread_syncs(thread_id, fetched_at) values(1, '2026-08-08T00:00:00Z')`,
		`insert into github_workflow_runs(repo_id, run_id, run_number, head_branch, head_sha, status, conclusion, workflow_name, event, html_url, created_at_gh, updated_at_gh, raw_json, fetched_at) values(1, 'RUN1', 1, 'main', 'abc123', 'completed', 'success', 'CI', 'pull_request', 'https://github.com/openclaw/gitcrawl/actions/runs/1', '2026-08-07T00:00:00Z', '2026-08-07T00:01:00Z', '{}', '2026-08-08T00:00:00Z')`,
		`update thread_observation_sequence set value = 7, last_started_at = '2026-08-08T00:00:00Z' where id = 1`,
		`insert into thread_child_observation_reservations(thread_id, family, source_updated_at, observation_sequence) values(1, 'comments', '2026-08-07T00:00:00Z', 7)`,
		`insert into thread_child_observation_memberships(thread_id, family, observation_sequence, member_ids_json) values(1, 'comments', 1, '["C1"]')`,
		`insert into workflow_run_observation_reservations(repo_id, head_sha, source_updated_at, observation_sequence) values(1, 'abc123', '2026-08-07T00:00:00Z', 7)`,
		`insert into repo_sync_state(repo_id, last_full_open_scan_started_at, updated_at) values(1, '2026-08-08T00:00:00Z', '2026-08-08T00:00:00Z')`,
		`insert into cluster_groups(id, repo_id, stable_key, stable_slug, status, created_at, updated_at) values(1, 1, 'key', 'slug', 'open', '2026-08-08T00:00:00Z', '2026-08-08T00:00:00Z')`,
		`insert into cluster_memberships(cluster_id, thread_id, role, state, added_by, added_reason_json, created_at, updated_at) values(1, 1, 'member', 'active', 'system', '{}', '2026-08-08T00:00:00Z', '2026-08-08T00:00:00Z')`,
		`insert into cluster_overrides(id, repo_id, cluster_id, thread_id, action, created_at) values(1, 1, 1, 1, 'exclude', '2026-08-08T00:00:00Z')`,
		`insert into cluster_aliases(cluster_id, alias_slug, reason, created_at) values(1, 'old-slug', 'lineage', '2026-08-08T00:00:00Z')`,
		`insert into cluster_closures(cluster_id, reason, actor_kind, created_at, updated_at) values(1, 'local', 'human', '2026-08-08T00:00:00Z', '2026-08-08T00:00:00Z')`,
		`create index custom_comments_author on comments(author_login)`,
		`create index custom_comment_revisions_body on comment_revisions(body)`,
		`create unique index unique_comments_github on comments(github_id)`,
		`create index custom_threads_title on threads(title)`,
		`create unique index unique_threads_github_id on threads(github_id)`,
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

func seedSecondExportRepository(t *testing.T, ctx context.Context, st *store.Store) {
	t.Helper()
	statements := []string{
		`insert into repositories(id, owner, name, full_name, raw_json, updated_at) values(2, 'openclaw', 'other', 'openclaw/other', '{}', '2026-08-08T00:00:00Z')`,
		`insert into threads(id, repo_id, github_id, number, kind, state, title, body, html_url, labels_json, assignees_json, raw_json, content_hash, updated_at) values(2, 2, 'T2', 8, 'pull_request', 'open', 'other repository', 'other body', 'https://github.com/openclaw/other/pull/8', '[]', '[]', '{}', 'other-hash', '2026-08-08T00:00:00Z')`,
		`insert into comments(id, thread_id, github_id, comment_type, body, raw_json) values(2, 2, 'C2', 'issue_comment', 'other-comment', '{}')`,
		`insert into comment_revisions(id, comment_id, body, raw_json, recorded_at) values(2, 2, 'other-history', '{}', '2026-08-08T00:00:00Z')`,
		`insert into thread_revisions(id, thread_id, content_hash, title_hash, body_hash, labels_hash, created_at) values(2, 2, 'other-content', 'other-title', 'other-body', 'other-labels', '2026-08-08T00:00:00Z')`,
		`insert into thread_fingerprints(id, thread_revision_id, algorithm_version, fingerprint_hash, fingerprint_slug, title_tokens_json, body_token_hash, linked_refs_json, file_set_hash, module_buckets_json, simhash64, feature_json, created_at) values(2, 2, 'v1', 'other-fingerprint', 'other', '[]', 'other-bodyhash', '[]', 'other-files', '[]', '2', '{}', '2026-08-08T00:00:00Z')`,
		`insert into thread_key_summaries(id, thread_revision_id, summary_kind, prompt_version, provider, model, input_hash, output_hash, key_text, created_at) values(2, 2, 'llm_key', 'v1', 'openai', 'test', 'other-in', 'other-out', 'other enrichment', '2026-08-08T00:00:00Z')`,
		`insert into pull_request_details(thread_id, repo_id, number, raw_json, fetched_at, updated_at) values(2, 2, 8, '{}', '2026-08-08T00:00:00Z', '2026-08-08T00:00:00Z')`,
		`insert into pull_request_files(thread_id, position, path, status, additions, deletions, changes, previous_path, patch, raw_json, fetched_at) values(2, 0, 'other.go', 'added', 4, 0, 4, null, '@@ other repository patch', '{}', '2026-08-08T00:00:00Z')`,
		`insert into pull_request_checks(id, thread_id, name, status, raw_json, fetched_at) values(2, 2, 'test', 'completed', '{}', '2026-08-08T00:00:00Z')`,
		`insert into thread_child_observation_memberships(thread_id, family, observation_sequence, member_ids_json) values(2, 'comments', 2, '["C2"]')`,
		`insert into cluster_groups(id, repo_id, stable_key, stable_slug, status, created_at, updated_at) values(2, 2, 'other-key', 'other-slug', 'open', '2026-08-08T00:00:00Z', '2026-08-08T00:00:00Z')`,
		`insert into cluster_memberships(cluster_id, thread_id, role, state, added_by, added_reason_json, created_at, updated_at) values(2, 2, 'member', 'active', 'system', '{}', '2026-08-08T00:00:00Z', '2026-08-08T00:00:00Z')`,
		`insert into cluster_overrides(id, repo_id, cluster_id, thread_id, action, created_at) values(2, 2, 2, 2, 'exclude', '2026-08-08T00:00:00Z')`,
		`insert into cluster_aliases(cluster_id, alias_slug, reason, created_at) values(2, 'other-old-slug', 'lineage', '2026-08-08T00:00:00Z')`,
		`insert into cluster_closures(cluster_id, reason, actor_kind, created_at, updated_at) values(2, 'local', 'human', '2026-08-08T00:00:00Z', '2026-08-08T00:00:00Z')`,
	}
	tx, err := st.DB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin second repository seed: %v", err)
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			_ = tx.Rollback()
			t.Fatalf("seed second repository with %q: %v", statement, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit second repository seed: %v", err)
	}
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

func testForeignKeyViolationCount(ctx context.Context, db *sql.DB) (int, error) {
	rows, err := db.QueryContext(ctx, `pragma foreign_key_check`)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
	}
	return count, rows.Err()
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

func columnExists(t *testing.T, db *sql.DB, table, column string) bool {
	t.Helper()
	rows, err := db.Query(`pragma table_info(` + quoteIdentifier(table) + `)`)
	if err != nil {
		t.Fatalf("inspect columns for %s: %v", table, err)
	}
	defer rows.Close()
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, columnType string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			t.Fatalf("scan columns for %s: %v", table, err)
		}
		if name == column {
			return true
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read columns for %s: %v", table, err)
	}
	return false
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

func assertManifestTableCounts(t *testing.T, db *sql.DB, tables []Table) {
	t.Helper()
	if len(tables) == 0 {
		t.Fatal("manifest tables are empty")
	}
	previous := ""
	for _, table := range tables {
		if previous != "" && table.Name <= previous {
			t.Fatalf("manifest tables not sorted: %q before %q", previous, table.Name)
		}
		if got := int64(rowCount(t, db, table.Name)); got != table.Rows {
			t.Fatalf("manifest table %s rows = %d, database = %d", table.Name, table.Rows, got)
		}
		previous = table.Name
	}
}

func assertPortablePRFilePatchStripped(t *testing.T, db *sql.DB) {
	t.Helper()
	var path, status, previousPath string
	var additions, deletions, changes int
	var patch sql.NullString
	if err := db.QueryRow(`
		select path, status, additions, deletions, changes, previous_path, patch
		from pull_request_files
	`).Scan(&path, &status, &additions, &deletions, &changes, &previousPath, &patch); err != nil {
		t.Fatalf("read portable PR file: %v", err)
	}
	if path != "internal/portable/export.go" || status != "modified" || additions != 10 || deletions != 2 || changes != 12 || previousPath != "internal/export.go" || patch.Valid {
		t.Fatalf("portable PR file=%q/%q/%d/%d/%d/%q patch=%#v", path, status, additions, deletions, changes, previousPath, patch)
	}
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
