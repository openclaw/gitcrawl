package portable

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestValidateManifestPairRejectsInconsistentArtifacts(t *testing.T) {
	tests := []struct {
		name      string
		wantError string
		mutate    func(t *testing.T, dbPath, manifestPath string, manifest *Manifest)
	}{
		{
			name:      "manifest missing",
			wantError: "re-read portable manifest",
			mutate: func(t *testing.T, _, manifestPath string, _ *Manifest) {
				t.Helper()
				if err := os.Remove(manifestPath); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:      "malformed manifest",
			wantError: "re-read portable manifest",
			mutate: func(t *testing.T, _, manifestPath string, _ *Manifest) {
				t.Helper()
				if err := os.WriteFile(manifestPath, []byte("{"), 0o600); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:      "manifest changed",
			wantError: "changed during finalization",
			mutate: func(t *testing.T, _, manifestPath string, manifest *Manifest) {
				t.Helper()
				actual := *manifest
				actual.Profile = "changed-profile"
				writeTestManifest(t, manifestPath, actual)
			},
		},
		{
			name:      "database missing",
			wantError: "re-stat portable database",
			mutate: func(t *testing.T, dbPath, _ string, _ *Manifest) {
				t.Helper()
				if err := os.Remove(dbPath); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:      "size mismatch",
			wantError: "does not match database size",
			mutate: func(t *testing.T, _, manifestPath string, manifest *Manifest) {
				manifest.OutputBytes++
				writeTestManifest(t, manifestPath, *manifest)
			},
		},
		{
			name:      "digest mismatch",
			wantError: "digest does not match",
			mutate: func(t *testing.T, _, manifestPath string, manifest *Manifest) {
				manifest.SHA256 = strings.Repeat("0", 64)
				manifest.ArtifactID = manifest.SHA256
				writeTestManifest(t, manifestPath, *manifest)
			},
		},
		{
			name:      "database is not SQLite",
			wantError: "file is not a database",
			mutate: func(t *testing.T, dbPath, manifestPath string, manifest *Manifest) {
				if err := os.WriteFile(dbPath, []byte("not a sqlite database"), 0o600); err != nil {
					t.Fatal(err)
				}
				info, err := os.Stat(dbPath)
				if err != nil {
					t.Fatal(err)
				}
				manifest.OutputBytes = info.Size()
				manifest.SHA256 = hashFile(t, dbPath)
				manifest.ArtifactID = manifest.SHA256
				writeTestManifest(t, manifestPath, *manifest)
			},
		},
		{
			name:      "quick check mismatch",
			wantError: "quickCheck does not match",
			mutate: func(t *testing.T, _, manifestPath string, manifest *Manifest) {
				manifest.QuickCheck = "not ok"
				writeTestManifest(t, manifestPath, *manifest)
			},
		},
		{
			name:      "integrity check mismatch",
			wantError: "integrityCheck does not match",
			mutate: func(t *testing.T, _, manifestPath string, manifest *Manifest) {
				manifest.IntegrityCheck = "not ok"
				writeTestManifest(t, manifestPath, *manifest)
			},
		},
		{
			name:      "foreign key violations",
			wantError: "records 1 foreign-key violations",
			mutate: func(t *testing.T, _, manifestPath string, manifest *Manifest) {
				manifest.ForeignKeyViolations = 1
				writeTestManifest(t, manifestPath, *manifest)
			},
		},
		{
			name:      "table counts mismatch",
			wantError: "table counts do not match",
			mutate: func(t *testing.T, _, manifestPath string, manifest *Manifest) {
				manifest.Tables[0].Rows++
				writeTestManifest(t, manifestPath, *manifest)
			},
		},
		{
			name:      "repository mismatch",
			wantError: "repository does not match",
			mutate: func(t *testing.T, _, manifestPath string, manifest *Manifest) {
				manifest.Repository.FullName = "openclaw/other"
				writeTestManifest(t, manifestPath, *manifest)
			},
		},
		{
			name:      "metadata missing",
			wantError: "validate portable metadata profile",
			mutate: func(t *testing.T, dbPath, manifestPath string, manifest *Manifest) {
				db := openRawDB(t, dbPath)
				if _, err := db.Exec(`delete from portable_metadata where key = 'profile'`); err != nil {
					t.Fatal(err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				refreshTestManifest(t, dbPath, manifest)
				writeTestManifest(t, manifestPath, *manifest)
			},
		},
		{
			name:      "metadata mismatch",
			wantError: "does not match manifest",
			mutate: func(t *testing.T, dbPath, manifestPath string, manifest *Manifest) {
				db := openRawDB(t, dbPath)
				if _, err := db.Exec(`update portable_metadata set value = 'other' where key = 'column_profile'`); err != nil {
					t.Fatal(err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				refreshTestManifest(t, dbPath, manifest)
				writeTestManifest(t, manifestPath, *manifest)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dbPath, manifestPath, manifest := newTestManifestPair(t)
			test.mutate(t, dbPath, manifestPath, &manifest)
			err := validateManifestPair(context.Background(), dbPath, manifestPath, manifest)
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("validate manifest pair error = %v, want %q", err, test.wantError)
			}
		})
	}
}

func TestValidateManifestPairReportsUnreadableDatabaseContent(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "artifact.db")
	if err := os.Mkdir(dbPath, 0o700); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	manifest := Manifest{OutputBytes: info.Size(), SHA256: strings.Repeat("0", 64), ArtifactID: strings.Repeat("0", 64)}
	manifestPath := filepath.Join(dir, "artifact.db.manifest.json")
	writeTestManifest(t, manifestPath, manifest)
	err = validateManifestPair(context.Background(), dbPath, manifestPath, manifest)
	if err == nil || !strings.Contains(err.Error(), "re-hash portable database") {
		t.Fatalf("unreadable database content error = %v", err)
	}
}

func TestSQLiteSnapshotAndCompactGenerationRejectUnsafeTargets(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	db := openRawDB(t, sourcePath)
	if _, err := db.Exec(`create table payload(value text); insert into payload values('kept')`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	if err := snapshotSQLiteWithOptions(ctx, sourcePath, filepath.Join(dir, "zero.db"), onlineBackupOptions{}); err == nil || !strings.Contains(err.Error(), "must be positive") {
		t.Fatalf("zero-page snapshot error = %v", err)
	}
	existing := filepath.Join(dir, "existing.db")
	if err := os.WriteFile(existing, []byte("occupied"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := snapshotSQLite(ctx, sourcePath, existing); err == nil || !strings.Contains(err.Error(), "target already exists") {
		t.Fatalf("existing snapshot target error = %v", err)
	}
	missingTarget := filepath.Join(dir, "missing", "snapshot.db")
	if err := snapshotSQLite(ctx, sourcePath, missingTarget); err == nil || !strings.Contains(err.Error(), "start online backup") {
		t.Fatalf("missing-parent snapshot error = %v", err)
	}
	if _, err := os.Stat(missingTarget); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed snapshot left target: %v", err)
	}
	if err := snapshotSQLite(ctx, filepath.Join(dir, "missing-source.db"), filepath.Join(dir, "missing-source-copy.db")); err == nil {
		t.Fatal("missing snapshot source succeeded")
	}

	closed := openRawDB(t, sourcePath)
	if err := closed.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := createCompactDatabase(ctx, closed, filepath.Join(dir, "working.db")); err == nil || !strings.Contains(err.Error(), "create compact portable database") {
		t.Fatalf("closed compact source error = %v", err)
	}
	parentFile := filepath.Join(dir, "not-a-directory")
	if err := os.WriteFile(parentFile, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	invalidParentDB := openRawDB(t, sourcePath)
	defer invalidParentDB.Close()
	if _, err := createCompactDatabase(ctx, invalidParentDB, filepath.Join(parentFile, "working.db")); err == nil || !strings.Contains(err.Error(), "reserve compact") {
		t.Fatalf("invalid compact directory error = %v", err)
	}
}

func TestSQLiteSnapshotCancellationCleansDatabaseAndReportsRetainedSidecars(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	db := openRawDB(t, sourcePath)
	if _, err := db.Exec(`create table payload(value blob); insert into payload values(zeroblob(1048576))`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	targetPath := filepath.Join(dir, "target.db")
	ctx, cancel := context.WithCancel(context.Background())
	err := snapshotSQLiteWithOptions(ctx, sourcePath, targetPath, onlineBackupOptions{
		PagesPerStep: 1,
		AfterStep: func(_, _ int) {
			for _, suffix := range []string{"-wal", "-shm"} {
				sidecar := targetPath + suffix
				if mkdirErr := os.Mkdir(sidecar, 0o700); mkdirErr != nil {
					t.Errorf("create retained sidecar: %v", mkdirErr)
					continue
				}
				if writeErr := os.WriteFile(filepath.Join(sidecar, "retained"), []byte("x"), 0o600); writeErr != nil {
					t.Errorf("seed retained sidecar: %v", writeErr)
				}
			}
			cancel()
		},
	})
	if err == nil || !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "remove partial snapshot sidecar") {
		t.Fatalf("canceled snapshot cleanup error = %v", err)
	}
	if _, statErr := os.Stat(targetPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("canceled snapshot left database: %v", statErr)
	}
}

func TestSQLiteSnapshotPreCanceledContextLeavesNoTarget(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	db := openRawDB(t, sourcePath)
	if _, err := db.Exec(`create table payload(value text)`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	targetPath := filepath.Join(dir, "target.db")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := snapshotSQLiteWithOptions(ctx, sourcePath, targetPath, onlineBackupOptions{PagesPerStep: 1})
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled snapshot error = %v", err)
	}
	if _, statErr := os.Stat(targetPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("pre-canceled snapshot left target: %v", statErr)
	}
}

func TestSQLiteSnapshotReportsPartialTargetCleanupFailure(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("replacing an open SQLite file exercises Unix unlink cleanup semantics")
	}
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	db := openRawDB(t, sourcePath)
	if _, err := db.Exec(`create table payload(value blob); insert into payload values(zeroblob(1048576))`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	targetPath := filepath.Join(dir, "target.db")
	ctx, cancel := context.WithCancel(context.Background())
	err := snapshotSQLiteWithOptions(ctx, sourcePath, targetPath, onlineBackupOptions{
		PagesPerStep: 1,
		AfterStep: func(_, _ int) {
			if removeErr := os.Remove(targetPath); removeErr != nil {
				t.Errorf("unlink partial target: %v", removeErr)
				cancel()
				return
			}
			if mkdirErr := os.Mkdir(targetPath, 0o700); mkdirErr != nil {
				t.Errorf("replace partial target with directory: %v", mkdirErr)
				cancel()
				return
			}
			if writeErr := os.WriteFile(filepath.Join(targetPath, "retained"), []byte("x"), 0o600); writeErr != nil {
				t.Errorf("seed retained partial target: %v", writeErr)
			}
			cancel()
		},
	})
	if err == nil || !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "remove partial snapshot") {
		t.Fatalf("partial target cleanup error = %v", err)
	}
}

func TestCompactReplacementFailsClosed(t *testing.T) {
	ctx := context.Background()
	t.Run("invalid candidate", func(t *testing.T) {
		dir := t.TempDir()
		working := filepath.Join(dir, "working.db")
		candidate := filepath.Join(dir, "candidate.db")
		if err := os.WriteFile(working, []byte("working"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(candidate, []byte("not sqlite"), 0o600); err != nil {
			t.Fatal(err)
		}
		err := replaceWithCompactDatabase(ctx, working, candidate)
		if err == nil || !strings.Contains(err.Error(), "validate compact database candidate") {
			t.Fatalf("invalid compact candidate error = %v", err)
		}
		if got := string(readFile(t, working)); got != "working" {
			t.Fatalf("invalid candidate changed working database to %q", got)
		}
	})

	t.Run("missing working database", func(t *testing.T) {
		dir := t.TempDir()
		candidate := filepath.Join(dir, "candidate.db")
		db := openRawDB(t, candidate)
		if _, err := db.Exec(`create table payload(value text)`); err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		err := replaceWithCompactDatabase(ctx, filepath.Join(dir, "missing.db"), candidate)
		if err == nil || !strings.Contains(err.Error(), "remove uncompact portable database") {
			t.Fatalf("missing working database error = %v", err)
		}
		if _, statErr := os.Stat(candidate); statErr != nil {
			t.Fatalf("failed replacement removed candidate: %v", statErr)
		}
	})

	t.Run("aliased paths", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "same.db")
		db := openRawDB(t, path)
		if _, err := db.Exec(`create table payload(value text)`); err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		if err := replaceWithCompactDatabase(ctx, path, path); err == nil || !strings.Contains(err.Error(), "promote compact portable database") {
			t.Fatalf("aliased replacement error = %v", err)
		}
	})
}

func TestPortableFileHelpersRejectUnsafeDestinations(t *testing.T) {
	dir := t.TempDir()
	existing := filepath.Join(dir, "existing")
	if err := os.WriteFile(existing, []byte("original"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := writeSyncedFile(existing, []byte("replacement"), 0o600); err == nil {
		t.Fatal("exclusive synced write replaced an existing file")
	}
	if got := string(readFile(t, existing)); got != "original" {
		t.Fatalf("exclusive synced write changed existing file to %q", got)
	}
	if err := writeSyncedFile(filepath.Join(dir, "missing", "file"), []byte("data"), 0o600); err == nil {
		t.Fatal("synced write into missing parent succeeded")
	}
	if err := syncRegularFile(filepath.Join(dir, "missing")); err == nil {
		t.Fatal("sync of missing file succeeded")
	}
	if _, err := fileSHA256(filepath.Join(dir, "missing")); err == nil {
		t.Fatal("hash of missing file succeeded")
	}
	if _, err := fileSHA256(dir); err == nil {
		t.Fatal("hash of directory succeeded")
	}
}

func TestPortableTableDropFailsClosedOnForeignKeyConstraint(t *testing.T) {
	ctx := context.Background()
	db := openRawDB(t, filepath.Join(t.TempDir(), "drop.db"))
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(ctx, `
		pragma foreign_keys = on;
		create table parent(id integer primary key);
		create table child(parent_id integer references parent(id));
		insert into parent values(1);
		insert into child values(1);
	`); err != nil {
		t.Fatal(err)
	}
	dropped, err := dropTableIfPresent(ctx, db, "parent")
	if err == nil || dropped || !strings.Contains(err.Error(), "drop portable table parent") {
		t.Fatalf("foreign-key constrained drop = %v, %v", dropped, err)
	}
	if !tableExists(t, db, "parent") {
		t.Fatal("failed constrained drop removed parent table")
	}
	if dropped, err := dropTableIfPresent(ctx, db, "missing"); err != nil || dropped {
		t.Fatalf("missing table drop = %v, %v", dropped, err)
	}
}

func TestCompactReplacementRejectsRetainedSidecars(t *testing.T) {
	ctx := context.Background()
	for _, target := range []string{"candidate", "working"} {
		t.Run(target, func(t *testing.T) {
			dir := t.TempDir()
			working := filepath.Join(dir, "working.db")
			candidate := filepath.Join(dir, "candidate.db")
			for _, path := range []string{working, candidate} {
				db := openRawDB(t, path)
				if _, err := db.Exec(`create table payload(value text)`); err != nil {
					t.Fatal(err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
			}
			sidecarBase := working
			if target == "candidate" {
				sidecarBase = candidate
			}
			sidecar := sidecarBase + "-wal"
			if err := os.Mkdir(sidecar, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(sidecar, "retained"), []byte("x"), 0o600); err != nil {
				t.Fatal(err)
			}
			err := replaceWithCompactDatabase(ctx, working, candidate)
			wantError := "remove portable SQLite sidecar"
			if target == "candidate" {
				wantError = "unable to open database file"
			}
			if err == nil || !strings.Contains(err.Error(), wantError) {
				t.Fatalf("retained %s sidecar error = %v", target, err)
			}
			if _, statErr := os.Stat(working); statErr != nil {
				t.Fatalf("retained sidecar removed working database: %v", statErr)
			}
		})
	}
}

func TestAppendUniquePreservesFirstOccurrence(t *testing.T) {
	values := []string{"threads", "repositories"}
	got := appendUnique(values, "threads")
	if len(got) != 2 || got[0] != "threads" || got[1] != "repositories" {
		t.Fatalf("duplicate append changed slice: %v", got)
	}
}

func TestPortableDatabaseHelpersPropagateCancellation(t *testing.T) {
	db := openRawDB(t, filepath.Join(t.TempDir(), "canceled.db"))
	defer db.Close()
	if _, err := db.Exec(`create table disposable(id integer)`); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	checks := []struct {
		name string
		run  func() error
	}{
		{name: "configure", run: func() error { return configureDisposableStore(ctx, db) }},
		{name: "indexes", run: func() error { _, err := ordinaryNonUniqueIndexes(ctx, db); return err }},
		{name: "tables", run: func() error { _, err := databaseTableNames(ctx, db); return err }},
		{name: "table stats", run: func() error { _, err := databaseTableStats(ctx, db); return err }},
		{name: "repository", run: func() error { _, err := singleRepository(ctx, db); return err }},
		{name: "verify repository", run: func() error { return verifyRepository(ctx, db, Repository{FullName: "openclaw/gitcrawl"}) }},
		{name: "drop table", run: func() error { _, err := dropTableIfPresent(ctx, db, "disposable"); return err }},
		{name: "metadata", run: func() error { return writeMetadata(ctx, db, map[string]string{"schema": "portable-v1"}) }},
		{name: "pragma", run: func() error { _, err := checkPragma(ctx, db, "quick_check"); return err }},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			if err := check.run(); err == nil {
				t.Fatal("canceled operation succeeded")
			}
		})
	}
}

func TestExportCancellationAtSafetyBoundariesCleansStaging(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	st := seedExportSource(t, context.Background(), sourcePath)
	defer st.Close()
	stages := []Stage{
		StageSnapshot,
		StageRepositoryScope,
		StageProfileOmissions,
		StageIndexRemoval,
		StageFinalVacuum,
		StageValidation,
		StageManifest,
		StageArtifactCommit,
	}
	for _, stage := range stages {
		t.Run(string(stage), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			outputDir := filepath.Join(dir, "cancel-"+strings.ReplaceAll(string(stage), " ", "-"))
			options := testExportOptions(sourcePath, outputDir)
			options.Progress = func(got Stage) {
				if got == stage {
					cancel()
				}
			}
			_, err := Export(ctx, options)
			if err == nil || !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), string(stage)) {
				t.Fatalf("cancellation at %q error = %v", stage, err)
			}
			if _, statErr := os.Stat(outputDir); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("cancellation at %q left output: %v", stage, statErr)
			}
			assertNoExportTemps(t, dir)
		})
	}
}

func TestExportRejectsInvalidSourcesBudgetsAndOutputParents(t *testing.T) {
	dir := t.TempDir()
	validSource := filepath.Join(dir, "source.db")
	st := seedExportSource(t, context.Background(), validSource)
	defer st.Close()

	tests := []struct {
		name      string
		options   ExportOptions
		wantError string
	}{
		{
			name: "default body budget",
			options: func() ExportOptions {
				options := testExportOptions(validSource, filepath.Join(dir, "default-body"))
				options.BodyChars = 0
				options.Progress = func(Stage) {}
				return options
			}(),
			wantError: "portable export canceled during snapshot",
		},
		{
			name:      "unknown profile",
			options:   ExportOptions{Profile: "unknown"},
			wantError: "unsupported portable export profile",
		},
		{
			name: "unsafe database name",
			options: func() ExportOptions {
				options := testExportOptions(validSource, filepath.Join(dir, "unsafe-name"))
				options.DatabaseName = "../unsafe.db"
				return options
			}(),
			wantError: "database-name must be a safe basename",
		},
		{
			name: "unsafe public path",
			options: func() ExportOptions {
				options := testExportOptions(validSource, filepath.Join(dir, "unsafe-public-path"))
				options.PublicPath = "../unsafe.db"
				return options
			}(),
			wantError: "public-path must be a clean relative slash path",
		},
		{
			name: "zero budget",
			options: func() ExportOptions {
				options := testExportOptions(validSource, filepath.Join(dir, "zero-budget"))
				zero := int64(0)
				options.MaxBytes = &zero
				return options
			}(),
			wantError: "max-bytes must be a positive integer",
		},
		{
			name:      "missing source",
			options:   testExportOptions(filepath.Join(dir, "missing.db"), filepath.Join(dir, "missing-source")),
			wantError: "stat source database",
		},
		{
			name:      "source directory",
			options:   testExportOptions(dir, filepath.Join(dir, "source-directory")),
			wantError: "source database is not a regular file",
		},
		{
			name: "output parent is file",
			options: func() ExportOptions {
				parent := filepath.Join(dir, "parent-file")
				if err := os.WriteFile(parent, []byte("x"), 0o600); err != nil {
					t.Fatal(err)
				}
				return testExportOptions(validSource, filepath.Join(parent, "artifact"))
			}(),
			wantError: "inspect output directory",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			if test.name == "default body budget" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			if _, err := Export(ctx, test.options); err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("export error = %v, want %q", err, test.wantError)
			}
		})
	}
}

func TestExportCommitRacesFailWithoutPublishingPartialPair(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	st := seedExportSource(t, ctx, sourcePath)
	defer st.Close()

	t.Run("target appears before final inspection", func(t *testing.T) {
		outputDir := filepath.Join(dir, "before-inspection")
		e := exporter{beforeCommit: func() error { return os.Mkdir(outputDir, 0o700) }}
		_, err := e.export(ctx, testExportOptions(sourcePath, outputDir))
		if err == nil || !strings.Contains(err.Error(), "output directory already exists") {
			t.Fatalf("pre-commit target race error = %v", err)
		}
		if _, err := os.Stat(filepath.Join(outputDir, "gitcrawl.db")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("pre-commit race published database: %v", err)
		}
		assertNoExportTemps(t, dir)
	})

	t.Run("target appears after final inspection", func(t *testing.T) {
		outputDir := filepath.Join(dir, "after-inspection")
		options := testExportOptions(sourcePath, outputDir)
		options.Progress = func(stage Stage) {
			if stage == StageArtifactCommit {
				if err := os.Mkdir(outputDir, 0o700); err != nil {
					t.Errorf("create racing output: %v", err)
				}
			}
		}
		_, err := Export(ctx, options)
		if err == nil || !strings.Contains(err.Error(), "commit portable artifact") {
			t.Fatalf("rename target race error = %v", err)
		}
		if _, err := os.Stat(filepath.Join(outputDir, "gitcrawl.db")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("rename race published database: %v", err)
		}
		assertNoExportTemps(t, dir)
	})
}

func TestExportLateFileFailuresLeaveNoArtifact(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	st := seedExportSource(t, ctx, sourcePath)
	defer st.Close()

	t.Run("manifest already exists", func(t *testing.T) {
		outputDir := filepath.Join(dir, "existing-manifest")
		e := exporter{beforeManifest: func() error {
			matches, err := filepath.Glob(filepath.Join(dir, ".gitcrawl-portable-export-*"))
			if err != nil || len(matches) != 1 {
				return errors.New("could not locate staging directory")
			}
			return os.WriteFile(filepath.Join(matches[0], "gitcrawl.db.manifest.json"), []byte("occupied"), 0o600)
		}}
		_, err := e.export(ctx, testExportOptions(sourcePath, outputDir))
		if err == nil || !strings.Contains(err.Error(), "write portable manifest") {
			t.Fatalf("existing manifest error = %v", err)
		}
		assertNoExportTemps(t, dir)
	})

	t.Run("database removed before sync", func(t *testing.T) {
		outputDir := filepath.Join(dir, "missing-database")
		e := exporter{beforeManifest: func() error {
			matches, err := filepath.Glob(filepath.Join(dir, ".gitcrawl-portable-export-*", "gitcrawl.db"))
			if err != nil || len(matches) != 1 {
				return errors.New("could not locate staging database")
			}
			return os.Remove(matches[0])
		}}
		_, err := e.export(ctx, testExportOptions(sourcePath, outputDir))
		if err == nil || !strings.Contains(err.Error(), "sync portable database") {
			t.Fatalf("missing database sync error = %v", err)
		}
		assertNoExportTemps(t, dir)
	})

	t.Run("late retained sidecar", func(t *testing.T) {
		outputDir := filepath.Join(dir, "retained-sidecar")
		e := exporter{beforeManifest: func() error {
			matches, err := filepath.Glob(filepath.Join(dir, ".gitcrawl-portable-export-*", "gitcrawl.db"))
			if err != nil || len(matches) != 1 {
				return errors.New("could not locate staging database")
			}
			sidecar := matches[0] + "-wal"
			if err := os.Mkdir(sidecar, 0o700); err != nil {
				return err
			}
			return os.WriteFile(filepath.Join(sidecar, "retained"), []byte("x"), 0o600)
		}}
		_, err := e.export(ctx, testExportOptions(sourcePath, outputDir))
		if err == nil || (!strings.Contains(err.Error(), "remove portable SQLite sidecar") && !strings.Contains(err.Error(), "unable to open database file")) {
			t.Fatalf("retained sidecar error = %v", err)
		}
		assertNoExportTemps(t, dir)
	})
}

func TestExportRejectsInvalidSQLiteSourceWithoutPublishing(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "not-sqlite.db")
	if err := os.WriteFile(sourcePath, []byte("not a SQLite database"), 0o600); err != nil {
		t.Fatal(err)
	}
	outputDir := filepath.Join(dir, "artifact")
	_, err := Export(context.Background(), testExportOptions(sourcePath, outputDir))
	if err == nil || !strings.Contains(err.Error(), "snapshot source database") {
		t.Fatalf("invalid SQLite source error = %v", err)
	}
	if _, statErr := os.Stat(outputDir); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("invalid SQLite source published output: %v", statErr)
	}
	assertNoExportTemps(t, dir)
}

func TestExportRejectsSnapshotSchemaLossDuringShaping(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	st := seedExportSource(t, ctx, sourcePath)
	defer st.Close()
	outputDir := filepath.Join(dir, "artifact")
	options := testExportOptions(sourcePath, outputDir)
	options.Progress = func(stage Stage) {
		if stage != StageCanonicalShaping {
			return
		}
		matches, err := filepath.Glob(filepath.Join(dir, ".gitcrawl-portable-export-*", "gitcrawl.db"))
		if err != nil || len(matches) != 1 {
			t.Errorf("locate shaping database: matches=%v err=%v", matches, err)
			return
		}
		db := openRawDB(t, matches[0])
		if _, err := db.ExecContext(ctx, `pragma foreign_keys = off; drop table threads`); err != nil {
			t.Errorf("remove required shaping table: %v", err)
		}
		if err := db.Close(); err != nil {
			t.Errorf("close shaping database: %v", err)
		}
	}
	_, err := Export(ctx, options)
	if err == nil || !strings.Contains(err.Error(), "canonical portable shaping") {
		t.Fatalf("snapshot schema loss error = %v", err)
	}
	if _, statErr := os.Stat(outputDir); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("schema-loss export published output: %v", statErr)
	}
	assertNoExportTemps(t, dir)
}

func newTestManifestPair(t *testing.T) (string, string, Manifest) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "artifact.db")
	db := openRawDB(t, dbPath)
	if _, err := db.Exec(`
		create table repositories(id integer primary key, owner text, name text, full_name text);
		create table portable_metadata(key text primary key, value text not null);
		insert into repositories values(1, 'openclaw', 'gitcrawl', 'openclaw/gitcrawl');
		insert into portable_metadata values
			('schema', 'portable-v1'),
			('profile', 'current-state-v1'),
			('profile_version', '1'),
			('source_path', 'data/gitcrawl.db'),
			('index_profile', 'unique-only'),
			('column_profile', 'sanitized-compatibility');
	`); err != nil {
		t.Fatal(err)
	}
	tables, err := databaseTableStats(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	manifest := Manifest{
		Schema: "portable-v1", PortableSchema: "portable-v1",
		Profile: CurrentStateV1, ProfileVersion: 1,
		OutputPath: "data/gitcrawl.db", Tables: tables,
		Repository:   &Repository{ID: 1, Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl"},
		ValidationOK: true, QuickCheck: "ok", IntegrityCheck: "ok",
		IndexProfile: "unique-only", ColumnProfile: "sanitized-compatibility",
	}
	refreshTestManifest(t, dbPath, &manifest)
	manifestPath := dbPath + ".manifest.json"
	writeTestManifest(t, manifestPath, manifest)
	return dbPath, manifestPath, manifest
}

func refreshTestManifest(t *testing.T, dbPath string, manifest *Manifest) {
	t.Helper()
	db := openRawDB(t, dbPath)
	tables, err := databaseTableStats(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	manifest.OutputBytes = info.Size()
	manifest.SHA256 = hashFile(t, dbPath)
	manifest.ArtifactID = manifest.SHA256
	manifest.Tables = tables
}

func writeTestManifest(t *testing.T, path string, manifest Manifest) {
	t.Helper()
	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}
