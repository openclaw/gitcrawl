//go:build !windows

package portable

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func consumeFixture(t *testing.T) ExportOptions {
	t.Helper()
	dir := t.TempDir()
	source := filepath.Join(dir, "runtime.db")
	st := seedExportSource(t, context.Background(), source)
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	db := openRawDB(t, source)
	if _, err := db.Exec(`pragma journal_mode=delete`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	return ExportOptions{SourceDBPath: source, OutputDir: filepath.Join(dir, "consumed"),
		DatabaseName: "archive.db", PublicPath: "data/archive.db", Profile: CurrentStateV1,
		Repository: "openclaw/gitcrawl", BodyChars: 8, Compression: CompressionGzip, ConsumeSource: true}
}

func TestConsumingExportMatchesSourcePreservingExport(t *testing.T) {
	options := consumeFixture(t)
	preserved := options
	preserved.ConsumeSource = false
	preserved.OutputDir += "-preserved"
	before, err := os.Stat(options.SourceDBPath)
	if err != nil {
		t.Fatal(err)
	}
	want, err := Export(context.Background(), preserved)
	if err != nil {
		t.Fatal(err)
	}
	if want.SourceConsumed {
		t.Fatal("ordinary export consumed its source")
	}
	var transferred bool
	options.Progress = func(stage Stage) {
		if stage != StageRepositoryScope {
			return
		}
		if _, err := os.Stat(options.SourceDBPath); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("source still exists after handoff: %v", err)
		}
		stages, err := filepath.Glob(filepath.Join(filepath.Dir(options.OutputDir), ".gitcrawl-portable-export-*", options.DatabaseName))
		if err != nil || len(stages) != 1 {
			t.Fatalf("private stage: %v %v", stages, err)
		}
		after, err := os.Stat(stages[0])
		if err != nil || !os.SameFile(before, after) {
			t.Fatalf("export copied rather than moved the source: %v", err)
		}
		transferred = true
	}
	got, err := Export(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	if !transferred || !got.SourceConsumed || !got.ArtifactCommitted || got.ArtifactID != want.ArtifactID ||
		got.QuickCheck != "ok" || got.IntegrityCheck != "ok" || got.ForeignKeyViolations != 0 {
		t.Fatalf("consuming export differs: got %+v; want identity %s", got, want.ArtifactID)
	}
}

func TestConsumingExportRejectsUnsafeSourceBeforeHandoff(t *testing.T) {
	for _, scenario := range []string{"hardlink", "symlink", "wal", "shm", "journal", "writer", "reader", "wal-mode", "cancelled"} {
		t.Run(scenario, func(t *testing.T) {
			options := consumeFixture(t)
			source := options.SourceDBPath
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch scenario {
			case "hardlink":
				if err := os.Link(source, source+".alias"); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Symlink(source, source+".alias"); err != nil {
					t.Fatal(err)
				}
				options.SourceDBPath += ".alias"
			case "wal", "shm", "journal":
				if err := os.WriteFile(source+"-"+scenario, []byte("pending"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "writer", "reader":
				db := openRawDB(t, source)
				defer db.Close()
				statement := `begin immediate`
				if scenario == "reader" {
					statement = `begin; select count(*) from threads`
				}
				if _, err := db.Exec(statement); err != nil {
					t.Fatal(err)
				}
				defer db.Exec(`rollback`)
			case "wal-mode":
				db := openRawDB(t, source)
				if _, err := db.Exec(`pragma journal_mode=wal`); err != nil {
					t.Fatal(err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
			case "cancelled":
				cancel()
			}
			before := readFile(t, source)
			result, err := Export(ctx, options)
			if err == nil || result.SourceConsumed || result.ArtifactCommitted {
				t.Fatalf("unsafe consume accepted: %+v %v", result, err)
			}
			if string(before) != string(readFile(t, source)) {
				t.Fatal("rejected export changed source bytes")
			}
			if _, err := os.Stat(options.OutputDir); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("output exists: %v", err)
			}
		})
	}
}

func TestConsumingExportFailureDiscardsOnlyConsumedStage(t *testing.T) {
	for _, stage := range []Stage{StageRepositoryScope, StageCanonicalShaping, StageFinalVacuum, StageManifest} {
		t.Run(string(stage), func(t *testing.T) {
			options := consumeFixture(t)
			checkpoint := readFile(t, options.SourceDBPath)
			backup := options.SourceDBPath + ".checkpoint"
			if err := os.WriteFile(backup, checkpoint, 0o600); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			options.Progress = func(current Stage) {
				if current == stage {
					cancel()
				}
			}
			result, err := Export(ctx, options)
			if !errors.Is(err, context.Canceled) || !result.SourceConsumed || result.ArtifactCommitted {
				t.Fatalf("failure result: %+v %v", result, err)
			}
			if _, err := os.Stat(options.SourceDBPath); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("source restored incorrectly: %v", err)
			}
			stages, err := filepath.Glob(filepath.Join(filepath.Dir(options.OutputDir), ".gitcrawl-portable-export-*"))
			if err != nil || len(stages) != 0 {
				t.Fatalf("stage leaked: %v %v", stages, err)
			}
			if string(checkpoint) != string(readFile(t, backup)) {
				t.Fatal("checkpoint changed")
			}
			if err := os.Rename(backup, options.SourceDBPath); err != nil {
				t.Fatal(err)
			}
			options.Progress = nil
			if _, err := Export(context.Background(), options); err != nil {
				t.Fatalf("recovery export: %v", err)
			}
		})
	}
}

func TestConsumeSourceRejectsDifferentFilesystem(t *testing.T) {
	if info, err := os.Stat("/dev/shm"); err != nil || !info.IsDir() {
		t.Skip("requires a separate shared-memory filesystem")
	}
	options := consumeFixture(t)
	dir, err := os.MkdirTemp("/dev/shm", "gitcrawl-consume-test-")
	if err != nil {
		t.Skipf("shared-memory filesystem unavailable: %v", err)
	}
	defer os.RemoveAll(dir)
	err = consumeSQLite(context.Background(), options.SourceDBPath, filepath.Join(dir, "artifact.db"))
	if err == nil || !strings.Contains(err.Error(), "same filesystem") {
		t.Fatalf("cross-filesystem handoff: %v", err)
	}
	if _, err := os.Stat(options.SourceDBPath); err != nil {
		t.Fatalf("source lost: %v", err)
	}
}
