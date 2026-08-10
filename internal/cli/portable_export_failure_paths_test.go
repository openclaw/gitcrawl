package cli

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/openclaw/gitcrawl/internal/store"
)

func TestPortableExportArgumentsFailBeforeOpeningRuntime(t *testing.T) {
	missingConfig := filepath.Join(t.TempDir(), "missing.toml")
	tests := []struct {
		name      string
		args      []string
		wantError string
	}{
		{name: "unknown flag", args: []string{"--unknown"}, wantError: "flag provided but not defined"},
		{name: "positional argument", args: []string{"--profile", "current-state-v1", "--output-dir", "out", "extra"}, wantError: "does not take positional arguments"},
		{name: "missing profile", args: []string{"--output-dir", "out"}, wantError: "--profile is required"},
		{name: "missing output", args: []string{"--profile", "current-state-v1"}, wantError: "--output-dir is required"},
		{name: "invalid body chars", args: []string{"--profile", "current-state-v1", "--output-dir", "out", "--body-chars", "nope"}, wantError: "positive integer"},
		{name: "invalid max bytes", args: []string{"--profile", "current-state-v1", "--output-dir", "out", "--max-bytes", "0"}, wantError: "--max-bytes must be a positive integer"},
		{name: "unsafe database name", args: []string{"--profile", "current-state-v1", "--output-dir", "out", "--database-name", "../archive.db"}, wantError: "database-name must be a safe basename"},
		{name: "unsafe public path", args: []string{"--profile", "current-state-v1", "--output-dir", "out", "--public-path", "../archive.db"}, wantError: "public-path must be a clean relative slash path"},
		{name: "zero body chars", args: []string{"--profile", "current-state-v1", "--output-dir", "out", "--body-chars", "0"}, wantError: "positive integer"},
		{name: "default body and public path reach runtime", args: []string{"--profile", "current-state-v1", "--output-dir", "out", "--body-chars="}, wantError: "no such file"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			app := New()
			app.configPath = missingConfig
			err := app.runPortableExport(context.Background(), test.args)
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("portable export argument error = %v, want %q", err, test.wantError)
			}
		})
	}
}

func TestPortableCommandHelpAndDurationFormatting(t *testing.T) {
	app := New()
	app.Stdout = io.Discard
	app.Stderr = io.Discard
	if err := app.runPortable(context.Background(), []string{"help"}); err != nil {
		t.Fatalf("portable help: %v", err)
	}
	if got := formatPortableProgressDuration(-time.Second); got != "0s" {
		t.Fatalf("negative portable duration = %q", got)
	}
	if got := formatPortableProgressDuration(149 * time.Millisecond); got != "100ms" {
		t.Fatalf("rounded portable duration = %q", got)
	}
}

func TestPortableManifestWriterRejectsUnsafePaths(t *testing.T) {
	if _, _, err := writePortableDBManifest(store.PortablePruneStats{DBPath: filepath.Join(t.TempDir(), "missing.db")}); err == nil || !strings.Contains(err.Error(), "stat portable db") {
		t.Fatalf("missing portable DB manifest error = %v", err)
	}

	dirPath := t.TempDir()
	if _, _, err := writePortableDBManifest(store.PortablePruneStats{DBPath: dirPath}); err == nil || !strings.Contains(err.Error(), "hash portable db") {
		t.Fatalf("directory portable DB manifest error = %v", err)
	}

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "portable.db")
	if err := os.WriteFile(dbPath, []byte("portable bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	manifestPath := portableDBManifestPath(dbPath)
	if err := os.Mkdir(manifestPath, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, _, err := writePortableDBManifest(store.PortablePruneStats{DBPath: dbPath}); err == nil || !strings.Contains(err.Error(), "write portable db manifest") {
		t.Fatalf("occupied portable manifest path error = %v", err)
	}
}

func TestPortableStorePathSanitization(t *testing.T) {
	if got := safePathName(".Repo Name_42."); got != "repo-name_42" {
		t.Fatalf("safe portable path = %q", got)
	}
	configPath := filepath.Join(t.TempDir(), "config.toml")
	if got := defaultPortableStoreDir(configPath, "/"); got != filepath.Join(filepath.Dir(configPath), "stores", "portable-store") {
		t.Fatalf("fallback portable store path = %q", got)
	}
}

func TestPortablePruneArgumentsFailBeforeMutation(t *testing.T) {
	tests := []struct {
		name      string
		args      []string
		wantError string
	}{
		{name: "unknown flag", args: []string{"--unknown"}, wantError: "flag provided but not defined"},
		{name: "positional", args: []string{"extra"}, wantError: "does not take positional arguments"},
		{name: "invalid body chars", args: []string{"--body-chars", "nope"}, wantError: "positive integer"},
		{name: "default body reaches runtime", args: []string{"--body-chars="}, wantError: "no such file"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			app := New()
			app.configPath = filepath.Join(t.TempDir(), "missing.toml")
			err := app.runPortablePrune(context.Background(), test.args)
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("portable prune argument error = %v, want %q", err, test.wantError)
			}
		})
	}
}
