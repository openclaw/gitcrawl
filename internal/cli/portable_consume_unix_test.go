//go:build !windows

package cli

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestPortableExportCommandConsumesClosedSource(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	config := filepath.Join(dir, "config.toml")
	source := filepath.Join(dir, "source.db")
	app := New()
	var stdout, stderr bytes.Buffer
	app.Stdout, app.Stderr = &stdout, &stderr
	if err := app.Run(ctx, []string{"--config", config, "init", "--db", source}); err != nil {
		t.Fatal(err)
	}
	seedPortableThread(t, source, 7, "consumed source")
	db, err := sql.Open("sqlite", source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`pragma journal_mode=delete`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	stdout.Reset()
	if err := app.Run(ctx, []string{"--config", config, "portable", "export", "--profile", "current-state-v1",
		"--output-dir", filepath.Join(dir, "artifact"), "--consume-source", "--json"}); err != nil {
		t.Fatal(err)
	}
	var result struct {
		SourceConsumed    bool   `json:"source_consumed"`
		ArtifactCommitted bool   `json:"artifact_committed"`
		IntegrityCheck    string `json:"integrity_check"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if !result.SourceConsumed || !result.ArtifactCommitted || result.IntegrityCheck != "ok" {
		t.Fatalf("result: %+v", result)
	}
	if _, err := os.Stat(source); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("source still exists: %v", err)
	}
}
