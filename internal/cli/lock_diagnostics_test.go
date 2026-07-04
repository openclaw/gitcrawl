package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/openclaw/gitcrawl/internal/store"
)

func TestDoctorLocksJSONReportsSQLiteHealth(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "gitcrawl.db")
	if err := New().Run(ctx, []string{"--config", configPath, "init", "--db", dbPath}); err != nil {
		t.Fatalf("init: %v", err)
	}
	st, err := store.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	oldDetect := detectDBProcesses
	detectDBProcesses = func(context.Context, string) processDetectionReport {
		return processDetectionReport{
			Method:    "test",
			Platform:  "test",
			Available: true,
			Processes: []lockProcess{{PID: 123, Command: "gitcrawl"}},
		}
	}
	defer func() { detectDBProcesses = oldDetect }()

	app := New()
	var stdout bytes.Buffer
	app.Stdout = &stdout
	if err := app.Run(ctx, []string{"--config", configPath, "doctor", "--locks", "--json"}); err != nil {
		t.Fatalf("doctor --locks: %v", err)
	}
	var payload struct {
		Locks lockDiagnostic `json:"locks"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
		t.Fatalf("decode doctor locks: %v\n%s", err, stdout.String())
	}
	if payload.Locks.DBPath != dbPath || !payload.Locks.DBExists {
		t.Fatalf("locks db metadata = %+v", payload.Locks)
	}
	if payload.Locks.ReadOnlyOpen != "ok" || payload.Locks.QuickCheck != "ok" || !payload.Locks.SafeReadOnlyInspection {
		t.Fatalf("locks health = %+v", payload.Locks)
	}
	if !payload.Locks.ProcessDetection.Available || len(payload.Locks.ProcessDetection.Processes) != 1 {
		t.Fatalf("process detection = %+v", payload.Locks.ProcessDetection)
	}
}

func TestParseLsofProcessOutput(t *testing.T) {
	got := parseLsofProcessOutput("p123\ncgitcrawl\np456\ncsqlite3\n")
	if len(got) != 2 {
		t.Fatalf("processes = %+v", got)
	}
	if got[0].PID != 123 || got[0].Command != "gitcrawl" || got[1].PID != 456 || got[1].Command != "sqlite3" {
		t.Fatalf("processes = %+v", got)
	}
}
