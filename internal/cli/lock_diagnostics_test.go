package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
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

func TestDoctorLocksUsesPortableRuntimeDB(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	remoteDir := filepath.Join(dir, "remote")
	checkoutDir := filepath.Join(dir, "checkout")
	dbRel := filepath.Join("data", "openclaw__openclaw.sync.db")
	if err := os.MkdirAll(filepath.Join(remoteDir, "data"), 0o755); err != nil {
		t.Fatalf("mkdir remote data: %v", err)
	}
	if err := runGit(ctx, remoteDir, "init", "-b", "main"); err != nil {
		t.Fatalf("git init: %v", err)
	}
	seedPortableThread(t, filepath.Join(remoteDir, dbRel), 1, "portable issue")
	if err := runGit(ctx, remoteDir, "add", dbRel); err != nil {
		t.Fatalf("git add seed: %v", err)
	}
	if err := runGit(ctx, remoteDir, "-c", "user.email=test@example.com", "-c", "user.name=Test", "commit", "-m", "seed store"); err != nil {
		t.Fatalf("git commit seed: %v", err)
	}
	if _, err := syncPortableStore(ctx, remoteDir, checkoutDir); err != nil {
		t.Fatalf("clone portable store: %v", err)
	}

	sourceDB := filepath.Join(checkoutDir, dbRel)
	configPath := filepath.Join(dir, "config.toml")
	if err := New().Run(ctx, []string{"--config", configPath, "init", "--db", sourceDB}); err != nil {
		t.Fatalf("init config: %v", err)
	}
	expected := New()
	expected.configPath = configPath
	expectedRuntimeDB, err := expected.portableRuntimeDBPath(sourceDB)
	if err != nil {
		t.Fatalf("runtime db path: %v", err)
	}

	oldDetect := detectDBProcesses
	detectedPath := ""
	detectDBProcesses = func(_ context.Context, dbPath string) processDetectionReport {
		detectedPath = dbPath
		return processDetectionReport{Method: "test", Platform: "test", Available: true}
	}
	defer func() { detectDBProcesses = oldDetect }()

	app := New()
	var stdout bytes.Buffer
	app.Stdout = &stdout
	if err := app.Run(ctx, []string{"--config", configPath, "doctor", "--locks", "--json"}); err != nil {
		t.Fatalf("doctor --locks: %v", err)
	}
	var payload struct {
		DBPath string         `json:"db_path"`
		Locks  lockDiagnostic `json:"locks"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
		t.Fatalf("decode doctor locks: %v\n%s", err, stdout.String())
	}
	if payload.DBPath != sourceDB {
		t.Fatalf("top-level db path = %q, want configured source %q", payload.DBPath, sourceDB)
	}
	if payload.Locks.DBPath != expectedRuntimeDB {
		t.Fatalf("lock db path = %q, want runtime mirror %q", payload.Locks.DBPath, expectedRuntimeDB)
	}
	if detectedPath != expectedRuntimeDB {
		t.Fatalf("process detection path = %q, want runtime mirror %q", detectedPath, expectedRuntimeDB)
	}
	if payload.Locks.DBPath == sourceDB {
		t.Fatalf("locks should not inspect portable source db %q", sourceDB)
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
