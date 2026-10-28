package cli

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/openclaw/gitcrawl/internal/store"
)

func TestChangeContractCLILifecycle(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db := filepath.Join(dir, "archive.db")
	cfg := filepath.Join(dir, "config.toml")
	if err := os.WriteFile(cfg, []byte(fmt.Sprintf("db_path=%q\n", db)), 0600); err != nil {
		t.Fatal(err)
	}
	s, err := store.Open(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	run := func(args ...string) (string, error) {
		a := New()
		var out, stderr bytes.Buffer
		a.Stdout = &out
		a.Stderr = &stderr
		err := a.Run(ctx, append([]string{"--config", cfg, "change-contract"}, append(args, "--json")...))
		return out.String(), err
	}
	out, err := run("status")
	if err != nil || !strings.Contains(out, `"enabled": false`) {
		t.Fatalf("status before enable = %s, %v", out, err)
	}
	out, err = run("enable", "--retention", "2048")
	if err != nil {
		t.Fatalf("enable: %v", err)
	}
	var enabled struct {
		Repair store.ChangeContractRepair `json:"repair"`
		Status store.ChangeContractStatus `json:"status"`
	}
	if err := json.Unmarshal([]byte(out), &enabled); err != nil {
		t.Fatal(err)
	}
	if !enabled.Repair.Created || !enabled.Status.Armed || enabled.Status.Retention != 2048 || enabled.Status.Generation != 1 {
		t.Fatalf("enable = %s", out)
	}
	if out, err = run("bump", "--reason", "owner rewrite"); err != nil || !strings.Contains(out, `"generation": 2`) {
		t.Fatalf("bump = %s, %v", out, err)
	}
	for _, args := range [][]string{{"bump"}, {"enable", "--retention", "10"}, {"status", "--retention", "2048"}, {"enable", "--reason", "x"}, {"unknown"}, {"status", "extra"}} {
		if _, err := run(args...); err == nil {
			t.Fatalf("invalid arguments accepted: %v", args)
		}
	}
	if out, err = run("disable"); err != nil || !strings.Contains(out, `"removed": true`) || !strings.Contains(out, `"enabled": false`) {
		t.Fatalf("disable = %s, %v", out, err)
	}
}

func TestChangeContractCLIRefusesMissingArchivesPortableStoresAndAbsentContracts(t *testing.T) {
	ctx := context.Background()
	run := func(cfg string, args ...string) error {
		a := New()
		var out, stderr bytes.Buffer
		a.Stdout = &out
		a.Stderr = &stderr
		return a.Run(ctx, append([]string{"--config", cfg, "change-contract"}, append(args, "--json")...))
	}
	writeConfig := func(dir, db string) string {
		cfg := filepath.Join(dir, "config.toml")
		if err := os.WriteFile(cfg, []byte(fmt.Sprintf("db_path=%q\n", db)), 0600); err != nil {
			t.Fatal(err)
		}
		return cfg
	}
	if err := run(filepath.Join(t.TempDir(), "config.toml")); err == nil {
		t.Fatal("missing action accepted")
	}

	missingDir := t.TempDir()
	missing := filepath.Join(missingDir, "missing.db")
	if err := run(writeConfig(missingDir, missing), "enable"); err == nil {
		t.Fatal("enable created a missing archive")
	}
	if _, err := os.Stat(missing); !os.IsNotExist(err) {
		t.Fatalf("missing archive was created: %v", err)
	}

	plainDir := t.TempDir()
	plain := filepath.Join(plainDir, "archive.db")
	s, err := store.Open(ctx, plain)
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	if err := run(writeConfig(plainDir, plain), "bump", "--reason", "rewrite"); err == nil || !strings.Contains(err.Error(), "not enabled") {
		t.Fatalf("bump without a contract = %v", err)
	}

	portableDir := t.TempDir()
	if out, err := exec.Command("git", "init", "-q", portableDir).CombinedOutput(); err != nil {
		t.Skipf("git init unavailable: %v %s", err, out)
	}
	portable := filepath.Join(portableDir, "data", "archive.db")
	if err := os.MkdirAll(filepath.Dir(portable), 0o700); err != nil {
		t.Fatal(err)
	}
	if s, err = store.Open(ctx, portable); err != nil {
		t.Fatal(err)
	}
	s.Close()
	if err := run(writeConfig(portableDir, portable), "enable"); err == nil || !strings.Contains(err.Error(), "portable") {
		t.Fatalf("enable on a portable store = %v", err)
	}
}

func TestChangeContractCLIRejectsMalformedInvocations(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfg := filepath.Join(dir, "config.toml")
	if err := os.WriteFile(cfg, []byte(fmt.Sprintf("db_path=%q\n", filepath.Join(dir, "missing.db"))), 0600); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"change-contract"},
		{"change-contract", "status", "--bogus"},
		{"change-contract", "status"},
		{"change-contract", "disable"},
	} {
		a := New()
		var out, stderr bytes.Buffer
		a.Stdout = &out
		a.Stderr = &stderr
		if err := a.Run(ctx, append([]string{"--config", cfg}, args...)); err == nil {
			t.Fatalf("%v accepted: %s", args, out.String())
		}
	}
	if err := sanitizeCloudSQLiteSnapshot(ctx, closedSQLiteDB(t)); err == nil {
		t.Fatal("cloud snapshot sanitization succeeded on a closed database")
	}
}

func closedSQLiteDB(t *testing.T) *sql.DB {
	t.Helper()
	s, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "closed.db"))
	if err != nil {
		t.Fatal(err)
	}
	db := s.DB()
	s.Close()
	return db
}
