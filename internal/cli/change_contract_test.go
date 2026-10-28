package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
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
