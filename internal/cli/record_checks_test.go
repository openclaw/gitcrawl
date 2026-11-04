package cli

import (
	"bytes"
	"context"
	"fmt"
	"github.com/openclaw/gitcrawl/internal/store"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRecordChecksCLILifecycleAndInvalidArguments(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db := filepath.Join(dir, "archive.db")
	cfg := filepath.Join(dir, "config.toml")
	if err := os.WriteFile(cfg, []byte(fmt.Sprintf("db_path=%q\n", db)), 0600); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	st.Close()
	run := func(args ...string) (string, error) {
		a := New()
		var out, stderr bytes.Buffer
		a.Stdout = &out
		a.Stderr = &stderr
		err := a.Run(ctx, append([]string{"--config", cfg, "record-checks"}, args...))
		return out.String(), err
	}
	for _, args := range [][]string{nil, {"unknown"}, {"status", "extra"}, {"status", "--bad-flag"}} {
		if _, err := run(args...); err == nil {
			t.Fatalf("accepted invalid arguments %v", args)
		}
	}
	if _, err := run("backfill", "--json"); err == nil {
		t.Fatal("backfill before enable accepted")
	}
	for _, action := range []string{"status", "enable", "backfill", "status", "disable", "status", "enable"} {
		out, err := run(action, "--json")
		if err != nil {
			t.Fatalf("%s: %v", action, err)
		}
		if action == "enable" && !strings.Contains(out, `"enabled": true`) {
			t.Fatalf("enable: %s", out)
		}
		if action == "disable" && !strings.Contains(out, `"enabled": false`) {
			t.Fatalf("disable: %s", out)
		}
	}
}
