package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSyncAndRefreshRejectCodeHydrationBeforeRuntime(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(dir, "config"))
	t.Setenv("XDG_CACHE_HOME", filepath.Join(dir, "cache"))
	t.Setenv("XDG_DATA_HOME", filepath.Join(dir, "data"))
	t.Setenv("GITCRAWL_DB_PATH", filepath.Join(dir, "archive.db"))
	t.Setenv("GITHUB_TOKEN", "")
	for _, command := range []string{"sync", "refresh"} {
		for _, option := range []string{"--include-code", "--include-code=true", "--include-code=false"} {
			t.Run(command+"/"+option, func(t *testing.T) {
				app := New()
				var stdout, stderr bytes.Buffer
				app.Stdout, app.Stderr = &stdout, &stderr
				lookups := 0
				app.githubAuthTokenLookup = func(context.Context) (string, error) {
					lookups++
					return "", nil
				}
				err := app.Run(context.Background(), []string{"--config", filepath.Join(dir, "missing.toml"), "--json", command, "fixture/project", option})
				if ExitCode(err) != 2 || err == nil || !strings.Contains(err.Error(), "flag provided but not defined: -include-code") {
					t.Fatalf("unsupported code hydration = %v (exit %d), want unknown flag with usage exit 2", err, ExitCode(err))
				}
				if lookups != 0 || stdout.Len() != 0 || stderr.Len() != 0 {
					t.Fatalf("rejected option performed credential lookup or output: lookups=%d stdout=%q stderr=%q", lookups, stdout.String(), stderr.String())
				}
			})
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("rejected options created runtime files: entries=%v err=%v", entries, err)
	}
}
