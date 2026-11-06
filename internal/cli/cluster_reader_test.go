package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestClusterReaderRefusesWritersBeforeOpeningRuntime(t *testing.T) {
	for _, test := range []struct {
		name      string
		args      []string
		wantError string
	}{
		{name: "sync", args: []string{"sync", "openclaw/openclaw"}, wantError: "cluster reader supports local cluster reads only"},
		{name: "embed", args: []string{"embed", "openclaw/openclaw"}, wantError: "cluster reader supports local cluster reads only"},
		{name: "cluster", args: []string{"cluster", "openclaw/openclaw"}, wantError: "cluster reader supports local cluster reads only"},
		{name: "purge threads", args: []string{"purge-threads", "openclaw/openclaw", "--numbers", "1"}, wantError: "cluster reader supports local cluster reads only"},
		{name: "provider credentials", args: []string{"--github-token-command", "never-run", "clusters", "openclaw/openclaw"}, wantError: "cluster reader does not use provider credentials"},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			t.Setenv("HOME", dir)
			t.Setenv("USERPROFILE", dir)
			t.Setenv("XDG_CONFIG_HOME", filepath.Join(dir, "config"))
			t.Setenv("XDG_CACHE_HOME", filepath.Join(dir, "cache"))
			t.Setenv("XDG_DATA_HOME", filepath.Join(dir, "data"))
			t.Setenv("XDG_STATE_HOME", filepath.Join(dir, "state"))
			t.Setenv("GITCRAWL_DB_PATH", filepath.Join(dir, "archive.db"))
			t.Setenv("GITHUB_TOKEN", "")
			t.Setenv("GH_TOKEN", "")
			app := New()
			var stdout, stderr bytes.Buffer
			app.Stdout, app.Stderr = &stdout, &stderr
			lookups := 0
			app.githubAuthTokenLookup = func(context.Context) (string, error) {
				lookups++
				return "", nil
			}
			args := append([]string{"--config", filepath.Join(dir, "missing.toml")}, test.args...)
			err := app.RunClusterReader(context.Background(), args)
			if err == nil || ExitCode(err) != 2 || err.Error() != test.wantError {
				t.Fatalf("rejected route = %v (exit %d), want %q with usage exit 2", err, ExitCode(err), test.wantError)
			}
			if lookups != 0 || stdout.Len() != 0 || stderr.Len() != 0 || app.configPath != "" {
				t.Fatalf("rejected route entered runtime setup: lookups=%d stdout=%q stderr=%q configApplied=%t", lookups, stdout.String(), stderr.String(), app.configPath != "")
			}
			entries, err := os.ReadDir(dir)
			if err != nil || len(entries) != 0 {
				t.Fatalf("rejected route created files: entries=%v err=%v", entries, err)
			}
		})
	}
}
