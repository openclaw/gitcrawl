package cli

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAcquireServiceUsesAPIWithoutSQLiteFallback(t *testing.T) {
	unit, err := os.ReadFile("../../operations/gitcrawl-acquire@.service")
	if err != nil {
		t.Fatal(err)
	}
	var command []string
	for _, line := range strings.Split(string(unit), "\n") {
		if strings.HasPrefix(line, "ExecStart=") {
			command = strings.Fields(strings.TrimPrefix(line, "ExecStart="))
		}
	}
	if len(command) < 7 || strings.Join(command[:6], " ") != "/usr/bin/flock --nonblock --conflict-exit-code=75 /var/lib/gitcrawl/%i/acquire.lock /usr/local/bin/gitcrawl --config" {
		t.Fatal("service must use the source lock and explicit Access config")
	}
	for _, arg := range command {
		if arg == "sync" || arg == "delivery" || arg == "--since" || arg == "--numbers-file" || arg == "--parent-selection-file" || strings.Contains(arg, "sqlite") {
			t.Fatalf("recurrence selects a local archive or overrides the API checkpoint: %s", arg)
		}
	}
	for _, boundary := range []string{"EnvironmentFile=/etc/gitcrawl/provider-%i.env", "TimeoutStartSec=4min", "KillMode=control-group", "MemoryMax=512M"} {
		if !strings.Contains(string(unit), boundary+"\n") {
			t.Fatalf("missing service boundary: %s", boundary)
		}
	}
	root := t.TempDir()
	database := filepath.Join(root, "archive", "native.sqlite")
	t.Setenv("GITCRAWL_DB", database)
	t.Setenv("GITCRAWL_PUBLISHER_TOKEN", "fixture-publisher")
	t.Setenv("GITCRAWL_READER_TOKEN", "fixture-reader")
	t.Setenv("OCTOPOOL_TOKEN", "fixture-relay")
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("failed archive API must not fall back to a provider")
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer provider.Close()
	t.Setenv("OCTOPOOL_URL", provider.URL)
	requests := 0
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.Method != http.MethodGet || r.URL.Path != "/v1/apps/gitcrawl/archives/fixture/acquisition/state" || r.Header.Get("Authorization") != "Bearer fixture-publisher" {
			t.Error("service lost the API source or publisher identity")
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer api.Close()
	configPath := filepath.Join(root, "access.toml")
	if err := os.WriteFile(configPath, nil, 0600); err != nil {
		t.Fatal(err)
	}
	vars := map[string]string{"GITCRAWL_ORIGIN": api.URL, "GITCRAWL_ARCHIVE": "fixture", "GITCRAWL_REPOSITORY": "example/project"}
	args := append([]string(nil), command[5:]...)
	for i, arg := range args {
		args[i] = os.Expand(arg, func(key string) string { return vars[key] })
		if args[i] == "/etc/gitcrawl/acquire-%i.toml" {
			args[i] = configPath
		}
	}
	app := New()
	app.Stdout, app.Stderr = io.Discard, io.Discard
	app.githubAuthTokenLookup = func(context.Context) (string, error) {
		t.Fatal("service resolved a personal GitHub credential")
		return "", nil
	}
	err = app.Run(context.Background(), args)
	if err == nil || !strings.Contains(err.Error(), "acquisition state failed (HTTP 503)") || requests != 1 {
		t.Fatalf("service did not stop on its API failure: requests=%d err=%v", requests, err)
	}
	if _, err := os.Stat(filepath.Dir(database)); !os.IsNotExist(err) {
		t.Fatalf("service entered SQLite runtime: %v", err)
	}
}
