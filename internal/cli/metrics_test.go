package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/openclaw/gitcrawl/internal/headlinemetrics"
)

func metricsConfigFixture(t *testing.T) (string, headlinemetrics.Config) {
	t.Helper()
	dir := t.TempDir()
	c := headlinemetrics.Config{Database: filepath.Join(dir, "metrics.sqlite"), Targets: []headlinemetrics.Target{{Entity: "OpenClaw", Target: "openclaw/openclaw"}}}
	raw, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "metrics.json")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	return path, c
}
func metricsApp(t *testing.T) (*App, *bytes.Buffer) {
	t.Helper()
	app := New()
	out := new(bytes.Buffer)
	app.Stdout = out
	app.Stderr = new(bytes.Buffer)
	app.githubAuthTokenLookup = func(context.Context) (string, error) {
		t.Error("unexpected credential lookup")
		return "", errors.New("no credential")
	}
	return app, out
}

func TestMetricsNativeHelpMetadataAndUsage(t *testing.T) {
	for _, args := range [][]string{{"--help"}, {"help", "metrics"}, {"metrics"}, {"metrics", "help"}, {"metrics", "-h"}, {"metrics", "collect", "--help"}, {"--json", "metrics", "status", "-h"}, {"metrics", "import", "--help"}} {
		app, out := metricsApp(t)
		if err := app.Run(context.Background(), args); err != nil || !strings.Contains(out.String(), "metrics") {
			t.Fatalf("%v: %v %s", args, err, out)
		}
	}
	app, out := metricsApp(t)
	if err := app.Run(context.Background(), []string{"metadata", "--json"}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"metrics-collect", "metrics-import", "metrics-status"} {
		if !strings.Contains(out.String(), name) {
			t.Fatalf("metadata missing %s", name)
		}
	}
	for _, args := range [][]string{{"metrics", "bad"}, {"metrics", "collect"}, {"metrics", "status", "--unknown"}, {"metrics", "import", "--config", "/missing", "extra"}, {"metrics", "collect", "--config", "/missing"}} {
		app, _ := metricsApp(t)
		if err := app.Run(context.Background(), args); err == nil || ExitCode(err) != 2 {
			t.Fatalf("%v: usage error %v", args, err)
		}
	}
	if releaseNotificationAllowed([]string{"metrics", "status"}) {
		t.Fatal("metrics triggers release side effects")
	}
}

func TestMetricsImportStatusNativeJSONAndArchiveIsolation(t *testing.T) {
	path, c := metricsConfigFixture(t)
	archive := filepath.Join(t.TempDir(), "archive.db")
	if err := os.WriteFile(archive, []byte("untouched archive"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GITCRAWL_DB", archive)
	t.Setenv("GITCRAWL_CONFIG", filepath.Join(t.TempDir(), "does-not-exist.toml"))
	row := `{"type":"metric","id":"history:1","entity":"OpenClaw","target":"openclaw/openclaw","metric":"watchers","kind":"counter","ts":"2026-09-14T00:00:00Z","value":null,"observed_at":"2026-09-15T00:00:00Z","provenance":"claw-track"}`
	for i, args := range [][]string{{"--json", "metrics", "import", "--config", path}, {"metrics", "import", "--config", path, "--json"}} {
		app, out := metricsApp(t)
		app.Stdin = strings.NewReader(row + "\n")
		if err := app.Run(context.Background(), args); err != nil {
			t.Fatal(err)
		}
		var r headlinemetrics.Result
		if err := json.Unmarshal(out.Bytes(), &r); err != nil {
			t.Fatal(err)
		}
		if r.RowsWritten != 1-i || !r.OK {
			t.Fatalf("import=%+v", r)
		}
	}
	for _, args := range [][]string{{"metrics", "status", "--config", path, "--json"}, {"--config", path, "--json", "metrics", "status"}, {"--format", "json", "metrics", "status", "--config", path}} {
		app, out := metricsApp(t)
		if err := app.Run(context.Background(), args); err != nil {
			t.Fatal(err)
		}
		var r headlinemetrics.Result
		if err := json.Unmarshal(out.Bytes(), &r); err != nil || r.Observations != 1 {
			t.Fatalf("status=%s %v", out, err)
		}
	}
	if b, _ := os.ReadFile(archive); string(b) != "untouched archive" {
		t.Fatal("archive changed")
	}
	info, err := os.Stat(c.Database)
	if err != nil || info.Size() == 0 {
		t.Fatalf("metrics DB absent: %v", err)
	}
	app, _ := metricsApp(t)
	app.Stdin = strings.NewReader("bad")
	if err := app.Run(context.Background(), []string{"metrics", "import", "--config", path}); err == nil || ExitCode(err) != 1 {
		t.Fatal("invalid history accepted")
	}
}

func TestMetricsCollectUsesNativeCredentialsAndKeepsPartialOutput(t *testing.T) {
	for _, mode := range []string{"environment", "gh-fallback", "managed", "partial"} {
		t.Run(mode, func(t *testing.T) {
			if mode == "managed" && runtime.GOOS == "windows" {
				t.Skip("managed helper is Unix-only")
			}
			path, c := metricsConfigFixture(t)
			c.TokenEnv = "GITCRAWL_METRICS_TEST_TOKEN"
			b, _ := json.Marshal(c)
			if err := os.WriteFile(path, b, 0600); err != nil {
				t.Fatal(err)
			}
			t.Setenv("GITHUB_TOKEN", "ambient-must-not-win")
			t.Setenv(c.TokenEnv, "")
			token := "fixture-token"
			if mode == "environment" || mode == "partial" {
				t.Setenv(c.TokenEnv, token)
			}
			requests := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				if r.Header.Get("Authorization") != "Bearer "+token {
					t.Error("wrong credential selected")
				}
				switch r.URL.Path {
				case "/repos/openclaw/openclaw":
					fmt.Fprint(w, `{"stargazers_count":4,"forks_count":2,"subscribers_count":1,"open_issues_count":8}`)
				case "/search/issues":
					if mode == "partial" {
						fmt.Fprint(w, `{"incomplete_results":true,"total_count":3}`)
					} else {
						fmt.Fprint(w, `{"total_count":3}`)
					}
				case "/repos/openclaw/openclaw/traffic/clones":
					w.WriteHeader(403)
				case "/repos/openclaw/openclaw/releases":
					fmt.Fprint(w, `[]`)
				default:
					t.Errorf("unexpected API/model/archive request %s", r.URL.Path)
					w.WriteHeader(404)
				}
			}))
			defer server.Close()
			t.Setenv("GITCRAWL_GITHUB_BASE_URL", server.URL)
			t.Setenv("GITCRAWL_OPENAI_BASE_URL", server.URL)
			app, out := metricsApp(t)
			args := []string{"metrics", "collect", "--config", path, "--json"}
			if mode == "gh-fallback" {
				app.githubAuthTokenLookup = func(context.Context) (string, error) { return token, nil }
			}
			if mode == "managed" {
				helper := filepath.Join(t.TempDir(), "credential-helper")
				if err := os.WriteFile(helper, []byte("#!/bin/sh\nprintf '%s\\n' 'fixture-token'\n"), 0700); err != nil {
					t.Fatal(err)
				}
				args = append([]string{"--github-token-command", helper}, args...)
			}
			err := app.Run(context.Background(), args)
			if (err != nil) != (mode == "partial") {
				t.Fatalf("error=%v", err)
			}
			var result headlinemetrics.Result
			if e := json.Unmarshal(out.Bytes(), &result); e != nil {
				t.Fatal(e)
			}
			if result.RowsWritten != 5 || result.OK == (mode == "partial") || requests != 4 {
				t.Fatalf("result=%+v requests=%d", result, requests)
			}
			if strings.Contains(out.String(), token) {
				t.Fatal("token printed")
			}
		})
	}
}
