//go:build !windows

package cli

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/openclaw/gitcrawl/internal/config"
	"github.com/openclaw/gitcrawl/internal/store"
)

func tokenCommandFixture(t *testing.T, script string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "token command")
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+script+"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestGitHubTokenCommandCaptureRejectedBeforeSideEffects(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "provider-ran")
	t.Setenv("TOKEN_TEST_MARKER", marker)
	path := tokenCommandFixture(t, "echo called > \"$TOKEN_TEST_MARKER\"\nprintf managed-token")
	output := filepath.Join(dir, "capture.json")
	prior := []byte("preserved capture")
	if err := os.WriteFile(output, prior, 0o600); err != nil {
		t.Fatal(err)
	}
	app := New()
	var stdout bytes.Buffer
	app.Stdout, app.Stderr = &stdout, io.Discard
	configDir := filepath.Join(dir, "uncreated")
	err := app.Run(context.Background(), []string{
		"--config", filepath.Join(configDir, "config.toml"),
		"--github-token-command", path, "capture", "owner/repo", "--output", output,
	})
	if err == nil || !strings.Contains(err.Error(), "capture does not support --github-token-command") ||
		strings.Contains(err.Error(), "run sync") || stdout.Len() != 0 {
		t.Fatalf("unsupported capture: err=%v output bytes=%d", err, stdout.Len())
	}
	for _, absent := range []string{marker, configDir} {
		if _, err := os.Stat(absent); !os.IsNotExist(err) {
			t.Fatalf("unexpected side effect at %s", filepath.Base(absent))
		}
	}
	if got, err := os.ReadFile(output); err != nil || !bytes.Equal(got, prior) {
		t.Fatal("capture output changed")
	}
}

func TestGitHubTokenCommandFillPRDetailsHydratesMissing(t *testing.T) {
	path := tokenCommandFixture(t, "printf managed-fill-token")
	testFillPRDetailsHydratesMissingPullRequestDetails(t, path)
}

func TestGitHubTokenCommandOutputBoundary(t *testing.T) {
	for _, tc := range []struct{ name, script, want string }{
		{"token", "printf 'fixture-token\\n'", "fixture-token"},
		{"crlf", "printf 'fixture-token\\r\\n'", "fixture-token"},
		{"no-newline", "printf fixture-token", "fixture-token"},
		{"empty", "true", ""},
		{"multiline", "printf 'one\\ntwo\\n'", ""},
		{"space", "printf 'secret token\\n'", ""},
		{"control", "printf 'secret\\001token\\n'", ""},
		{"invalid-utf8", "printf '\\377'", ""},
		{"failure", "printf secret-output; printf secret-stderr >&2; exit 1", ""},
		{"overflow", "while :; do printf secret-output; done", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := tokenCommandFixture(t, tc.script)
			provider, err := githubTokenProvider(path)
			if err != nil {
				t.Fatal(err)
			}
			got, err := provider(context.Background())
			if tc.want != "" {
				if err != nil || got != tc.want {
					t.Fatalf("valid token err=%v", err)
				}
				return
			}
			if err == nil || got != "" {
				t.Fatal("invalid output accepted")
			}
			for _, poison := range []string{"secret", path} {
				if strings.Contains(err.Error(), poison) {
					t.Fatal("private provider data leaked")
				}
			}
		})
	}
	for _, path := range []string{"", "relative", t.TempDir(), filepath.Join(t.TempDir(), "missing")} {
		if _, err := githubTokenProvider(path); err == nil {
			t.Fatal("invalid executable accepted")
		}
	}
	path := tokenCommandFixture(t, "true")
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := githubTokenProvider(path); err == nil {
		t.Fatal("non-executable accepted")
	}
}

func TestGitHubTokenCommandCancellationKillsDescendants(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(fmt.Sprint(deadline), func(t *testing.T) {
			pidPath := filepath.Join(t.TempDir(), "child.pid")
			t.Setenv("TOKEN_TEST_PID", pidPath)
			path := tokenCommandFixture(t, "(trap '' TERM; while :; do sleep 1; done) &\necho $! > \"$TOKEN_TEST_PID\"\nwait")
			provider, err := githubTokenProvider(path)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			if deadline {
				ctx, cancel = context.WithTimeout(context.Background(), 500*time.Millisecond)
			}
			defer cancel()
			result := make(chan error, 1)
			go func() { _, err := provider(ctx); result <- err }()
			var pid int
			for until := time.Now().Add(3 * time.Second); time.Now().Before(until); {
				if data, err := os.ReadFile(pidPath); err == nil {
					pid, _ = strconv.Atoi(strings.TrimSpace(string(data)))
					if pid > 0 {
						break
					}
				}
				time.Sleep(10 * time.Millisecond)
			}
			if pid == 0 {
				cancel()
				<-result
				t.Fatal("descendant did not start")
			}
			start := time.Now()
			if !deadline {
				cancel()
			}
			select {
			case err := <-result:
				if err == nil {
					t.Fatal("cancellation succeeded")
				}
			case <-time.After(3 * time.Second):
				t.Fatal("provider cancellation exceeded cleanup bound")
			}
			if time.Since(start) > 3*time.Second {
				t.Fatal("unbounded cancellation")
			}
			// Linux may retain a dead orphan as a zombie until init reaps it.
			for until := time.Now().Add(2 * time.Second); time.Now().Before(until); {
				if syscall.Kill(pid, 0) != nil {
					return
				}
				if data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid)); err == nil {
					if tail := strings.LastIndex(string(data), ") "); tail >= 0 && strings.HasPrefix(string(data)[tail+2:], "Z ") {
						return
					}
				}
				time.Sleep(10 * time.Millisecond)
			}
			t.Fatal("provider descendant remained running")
		})
	}
}

func TestGitHubTokenCommandSharedSyncOwnerAndInspection(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DBPath, cfg.CacheDir = filepath.Join(dir, "archive.db"), filepath.Join(dir, "cache")
	cfgPath := filepath.Join(dir, "config.toml")
	if err := config.Save(cfgPath, cfg); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(context.Background(), cfg.DBPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(dir, "provider-ran")
	t.Setenv("TOKEN_TEST_MARKER", marker)
	t.Setenv("GITHUB_TOKEN", "ambient-must-not-win")
	path := tokenCommandFixture(t, "echo called >> \"$TOKEN_TEST_MARKER\"\nprintf managed-fixture")
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.Header.Get("Authorization") != "Bearer managed-fixture" {
			t.Error("static credential won")
		}
		switch r.URL.Path {
		case "/repos/owner/repo":
			fmt.Fprint(w, `{"id":1,"full_name":"owner/repo","name":"repo","owner":{"login":"owner"}}`)
		case "/repos/owner/repo/issues":
			fmt.Fprint(w, `[]`)
		default:
			t.Errorf("unexpected request %s", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	t.Setenv("GITCRAWL_GITHUB_BASE_URL", server.URL)
	for _, args := range [][]string{
		{"sync", "owner/repo", "--state", "all", "--json"},
		{"refresh", "owner/repo", "--no-embed", "--no-cluster", "--json"},
		{"search", "issues", "missing", "-R", "owner/repo", "--sync-if-stale", "1ns", "--json", "number"},
	} {
		app := New()
		app.Stdout, app.Stderr = io.Discard, io.Discard
		if err := app.Run(context.Background(), append([]string{"--config", cfgPath, "--github-token-command", path}, args...)); err != nil {
			t.Fatal(err)
		}
	}
	if requests != 8 {
		t.Fatalf("sync requests=%d, want 8 including refresh/search closed reconciliation", requests)
	}
	before, err := os.ReadFile(marker)
	if err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"status", "--json"}, {"doctor", "--json"}, {"metadata", "--json"},
		{"refresh", "owner/repo", "--no-sync", "--no-embed", "--json"},
		{"fill-pr-details", "owner/repo", "--json"},
	} {
		app := New()
		app.Stdout, app.Stderr = io.Discard, io.Discard
		if err := app.Run(context.Background(), append([]string{"--config", cfgPath, "--github-token-command", path}, args...)); err != nil {
			t.Fatal(err)
		}
	}
	after, err := os.ReadFile(marker)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("read-only/no-sync/empty-fill invoked provider")
	}
}
