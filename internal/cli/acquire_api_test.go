package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openclaw/gitcrawl/internal/github"
)

func TestAcquireAPIValidationDoesNotOpenArchiveOrResolveGitHubToken(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "native.sqlite")
	t.Setenv("GITCRAWL_DB", target)
	t.Setenv("EMPTY_PUBLISHER", "")
	t.Setenv("EMPTY_READER", "")
	a := New()
	a.Stdout = io.Discard
	a.Stderr = io.Discard
	a.githubAuthTokenLookup = func(context.Context) (string, error) { t.Fatal("looked up personal GitHub token"); return "", nil }
	if releaseNotificationAllowed([]string{"acquire-api"}) {
		t.Fatal("unrelated GitHub release lookup enabled")
	}
	err := a.Run(context.Background(), []string{"acquire-api", "--endpoint", "https://archive.example", "--source", "fixture", "--repo", "example/project", "--publisher-token-env", "EMPTY_PUBLISHER", "--reader-token-env", "EMPTY_READER", "--since", "2026-01-01T00:00:00Z"})
	if err == nil {
		t.Fatal("missing explicit credentials accepted")
	}
	if _, err = os.Stat(target); !os.IsNotExist(err) {
		t.Fatal("archive touched", err)
	}
}

func TestAcquireAPIReadsOneScopedAppCredentialFromStdin(t *testing.T) {
	for _, key := range []string{"OCTOPOOL_TOKEN", "OCTOPOOL_URL", "OCTOPOOL_POOL"} {
		t.Setenv(key, "")
	}
	const secret = "synthetic-installation-secret"
	token := github.AcquisitionToken{
		Token: secret, AppID: 1, InstallationID: 2, RepositoryID: 42, Repository: "example/project",
		ExpiresAt:   time.Now().Add(time.Hour),
		Permissions: map[string]string{"metadata": "read", "contents": "read", "issues": "read", "pull_requests": "read"},
	}
	envelope, err := json.Marshal(token)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("PUB", "fixture-publisher")
	t.Setenv("READ", "fixture-reader")
	t.Setenv("APP", string(envelope))
	t.Setenv("APP_BEARER", secret)

	var calls atomic.Int32
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/rate_limit" || r.Header.Get("Authorization") != "Bearer "+secret {
			t.Errorf("wrong App bootstrap request")
		}
		w.WriteHeader(http.StatusForbidden)
	}))
	t.Cleanup(provider.Close)
	origin, _ := url.Parse(provider.URL)
	original := http.DefaultTransport
	http.DefaultTransport = appSeedTransport{t, origin, original}
	t.Cleanup(func() { http.DefaultTransport = original })

	base := []string{"acquire-api", "--cycle", "--endpoint", "http://127.0.0.1:1", "--source", "fixture",
		"--repo", token.Repository, "--publisher-token-env", "PUB", "--reader-token-env", "READ"}
	for _, tc := range []struct {
		name, input, want string
		args              []string
		reachesProvider   bool
		octopool          bool
	}{
		{name: "pipe", input: string(envelope), args: []string{"--github-app-token-stdin"}, reachesProvider: true},
		{name: "environment", args: []string{"--github-app-token-env", "APP"}, reachesProvider: true},
		{name: "two envelopes", input: string(envelope) + string(envelope), args: []string{"--github-app-token-stdin"}, want: "invalid installation credential JSON"},
		{name: "oversize", input: strings.Repeat(" ", github.MaxAcquisitionTokenBytes+1), args: []string{"--github-app-token-stdin"}, want: "credential exceeds size limit"},
		{name: "both input methods", input: string(envelope), args: []string{"--github-app-token-stdin", "--github-app-token-env", "APP"}, want: "cannot be combined"},
		{name: "mixed providers", input: string(envelope), args: []string{"--github-app-token-stdin"}, octopool: true, want: "Octopool configuration"},
		{name: "reused bearer", input: string(envelope), args: []string{"--github-app-token-stdin", "--lifecycle-reader-endpoint", "https://reader.example", "--lifecycle-reader-token-env", "APP_BEARER"}, want: "uses provider authentication"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.octopool {
				t.Setenv("OCTOPOOL_URL", "https://relay.example")
			}
			a := New()
			a.Stdin = strings.NewReader(tc.input)
			a.Stdout, a.Stderr = io.Discard, io.Discard
			a.githubAuthTokenLookup = func(context.Context) (string, error) {
				t.Fatal("consulted personal GitHub authentication")
				return "", nil
			}
			before := calls.Load()
			err := a.Run(context.Background(), append(slices.Clone(base), tc.args...))
			if err == nil || strings.Contains(err.Error(), secret) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("wrong acquisition result: %v", err)
			}
			if tc.reachesProvider {
				before++
			}
			if calls.Load() != before {
				t.Fatal("unexpected provider dispatch")
			}
		})
	}
}

func TestAcquireAPIAppCredentialPipeHonorsTimeout(t *testing.T) {
	for _, key := range []string{"OCTOPOOL_TOKEN", "OCTOPOOL_URL", "OCTOPOOL_POOL"} {
		t.Setenv(key, "")
	}
	t.Setenv("PUB", "fixture-publisher")
	t.Setenv("READ", "fixture-reader")
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close(); _ = w.Close() })
	if _, err := io.WriteString(w, "{}"); err != nil {
		t.Fatal(err)
	}
	a := New()
	a.Stdin, a.Stdout, a.Stderr = r, io.Discard, io.Discard
	done := make(chan error, 1)
	go func() {
		done <- a.Run(context.Background(), []string{"acquire-api", "--endpoint", "http://127.0.0.1:1",
			"--source", "fixture", "--repo", "example/project", "--publisher-token-env", "PUB",
			"--reader-token-env", "READ", "--github-app-token-stdin", "--timeout", "40ms"})
	}()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected an acquisition timeout waiting for EOF, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("acquisition waited past its timeout for credential EOF")
	}
}

func TestAcquireAPILifecycleDispatchWithoutSQLite(t *testing.T) {
	root := t.TempDir()
	db := filepath.Join(root, "archive", "native.sqlite")
	selection := filepath.Join(root, "numbers.txt")
	if err := os.WriteFile(selection, []byte("7\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GITCRAWL_DB", db)
	t.Setenv("LIFECYCLE_PUBLISHER", "publisher")
	t.Setenv("LIFECYCLE_READER", "reader")
	t.Setenv("CANDIDATE_READER", "separate-reader")
	t.Setenv("OCTOPOOL_TOKEN", "relay")
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("other active mode must stop before provider call")
		w.WriteHeader(500)
	}))
	defer provider.Close()
	t.Setenv("OCTOPOOL_URL", provider.URL)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || !strings.HasSuffix(r.URL.Path, "/acquisition/state") || r.Header.Get("Authorization") != "Bearer publisher" {
			t.Error("wrong acquisition route")
		}
		fmt.Fprint(w, `{"revision":1,"sequence":20,"checkpoint":{},"completedThrough":"2026-01-04T00:00:00Z","activeRun":{"id":"other","mode":"historical-parents","expectedRevision":1,"horizon":"2026-01-04T00:00:00Z","scope":{}}}`)
	}))
	defer api.Close()
	a := New()
	a.Stdout = io.Discard
	a.Stderr = io.Discard
	a.githubAuthTokenLookup = func(context.Context) (string, error) { t.Fatal("personal GitHub credential lookup"); return "", nil }
	err := a.Run(context.Background(), []string{"acquire-api", "--endpoint", api.URL, "--source", "fixture", "--repo", "example/project", "--publisher-token-env", "LIFECYCLE_PUBLISHER", "--reader-token-env", "LIFECYCLE_READER", "--lifecycle-numbers-file", selection, "--since", "2026-01-01T00:00:00Z", "--lifecycle-max-pages", "1"})
	if err == nil || !strings.Contains(err.Error(), "another acquisition mode is active") {
		t.Fatal("lifecycle selector not dispatched", err)
	}
	cycleArgs := []string{"acquire-api", "--cycle", "--lifecycle-max-pages", "1", "--lifecycle-reader-endpoint", api.URL, "--lifecycle-reader-token-env", "CANDIDATE_READER", "--endpoint", api.URL, "--source", "fixture", "--repo", "example/project", "--publisher-token-env", "LIFECYCLE_PUBLISHER", "--reader-token-env", "LIFECYCLE_READER"}
	for _, historyArgs := range [][]string{nil, {"--numbers-file", selection}} {
		err = a.Run(context.Background(), append(slices.Clone(cycleArgs), historyArgs...))
		if err == nil || !strings.Contains(err.Error(), "outside the admitted cycle") {
			t.Fatal("scheduled window did not reach the active-run owner", err)
		}
	}
	var output bytes.Buffer
	a.Stdout = &output
	err = a.Run(context.Background(), append(slices.Clone(cycleArgs), "--cycle-turns", "3", "--cycle-one-window", "--json"))
	var report map[string]json.RawMessage
	if err == nil || !strings.Contains(err.Error(), "outside the admitted cycle") || json.Unmarshal(output.Bytes(), &report) != nil || string(report["turns"]) != "[]" {
		t.Fatalf("multi-turn pre-dispatch error must report an empty observed batch: %s, %v", output.String(), err)
	}
	if _, err = os.Stat(filepath.Dir(db)); !os.IsNotExist(err) {
		t.Fatal("SQLite path touched", err)
	}
}

func TestAcquireAPILifecycleRejectsMixedSelectors(t *testing.T) {
	a := New()
	a.Stdout = io.Discard
	a.Stderr = io.Discard
	base := []string{"acquire-api", "--endpoint", "https://archive.example", "--source", "fixture", "--repo", "example/project", "--publisher-token-env", "PUB", "--reader-token-env", "READ"}
	for _, args := range [][]string{{"--cycle-turns", "1"}, {"--cycle", "--cycle-turns", "0"}, {"--cycle", "--cycle-turns", "9"}} {
		if err := a.Run(context.Background(), append(slices.Clone(base), args...)); err == nil || !strings.Contains(err.Error(), "cycle-turns requires") {
			t.Fatalf("invalid cycle count accepted (%v): %v", args, err)
		}
	}
	for _, args := range [][]string{{"--cycle-one-window"}, {"--cycle", "--cycle-one-window"}} {
		if err := a.Run(context.Background(), append(slices.Clone(base), args...)); err == nil || !strings.Contains(err.Error(), "cycle-one-window requires") {
			t.Fatalf("invalid cycle window accepted (%v): %v", args, err)
		}
	}
	for _, args := range [][]string{{"--commits", "--numbers-file", "unused"}, {"--commits", "--parent-selection-file", "unused"}, {"--commits", "--lifecycle-numbers-file", "unused"}, {"--commit-max-pages", "1"}, {"--lifecycle-numbers-file", "unused", "--numbers-file", "unused"}, {"--lifecycle-numbers-file", "unused", "--parent-selection-file", "unused"}, {"--lifecycle-max-pages", "1"}} {
		if err := a.Run(context.Background(), append(append([]string{}, base...), args...)); err == nil {
			t.Fatal("mixed or unselected lifecycle flags accepted", args)
		}
	}
}

func TestAcquireAPIParentSelectionRejectsAmbiguousModeWithoutOpeningArchive(t *testing.T) {
	root := t.TempDir()
	db := filepath.Join(root, "native.sqlite")
	t.Setenv("GITCRAWL_DB", db)
	a := New()
	a.Stdout = io.Discard
	a.Stderr = io.Discard
	a.githubAuthTokenLookup = func(context.Context) (string, error) { t.Fatal("personal credential lookup"); return "", nil }
	err := a.Run(context.Background(), []string{"acquire-api", "--endpoint", "https://archive.example", "--source", "fixture", "--repo", "example/project", "--publisher-token-env", "PUB", "--reader-token-env", "READER", "--parent-selection-file", "unused.json", "--since", "2026-01-01T00:00:00Z"})
	if err == nil {
		t.Fatal("accepted mixed parent and incremental mode")
	}

	if _, err = os.Stat(db); !os.IsNotExist(err) {
		t.Fatal("opened SQLite", err)
	}
}

func TestAcquireAPICandidateCredentialRejectsAmbiguousAuthority(t *testing.T) {
	for _, key := range []string{"OCTOPOOL_TOKEN", "OCTOPOOL_URL", "OCTOPOOL_POOL"} {
		t.Setenv(key, "")
	}
	root := t.TempDir()
	selection := filepath.Join(root, "history")
	if err := os.WriteFile(selection, []byte("7\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PUB", "write-only-fixture")
	t.Setenv("READ", "read-only-fixture")
	t.Setenv("APP", "app-fixture-envelope")
	t.Setenv("ALIAS_PUB", os.Getenv("PUB"))
	t.Setenv("ALIAS_APP", os.Getenv("APP"))
	t.Setenv("NO_SECRET", "")
	a := New()
	a.Stdout, a.Stderr = io.Discard, io.Discard
	base := []string{"acquire-api", "--cycle", "--numbers-file", selection, "--repo", "example/project", "--source", "fixture", "--endpoint", "https://archive.example", "--publisher-token-env", "PUB", "--reader-token-env", "READ", "--github-app-token-env", "APP"}
	for _, tc := range []struct {
		endpoint  bool
		env, want string
	}{
		{false, "READ", "requires a separate lifecycle endpoint"},
		{true, "", "credential environment name"},
		{true, "NO_SECRET", "credential is missing"},
		{true, "PUB", "credential environment name"},
		{true, "APP", "credential environment name"},
		{true, "ALIAS_PUB", "invalid lifecycle reader credential"},
		{true, "ALIAS_APP", "uses provider authentication"},
	} {
		args := append([]string{}, base...)
		if tc.endpoint {
			args = append(args, "--lifecycle-reader-endpoint", "https://reader.example")
		}
		err := a.Run(context.Background(), append(args, "--lifecycle-reader-token-env", tc.env))
		if err == nil || !strings.Contains(err.Error(), tc.want) || strings.Contains(err.Error(), "fixture-envelope") || strings.Contains(err.Error(), "write-only-fixture") {
			t.Fatalf("invalid reader admission (%q, %t): %v", tc.env, tc.endpoint, err)
		}
	}
	t.Setenv("OCTOPOOL_TOKEN", "relay-fixture")
	t.Setenv("ALIAS_RELAY", os.Getenv("OCTOPOOL_TOKEN"))
	args := append([]string{}, base[:len(base)-2]...)
	err := a.Run(context.Background(), append(args, "--lifecycle-reader-endpoint", "https://reader.example", "--lifecycle-reader-token-env", "ALIAS_RELAY"))
	if err == nil || !strings.Contains(err.Error(), "uses provider authentication") {
		t.Fatal("accepted provider relay credential", err)
	}

}
