package cli

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openclaw/gitcrawl/internal/store"
)

type pooledSyncRequest struct {
	Pool    string              `json:"pool"`
	Method  string              `json:"method"`
	Path    string              `json:"path"`
	Query   map[string][]string `json:"query"`
	Headers map[string]string   `json:"headers"`
}

func newPooledSyncFixture(t *testing.T, handler func(http.ResponseWriter, pooledSyncRequest)) (string, string) {
	t.Helper()
	dir := t.TempDir()
	configPath, dbPath := filepath.Join(dir, "config.toml"), filepath.Join(dir, "archive.db")
	app := New()
	app.Stdout, app.Stderr = io.Discard, io.Discard
	if err := app.Run(context.Background(), []string{"--config", configPath, "init", "--db", dbPath}); err != nil {
		t.Fatal(err)
	}
	configureTestGitHubCache(t, configPath, filepath.Join(dir, "cache"), "provider-token-must-not-be-used")
	t.Setenv("GITHUB_TOKEN", "provider-token-must-not-be-used")
	direct := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unexpected direct GitHub request: %s", r.URL.Path)
		w.WriteHeader(http.StatusBadRequest)
	}))
	t.Cleanup(direct.Close)
	t.Setenv("GITCRAWL_GITHUB_BASE_URL", direct.URL)
	relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/github/request" || r.Header.Get("Authorization") != "Bearer fixture-caller" {
			t.Error("incorrect relay endpoint or caller authentication")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var req pooledSyncRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if req.Method != http.MethodGet || req.Pool != "fixture-pool" || req.Headers["cache-control"] != "max-age=0" || req.Headers["authorization"] != "" {
			t.Errorf("incorrect pooled read contract: method=%s pool=%s", req.Method, req.Pool)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		handler(w, req)
	}))
	t.Cleanup(relay.Close)
	t.Setenv("OCTOPOOL_URL", relay.URL)
	t.Setenv("OCTOPOOL_POOL", "fixture-pool")
	t.Setenv("OCTOPOOL_TOKEN", "fixture-caller")
	return configPath, dbPath
}

func pooledReply(t *testing.T, w http.ResponseWriter, status int, body any, headers map[string]string, encoding string) {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	var wire any = json.RawMessage(data)
	if encoding == "text" {
		wire = string(data)
	}
	if encoding == "base64" {
		wire = base64.StdEncoding.EncodeToString(data)
	}
	if err := json.NewEncoder(w).Encode(map[string]any{"status": status, "headers": headers, "body": wire, "body_encoding": encoding, "relay": map[string]any{"cache": "miss", "stale_ok": false}}); err != nil {
		t.Error(err)
	}
}

func runPooledSync(t *testing.T, configPath string, extra ...string) (string, error) {
	t.Helper()
	app := New()
	var output bytes.Buffer
	app.Stdout, app.Stderr = &output, io.Discard
	app.githubAuthTokenLookup = func(context.Context) (string, error) {
		t.Error("pooled sync resolved provider credentials")
		return "", fmt.Errorf("forbidden")
	}
	args := []string{"--config", configPath, "sync", "fixture/repo", "--octopool", "--state", "all", "--since", "2026-04-01T00:00:00Z", "--include-comments", "--json"}
	err := app.Run(context.Background(), append(args, extra...))
	return output.String(), err
}

func TestOctopoolSyncPaginatesRetriesAndRecoversIncompleteRun(t *testing.T) {
	var unavailable atomic.Bool
	var pageFailure atomic.Bool
	unavailable.Store(true)
	var pageTwoAttempts atomic.Int32
	configPath, dbPath := newPooledSyncFixture(t, func(w http.ResponseWriter, req pooledSyncRequest) {
		switch req.Path {
		case "/repos/fixture/repo":
			pooledReply(t, w, 200, map[string]any{"id": 123, "full_name": "fixture/repo", "private": false}, nil, "json")
		case "/repos/fixture/repo/issues":
			if req.Query["since"][0] != "2026-04-01T00:00:00Z" || req.Query["state"][0] != "all" {
				t.Error("lost incremental query")
			}
			if len(req.Query["page"]) == 0 {
				issue := githubIssueJSON(101, "issue", "retained issue")
				issue["id"] = "9007199254740993"
				pooledReply(t, w, 200, []any{issue}, map[string]string{"link": `<https://api.github.com/repos/fixture/repo/issues?state=all&since=2026-04-01T00%3A00%3A00Z&page=2>; rel="next"`}, "json")
			} else {
				if pageFailure.Load() {
					w.WriteHeader(424)
					return
				}
				if pageTwoAttempts.Add(1) == 1 {
					pooledReply(t, w, 503, map[string]string{"message": "temporary"}, map[string]string{"retry-after": "0"}, "text")
					return
				}
				pr := githubIssueJSON(102, "pull_request", "retained PR")
				pr["id"] = json.Number("9007199254740995")
				pooledReply(t, w, 200, []any{pr}, nil, "text")
			}
		case "/repos/fixture/repo/issues/101/comments":
			comment := githubCommentJSON(1001, "retained issue comment")
			comment["id"] = "9007199254740997"
			pooledReply(t, w, 200, []any{comment}, nil, "base64")
		case "/repos/fixture/repo/issues/102/comments":
			pooledReply(t, w, 200, []any{githubCommentJSON(1002, "retained PR comment")}, nil, "json")
		case "/repos/fixture/repo/pulls/102/reviews":
			if unavailable.Load() {
				w.WriteHeader(424)
				_, _ = io.WriteString(w, `{"error":{"code":"fallback_local"}}`)
				return
			}
			pooledReply(t, w, 200, []any{githubCommentJSON(1003, "retained review")}, nil, "json")
		case "/repos/fixture/repo/pulls/102/comments":
			pooledReply(t, w, 200, []any{githubCommentJSON(1004, "retained inline comment")}, nil, "json")
		default:
			t.Errorf("unexpected acquisition path %s", req.Path)
			w.WriteHeader(400)
		}
	})
	if _, err := runPooledSync(t, configPath); err == nil || !strings.Contains(err.Error(), "424") {
		t.Fatalf("missing partial failure: %v", err)
	}
	ctx := context.Background()
	inspect := func(complete bool) time.Time {
		st, err := store.Open(ctx, dbPath)
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()
		repo, err := st.RepositoryByFullName(ctx, "fixture/repo")
		if err != nil {
			t.Fatal(err)
		}
		watermark, err := st.LastSuccessfulListSyncAt(ctx, repo.ID, "all")
		if err != nil {
			t.Fatal(err)
		}
		if watermark.IsZero() == complete {
			t.Fatalf("completion watermark=%s complete=%t", watermark, complete)
		}
		threads, err := st.ListThreads(ctx, repo.ID, true)
		if err != nil {
			t.Fatal(err)
		}
		// Native failure receipts retain parent metadata, but not partial children.
		if len(threads) != 2 {
			t.Fatalf("threads=%d want=2", len(threads))
		}
		for _, thread := range threads {
			wantID := "9007199254740993"
			wantComments := 1
			if thread.Number == 102 {
				wantID = "9007199254740995"
				wantComments = 3
				if !complete {
					wantComments = 0
				}
			}
			if thread.GitHubID != wantID {
				t.Fatalf("provider identity changed: %s", thread.GitHubID)
			}
			comments, err := st.ListComments(ctx, thread.ID)
			if err != nil {
				t.Fatal(err)
			}
			if len(comments) != wantComments {
				t.Fatalf("comments=%d want=%d", len(comments), wantComments)
			}
			if thread.Number == 101 && comments[0].GitHubID != "9007199254740997" {
				t.Fatal("comment identity changed")
			}
		}
		return watermark
	}
	inspect(false)
	if pageTwoAttempts.Load() != 2 {
		t.Fatalf("transient page was not retried: %d", pageTwoAttempts.Load())
	}
	unavailable.Store(false)
	if _, err := runPooledSync(t, configPath); err != nil {
		t.Fatal(err)
	}
	inspect(true)
	if _, err := runPooledSync(t, configPath); err != nil {
		t.Fatal(err)
	}
	previous := inspect(true)
	pageFailure.Store(true)
	if _, err := runPooledSync(t, configPath); err == nil {
		t.Fatal("failed page reported success")
	}
	if after := inspect(true); !after.Equal(previous) {
		t.Fatal("failed pagination advanced the successful cutoff")
	}
}

func TestOctopoolSyncRejectsUnsupportedModesBeforeCredentials(t *testing.T) {
	for _, extra := range [][]string{{"--graphql-history"}, {"--include-pr-details"}, {"--with", "pr-metadata"}, {"--with", "pr-details"}} {
		t.Run(strings.Join(extra, " "), func(t *testing.T) {
			t.Setenv("OCTOPOOL_TOKEN", "")
			app := New()
			app.Stdout, app.Stderr = io.Discard, io.Discard
			app.githubAuthTokenLookup = func(context.Context) (string, error) {
				t.Error("credential resolution before rejection")
				return "", nil
			}
			err := app.Run(context.Background(), append([]string{"--config", filepath.Join(t.TempDir(), "missing.toml"), "sync", "fixture/repo", "--octopool"}, extra...))
			if err == nil || !strings.Contains(err.Error(), "--octopool does not support") {
				t.Fatalf("unsupported mode: %v", err)
			}
		})
	}
	t.Run("token-command", func(t *testing.T) {
		app := New()
		app.Stdout, app.Stderr = io.Discard, io.Discard
		err := app.Run(context.Background(), []string{"--github-token-command", filepath.Join(t.TempDir(), "missing-helper"), "sync", "fixture/repo", "--octopool"})
		if err == nil || !strings.Contains(err.Error(), "--octopool does not support") {
			t.Fatalf("token command not rejected: %v", err)
		}
	})
}

func TestOctopoolSyncRefusesRelayFailuresAndRedirects(t *testing.T) {
	var redirected atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { redirected.Add(1); w.WriteHeader(500) }))
	defer target.Close()
	for _, tc := range []struct {
		name, body, want string
		status           int
	}{
		{"authorization", `fixture-caller private diagnostic`, "401", 401},
		{"redirect", "", "302", 302},
		{"stale", `{"status":200,"body":{},"body_encoding":"json","relay":{"cache":"stale","stale_ok":true}}`, "stale", 200},
		{"truncated-envelope", `{"status":200`, "invalid Octopool", 200},
		{"invalid-encoding", `{"status":200,"body":{},"body_encoding":"unknown"}`, "unsupported Octopool body", 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			configPath, dbPath := newPooledSyncFixture(t, func(w http.ResponseWriter, _ pooledSyncRequest) {
				w.Header().Set("Location", target.URL)
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			})
			_, err := runPooledSync(t, configPath)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("failure not preserved: %v", err)
			}
			if strings.Contains(err.Error(), "fixture-caller") || strings.Contains(err.Error(), "private diagnostic") {
				t.Fatal("relay diagnostic leaked")
			}
			st, err := store.Open(context.Background(), dbPath)
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			if _, err := st.RepositoryByFullName(context.Background(), "fixture/repo"); err == nil {
				t.Fatal("failed relay created repository state")
			}
		})
	}
	if redirected.Load() != 0 {
		t.Fatal("caller credential followed a redirect")
	}
}

func TestOctopoolSyncRequiresCallerTokenWithoutProviderLookup(t *testing.T) {
	configPath, _ := newPooledSyncFixture(t, func(http.ResponseWriter, pooledSyncRequest) { t.Error("request without caller token") })
	t.Setenv("OCTOPOOL_TOKEN", "")
	_, err := runPooledSync(t, configPath)
	if err == nil || !strings.Contains(err.Error(), "OCTOPOOL_TOKEN") {
		t.Fatalf("missing caller token: %v", err)
	}
	for _, args := range [][]string{{"sync", "fixture/repo", "--octopool"}, {"sync", "--octopool=true", "fixture/repo"}} {
		if releaseNotificationAllowed(args) {
			t.Fatal("pooled sync permits direct release notification")
		}
	}
}
