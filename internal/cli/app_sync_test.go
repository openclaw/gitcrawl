package cli

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/openclaw/gitcrawl/internal/config"
	gh "github.com/openclaw/gitcrawl/internal/github"
	"github.com/openclaw/gitcrawl/internal/store"
	"github.com/openclaw/gitcrawl/internal/syncer"
)

type appSeedTransport struct {
	t       *testing.T
	origin  *url.URL
	backend http.RoundTripper
}

func (f appSeedTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.Scheme != "https" || r.URL.Host != "api.github.com" {
		f.t.Errorf("App seed escaped its HTTPS origin: %s", r.URL.Host)
		return nil, errors.New("unexpected origin")
	}
	copyRequest := r.Clone(r.Context())
	copyURL := *r.URL
	copyURL.Scheme, copyURL.Host = f.origin.Scheme, f.origin.Host
	copyRequest.URL = &copyURL
	response, err := f.backend.RoundTrip(copyRequest)
	if response != nil {
		response.Request = r
	}
	return response, err
}

func appSeedFixture(t *testing.T, handler http.HandlerFunc) (string, string) {
	t.Helper()
	for _, name := range []string{"OCTOPOOL_URL", "OCTOPOOL_POOL", "OCTOPOOL_TOKEN", "GITCRAWL_GITHUB_BASE_URL", "GITHUB_BASE_URL"} {
		t.Setenv(name, "")
	}
	credential := gh.AcquisitionToken{Token: "synthetic-app-secret", AppID: 1, InstallationID: 2, RepositoryID: 123,
		Repository: "fixture/repo", ExpiresAt: time.Now().Add(time.Hour),
		Permissions: map[string]string{"metadata": "read", "contents": "read", "issues": "read", "pull_requests": "read"}}
	raw, _ := json.Marshal(credential)
	t.Setenv("APP_SEED_TOKEN", string(raw))
	t.Setenv("GITHUB_TOKEN", "personal-token-must-not-be-used")
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	origin, _ := url.Parse(server.URL)
	original := http.DefaultTransport
	http.DefaultTransport = appSeedTransport{t, origin, original}
	t.Cleanup(func() { http.DefaultTransport = original })
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	a := New()
	a.Stdout, a.Stderr = io.Discard, io.Discard
	if err := a.Run(context.Background(), []string{"--config", configPath, "init", "--runtime-dir", dir}); err != nil {
		t.Fatal(err)
	}
	// init chooses its native database name; keep the fixture on that exact owner.
	cfg, err := config.Load(configPath)
	if err != nil {
		t.Fatal(err)
	}
	return configPath, cfg.DBPath
}

func runAppSeed(t *testing.T, configPath string, extra ...string) (syncer.Stats, string, error) {
	t.Helper()
	a := New()
	var output, diagnostic bytes.Buffer
	a.Stdout, a.Stderr = &output, &diagnostic
	a.githubAuthTokenLookup = func(context.Context) (string, error) {
		t.Error("App seed resolved personal auth")
		return "", errors.New("forbidden")
	}
	args := []string{"--config", configPath, "sync", "fixture/repo", "--github-app-token-env", "APP_SEED_TOKEN", "--timeout", "1m", "--state", "all", "--since", "2026-04-01T00:00:00Z", "--include-comments", "--with", "pr-metadata", "--limit", "3", "--json"}
	err := a.Run(context.Background(), append(args, extra...))
	var result syncer.Stats
	if output.Len() > 0 {
		if e := json.Unmarshal(output.Bytes(), &result); e != nil {
			t.Fatalf("invalid sync result: %v", e)
		}
	}
	if strings.Contains(output.String()+diagnostic.String()+fmt.Sprint(err), "synthetic-app-secret") {
		t.Fatal("credential escaped into diagnostics")
	}
	return result, diagnostic.String(), err
}

func TestAppSeedQuotaStopAndReplayPreserveCompletedRows(t *testing.T) {
	remaining, comments := 4900, 0
	deplete := true
	configPath, dbPath := appSeedFixture(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer synthetic-app-secret" {
			t.Error("wrong provider credential")
		}
		w.Header().Set("X-RateLimit-Limit", "5000")
		w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(remaining))
		w.Header().Set("X-RateLimit-Reset", fmt.Sprint(time.Now().Add(time.Hour).Unix()))
		switch r.URL.Path {
		case "/rate_limit":
			fmt.Fprintf(w, `{"resources":{"core":{"limit":5000,"remaining":%d,"reset":%d},"graphql":{"limit":5000,"remaining":4900,"reset":%d}}}`, remaining, time.Now().Add(time.Hour).Unix(), time.Now().Add(time.Hour).Unix())
		case "/installation/repositories":
			fmt.Fprint(w, `{"total_count":1,"repositories":[{"id":123,"full_name":"fixture/repo","private":false,"visibility":"public"}]}`)
		case "/repos/fixture/repo":
			fmt.Fprint(w, `{"id":123,"full_name":"fixture/repo","private":false,"visibility":"public"}`)
		case "/repos/fixture/repo/issues":
			if r.URL.Query().Get("since") != "2026-04-01T00:00:00Z" || r.URL.Query().Get("state") != "all" {
				t.Error("seed lost its explicit lower bound or state")
			}
			json.NewEncoder(w).Encode([]any{githubIssueJSON(101, "issue", "first"), githubIssueJSON(102, "pull_request", "second"), githubIssueJSON(103, "issue", "third")})
		case "/repos/fixture/repo/pulls/102":
			fmt.Fprint(w, `{"number":102,"merged_by":{"login":"maintainer"},"changed_files":0}`)
		case "/repos/fixture/repo/pulls/102/reviews":
			json.NewEncoder(w).Encode([]any{githubCommentJSON(2001, "review")})
		case "/repos/fixture/repo/pulls/102/comments":
			json.NewEncoder(w).Encode([]any{githubCommentJSON(2002, "inline comment")})
		case "/repos/fixture/repo/issues/101/comments", "/repos/fixture/repo/issues/102/comments", "/repos/fixture/repo/issues/103/comments":
			comments++
			if deplete {
				if comments != 1 {
					t.Error("request dispatched after quota floor")
				}
				remaining = 2500
				w.Header().Set("X-RateLimit-Remaining", "2500")
			}
			number, _ := strconv.Atoi(strings.Split(r.URL.Path, "/")[5])
			json.NewEncoder(w).Encode([]any{githubCommentJSON(1000+number, "retained comment")})
		default:
			t.Errorf("unexpected seed request %s", r.URL.Path)
			w.WriteHeader(400)
		}
	})
	stats, _, err := runAppSeed(t, configPath)
	var reserve *gh.RateLimitReserveError
	if !errors.As(err, &reserve) || stats.ThreadsSynced != 1 || stats.CommentsSynced != 1 || comments != 1 {
		t.Fatalf("quota stop lost partial result: stats=%+v comments=%d err=%v", stats, comments, err)
	}
	ctx := context.Background()
	st, err := store.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	repo, err := st.RepositoryByFullName(ctx, "fixture/repo")
	if err != nil {
		t.Fatal(err)
	}
	threads, err := st.ListThreads(ctx, repo.ID, true)
	if err != nil {
		t.Fatal(err)
	}
	var firstID int64
	for _, thread := range threads {
		if thread.Number == 101 {
			firstID = thread.ID
		}
	}
	if firstID == 0 {
		t.Fatal("completed thread missing")
	}
	watermark, err := st.LastSuccessfulListSyncAt(ctx, repo.ID, "all")
	if err != nil || !watermark.IsZero() {
		t.Fatalf("partial traversal acquired a success watermark: %v %v", watermark, err)
	}
	st.Close()
	deplete, remaining = false, 4900
	stats, _, err = runAppSeed(t, configPath)
	if err != nil || stats.ThreadsSynced != 3 || stats.PRDetailsSynced != 1 {
		t.Fatalf("bounded replay failed: %+v %v", stats, err)
	}
	st, err = store.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	threads, err = st.ListThreads(ctx, repo.ID, true)
	if err != nil || len(threads) != 3 {
		t.Fatalf("replay inventory: %v %d", err, len(threads))
	}
	for _, thread := range threads {
		if thread.Number == 101 && thread.ID != firstID {
			t.Fatal("replay replaced native identity")
		}
		rows, err := st.ListComments(ctx, thread.ID)
		want := 1
		if thread.Number == 102 {
			want = 3
		}
		if err != nil || len(rows) != want {
			t.Fatalf("replay changed child inventory: number=%d children=%d err=%v", thread.Number, len(rows), err)
		}
	}
}

func TestAppSeedProviderAdmissionStopsBeforeParentReads(t *testing.T) {
	for _, kind := range []string{"core reserve", "GraphQL reserve", "private repository", "redirect"} {
		t.Run(kind, func(t *testing.T) {
			parentReads := 0
			configPath, _ := appSeedFixture(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("X-RateLimit-Limit", "5000")
				w.Header().Set("X-RateLimit-Remaining", "4900")
				w.Header().Set("X-RateLimit-Reset", fmt.Sprint(time.Now().Add(time.Hour).Unix()))
				switch r.URL.Path {
				case "/rate_limit":
					core, graph := 4900, 4900
					if kind == "core reserve" {
						core = 2500
					}
					if kind == "GraphQL reserve" {
						graph = 2500
					}
					fmt.Fprintf(w, `{"resources":{"core":{"limit":5000,"remaining":%d,"reset":%d},"graphql":{"limit":5000,"remaining":%d,"reset":%d}}}`, core, time.Now().Add(time.Hour).Unix(), graph, time.Now().Add(time.Hour).Unix())
				case "/installation/repositories":
					if kind == "redirect" {
						w.Header().Set("Location", "https://outside.example/credential-sink")
						w.WriteHeader(302)
						return
					}
					fmt.Fprintf(w, `{"total_count":1,"repositories":[{"id":123,"full_name":"fixture/repo","private":%t,"visibility":"public"}]}`, kind == "private repository")
				case "/repos/fixture/repo":
					fmt.Fprint(w, `{"id":123,"full_name":"fixture/repo","private":false,"visibility":"public"}`)
				default:
					parentReads++
					w.WriteHeader(400)
				}
			})
			_, _, err := runAppSeed(t, configPath)
			if err == nil || parentReads != 0 {
				t.Fatalf("unsafe provider admission: reads=%d err=%v", parentReads, err)
			}
			if strings.HasSuffix(kind, "reserve") {
				var reserve *gh.RateLimitReserveError
				if !errors.As(err, &reserve) {
					t.Fatalf("lost typed reserve: %v", err)
				}
			}
		})
	}
}

func TestAppSeedRefusesInvalidAuthAndMixedModesBeforeRuntime(t *testing.T) {
	for _, tc := range []struct {
		name       string
		args       []string
		env, value string
	}{
		{"empty flag", []string{"--github-app-token-env="}, "", ""},
		{"missing token", nil, "APP_SEED_TOKEN", ""},
		{"malformed", nil, "APP_SEED_TOKEN", "{}"},
		{"expired", nil, "APP_SEED_TOKEN", "expired"},
		{"relay flag", []string{"--octopool"}, "", ""},
		{"relay config", nil, "OCTOPOOL_TOKEN", "relay"},
		{"unsafe origin", nil, "GITCRAWL_GITHUB_BASE_URL", "http://127.0.0.1:1"},
		{"GraphQL history", []string{"--graphql-history"}, "", ""},
		{"full details", []string{"--include-pr-details"}, "", ""},
		{"deadline", []string{"--timeout", "0s"}, "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requests := 0
			configPath, dbPath := appSeedFixture(t, func(w http.ResponseWriter, r *http.Request) { requests++; w.WriteHeader(500) })
			if tc.env != "" {
				value := tc.value
				if value == "expired" {
					var credential gh.AcquisitionToken
					if err := json.Unmarshal([]byte(os.Getenv(tc.env)), &credential); err != nil {
						t.Fatal(err)
					}
					credential.ExpiresAt = time.Now().Add(-time.Minute)
					raw, _ := json.Marshal(credential)
					value = string(raw)
				}
				t.Setenv(tc.env, value)
			}
			before, err := os.Stat(dbPath)
			if err != nil {
				t.Fatal(err)
			}
			_, _, err = runAppSeed(t, configPath, tc.args...)
			after, statErr := os.Stat(dbPath)
			if err == nil || requests != 0 || statErr != nil || before.Size() != after.Size() || before.ModTime() != after.ModTime() {
				t.Fatalf("refusal touched provider/runtime: calls=%d err=%v stat=%v", requests, err, statErr)
			}
		})
	}
	if releaseNotificationAllowed([]string{"sync", "fixture/repo", "--github-app-token-env=APP_SEED_TOKEN"}) {
		t.Fatal("App seed would perform unrelated release lookup")
	}
}

func TestAppSeedRejectsGlobalTokenCommand(t *testing.T) {
	a := New()
	a.Stdout, a.Stderr = io.Discard, io.Discard
	err := a.Run(context.Background(), []string{"--github-token-command", filepath.Join(t.TempDir(), "must-not-run"), "sync", "fixture/repo", "--github-app-token-env", "APP_SEED_TOKEN"})
	if err == nil || !strings.Contains(err.Error(), "cannot be combined") {
		t.Fatalf("App seed accepted another credential owner: %v", err)
	}
}

func TestAppSeedRejectsChangedRepositoryBeforePersistence(t *testing.T) {
	for _, change := range []string{"private", "identity"} {
		t.Run(change, func(t *testing.T) {
			metadataReads, parentReads := 0, 0
			configPath, dbPath := appSeedFixture(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("X-RateLimit-Limit", "5000")
				w.Header().Set("X-RateLimit-Remaining", "4900")
				w.Header().Set("X-RateLimit-Reset", fmt.Sprint(time.Now().Add(time.Hour).Unix()))
				switch r.URL.Path {
				case "/rate_limit":
					fmt.Fprintf(w, `{"resources":{"core":{"limit":5000,"remaining":4900,"reset":%d},"graphql":{"limit":5000,"remaining":4900,"reset":%d}}}`, time.Now().Add(time.Hour).Unix(), time.Now().Add(time.Hour).Unix())
				case "/installation/repositories":
					fmt.Fprint(w, `{"total_count":1,"repositories":[{"id":123,"full_name":"fixture/repo","private":false,"visibility":"public"}]}`)
				case "/repos/fixture/repo":
					metadataReads++
					id, private := 123, false
					if metadataReads > 1 {
						if change == "private" {
							private = true
						} else {
							id = 456
						}
					}
					fmt.Fprintf(w, `{"id":%d,"full_name":"fixture/repo","private":%t,"visibility":"public"}`, id, private)
				default:
					parentReads++
					fmt.Fprint(w, `[]`)
				}
			})
			_, _, err := runAppSeed(t, configPath)
			if err == nil || metadataReads != 2 || parentReads != 0 {
				t.Fatalf("changed identity reached seed: metadata=%d parents=%d err=%v", metadataReads, parentReads, err)
			}
			st, err := store.Open(context.Background(), dbPath)
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			if _, err = st.RepositoryByFullName(context.Background(), "fixture/repo"); !errors.Is(err, sql.ErrNoRows) {
				t.Fatalf("changed repository was persisted: %v", err)
			}
		})
	}
}

func TestAppSeedPaginationPreservesSelection(t *testing.T) {
	for _, change := range []string{"numeric link", "host", "resource", "since", "state", "page size"} {
		t.Run(change, func(t *testing.T) {
			pageReads := 0
			configPath, _ := appSeedFixture(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("X-RateLimit-Limit", "5000")
				w.Header().Set("X-RateLimit-Remaining", "4900")
				w.Header().Set("X-RateLimit-Reset", fmt.Sprint(time.Now().Add(time.Hour).Unix()))
				switch r.URL.Path {
				case "/rate_limit":
					fmt.Fprintf(w, `{"resources":{"core":{"limit":5000,"remaining":4900,"reset":%d},"graphql":{"limit":5000,"remaining":4900,"reset":%d}}}`, time.Now().Add(time.Hour).Unix(), time.Now().Add(time.Hour).Unix())
				case "/installation/repositories":
					fmt.Fprint(w, `{"total_count":1,"repositories":[{"id":123,"full_name":"fixture/repo","private":false,"visibility":"public"}]}`)
				case "/repos/fixture/repo":
					fmt.Fprint(w, `{"id":123,"full_name":"fixture/repo","private":false,"visibility":"public"}`)
				default:
					pageReads++
					if pageReads == 1 {
						next := url.URL{Scheme: "https", Host: "api.github.com", Path: "/repositories/123/issues"}
						query := r.URL.Query()
						query.Set("page", "2")
						switch change {
						case "host":
							next.Host = "outside.example"
						case "resource":
							next.Path = "/repos/fixture/repo/issues/101/comments"
						case "since":
							query.Set("since", "2025-01-01T00:00:00Z")
						case "state":
							query.Set("state", "open")
						case "page size":
							query.Set("per_page", "1")
						}
						next.RawQuery = query.Encode()
						w.Header().Set("Link", "<"+next.String()+">; rel=\"next\"")
					}
					json.NewEncoder(w).Encode([]any{githubIssueJSON(100+pageReads, "issue", "bounded page")})
				}
			})
			stats, _, err := runAppSeed(t, configPath, "--include-comments=false", "--with=")
			if change == "numeric link" {
				if err != nil || pageReads != 2 || stats.ThreadsSynced != 2 {
					t.Fatalf("numeric pagination failed: %+v reads=%d err=%v", stats, pageReads, err)
				}
			} else if err == nil || pageReads != 1 {
				t.Fatalf("changed page dispatched: reads=%d err=%v", pageReads, err)
			}
		})
	}
}
