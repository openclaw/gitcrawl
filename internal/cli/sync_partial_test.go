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
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	gh "github.com/openclaw/gitcrawl/internal/github"
	"github.com/openclaw/gitcrawl/internal/store"
	"github.com/openclaw/gitcrawl/internal/syncer"
)

func TestSyncCommandsReturnCommittedCountsAndFailure(t *testing.T) {
	for _, command := range []string{"sync", "fill-pr-details"} {
		t.Run(command, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			configPath, dbPath := filepath.Join(dir, "config.toml"), filepath.Join(dir, "archive.db")
			if err := New().Run(ctx, []string{"--config", configPath, "init", "--db", dbPath}); err != nil {
				t.Fatal(err)
			}
			configureTestGitHubCache(t, configPath, filepath.Join(dir, "cache"), "test-token")
			if command == "fill-pr-details" {
				st, err := store.Open(ctx, dbPath)
				if err != nil {
					t.Fatal(err)
				}
				repoID, err := st.UpsertRepository(ctx, store.Repository{
					Owner: "fixture", Name: "repo", FullName: "fixture/repo", RawJSON: "{}", UpdatedAt: "2026-04-01T00:00:00Z",
				})
				if err != nil {
					t.Fatal(err)
				}
				for _, number := range []int{101, 102, 103} {
					if _, err := st.UpsertThread(ctx, store.Thread{
						RepoID: repoID, GitHubID: strconv.Itoa(number + 10000), Number: number, Kind: "pull_request", State: "open",
						Title: "fixture", LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}",
						ContentHash: fmt.Sprint(number), UpdatedAt: "2026-04-01T00:00:00Z",
					}); err != nil {
						t.Fatal(err)
					}
				}
				if err := st.Close(); err != nil {
					t.Fatal(err)
				}
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("X-RateLimit-Limit", "5000")
				w.Header().Set("X-RateLimit-Remaining", "4990")
				w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10))
				path := r.URL.Path
				switch {
				case path == "/rate_limit":
					_ = json.NewEncoder(w).Encode(map[string]any{"resources": map[string]any{
						"core":    map[string]any{"limit": 5000, "remaining": 4990, "reset": time.Now().Add(time.Hour).Unix()},
						"graphql": map[string]any{"limit": 5000, "remaining": 4990, "reset": time.Now().Add(time.Hour).Unix()},
					}})
				case path == "/repos/fixture/repo":
					_ = json.NewEncoder(w).Encode(map[string]any{"id": 123, "full_name": "fixture/repo"})
				case path == "/repos/fixture/repo/issues/102":
					http.Error(w, "unavailable", http.StatusNotFound)
				case path == "/repos/fixture/repo/issues/101" || path == "/repos/fixture/repo/issues/103":
					number, _ := strconv.Atoi(path[strings.LastIndex(path, "/")+1:])
					kind := "issue"
					if command == "fill-pr-details" {
						kind = "pull_request"
					}
					_ = json.NewEncoder(w).Encode(githubIssueJSON(number, kind, "fixture"))
				case strings.HasSuffix(path, "/comments"), strings.HasSuffix(path, "/files"), strings.HasSuffix(path, "/commits"):
					_ = json.NewEncoder(w).Encode([]any{})
				case path == "/repos/fixture/repo/pulls/101" || path == "/repos/fixture/repo/pulls/103":
					_ = json.NewEncoder(w).Encode(map[string]any{"head": map[string]any{"sha": ""}})
				case path == "/graphql":
					_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{
						"repository": map[string]any{"pullRequest": map[string]any{"reviewThreads": map[string]any{
							"nodes": []any{}, "pageInfo": map[string]any{"hasNextPage": false, "endCursor": ""},
						}}},
					}})
				default:
					t.Errorf("unexpected request: %s", path)
					http.Error(w, "unexpected request", http.StatusBadRequest)
				}
			}))
			defer server.Close()
			t.Setenv("GITHUB_TOKEN", "test-token")
			t.Setenv("GITCRAWL_GITHUB_BASE_URL", server.URL)
			progressPath := filepath.Join(dir, "progress.json")
			args := []string{"--config", configPath, command, "fixture/repo", "--json"}
			if command == "sync" {
				args = append(args, "--numbers", "101,102,103", "--include-comments", "--progress-file", progressPath)
			} else {
				args = append(args, "--batch-size", "3", "--reserve-rate-limit", "10")
			}
			run := New()
			var stdout, stderr bytes.Buffer
			run.Stdout, run.Stderr = &stdout, &stderr
			err := run.Run(ctx, args)
			if err == nil || ExitCode(err) == 0 {
				t.Fatalf("incomplete acquisition reported success: %v", err)
			}
			if command == "sync" {
				var stats syncer.Stats
				if err := json.Unmarshal(stdout.Bytes(), &stats); err != nil {
					t.Fatalf("result=%s err=%v", stdout.String(), err)
				}
				if stats.ThreadsSynced != 2 || stats.ClosedSweepThrough != "" {
					t.Fatalf("committed stats=%+v", stats)
				}
				data, err := os.ReadFile(progressPath)
				if err != nil {
					t.Fatal(err)
				}
				var progress syncProgressSnapshot
				if err := json.Unmarshal(data, &progress); err != nil || progress.State != syncProgressFailed {
					t.Fatalf("terminal progress=%+v err=%v", progress, err)
				}
			} else {
				var result fillPRDetailsResult
				if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
					t.Fatalf("result=%s err=%v", stdout.String(), err)
				}
				if result.Filled != 2 || result.Remaining != 1 || result.Selected != 3 || result.StoppedReason != "sync-failed" ||
					len(result.Batches) != 1 || result.Batches[0].PRDetailsSynced != 2 {
					t.Fatalf("committed fill result=%+v err=%v stderr=%s", result, err, stderr.String())
				}
			}
		})
	}
}

func TestFillQuotaStopReturnsCommittedCountsWithoutPostBatchLookup(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	configPath, dbPath := filepath.Join(dir, "config.toml"), filepath.Join(dir, "archive.db")
	if err := New().Run(ctx, []string{"--config", configPath, "init", "--db", dbPath}); err != nil {
		t.Fatal(err)
	}
	configureTestGitHubCache(t, configPath, filepath.Join(dir, "cache"), "")
	st, err := store.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	repoID, err := st.UpsertRepository(ctx, store.Repository{
		Owner: "fixture", Name: "repo", FullName: "fixture/repo", RawJSON: "{}", UpdatedAt: "2026-04-01T00:00:00Z",
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, number := range []int{101, 102} {
		if _, err := st.UpsertThread(ctx, store.Thread{
			RepoID: repoID, GitHubID: strconv.Itoa(number), Number: number, Kind: "pull_request", State: "open",
			Title: "fixture", LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}",
			ContentHash: fmt.Sprint(number), UpdatedAt: "2026-04-01T00:00:00Z",
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	var completed, stopped atomic.Bool
	var authCalls, postStopRequests atomic.Int32
	resetAt := time.Now().Add(time.Hour).Truncate(time.Second)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if stopped.Load() {
			postStopRequests.Add(1)
			http.Error(w, "request after quota stop", http.StatusBadRequest)
			return
		}
		path := r.URL.Path
		switch {
		case path == "/rate_limit":
			remaining := 100
			if completed.Load() {
				stopped.Store(true)
				remaining = 10
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"resources": map[string]any{
				"core":    map[string]any{"limit": 5000, "remaining": remaining, "reset": resetAt.Unix()},
				"graphql": map[string]any{"limit": 5000, "remaining": remaining, "reset": resetAt.Unix()},
			}})
		case path == "/repos/fixture/repo":
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 123, "full_name": "fixture/repo"})
		case path == "/repos/fixture/repo/issues/101" || path == "/repos/fixture/repo/issues/102":
			number, _ := strconv.Atoi(path[strings.LastIndex(path, "/")+1:])
			_ = json.NewEncoder(w).Encode(githubIssueJSON(number, "pull_request", "fixture"))
		case path == "/graphql":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{
				"repository": map[string]any{"pullRequest": map[string]any{"reviewThreads": map[string]any{
					"nodes": []any{}, "pageInfo": map[string]any{"hasNextPage": false, "endCursor": ""},
				}}},
			}})
		case strings.HasSuffix(path, "/files"):
			_ = json.NewEncoder(w).Encode([]any{})
		case strings.HasSuffix(path, "/commits"):
			completed.Store(true)
			_ = json.NewEncoder(w).Encode([]any{})
		case path == "/repos/fixture/repo/pulls/101" || path == "/repos/fixture/repo/pulls/102":
			_ = json.NewEncoder(w).Encode(map[string]any{"head": map[string]any{"sha": ""}})
		default:
			t.Errorf("unexpected request: %s", path)
			http.Error(w, "unexpected request", http.StatusBadRequest)
		}
	}))
	defer server.Close()
	t.Setenv("GITHUB_TOKEN", "")
	t.Setenv("GITCRAWL_GITHUB_BASE_URL", server.URL)
	run := New()
	run.githubAuthTokenLookup = func(context.Context) (string, error) {
		authCalls.Add(1)
		return "fixture-token", nil
	}
	var stdout, stderr bytes.Buffer
	run.Stdout, run.Stderr = &stdout, &stderr
	err = run.Run(ctx, []string{"--config", configPath, "fill-pr-details", "fixture/repo", "--batch-size", "2", "--reserve-rate-limit", "10", "--json"})
	var reserveErr *gh.RateLimitReserveError
	if !errors.As(err, &reserveErr) || ExitCode(err) == 0 {
		t.Fatalf("quota error lost: %v", err)
	}
	var result fillPRDetailsResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("result=%s err=%v", stdout.String(), err)
	}
	if result.Filled != 1 || result.Remaining != 1 || result.StoppedReason != "rate-limit-reserve" ||
		len(result.Batches) != 1 || result.Batches[0].PRDetailsSynced != 1 {
		t.Fatalf("partial quota result=%+v", result)
	}
	wantRate := fillRateLimitResultFromSnapshot(reserveErr.RateLimit, 10)
	if result.RateLimit == nil || *result.RateLimit != wantRate ||
		result.Batches[0].RateLimit == nil || *result.Batches[0].RateLimit != wantRate {
		t.Fatalf("blocking quota snapshot lost: result=%+v batch=%+v want=%+v",
			result.RateLimit, result.Batches[0].RateLimit, wantRate)
	}
	if !stopped.Load() || postStopRequests.Load() != 0 || authCalls.Load() != 1 {
		t.Fatalf("post-stop work: stopped=%t requests=%d auth=%d", stopped.Load(), postStopRequests.Load(), authCalls.Load())
	}
}
