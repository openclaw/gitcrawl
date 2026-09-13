package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

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
