package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

func TestSyncAndRefreshForceCommentDownload(t *testing.T) {
	for _, command := range []string{"sync", "refresh"} {
		t.Run(command, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			configPath, dbPath := filepath.Join(dir, "config.toml"), filepath.Join(dir, "archive.db")
			if err := New().Run(ctx, []string{"--config", configPath, "init", "--db", dbPath}); err != nil {
				t.Fatal(err)
			}
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/repos/fixture/repo":
					_ = json.NewEncoder(w).Encode(map[string]any{"id": 123})
				case "/repos/fixture/repo/issues":
					row := githubIssueJSON(1, "issue", "Discussion")
					row["comments"] = 1
					_ = json.NewEncoder(w).Encode([]map[string]any{row})
				case "/repos/fixture/repo/issues/1/comments":
					calls++
					_ = json.NewEncoder(w).Encode([]map[string]any{{"id": 11, "body": "saved discussion"}})
				default:
					t.Errorf("unexpected request: %s", r.URL.Path)
					http.Error(w, "unexpected", http.StatusBadRequest)
				}
			}))
			defer server.Close()
			t.Setenv("GITHUB_TOKEN", "test-gh-token")
			t.Setenv("GITCRAWL_GITHUB_BASE_URL", server.URL)
			args := []string{"--config", configPath, command, "fixture/repo", "--include-comments", "--limit", "1", "--json"}
			if command == "refresh" {
				args = append(args, "--no-embed", "--no-cluster")
			}
			for run, wantCalls := range []int{1, 1, 2} {
				var stdout bytes.Buffer
				app := New()
				app.Stdout = &stdout
				if run == 2 {
					args = append(args, "--force")
				}
				if err := app.Run(ctx, args); err != nil {
					t.Fatal(err)
				}
				if calls != wantCalls {
					t.Fatalf("run=%d calls=%d want=%d", run, calls, wantCalls)
				}
				if run == 1 && !strings.Contains(stdout.String(), `"comments_synced": 0`) {
					t.Fatalf("reused comments counted as downloaded: %s", stdout.String())
				}
			}
		})
	}
}
