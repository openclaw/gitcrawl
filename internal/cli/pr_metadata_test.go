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

	"github.com/openclaw/gitcrawl/internal/store"
)

func TestPRMetadataSyncAndRefreshCommands(t *testing.T) {
	for _, command := range []string{"sync", "refresh"} {
		t.Run(command, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			configPath, dbPath := filepath.Join(dir, "config.toml"), filepath.Join(dir, "gitcrawl.db")
			if err := New().Run(ctx, []string{"--config", configPath, "init", "--db", dbPath}); err != nil {
				t.Fatal(err)
			}
			pulls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/repos/openclaw/gitcrawl":
					_ = json.NewEncoder(w).Encode(map[string]any{"id": 123})
				case "/repos/openclaw/gitcrawl/issues":
					_ = json.NewEncoder(w).Encode([]map[string]any{
						githubIssueJSON(1, "issue", "Issue"),
						githubIssueJSON(2, "pull_request", "Pull request"),
					})
				case "/repos/openclaw/gitcrawl/pulls/2":
					pulls++
					_ = json.NewEncoder(w).Encode(map[string]any{
						"number": 2, "merged_by": map[string]any{"login": "alice"}, "changed_files": 0,
					})
				default:
					t.Errorf("unexpected hydration request: %s", r.URL.Path)
					http.Error(w, "unexpected request", http.StatusBadRequest)
				}
			}))
			defer server.Close()
			t.Setenv("GITHUB_TOKEN", "test-gh-token")
			t.Setenv("GITCRAWL_GITHUB_BASE_URL", server.URL)
			app := New()
			var stdout bytes.Buffer
			app.Stdout = &stdout
			args := []string{"--config", configPath, command, "openclaw/gitcrawl", "--with", "pr-metadata", "--json"}
			if command == "refresh" {
				args = append(args, "--no-embed", "--no-cluster")
			}
			if err := app.Run(ctx, args); err != nil {
				t.Fatal(err)
			}
			if pulls != 1 || !strings.Contains(stdout.String(), `"pr_details_synced": 1`) {
				t.Fatalf("pull requests=%d output=%s", pulls, stdout.String())
			}
			st, err := store.Open(ctx, dbPath)
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			coverage, err := st.ArchiveCoverage(ctx, store.ArchiveCoverageOptions{})
			if err != nil || len(coverage.Rows) != 1 {
				t.Fatalf("coverage=%+v err=%v", coverage, err)
			}
			if coverage.Rows[0].Enrichment.PRFiles.Covered != 0 {
				t.Fatal("zero-file metadata was reported as hydrated files")
			}
		})
	}
}

func TestPRMetadataHelpDoesNotRequireAuthentication(t *testing.T) {
	t.Setenv("GITHUB_TOKEN", "")
	for _, command := range []string{"sync", "refresh"} {
		app := New()
		app.githubAuthTokenLookup = func(context.Context) (string, error) {
			t.Fatal("help requested GitHub credentials")
			return "", nil
		}
		var stdout bytes.Buffer
		app.Stdout = &stdout
		if err := app.Run(context.Background(), []string{"help", command}); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(stdout.String(), "pr-metadata") {
			t.Fatalf("%s help omits metadata hydration", command)
		}
	}
}
