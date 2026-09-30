package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/openclaw/gitcrawl/internal/store"
)

func TestAnalyticsAcceptsEmptyHistoricalBaseline(t *testing.T) {
	for _, tc := range []struct {
		repository string
		reject     bool
	}{
		{"fixture/repo", false}, {"FIXTURE/REPO", false}, {"other/repo", true}, {"", true},
	} {
		t.Run(tc.repository, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			path := filepath.Join(dir, "source.db")
			st, err := store.Open(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			cfg := writeDoctorTestConfig(t, dir, path)
			receipt := fmt.Sprintf(`{"repository":%q,"phase":"complete","discovery":[{"kind":"issues","done":1,"total":0,"updated_at":"2026-01-01T00:00:00Z"},{"kind":"pullRequests","done":1,"total":0,"updated_at":"2026-01-01T00:00:00Z"}]}`, tc.repository)
			if err := os.WriteFile(filepath.Join(dir, "status.json"), []byte(receipt), 0600); err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/rate_limit" {
					fmt.Fprint(w, `{"resources":{"graphql":{"limit":20000,"remaining":19000,"reset":4102444800},"core":{"limit":20000,"remaining":19000,"reset":4102444800}}}`)
					return
				}
				conn := map[string]any{"totalCount": 0, "nodes": []any{}, "pageInfo": map[string]any{"hasNextPage": false, "endCursor": nil}}
				json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"rateLimit": map[string]any{"cost": 1, "remaining": 19000, "resetAt": "2099-01-01T00:00:00Z"}, "repository": map[string]any{"issues": conn, "pullRequests": conn}}})
			}))
			defer server.Close()
			t.Setenv("GITCRAWL_GITHUB_BASE_URL", server.URL)
			t.Setenv("GITHUB_TOKEN", "test-token-placeholder")
			app := New()
			app.Stdout, app.Stderr = io.Discard, io.Discard
			err = app.Run(ctx, []string{"--config", cfg, "analytics", "fixture/repo", "--once"})
			if tc.reject {
				if err == nil {
					t.Fatal("unbound historical receipt established coverage")
				}
				var count int
				if err := st.DB().QueryRow(`SELECT count(*) FROM analytics_coverage WHERE repository='fixture/repo'`).Scan(&count); err != nil || count != 0 {
					t.Fatalf("unbound baseline persisted: count=%d err=%v", count, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}

			var complete, issues, prs int
			if err := st.DB().QueryRow(`SELECT complete,issues,pull_requests FROM analytics_coverage WHERE repository='fixture/repo'`).Scan(&complete, &issues, &prs); err != nil || complete != 1 || issues != 0 || prs != 0 {
				t.Fatalf("empty baseline coverage: %d %d %d %v", complete, issues, prs, err)
			}
		})
	}
}
