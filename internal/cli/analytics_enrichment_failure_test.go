package cli

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	gh "github.com/openclaw/gitcrawl/internal/github"
	"github.com/openclaw/gitcrawl/internal/store"
)

type cancelEnrichmentRetry struct{ cancel context.CancelFunc }

func (w cancelEnrichmentRetry) Write(p []byte) (int, error) {
	if strings.Contains(string(p), "actor_enrichment_retry") {
		w.cancel()
	}
	return len(p), nil
}
func TestAnalyticsOneShotEnrichmentReturnsSafeFailure(t *testing.T) {
	for _, once := range []bool{false, true} {
		t.Run(map[bool]string{false: "enrich", true: "enrich_once"}[once], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			dir := t.TempDir()
			path := filepath.Join(dir, "source.db")
			st, err := store.Open(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = st.DB().Exec("INSERT INTO analytics_pending_nodes(node_id,kind) VALUES('fixture-actor','profile')"); err != nil {
				t.Fatal(err)
			}
			if err = st.Close(); err != nil {
				t.Fatal(err)
			}
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				http.Error(w, "private-provider-body", http.StatusUnauthorized)
			}))
			defer server.Close()
			t.Setenv("GITCRAWL_GITHUB_BASE_URL", server.URL)
			t.Setenv("GITHUB_TOKEN", "fixture-token")
			app := New()
			app.Stderr = cancelEnrichmentRetry{cancel}
			args := []string{"--config", writeDoctorTestConfig(t, dir, path), "analytics", "fixture/repo", "--enrich"}
			if once {
				args = append(args, "--once")
			}
			err = app.Run(ctx, args)
			var response *gh.RequestError
			if !errors.As(err, &response) || response.Status != 401 {
				t.Fatalf("one-shot retried instead of returning provider failure: %v", err)
			}
			if strings.Contains(err.Error(), "private-provider-body") {
				t.Fatal("private provider body escaped")
			}
			if requests.Load() != 1 {
				t.Fatalf("one-shot dispatched %d requests", requests.Load())
			}
		})
	}
}
