package cli

import (
	"context"
	"encoding/json"
	"errors"
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

func TestAnalyticsEnrichIsolatesForbiddenActorAndTerminates(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	dir := t.TempDir()
	path := filepath.Join(dir, "source.db")
	st, err := store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if _, err = st.DB().Exec(`INSERT INTO analytics_pending_nodes(node_id,kind) VALUES('blocked','profile'),('healthy','profile')`); err != nil {
		t.Fatal(err)
	}
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/rate_limit" {
			fmt.Fprint(w, `{"resources":{"graphql":{"limit":20000,"remaining":19000,"reset":4102444800},"core":{"limit":20000,"remaining":19000,"reset":4102444800}}}`)
			return
		}
		requests.Add(1)
		var request struct {
			Variables struct {
				IDs []string `json:"ids"`
			} `json:"variables"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		nodes, failures := []any{}, []any{}
		for i, id := range request.Variables.IDs {
			if id == "blocked" {
				nodes = append(nodes, nil)
				failures = append(failures, map[string]any{"type": "FORBIDDEN", "path": []any{"nodes", i}, "message": "synthetic-private-actor-failure"})
			} else {
				nodes = append(nodes, map[string]any{"id": id, "__typename": "User", "login": "fixture"})
			}
		}
		json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"nodes": nodes}, "errors": failures})
	}))
	defer server.Close()
	t.Setenv("GITCRAWL_GITHUB_BASE_URL", server.URL)
	t.Setenv("GITHUB_TOKEN", "test-token-placeholder")
	app := New()
	app.Stdout, app.Stderr = io.Discard, io.Discard
	err = app.Run(ctx, []string{"--config", writeDoctorTestConfig(t, dir, path), "analytics", "fixture/repo", "--enrich"})
	if err == nil || errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "synthetic-private-actor-failure") {
		t.Errorf("expected bounded, safe incomplete-enrichment error, got %v", err)
	}
	var healthy, blocked int
	if err = st.DB().QueryRow(`SELECT count(*) FROM actor_profiles WHERE node_id='healthy'`).Scan(&healthy); err != nil {
		t.Fatal(err)
	}
	if err = st.DB().QueryRow(`SELECT count(*) FROM actor_profiles WHERE node_id='blocked'`).Scan(&blocked); err != nil {
		t.Fatal(err)
	}
	if healthy != 1 || blocked != 0 {
		t.Errorf("healthy=%d blocked=%d; forbidden evidence must stay unknown without blocking its peer", healthy, blocked)
	}
	if requests.Load() > 4 {
		t.Errorf("unbounded actor retry: %d", requests.Load())
	}
}
