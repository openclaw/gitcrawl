package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	gh "github.com/openclaw/gitcrawl/internal/github"
	"github.com/openclaw/gitcrawl/internal/store"
)

type analyticsLogWriter struct {
	mu sync.Mutex
	bytes.Buffer
	cancel context.CancelFunc
}

func (w *analyticsLogWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n, err := w.Buffer.Write(p)
	if bytes.Contains(p, []byte(`"event":"github_update_failed"`)) {
		w.cancel()
	}
	return n, err
}

func TestAnalyticsDirectWatchAndOnceDoNotRenderRejectedBodies(t *testing.T) {
	for _, mode := range []string{"--watch", "--once"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			dir := t.TempDir()
			path := filepath.Join(dir, "source.db")
			st, err := store.Open(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			if err = st.SaveAnalyticsCoverage(ctx, "fixture/repo", "2026-01-01T00:00:00Z", 1, 1); err != nil {
				t.Fatal(err)
			}
			if err = st.Close(); err != nil {
				t.Fatal(err)
			}
			const sentinel = "synthetic-private-provider-body"
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, sentinel, http.StatusUnauthorized) }))
			defer server.Close()
			t.Setenv("GITCRAWL_GITHUB_BASE_URL", server.URL)
			t.Setenv("GITHUB_TOKEN", "fixture-token")
			app := New()
			log := &analyticsLogWriter{cancel: cancel}
			app.Stdout, app.Stderr = io.Discard, log
			err = app.Run(ctx, []string{"--config", writeDoctorTestConfig(t, dir, path), "analytics", "fixture/repo", mode})
			if err == nil {
				t.Fatal("failed acquisition reported success")
			}
			if strings.Contains(err.Error(), sentinel) || strings.Contains(log.String(), sentinel) {
				t.Fatal("provider body escaped through command error/log")
			}
			if !strings.Contains(log.String(), `"event":"github_update_failed"`) {
				t.Fatal("missing failure event")
			}
		})
	}
}

func TestAnalyticsJoinedIncompleteErrorIsSafe(t *testing.T) {
	err := errors.Join(errAnalyticsIncomplete, &gh.RequestError{Status: 401, Body: "private-provider-body"})
	var out bytes.Buffer
	app := New()
	app.Stderr = &out
	app.analyticsUpdateLog(err)
	var event map[string]any
	if e := json.Unmarshal(out.Bytes(), &event); e != nil {
		t.Fatal(e)
	}
	if event["event"] != "github_coverage_pending" || strings.Contains(out.String(), "private-provider-body") {
		t.Fatalf("unsafe joined failure: %s", out.String())
	}
	safe := safeAnalyticsError{err}
	if !errors.Is(safe, errAnalyticsIncomplete) || strings.Contains(safe.Error(), "private-provider-body") {
		t.Fatal("unsafe failure wrapper")
	}
}
