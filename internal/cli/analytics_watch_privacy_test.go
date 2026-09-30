package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/openclaw/gitcrawl/internal/store"
)

type analyticsCancelLog struct {
	bytes.Buffer
	cancel context.CancelFunc
}

func (w *analyticsCancelLog) Write(p []byte) (int, error) {
	n, err := w.Buffer.Write(p)
	if bytes.Contains(p, []byte(`"event":"github_update_failed"`)) {
		w.cancel()
	}
	return n, err
}

func TestAnalyticsWatchRedactsProviderFailures(t *testing.T) {
	for _, mode := range []string{"--watch", "--once"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/rate_limit" {
					fmt.Fprint(w, `{"resources":{"graphql":{"limit":20000,"remaining":19000,"reset":4102444800},"core":{"limit":20000,"remaining":19000,"reset":4102444800}}}`)
					return
				}
				fmt.Fprint(w, `{"errors":[{"type":"FORBIDDEN","message":"synthetic-private-provider-message"}]}`)
			}))
			defer server.Close()
			t.Setenv("GITCRAWL_GITHUB_BASE_URL", server.URL)
			t.Setenv("GITHUB_TOKEN", "test-token-placeholder")
			app := New()
			log := &analyticsCancelLog{cancel: cancel}
			app.Stdout, app.Stderr = io.Discard, log
			err = app.Run(ctx, []string{"--config", writeDoctorTestConfig(t, dir, path), "analytics", "fixture/repo", mode})
			if mode == "--watch" && !errors.Is(err, context.Canceled) {
				t.Fatalf("watch error: %v", err)
			}
			if bytes.Contains(log.Bytes(), []byte("synthetic-private-provider-message")) {
				t.Fatal("watch exposed provider prose")
			}
			if mode == "--once" && (err == nil || strings.Contains(err.Error(), "synthetic-private-provider-message")) {
				t.Fatalf("one-shot returned unsafe failure: %v", err)
			}
			if mode == "--watch" && !bytes.Contains(log.Bytes(), []byte(`"error_class"`)) {
				t.Fatalf("missing safe failure classification: %s", log.String())
			}
		})
	}
}

func TestAnalyticsIncompleteErrorsPreserveIdentityWithoutJoinedProse(t *testing.T) {
	cause := errors.Join(errAnalyticsIncomplete, fmt.Errorf("synthetic-private-joined-failure"))
	wrapped := &analyticsCommandError{cause: cause}
	if !errors.Is(wrapped, errAnalyticsIncomplete) || strings.Contains(wrapped.Error(), "synthetic-private") {
		t.Fatal("unsafe or unidentifiable incomplete error")
	}
	var log bytes.Buffer
	app := New()
	app.Stderr = &log
	app.analyticsUpdateLog(cause)
	if strings.Contains(log.String(), "synthetic-private") {
		t.Fatal("incomplete log exposed joined provider prose")
	}
}
