//go:build !windows

package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/openclaw/gitcrawl/internal/config"
)

func TestAnalyticsCredentialRotationAndFailureAtDispatch(t *testing.T) {
	ctx := context.Background()
	credentialPath := filepath.Join(t.TempDir(), "credential")
	t.Setenv("ANALYTICS_TEST_CREDENTIAL", credentialPath)
	helper := tokenCommandFixture(t, `cat "$ANALYTICS_TEST_CREDENTIAL"`)
	writeCredential := func(value string) {
		t.Helper()
		if err := os.WriteFile(credentialPath, []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
	}
	var mu sync.Mutex
	var received []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		received = append(received, r.URL.Path+":"+r.Header.Get("Authorization"))
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/rate_limit" {
			fmt.Fprintf(w, `{"resources":{"graphql":{"limit":5000,"remaining":4900,"reset":%d}}}`, time.Now().Add(time.Hour).Unix())
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"nodes": []any{map[string]any{"id": "actor", "login": "fixture", "__typename": "User"}}}})
	}))
	defer server.Close()
	t.Setenv("GITCRAWL_GITHUB_BASE_URL", server.URL)
	a := New()
	dir := t.TempDir()
	a.configPath = writeDoctorTestConfig(t, dir, filepath.Join(dir, "archive.db"))
	a.githubTokenCommand = &helper
	client, err := a.analyticsClient(ctx, config.Config{})
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"fixture-first", "fixture-rotated"} {
		writeCredential(value)
		if _, err := client.AnalyticsNodes(ctx, []string{"actor"}, true); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Remove(credentialPath); err != nil {
		t.Fatal(err)
	}
	if _, err := client.AnalyticsNodes(ctx, []string{"actor"}, true); err == nil {
		t.Fatal("failed helper authorized a request")
	}
	mu.Lock()
	defer mu.Unlock()
	want := []string{
		"/rate_limit:Bearer fixture-first", "/graphql:Bearer fixture-first",
		"/rate_limit:Bearer fixture-rotated", "/graphql:Bearer fixture-rotated",
	}
	if strings.Join(received, "\n") != strings.Join(want, "\n") {
		t.Fatalf("unexpected final request trace: %v", received)
	}
}
