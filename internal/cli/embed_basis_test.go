package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/openclaw/gitcrawl/internal/config"
	"github.com/openclaw/gitcrawl/internal/store"
)

func TestEmbedValidatesBasisWithoutTasks(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DBPath = filepath.Join(dir, "archive.db")
	cfg.CacheDir = filepath.Join(dir, "cache")
	cfg.VectorDir = filepath.Join(dir, "vectors")
	cfg.LogDir = filepath.Join(dir, "logs")
	configPath := filepath.Join(dir, "config.toml")
	t.Setenv("GITCRAWL_DB_PATH", cfg.DBPath)
	t.Setenv("OPENAI_API_KEY", "test-openai-key")
	t.Setenv("GITCRAWL_OPENAI_RETRY_DISABLED", "1")

	st, err := store.Open(ctx, cfg.DBPath)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if _, err := st.UpsertRepository(ctx, store.Repository{
		Owner: "fixture", Name: "project", FullName: "fixture/project",
		RawJSON: "{}", UpdatedAt: "2026-01-01T00:00:00Z",
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		http.Error(w, "unexpected embedding request", http.StatusInternalServerError)
	}))
	defer server.Close()
	cfg.OpenAI.EmbedBaseURL = server.URL

	for _, tc := range []struct {
		basis       string
		unsupported bool
	}{
		{basis: "title_original"},
		{basis: "dedupe_text"},
		{basis: "llm_key_summary"},
		{basis: "title_summary", unsupported: true},
		{basis: "external_basis", unsupported: true},
	} {
		t.Run(tc.basis, func(t *testing.T) {
			cfg.EmbeddingBasis = tc.basis
			if err := config.Save(configPath, cfg); err != nil {
				t.Fatal(err)
			}
			app := New()
			var stdout bytes.Buffer
			app.Stdout = &stdout
			app.Stderr = io.Discard
			err := app.Run(ctx, []string{"--config", configPath, "--json", "embed", "fixture/project"})
			if tc.unsupported {
				if err == nil || !strings.Contains(err.Error(), "is not supported") || !strings.Contains(err.Error(), tc.basis) {
					t.Fatalf("unsupported basis error = %v", err)
				}
				if stdout.Len() != 0 {
					t.Fatalf("unsupported basis produced success output: %s", stdout.String())
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			var result embedResult
			if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
			if result.Repository != "fixture/project" || result.Basis != tc.basis || result.Selected != 0 || result.Embedded != 0 || result.Status != "success" {
				t.Fatalf("empty embedding result = %+v", result)
			}
		})
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("empty embedding made %d provider requests", got)
	}
}
