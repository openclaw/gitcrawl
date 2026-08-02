package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/openclaw/gitcrawl/internal/config"
)

func TestResolveGitHubTokenFallsBackToGHAuthToken(t *testing.T) {
	t.Setenv("GITHUB_TOKEN", "")

	app := New()
	app.githubAuthTokenLookup = func(context.Context) (string, error) {
		return "gh-fallback-token", nil
	}
	token := app.resolveGitHubToken(context.Background(), config.Default())
	if token.Value != "gh-fallback-token" || token.Source != "gh auth token" {
		t.Fatalf("token mismatch: source=%q value_present=%t value_length=%d", token.Source, token.Value != "", len(token.Value))
	}
}

func TestDoctorReportsGHAuthTokenFallback(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "gitcrawl.db")
	if err := os.WriteFile(configPath, []byte("version = 1\ndb_path = "+strconv.Quote(dbPath)+"\n[github]\ntoken_env = 'GITHUB_TOKEN'\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	t.Setenv("GITHUB_TOKEN", "")

	doctor := New()
	doctor.githubAuthTokenLookup = func(context.Context) (string, error) {
		return "gh-fallback-token", nil
	}
	var stdout bytes.Buffer
	doctor.Stdout = &stdout
	if err := doctor.Run(context.Background(), []string{"--config", configPath, "doctor", "--json"}); err != nil {
		t.Fatalf("doctor: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
		t.Fatalf("parse doctor json: %v (output_bytes=%d)", err, stdout.Len())
	}
	if got := payload["github_token_present"]; got != true {
		t.Fatalf("github_token_present = %#v", got)
	}
	if got := payload["github_token_source"]; got != "gh auth token" {
		t.Fatalf("github_token_source = %#v", got)
	}
}
