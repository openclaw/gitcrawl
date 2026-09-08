package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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

func TestGitHubTokenCommandRootParsingAndHelp(t *testing.T) {
	for _, args := range [][]string{
		{"--github-token-command", "/unused/provider", "version"},
		{"--github-token-command=/unused/provider", "version"},
	} {
		app := New()
		var stdout bytes.Buffer
		app.Stdout = &stdout
		if err := app.Run(context.Background(), args); err != nil {
			t.Fatal(err)
		}
		if app.githubTokenCommand == nil || *app.githubTokenCommand != "/unused/provider" {
			t.Fatal("credential command not parsed")
		}
	}
	for _, args := range [][]string{
		{"--github-token-command", "/unused/provider", "--help"},
		{"--github-token-command=/unused/provider", "--help"},
	} {
		app := New()
		var stdout bytes.Buffer
		app.Stdout = &stdout
		if err := app.Run(context.Background(), args); err != nil || !strings.Contains(stdout.String(), "--github-token-command") {
			t.Fatalf("help err=%v output=%s", err, stdout.String())
		}
	}
	var parsed gitcrawlRootArgs
	if err := parseKongArgs(&parsed, []string{"--github-token-command=", "version"}, "gitcrawl", &bytes.Buffer{}, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if parsed.GitHubTokenCommand == nil || *parsed.GitHubTokenCommand != "" {
		t.Fatal("explicit empty selection became static authentication")
	}
}

func TestGitHubTokenCommandInspectionDoesNotResolveAmbientCredentials(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	if err := os.WriteFile(configPath, []byte("version = 1\ndb_path = "+strconv.Quote(filepath.Join(dir, "gitcrawl.db"))+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GITHUB_TOKEN", "ambient-must-not-win")
	app := New()
	app.githubAuthTokenLookup = func(context.Context) (string, error) {
		t.Fatal("managed metadata used gh fallback")
		return "", nil
	}
	var stdout bytes.Buffer
	app.Stdout = &stdout
	if err := app.Run(context.Background(), []string{"--config", configPath, "--github-token-command", "/does-not-exist", "doctor", "--json"}); err != nil {
		t.Fatal(err)
	}
	var result map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result["github_token_present"] != false || result["github_token_source"] != "github-token-command" {
		t.Fatalf("credential metadata=%v/%v", result["github_token_present"], result["github_token_source"])
	}
	if _, ok := app.sharedRateLimitState(context.Background()); ok {
		t.Fatal("unobserved managed quota reported available")
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
