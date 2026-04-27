package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSaveLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	cfg := Default()
	cfg.DBPath = filepath.Join(dir, "gitcrawl.db")
	cfg.OpenAI.SummaryModel = "gpt-5-mini"

	if err := Save(path, cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}
	loaded, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if loaded.DBPath != cfg.DBPath {
		t.Fatalf("db path mismatch: got %q want %q", loaded.DBPath, cfg.DBPath)
	}
	if loaded.OpenAI.SummaryModel != "gpt-5-mini" {
		t.Fatalf("summary model mismatch: %q", loaded.OpenAI.SummaryModel)
	}
}

func TestResolvePathUsesEnv(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "custom.toml")
	t.Setenv(DefaultConfigEnv, path)

	if got := ResolvePath(""); got != path {
		t.Fatalf("resolve path: got %q want %q", got, path)
	}
}

func TestNormalizeUsesDBEnv(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "override.db")
	t.Setenv("GITCRAWL_DB_PATH", dbPath)

	cfg := Default()
	cfg.DBPath = ""
	if err := cfg.Normalize(); err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if cfg.DBPath != dbPath {
		t.Fatalf("db path: got %q want %q", cfg.DBPath, dbPath)
	}
}

func TestResolveTokens(t *testing.T) {
	t.Setenv("GITHUB_TOKEN", "ghp_test")
	t.Setenv("OPENAI_API_KEY", "sk_test")

	cfg := Default()
	if got := ResolveGitHubToken(cfg); got.Value != "ghp_test" || got.Source != "GITHUB_TOKEN" {
		t.Fatalf("github token resolution mismatch: %#v", got)
	}
	if got := ResolveOpenAIKey(cfg); got.Value != "sk_test" || got.Source != "OPENAI_API_KEY" {
		t.Fatalf("openai key resolution mismatch: %#v", got)
	}
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
