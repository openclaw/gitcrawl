package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	crawlconfig "github.com/vincentkoc/crawlkit/config"
)

func TestSaveLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	cfg := Default()
	cfg.DBPath = filepath.Join(dir, "gitcrawl.db")
	cfg.OpenAI.SummaryModel = "gpt-5-mini"
	cfg.Env = map[string]string{
		"GITHUB_TOKEN": "config-gh",
	}

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
	if loaded.Env["GITHUB_TOKEN"] != "config-gh" {
		t.Fatalf("env table mismatch: %#v", loaded.Env)
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

func TestApplyRuntimeEnvUsesDBEnv(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "override.db")
	t.Setenv("GITCRAWL_DB_PATH", dbPath)

	cfg := Default()
	cfg.DBPath = ""
	if err := cfg.Normalize(); err != nil {
		t.Fatalf("normalize: %v", err)
	}
	cfg.ApplyRuntimeEnv()
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

	t.Setenv("GITHUB_TOKEN", "")
	t.Setenv("OPENAI_API_KEY", "")
	cfg.GitHub.TokenEnv = "CUSTOM_GITHUB_TOKEN"
	cfg.OpenAI.APIKeyEnv = "CUSTOM_OPENAI_KEY"
	t.Setenv("CUSTOM_GITHUB_TOKEN", "custom-gh")
	t.Setenv("CUSTOM_OPENAI_KEY", "custom-openai")
	if got := ResolveGitHubToken(cfg); got.Value != "custom-gh" || got.Source != "CUSTOM_GITHUB_TOKEN" {
		t.Fatalf("custom github token mismatch: %#v", got)
	}
	if got := ResolveOpenAIKey(cfg); got.Value != "custom-openai" || got.Source != "CUSTOM_OPENAI_KEY" {
		t.Fatalf("custom openai key mismatch: %#v", got)
	}

	t.Setenv("CUSTOM_GITHUB_TOKEN", "")
	t.Setenv("CUSTOM_OPENAI_KEY", "")
	if got := ResolveGitHubToken(cfg); got.Value != "" || got.Source != "" {
		t.Fatalf("empty github token mismatch: %#v", got)
	}
	if got := ResolveOpenAIKey(cfg); got.Value != "" || got.Source != "" {
		t.Fatalf("empty openai key mismatch: %#v", got)
	}
}

func TestResolveTokensFromConfigEnv(t *testing.T) {
	t.Setenv("GITHUB_TOKEN", "")
	t.Setenv("OPENAI_API_KEY", "")

	cfg := Default()
	cfg.Env = map[string]string{
		"GITHUB_TOKEN":   "config-gh",
		"OPENAI_API_KEY": "config-openai",
	}
	if got := ResolveGitHubToken(cfg); got.Value != "config-gh" || got.Source != "config.toml [env].GITHUB_TOKEN" {
		t.Fatalf("github token config env mismatch: %#v", got)
	}
	if got := ResolveOpenAIKey(cfg); got.Value != "config-openai" || got.Source != "config.toml [env].OPENAI_API_KEY" {
		t.Fatalf("openai key config env mismatch: %#v", got)
	}

	t.Setenv("GITHUB_TOKEN", "process-gh")
	if got := ResolveGitHubToken(cfg); got.Value != "process-gh" || got.Source != "GITHUB_TOKEN" {
		t.Fatalf("process env should win: %#v", got)
	}
}

func TestResolveTokensFromCustomConfigEnv(t *testing.T) {
	t.Setenv("CUSTOM_GITHUB_TOKEN", "")
	t.Setenv("CUSTOM_OPENAI_KEY", "")

	cfg := Default()
	cfg.GitHub.TokenEnv = "CUSTOM_GITHUB_TOKEN"
	cfg.OpenAI.APIKeyEnv = "CUSTOM_OPENAI_KEY"
	cfg.Env = map[string]string{
		"CUSTOM_GITHUB_TOKEN": "config-custom-gh",
		"CUSTOM_OPENAI_KEY":   "config-custom-openai",
		"GITHUB_TOKEN":        "ignored-default-gh",
		"OPENAI_API_KEY":      "ignored-default-openai",
	}
	if got := ResolveGitHubToken(cfg); got.Value != "config-custom-gh" || got.Source != "config.toml [env].CUSTOM_GITHUB_TOKEN" {
		t.Fatalf("custom github token config env mismatch: %#v", got)
	}
	if got := ResolveOpenAIKey(cfg); got.Value != "config-custom-openai" || got.Source != "config.toml [env].CUSTOM_OPENAI_KEY" {
		t.Fatalf("custom openai key config env mismatch: %#v", got)
	}

	cfg.Env["CUSTOM_GITHUB_TOKEN"] = "   "
	if got := ResolveGitHubToken(cfg); got.Value != "" || got.Source != "" {
		t.Fatalf("empty config env should be ignored: %#v", got)
	}
}

func TestApplyRuntimeEnvUsesConfigEnvFallback(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "config-env.db")
	t.Setenv("GITCRAWL_DB_PATH", "")
	t.Setenv("GITCRAWL_SUMMARY_MODEL", "")
	t.Setenv("GITCRAWL_EMBED_MODEL", "")

	cfg := Config{
		Env: map[string]string{
			"GITCRAWL_DB_PATH":       dbPath,
			"GITCRAWL_SUMMARY_MODEL": "summary-config",
			"GITCRAWL_EMBED_MODEL":   "embed-config",
		},
	}
	if err := cfg.Normalize(); err != nil {
		t.Fatalf("normalize: %v", err)
	}
	cfg.ApplyRuntimeEnv()
	if cfg.DBPath != dbPath {
		t.Fatalf("db path: got %q want %q", cfg.DBPath, dbPath)
	}
	if cfg.OpenAI.SummaryModel != "summary-config" || cfg.OpenAI.EmbedModel != "embed-config" {
		t.Fatalf("config env models not used: %+v", cfg.OpenAI)
	}
}

func TestLoadRuntimeUsesConfigEnvModelOverrides(t *testing.T) {
	t.Setenv("GITCRAWL_SUMMARY_MODEL", "")
	t.Setenv("GITCRAWL_EMBED_MODEL", "")

	path := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(path, []byte(`
[env]
GITCRAWL_SUMMARY_MODEL = "summary-from-config-env"
GITCRAWL_EMBED_MODEL = "embed-from-config-env"
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadRuntime(path)
	if err != nil {
		t.Fatalf("load runtime config: %v", err)
	}
	if cfg.OpenAI.SummaryModel != "summary-from-config-env" || cfg.OpenAI.EmbedModel != "embed-from-config-env" {
		t.Fatalf("load skipped config env model overrides: %+v", cfg.OpenAI)
	}
}

func TestLoadDoesNotApplyRuntimeEnvFallback(t *testing.T) {
	t.Setenv("GITCRAWL_SUMMARY_MODEL", "summary-from-process")
	t.Setenv("GITCRAWL_EMBED_MODEL", "")

	path := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(path, []byte(`
[env]
GITCRAWL_SUMMARY_MODEL = "summary-from-config-env"
GITCRAWL_EMBED_MODEL = "embed-from-config-env"
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.OpenAI.SummaryModel != Default().OpenAI.SummaryModel || cfg.OpenAI.EmbedModel != Default().OpenAI.EmbedModel {
		t.Fatalf("load should not apply runtime fallback: %+v", cfg.OpenAI)
	}
}

func TestSaveDoesNotApplyConfigEnvFallback(t *testing.T) {
	t.Setenv("GITCRAWL_SUMMARY_MODEL", "process-summary")
	t.Setenv("GITCRAWL_EMBED_MODEL", "")

	path := filepath.Join(t.TempDir(), "config.toml")
	cfg := Default()
	cfg.OpenAI.SummaryModel = "explicit-summary"
	cfg.OpenAI.EmbedModel = "explicit-embed"
	cfg.Env = map[string]string{
		"GITCRAWL_SUMMARY_MODEL": "config-summary",
		"GITCRAWL_EMBED_MODEL":   "config-embed",
	}

	if err := Save(path, cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}

	var raw Config
	if err := crawlconfig.LoadTOML(path, &raw); err != nil {
		t.Fatalf("load raw config: %v", err)
	}
	if raw.OpenAI.SummaryModel != "explicit-summary" {
		t.Fatalf("save applied process env fallback: %q", raw.OpenAI.SummaryModel)
	}
	if raw.OpenAI.EmbedModel != "explicit-embed" {
		t.Fatalf("save applied config env fallback: %q", raw.OpenAI.EmbedModel)
	}
}

func TestLoadThenSaveDoesNotMaterializeRuntimeEnvFallback(t *testing.T) {
	t.Setenv("GITCRAWL_SUMMARY_MODEL", "process-summary")
	t.Setenv("GITCRAWL_EMBED_MODEL", "")

	path := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(path, []byte(`
[env]
GITCRAWL_SUMMARY_MODEL = "config-summary"
GITCRAWL_EMBED_MODEL = "config-embed"
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	cfg.EmbeddingBasis = "title_original"
	if err := Save(path, cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}

	var raw Config
	if err := crawlconfig.LoadTOML(path, &raw); err != nil {
		t.Fatalf("load raw config: %v", err)
	}
	if raw.OpenAI.SummaryModel == "process-summary" || raw.OpenAI.SummaryModel == "config-summary" {
		t.Fatalf("load/save materialized summary fallback: %q", raw.OpenAI.SummaryModel)
	}
	if raw.OpenAI.EmbedModel == "config-embed" {
		t.Fatalf("load/save materialized embed fallback: %q", raw.OpenAI.EmbedModel)
	}
}

func TestNormalizeDefaultsAndRuntimeDirs(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)
	t.Setenv("GITCRAWL_SUMMARY_MODEL", "summary-env")
	t.Setenv("GITCRAWL_EMBED_MODEL", "embed-env")
	cfg := Config{
		DBPath:    "~/gitcrawl/test.db",
		CacheDir:  "~/gitcrawl/cache",
		VectorDir: "~/gitcrawl/vectors",
		LogDir:    "~/gitcrawl/logs",
	}
	if err := cfg.Normalize(); err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if cfg.Version != 1 || cfg.GitHub.TokenEnv == "" || cfg.OpenAI.APIKeyEnv == "" {
		t.Fatalf("defaults not filled: %+v", cfg)
	}
	if cfg.OpenAI.SummaryModel != Default().OpenAI.SummaryModel || cfg.OpenAI.EmbedModel != Default().OpenAI.EmbedModel {
		t.Fatalf("normalize should not apply runtime env: %+v", cfg.OpenAI)
	}
	if !filepath.IsAbs(cfg.DBPath) || !strings.Contains(cfg.DBPath, dir) {
		t.Fatalf("home path not expanded: %s", cfg.DBPath)
	}
	if err := EnsureRuntimeDirs(cfg); err != nil {
		t.Fatalf("ensure dirs: %v", err)
	}
	for _, path := range []string{cfg.CacheDir, cfg.VectorDir, cfg.LogDir, filepath.Dir(cfg.DBPath)} {
		if info, err := os.Stat(path); err != nil || !info.IsDir() {
			t.Fatalf("runtime dir %s missing: %v", path, err)
		}
	}
}

func TestLoadRejectsInvalidConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.toml")
	if err := os.WriteFile(path, []byte("version = ["), 0o600); err != nil {
		t.Fatalf("write bad config: %v", err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("invalid config should fail")
	}
}

func TestResolvePathAndSaveErrorBranches(t *testing.T) {
	t.Setenv(DefaultConfigEnv, "")
	if got := ResolvePath(""); !strings.HasSuffix(got, filepath.Join(".config", "gitcrawl", "config.toml")) {
		t.Fatalf("default config path = %q", got)
	}
	if got := ResolvePath("~/custom.toml"); !strings.Contains(got, "custom.toml") {
		t.Fatalf("home config path = %q", got)
	}
	dir := t.TempDir()
	if err := Save(dir, Default()); err == nil {
		t.Fatal("saving to directory should fail")
	}
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
