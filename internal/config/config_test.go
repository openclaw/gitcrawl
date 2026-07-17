package config

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	crawlconfig "github.com/openclaw/crawlkit/config"
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

func TestDefaultPathsUseXDGDirs(t *testing.T) {
	home := t.TempDir()
	configHome := filepath.Join(home, "xdg-config")
	dataHome := filepath.Join(home, "xdg-data")
	cacheHome := filepath.Join(home, "xdg-cache")
	stateHome := filepath.Join(home, "xdg-state")
	setTestHome(t, home)
	t.Setenv("XDG_CONFIG_HOME", configHome)
	t.Setenv("XDG_DATA_HOME", dataHome)
	t.Setenv("XDG_CACHE_HOME", cacheHome)
	t.Setenv("XDG_STATE_HOME", stateHome)

	cfg := Default()
	if cfg.DBPath != filepath.Join(dataHome, "gitcrawl", "gitcrawl.db") {
		t.Fatalf("db path = %q", cfg.DBPath)
	}
	if cfg.CacheDir != filepath.Join(cacheHome, "gitcrawl") {
		t.Fatalf("cache dir = %q", cfg.CacheDir)
	}
	if cfg.VectorDir != filepath.Join(dataHome, "gitcrawl", "vectors") {
		t.Fatalf("vector dir = %q", cfg.VectorDir)
	}
	if cfg.LogDir != filepath.Join(stateHome, "gitcrawl", "logs") {
		t.Fatalf("log dir = %q", cfg.LogDir)
	}
	if got := ResolvePath(""); got != filepath.Join(configHome, "gitcrawl", "config.toml") {
		t.Fatalf("config path = %q", got)
	}
}

func TestDefaultPathsUsePlatformFallbacks(t *testing.T) {
	home := t.TempDir()
	configHome, dataHome, cacheHome, stateHome := defaultPlatformTestDirs(home)
	setTestHome(t, home)
	clearXDGEnv(t)

	cfg := Default()
	if cfg.DBPath != filepath.Join(dataHome, "gitcrawl", "gitcrawl.db") {
		t.Fatalf("db path = %q", cfg.DBPath)
	}
	if cfg.CacheDir != filepath.Join(cacheHome, "gitcrawl") {
		t.Fatalf("cache dir = %q", cfg.CacheDir)
	}
	if cfg.VectorDir != filepath.Join(dataHome, "gitcrawl", "vectors") {
		t.Fatalf("vector dir = %q", cfg.VectorDir)
	}
	if cfg.LogDir != filepath.Join(stateHome, "gitcrawl", "logs") {
		t.Fatalf("log dir = %q", cfg.LogDir)
	}
	if got := ResolvePath(""); got != filepath.Join(configHome, "gitcrawl", "config.toml") {
		t.Fatalf("config path = %q", got)
	}
}

func TestDefaultPathsPreferExistingLegacyInstallPaths(t *testing.T) {
	home := t.TempDir()
	legacy := filepath.Join(home, ".config", "gitcrawl")
	if err := os.MkdirAll(filepath.Join(legacy, "cache"), 0o755); err != nil {
		t.Fatalf("mkdir cache: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(legacy, "logs"), 0o755); err != nil {
		t.Fatalf("mkdir logs: %v", err)
	}
	if err := os.WriteFile(filepath.Join(legacy, "gitcrawl.db"), nil, 0o600); err != nil {
		t.Fatalf("write db: %v", err)
	}
	if err := os.WriteFile(filepath.Join(legacy, "config.toml"), nil, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	setTestHome(t, home)
	clearXDGEnv(t)

	cfg := Default()
	if cfg.DBPath != filepath.Join(legacy, "gitcrawl.db") {
		t.Fatalf("db path = %q", cfg.DBPath)
	}
	if cfg.CacheDir != filepath.Join(legacy, "cache") {
		t.Fatalf("cache dir = %q", cfg.CacheDir)
	}
	if cfg.VectorDir != filepath.Join(legacy, "vectors") {
		t.Fatalf("vector dir = %q", cfg.VectorDir)
	}
	if cfg.LogDir != filepath.Join(legacy, "logs") {
		t.Fatalf("log dir = %q", cfg.LogDir)
	}
	if got := ResolvePath(""); got != filepath.Join(legacy, "config.toml") {
		t.Fatalf("config path = %q", got)
	}
}

func TestDefaultPathsKeepLegacyInstallWithXDGEnv(t *testing.T) {
	home := t.TempDir()
	legacy := filepath.Join(home, ".config", "gitcrawl")
	configHome := filepath.Join(home, "xdg-config")
	dataHome := filepath.Join(home, "xdg-data")
	cacheHome := filepath.Join(home, "xdg-cache")
	stateHome := filepath.Join(home, "xdg-state")
	if err := os.MkdirAll(filepath.Join(legacy, "cache"), 0o755); err != nil {
		t.Fatalf("mkdir cache: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(legacy, "logs"), 0o755); err != nil {
		t.Fatalf("mkdir logs: %v", err)
	}
	if err := os.WriteFile(filepath.Join(legacy, "gitcrawl.db"), nil, 0o600); err != nil {
		t.Fatalf("write db: %v", err)
	}
	if err := os.WriteFile(filepath.Join(legacy, "config.toml"), nil, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	setTestHome(t, home)
	t.Setenv("XDG_CONFIG_HOME", configHome)
	t.Setenv("XDG_DATA_HOME", dataHome)
	t.Setenv("XDG_CACHE_HOME", cacheHome)
	t.Setenv("XDG_STATE_HOME", stateHome)

	cfg := Default()
	if cfg.DBPath != filepath.Join(legacy, "gitcrawl.db") {
		t.Fatalf("db path = %q", cfg.DBPath)
	}
	if cfg.CacheDir != filepath.Join(legacy, "cache") {
		t.Fatalf("cache dir = %q", cfg.CacheDir)
	}
	if cfg.VectorDir != filepath.Join(legacy, "vectors") {
		t.Fatalf("vector dir = %q", cfg.VectorDir)
	}
	if cfg.LogDir != filepath.Join(legacy, "logs") {
		t.Fatalf("log dir = %q", cfg.LogDir)
	}
	if got := ResolvePath(""); got != filepath.Join(legacy, "config.toml") {
		t.Fatalf("config path = %q", got)
	}
}

func TestDefaultPathsPreferNewConfigOverLegacy(t *testing.T) {
	home := t.TempDir()
	configHome, _, _, _ := defaultPlatformTestDirs(home)
	legacyConfig := filepath.Join(home, ".config", "gitcrawl", "config.toml")
	newConfig := filepath.Join(configHome, "gitcrawl", "config.toml")
	if err := os.MkdirAll(filepath.Dir(legacyConfig), 0o755); err != nil {
		t.Fatalf("mkdir legacy: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(newConfig), 0o755); err != nil {
		t.Fatalf("mkdir new: %v", err)
	}
	if err := os.WriteFile(legacyConfig, nil, 0o600); err != nil {
		t.Fatalf("write legacy: %v", err)
	}
	if err := os.WriteFile(newConfig, nil, 0o600); err != nil {
		t.Fatalf("write new: %v", err)
	}
	setTestHome(t, home)
	clearXDGEnv(t)

	if got := ResolvePath(""); got != newConfig {
		t.Fatalf("config path = %q", got)
	}
}

func TestApplyRuntimeEnvUsesDBEnv(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "override.db")
	t.Setenv("GITCRAWL_DB_PATH", dbPath)
	t.Setenv("GITCRAWL_VECTOR_BACKEND", "turbovec")

	cfg := Default()
	cfg.DBPath = ""
	cfg.Env = map[string]string{"GITCRAWL_TUI_LAYOUT": "focus"}
	if err := cfg.Normalize(); err != nil {
		t.Fatalf("normalize: %v", err)
	}
	cfg.ApplyRuntimeEnv()
	if cfg.DBPath != dbPath {
		t.Fatalf("db path: got %q want %q", cfg.DBPath, dbPath)
	}
	wantVectorDir := filepath.Join(dir, "vectors")
	if cfg.VectorDir != wantVectorDir {
		t.Fatalf("vector dir: got %q want %q", cfg.VectorDir, wantVectorDir)
	}
	if cfg.TUI.DefaultLayout != "focus" {
		t.Fatalf("tui layout: got %q want focus", cfg.TUI.DefaultLayout)
	}
	if cfg.VectorBackend != "turbovec" {
		t.Fatalf("vector backend: got %q want turbovec", cfg.VectorBackend)
	}
}

func TestNormalizeDoesNotPersistTUILayoutEnv(t *testing.T) {
	t.Setenv("GITCRAWL_TUI_LAYOUT", "right-stack")

	cfg := Default()
	cfg.TUI.DefaultLayout = ""
	if err := cfg.Normalize(); err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if cfg.TUI.DefaultLayout != "columns" {
		t.Fatalf("tui layout: got %q want columns", cfg.TUI.DefaultLayout)
	}
}

func TestNormalizeValidatesVectorBackend(t *testing.T) {
	cfg := Default()
	cfg.VectorBackend = "TURBOVEC"
	if err := cfg.Normalize(); err != nil {
		t.Fatalf("normalize turbovec: %v", err)
	}
	if cfg.VectorBackend != "turbovec" {
		t.Fatalf("vector backend: got %q want turbovec", cfg.VectorBackend)
	}

	cfg.VectorBackend = "bogus"
	if err := cfg.Normalize(); err == nil || !strings.Contains(err.Error(), "unsupported vector_backend") {
		t.Fatalf("normalize bogus backend err = %v", err)
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
	if cfg.VectorDir != filepath.Join(dir, "vectors") {
		t.Fatalf("vector dir: got %q want custom db sibling vectors", cfg.VectorDir)
	}
	if cfg.OpenAI.SummaryModel != "summary-config" || cfg.OpenAI.EmbedModel != "embed-config" {
		t.Fatalf("config env models not used: %+v", cfg.OpenAI)
	}
}

func TestNormalizeDerivesVectorDirFromCustomDB(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "stores", "gitcrawl.db")
	cfg := Config{DBPath: dbPath}
	if err := cfg.Normalize(); err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if cfg.VectorDir != filepath.Join(dir, "stores", "vectors") {
		t.Fatalf("vector dir: got %q want sibling vectors", cfg.VectorDir)
	}
}

func TestNormalizeMovesDefaultVectorDirWithChangedDB(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "custom.db")
	cfg := Default()
	cfg.DBPath = dbPath
	if err := cfg.Normalize(); err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if cfg.VectorDir != filepath.Join(dir, "vectors") {
		t.Fatalf("vector dir: got %q want custom db sibling vectors", cfg.VectorDir)
	}
}

func TestNormalizeKeepsExplicitVectorDir(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "custom.db")
	vectorDir := filepath.Join(dir, "explicit-vectors")
	cfg := Config{DBPath: dbPath, VectorDir: vectorDir}
	if err := cfg.Normalize(); err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if cfg.VectorDir != vectorDir {
		t.Fatalf("vector dir: got %q want explicit %q", cfg.VectorDir, vectorDir)
	}
}

func TestApplyRuntimeEnvKeepsExplicitVectorDir(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "override.db")
	vectorDir := filepath.Join(dir, "explicit-vectors")
	t.Setenv("GITCRAWL_DB_PATH", dbPath)

	cfg := Config{VectorDir: vectorDir}
	if err := cfg.Normalize(); err != nil {
		t.Fatalf("normalize: %v", err)
	}
	cfg.ApplyRuntimeEnv()
	if cfg.VectorDir != vectorDir {
		t.Fatalf("vector dir: got %q want explicit %q", cfg.VectorDir, vectorDir)
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

func TestLoadRuntimeAppliesEmbedBaseURLOverride(t *testing.T) {
	t.Setenv("GITCRAWL_EMBED_BASE_URL", "https://process.example.com/v1")

	path := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(path, []byte(`
[openai]
embed_base_url = "https://config.example.com/v1"
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	loaded, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if loaded.OpenAI.EmbedBaseURL != "https://config.example.com/v1" {
		t.Fatalf("config embed_base_url = %q", loaded.OpenAI.EmbedBaseURL)
	}

	runtime, err := LoadRuntime(path)
	if err != nil {
		t.Fatalf("load runtime config: %v", err)
	}
	if runtime.OpenAI.EmbedBaseURL != "https://process.example.com/v1" {
		t.Fatalf("runtime embed_base_url = %q", runtime.OpenAI.EmbedBaseURL)
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
	home := t.TempDir()
	configHome, _, _, _ := defaultPlatformTestDirs(home)
	setTestHome(t, home)
	clearXDGEnv(t)
	if got := ResolvePath(""); got != filepath.Join(configHome, "gitcrawl", "config.toml") {
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

func setTestHome(t *testing.T, home string) {
	t.Helper()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("LOCALAPPDATA", filepath.Join(home, "AppData", "Local"))
	t.Setenv("APPDATA", filepath.Join(home, "AppData", "Roaming"))
}

func clearXDGEnv(t *testing.T) {
	t.Helper()
	t.Setenv("XDG_CONFIG_HOME", "")
	t.Setenv("XDG_DATA_HOME", "")
	t.Setenv("XDG_CACHE_HOME", "")
	t.Setenv("XDG_STATE_HOME", "")
}

func defaultPlatformTestDirs(home string) (configHome, dataHome, cacheHome, stateHome string) {
	switch runtime.GOOS {
	case "darwin":
		appSupport := filepath.Join(home, "Library", "Application Support")
		return appSupport, appSupport, filepath.Join(home, "Library", "Caches"), appSupport
	case "windows":
		localAppData := filepath.Join(home, "AppData", "Local")
		return localAppData, localAppData, filepath.Join(localAppData, "cache"), localAppData
	default:
		return filepath.Join(home, ".config"),
			filepath.Join(home, ".local", "share"),
			filepath.Join(home, ".cache"),
			filepath.Join(home, ".local", "state")
	}
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
