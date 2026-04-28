package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/pelletier/go-toml/v2"
)

const (
	DefaultConfigEnv = "GITCRAWL_CONFIG"
	DefaultTokenEnv  = "GITHUB_TOKEN"
	DefaultOpenAIEnv = "OPENAI_API_KEY"
)

type Config struct {
	Version        int          `toml:"version"`
	DBPath         string       `toml:"db_path"`
	CacheDir       string       `toml:"cache_dir"`
	VectorDir      string       `toml:"vector_dir"`
	LogDir         string       `toml:"log_dir"`
	GitHub         GitHubConfig `toml:"github"`
	OpenAI         OpenAIConfig `toml:"openai"`
	EmbeddingBasis string       `toml:"embedding_basis"`
	TUI            TUIConfig    `toml:"tui"`
}

type GitHubConfig struct {
	TokenEnv string `toml:"token_env"`
}

type OpenAIConfig struct {
	APIKeyEnv       string `toml:"api_key_env"`
	SummaryModel    string `toml:"summary_model"`
	EmbedModel      string `toml:"embed_model"`
	EmbedDimensions int    `toml:"embed_dimensions"`
	BatchSize       int    `toml:"batch_size"`
	Concurrency     int    `toml:"concurrency"`
}

type TUIConfig struct {
	DefaultSort string `toml:"default_sort"`
}

type TokenResolution struct {
	Value  string
	Source string
}

func Default() Config {
	home := homeDir()
	base := filepath.Join(home, ".config", "gitcrawl")
	return Config{
		Version:        1,
		DBPath:         filepath.Join(base, "gitcrawl.db"),
		CacheDir:       filepath.Join(base, "cache"),
		VectorDir:      filepath.Join(base, "vectors"),
		LogDir:         filepath.Join(base, "logs"),
		EmbeddingBasis: "title_original",
		GitHub: GitHubConfig{
			TokenEnv: DefaultTokenEnv,
		},
		OpenAI: OpenAIConfig{
			APIKeyEnv:       DefaultOpenAIEnv,
			SummaryModel:    "gpt-5.4",
			EmbedModel:      "text-embedding-3-small",
			EmbedDimensions: 1024,
			BatchSize:       64,
			Concurrency:     2,
		},
		TUI: TUIConfig{
			DefaultSort: "size",
		},
	}
}

func ResolvePath(flagPath string) string {
	if strings.TrimSpace(flagPath) != "" {
		return expandHome(flagPath)
	}
	if envPath := strings.TrimSpace(os.Getenv(DefaultConfigEnv)); envPath != "" {
		return expandHome(envPath)
	}
	home := homeDir()
	return filepath.Join(home, ".config", "gitcrawl", "config.toml")
}

func Load(path string) (Config, error) {
	cfg := Default()
	resolved := ResolvePath(path)
	data, err := os.ReadFile(resolved)
	if err != nil {
		return Config{}, err
	}
	if err := toml.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("parse config: %w", err)
	}
	if err := cfg.Normalize(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func Save(path string, cfg Config) error {
	if err := cfg.Normalize(); err != nil {
		return err
	}
	resolved := ResolvePath(path)
	if err := os.MkdirAll(filepath.Dir(resolved), 0o755); err != nil {
		return fmt.Errorf("create config dir: %w", err)
	}
	data, err := toml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}
	return os.WriteFile(resolved, data, 0o600)
}

func EnsureRuntimeDirs(cfg Config) error {
	for _, path := range []string{cfg.CacheDir, cfg.VectorDir, cfg.LogDir, filepath.Dir(cfg.DBPath)} {
		if err := os.MkdirAll(expandHome(path), 0o755); err != nil {
			return fmt.Errorf("create runtime dir %s: %w", path, err)
		}
	}
	return nil
}

func (c *Config) Normalize() error {
	def := Default()
	if c.Version == 0 {
		c.Version = def.Version
	}
	if c.DBPath == "" {
		c.DBPath = def.DBPath
	}
	if c.CacheDir == "" {
		c.CacheDir = def.CacheDir
	}
	if c.VectorDir == "" {
		c.VectorDir = def.VectorDir
	}
	if c.LogDir == "" {
		c.LogDir = def.LogDir
	}
	if c.GitHub.TokenEnv == "" {
		c.GitHub.TokenEnv = def.GitHub.TokenEnv
	}
	if c.OpenAI.APIKeyEnv == "" {
		c.OpenAI.APIKeyEnv = def.OpenAI.APIKeyEnv
	}
	if c.OpenAI.SummaryModel == "" {
		c.OpenAI.SummaryModel = envOrDefault("GITCRAWL_SUMMARY_MODEL", def.OpenAI.SummaryModel)
	}
	if c.OpenAI.EmbedModel == "" {
		c.OpenAI.EmbedModel = envOrDefault("GITCRAWL_EMBED_MODEL", def.OpenAI.EmbedModel)
	}
	if c.OpenAI.EmbedDimensions <= 0 {
		c.OpenAI.EmbedDimensions = def.OpenAI.EmbedDimensions
	}
	if c.OpenAI.BatchSize <= 0 {
		c.OpenAI.BatchSize = def.OpenAI.BatchSize
	}
	if c.OpenAI.Concurrency <= 0 {
		c.OpenAI.Concurrency = def.OpenAI.Concurrency
	}
	if c.EmbeddingBasis == "" {
		c.EmbeddingBasis = def.EmbeddingBasis
	}
	if c.TUI.DefaultSort == "" {
		c.TUI.DefaultSort = def.TUI.DefaultSort
	}
	c.DBPath = expandHome(envOrDefault("GITCRAWL_DB_PATH", c.DBPath))
	c.CacheDir = expandHome(c.CacheDir)
	c.VectorDir = expandHome(c.VectorDir)
	c.LogDir = expandHome(c.LogDir)
	return nil
}

func ResolveGitHubToken(cfg Config) TokenResolution {
	if value := strings.TrimSpace(os.Getenv(cfg.GitHub.TokenEnv)); value != "" {
		return TokenResolution{Value: value, Source: cfg.GitHub.TokenEnv}
	}
	return TokenResolution{}
}

func ResolveOpenAIKey(cfg Config) TokenResolution {
	if value := strings.TrimSpace(os.Getenv(cfg.OpenAI.APIKeyEnv)); value != "" {
		return TokenResolution{Value: value, Source: cfg.OpenAI.APIKeyEnv}
	}
	return TokenResolution{}
}

func envOrDefault(primary, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(primary)); value != "" {
		return value
	}
	return fallback
}

func expandHome(path string) string {
	if path == "~" {
		return homeDir()
	}
	if strings.HasPrefix(path, "~/") {
		return filepath.Join(homeDir(), strings.TrimPrefix(path, "~/"))
	}
	return path
}

func homeDir() string {
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		return home
	}
	return "."
}
