package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/openclaw/gitcrawl/internal/config"
)

type App struct {
	Stdout io.Writer
	Stderr io.Writer

	configPath string
	format     OutputFormat
}

type OutputFormat string

const (
	FormatText OutputFormat = "text"
	FormatJSON OutputFormat = "json"
	FormatLog  OutputFormat = "log"
)

var version = "dev"

func New() *App {
	return &App{
		Stdout: os.Stdout,
		Stderr: os.Stderr,
		format: FormatText,
	}
}

func (a *App) Run(ctx context.Context, args []string) error {
	global := flag.NewFlagSet("gitcrawl", flag.ContinueOnError)
	global.SetOutput(io.Discard)
	configPath := global.String("config", "", "config path")
	format := global.String("format", string(FormatText), "output format: text|json|log")
	jsonOut := global.Bool("json", false, "write JSON output")
	versionFlag := global.Bool("version", false, "print version")
	global.Bool("no-color", false, "disable color output")
	if err := global.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			a.printUsage()
			return nil
		}
		return usageErr(err)
	}

	resolvedFormat, err := resolveOutputFormat(*format, *jsonOut)
	if err != nil {
		return usageErr(err)
	}
	a.configPath = strings.TrimSpace(*configPath)
	a.format = resolvedFormat

	rest := global.Args()
	if *versionFlag {
		return a.writeOutput("version", map[string]string{"version": version}, false)
	}
	if len(rest) == 0 || rest[0] == "help" || rest[0] == "--help" || rest[0] == "-h" {
		a.printUsage()
		return nil
	}

	switch rest[0] {
	case "version":
		return a.writeOutput("version", map[string]string{"version": version}, false)
	case "serve":
		return usageErr(fmt.Errorf("serve is not supported in gitcrawl"))
	case "init":
		return a.runInit(rest[1:])
	case "doctor":
		return a.runDoctor(ctx, rest[1:])
	case "configure", "sync", "refresh", "summarize", "key-summaries", "embed", "cluster", "cluster-experiment", "threads", "runs", "clusters", "durable-clusters", "cluster-detail", "cluster-explain", "neighbors", "search", "close-thread", "close-cluster", "exclude-cluster-member", "include-cluster-member", "set-cluster-canonical", "merge-clusters", "split-cluster", "export-sync", "import-sync", "validate-sync", "portable-size", "sync-status", "optimize", "tui", "completion":
		_ = ctx
		return notImplemented(rest[0])
	default:
		return usageErr(fmt.Errorf("unknown command %q", rest[0]))
	}
}

func (a *App) runInit(args []string) error {
	fs := flag.NewFlagSet("init", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", "", "database path")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}

	cfg := config.Default()
	if strings.TrimSpace(*dbPath) != "" {
		cfg.DBPath = strings.TrimSpace(*dbPath)
	}
	if err := config.Save(a.configPath, cfg); err != nil {
		return err
	}
	if err := config.EnsureRuntimeDirs(cfg); err != nil {
		return err
	}
	return a.writeOutput("init", map[string]any{
		"config_path": config.ResolvePath(a.configPath),
		"db_path":     cfg.DBPath,
		"cache_dir":   cfg.CacheDir,
		"vector_dir":  cfg.VectorDir,
	}, true)
}

func (a *App) runDoctor(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("doctor", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	_ = ctx

	cfg, err := config.Load(a.configPath)
	configExists := true
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		configExists = false
		cfg = config.Default()
		if err := cfg.Normalize(); err != nil {
			return err
		}
	}
	if err := config.EnsureRuntimeDirs(cfg); err != nil {
		return err
	}

	githubToken := config.ResolveGitHubToken(cfg)
	openAIKey := config.ResolveOpenAIKey(cfg)
	return a.writeOutput("doctor", map[string]any{
		"version":              version,
		"config_path":          config.ResolvePath(a.configPath),
		"config_exists":        configExists,
		"db_path":              cfg.DBPath,
		"github_token_present": githubToken.Value != "",
		"github_token_source":  githubToken.Source,
		"openai_key_present":   openAIKey.Value != "",
		"openai_key_source":    openAIKey.Source,
		"summary_model":        cfg.OpenAI.SummaryModel,
		"embed_model":          cfg.OpenAI.EmbedModel,
		"embedding_basis":      cfg.EmbeddingBasis,
		"api_supported":        false,
	}, true)
}

func resolveOutputFormat(value string, jsonOut bool) (OutputFormat, error) {
	if jsonOut {
		return FormatJSON, nil
	}
	switch OutputFormat(strings.ToLower(strings.TrimSpace(value))) {
	case "", FormatText:
		return FormatText, nil
	case FormatJSON:
		return FormatJSON, nil
	case FormatLog:
		return FormatLog, nil
	default:
		return "", fmt.Errorf("unsupported format %q: use text, json, or log", value)
	}
}

func (a *App) writeOutput(title string, payload any, allowLog bool) error {
	switch a.format {
	case FormatJSON:
		data, err := json.MarshalIndent(payload, "", "  ")
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(a.Stdout, "%s\n", data)
		return err
	case FormatLog:
		if allowLog {
			_, err := fmt.Fprintf(a.Stdout, "%s=%v\n", title, payload)
			return err
		}
		fallthrough
	default:
		if versionPayload, ok := payload.(map[string]string); ok && title == "version" {
			_, err := fmt.Fprintln(a.Stdout, versionPayload["version"])
			return err
		}
		data, err := json.MarshalIndent(payload, "", "  ")
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(a.Stdout, "%s\n%s\n", title, data)
		return err
	}
}

func (a *App) printUsage() {
	fmt.Fprint(a.Stdout, usageText)
}

const usageText = `gitcrawl mirrors GitHub issues and pull requests into local SQLite for maintainer triage.

Usage:
  gitcrawl [global flags] <command> [command flags]

Global flags:
  --config <path>       config path
  --format <mode>      output format: text|json|log
  --json               write JSON output
  --version            print version

Core commands:
  init                 create config
  doctor               check config, token, and database readiness
  sync                 sync GitHub issue and pull request metadata
  refresh              run sync, enrichment, embedding, and clustering pipeline
  threads              list local issue and pull request rows
  clusters             list cluster summaries
  cluster-detail       dump one durable cluster
  search               search local thread documents
  tui                  browse local clusters in a terminal UI

No API server is provided. The ghcrawl serve command is intentionally omitted.
`
