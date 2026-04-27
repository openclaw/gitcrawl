package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/openclaw/gitcrawl/internal/config"
	gh "github.com/openclaw/gitcrawl/internal/github"
	"github.com/openclaw/gitcrawl/internal/store"
	"github.com/openclaw/gitcrawl/internal/syncer"
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
	case "sync":
		return a.runSync(ctx, rest[1:])
	case "threads":
		return a.runThreads(ctx, rest[1:])
	case "configure", "refresh", "summarize", "key-summaries", "embed", "cluster", "cluster-experiment", "runs", "clusters", "durable-clusters", "cluster-detail", "cluster-explain", "neighbors", "search", "close-thread", "close-cluster", "exclude-cluster-member", "include-cluster-member", "set-cluster-canonical", "merge-clusters", "split-cluster", "export-sync", "import-sync", "validate-sync", "portable-size", "sync-status", "optimize", "tui", "completion":
		_ = ctx
		return notImplemented(rest[0])
	default:
		return usageErr(fmt.Errorf("unknown command %q", rest[0]))
	}
}

func (a *App) runThreads(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("threads", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	includeClosed := fs.Bool("include-closed", false, "include locally closed rows")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 1 {
		return usageErr(fmt.Errorf("threads requires owner/repo"))
	}
	owner, repoName, err := parseOwnerRepo(fs.Arg(0))
	if err != nil {
		return usageErr(err)
	}

	cfg, err := config.Load(a.configPath)
	if err != nil {
		return err
	}
	st, err := store.Open(ctx, cfg.DBPath)
	if err != nil {
		return err
	}
	defer st.Close()

	repo, err := st.RepositoryByFullName(ctx, owner+"/"+repoName)
	if err != nil {
		return err
	}
	threads, err := st.ListThreads(ctx, repo.ID, *includeClosed)
	if err != nil {
		return err
	}
	return a.writeOutput("threads", map[string]any{
		"repository": repo.FullName,
		"threads":    threads,
	}, true)
}

func (a *App) runSync(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("sync", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	since := fs.String("since", "", "GitHub since timestamp")
	limitRaw := fs.String("limit", "", "maximum issue/PR rows")
	jsonOut := fs.Bool("json", false, "write JSON output")
	fs.Bool("include-comments", false, "accepted for compatibility; hydration is not implemented yet")
	fs.Bool("include-code", false, "accepted for compatibility; code hydration is not implemented yet")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 1 {
		return usageErr(fmt.Errorf("sync requires owner/repo"))
	}
	owner, repo, err := parseOwnerRepo(fs.Arg(0))
	if err != nil {
		return usageErr(err)
	}
	limit, err := parseOptionalPositiveInt(*limitRaw)
	if err != nil {
		return usageErr(err)
	}

	cfg, err := config.Load(a.configPath)
	if err != nil {
		return err
	}
	token := config.ResolveGitHubToken(cfg)
	if token.Value == "" {
		return fmt.Errorf("missing GitHub token: set %s", cfg.GitHub.TokenEnv)
	}
	if err := config.EnsureRuntimeDirs(cfg); err != nil {
		return err
	}
	st, err := store.Open(ctx, cfg.DBPath)
	if err != nil {
		return err
	}
	defer st.Close()

	client := gh.New(gh.Options{Token: token.Value})
	service := syncer.New(client, st)
	stats, err := service.Sync(ctx, syncer.Options{
		Owner: owner,
		Repo:  repo,
		Since: strings.TrimSpace(*since),
		Limit: limit,
		Reporter: func(message string) {
			fmt.Fprintln(a.Stderr, message)
		},
	})
	if err != nil {
		return err
	}
	return a.writeOutput("sync", stats, true)
}

func (a *App) runInit(args []string) error {
	fs := flag.NewFlagSet("init", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", "", "database path")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)

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
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
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
	st, err := store.Open(ctx, cfg.DBPath)
	if err != nil {
		return err
	}
	defer st.Close()
	storeStatus, err := st.Status(ctx)
	if err != nil {
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
		"repository_count":     storeStatus.RepositoryCount,
		"thread_count":         storeStatus.ThreadCount,
		"cluster_count":        storeStatus.ClusterCount,
		"summary_model":        cfg.OpenAI.SummaryModel,
		"embed_model":          cfg.OpenAI.EmbedModel,
		"embedding_basis":      cfg.EmbeddingBasis,
		"api_supported":        false,
	}, true)
}

func (a *App) applyCommandJSON(enabled bool) {
	if enabled {
		a.format = FormatJSON
	}
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

func parseOwnerRepo(value string) (string, string, error) {
	parts := strings.Split(value, "/")
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return "", "", fmt.Errorf("expected owner/repo, got %q", value)
	}
	return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]), nil
}

func parseOptionalPositiveInt(value string) (int, error) {
	if strings.TrimSpace(value) == "" {
		return 0, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("expected positive integer, got %q", value)
	}
	return parsed, nil
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
