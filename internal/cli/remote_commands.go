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
	"time"

	"github.com/openclaw/crawlkit/control"
	crawlremote "github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/gitcrawl/internal/config"
	"github.com/openclaw/gitcrawl/internal/store"
)

func (a *App) runRemote(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return usageErr(fmt.Errorf("remote requires a subcommand"))
	}
	switch args[0] {
	case "help", "--help", "-h":
		return a.printCommandUsage("remote")
	case "status":
		return a.runRemoteStatus(ctx, args[1:])
	case "archives":
		return a.runRemoteArchives(ctx, args[1:])
	case "login":
		return a.runRemoteLogin(ctx, args[1:])
	case "whoami":
		return a.runRemoteWhoami(ctx, args[1:])
	default:
		return usageErr(fmt.Errorf("unknown remote subcommand %q", args[0]))
	}
}

func (a *App) runRemoteLogin(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("remote login", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	endpoint := fs.String("endpoint", "", "remote archive endpoint")
	githubTokenEnv := fs.String("github-token-env", "", "environment variable containing a GitHub token to exchange for a remote session")
	noBrowser := fs.Bool("no-browser", false, "print login URL without opening a browser")
	timeoutRaw := fs.String("timeout", "5m", "login timeout")
	pollRaw := fs.String("poll-interval", "2s", "login poll interval")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"endpoint": true, "github-token-env": true, "timeout": true, "poll-interval": true})); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() > 1 {
		return usageErr(fmt.Errorf("remote login accepts at most one endpoint"))
	}
	if fs.NArg() == 1 {
		if *endpoint != "" {
			return usageErr(fmt.Errorf("use either --endpoint or a positional endpoint"))
		}
		*endpoint = fs.Arg(0)
	}
	timeout, err := time.ParseDuration(*timeoutRaw)
	if err != nil || timeout <= 0 {
		return usageErr(fmt.Errorf("invalid --timeout %q", *timeoutRaw))
	}
	pollInterval, err := time.ParseDuration(*pollRaw)
	if err != nil || pollInterval <= 0 {
		return usageErr(fmt.Errorf("invalid --poll-interval %q", *pollRaw))
	}
	cfg, configExists, err := a.loadConfigOrDefault()
	if err != nil {
		return err
	}
	if strings.TrimSpace(*endpoint) != "" {
		cfg.Remote.Endpoint = *endpoint
	}
	cfg.Remote.Normalize()
	if cfg.Remote.Endpoint == "" {
		return usageErr(fmt.Errorf("remote login requires --endpoint or remote.endpoint"))
	}
	client, err := crawlremote.NewClientFromConfig(cfg.Remote, crawlremote.Options{UserAgent: "gitcrawl/" + version})
	if err != nil {
		return err
	}
	if tokenEnv := strings.TrimSpace(*githubTokenEnv); tokenEnv != "" {
		githubToken := strings.TrimSpace(os.Getenv(tokenEnv))
		if githubToken == "" {
			return fmt.Errorf("%s is empty", tokenEnv)
		}
		result, err := client.LoginWithGitHubToken(ctx, githubToken)
		if err != nil {
			return err
		}
		return a.finishRemoteLogin(cfg, configExists, strings.TrimSpace(*endpoint), "github-token", result)
	}
	pollSecret, err := crawlremote.NewLoginPollSecret()
	if err != nil {
		return err
	}
	start, err := client.StartGitHubLogin(ctx, crawlremote.LoginPollSecretHash(pollSecret))
	if err != nil {
		return err
	}
	if !*noBrowser {
		if err := openURL(start.URL); err != nil && a.format != FormatJSON {
			_, _ = fmt.Fprintf(a.Stdout, "Open this URL to continue login:\n%s\n", start.URL)
		}
	} else if a.format != FormatJSON {
		_, _ = fmt.Fprintf(a.Stdout, "Open this URL to continue login:\n%s\n", start.URL)
	}
	result, err := pollRemoteLogin(ctx, client, start.LoginID, pollSecret, timeout, pollInterval)
	if err != nil {
		return err
	}
	return a.finishRemoteLogin(cfg, configExists, strings.TrimSpace(*endpoint), "github-oauth", result)
}

func (a *App) finishRemoteLogin(cfg config.Config, configExists bool, endpointOverride string, method string, result crawlremote.LoginPollResult) error {
	if strings.ToLower(strings.TrimSpace(result.Status)) != "complete" {
		return fmt.Errorf("remote login returned status %q", result.Status)
	}
	if strings.TrimSpace(result.Token) == "" {
		return fmt.Errorf("remote login completed without token")
	}
	auth, err := config.StoreRemoteToken(cfg, result.Token)
	if err != nil {
		return fmt.Errorf("store remote token: %w", err)
	}
	if cfg.Remote.Mode == "" || cfg.Remote.Mode == crawlremote.ModeLocal {
		cfg.Remote.Mode = crawlremote.ModeCloud
	}
	cfg.Remote.Auth = auth
	if !configExists || endpointOverride != "" || cfg.Remote.Auth.TokenSource == "keyring" {
		if err := config.Save(a.configPath, cfg); err != nil {
			return err
		}
	}
	return a.writeOutput("remote login", map[string]any{
		"config_path":     config.ResolvePath(a.configPath),
		"endpoint":        cfg.Remote.Endpoint,
		"archive":         cfg.Remote.Archive,
		"login":           result.Login,
		"org":             result.Org,
		"owner":           result.Owner,
		"login_method":    method,
		"auth_source":     cfg.Remote.Auth.TokenSource,
		"keyring_service": cfg.Remote.Auth.KeyringService,
		"keyring_account": cfg.Remote.Auth.KeyringAccount,
		"updated":         true,
	}, true)
}

func (a *App) loadConfigOrDefault() (config.Config, bool, error) {
	cfg, err := config.Load(a.configPath)
	if err == nil {
		return cfg, true, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return config.Config{}, false, err
	}
	return config.Default(), false, nil
}

func pollRemoteLogin(ctx context.Context, client *crawlremote.Client, loginID, pollSecret string, timeout, interval time.Duration) (crawlremote.LoginPollResult, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		result, err := client.PollGitHubLogin(ctx, loginID, pollSecret)
		if err != nil {
			return crawlremote.LoginPollResult{}, err
		}
		switch strings.ToLower(strings.TrimSpace(result.Status)) {
		case "complete":
			if strings.TrimSpace(result.Token) == "" {
				return crawlremote.LoginPollResult{}, fmt.Errorf("remote login completed without token")
			}
			return result, nil
		case "error":
			if result.Error != "" {
				return crawlremote.LoginPollResult{}, fmt.Errorf("remote login failed: %s", result.Error)
			}
			return crawlremote.LoginPollResult{}, fmt.Errorf("remote login failed")
		case "", "pending":
		default:
			return crawlremote.LoginPollResult{}, fmt.Errorf("remote login returned status %q", result.Status)
		}
		select {
		case <-ctx.Done():
			return crawlremote.LoginPollResult{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (a *App) runRemoteWhoami(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("whoami", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, nil)); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 0 {
		return usageErr(fmt.Errorf("whoami takes flags only"))
	}
	cfg, err := config.LoadRuntime(a.configPath)
	if err != nil {
		return err
	}
	client, err := a.remoteClient(cfg)
	if err != nil {
		return err
	}
	identity, err := client.Whoami(ctx)
	if err != nil {
		return err
	}
	return a.writeOutput("whoami", identity, false)
}

func (a *App) runRemoteArchives(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("remote archives", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, nil)); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 0 {
		return usageErr(fmt.Errorf("remote archives takes flags only"))
	}
	cfg, err := config.LoadRuntime(a.configPath)
	if err != nil {
		return err
	}
	client, err := a.remoteClient(cfg)
	if err != nil {
		return err
	}
	archives, err := client.Archives(ctx)
	if err != nil {
		return err
	}
	return a.writeOutput("remote archives", map[string]any{"archives": archives}, false)
}

func (a *App) runRemoteStatus(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("remote status", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, nil)); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 0 {
		return usageErr(fmt.Errorf("remote status takes flags only"))
	}
	cfg, err := config.LoadRuntime(a.configPath)
	if err != nil {
		return err
	}
	return a.runRemoteStatusWithConfig(ctx, cfg)
}

func (a *App) runRemoteSearch(ctx context.Context, cfg config.Config, owner, repoName, query string, limit int, mode string) error {
	client, err := a.remoteClient(cfg)
	if err != nil {
		return err
	}
	result, err := client.Query(ctx, "gitcrawl", cfg.Remote.Archive, crawlremote.QueryRequest{
		Name: "gitcrawl.threads.search",
		Args: map[string]any{
			"owner": owner,
			"repo":  repoName,
			"query": query,
			"mode":  mode,
			"limit": limit,
		},
		Limit: limit,
	})
	if err != nil {
		return err
	}
	hits := remoteSearchHits(result)
	payload := map[string]any{
		"repository": owner + "/" + repoName,
		"query":      query,
		"mode":       mode,
		"remote": map[string]any{
			"endpoint": cfg.Remote.Endpoint,
			"archive":  cfg.Remote.Archive,
			"stats":    result.Stats,
		},
		"hits": hits,
	}
	return a.writeOutput("search", payload, true)
}

type remoteGHSearchOptions struct {
	Owner      string
	Repo       string
	Query      string
	Kind       string
	State      string
	Limit      int
	JSONFields string
	JQ         string
	TextKind   string
}

func (a *App) runRemoteGHSearch(ctx context.Context, cfg config.Config, opts remoteGHSearchOptions) error {
	client, err := a.remoteClient(cfg)
	if err != nil {
		return err
	}
	result, err := client.Query(ctx, "gitcrawl", cfg.Remote.Archive, crawlremote.QueryRequest{
		Name: "gitcrawl.threads.search",
		Args: map[string]any{
			"owner": opts.Owner,
			"repo":  opts.Repo,
			"query": opts.Query,
			"kind":  opts.Kind,
			"state": opts.State,
			"limit": opts.Limit,
		},
		Limit: opts.Limit,
	})
	if err != nil {
		return err
	}
	threads := remoteThreads(result)
	jsonFields := strings.TrimSpace(opts.JSONFields)
	if jsonFields != "" || a.format == FormatJSON {
		if jsonFields == "" {
			jsonFields = "number,title,state,url"
		}
		rows, err := ghSearchJSONRows(threads, jsonFields)
		if err != nil {
			return usageErr(err)
		}
		return a.writeJSONValue(rows, strings.TrimSpace(opts.JQ))
	}
	for _, thread := range threads {
		if _, err := fmt.Fprintf(a.Stdout, "%s\t#%d\t%s\t%s\n", firstNonEmpty(opts.TextKind, thread.Kind), thread.Number, thread.Title, thread.HTMLURL); err != nil {
			return err
		}
	}
	return nil
}

func (a *App) remoteClient(cfg config.Config) (*crawlremote.Client, error) {
	cfg.Remote.Normalize()
	if !cfg.Remote.Enabled() {
		return nil, fmt.Errorf("remote archive is not configured")
	}
	tokenProvider := crawlremote.TokenProvider(crawlremote.EnvTokenProvider{Name: cfg.Remote.TokenEnv})
	if token, err := config.ResolveRemoteTokenWithKeyring(cfg); err != nil {
		return nil, err
	} else if token.Value != "" {
		tokenProvider = crawlremote.StaticToken(token.Value)
	}
	return crawlremote.NewClientFromConfig(cfg.Remote, crawlremote.Options{
		TokenProvider: tokenProvider,
		UserAgent:     "gitcrawl/" + version,
	})
}

func remoteThreads(result crawlremote.QueryResult) []store.Thread {
	values := result.Values
	if len(values) == 0 && len(result.Columns) > 0 {
		values = mapsFromRows(result.Columns, result.Rows)
	}
	threads := make([]store.Thread, 0, len(values))
	for _, value := range values {
		threads = append(threads, store.Thread{
			ID:              int64Value(value, "thread_id", "id"),
			GitHubID:        stringValue(value, "github_id"),
			Number:          intValue(value, "number"),
			Kind:            stringValue(value, "kind"),
			State:           stringValue(value, "state"),
			Title:           stringValue(value, "title"),
			Body:            stringValue(value, "body"),
			AuthorLogin:     stringValue(value, "author_login", "author"),
			AuthorType:      stringValue(value, "author_type"),
			HTMLURL:         stringValue(value, "html_url", "url"),
			LabelsJSON:      firstNonEmpty(stringValue(value, "labels_json"), "[]"),
			AssigneesJSON:   firstNonEmpty(stringValue(value, "assignees_json"), "[]"),
			IsDraft:         boolValue(value, "is_draft", "isDraft"),
			CreatedAtGitHub: stringValue(value, "created_at_gh", "createdAt"),
			UpdatedAtGitHub: stringValue(value, "updated_at_gh", "updatedAt"),
			ClosedAtGitHub:  stringValue(value, "closed_at_gh", "closedAt"),
			MergedAtGitHub:  stringValue(value, "merged_at_gh", "mergedAt"),
			UpdatedAt:       stringValue(value, "updated_at"),
		})
	}
	return threads
}

func remoteSearchHits(result crawlremote.QueryResult) []store.SearchHit {
	values := result.Values
	if len(values) == 0 && len(result.Columns) > 0 {
		values = mapsFromRows(result.Columns, result.Rows)
	}
	hits := make([]store.SearchHit, 0, len(values))
	for _, value := range values {
		hits = append(hits, store.SearchHit{
			ThreadID:    int64Value(value, "thread_id", "id"),
			Number:      intValue(value, "number"),
			Kind:        stringValue(value, "kind"),
			State:       stringValue(value, "state"),
			Title:       stringValue(value, "title"),
			HTMLURL:     stringValue(value, "html_url", "url"),
			AuthorLogin: stringValue(value, "author_login", "author"),
			Snippet:     stringValue(value, "snippet", "body_excerpt", "title"),
			Score:       floatValue(value, "score"),
		})
	}
	return hits
}

func boolValue(value map[string]any, keys ...string) bool {
	for _, key := range keys {
		switch v := value[key].(type) {
		case bool:
			return v
		case string:
			parsed, err := strconv.ParseBool(strings.TrimSpace(v))
			if err == nil {
				return parsed
			}
		case int:
			return v != 0
		case int64:
			return v != 0
		case float64:
			return v != 0
		case json.Number:
			parsed, err := v.Int64()
			if err == nil {
				return parsed != 0
			}
		}
	}
	return false
}

func mapsFromRows(columns []string, rows [][]any) []map[string]any {
	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		item := make(map[string]any, len(columns))
		for i, column := range columns {
			if i < len(row) {
				item[column] = row[i]
			}
		}
		out = append(out, item)
	}
	return out
}

func nonEmptyCount(values ...string) int {
	count := 0
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			count++
		}
	}
	return count
}

func countValue(counts []control.Count, id string) int64 {
	for _, count := range counts {
		if count.ID == id {
			return count.Value
		}
	}
	return 0
}

func stringValue(value map[string]any, keys ...string) string {
	for _, key := range keys {
		switch v := value[key].(type) {
		case string:
			if strings.TrimSpace(v) != "" {
				return v
			}
		case fmt.Stringer:
			if strings.TrimSpace(v.String()) != "" {
				return v.String()
			}
		}
	}
	return ""
}

func intValue(value map[string]any, keys ...string) int {
	const maxInt = int64(1<<(strconv.IntSize-1) - 1)
	const minInt = -maxInt - 1
	for _, key := range keys {
		switch v := value[key].(type) {
		case int:
			return v
		case int64:
			if v >= minInt && v <= maxInt {
				return int(v)
			}
		case float64:
			if v >= float64(minInt) && v <= float64(maxInt) {
				return int(v)
			}
		case json.Number:
			if parsed, err := strconv.Atoi(v.String()); err == nil {
				return parsed
			}
		case string:
			if parsed, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
				return parsed
			}
		}
	}
	return 0
}

func int64Value(value map[string]any, keys ...string) int64 {
	for _, key := range keys {
		switch v := value[key].(type) {
		case int:
			return int64(v)
		case int64:
			return v
		case float64:
			return int64(v)
		case json.Number:
			if parsed, err := v.Int64(); err == nil {
				return parsed
			}
		case string:
			if parsed, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64); err == nil {
				return parsed
			}
		}
	}
	return 0
}

func floatValue(value map[string]any, keys ...string) float64 {
	for _, key := range keys {
		switch v := value[key].(type) {
		case float64:
			return v
		case int:
			return float64(v)
		case int64:
			return float64(v)
		case json.Number:
			if parsed, err := v.Float64(); err == nil {
				return parsed
			}
		case string:
			if parsed, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
				return parsed
			}
		}
	}
	return 0
}
