package cli

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"

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
	case "whoami":
		return a.runRemoteWhoami(ctx, args[1:])
	default:
		return usageErr(fmt.Errorf("unknown remote subcommand %q", args[0]))
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
	if token := config.ResolveRemoteToken(cfg); token.Value != "" {
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
	return int(int64Value(value, keys...))
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
