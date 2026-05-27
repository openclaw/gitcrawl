package cli

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"strings"

	crawlremote "github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/gitcrawl/internal/config"
)

const gitcrawlCloudBatchSize = 250

func (a *App) runCloud(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return usageErr(fmt.Errorf("cloud requires a subcommand"))
	}
	switch args[0] {
	case "publish":
		return a.runCloudPublish(ctx, args[1:])
	default:
		return usageErr(fmt.Errorf("unknown cloud subcommand %q", args[0]))
	}
}

func (a *App) runCloudPublish(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("cloud publish", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	remoteEndpoint := fs.String("remote", "", "remote archive endpoint")
	archive := fs.String("archive", "", "remote archive id")
	tokenEnv := fs.String("token-env", "", "remote token environment variable")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"remote": true, "archive": true, "token-env": true})); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)
	if fs.NArg() != 0 {
		return usageErr(fmt.Errorf("cloud publish takes flags only"))
	}

	cfg, err := config.LoadRuntime(a.configPath)
	if err != nil {
		return err
	}
	endpoint := firstNonEmpty(*remoteEndpoint, cfg.Remote.Endpoint)
	archiveID := firstNonEmpty(*archive, cfg.Remote.Archive)
	if endpoint == "" {
		return usageErr(fmt.Errorf("cloud publish requires --remote or remote.endpoint"))
	}
	if archiveID == "" {
		return usageErr(fmt.Errorf("cloud publish requires --archive or remote.archive"))
	}

	rt, err := a.openLocalRuntimeReadOnly(ctx)
	if err != nil {
		return err
	}
	defer rt.Store.Close()

	remoteCfg := crawlremote.Config{
		Mode:     crawlremote.ModePublisher,
		Endpoint: endpoint,
		Archive:  archiveID,
		TokenEnv: firstNonEmpty(*tokenEnv, cfg.Remote.TokenEnv, crawlremote.DefaultTokenEnv),
	}
	client, err := crawlremote.NewClientFromConfig(remoteCfg, crawlremote.Options{
		UserAgent: "gitcrawl/" + version,
	})
	if err != nil {
		return err
	}
	manifest := crawlremote.IngestManifest{
		App:           "gitcrawl",
		Archive:       archiveID,
		SchemaName:    "gitcrawl-cloud-v1",
		SchemaVersion: 1,
		SchemaHash:    "gitcrawl-cloud-v1",
		Mode:          crawlremote.ModePublisher,
		Source:        "sqlite",
	}
	repoRows, err := publishRows(ctx, rt.Store.DB(), gitcrawlRepositoryExportSQL, func(values []any) []any { return values })
	if err != nil {
		return err
	}
	threadRows, err := publishRows(ctx, rt.Store.DB(), gitcrawlThreadExportSQL, func(values []any) []any { return values })
	if err != nil {
		return err
	}
	repoAccepted, err := sendIngestRows(ctx, client, "gitcrawl", archiveID, manifest, "repositories", gitcrawlRepositoryColumns, repoRows, false)
	if err != nil {
		return err
	}
	threadAccepted, err := sendIngestRows(ctx, client, "gitcrawl", archiveID, manifest, "threads", gitcrawlThreadColumns, threadRows, true)
	if err != nil {
		return err
	}
	return a.writeOutput("cloud publish", map[string]any{
		"remote":       strings.TrimRight(endpoint, "/"),
		"archive":      archiveID,
		"repositories": repoAccepted,
		"threads":      threadAccepted,
	}, true)
}

func publishRows(ctx context.Context, db *sql.DB, query string, mapRow func([]any) []any) ([][]any, error) {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	out := make([][]any, 0)
	for rows.Next() {
		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		for i, value := range values {
			if bytes, ok := value.([]byte); ok {
				values[i] = string(bytes)
			}
		}
		out = append(out, mapRow(values))
	}
	return out, rows.Err()
}

func sendIngestRows(ctx context.Context, client *crawlremote.Client, app, archive string, manifest crawlremote.IngestManifest, table string, columns []string, rows [][]any, final bool) (int64, error) {
	var total int64
	if len(rows) == 0 {
		result, err := client.Ingest(ctx, app, archive, crawlremote.IngestRequest{
			Manifest: manifest,
			Table:    table,
			Columns:  columns,
			Rows:     [][]any{},
			Final:    final,
		})
		return result.RowsAccepted, err
	}
	for start := 0; start < len(rows); start += gitcrawlCloudBatchSize {
		end := start + gitcrawlCloudBatchSize
		if end > len(rows) {
			end = len(rows)
		}
		result, err := client.Ingest(ctx, app, archive, crawlremote.IngestRequest{
			Manifest: manifest,
			Table:    table,
			Columns:  columns,
			Rows:     rows[start:end],
			Cursor:   cursorFor(start),
			Final:    final && end == len(rows),
		})
		if err != nil {
			return total, err
		}
		total += result.RowsAccepted
	}
	return total, nil
}

func cursorFor(start int) string {
	if start == 0 {
		return ""
	}
	return fmt.Sprintf("%d", start)
}

var gitcrawlRepositoryColumns = []string{"id", "full_name", "owner", "name", "html_url", "default_branch", "updated_at"}

const gitcrawlRepositoryExportSQL = `
select id, full_name, owner, name, '' as html_url, '' as default_branch, updated_at
from repositories
order by id`

var gitcrawlThreadColumns = []string{"id", "repo_id", "github_id", "number", "kind", "state", "title", "body", "author_login", "author_type", "html_url", "labels_json", "assignees_json", "is_draft", "created_at_gh", "updated_at_gh", "closed_at_gh", "merged_at_gh", "updated_at"}

const gitcrawlThreadExportSQL = `
select id, repo_id, github_id, number, kind, state, title, coalesce(body, ''),
       coalesce(author_login, ''), coalesce(author_type, ''), html_url,
       labels_json, assignees_json, is_draft, coalesce(created_at_gh, ''),
       coalesce(updated_at_gh, ''), coalesce(closed_at_gh, ''),
       coalesce(merged_at_gh, ''), updated_at
from threads
order by repo_id, number`
