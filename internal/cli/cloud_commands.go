package cli

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

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
		UserAgent:  "gitcrawl/" + version,
		HTTPClient: &http.Client{Timeout: 10 * time.Minute},
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
	threadSQL, err := gitcrawlThreadExportSQL(ctx, rt.Store.DB())
	if err != nil {
		return err
	}
	threadRows, err := publishRows(ctx, rt.Store.DB(), threadSQL, func(values []any) []any { return values })
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
	sqliteBundle, err := uploadSQLiteArchive(ctx, client, "gitcrawl", archiveID, rt.Store.DB(), rt.SourceDBPath, manifest, map[string]int64{
		"repositories": repoAccepted,
		"threads":      threadAccepted,
	})
	if err != nil {
		return err
	}
	return a.writeOutput("cloud publish", map[string]any{
		"remote":        strings.TrimRight(endpoint, "/"),
		"archive":       archiveID,
		"repositories":  repoAccepted,
		"threads":       threadAccepted,
		"sqlite_bundle": sqliteBundle,
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

func uploadSQLiteArchive(ctx context.Context, client *crawlremote.Client, app, archive string, db *sql.DB, dbPath string, manifest crawlremote.IngestManifest, counts map[string]int64) (*crawlremote.SQLiteBundle, error) {
	snapshotPath, cleanup, err := sqliteSnapshotPath(ctx, db, dbPath)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	bundle, err := crawlremote.BuildGzipSQLiteBundle(ctx, crawlremote.SQLiteBundleBuildOptions{
		App:        app,
		Archive:    archive,
		SourcePath: snapshotPath,
		Counts:     counts,
		Privacy: map[string]any{
			"includes_private_messages": false,
			"includes_raw_json":         false,
		},
	})
	if err != nil {
		return nil, err
	}
	defer bundle.Cleanup()
	result, err := client.UploadSQLiteBundleFiles(ctx, app, archive, bundle.Manifest, bundle.Parts)
	if err != nil {
		return nil, err
	}
	return result.Bundle, nil
}

func sqliteSnapshotPath(ctx context.Context, db *sql.DB, dbPath string) (string, func(), error) {
	tmpDir, err := os.MkdirTemp("", "gitcrawl-cloud-sqlite-*")
	if err != nil {
		return "", func() {}, fmt.Errorf("create sqlite snapshot dir: %w", err)
	}
	cleanup := func() { _ = os.RemoveAll(tmpDir) }
	snapshotPath := filepath.Join(tmpDir, "archive.db")
	if _, err := db.ExecContext(ctx, "vacuum main into ?", snapshotPath); err == nil {
		return snapshotPath, cleanup, nil
	}
	source := strings.TrimSpace(dbPath)
	if source == "" {
		cleanup()
		return "", func() {}, fmt.Errorf("sqlite snapshot failed and no source db path is available")
	}
	return source, cleanup, nil
}

func cloudFileSHA256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open sqlite snapshot for hash: %w", err)
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", fmt.Errorf("hash sqlite snapshot: %w", err)
	}
	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}

var gitcrawlRepositoryColumns = []string{"id", "full_name", "owner", "name", "html_url", "default_branch", "updated_at"}

const gitcrawlRepositoryExportSQL = `
select id, full_name, owner, name, '' as html_url, '' as default_branch, updated_at
from repositories
order by id`

var gitcrawlThreadColumns = []string{"id", "repo_id", "github_id", "number", "kind", "state", "title", "body", "author_login", "author_type", "html_url", "labels_json", "assignees_json", "is_draft", "created_at_gh", "updated_at_gh", "closed_at_gh", "merged_at_gh", "updated_at"}

func gitcrawlThreadExportSQL(ctx context.Context, db *sql.DB) (string, error) {
	bodyExpr, err := gitcrawlThreadBodyExpr(ctx, db)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`
select id, repo_id, github_id, number, kind, state, title, %s,
       coalesce(author_login, ''), coalesce(author_type, ''), html_url,
       labels_json, assignees_json, is_draft, coalesce(created_at_gh, ''),
       coalesce(updated_at_gh, ''), coalesce(closed_at_gh, ''),
       coalesce(merged_at_gh, ''), updated_at
from threads
order by repo_id, number`, bodyExpr), nil
}

func gitcrawlThreadBodyExpr(ctx context.Context, db *sql.DB) (string, error) {
	hasBody, err := sqliteColumnExists(ctx, db, "threads", "body")
	if err != nil {
		return "", err
	}
	hasExcerpt, err := sqliteColumnExists(ctx, db, "threads", "body_excerpt")
	if err != nil {
		return "", err
	}
	switch {
	case hasBody && hasExcerpt:
		return "coalesce(body, body_excerpt, '')", nil
	case hasBody:
		return "coalesce(body, '')", nil
	case hasExcerpt:
		return "coalesce(body_excerpt, '')", nil
	default:
		return "''", nil
	}
}

func sqliteColumnExists(ctx context.Context, db *sql.DB, table, column string) (bool, error) {
	rows, err := db.QueryContext(ctx, `pragma table_info(`+table+`)`)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, typ string
		var notNull int
		var defaultValue any
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk); err != nil {
			return false, err
		}
		if name == column {
			return true, nil
		}
	}
	return false, rows.Err()
}
