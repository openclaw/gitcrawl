package cli

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func gitcrawlCloudSQLiteBundlePrivacy() map[string]any {
	return map[string]any{
		"includes_private_messages": true,
		"includes_raw_json":         false,
		"includes_source_code":      true,
	}
}

func cloudSQLiteSnapshotPath(ctx context.Context, db *sql.DB, dbPath string) (string, func(), error) {
	snapshotPath, cleanup, err := sqliteSnapshotPath(ctx, db, "")
	if err != nil {
		source := strings.TrimSpace(dbPath)
		if source == "" {
			return "", func() {}, err
		}
		if _, statErr := os.Stat(source); statErr != nil {
			return "", func() {}, fmt.Errorf("stat cloud SQLite source: %w", statErr)
		}
		reopened, openErr := sql.Open("sqlite", source)
		if openErr != nil {
			return "", func() {}, fmt.Errorf("reopen cloud SQLite source: %w", openErr)
		}
		snapshotPath, cleanup, err = sqliteSnapshotPath(ctx, reopened, "")
		closeErr := reopened.Close()
		if err != nil {
			return "", func() {}, err
		}
		if closeErr != nil {
			cleanup()
			return "", func() {}, fmt.Errorf("close reopened cloud SQLite source: %w", closeErr)
		}
	}
	snapshotDB, err := sql.Open("sqlite", snapshotPath)
	if err != nil {
		cleanup()
		return "", func() {}, fmt.Errorf("open cloud SQLite snapshot: %w", err)
	}
	if err := sanitizeCloudSQLiteSnapshot(ctx, snapshotDB); err != nil {
		_ = snapshotDB.Close()
		cleanup()
		return "", func() {}, err
	}
	if _, err := snapshotDB.ExecContext(ctx, `vacuum`); err != nil {
		_ = snapshotDB.Close()
		cleanup()
		return "", func() {}, fmt.Errorf("compact cloud SQLite snapshot: %w", err)
	}
	if err := snapshotDB.Close(); err != nil {
		cleanup()
		return "", func() {}, fmt.Errorf("close cloud SQLite snapshot: %w", err)
	}
	return snapshotPath, cleanup, nil
}

func sanitizeCloudSQLiteSnapshot(ctx context.Context, db *sql.DB) error {
	for _, column := range []struct {
		table string
		name  string
	}{
		{table: "repositories", name: "raw_json"},
		{table: "threads", name: "raw_json"},
		{table: "comments", name: "raw_json"},
		{table: "comment_revisions", name: "raw_json"},
		{table: "pull_request_details", name: "raw_json"},
		{table: "pull_request_files", name: "raw_json"},
		{table: "pull_request_commits", name: "raw_json"},
		{table: "pull_request_checks", name: "raw_json"},
		{table: "pull_request_review_threads", name: "raw_json"},
		{table: "pull_request_review_threads", name: "comments_json"},
		{table: "pull_request_review_thread_revisions", name: "raw_json"},
		{table: "pull_request_review_thread_revisions", name: "comments_json"},
		{table: "github_workflow_runs", name: "raw_json"},
		{table: "sync_runs", name: "error_text"},
		{table: "sync_runs", name: "stats_json"},
		{table: "sync_attempt_failures", name: "error_message"},
		{table: "summary_runs", name: "error_text"},
		{table: "summary_runs", name: "stats_json"},
		{table: "embedding_runs", name: "error_text"},
		{table: "embedding_runs", name: "stats_json"},
		{table: "cluster_runs", name: "error_text"},
		{table: "cluster_runs", name: "stats_json"},
	} {
		exists, err := sqliteColumnExists(ctx, db, column.table, column.name)
		if err != nil {
			return fmt.Errorf("inspect cloud snapshot %s.%s: %w", column.table, column.name, err)
		}
		if !exists {
			continue
		}
		if _, err := db.ExecContext(
			ctx,
			`update `+column.table+` set `+column.name+` = '' where `+column.name+` is not null and `+column.name+` != ''`,
		); err != nil {
			return fmt.Errorf("clear cloud snapshot %s.%s: %w", column.table, column.name, err)
		}
	}
	for _, column := range []struct {
		table string
		name  string
	}{
		{table: "comments", name: "raw_json_blob_id"},
		{table: "thread_revisions", name: "raw_json_blob_id"},
	} {
		exists, err := sqliteColumnExists(ctx, db, column.table, column.name)
		if err != nil {
			return fmt.Errorf("inspect cloud snapshot %s.%s: %w", column.table, column.name, err)
		}
		if !exists {
			continue
		}
		if _, err := db.ExecContext(
			ctx,
			`update `+column.table+` set `+column.name+` = null where `+column.name+` is not null`,
		); err != nil {
			return fmt.Errorf("clear cloud snapshot %s.%s: %w", column.table, column.name, err)
		}
	}
	for _, table := range []string{
		"thread_changed_files",
		"thread_hunk_signatures",
		"thread_code_snapshots",
		"code_documents_fts",
		"code_documents",
		"code_snapshots",
	} {
		if _, err := db.ExecContext(ctx, `drop table if exists `+table); err != nil {
			return fmt.Errorf("drop local-only cloud snapshot table %s: %w", table, err)
		}
	}
	if exists, err := sqliteTableExists(ctx, db, "blobs"); err != nil {
		return err
	} else if exists {
		if _, err := db.ExecContext(ctx, `delete from blobs`); err != nil {
			return fmt.Errorf("clear cloud snapshot blobs: %w", err)
		}
	}
	if exists, err := sqliteTableExists(ctx, db, "portable_metadata"); err != nil {
		return err
	} else if exists {
		if _, err := db.ExecContext(
			ctx,
			`update portable_metadata set value = '' where key = 'source_path'`,
		); err != nil {
			return fmt.Errorf("clear cloud snapshot portable source path: %w", err)
		}
	}
	return nil
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
