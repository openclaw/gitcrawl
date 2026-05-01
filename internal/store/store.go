package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/vincentkoc/crawlkit/sqlitekit"
)

const (
	schemaVersion = 1
	timeLayout    = time.RFC3339Nano
)

type Store struct {
	db      *sql.DB
	queries dbQueries
	path    string
}

type dbQueries interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type Status struct {
	DBPath          string    `json:"db_path"`
	RepositoryCount int       `json:"repository_count"`
	ThreadCount     int       `json:"thread_count"`
	OpenThreadCount int       `json:"open_thread_count"`
	ClusterCount    int       `json:"cluster_count"`
	LastSyncAt      time.Time `json:"last_sync_at,omitempty"`
}

func Open(ctx context.Context, path string) (*Store, error) {
	base, err := sqlitekit.Open(ctx, sqlitekit.Options{Path: path})
	if err != nil {
		return nil, err
	}
	db := base.DB()
	st := &Store{db: db, path: path}
	if err := st.migrate(ctx); err != nil {
		_ = base.Close()
		return nil, err
	}
	return st, nil
}

func OpenReadOnly(ctx context.Context, path string) (*Store, error) {
	base, err := sqlitekit.OpenReadOnly(ctx, path)
	if err != nil {
		return nil, err
	}
	db := base.DB()
	st := &Store{db: db, path: path}
	current, err := st.schemaVersion(ctx)
	if err != nil {
		_ = base.Close()
		return nil, err
	}
	if current > schemaVersion {
		_ = base.Close()
		return nil, fmt.Errorf("database schema version %d is newer than supported version %d", current, schemaVersion)
	}
	return st, nil
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) DB() *sql.DB {
	return s.db
}

func (s *Store) Path() string {
	return s.path
}

func (s *Store) q() dbQueries {
	if s.queries != nil {
		return s.queries
	}
	return s.db
}

func (s *Store) WithTx(ctx context.Context, fn func(*Store) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	txStore := &Store{db: s.db, queries: tx, path: s.path}
	if err := fn(txStore); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}

func (s *Store) Status(ctx context.Context) (Status, error) {
	status := Status{DBPath: s.path}
	if err := s.db.QueryRowContext(ctx, `select count(*) from repositories`).Scan(&status.RepositoryCount); err != nil {
		return Status{}, fmt.Errorf("count repositories: %w", err)
	}
	if err := s.db.QueryRowContext(ctx, `select count(*) from threads`).Scan(&status.ThreadCount); err != nil {
		return Status{}, fmt.Errorf("count threads: %w", err)
	}
	if err := s.db.QueryRowContext(ctx, `select count(*) from threads where state = 'open' and closed_at_local is null`).Scan(&status.OpenThreadCount); err != nil {
		return Status{}, fmt.Errorf("count open threads: %w", err)
	}
	if err := s.db.QueryRowContext(ctx, `select count(*) from cluster_groups`).Scan(&status.ClusterCount); err != nil {
		return Status{}, fmt.Errorf("count clusters: %w", err)
	}
	var lastSync string
	if s.hasTable(ctx, "sync_runs") {
		if err := s.db.QueryRowContext(ctx, `select coalesce(max(finished_at), '') from sync_runs where status in ('success', 'completed')`).Scan(&lastSync); err != nil {
			return Status{}, fmt.Errorf("read last sync: %w", err)
		}
	}
	if lastSync == "" && s.hasTable(ctx, "portable_metadata") {
		if err := s.db.QueryRowContext(ctx, `select value from portable_metadata where key = 'exported_at'`).Scan(&lastSync); err != nil && err != sql.ErrNoRows {
			return Status{}, fmt.Errorf("read portable exported timestamp: %w", err)
		}
	}
	if lastSync == "" && s.hasTable(ctx, "repo_sync_state") {
		if err := s.db.QueryRowContext(ctx, `
			select coalesce(
				max(last_open_close_reconciled_at),
				max(last_overlapping_open_scan_completed_at),
				max(last_non_overlapping_scan_completed_at),
				max(last_full_open_scan_started_at),
				max(updated_at),
				''
			)
			from repo_sync_state
		`).Scan(&lastSync); err != nil {
			return Status{}, fmt.Errorf("read portable sync state: %w", err)
		}
	}
	if lastSync != "" {
		parsed, err := time.Parse(timeLayout, lastSync)
		if err == nil {
			status.LastSyncAt = parsed
		}
	}
	return status, nil
}

func (s *Store) migrate(ctx context.Context) error {
	current, err := s.schemaVersion(ctx)
	if err != nil {
		return err
	}
	if current > schemaVersion {
		return fmt.Errorf("database schema version %d is newer than supported version %d", current, schemaVersion)
	}
	if _, err := s.db.ExecContext(ctx, schemaSQL); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	if err := s.ensureLegacyPortableColumns(ctx); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf(`pragma user_version = %d`, schemaVersion)); err != nil {
		return fmt.Errorf("set schema version: %w", err)
	}
	return nil
}

func (s *Store) ensureLegacyPortableColumns(ctx context.Context) error {
	if err := s.ensureColumn(ctx, "repositories", "raw_json", "text"); err != nil {
		return err
	}
	hadThreadBody := s.hasColumn(ctx, "threads", "body")
	if err := s.ensureColumn(ctx, "threads", "body", "text"); err != nil {
		return err
	}
	if err := s.ensureColumn(ctx, "threads", "raw_json", "text"); err != nil {
		return err
	}
	if !hadThreadBody && s.hasColumn(ctx, "threads", "body_excerpt") {
		if _, err := s.db.ExecContext(ctx, `update threads set body = body_excerpt where body is null and body_excerpt is not null`); err != nil {
			return fmt.Errorf("backfill thread body from portable excerpt: %w", err)
		}
	}
	return nil
}

func (s *Store) hasTable(ctx context.Context, table string) bool {
	var name string
	err := s.db.QueryRowContext(ctx, `select name from sqlite_schema where type in ('table', 'virtual table') and name = ?`, table).Scan(&name)
	return err == nil
}

func (s *Store) ensureColumn(ctx context.Context, table, column, definition string) error {
	if s.hasColumn(ctx, table, column) {
		return nil
	}
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf(`alter table %s add column %s %s`, table, column, definition)); err != nil {
		return fmt.Errorf("add %s.%s: %w", table, column, err)
	}
	return nil
}

func (s *Store) hasColumn(ctx context.Context, table, column string) bool {
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`pragma table_info(%s)`, table))
	if err != nil {
		return false
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, typ string
		var notNull int
		var defaultValue sql.NullString
		var primaryKey int
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &primaryKey); err != nil {
			return false
		}
		if name == column {
			return true
		}
	}
	return false
}

func (s *Store) schemaVersion(ctx context.Context) (int, error) {
	var version int
	if err := s.db.QueryRowContext(ctx, `pragma user_version`).Scan(&version); err != nil {
		return 0, fmt.Errorf("read schema version: %w", err)
	}
	return version, nil
}
