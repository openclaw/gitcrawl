package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"

	_ "modernc.org/sqlite"
)

const (
	schemaVersion = 1
	timeLayout    = time.RFC3339Nano
)

type Store struct {
	db   *sql.DB
	path string
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
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create db dir: %w", err)
	}
	if err := ensureDBFile(path); err != nil {
		return nil, err
	}
	dsn := fmt.Sprintf(
		"file:%s?_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=temp_store(MEMORY)&_pragma=mmap_size(268435456)&_pragma=busy_timeout(5000)",
		path,
	)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}
	if err := tightenDBFilePerms(path); err != nil {
		_ = db.Close()
		return nil, err
	}
	st := &Store{db: db, path: path}
	if err := st.migrate(ctx); err != nil {
		_ = db.Close()
		return nil, err
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
	if err := s.db.QueryRowContext(ctx, `select coalesce(max(finished_at), '') from sync_runs where status in ('success', 'completed')`).Scan(&lastSync); err != nil {
		return Status{}, fmt.Errorf("read last sync: %w", err)
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

func ensureDBFile(path string) error {
	if _, err := os.Stat(path); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("stat db file: %w", err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil && !os.IsExist(err) {
		return fmt.Errorf("create db file: %w", err)
	}
	if file != nil {
		if err := file.Close(); err != nil {
			return fmt.Errorf("close db file: %w", err)
		}
	}
	return nil
}

func tightenDBFilePerms(path string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	if err := os.Chmod(path, 0o600); err != nil {
		return fmt.Errorf("chmod db file: %w", err)
	}
	return nil
}
