package portable

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/openclaw/gitcrawl/internal/store"
	moderncsqlite "modernc.org/sqlite"
)

type onlineBackupOptions struct {
	PagesPerStep int32
	AfterStep    func(remaining, pageCount int)
}

type backupCreator interface {
	NewBackup(string) (*moderncsqlite.Backup, error)
}

func configureDisposableStore(ctx context.Context, db *sql.DB) error {
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	var mode string
	if err := db.QueryRowContext(ctx, `pragma journal_mode = off`).Scan(&mode); err != nil {
		return fmt.Errorf("configure disposable journal mode: %w", err)
	}
	if !strings.EqualFold(strings.TrimSpace(mode), "off") {
		return fmt.Errorf("configure disposable journal mode: got %q, want off", mode)
	}
	// The working generation is private and deleted on any error, so it needs no
	// rollback journal. The mandatory compact generation restores privacy and is
	// fully validated, hashed, fsynced, and atomically committed for durability.
	if _, err := db.ExecContext(ctx, `pragma synchronous = off`); err != nil {
		return fmt.Errorf("configure disposable synchronous mode: %w", err)
	}
	if _, err := db.ExecContext(ctx, `pragma secure_delete = off`); err != nil {
		return fmt.Errorf("configure disposable secure-delete mode: %w", err)
	}
	if _, err := db.ExecContext(ctx, `pragma temp_store = memory`); err != nil {
		return fmt.Errorf("configure disposable temp store: %w", err)
	}
	var synchronous, secureDelete, tempStore int
	if err := db.QueryRowContext(ctx, `pragma synchronous`).Scan(&synchronous); err != nil {
		return fmt.Errorf("verify disposable synchronous mode: %w", err)
	}
	if err := db.QueryRowContext(ctx, `pragma secure_delete`).Scan(&secureDelete); err != nil {
		return fmt.Errorf("verify disposable secure-delete mode: %w", err)
	}
	if err := db.QueryRowContext(ctx, `pragma temp_store`).Scan(&tempStore); err != nil {
		return fmt.Errorf("verify disposable temp store: %w", err)
	}
	if synchronous != 0 || secureDelete != 0 || tempStore != 2 {
		return fmt.Errorf("configure disposable settings: synchronous=%d secure_delete=%d temp_store=%d, want 0/0/2", synchronous, secureDelete, tempStore)
	}
	return nil
}

func snapshotSQLite(ctx context.Context, sourcePath, targetPath string) error {
	return snapshotSQLiteWithOptions(ctx, sourcePath, targetPath, onlineBackupOptions{PagesPerStep: onlineBackupPageChunk})
}

func snapshotSQLiteWithOptions(ctx context.Context, sourcePath, targetPath string, options onlineBackupOptions) (retErr error) {
	if options.PagesPerStep <= 0 {
		return fmt.Errorf("online backup pages per step must be positive")
	}
	if _, err := os.Lstat(targetPath); err == nil {
		return fmt.Errorf("snapshot target already exists: %s", targetPath)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect snapshot target: %w", err)
	}
	abs, err := filepath.Abs(sourcePath)
	if err != nil {
		return fmt.Errorf("resolve snapshot source: %w", err)
	}
	if runtime.GOOS == "windows" {
		abs = filepath.ToSlash(abs)
		if filepath.VolumeName(abs) != "" && !strings.HasPrefix(abs, "/") {
			abs = "/" + abs
		}
	}
	u := url.URL{Scheme: "file", Path: abs}
	query := u.Query()
	query.Set("mode", "ro")
	query.Add("_pragma", "busy_timeout(5000)")
	u.RawQuery = query.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return fmt.Errorf("open source database for snapshot: %w", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("open source backup connection: %w", err)
	}
	defer conn.Close()
	complete := false
	defer func() {
		if complete {
			return
		}
		if err := os.Remove(targetPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			retErr = errors.Join(retErr, fmt.Errorf("remove partial snapshot: %w", err))
		}
		for _, suffix := range []string{"-wal", "-shm"} {
			if err := os.Remove(targetPath + suffix); err != nil && !errors.Is(err, os.ErrNotExist) {
				retErr = errors.Join(retErr, fmt.Errorf("remove partial snapshot sidecar: %w", err))
			}
		}
	}()
	if err := conn.Raw(func(driverConn any) (rawErr error) {
		creator, ok := driverConn.(backupCreator)
		if !ok {
			return fmt.Errorf("SQLite driver connection does not support online backup")
		}
		backup, err := creator.NewBackup(targetPath)
		if err != nil {
			return fmt.Errorf("start online backup: %w", err)
		}
		finished := false
		defer func() {
			if finished {
				return
			}
			if err := backup.Finish(); err != nil {
				rawErr = errors.Join(rawErr, fmt.Errorf("finish online backup after failure: %w", err))
			}
		}()
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			more, err := backup.Step(options.PagesPerStep)
			if err != nil {
				if store.IsTransientSQLiteBusy(err) {
					timer := time.NewTimer(onlineBackupBusyRetry)
					select {
					case <-ctx.Done():
						timer.Stop()
						return ctx.Err()
					case <-timer.C:
					}
					continue
				}
				return fmt.Errorf("step online backup: %w", err)
			}
			if options.AfterStep != nil {
				options.AfterStep(backup.Remaining(), backup.PageCount())
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if !more {
				break
			}
		}
		if err := backup.Finish(); err != nil {
			finished = true
			return fmt.Errorf("finish online backup: %w", err)
		}
		finished = true
		return nil
	}); err != nil {
		return fmt.Errorf("snapshot source database: %w", err)
	}
	if _, err := os.Stat(targetPath); err != nil {
		return fmt.Errorf("stat completed snapshot: %w", err)
	}
	complete = true
	return nil
}

func ordinaryNonUniqueIndexes(ctx context.Context, db *sql.DB) ([]string, error) {
	tables, err := databaseTableNames(ctx, db)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]struct{})
	for _, table := range tables {
		rows, err := db.QueryContext(ctx, `pragma index_list(`+quoteIdentifier(table)+`)`)
		if err != nil {
			return nil, fmt.Errorf("list indexes for %s: %w", table, err)
		}
		for rows.Next() {
			var sequence, unique, partial int
			var name, origin string
			if err := rows.Scan(&sequence, &name, &unique, &origin, &partial); err != nil {
				_ = rows.Close()
				return nil, fmt.Errorf("scan indexes for %s: %w", table, err)
			}
			if origin == "c" && unique == 0 {
				seen[name] = struct{}{}
			}
		}
		if err := rows.Close(); err != nil {
			return nil, fmt.Errorf("close indexes for %s: %w", table, err)
		}
	}
	indexes := make([]string, 0, len(seen))
	for name := range seen {
		indexes = append(indexes, name)
	}
	sort.Strings(indexes)
	return indexes, nil
}

func databaseTableNames(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx, `select name from sqlite_schema where type = 'table' and name not like 'sqlite_%' order by name`)
	if err != nil {
		return nil, fmt.Errorf("list portable tables: %w", err)
	}
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan portable table: %w", err)
		}
		tables = append(tables, name)
	}
	return tables, rows.Err()
}

func databaseTableStats(ctx context.Context, db *sql.DB) ([]Table, error) {
	names, err := databaseTableNames(ctx, db)
	if err != nil {
		return nil, err
	}
	tables := make([]Table, 0, len(names))
	for _, name := range names {
		var rows int64
		if err := db.QueryRowContext(ctx, `select count(*) from `+quoteIdentifier(name)).Scan(&rows); err != nil {
			return nil, fmt.Errorf("count portable table %s: %w", name, err)
		}
		tables = append(tables, Table{Name: name, Rows: rows})
	}
	return tables, nil
}

func repositoryFromStore(repo store.Repository) *Repository {
	return &Repository{ID: repo.ID, Owner: repo.Owner, Name: repo.Name, FullName: repo.FullName}
}

func singleRepository(ctx context.Context, db *sql.DB) (*Repository, error) {
	var count int64
	if err := db.QueryRowContext(ctx, `select count(*) from repositories`).Scan(&count); err != nil {
		return nil, fmt.Errorf("count portable repositories: %w", err)
	}
	if count != 1 {
		return nil, nil
	}
	var repo Repository
	if err := db.QueryRowContext(ctx, `select id, owner, name, full_name from repositories`).Scan(&repo.ID, &repo.Owner, &repo.Name, &repo.FullName); err != nil {
		return nil, fmt.Errorf("read portable repository metadata: %w", err)
	}
	return &repo, nil
}

func verifyRepository(ctx context.Context, db *sql.DB, expected Repository) error {
	actual, err := singleRepository(ctx, db)
	if err != nil {
		return err
	}
	if actual == nil || *actual != expected {
		return fmt.Errorf("portable repository restriction did not preserve exactly %s", expected.FullName)
	}
	return nil
}

func quoteIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func writeMetadata(ctx context.Context, db *sql.DB, metadata map[string]string) error {
	keys := make([]string, 0, len(metadata))
	for key := range metadata {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if _, err := db.ExecContext(ctx, `
			insert into portable_metadata(key, value) values(?, ?)
			on conflict(key) do update set value = excluded.value
		`, key, metadata[key]); err != nil {
			return fmt.Errorf("write portable metadata %s: %w", key, err)
		}
	}
	return nil
}

func createCompactDatabase(ctx context.Context, db *sql.DB, workingPath string) (_ string, retErr error) {
	placeholder, err := os.CreateTemp(filepath.Dir(workingPath), "."+filepath.Base(workingPath)+".compact-*")
	if err != nil {
		return "", fmt.Errorf("reserve compact portable database path: %w", err)
	}
	compactPath := placeholder.Name()
	if err := placeholder.Close(); err != nil {
		_ = os.Remove(compactPath)
		return "", fmt.Errorf("close compact portable database placeholder: %w", err)
	}
	if err := os.Remove(compactPath); err != nil {
		return "", fmt.Errorf("prepare compact portable database path: %w", err)
	}
	complete := false
	defer func() {
		if complete {
			return
		}
		if err := os.Remove(compactPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			retErr = errors.Join(retErr, fmt.Errorf("remove partial compact database: %w", err))
		}
		for _, suffix := range []string{"-wal", "-shm"} {
			if err := os.Remove(compactPath + suffix); err != nil && !errors.Is(err, os.ErrNotExist) {
				retErr = errors.Join(retErr, fmt.Errorf("remove partial compact sidecar: %w", err))
			}
		}
	}()
	if _, err := db.ExecContext(ctx, `vacuum into ?`, compactPath); err != nil {
		return "", fmt.Errorf("create compact portable database: %w", err)
	}
	if _, err := os.Stat(compactPath); err != nil {
		return "", fmt.Errorf("stat compact portable database: %w", err)
	}
	complete = true
	return compactPath, nil
}

func replaceWithCompactDatabase(ctx context.Context, workingPath, compactPath string) error {
	db, err := sql.Open("sqlite", compactPath)
	if err != nil {
		return fmt.Errorf("open compact database candidate: %w", err)
	}
	db.SetMaxOpenConns(1)
	quickCheck, checkErr := checkPragma(ctx, db, "quick_check")
	closeErr := db.Close()
	if checkErr != nil {
		return fmt.Errorf("validate compact database candidate: %w", checkErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close compact database candidate: %w", closeErr)
	}
	if quickCheck != "ok" {
		return fmt.Errorf("compact database candidate quick_check = %q", quickCheck)
	}
	if err := removeSQLiteSidecars(compactPath); err != nil {
		return err
	}
	if err := removeSQLiteSidecars(workingPath); err != nil {
		return err
	}
	if err := os.Remove(workingPath); err != nil {
		return fmt.Errorf("remove uncompact portable database: %w", err)
	}
	if err := os.Rename(compactPath, workingPath); err != nil {
		return fmt.Errorf("promote compact portable database: %w", err)
	}
	return nil
}

func checkPragma(ctx context.Context, db *sql.DB, pragma string) (string, error) {
	rows, err := db.QueryContext(ctx, `pragma `+pragma)
	if err != nil {
		return "", fmt.Errorf("run SQLite %s: %w", pragma, err)
	}
	defer rows.Close()
	var messages []string
	for rows.Next() {
		var message string
		if err := rows.Scan(&message); err != nil {
			return "", fmt.Errorf("scan SQLite %s: %w", pragma, err)
		}
		messages = append(messages, strings.TrimSpace(message))
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("read SQLite %s: %w", pragma, err)
	}
	return strings.Join(messages, "; "), nil
}
