//go:build !windows

package portable

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"syscall"
)

// The caller owns the source exclusively and has already saved its recovery
// copy. After the rename, every failure discards the consumed staging file.
func consumeSQLite(ctx context.Context, sourcePath, targetPath string) error {
	source, err := os.Lstat(sourcePath)
	if err != nil {
		return fmt.Errorf("inspect consumed source: %w", err)
	}
	if !source.Mode().IsRegular() || source.Sys().(*syscall.Stat_t).Nlink != 1 {
		return fmt.Errorf("consume-source requires a regular file with exactly one link")
	}
	parent, err := os.Stat(filepath.Dir(targetPath))
	if err != nil {
		return fmt.Errorf("inspect consume-source staging filesystem: %w", err)
	}
	if source.Sys().(*syscall.Stat_t).Dev != parent.Sys().(*syscall.Stat_t).Dev {
		return fmt.Errorf("consume-source requires source and output on the same filesystem")
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if _, err := os.Lstat(sourcePath + suffix); !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("consume-source requires a closed database without SQLite sidecars")
		}
	}
	u := url.URL{Scheme: "file", Path: sourcePath}
	query := u.Query()
	query.Set("mode", "rw")
	query.Add("_pragma", "busy_timeout(0)")
	u.RawQuery = query.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return fmt.Errorf("open consumed source: %w", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	var mode string
	if err := db.QueryRowContext(ctx, `pragma journal_mode`).Scan(&mode); err != nil {
		return fmt.Errorf("read consumed source journal mode: %w", err)
	}
	if mode != "delete" {
		return fmt.Errorf("consume-source requires a checkpointed database in journal_mode=delete")
	}
	// A rollback-mode exclusive transaction rejects active readers and writers.
	// It makes no writes and holds the SQLite file lock through the rename.
	if _, err := db.ExecContext(ctx, `begin exclusive`); err != nil {
		return fmt.Errorf("lock consumed source exclusively: %w", err)
	}
	defer db.ExecContext(context.Background(), `rollback`)
	current, err := os.Lstat(sourcePath)
	if err != nil || !os.SameFile(source, current) || current.Sys().(*syscall.Stat_t).Nlink != 1 ||
		source.Size() != current.Size() || !source.ModTime().Equal(current.ModTime()) {
		return fmt.Errorf("consume-source file changed before handoff")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Rename is the ownership transfer. In particular, EXDEV must not copy.
	if err := os.Rename(sourcePath, targetPath); err != nil {
		return fmt.Errorf("move consumed source into export staging: %w", err)
	}
	return nil
}
