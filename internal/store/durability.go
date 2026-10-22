package store

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"

	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// sqliteNotADatabase reports a file that SQLite cannot interpret. Such a
// connection cannot acknowledge any commit, so the hook leaves the original
// open/read error to the caller instead of masking it with a policy failure.
func sqliteNotADatabase(err error) bool {
	var sqliteErr *sqlite.Error
	return errors.As(err, &sqliteErr) && sqliteErr.Code()&0xff == sqlite3.SQLITE_NOTADB
}

// darwinFullFlush gates the Darwin F_FULLFSYNC flags. Production binaries
// always enable it. Go test binaries start with it disabled because a device
// flush on every disposable fixture commit made package suites about 30 times
// slower and exceed their deadlines; durability tests enable it explicitly.
// synchronous=FULL is applied in every binary.
var darwinFullFlush atomic.Bool

func init() {
	darwinFullFlush.Store(!testing.Testing())
	// Crawlkit's default DSN requests NORMAL. Apply the archive's durability
	// policy after DSN setup, including reconnects. Do not parse the DSN here:
	// SQLite accepts non-URL filenames and :memory:. This connection-local
	// setting never changes read-only/immutable access permissions.
	sqlite.RegisterConnectionHook(func(conn sqlite.ExecQuerierContext, _ string) error {
		rows, err := conn.QueryContext(context.Background(), "PRAGMA synchronous", nil)
		if err != nil {
			if sqliteNotADatabase(err) {
				return nil
			}
			return fmt.Errorf("read SQLite commit durability: %w", err)
		}
		values := make([]driver.Value, 1)
		err = rows.Next(values)
		closeErr := rows.Close()
		if err != nil {
			if sqliteNotADatabase(err) {
				return nil
			}
			return fmt.Errorf("read SQLite commit durability row: %w", err)
		}
		if closeErr != nil {
			return fmt.Errorf("close SQLite commit durability row: %w", closeErr)
		}
		if level, ok := values[0].(int64); !ok || level < 2 {
			if _, err := conn.ExecContext(context.Background(), "PRAGMA synchronous=FULL", nil); err != nil {
				return fmt.Errorf("configure SQLite commit durability: %w", err)
			}
		}
		// Darwin's VFS uses F_FULLFSYNC for these flags. Apply them even when
		// the DSN already requests FULL or EXTRA; they are independent settings.
		if runtime.GOOS == "darwin" && darwinFullFlush.Load() {
			for _, pragma := range []string{"PRAGMA fullfsync=ON", "PRAGMA checkpoint_fullfsync=ON"} {
				if _, err := conn.ExecContext(context.Background(), pragma, nil); err != nil {
					return fmt.Errorf("configure SQLite Darwin flush policy (%s): %w", pragma, err)
				}
			}
		}
		return nil
	})
}
