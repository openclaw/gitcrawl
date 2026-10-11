package store

import (
	"context"
	"database/sql/driver"
	"fmt"
	"runtime"

	"modernc.org/sqlite"
)

func init() {
	// Crawlkit's default DSN requests NORMAL. Apply the archive's durability
	// policy after DSN setup, including reconnects. Do not parse the DSN here:
	// SQLite accepts non-URL filenames and :memory:. This connection-local
	// setting never changes read-only/immutable access permissions.
	sqlite.RegisterConnectionHook(func(conn sqlite.ExecQuerierContext, _ string) error {
		rows, err := conn.QueryContext(context.Background(), "PRAGMA synchronous", nil)
		if err != nil {
			return fmt.Errorf("read SQLite commit durability: %w", err)
		}
		values := make([]driver.Value, 1)
		err = rows.Next(values)
		closeErr := rows.Close()
		if err != nil {
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
		if runtime.GOOS == "darwin" {
			for _, pragma := range []string{"PRAGMA fullfsync=ON", "PRAGMA checkpoint_fullfsync=ON"} {
				if _, err := conn.ExecContext(context.Background(), pragma, nil); err != nil {
					return fmt.Errorf("configure SQLite Darwin flush policy (%s): %w", pragma, err)
				}
			}
		}
		return nil
	})
}
