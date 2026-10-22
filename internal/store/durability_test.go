package store

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestWriterDurabilitySurvivesConnectionReplacement(t *testing.T) {
	enableDarwinFullFlush(t)
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "durable.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	check := func() {
		t.Helper()
		var sync int
		if err := st.DB().QueryRowContext(ctx, "PRAGMA synchronous").Scan(&sync); err != nil || sync != 2 {
			t.Fatalf("writer synchronous=%d, err=%v; want FULL", sync, err)
		}
		checkDarwinFlushPolicy(t, st.DB())
	}
	check()
	// Closing idle connections forces a new connection through the original
	// Crawlkit NORMAL DSN, so a one-time PRAGMA after Open cannot pass this test.
	st.DB().SetMaxIdleConns(0)
	check()
	check()
}

func TestDurabilityPreservesReadOnlyBoundary(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "readonly.db")
	st, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	reader, err := OpenReadOnly(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if _, err := reader.DB().ExecContext(ctx, "CREATE TABLE forbidden(id INTEGER)"); err == nil {
		t.Fatal("read-only connection accepted a write")
	}
	// Other native SQLite writers also use the registered driver. Their DSN
	// cannot accidentally weaken this policy after a connection is opened.
	db, err := sql.Open("sqlite", "file:"+path+"?mode=rw&_pragma=synchronous(NORMAL)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var sync int
	if err := db.QueryRowContext(ctx, "PRAGMA synchronous").Scan(&sync); err != nil || sync != 2 {
		t.Fatalf("direct writer synchronous=%d, err=%v", sync, err)
	}
}

func TestDurabilityAcceptsSQLiteDSNsAndPreservesAccess(t *testing.T) {
	enableDarwinFullFlush(t)
	for _, tc := range []struct {
		name, dsn string
		want      int
		readOnly  bool
	}{
		{"memory", ":memory:", 2, false},
		{"plain-query", filepath.Join(t.TempDir(), "plain.db") + "?mode=ro&_pragma=synchronous(NORMAL)", 2, false},
		{"extra", "file:" + filepath.Join(t.TempDir(), "extra.db") + "?_pragma=synchronous(EXTRA)", 3, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, err := sql.Open("sqlite", tc.dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			var level int
			if err = db.QueryRow("PRAGMA synchronous").Scan(&level); err != nil || level != tc.want {
				t.Fatalf("level=%d err=%v", level, err)
			}
			checkDarwinFlushPolicy(t, db)
			if _, err = db.Exec("CREATE TABLE writable(id INTEGER)"); err != nil {
				t.Fatal(err)
			}
		})
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "source.db")
	writer, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = writer.Exec("CREATE TABLE keep(id INTEGER)"); err != nil {
		t.Fatal(err)
	}
	writer.Close()
	for _, suffix := range []string{"mode=ro", "immutable=1", "immutable=true", "immutable=on", "immutable=yes", "immutable=2"} {
		t.Run(suffix, func(t *testing.T) {
			db, err := sql.Open("sqlite", "file:"+path+"?"+suffix+"&_pragma=synchronous(NORMAL)")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if err = db.Ping(); err != nil {
				t.Fatal(err)
			}
			checkDarwinFlushPolicy(t, db)
			if _, err = db.Exec("INSERT INTO keep VALUES(1)"); err == nil {
				t.Fatal("read-only access weakened")
			}
		})
	}
}

// enableDarwinFullFlush restores the production Darwin flush policy for the
// calling test; test binaries otherwise skip per-commit F_FULLFSYNC.
func enableDarwinFullFlush(t *testing.T) {
	t.Helper()
	previous := darwinFullFlush.Swap(true)
	t.Cleanup(func() { darwinFullFlush.Store(previous) })
}

func checkDarwinFlushPolicy(t *testing.T, db *sql.DB) {
	t.Helper()
	if runtime.GOOS != "darwin" {
		return
	}
	for _, pragma := range []string{"fullfsync", "checkpoint_fullfsync"} {
		var value int
		if err := db.QueryRow("PRAGMA " + pragma).Scan(&value); err != nil || value != 1 {
			t.Fatalf("%s=%d err=%v; want ON", pragma, value, err)
		}
	}
}

func TestDarwinFlushPolicyOverridesDSNOnReplacement(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("Darwin flush policy")
	}
	enableDarwinFullFlush(t)
	for _, sync := range []string{"NORMAL", "FULL", "EXTRA"} {
		t.Run(sync, func(t *testing.T) {
			db, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "writer.db")+
				"?_pragma=journal_mode(WAL)&_pragma=synchronous("+sync+")&_pragma=fullfsync(OFF)&_pragma=checkpoint_fullfsync(OFF)")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			for i := 0; i < 2; i++ {
				checkDarwinFlushPolicy(t, db)
				var got int
				want := 2
				if sync == "EXTRA" {
					want = 3
				}
				if err := db.QueryRow("PRAGMA synchronous").Scan(&got); err != nil || got != want {
					t.Fatalf("synchronous=%d err=%v; want %d", got, err, want)
				}
				if _, err := db.Exec("CREATE TABLE IF NOT EXISTS retained(id INTEGER); INSERT INTO retained VALUES(1)"); err != nil {
					t.Fatal(err)
				}
				var busy, frames, checkpointed int
				if err := db.QueryRow("PRAGMA wal_checkpoint(FULL)").Scan(&busy, &frames, &checkpointed); err != nil || busy != 0 {
					t.Fatalf("checkpoint busy=%d err=%v", busy, err)
				}
				db.SetMaxIdleConns(0)
			}
		})
	}
}

func TestDurabilityHookLeavesNonDatabaseErrorsToCaller(t *testing.T) {
	path := filepath.Join(t.TempDir(), "not-sqlite.db")
	if err := os.WriteFile(path, []byte("not a SQLite database at all, padded beyond one header"), 0o600); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", "file:"+path+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// Callers see SQLite's own error rather than a durability policy failure
	// raised by the connection hook.
	_, err = db.Exec("SELECT count(*) FROM sqlite_schema")
	if err == nil || strings.Contains(err.Error(), "durability") || !strings.Contains(err.Error(), "not a database") {
		t.Fatalf("query error = %v, want native not-a-database error", err)
	}
}

func TestDarwinFullFlushIsProductionDefault(t *testing.T) {
	// Test binaries opt out for speed; every non-test binary enables the flags.
	if darwinFullFlush.Load() != !testing.Testing() {
		t.Fatalf("darwinFullFlush=%v, want %v", darwinFullFlush.Load(), !testing.Testing())
	}
}
