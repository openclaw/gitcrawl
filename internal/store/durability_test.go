package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
)

func TestWriterDurabilitySurvivesConnectionReplacement(t *testing.T) {
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
			if _, err = db.Exec("INSERT INTO keep VALUES(1)"); err == nil {
				t.Fatal("read-only access weakened")
			}
		})
	}
}
