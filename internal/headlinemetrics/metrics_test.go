package headlinemetrics

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/openclaw/crawlkit/store"
)

func testConfig(t *testing.T) Config {
	t.Helper()
	return Config{Database: filepath.Join(t.TempDir(), "metrics.sqlite"), Targets: []Target{{Entity: "OpenClaw", Target: "openclaw/openclaw"}, {Entity: "Example", Target: "example/project"}}}
}
func testRow() Row {
	return Counter(Target{"OpenClaw", "openclaw/openclaw"}, "stars", Value(12), "2026-09-15T01:00:00Z", "github_rest")
}
func openTestStore(t *testing.T, c Config) *store.Store {
	t.Helper()
	s, err := Open(context.Background(), c.Database)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}
func ndjson(t *testing.T, rows ...Row) string {
	t.Helper()
	var out strings.Builder
	for _, r := range rows {
		b, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		out.Write(b)
		out.WriteByte('\n')
	}
	return out.String()
}

func TestStorePreservesZerosNullsDecreasesAndIdempotentImports(t *testing.T) {
	ctx := context.Background()
	c := testConfig(t)
	s := openTestStore(t, c)
	rows := []Row{}
	for i, v := range []*float64{Value(12), Value(8), Value(0), nil} {
		r := testRow()
		r.ID = fmt.Sprint(i)
		r.Value = v
		rows = append(rows, r)
	}
	event := Row{Type: "event", ID: "import-release", Entity: "Example", Target: "example/project", Kind: "release", TS: "2026-09-14T00:00:00Z", ObservedAt: "2026-09-15T00:00:00Z", Provenance: "claw-track", Label: "v1", URL: "https://github.com/example/project/releases/tag/v1"}
	rows = append(rows, event)
	for _, want := range []int{5, 0} {
		n, err := Import(ctx, s, c, strings.NewReader(ndjson(t, rows...)))
		if err != nil || n != want {
			t.Fatalf("import = %d,%v want %d", n, err, want)
		}
	}
	cursor, err := s.DB().Query("SELECT value FROM metric_observations ORDER BY sequence")
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()
	for _, want := range []*float64{Value(12), Value(8), Value(0), nil} {
		if !cursor.Next() {
			t.Fatal("missing observation")
		}
		var got sql.NullFloat64
		if err := cursor.Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got.Valid != (want != nil) || (want != nil && got.Float64 != *want) {
			t.Fatalf("value = %+v want %v", got, want)
		}
	}
	if cursor.Next() {
		t.Fatal("duplicate observations")
	}
	result, err := Execute(ctx, "status", c, nil, nil)
	if err != nil || result.Observations != 4 || result.Events != 1 || result.LastObserved == nil {
		t.Fatalf("status = %+v %v", result, err)
	}
	info, err := os.Stat(c.Database)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm()&0077 != 0 {
		t.Fatalf("database permissions: %v", info.Mode())
	}
}

func TestImportRollbackAfterBatchBoundaryAndInvalidInput(t *testing.T) {
	for _, bad := range []string{"not json", `{"type":"metric"}`, "", strings.Repeat("x", 4*1024*1024+1)} {
		t.Run(fmt.Sprint(len(bad)), func(t *testing.T) {
			c := testConfig(t)
			s := openTestStore(t, c)
			var input strings.Builder
			for i := 0; i < 501; i++ {
				r := testRow()
				r.ID = fmt.Sprint(i)
				input.WriteString(ndjson(t, r))
			}
			input.WriteString(bad + "\n")
			n, err := Import(context.Background(), s, c, strings.NewReader(input.String()))
			if err == nil || n != 0 {
				t.Fatalf("import=%d,%v", n, err)
			}
			var count int
			if err := s.DB().QueryRow("SELECT count(*) FROM metric_observations").Scan(&count); err != nil || count != 0 {
				t.Fatalf("partial import committed: %d,%v", count, err)
			}
		})
	}
	c := testConfig(t)
	s := openTestStore(t, c)
	for _, mutate := range []func(*Row){func(r *Row) { r.Entity = "wrong" }, func(r *Row) { r.Target = "other/repo" }, func(r *Row) { r.ID = "" }, func(r *Row) { r.Value = Value(-1); r.TS = "invalid" }} {
		r := testRow()
		r.ID = "id"
		mutate(&r)
		if _, err := Import(context.Background(), s, c, strings.NewReader(ndjson(t, r))); err == nil {
			t.Fatalf("accepted invalid row %+v", r)
		}
	}
}

func TestDailyRevisionsAppendAndImportedIDsRemainIndependent(t *testing.T) {
	c := testConfig(t)
	s := openTestStore(t, c)
	ctx := context.Background()
	r := testRow()
	r.Kind = "daily"
	r.Metric = "clones"
	r.TS = "2026-09-14T23:59:59.999Z"
	r.Provenance = "github_traffic"
	for i, v := range []float64{10, 10, 9, 10} {
		r.Value = Value(v)
		r.ObservedAt = fmt.Sprintf("2026-09-15T%02d:00:00Z", i)
		n, err := Write(ctx, s, []Row{r})
		want := 1
		if i == 1 {
			want = 0
		}
		if err != nil || n != want {
			t.Fatalf("daily revision %d: %d,%v", i, n, err)
		}
	}
	r.ID = "historical-1"
	r.Provenance = "claw-track"
	n, err := Import(ctx, s, c, strings.NewReader(ndjson(t, r)))
	if err != nil || n != 1 {
		t.Fatalf("history: %d,%v", n, err)
	}
	r.ID = "historical-2"
	n, err = Import(ctx, s, c, strings.NewReader(ndjson(t, r)))
	if err != nil || n != 1 {
		t.Fatalf("distinct history ID: %d,%v", n, err)
	}
	var count int
	var latest float64
	if err = s.DB().QueryRow("SELECT count(*) FROM metric_observations").Scan(&count); err != nil || count != 5 {
		t.Fatalf("count %d %v", count, err)
	}
	if err = s.DB().QueryRow("SELECT value FROM metric_observations ORDER BY sequence DESC LIMIT 1").Scan(&latest); err != nil || latest != 10 {
		t.Fatalf("latest %f %v", latest, err)
	}
}

func TestRefuseArchiveWrongOwnerVersionAndLinksWithoutChangingBytes(t *testing.T) {
	for _, schema := range []string{
		"CREATE TABLE threads(id INTEGER PRIMARY KEY)",
		"CREATE TABLE metric_meta(key TEXT PRIMARY KEY,value TEXT);INSERT INTO metric_meta VALUES('owner','redcrawl'),('version','1')",
		"CREATE TABLE metric_meta(key TEXT PRIMARY KEY,value TEXT);INSERT INTO metric_meta VALUES('owner','gitcrawl'),('version','2')",
		"CREATE TABLE metric_meta(key TEXT PRIMARY KEY,value TEXT);INSERT INTO metric_meta VALUES('owner','gitcrawl'),('version','1');CREATE TABLE threads(id INTEGER)",
	} {
		t.Run(schema, func(t *testing.T) {
			c := testConfig(t)
			s, err := store.Open(context.Background(), store.Options{Path: c.Database, Schema: schema})
			if err != nil {
				t.Fatal(err)
			}
			s.Close()
			before, err := os.ReadFile(c.Database)
			if err != nil {
				t.Fatal(err)
			}
			if s, err := Open(context.Background(), c.Database); err == nil {
				s.Close()
				t.Fatal("archive accepted")
			}
			if _, err := Execute(context.Background(), "status", c, nil, nil); err == nil {
				t.Fatal("wrong schema status accepted")
			}
			after, _ := os.ReadFile(c.Database)
			if string(before) != string(after) {
				t.Fatal("archive bytes changed")
			}
		})
	}
	c := testConfig(t)
	s := openTestStore(t, c)
	s.Close()
	link := filepath.Join(t.TempDir(), "linked.db")
	if err := os.Symlink(c.Database, link); err != nil {
		t.Skip(err)
	}
	if s, err := Open(context.Background(), link); err == nil {
		s.Close()
		t.Fatal("database symlink accepted")
	}
	empty := filepath.Join(t.TempDir(), "empty.db")
	if err := os.WriteFile(empty, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if s, err := Open(context.Background(), empty); err == nil {
		s.Close()
		t.Fatal("empty existing database accepted")
	}
}

func TestValidationAndReadOnlyStatus(t *testing.T) {
	c := testConfig(t)
	if _, err := Execute(context.Background(), "status", c, nil, nil); err == nil {
		t.Fatal("missing status accepted")
	}
	if _, err := os.Stat(c.Database); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("status created database: %v", err)
	}
	for _, mutate := range []func(*Config){func(c *Config) { c.Database = "relative.db" }, func(c *Config) { c.Targets = nil }, func(c *Config) { c.Targets = append(c.Targets, c.Targets[0]) }, func(c *Config) { c.Targets[0].Target = "../archive" }, func(c *Config) { c.TokenEnv = "bad-name" }, func(c *Config) { c.CookieJar = "/unused" }} {
		cfg := testConfig(t)
		mutate(&cfg)
		if err := cfg.Validate(); err == nil {
			t.Fatalf("config accepted: %+v", cfg)
		}
	}
	for _, mutate := range []func(*Row){func(r *Row) { r.Type = "unknown" }, func(r *Row) { r.Entity = "" }, func(r *Row) { r.Provenance = "" }, func(r *Row) { r.TS = "bad" }, func(r *Row) { r.Kind = "gauge" }, func(r *Row) { r.Value = new(float64); *r.Value = -1 }, func(r *Row) { r.Value = new(float64); *r.Value = math.Inf(1) }, func(r *Row) { r.Kind = "daily" }, func(r *Row) { r.Type = "event"; r.Label = "" }} {
		r := testRow()
		mutate(&r)
		if err := Validate(r); err == nil {
			t.Fatalf("row accepted: %+v", r)
		}
	}
	for _, value := range []float64{-1, math.Inf(1), math.NaN()} {
		if Value(value) != nil {
			t.Fatal("invalid value accepted")
		}
	}
	for _, body := range []string{`{"database":"/tmp/test","targets":[]} invalid`, `{"database":"/tmp/test","targets":[],"unknown":true}`} {
		path := filepath.Join(t.TempDir(), "config.json")
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadConfig(path); err == nil {
			t.Fatal("invalid JSON config accepted")
		}
	}
}

func TestDatabaseWhitespaceCannotBypassArchiveOwnership(t *testing.T) {
	c := testConfig(t)
	if err := os.WriteFile(c.Database, []byte("archive bytes"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{" ", "\t", "\n"} {
		path := c.Database + suffix
		if s, err := Open(context.Background(), path); err == nil {
			s.Close()
			t.Fatal("whitespace path accepted")
		}
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("created alias path: %v", err)
		}
		cfg := c
		cfg.Database = path
		if err := cfg.Validate(); err == nil {
			t.Fatal("config accepted whitespace alias")
		}
	}
	if b, _ := os.ReadFile(c.Database); string(b) != "archive bytes" {
		t.Fatal("archive changed")
	}
	c.Targets[0].Target = "openclaw/.github"
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	c.Targets[0].Target = "openclaw/.."
	if err := c.Validate(); err == nil {
		t.Fatal("traversal target accepted")
	}
}

func TestCollectRetainsPartialAndCancelledResultsAtomically(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		t.Run(fmt.Sprint(cancelled), func(t *testing.T) {
			c := testConfig(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			collect := func(_ context.Context, _ Config, ts string) ([]Row, error) {
				if cancelled {
					cancel()
				}
				r := testRow()
				r.TS = ts
				r.ObservedAt = ts
				missing := r
				missing.Metric = "watchers"
				missing.Value = nil
				return []Row{r, missing}, errors.New("do not expose source credentials")
			}
			result, err := Execute(ctx, "collect", c, collect, nil)
			if err == nil || result.OK || result.RowsWritten != 2 || strings.Contains(err.Error(), "credentials") {
				t.Fatalf("result=%+v error=%v", result, err)
			}
			s, err := ownedReadOnly(context.Background(), c.Database)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			var status string
			var n int
			if err := s.DB().QueryRow("SELECT status,rows_written FROM metric_runs").Scan(&status, &n); err != nil || status != "partial" || n != 2 {
				t.Fatalf("run=%s,%d %v", status, n, err)
			}
		})
	}
	c := testConfig(t)
	result, err := Execute(context.Background(), "collect", c, func(context.Context, Config, string) ([]Row, error) {
		r := testRow()
		r.Target = "outside/scope"
		return []Row{r}, nil
	}, nil)
	if err == nil || result.RowsWritten != 0 {
		t.Fatalf("scope leak: %+v %v", result, err)
	}
	s := openTestStore(t, c)
	r := testRow()
	bad := r
	bad.Type = "invalid"
	n, err := Write(context.Background(), s, []Row{r, bad})
	if err == nil || n != 0 {
		t.Fatalf("write rollback=%d %v", n, err)
	}
}

func TestStatusOrdersExistingTimestampsChronologically(t *testing.T) {
	for _, times := range [][]string{
		{"2026-09-15T01:00:00+02:00", "2026-09-15T00:00:00Z"},
		{"2026-09-15T00:00:00Z", "2026-09-15T00:00:00.000000001Z"},
	} {
		c := testConfig(t)
		s := openTestStore(t, c)
		for i, at := range times {
			r := testRow()
			r.ID = fmt.Sprint(i)
			r.ObservedAt = at
			if _, err := Import(context.Background(), s, c, strings.NewReader(ndjson(t, r))); err != nil {
				t.Fatal(err)
			}
		}
		result, err := Execute(context.Background(), "status", c, nil, nil)
		if err != nil || result.LastObserved == nil || *result.LastObserved != times[1] {
			t.Fatalf("status = %+v, %v; latest instant = %s", result, err, times[1])
		}
	}
}

func TestFailedInitializationCanRetryWithoutAdoptingForeignFiles(t *testing.T) {
	c := testConfig(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if s, err := Open(ctx, c.Database); err == nil {
		s.Close()
		t.Fatal("canceled initialization succeeded")
	}
	if _, err := os.Stat(c.Database); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed initialization stranded a file: %v", err)
	}
	s := openTestStore(t, c)
	if _, err := Write(context.Background(), s, []Row{testRow()}); err != nil {
		t.Fatal(err)
	}
	s.Close()
	if s, err := Open(ctx, c.Database); err == nil {
		s.Close()
		t.Fatal("canceled reopen succeeded")
	}
	result, err := Execute(context.Background(), "status", c, nil, nil)
	if err != nil || result.Observations != 1 {
		t.Fatalf("existing history lost after failed reopen: %+v, %v", result, err)
	}
}

func TestRefuseMetricsDatabaseHardlinkAliases(t *testing.T) {
	c := testConfig(t)
	s := openTestStore(t, c)
	s.Close()
	alias := filepath.Join(t.TempDir(), "alias.sqlite")
	if err := os.Link(c.Database, alias); err != nil {
		t.Skip(err)
	}
	before, err := os.ReadFile(c.Database)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{c.Database, alias} {
		cfg := c
		cfg.Database = path
		if _, err := Execute(context.Background(), "import", cfg, nil, strings.NewReader("")); err == nil {
			t.Fatal("hardlinked database bypasses writer serialization")
		}
	}
	after, err := os.ReadFile(c.Database)
	if err != nil || string(after) != string(before) {
		t.Fatal("hardlinked database changed")
	}
}

func TestDailyCollectionUsesLatestImportedUTCDay(t *testing.T) {
	c := testConfig(t)
	s := openTestStore(t, c)
	ctx := context.Background()
	r := testRow()
	r.ID, r.Kind, r.Metric = "imported-day", "daily", "clones"
	r.TS, r.Provenance = "2026-09-15T01:00:00+02:00", "github_traffic"
	if _, err := Import(ctx, s, c, strings.NewReader(ndjson(t, r))); err != nil {
		t.Fatal(err)
	}
	r.ID, r.TS = "", "2026-09-14T23:59:59.999Z"
	for i, value := range []*float64{Value(12), nil, nil, Value(0), Value(12)} {
		r.Value = value
		r.ObservedAt = fmt.Sprintf("2026-09-15T%02d:00:00Z", i+2)
		n, err := Write(ctx, s, []Row{r})
		want := 1
		if i == 0 || i == 2 {
			want = 0
		}
		if err != nil || n != want {
			t.Fatalf("daily revision %d = %d, %v; want %d", i, n, err, want)
		}
	}
}

func TestConfigRejectsContentBeyondSizeLimit(t *testing.T) {
	c := testConfig(t)
	data, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "metrics.json")
	data = append(data, []byte(strings.Repeat(" ", 1024*1024)+`{"ignored":true}`)...)
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadConfig(path); err == nil {
		t.Fatal("oversized config with trailing JSON was accepted")
	}
}

func TestOpenRejectsReplacedDatabaseBeforeSchemaWrites(t *testing.T) {
	ctx := context.Background()
	for _, existing := range []bool{false, true} {
		for _, parent := range []bool{false, true} {
			for _, metrics := range []bool{false, true} {
				t.Run(fmt.Sprintf("existing=%t/parent=%t/metrics=%t", existing, parent, metrics), func(t *testing.T) {
					root := t.TempDir()
					active := filepath.Join(root, "active")
					if err := os.Mkdir(active, 0700); err != nil {
						t.Fatal(err)
					}
					path := filepath.Join(active, "metrics.sqlite")
					if existing {
						s, err := Open(ctx, path)
						if err != nil {
							t.Fatal(err)
						}
						if err := s.Close(); err != nil {
							t.Fatal(err)
						}
					}
					replacementDir := filepath.Join(root, "replacement")
					replacement := filepath.Join(replacementDir, "metrics.sqlite")
					schema := "CREATE TABLE threads(id INTEGER PRIMARY KEY,body TEXT); INSERT INTO threads VALUES(1,'archive retained')"
					if metrics {
						schema = Schema + "INSERT INTO metric_meta VALUES('owner','gitcrawl'),('version','1'); INSERT INTO metric_runs(ts,status,rows_written) VALUES('retained','ok',7)"
					}
					s, err := store.Open(ctx, store.Options{Path: replacement, Schema: schema})
					if err != nil {
						t.Fatal(err)
					}
					if err := s.Close(); err != nil {
						t.Fatal(err)
					}
					replacementInfo, err := os.Stat(replacement)
					if err != nil {
						t.Fatal(err)
					}
					called := false
					beforeWritableOpen = func() {
						called = true
						from, to := replacement, path
						if parent {
							from, to = replacementDir, active
						}
						if err := os.Rename(to, to+".original"); err != nil {
							t.Fatal(err)
						}
						if err := os.Rename(from, to); err != nil {
							t.Fatal(err)
						}
					}
					t.Cleanup(func() { beforeWritableOpen = nil })
					opened, err := Open(ctx, path)
					if opened != nil {
						opened.Close()
					}
					if !called || err == nil || opened != nil {
						t.Fatalf("replaced database accepted: called=%t store=%v error=%v", called, opened, err)
					}
					current, err := os.Stat(path)
					if err != nil || !os.SameFile(replacementInfo, current) {
						t.Fatalf("replacement removed or changed: %v", err)
					}
					read, err := store.OpenReadOnly(ctx, path)
					if err != nil {
						t.Fatal(err)
					}
					defer read.Close()
					if metrics {
						var retained int
						if err := read.DB().QueryRow("SELECT rows_written FROM metric_runs WHERE ts='retained'").Scan(&retained); err != nil || retained != 7 {
							t.Fatalf("replacement metrics history changed: %d, %v", retained, err)
						}
					} else {
						var objects int
						if err := read.DB().QueryRow("SELECT count(*) FROM sqlite_master WHERE name GLOB 'metric_*'").Scan(&objects); err != nil || objects != 0 {
							t.Fatalf("metrics schema landed in archive: %d, %v", objects, err)
						}
						var body string
						if err := read.DB().QueryRow("SELECT body FROM threads WHERE id=1").Scan(&body); err != nil || body != "archive retained" {
							t.Fatalf("archive contents changed: %q, %v", body, err)
						}
					}
				})
			}
		}
	}
}

func TestOpenRevalidatesDatabaseChangedInPlace(t *testing.T) {
	ctx := context.Background()
	for _, existing := range []bool{false, true} {
		t.Run(fmt.Sprint(existing), func(t *testing.T) {
			c := testConfig(t)
			if existing {
				s := openTestStore(t, c)
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
			}
			beforeWritableOpen = func() {
				schema := "CREATE VIEW unrelated AS SELECT 1"
				if existing {
					schema = "UPDATE metric_meta SET value='another-owner' WHERE key='owner'"
				}
				s, err := store.Open(ctx, store.Options{Path: c.Database, Schema: schema})
				if err != nil {
					t.Fatal(err)
				}
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
			}
			t.Cleanup(func() { beforeWritableOpen = nil })
			s, err := Open(ctx, c.Database)
			if s != nil {
				s.Close()
			}
			if err == nil || s != nil {
				t.Fatalf("changed database accepted: %v, %v", s, err)
			}
			if existing {
				read, err := store.OpenReadOnly(ctx, c.Database)
				if err != nil {
					t.Fatal(err)
				}
				defer read.Close()
				var owner string
				if err := read.DB().QueryRow("SELECT value FROM metric_meta WHERE key='owner'").Scan(&owner); err != nil || owner != "another-owner" {
					t.Fatalf("replaced ownership overwritten: %q, %v", owner, err)
				}
			}
		})
	}
}

func TestOpenRollsBackSchemaWhenOwnershipInsertFails(t *testing.T) {
	ctx := context.Background()
	c := testConfig(t)
	s, err := store.Open(ctx, store.Options{Path: c.Database, Schema: `
CREATE TABLE metric_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
INSERT INTO metric_meta VALUES('owner','gitcrawl'),('version','1');
CREATE TRIGGER reject_ownership BEFORE INSERT ON metric_meta BEGIN SELECT RAISE(ABORT,'fixture rejection'); END;
`})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if s, err := Open(ctx, c.Database); err == nil {
		s.Close()
		t.Fatal("ownership insert unexpectedly succeeded")
	}
	read, err := store.OpenReadOnly(ctx, c.Database)
	if err != nil {
		t.Fatal(err)
	}
	var tables int
	err = read.DB().QueryRow("SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN ('metric_observations','metric_events','metric_runs')").Scan(&tables)
	closeErr := read.Close()
	if err != nil || closeErr != nil || tables != 0 {
		t.Fatalf("schema was not rolled back: tables=%d error=%v close=%v", tables, err, closeErr)
	}
	// The failed open must release its transaction and connection for a retry.
	s, err = store.Open(ctx, store.Options{Path: c.Database, Schema: "DROP TRIGGER reject_ownership"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(ctx, c.Database)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}
