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
	return Config{Database: filepath.Join(t.TempDir(), "metrics.sqlite"), Targets: []Target{{Entity: "OpenClaw", Target: "openclaw/openclaw"}, {Entity: "Hermes", Target: "NousResearch/hermes-agent"}}}
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
	event := Row{Type: "event", ID: "import-release", Entity: "Hermes", Target: "NousResearch/hermes-agent", Kind: "release", TS: "2026-09-14T00:00:00Z", ObservedAt: "2026-09-15T00:00:00Z", Provenance: "claw-track", Label: "v1", URL: "https://github.com/NousResearch/hermes-agent/releases/tag/v1"}
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
