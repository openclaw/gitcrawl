// Package headlinemetrics owns a separate, append-only repository metrics store.
// It never opens Gitcrawl's thread archive or starts embedding/model work.
package headlinemetrics

import (
	"bufio"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/openclaw/crawlkit/store"
)

const Owner = "gitcrawl"

type Target struct {
	Entity string `json:"entity"`
	Target string `json:"target"`
}
type Config struct {
	Database  string   `json:"database"`
	Targets   []Target `json:"targets"`
	CookieJar string   `json:"cookieJar,omitempty"`
	TokenEnv  string   `json:"tokenEnv,omitempty"`
}
type Row struct {
	Type       string   `json:"type"`
	ID         string   `json:"id,omitempty"`
	Entity     string   `json:"entity"`
	Target     string   `json:"target"`
	Metric     string   `json:"metric,omitempty"`
	Kind       string   `json:"kind"`
	TS         string   `json:"ts"`
	Value      *float64 `json:"value"`
	ObservedAt string   `json:"observed_at"`
	Provenance string   `json:"provenance"`
	Label      string   `json:"label,omitempty"`
	URL        string   `json:"url,omitempty"`
}
type Collector func(context.Context, Config, string) ([]Row, error)

type Result struct {
	Source       string  `json:"source"`
	Command      string  `json:"command"`
	RowsWritten  int     `json:"rows_written"`
	OK           bool    `json:"ok"`
	Observations int     `json:"observations,omitempty"`
	Events       int     `json:"events,omitempty"`
	LastObserved *string `json:"last_observed,omitempty"`
}

var repositoryPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9-]*/[A-Za-z0-9_.-]+$`)
var envPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func ReadConfig(path string) (Config, error) {
	var c Config
	f, err := os.Open(path)
	if err != nil {
		return c, fmt.Errorf("read metrics config: %w", err)
	}
	defer f.Close()
	d := json.NewDecoder(io.LimitReader(f, 1024*1024))
	d.DisallowUnknownFields()
	if err := d.Decode(&c); err != nil {
		return c, errors.New("invalid metrics config JSON")
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return c, errors.New("metrics config must contain one JSON object")
	}
	return c, c.Validate()
}

func (c Config) Validate() error {
	if !filepath.IsAbs(c.Database) || strings.TrimSpace(c.Database) != c.Database || strings.ContainsAny(c.Database, "\x00?") {
		return errors.New("metrics database must be an absolute filesystem path")
	}
	if len(c.Targets) == 0 {
		return errors.New("metrics config requires targets")
	}
	if c.TokenEnv != "" && !envPattern.MatchString(c.TokenEnv) {
		return errors.New("invalid tokenEnv name")
	}
	if c.CookieJar != "" {
		return errors.New("GitHub metrics do not use cookieJar")
	}
	seen := map[string]bool{}
	for _, t := range c.Targets {
		if strings.TrimSpace(t.Entity) == "" || len(t.Entity) > 200 || len(t.Target) > 300 || !validRepository(t.Target) {
			return errors.New("metrics target requires an entity and owner/repo")
		}
		key := strings.ToLower(t.Target)
		if seen[key] {
			return errors.New("duplicate metrics repository target")
		}
		seen[key] = true
	}
	return nil
}

func validRepository(target string) bool {
	if !repositoryPattern.MatchString(target) {
		return false
	}
	_, name, _ := strings.Cut(target, "/")
	return name != "." && name != ".."
}

func Value(n float64) *float64 {
	if math.IsNaN(n) || math.IsInf(n, 0) || n < 0 {
		return nil
	}
	return &n
}
func Counter(t Target, metric string, value *float64, ts, basis string) Row {
	return Row{Type: "metric", Entity: t.Entity, Target: t.Target, Metric: metric, Kind: "counter", TS: ts, Value: value, ObservedAt: ts, Provenance: basis}
}

const Schema = `
CREATE TABLE IF NOT EXISTS metric_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS metric_observations(sequence INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT NOT NULL UNIQUE,entity TEXT NOT NULL,target TEXT NOT NULL,metric TEXT NOT NULL,kind TEXT NOT NULL CHECK(kind IN ('counter','daily')),ts TEXT NOT NULL,value REAL,observed_at TEXT NOT NULL,provenance TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS metric_series ON metric_observations(target,metric,ts,sequence);
CREATE TABLE IF NOT EXISTS metric_events(sequence INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT NOT NULL UNIQUE,entity TEXT NOT NULL,target TEXT NOT NULL,kind TEXT NOT NULL,ts TEXT NOT NULL,label TEXT NOT NULL,url TEXT NOT NULL,observed_at TEXT NOT NULL,provenance TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS metric_runs(sequence INTEGER PRIMARY KEY AUTOINCREMENT,ts TEXT NOT NULL,status TEXT NOT NULL,rows_written INTEGER NOT NULL);
`

// ownedReadOnly checks identity and schema version before any writable SQLite open.
func ownedReadOnly(ctx context.Context, path string) (*store.Store, error) {
	if !filepath.IsAbs(path) || strings.TrimSpace(path) != path {
		return nil, errors.New("metrics database must be an absolute path without surrounding whitespace")
	}
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() == 0 {
		return nil, errors.New("metrics database must be a nonempty regular file")
	}
	s, err := store.OpenReadOnly(ctx, path)
	if err != nil {
		return nil, err
	}
	var owner, version string
	err = s.DB().QueryRowContext(ctx, "SELECT value FROM metric_meta WHERE key='owner'").Scan(&owner)
	if err == nil {
		err = s.DB().QueryRowContext(ctx, "SELECT value FROM metric_meta WHERE key='version'").Scan(&version)
	}
	var foreign int
	if err == nil {
		err = s.DB().QueryRowContext(ctx, `SELECT count(*) FROM sqlite_master WHERE type='table' AND name NOT IN ('metric_meta','metric_observations','metric_events','metric_runs','sqlite_sequence') AND name NOT LIKE 'sqlite_%'`).Scan(&foreign)
	}
	if err != nil || owner != Owner || version != "1" || foreign != 0 {
		s.Close()
		return nil, errors.New("refusing a database not exclusively owned by gitcrawl metrics schema version 1")
	}
	return s, nil
}

func Open(ctx context.Context, path string) (*store.Store, error) {
	if !filepath.IsAbs(path) || strings.TrimSpace(path) != path {
		return nil, errors.New("metrics database path must be absolute")
	}
	info, err := os.Lstat(path)
	if err == nil {
		if !info.Mode().IsRegular() {
			return nil, errors.New("refusing a non-regular metrics database")
		}
		read, err := ownedReadOnly(ctx, path)
		if err != nil {
			return nil, err
		}
		if err = read.Close(); err != nil {
			return nil, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	} else {
		if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			return nil, err
		}
		// Never initialize an existing empty archive or follow a database symlink.
		f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return nil, err
		}
		if err = f.Close(); err != nil {
			return nil, err
		}
	}
	s, err := store.Open(ctx, store.Options{Path: path, Schema: Schema, MaxOpenConns: 1, MaxIdleConns: 1})
	if err != nil {
		return nil, err
	}
	_, err = s.DB().ExecContext(ctx, "INSERT OR IGNORE INTO metric_meta VALUES('owner',?),('version','1')", Owner)
	if err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

func Validate(r Row) error {
	if r.Type != "metric" && r.Type != "event" {
		return errors.New("invalid observation type")
	}
	if strings.TrimSpace(r.Entity) == "" || !validRepository(r.Target) || len(r.Entity) > 200 || len(r.Target) > 300 || strings.TrimSpace(r.Provenance) == "" {
		return errors.New("invalid observation identity")
	}
	for _, v := range []string{r.TS, r.ObservedAt} {
		if _, err := time.Parse(time.RFC3339Nano, v); err != nil {
			return errors.New("invalid observation time")
		}
	}
	if r.Type == "metric" {
		if r.Metric == "" || (r.Kind != "counter" && r.Kind != "daily") || (r.Value != nil && Value(*r.Value) == nil) {
			return errors.New("invalid metric")
		}
		if r.Kind == "daily" {
			at, _ := time.Parse(time.RFC3339Nano, r.TS)
			observed, _ := time.Parse(time.RFC3339Nano, r.ObservedAt)
			if !at.UTC().Truncate(24 * time.Hour).Before(observed.UTC().Truncate(24 * time.Hour)) {
				return errors.New("daily observation must describe a completed UTC day")
			}
		}
	} else if r.Kind == "" || r.Label == "" {
		return errors.New("event kind and label are required")
	}
	return nil
}

func insert(ctx context.Context, tx *sql.Tx, r Row) (int, error) {
	if err := Validate(r); err != nil {
		return 0, err
	}
	// Imported IDs preserve their exact history. Only freshly collected daily
	// values suppress unchanged re-reads; later corrections append a new sequence.
	if r.ID == "" && r.Type == "metric" && r.Kind == "daily" {
		var previous sql.NullFloat64
		var provenance string
		err := tx.QueryRowContext(ctx, `SELECT value,provenance FROM metric_observations WHERE entity=? AND target=? AND metric=? AND kind='daily' AND ts=? ORDER BY sequence DESC LIMIT 1`, r.Entity, r.Target, r.Metric, r.TS).Scan(&previous, &provenance)
		if err == nil && provenance == r.Provenance && ((r.Value == nil && !previous.Valid) || (r.Value != nil && previous.Valid && previous.Float64 == *r.Value)) {
			return 0, nil
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return 0, err
		}
	}
	if r.ID == "" {
		b, _ := json.Marshal(r)
		h := sha256.Sum256(b)
		r.ID = hex.EncodeToString(h[:])
	}
	var result sql.Result
	var err error
	if r.Type == "metric" {
		result, err = tx.ExecContext(ctx, "INSERT INTO metric_observations(id,entity,target,metric,kind,ts,value,observed_at,provenance) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO NOTHING", r.ID, r.Entity, r.Target, r.Metric, r.Kind, r.TS, r.Value, r.ObservedAt, r.Provenance)
	} else {
		result, err = tx.ExecContext(ctx, "INSERT INTO metric_events(id,entity,target,kind,ts,label,url,observed_at,provenance) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO NOTHING", r.ID, r.Entity, r.Target, r.Kind, r.TS, r.Label, r.URL, r.ObservedAt, r.Provenance)
	}
	if err != nil {
		return 0, err
	}
	n, err := result.RowsAffected()
	return int(n), err
}

func Write(ctx context.Context, s *store.Store, rows []Row) (int, error) {
	written := 0
	err := s.WithTx(ctx, func(tx *sql.Tx) error {
		for _, r := range rows {
			n, err := insert(ctx, tx, r)
			if err != nil {
				return err
			}
			written += n
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return written, nil
}

func inScope(c Config, r Row) bool {
	for _, t := range c.Targets {
		if t.Target == r.Target && t.Entity == r.Entity {
			return true
		}
	}
	return false
}

// Import validates scope and streams the whole input in one transaction. A bad
// row (including one past a batch boundary) never leaves a partial history.
func Import(ctx context.Context, s *store.Store, c Config, in io.Reader) (int, error) {
	written := 0
	err := s.WithTx(ctx, func(tx *sql.Tx) error {
		scanner := bufio.NewScanner(in)
		scanner.Buffer(make([]byte, 65536), 4*1024*1024)
		line := 0
		for scanner.Scan() {
			line++
			var r Row
			d := json.NewDecoder(strings.NewReader(scanner.Text()))
			d.DisallowUnknownFields()
			if d.Decode(&r) != nil || d.Decode(new(any)) != io.EOF || !inScope(c, r) {
				return fmt.Errorf("invalid import scope or JSON at line %d", line)
			}
			if r.ID == "" {
				return fmt.Errorf("import ID required at line %d", line)
			}
			n, err := insert(ctx, tx, r)
			if err != nil {
				return fmt.Errorf("import line %d: %w", line, err)
			}
			written += n
		}
		return scanner.Err()
	})
	if err != nil {
		return 0, err
	}
	return written, nil
}

func Execute(ctx context.Context, command string, c Config, collect Collector, in io.Reader) (Result, error) {
	result := Result{Source: Owner, Command: command}
	if err := c.Validate(); err != nil {
		return result, err
	}
	if command != "status" && command != "import" && command != "collect" {
		return result, errors.New("unknown metrics command")
	}
	if command == "status" {
		s, err := ownedReadOnly(ctx, c.Database)
		if err != nil {
			return result, err
		}
		defer s.Close()
		var last sql.NullString
		err = s.DB().QueryRowContext(ctx, "SELECT count(*),max(observed_at) FROM metric_observations").Scan(&result.Observations, &last)
		if err == nil {
			err = s.DB().QueryRowContext(ctx, "SELECT count(*) FROM metric_events").Scan(&result.Events)
		}
		if err == nil && last.Valid {
			result.LastObserved = &last.String
		}
		result.OK = err == nil
		return result, err
	}
	// Own the database from before initialization through provider reads, commit,
	// and close. SQLite transactions alone do not serialize collection attempts.
	lock, err := acquireWriter(c.Database)
	if err != nil {
		return result, err
	}
	defer lock.Close()
	s, err := Open(ctx, c.Database)
	if err != nil {
		return result, err
	}
	defer s.Close()
	if command == "import" {
		result.RowsWritten, err = Import(ctx, s, c, in)
		result.OK = err == nil
		return result, err
	}
	ts := time.Now().UTC().Format(time.RFC3339Nano)
	rows, collectionErr := collect(ctx, c, ts)
	for _, r := range rows {
		if !inScope(c, r) {
			return result, errors.New("collector returned invalid target")
		}
	}
	// Preserve completed reads even when collection is canceled. This bounded,
	// independent write does not start another request or retry collection.
	writeCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	err = s.WithTx(writeCtx, func(tx *sql.Tx) error {
		for _, r := range rows {
			n, err := insert(writeCtx, tx, r)
			if err != nil {
				return err
			}
			result.RowsWritten += n
		}
		status := "ok"
		if collectionErr != nil {
			status = "partial"
		}
		_, err := tx.ExecContext(writeCtx, "INSERT INTO metric_runs(ts,status,rows_written) VALUES(?,?,?)", ts, status, result.RowsWritten)
		return err
	})
	if err != nil {
		result.RowsWritten = 0
		return result, err
	}
	result.OK = collectionErr == nil
	if collectionErr != nil {
		return result, errors.New("one or more GitHub metrics unavailable; successful values and unknown observations were retained")
	}
	return result, nil
}
