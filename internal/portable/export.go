package portable

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/openclaw/gitcrawl/internal/store"
	_ "modernc.org/sqlite"
)

const (
	CurrentStateV1        = "current-state-v1"
	portableSchema        = "gitcrawl-portable-sync-v2"
	currentProfileVersion = 1
	defaultBodyChars      = 256
)

// Profile defines a portable artifact's semantic policy independently of CLI
// parsing and publication concerns.
type Profile struct {
	Name          string
	Version       int
	DroppedTables []string
	Capabilities  []string
	Excluded      []string
	IndexProfile  string
}

var currentStateProfile = Profile{
	Name:    CurrentStateV1,
	Version: currentProfileVersion,
	DroppedTables: []string{
		"comment_revisions",
		"thread_key_summaries",
		"cluster_closures",
		"cluster_overrides",
		"cluster_aliases",
		"cluster_memberships",
		"cluster_groups",
	},
	Capabilities: []string{
		"body_excerpts",
		"comment_excerpts",
		"author_association",
		"current_comments",
		"thread_revisions",
		"thread_fingerprints",
		"pr_details",
		"pr_files",
		"pr_commits",
		"pr_checks",
		"pr_review_threads",
		"workflow_runs",
		"family_tombstones",
		"thread_child_observation_memberships",
		"raw_json_stripped",
	},
	Excluded: []string{
		"raw_json",
		"documents",
		"fts",
		"vectors",
		"code_snapshots",
		"code_documents",
		"comment_revision_history",
		"thread_key_summaries",
		"cluster_governance",
		"cluster_lineage",
		"cluster_state",
		"run_history",
		"similarity_edges",
		"blobs",
		"sync_attempt_failures",
		"ordinary_indexes",
	},
	IndexProfile: "constraints-only",
}

func ResolveProfile(name string) (Profile, error) {
	if name != CurrentStateV1 {
		return Profile{}, fmt.Errorf("unsupported portable export profile %q; supported profile: %s", name, CurrentStateV1)
	}
	return currentStateProfile, nil
}

type ExportOptions struct {
	SourceDBPath string
	OutputDir    string
	DatabaseName string
	PublicPath   string
	Profile      string
	Repository   string
	BodyChars    int
	MaxBytes     *int64
}

type Repository struct {
	ID       int64  `json:"id"`
	Owner    string `json:"owner"`
	Name     string `json:"name"`
	FullName string `json:"fullName"`
}

type Table struct {
	Name string `json:"name"`
	Rows int64  `json:"rows"`
}

type Manifest struct {
	Schema               string      `json:"schema"`
	PortableSchema       string      `json:"portableSchema"`
	Profile              string      `json:"profile"`
	ProfileVersion       int         `json:"profileVersion"`
	ExportedAt           string      `json:"exportedAt"`
	OutputPath           string      `json:"outputPath"`
	OutputBytes          int64       `json:"outputBytes"`
	SHA256               string      `json:"sha256"`
	ArtifactID           string      `json:"artifactId"`
	Repository           *Repository `json:"repository,omitempty"`
	BodyChars            int         `json:"bodyChars"`
	MaxBytes             *int64      `json:"maxBytes,omitempty"`
	Tables               []Table     `json:"tables"`
	Excluded             []string    `json:"excluded"`
	ValidationOK         bool        `json:"validationOk"`
	QuickCheck           string      `json:"quickCheck"`
	IntegrityCheck       string      `json:"integrityCheck"`
	ForeignKeyViolations int         `json:"foreignKeyViolations"`
	DroppedTables        []string    `json:"droppedTables"`
	DroppedIndexes       []string    `json:"droppedIndexes"`
	IndexProfile         string      `json:"indexProfile"`
}

type ExportResult struct {
	Profile              string      `json:"profile"`
	PortableSchema       string      `json:"portable_schema"`
	Schema               string      `json:"schema"`
	SourceDBPath         string      `json:"source_db_path"`
	OutputDir            string      `json:"output_dir"`
	DatabasePath         string      `json:"database_path"`
	ManifestPath         string      `json:"manifest_path"`
	PublicPath           string      `json:"public_path"`
	Repository           *Repository `json:"repository,omitempty"`
	BodyChars            int         `json:"body_chars"`
	BytesBefore          int64       `json:"bytes_before"`
	BytesAfter           int64       `json:"bytes_after"`
	MaxBytes             *int64      `json:"max_bytes,omitempty"`
	ByteBudgetOK         bool        `json:"byte_budget_ok"`
	ArtifactID           string      `json:"artifact_id"`
	SHA256               string      `json:"sha256"`
	QuickCheck           string      `json:"quick_check"`
	IntegrityCheck       string      `json:"integrity_check"`
	ForeignKeyViolations int         `json:"foreign_key_violations"`
	Vacuumed             bool        `json:"vacuumed"`
	DroppedTables        []string    `json:"dropped_tables"`
	DroppedIndexes       []string    `json:"dropped_indexes"`
	ArtifactCommitted    bool        `json:"artifact_committed"`
}

type exporter struct {
	beforeManifest func() error
	beforeCommit   func() error
}

func Export(ctx context.Context, options ExportOptions) (ExportResult, error) {
	return exporter{}.export(ctx, options)
}

func (e exporter) export(ctx context.Context, options ExportOptions) (result ExportResult, err error) {
	profile, err := ResolveProfile(options.Profile)
	if err != nil {
		return result, err
	}
	if options.BodyChars <= 0 {
		options.BodyChars = defaultBodyChars
	}
	if err := ValidateDatabaseName(options.DatabaseName); err != nil {
		return result, err
	}
	if err := ValidatePublicPath(options.PublicPath); err != nil {
		return result, err
	}
	if options.MaxBytes != nil && *options.MaxBytes <= 0 {
		return result, fmt.Errorf("max-bytes must be a positive integer")
	}
	sourcePath, err := filepath.Abs(options.SourceDBPath)
	if err != nil {
		return result, fmt.Errorf("resolve source database path: %w", err)
	}
	outputDir, err := filepath.Abs(options.OutputDir)
	if err != nil {
		return result, fmt.Errorf("resolve output directory: %w", err)
	}
	if _, err := os.Lstat(outputDir); err == nil {
		return result, fmt.Errorf("output directory already exists: %s", outputDir)
	} else if !errors.Is(err, os.ErrNotExist) {
		return result, fmt.Errorf("inspect output directory: %w", err)
	}
	sourceInfo, err := os.Stat(sourcePath)
	if err != nil {
		return result, fmt.Errorf("stat source database: %w", err)
	}
	if !sourceInfo.Mode().IsRegular() {
		return result, fmt.Errorf("source database is not a regular file: %s", sourcePath)
	}
	if err := os.MkdirAll(filepath.Dir(outputDir), 0o755); err != nil {
		return result, fmt.Errorf("create output parent: %w", err)
	}
	stageDir, err := os.MkdirTemp(filepath.Dir(outputDir), ".gitcrawl-portable-export-*")
	if err != nil {
		return result, fmt.Errorf("create portable export staging directory: %w", err)
	}
	defer func() {
		if !result.ArtifactCommitted {
			_ = os.RemoveAll(stageDir)
		}
	}()
	dbPath := filepath.Join(stageDir, options.DatabaseName)
	manifestPath := dbPath + ".manifest.json"
	result = ExportResult{
		Profile:        profile.Name,
		PortableSchema: portableSchema,
		Schema:         portableSchema,
		SourceDBPath:   sourcePath,
		OutputDir:      outputDir,
		DatabasePath:   filepath.Join(outputDir, options.DatabaseName),
		ManifestPath:   filepath.Join(outputDir, options.DatabaseName+".manifest.json"),
		PublicPath:     options.PublicPath,
		BodyChars:      options.BodyChars,
		BytesBefore:    sourceInfo.Size(),
		MaxBytes:       options.MaxBytes,
		ByteBudgetOK:   true,
	}
	if err := snapshotSQLite(ctx, sourcePath, dbPath); err != nil {
		return result, err
	}
	st, err := store.Open(ctx, dbPath)
	if err != nil {
		return result, fmt.Errorf("open disposable portable snapshot: %w", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = st.Close()
		}
	}()
	if options.Repository != "" {
		scope, err := st.RestrictPortableRepository(ctx, options.Repository)
		if err != nil {
			return result, fmt.Errorf("restrict portable repository: %w", err)
		}
		result.Repository = repositoryFromStore(scope.Repository)
	}
	pruneStats, err := st.PrunePortablePayloads(ctx, store.PortablePruneOptions{
		BodyChars:           options.BodyChars,
		Vacuum:              false,
		IncludeSyncFailures: false,
		DeferSecureRewrite:  true,
	})
	if err != nil {
		return result, fmt.Errorf("apply canonical portable shaping: %w", err)
	}
	droppedTables := append([]string(nil), pruneStats.DroppedTables...)
	for _, table := range profile.DroppedTables {
		dropped, err := dropTableIfPresent(ctx, st.DB(), table)
		if err != nil {
			return result, err
		}
		if dropped {
			droppedTables = append(droppedTables, table)
		}
	}
	droppedIndexes, err := ordinaryNonUniqueIndexes(ctx, st.DB())
	if err != nil {
		return result, err
	}
	for _, index := range droppedIndexes {
		if _, err := st.DB().ExecContext(ctx, `drop index if exists `+quoteIdentifier(index)); err != nil {
			return result, fmt.Errorf("drop portable index %s: %w", index, err)
		}
	}
	tableNames, err := databaseTableNames(ctx, st.DB())
	if err != nil {
		return result, err
	}
	exportedAt := time.Now().UTC().Format(time.RFC3339Nano)
	metadata := map[string]string{
		"schema":          portableSchema,
		"profile":         profile.Name,
		"profile_version": fmt.Sprintf("%d", profile.Version),
		"body_chars":      fmt.Sprintf("%d", options.BodyChars),
		"capabilities":    strings.Join(profile.Capabilities, ","),
		"includes":        strings.Join(tableNames, ","),
		"excluded":        strings.Join(profile.Excluded, ","),
		"exported_at":     exportedAt,
		"source_path":     options.PublicPath,
		"index_profile":   profile.IndexProfile,
	}
	if err := writeMetadata(ctx, st.DB(), metadata); err != nil {
		return result, err
	}
	if _, err := st.DB().ExecContext(ctx, `delete from portable_metadata where key = 'sync_failure_scrub_pending'`); err != nil {
		return result, fmt.Errorf("clear portable scrub marker: %w", err)
	}
	if err := finalVacuum(ctx, st.DB()); err != nil {
		return result, err
	}
	result.Vacuumed = true
	quickCheck, err := checkPragma(ctx, st.DB(), "quick_check")
	if err != nil {
		return result, err
	}
	integrityCheck, err := checkPragma(ctx, st.DB(), "integrity_check")
	if err != nil {
		return result, err
	}
	foreignKeyViolations, err := foreignKeyCheck(ctx, st.DB())
	if err != nil {
		return result, err
	}
	result.QuickCheck = quickCheck
	result.IntegrityCheck = integrityCheck
	result.ForeignKeyViolations = foreignKeyViolations
	if quickCheck != "ok" || integrityCheck != "ok" || foreignKeyViolations != 0 {
		return result, fmt.Errorf("portable database validation failed: quick_check=%q integrity_check=%q foreign_key_violations=%d", quickCheck, integrityCheck, foreignKeyViolations)
	}
	if result.Repository == nil {
		result.Repository, err = singleRepository(ctx, st.DB())
		if err != nil {
			return result, err
		}
	} else if err := verifyRepository(ctx, st.DB(), *result.Repository); err != nil {
		return result, err
	}
	tables, err := databaseTableStats(ctx, st.DB())
	if err != nil {
		return result, err
	}
	if err := st.Close(); err != nil {
		return result, fmt.Errorf("close portable snapshot: %w", err)
	}
	closed = true
	if err := removeSQLiteSidecars(dbPath); err != nil {
		return result, err
	}
	info, err := os.Stat(dbPath)
	if err != nil {
		return result, fmt.Errorf("stat finalized portable database: %w", err)
	}
	result.BytesAfter = info.Size()
	if options.MaxBytes != nil && info.Size() > *options.MaxBytes {
		result.ByteBudgetOK = false
		return result, fmt.Errorf("portable database size %d exceeds max-bytes %d", info.Size(), *options.MaxBytes)
	}
	sha, err := fileSHA256(dbPath)
	if err != nil {
		return result, fmt.Errorf("hash portable database: %w", err)
	}
	result.SHA256 = sha
	result.ArtifactID = sha
	result.DroppedTables = droppedTables
	result.DroppedIndexes = droppedIndexes
	manifest := Manifest{
		Schema:               portableSchema,
		PortableSchema:       portableSchema,
		Profile:              profile.Name,
		ProfileVersion:       profile.Version,
		ExportedAt:           exportedAt,
		OutputPath:           options.PublicPath,
		OutputBytes:          info.Size(),
		SHA256:               sha,
		ArtifactID:           sha,
		Repository:           result.Repository,
		BodyChars:            options.BodyChars,
		MaxBytes:             options.MaxBytes,
		Tables:               tables,
		Excluded:             append([]string(nil), profile.Excluded...),
		ValidationOK:         true,
		QuickCheck:           quickCheck,
		IntegrityCheck:       integrityCheck,
		ForeignKeyViolations: foreignKeyViolations,
		DroppedTables:        droppedTables,
		DroppedIndexes:       droppedIndexes,
		IndexProfile:         profile.IndexProfile,
	}
	if e.beforeManifest != nil {
		if err := e.beforeManifest(); err != nil {
			return result, err
		}
	}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return result, fmt.Errorf("marshal portable manifest: %w", err)
	}
	if err := writeSyncedFile(manifestPath, append(data, '\n'), 0o644); err != nil {
		return result, fmt.Errorf("write portable manifest: %w", err)
	}
	if err := syncRegularFile(dbPath); err != nil {
		return result, fmt.Errorf("sync portable database: %w", err)
	}
	if err := validateManifestPair(ctx, dbPath, manifestPath, manifest); err != nil {
		return result, err
	}
	if err := removeSQLiteSidecars(dbPath); err != nil {
		return result, err
	}
	if finalSHA, err := fileSHA256(dbPath); err != nil {
		return result, fmt.Errorf("re-hash portable database after validation: %w", err)
	} else if finalSHA != sha {
		return result, fmt.Errorf("portable database changed during manifest validation")
	}
	bestEffortSyncDir(stageDir)
	if e.beforeCommit != nil {
		if err := e.beforeCommit(); err != nil {
			return result, err
		}
	}
	if _, err := os.Lstat(outputDir); err == nil {
		return result, fmt.Errorf("output directory already exists: %s", outputDir)
	} else if !errors.Is(err, os.ErrNotExist) {
		return result, fmt.Errorf("inspect output directory before commit: %w", err)
	}
	// The portable artifact contract deliberately uses a sibling rename and
	// portable Go filesystem APIs, so recheck the nonexistent target at the
	// last possible point before committing the directory.
	if err := os.Rename(stageDir, outputDir); err != nil {
		return result, fmt.Errorf("commit portable artifact: %w", err)
	}
	bestEffortSyncDir(filepath.Dir(outputDir))
	result.ArtifactCommitted = true
	return result, nil
}

func ValidateDatabaseName(name string) error {
	if name == "" || name == "." || name == ".." || strings.TrimSpace(name) != name ||
		strings.ContainsAny(name, "/\\\x00") || filepath.Base(name) != name {
		return fmt.Errorf("database-name must be a safe basename")
	}
	lower := strings.ToLower(name)
	if strings.HasSuffix(lower, "-wal") || strings.HasSuffix(lower, "-shm") || strings.HasSuffix(lower, ".manifest.json") {
		return fmt.Errorf("database-name must not use a SQLite sidecar or manifest suffix")
	}
	for _, r := range name {
		if !unicode.IsLetter(r) && !unicode.IsNumber(r) && r != '.' && r != '-' && r != '_' {
			return fmt.Errorf("database-name must contain only letters, numbers, dots, dashes, or underscores")
		}
	}
	return nil
}

func ValidatePublicPath(value string) error {
	if value == "" || strings.TrimSpace(value) != value || strings.ContainsAny(value, "\\\x00") ||
		strings.HasPrefix(value, "/") || path.Clean(value) != value || value == "." {
		return fmt.Errorf("public-path must be a clean relative slash path")
	}
	for _, part := range strings.Split(value, "/") {
		if part == "" || part == "." || part == ".." {
			return fmt.Errorf("public-path must be a clean relative slash path without traversal")
		}
	}
	if len(value) >= 2 && value[1] == ':' {
		return fmt.Errorf("public-path must not be an absolute host path")
	}
	return nil
}

func snapshotSQLite(ctx context.Context, sourcePath, targetPath string) error {
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
	if _, err := db.ExecContext(ctx, `vacuum into ?`, targetPath); err != nil {
		return fmt.Errorf("snapshot source database: %w", err)
	}
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

func dropTableIfPresent(ctx context.Context, db *sql.DB, table string) (bool, error) {
	var exists int
	if err := db.QueryRowContext(ctx, `select exists(select 1 from sqlite_schema where type = 'table' and name = ?)`, table).Scan(&exists); err != nil {
		return false, fmt.Errorf("inspect portable table %s: %w", table, err)
	}
	if exists == 0 {
		return false, nil
	}
	if _, err := db.ExecContext(ctx, `drop table `+quoteIdentifier(table)); err != nil {
		return false, fmt.Errorf("drop portable table %s: %w", table, err)
	}
	return true, nil
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

func finalVacuum(ctx context.Context, db *sql.DB) error {
	var busy, logFrames, checkpointedFrames int
	if err := db.QueryRowContext(ctx, `pragma wal_checkpoint(TRUNCATE)`).Scan(&busy, &logFrames, &checkpointedFrames); err != nil {
		return fmt.Errorf("checkpoint portable snapshot: %w", err)
	}
	if busy != 0 {
		return fmt.Errorf("checkpoint portable snapshot: busy with %d of %d frames checkpointed", checkpointedFrames, logFrames)
	}
	if _, err := db.ExecContext(ctx, `vacuum`); err != nil {
		return fmt.Errorf("vacuum portable snapshot: %w", err)
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

func foreignKeyCheck(ctx context.Context, db *sql.DB) (int, error) {
	rows, err := db.QueryContext(ctx, `pragma foreign_key_check`)
	if err != nil {
		return 0, fmt.Errorf("run SQLite foreign_key_check: %w", err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("read SQLite foreign_key_check: %w", err)
	}
	return count, nil
}

func fileSHA256(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func writeSyncedFile(filePath string, data []byte, mode os.FileMode) error {
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return err
	}
	ok := false
	defer func() {
		_ = file.Close()
		if !ok {
			_ = os.Remove(filePath)
		}
	}()
	if _, err := file.Write(data); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	ok = true
	return nil
}

func syncRegularFile(filePath string) error {
	file, err := os.OpenFile(filePath, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}

func bestEffortSyncDir(dir string) {
	file, err := os.Open(dir)
	if err == nil {
		_ = file.Sync()
		_ = file.Close()
	}
}

func removeSQLiteSidecars(dbPath string) error {
	for _, suffix := range []string{"-wal", "-shm"} {
		sidecar := dbPath + suffix
		if err := os.Remove(sidecar); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove portable SQLite sidecar %s: %w", sidecar, err)
		}
		if _, err := os.Lstat(sidecar); err == nil {
			return fmt.Errorf("portable SQLite sidecar remains after removal: %s", sidecar)
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("verify portable SQLite sidecar removal %s: %w", sidecar, err)
		}
	}
	return nil
}

func validateManifestPair(ctx context.Context, dbPath, manifestPath string, expected Manifest) error {
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		return fmt.Errorf("re-read portable manifest: %w", err)
	}
	var actual Manifest
	if err := json.Unmarshal(data, &actual); err != nil {
		return fmt.Errorf("re-read portable manifest: %w", err)
	}
	if !reflect.DeepEqual(actual, expected) {
		return fmt.Errorf("portable manifest changed during finalization")
	}
	info, err := os.Stat(dbPath)
	if err != nil {
		return fmt.Errorf("re-stat portable database: %w", err)
	}
	if info.Size() != actual.OutputBytes {
		return fmt.Errorf("portable manifest size %d does not match database size %d", actual.OutputBytes, info.Size())
	}
	sha, err := fileSHA256(dbPath)
	if err != nil {
		return fmt.Errorf("re-hash portable database: %w", err)
	}
	if sha != actual.SHA256 || sha != actual.ArtifactID {
		return fmt.Errorf("portable manifest digest does not match database")
	}
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return fmt.Errorf("reopen portable database for manifest validation: %w", err)
	}
	defer db.Close()
	quick, err := checkPragma(ctx, db, "quick_check")
	if err != nil {
		return err
	}
	if quick != actual.QuickCheck || quick != "ok" {
		return fmt.Errorf("portable manifest quickCheck does not match database")
	}
	integrity, err := checkPragma(ctx, db, "integrity_check")
	if err != nil {
		return err
	}
	if integrity != actual.IntegrityCheck || integrity != "ok" {
		return fmt.Errorf("portable manifest integrityCheck does not match database")
	}
	violations, err := foreignKeyCheck(ctx, db)
	if err != nil {
		return err
	}
	if violations != actual.ForeignKeyViolations || violations != 0 {
		return fmt.Errorf("portable manifest foreignKeyViolations does not match database")
	}
	tables, err := databaseTableStats(ctx, db)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(tables, actual.Tables) {
		return fmt.Errorf("portable manifest table counts do not match database")
	}
	repository, err := singleRepository(ctx, db)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(repository, actual.Repository) {
		return fmt.Errorf("portable manifest repository does not match database scope")
	}
	for key, want := range map[string]string{
		"schema":          actual.Schema,
		"profile":         actual.Profile,
		"profile_version": fmt.Sprintf("%d", actual.ProfileVersion),
		"source_path":     actual.OutputPath,
		"index_profile":   actual.IndexProfile,
	} {
		var got string
		if err := db.QueryRowContext(ctx, `select value from portable_metadata where key = ?`, key).Scan(&got); err != nil {
			return fmt.Errorf("validate portable metadata %s: %w", key, err)
		}
		if got != want {
			return fmt.Errorf("portable metadata %s %q does not match manifest %q", key, got, want)
		}
	}
	return nil
}
