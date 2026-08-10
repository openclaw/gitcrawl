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
	moderncsqlite "modernc.org/sqlite"
)

const (
	CurrentStateV1        = "current-state-v1"
	portableSchema        = "gitcrawl-portable-sync-v2"
	currentProfileVersion = 1
	defaultBodyChars      = 256
	onlineBackupPageChunk = int32(1024)
	onlineBackupBusyRetry = 50 * time.Millisecond
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
	ColumnProfile string
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
		"pull_request_file_patches",
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
	IndexProfile:  "constraints-only",
	ColumnProfile: store.PortableColumnProfileSanitizedCompatibility,
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
	Progress     ProgressFunc
}

type Stage string

const (
	StageSnapshot         Stage = "snapshot"
	StageRepositoryScope  Stage = "repository scope"
	StageProfileOmissions Stage = "profile omissions"
	StageCanonicalShaping Stage = "canonical shaping"
	StageIndexRemoval     Stage = "index removal"
	StageFinalVacuum      Stage = "final vacuum"
	StageValidation       Stage = "validation"
	StageManifest         Stage = "manifest"
	StageArtifactCommit   Stage = "artifact commit"
	StageComplete         Stage = "complete"
)

type ProgressFunc func(Stage)

type onlineBackupOptions struct {
	PagesPerStep int32
	AfterStep    func(remaining, pageCount int)
}

type backupCreator interface {
	NewBackup(string) (*moderncsqlite.Backup, error)
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
	ColumnProfile        string      `json:"columnProfile,omitempty"`
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
	ColumnProfile        string      `json:"column_profile,omitempty"`
}

type exporter struct {
	beforeManifest func() error
	beforeCommit   func() error
	now            func() time.Time
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
		ColumnProfile:  profile.ColumnProfile,
	}
	if err := reportProgress(ctx, options.Progress, StageSnapshot); err != nil {
		return result, err
	}
	if err := snapshotSQLite(ctx, sourcePath, dbPath); err != nil {
		return result, err
	}
	// Opening the disposable snapshot migrates valid older/physically-pruned
	// portable schemas back to the current writable schema before shaping.
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
	if err := configureDisposableStore(ctx, st.DB()); err != nil {
		return result, err
	}
	if err := reportProgress(ctx, options.Progress, StageRepositoryScope); err != nil {
		return result, err
	}
	if options.Repository != "" {
		scope, err := st.RestrictPortableRepositoryWithOptions(ctx, options.Repository, store.PortableRepositoryScopeOptions{DeferForeignKeyValidation: true})
		if err != nil {
			return result, fmt.Errorf("restrict portable repository: %w", err)
		}
		result.Repository = repositoryFromStore(scope.Repository)
	}
	if err := reportProgress(ctx, options.Progress, StageProfileOmissions); err != nil {
		return result, err
	}
	var droppedTables []string
	for _, table := range profile.DroppedTables {
		dropped, err := dropTableIfPresent(ctx, st.DB(), table)
		if err != nil {
			return result, err
		}
		if dropped {
			droppedTables = appendUnique(droppedTables, table)
		}
	}
	if err := reportProgress(ctx, options.Progress, StageCanonicalShaping); err != nil {
		return result, err
	}
	var pruneProgress store.PortablePruneProgressFunc
	if options.Progress != nil {
		pruneProgress = func(stage store.PortablePruneStage) {
			options.Progress(Stage("canonical shaping: " + string(stage)))
		}
	}
	pruneStats, err := st.PrunePortablePayloads(ctx, store.PortablePruneOptions{
		BodyChars:                     options.BodyChars,
		Vacuum:                        false,
		IncludeSyncFailures:           false,
		DeferSecureRewrite:            true,
		RetainSanitizedPayloadColumns: true,
		Progress:                      pruneProgress,
	})
	if err != nil {
		return result, fmt.Errorf("apply canonical portable shaping: %w", err)
	}
	for _, table := range pruneStats.DroppedTables {
		droppedTables = appendUnique(droppedTables, table)
	}
	// Current-state shaping proves foreign keys once, immediately before its
	// threads rebuild while the original table and ordinary indexes still exist.
	if !pruneStats.ForeignKeyValidated {
		return result, fmt.Errorf("canonical portable shaping did not validate foreign keys")
	}
	foreignKeyViolations := pruneStats.ForeignKeyViolations
	result.ForeignKeyViolations = foreignKeyViolations
	if foreignKeyViolations != 0 {
		return result, fmt.Errorf("canonical portable shaping found %d foreign-key violations", foreignKeyViolations)
	}
	if err := reportProgress(ctx, options.Progress, StageIndexRemoval); err != nil {
		return result, err
	}
	remainingIndexes, err := ordinaryNonUniqueIndexes(ctx, st.DB())
	if err != nil {
		return result, err
	}
	for _, index := range remainingIndexes {
		if _, err := st.DB().ExecContext(ctx, `drop index if exists `+quoteIdentifier(index)); err != nil {
			return result, fmt.Errorf("drop portable index %s: %w", index, err)
		}
	}
	var droppedIndexes []string
	for _, index := range append(pruneStats.DroppedIndexes, remainingIndexes...) {
		droppedIndexes = appendUnique(droppedIndexes, index)
	}
	sort.Strings(droppedIndexes)
	tableNames, err := databaseTableNames(ctx, st.DB())
	if err != nil {
		return result, err
	}
	exportTime := time.Now
	if e.now != nil {
		exportTime = e.now
	}
	exportedAt := exportTime().UTC().Format(time.RFC3339Nano)
	metadata := map[string]string{
		"schema":                portableSchema,
		"profile":               profile.Name,
		"profile_version":       fmt.Sprintf("%d", profile.Version),
		"body_chars":            fmt.Sprintf("%d", options.BodyChars),
		"capabilities":          strings.Join(profile.Capabilities, ","),
		"includes":              strings.Join(tableNames, ","),
		"excluded":              strings.Join(profile.Excluded, ","),
		"source_path":           options.PublicPath,
		"index_profile":         profile.IndexProfile,
		"column_profile":        profile.ColumnProfile,
		"thread_author_profile": "login,type,association",
	}
	if _, err := st.DB().ExecContext(ctx, `delete from portable_metadata`); err != nil {
		return result, fmt.Errorf("clear inherited portable metadata: %w", err)
	}
	if err := writeMetadata(ctx, st.DB(), metadata); err != nil {
		return result, err
	}
	if err := reportProgress(ctx, options.Progress, StageFinalVacuum); err != nil {
		return result, err
	}
	compactPath, err := createCompactDatabase(ctx, st.DB(), dbPath)
	if err != nil {
		return result, err
	}
	if err := st.Close(); err != nil {
		return result, fmt.Errorf("close uncompact portable snapshot: %w", err)
	}
	closed = true
	if err := replaceWithCompactDatabase(ctx, dbPath, compactPath); err != nil {
		return result, err
	}
	result.Vacuumed = true
	if err := reportProgress(ctx, options.Progress, StageValidation); err != nil {
		return result, err
	}
	finalDB, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return result, fmt.Errorf("open compact portable database: %w", err)
	}
	finalDB.SetMaxOpenConns(1)
	quickCheck, err := checkPragma(ctx, finalDB, "quick_check")
	if err != nil {
		_ = finalDB.Close()
		return result, err
	}
	integrityCheck, err := checkPragma(ctx, finalDB, "integrity_check")
	if err != nil {
		_ = finalDB.Close()
		return result, err
	}
	result.QuickCheck = quickCheck
	result.IntegrityCheck = integrityCheck
	if quickCheck != "ok" || integrityCheck != "ok" {
		_ = finalDB.Close()
		return result, fmt.Errorf("portable database validation failed: quick_check=%q integrity_check=%q", quickCheck, integrityCheck)
	}
	if result.Repository == nil {
		result.Repository, err = singleRepository(ctx, finalDB)
		if err != nil {
			_ = finalDB.Close()
			return result, err
		}
	} else if err := verifyRepository(ctx, finalDB, *result.Repository); err != nil {
		_ = finalDB.Close()
		return result, err
	}
	tables, err := databaseTableStats(ctx, finalDB)
	if err != nil {
		_ = finalDB.Close()
		return result, err
	}
	if err := finalDB.Close(); err != nil {
		return result, fmt.Errorf("close compact portable database: %w", err)
	}
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
	if err := reportProgress(ctx, options.Progress, StageManifest); err != nil {
		return result, err
	}
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
		ColumnProfile:        profile.ColumnProfile,
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
	if err := reportProgress(ctx, options.Progress, StageArtifactCommit); err != nil {
		return result, err
	}
	// The portable artifact contract deliberately uses a sibling rename and
	// portable Go filesystem APIs, so recheck the nonexistent target at the
	// last possible point before committing the directory.
	if err := os.Rename(stageDir, outputDir); err != nil {
		return result, fmt.Errorf("commit portable artifact: %w", err)
	}
	bestEffortSyncDir(filepath.Dir(outputDir))
	result.ArtifactCommitted = true
	if options.Progress != nil {
		options.Progress(StageComplete)
	}
	return result, nil
}

func reportProgress(ctx context.Context, progress ProgressFunc, stage Stage) error {
	if progress != nil {
		progress(stage)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("portable export canceled during %s: %w", stage, err)
	}
	return nil
}

func appendUnique(values []string, value string) []string {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
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
	// The semantic pipeline proves foreign keys before dropping transport-only
	// indexes. Repeating foreign_key_check here would turn manifest validation
	// into a pathological unindexed scan on large archives.
	if actual.ForeignKeyViolations != 0 {
		return fmt.Errorf("portable manifest records %d foreign-key violations", actual.ForeignKeyViolations)
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
		"column_profile":  actual.ColumnProfile,
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
