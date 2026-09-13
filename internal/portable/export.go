package portable

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/openclaw/gitcrawl/internal/store"
)

const (
	CurrentStateV1        = "current-state-v1"
	CompressionGzip       = "gzip"
	portableSchema        = "gitcrawl-portable-sync-v2"
	currentProfileVersion = 1
	defaultBodyChars      = 256
	onlineBackupPageChunk = int32(1024)
	onlineBackupBusyRetry = 50 * time.Millisecond
)

type ExportOptions struct {
	SourceDBPath    string
	OutputDir       string
	DatabaseName    string
	PublicPath      string
	Profile         string
	Repository      string
	BodyChars       int
	MaxBytes        *int64
	Compression     string
	MaxArchiveBytes *int64
	Progress        ProgressFunc
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
	StageArtifactIdentity Stage = "artifact identity"
	StageCompression      Stage = "compression"
	StageManifest         Stage = "manifest"
	StageArtifactCommit   Stage = "artifact commit"
	StageComplete         Stage = "complete"
)

type ProgressFunc func(Stage)

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
	Compression          string      `json:"compression,omitempty"`
	ArchivePath          string      `json:"archive_path,omitempty"`
	ArchiveBytes         int64       `json:"archive_bytes,omitempty"`
	ArchiveSHA256        string      `json:"archive_sha256,omitempty"`
	MaxArchiveBytes      *int64      `json:"max_archive_bytes,omitempty"`
	ArchiveByteBudgetOK  bool        `json:"archive_byte_budget_ok"`
	ArtifactID           string      `json:"artifact_id"`
	ArtifactIDProfile    string      `json:"artifact_id_profile"`
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
	if options.Compression != "" && options.Compression != CompressionGzip {
		return result, fmt.Errorf("unsupported compression %q; supported compression: %s", options.Compression, CompressionGzip)
	}
	if options.MaxArchiveBytes != nil {
		if *options.MaxArchiveBytes <= 0 {
			return result, fmt.Errorf("max-archive-bytes must be a positive integer")
		}
		if options.Compression == "" {
			return result, fmt.Errorf("max-archive-bytes requires compression")
		}
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
	archivePath := ""
	if options.Compression == CompressionGzip {
		archivePath = dbPath + ".gz"
	}
	result = ExportResult{
		Profile:             profile.Name,
		PortableSchema:      portableSchema,
		Schema:              portableSchema,
		SourceDBPath:        sourcePath,
		OutputDir:           outputDir,
		DatabasePath:        filepath.Join(outputDir, options.DatabaseName),
		ManifestPath:        filepath.Join(outputDir, options.DatabaseName+".manifest.json"),
		PublicPath:          options.PublicPath,
		BodyChars:           options.BodyChars,
		BytesBefore:         sourceInfo.Size(),
		MaxBytes:            options.MaxBytes,
		ByteBudgetOK:        true,
		Compression:         options.Compression,
		MaxArchiveBytes:     options.MaxArchiveBytes,
		ArchiveByteBudgetOK: true,
		ColumnProfile:       profile.ColumnProfile,
	}
	if archivePath != "" {
		result.ArchivePath = filepath.Join(outputDir, filepath.Base(archivePath))
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
	for _, table := range profile.ClearedTables {
		if _, err := st.DB().ExecContext(ctx, `delete from `+quoteIdentifier(table)); err != nil {
			return result, fmt.Errorf("clear portable table %s: %w", table, err)
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
	result.ArtifactIDProfile = CurrentStateSemanticV1
	if err := reportProgress(ctx, options.Progress, StageArtifactIdentity); err != nil {
		return result, err
	}
	artifactID, err := ComputeArtifactID(ctx, dbPath, result.ArtifactIDProfile)
	if err != nil {
		return result, fmt.Errorf("compute portable artifact identity: %w", err)
	}
	result.ArtifactID = artifactID
	result.DroppedTables = droppedTables
	result.DroppedIndexes = droppedIndexes
	if options.Compression == CompressionGzip {
		if err := reportProgress(ctx, options.Progress, StageCompression); err != nil {
			return result, err
		}
		if err := writeGzipArchive(dbPath, archivePath); err != nil {
			return result, fmt.Errorf("compress portable database: %w", err)
		}
		archiveInfo, err := os.Stat(archivePath)
		if err != nil {
			return result, fmt.Errorf("stat portable archive: %w", err)
		}
		result.ArchiveBytes = archiveInfo.Size()
		if options.MaxArchiveBytes != nil && archiveInfo.Size() > *options.MaxArchiveBytes {
			result.ArchiveByteBudgetOK = false
			return result, fmt.Errorf("portable archive size %d exceeds max-archive-bytes %d", archiveInfo.Size(), *options.MaxArchiveBytes)
		}
		result.ArchiveSHA256, err = fileSHA256(archivePath)
		if err != nil {
			return result, fmt.Errorf("hash portable archive: %w", err)
		}
	}
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
		ArtifactID:           artifactID,
		ArtifactIDProfile:    result.ArtifactIDProfile,
		Repository:           result.Repository,
		BodyChars:            options.BodyChars,
		MaxBytes:             options.MaxBytes,
		Compression:          options.Compression,
		ArchiveBytes:         result.ArchiveBytes,
		ArchiveSHA256:        result.ArchiveSHA256,
		MaxArchiveBytes:      options.MaxArchiveBytes,
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
	if archivePath != "" {
		manifest.ArchivePath = filepath.Base(archivePath)
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
	if archivePath != "" {
		if err := syncRegularFile(archivePath); err != nil {
			return result, fmt.Errorf("sync portable archive: %w", err)
		}
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
	if archivePath != "" {
		if err := os.Remove(dbPath); err != nil {
			return result, fmt.Errorf("remove unpackaged portable database: %w", err)
		}
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
