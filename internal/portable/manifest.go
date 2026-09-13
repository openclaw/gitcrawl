package portable

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strings"
	"unicode"
)

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
	ArtifactIDProfile    string      `json:"artifactIdProfile"`
	Repository           *Repository `json:"repository,omitempty"`
	BodyChars            int         `json:"bodyChars"`
	MaxBytes             *int64      `json:"maxBytes,omitempty"`
	Compression          string      `json:"compression,omitempty"`
	ArchivePath          string      `json:"archivePath,omitempty"`
	ArchiveBytes         int64       `json:"archiveBytes,omitempty"`
	ArchiveSHA256        string      `json:"archiveSha256,omitempty"`
	MaxArchiveBytes      *int64      `json:"maxArchiveBytes,omitempty"`
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
	if sha != actual.SHA256 {
		return fmt.Errorf("portable manifest sha256 does not match database")
	}
	if actual.Compression != "" {
		if actual.Compression != CompressionGzip {
			return fmt.Errorf("portable manifest compression %q is unsupported", actual.Compression)
		}
		if actual.ArchivePath == "" || filepath.Base(actual.ArchivePath) != actual.ArchivePath {
			return fmt.Errorf("portable manifest archivePath must be a basename")
		}
		if actual.ArchiveBytes <= 0 || actual.ArchiveSHA256 == "" {
			return fmt.Errorf("portable manifest archive metadata is incomplete")
		}
		if err := validateGzipArchive(filepath.Join(filepath.Dir(manifestPath), actual.ArchivePath), actual.ArchiveBytes, actual.ArchiveSHA256, actual.OutputBytes, actual.SHA256); err != nil {
			return err
		}
	}
	if actual.Profile != CurrentStateV1 {
		return fmt.Errorf("portable manifest profile %q does not support semantic artifact identity", actual.Profile)
	}
	if actual.ArtifactIDProfile != CurrentStateSemanticV1 {
		return fmt.Errorf("portable manifest artifactIdProfile %q is unsupported", actual.ArtifactIDProfile)
	}
	artifactID, err := ComputeArtifactID(ctx, dbPath, actual.ArtifactIDProfile)
	if err != nil {
		return fmt.Errorf("recompute portable artifact identity: %w", err)
	}
	if artifactID != actual.ArtifactID {
		return fmt.Errorf("portable manifest artifactId does not match database semantic state")
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
