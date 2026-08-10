package portable

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const CurrentStateSemanticV1 = "current-state-semantic-v1"

type identityNormalizationKind uint8

const (
	identityEmptyText identityNormalizationKind = iota + 1
	identityNullText
	identityPresentText
	identityZeroInteger
	identityOneInteger
)

type identityColumnPolicy struct {
	Name         string
	DeclaredType string
	Kind         identityNormalizationKind
}

type identityTablePolicy struct {
	Name              string
	Columns           []identityColumnPolicy
	CanonicalRowOrder []string
}

type artifactIdentityPolicy struct {
	Profile         string
	DroppedTriggers []string
	DroppedTables   []string
	Tables          []identityTablePolicy
}

// currentStateSemanticPolicy is deliberately an exact allowlist. Unknown
// future tables and columns remain in the compact identity database, so a
// schema addition cannot accidentally hide public data by matching a name
// pattern. Known tables and columns are optional for legacy portable schemas.
var currentStateSemanticPolicy = artifactIdentityPolicy{
	Profile: CurrentStateSemanticV1,
	DroppedTriggers: []string{
		"observation_convergence_threads_insert",
		"observation_convergence_threads_update",
		"observation_convergence_threads_delete",
		"observation_convergence_revisions_insert",
		"observation_convergence_revisions_update",
		"observation_convergence_revisions_delete",
		"observation_convergence_pr_details_insert",
		"observation_convergence_pr_details_update",
		"observation_convergence_pr_details_delete",
		"observation_convergence_children_insert",
		"observation_convergence_children_update",
		"observation_convergence_children_delete",
		"observation_convergence_workflows_insert",
		"observation_convergence_workflows_update",
		"observation_convergence_workflows_delete",
		"observation_convergence_allocator_insert",
		"observation_convergence_allocator_update",
		"observation_convergence_allocator_delete",
	},
	DroppedTables: []string{
		"observation_schema_convergence",
		"repo_pipeline_state",
		"repo_sync_state",
		"sqlite_stat1",
		"sqlite_stat4",
		"thread_observation_sequence",
		"thread_child_observation_reservations",
		"workflow_run_observation_reservations",
		"pull_request_review_thread_syncs",
	},
	Tables: []identityTablePolicy{
		{Name: "repositories", Columns: []identityColumnPolicy{
			{Name: "updated_at", DeclaredType: "TEXT", Kind: identityEmptyText},
		}},
		{Name: "threads", Columns: []identityColumnPolicy{
			{Name: "first_pulled_at", DeclaredType: "TEXT", Kind: identityNullText},
			{Name: "last_pulled_at", DeclaredType: "TEXT", Kind: identityNullText},
			{Name: "updated_at", DeclaredType: "TEXT", Kind: identityEmptyText},
			{Name: "observation_sequence", DeclaredType: "INTEGER", Kind: identityZeroInteger},
			{Name: "evidence_observation_sequence", DeclaredType: "INTEGER", Kind: identityZeroInteger},
			{Name: "evidence_source_updated_at", DeclaredType: "TEXT", Kind: identityEmptyText},
			// Preserve whether a local absence was observed, but not when it was observed.
			{Name: "closed_at_local", DeclaredType: "TEXT", Kind: identityPresentText},
		}},
		{Name: "comments", Columns: []identityColumnPolicy{
			{Name: "deleted_at", DeclaredType: "TEXT", Kind: identityPresentText},
		}},
		{Name: "thread_revisions", Columns: []identityColumnPolicy{
			{Name: "observation_sequence", DeclaredType: "INTEGER", Kind: identityZeroInteger},
			{Name: "created_at", DeclaredType: "TEXT", Kind: identityEmptyText},
		}},
		{
			Name: "thread_child_observation_memberships",
			Columns: []identityColumnPolicy{
				{Name: "observation_sequence", DeclaredType: "INTEGER", Kind: identityOneInteger},
			},
			CanonicalRowOrder: []string{"thread_id", "family"},
		},
		{Name: "thread_fingerprints", Columns: []identityColumnPolicy{
			{Name: "created_at", DeclaredType: "TEXT", Kind: identityEmptyText},
		}},
		{Name: "pull_request_details", Columns: []identityColumnPolicy{
			{Name: "fetched_at", DeclaredType: "TEXT", Kind: identityEmptyText},
			{Name: "updated_at", DeclaredType: "TEXT", Kind: identityEmptyText},
		}},
		{
			Name: "pull_request_files",
			Columns: []identityColumnPolicy{
				{Name: "fetched_at", DeclaredType: "TEXT", Kind: identityEmptyText},
			},
			CanonicalRowOrder: []string{"thread_id", "position"},
		},
		{
			Name: "pull_request_commits",
			Columns: []identityColumnPolicy{
				{Name: "fetched_at", DeclaredType: "TEXT", Kind: identityEmptyText},
				{Name: "deleted_at", DeclaredType: "TEXT", Kind: identityPresentText},
			},
			CanonicalRowOrder: []string{"thread_id", "sha"},
		},
		{Name: "pull_request_checks", Columns: []identityColumnPolicy{
			{Name: "fetched_at", DeclaredType: "TEXT", Kind: identityEmptyText},
		}},
		{
			Name: "pull_request_review_threads",
			Columns: []identityColumnPolicy{
				{Name: "fetched_at", DeclaredType: "TEXT", Kind: identityEmptyText},
				{Name: "deleted_at", DeclaredType: "TEXT", Kind: identityPresentText},
			},
			CanonicalRowOrder: []string{"thread_id", "review_thread_id"},
		},
		{Name: "pull_request_review_thread_revisions", Columns: []identityColumnPolicy{
			{Name: "fetched_at", DeclaredType: "TEXT", Kind: identityEmptyText},
			{Name: "recorded_at", DeclaredType: "TEXT", Kind: identityEmptyText},
			{Name: "deleted_at", DeclaredType: "TEXT", Kind: identityPresentText},
		}},
		{
			Name: "github_workflow_runs",
			Columns: []identityColumnPolicy{
				{Name: "fetched_at", DeclaredType: "TEXT", Kind: identityEmptyText},
			},
			CanonicalRowOrder: []string{"repo_id", "run_id"},
		},
		{Name: "portable_metadata", CanonicalRowOrder: []string{"key"}},
	},
}

type artifactIdentityOptions struct {
	TempParent     string
	Backup         onlineBackupOptions
	AfterSnapshot  func(string) error
	AfterNormalize func(*sql.DB) error
}

// ComputeArtifactID returns the semantic identity for a finalized current-state
// SQLite artifact. It never opens the artifact writable.
func ComputeArtifactID(ctx context.Context, dbPath, profile string) (string, error) {
	if profile != CurrentStateSemanticV1 {
		return "", fmt.Errorf("unsupported artifact identity profile %q; supported profile: %s", profile, CurrentStateSemanticV1)
	}
	return computeArtifactIDWithOptions(ctx, dbPath, currentStateSemanticPolicy, artifactIdentityOptions{})
}

func computeArtifactIDWithOptions(
	ctx context.Context,
	dbPath string,
	policy artifactIdentityPolicy,
	options artifactIdentityOptions,
) (_ string, retErr error) {
	if err := validateArtifactIdentityPolicy(policy); err != nil {
		return "", err
	}
	tempDir, err := os.MkdirTemp(options.TempParent, "gitcrawl-artifact-identity-*")
	if err != nil {
		return "", fmt.Errorf("create artifact identity directory: %w", err)
	}
	defer func() {
		if err := os.RemoveAll(tempDir); err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("remove artifact identity directory: %w", err))
		}
	}()

	workingPath := filepath.Join(tempDir, "identity.db")
	backupOptions := options.Backup
	if backupOptions.PagesPerStep <= 0 {
		backupOptions.PagesPerStep = onlineBackupPageChunk
	}
	if err := snapshotSQLiteWithOptions(ctx, dbPath, workingPath, backupOptions); err != nil {
		return "", fmt.Errorf("snapshot artifact identity database: %w", err)
	}
	if options.AfterSnapshot != nil {
		if err := options.AfterSnapshot(workingPath); err != nil {
			return "", fmt.Errorf("prepare artifact identity database: %w", err)
		}
	}

	db, err := sql.Open("sqlite", workingPath)
	if err != nil {
		return "", fmt.Errorf("open artifact identity database: %w", err)
	}
	closed := false
	defer func() {
		if !closed {
			retErr = errors.Join(retErr, db.Close())
		}
	}()
	if err := configureDisposableStore(ctx, db); err != nil {
		return "", fmt.Errorf("configure artifact identity database: %w", err)
	}
	// Identity compaction scans the full disposable copy once. A bounded cache
	// and large canonical output pages keep that pass fast without making the
	// digest depend on the source database's page size.
	if _, err := db.ExecContext(ctx, `pragma cache_size = -65536`); err != nil {
		return "", fmt.Errorf("configure artifact identity cache: %w", err)
	}
	if _, err := db.ExecContext(ctx, `pragma page_size = 65536`); err != nil {
		return "", fmt.Errorf("configure artifact identity page size: %w", err)
	}
	if err := normalizeArtifactIdentity(ctx, db, policy); err != nil {
		return "", err
	}
	if options.AfterNormalize != nil {
		if err := options.AfterNormalize(db); err != nil {
			return "", fmt.Errorf("normalize artifact identity database: %w", err)
		}
	}
	if err := ctx.Err(); err != nil {
		return "", fmt.Errorf("artifact identity canceled before compaction: %w", err)
	}
	compactPath, err := createCompactDatabase(ctx, db, workingPath)
	if err != nil {
		return "", fmt.Errorf("compact artifact identity database: %w", err)
	}
	if err := db.Close(); err != nil {
		return "", fmt.Errorf("close artifact identity database: %w", err)
	}
	closed = true
	if err := removeSQLiteSidecars(workingPath); err != nil {
		return "", err
	}
	if err := os.Remove(workingPath); err != nil {
		return "", fmt.Errorf("remove uncompact artifact identity database: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return "", fmt.Errorf("artifact identity canceled before hashing: %w", err)
	}
	digest, err := hashArtifactIdentityFile(compactPath)
	if err != nil {
		return "", fmt.Errorf("hash artifact identity database: %w", err)
	}
	return digest, nil
}

func hashArtifactIdentityFile(filePath string) (string, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR, 0)
	if err != nil {
		return "", err
	}
	header := make([]byte, 100)
	n, readErr := file.ReadAt(header, 0)
	if readErr != nil {
		_ = file.Close()
		return "", readErr
	}
	if n != len(header) || string(header[:16]) != "SQLite format 3\x00" {
		_ = file.Close()
		return "", fmt.Errorf("artifact identity file has invalid SQLite header")
	}
	// SQLite's file change counter, version-valid-for counter, and writer
	// library version describe the engine write event rather than logical
	// database state. The identity file is closed and journal-free, so these
	// informational header words can be canonicalized before hashing.
	binary.BigEndian.PutUint32(header[24:28], 0)
	binary.BigEndian.PutUint32(header[92:96], 0)
	binary.BigEndian.PutUint32(header[96:100], 0)
	if _, err := file.WriteAt(header, 0); err != nil {
		_ = file.Close()
		return "", err
	}
	if err := file.Close(); err != nil {
		return "", err
	}
	return fileSHA256(filePath)
}

func normalizeArtifactIdentity(ctx context.Context, db *sql.DB, policy artifactIdentityPolicy) error {
	if _, err := db.ExecContext(ctx, `pragma foreign_keys = off`); err != nil {
		return fmt.Errorf("disable artifact identity foreign keys: %w", err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin artifact identity normalization: %w", err)
	}
	defer tx.Rollback()
	for _, trigger := range policy.DroppedTriggers {
		if _, err := tx.ExecContext(ctx, `drop trigger if exists `+quoteIdentifier(trigger)); err != nil {
			return fmt.Errorf("drop artifact identity trigger %s: %w", trigger, err)
		}
	}
	retainedTriggers, err := artifactIdentityTriggers(ctx, tx)
	if err != nil {
		return err
	}
	for _, trigger := range retainedTriggers {
		if _, err := tx.ExecContext(ctx, `drop trigger `+quoteIdentifier(trigger.Name)); err != nil {
			return fmt.Errorf("temporarily drop artifact identity trigger %s: %w", trigger.Name, err)
		}
	}
	for _, table := range policy.DroppedTables {
		var exists int
		if err := tx.QueryRowContext(ctx, `select exists(select 1 from sqlite_schema where type = 'table' and name = ?)`, table).Scan(&exists); err != nil {
			return fmt.Errorf("inspect artifact identity table %s: %w", table, err)
		}
		if exists == 0 {
			continue
		}
		if _, err := tx.ExecContext(ctx, `drop table `+quoteIdentifier(table)); err != nil {
			return fmt.Errorf("drop artifact identity table %s: %w", table, err)
		}
	}
	for _, table := range policy.Tables {
		columns, err := artifactIdentityColumns(ctx, tx, table.Name)
		if err != nil {
			return err
		}
		if columns == nil {
			continue
		}
		assignments := make([]string, 0, len(table.Columns))
		for _, column := range table.Columns {
			declaredType, ok := columns.DeclaredTypes[column.Name]
			if !ok {
				continue
			}
			if !strings.EqualFold(strings.TrimSpace(declaredType), column.DeclaredType) {
				return fmt.Errorf("artifact identity column %s.%s has type %q, want %s", table.Name, column.Name, declaredType, column.DeclaredType)
			}
			identifier := quoteIdentifier(column.Name)
			switch column.Kind {
			case identityEmptyText:
				assignments = append(assignments, identifier+` = ''`)
			case identityNullText:
				assignments = append(assignments, identifier+` = null`)
			case identityPresentText:
				assignments = append(assignments, identifier+` = case when `+identifier+` is null then null else '' end`)
			case identityZeroInteger:
				assignments = append(assignments, identifier+` = 0`)
			case identityOneInteger:
				assignments = append(assignments, identifier+` = 1`)
			default:
				return fmt.Errorf("artifact identity column %s.%s has unknown normalization %d", table.Name, column.Name, column.Kind)
			}
		}
		if len(assignments) == 0 {
			// A table may still need hidden rowid canonicalization below.
		} else if _, err := tx.ExecContext(ctx, `update `+quoteIdentifier(table.Name)+` set `+strings.Join(assignments, ", ")); err != nil {
			return fmt.Errorf("normalize artifact identity table %s: %w", table.Name, err)
		}
		if len(table.CanonicalRowOrder) != 0 {
			if err := canonicalizeArtifactIdentityRows(ctx, tx, table.Name, columns.Names, table.CanonicalRowOrder); err != nil {
				return err
			}
		}
		rowOrder := make(map[string]struct{})
		for _, column := range table.CanonicalRowOrder {
			if err := validateIdentityPolicyName(column); err != nil {
				return err
			}
			if _, ok := rowOrder[column]; ok {
				return fmt.Errorf("artifact identity policy repeats row order column %s.%s", table.Name, column)
			}
			rowOrder[column] = struct{}{}
		}
	}
	for _, trigger := range retainedTriggers {
		if _, err := tx.ExecContext(ctx, trigger.SQL); err != nil {
			return fmt.Errorf("restore artifact identity trigger %s: %w", trigger.Name, err)
		}
	}
	// SQLite advances its schema cookie for a transient local table even when
	// that table is removed above. The cookie is database-engine bookkeeping,
	// not portable schema content; canonicalize it before VACUUM rebuilds the
	// compact identity file.
	if _, err := tx.ExecContext(ctx, `pragma schema_version = 0`); err != nil {
		return fmt.Errorf("normalize artifact identity schema version: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit artifact identity normalization: %w", err)
	}
	return nil
}

type identityTableColumns struct {
	Names         []string
	DeclaredTypes map[string]string
}

func artifactIdentityColumns(ctx context.Context, tx *sql.Tx, table string) (*identityTableColumns, error) {
	var exists int
	if err := tx.QueryRowContext(ctx, `select exists(select 1 from sqlite_schema where type = 'table' and name = ?)`, table).Scan(&exists); err != nil {
		return nil, fmt.Errorf("inspect artifact identity table %s: %w", table, err)
	}
	if exists == 0 {
		return nil, nil
	}
	rows, err := tx.QueryContext(ctx, `pragma table_info(`+quoteIdentifier(table)+`)`)
	if err != nil {
		return nil, fmt.Errorf("inspect artifact identity columns for %s: %w", table, err)
	}
	defer rows.Close()
	columns := &identityTableColumns{DeclaredTypes: make(map[string]string)}
	for rows.Next() {
		var sequence, notNull, primaryKey int
		var name, declaredType string
		var defaultValue any
		if err := rows.Scan(&sequence, &name, &declaredType, &notNull, &defaultValue, &primaryKey); err != nil {
			return nil, fmt.Errorf("scan artifact identity columns for %s: %w", table, err)
		}
		columns.Names = append(columns.Names, name)
		columns.DeclaredTypes[name] = declaredType
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read artifact identity columns for %s: %w", table, err)
	}
	return columns, nil
}

type identityTrigger struct {
	Name string
	SQL  string
}

func artifactIdentityTriggers(ctx context.Context, tx *sql.Tx) ([]identityTrigger, error) {
	rows, err := tx.QueryContext(ctx, `
		select name, sql
		from sqlite_schema
		where type = 'trigger' and sql is not null
		order by name
	`)
	if err != nil {
		return nil, fmt.Errorf("list artifact identity triggers: %w", err)
	}
	defer rows.Close()
	var triggers []identityTrigger
	for rows.Next() {
		var trigger identityTrigger
		if err := rows.Scan(&trigger.Name, &trigger.SQL); err != nil {
			return nil, fmt.Errorf("scan artifact identity trigger: %w", err)
		}
		triggers = append(triggers, trigger)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read artifact identity triggers: %w", err)
	}
	return triggers, nil
}

func canonicalizeArtifactIdentityRows(
	ctx context.Context,
	tx *sql.Tx,
	table string,
	columns []string,
	rowOrder []string,
) error {
	if len(columns) == 0 {
		return nil
	}
	available := make(map[string]struct{}, len(columns))
	columnSQL := make([]string, 0, len(columns))
	for _, column := range columns {
		available[column] = struct{}{}
		columnSQL = append(columnSQL, quoteIdentifier(column))
	}
	orderSQL := make([]string, 0, len(rowOrder))
	for _, column := range rowOrder {
		if _, ok := available[column]; !ok {
			return fmt.Errorf("artifact identity row order column %s.%s is missing", table, column)
		}
		orderSQL = append(orderSQL, quoteIdentifier(column)+` collate binary`)
	}
	const tempTable = "_gitcrawl_artifact_identity_rows"
	if _, err := tx.ExecContext(ctx, `drop table if exists temp.`+quoteIdentifier(tempTable)); err != nil {
		return fmt.Errorf("prepare artifact identity rows for %s: %w", table, err)
	}
	sourceSelectSQL := `select ` + strings.Join(columnSQL, ", ") + ` from ` + quoteIdentifier(table) +
		` order by ` + strings.Join(orderSQL, ", ")
	if _, err := tx.ExecContext(ctx, `create temp table `+quoteIdentifier(tempTable)+` as `+sourceSelectSQL); err != nil {
		return fmt.Errorf("capture canonical artifact identity rows for %s: %w", table, err)
	}
	if _, err := tx.ExecContext(ctx, `delete from `+quoteIdentifier(table)); err != nil {
		return fmt.Errorf("clear artifact identity rows for %s: %w", table, err)
	}
	tempSelectSQL := `select ` + strings.Join(columnSQL, ", ") + ` from temp.` + quoteIdentifier(tempTable) +
		` order by ` + strings.Join(orderSQL, ", ")
	if _, err := tx.ExecContext(ctx, `insert into `+quoteIdentifier(table)+` (`+strings.Join(columnSQL, ", ")+") "+tempSelectSQL); err != nil {
		return fmt.Errorf("restore canonical artifact identity rows for %s: %w", table, err)
	}
	if _, err := tx.ExecContext(ctx, `drop table temp.`+quoteIdentifier(tempTable)); err != nil {
		return fmt.Errorf("drop canonical artifact identity rows for %s: %w", table, err)
	}
	return nil
}

func validateArtifactIdentityPolicy(policy artifactIdentityPolicy) error {
	if policy.Profile == "" {
		return fmt.Errorf("artifact identity policy profile is empty")
	}
	seen := make(map[string]string)
	for _, object := range policy.DroppedTriggers {
		if err := validateIdentityPolicyName(object); err != nil {
			return err
		}
		key := "trigger:" + object
		if previous, ok := seen[key]; ok {
			return fmt.Errorf("artifact identity policy repeats %s as %s", object, previous)
		}
		seen[key] = "trigger"
	}
	for _, object := range policy.DroppedTables {
		if err := validateIdentityPolicyName(object); err != nil {
			return err
		}
		key := "table:" + object
		if previous, ok := seen[key]; ok {
			return fmt.Errorf("artifact identity policy repeats %s as %s", object, previous)
		}
		seen[key] = "dropped table"
	}
	for _, table := range policy.Tables {
		if err := validateIdentityPolicyName(table.Name); err != nil {
			return err
		}
		key := "table:" + table.Name
		if previous, ok := seen[key]; ok {
			return fmt.Errorf("artifact identity policy repeats %s as %s and normalized table", table.Name, previous)
		}
		seen[key] = "normalized table"
		columns := make(map[string]struct{})
		for _, column := range table.Columns {
			if err := validateIdentityPolicyName(column.Name); err != nil {
				return err
			}
			if _, ok := columns[column.Name]; ok {
				return fmt.Errorf("artifact identity policy repeats column %s.%s", table.Name, column.Name)
			}
			columns[column.Name] = struct{}{}
			if column.DeclaredType != "TEXT" && column.DeclaredType != "INTEGER" {
				return fmt.Errorf("artifact identity column %s.%s has unsupported type %q", table.Name, column.Name, column.DeclaredType)
			}
			if column.Kind < identityEmptyText || column.Kind > identityOneInteger {
				return fmt.Errorf("artifact identity column %s.%s has unknown normalization %d", table.Name, column.Name, column.Kind)
			}
		}
	}
	return nil
}

func validateIdentityPolicyName(name string) error {
	if name == "" || strings.ContainsAny(name, "\x00\"") {
		return fmt.Errorf("artifact identity policy has unsafe name %q", name)
	}
	return nil
}
