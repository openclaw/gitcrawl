package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

func (s *Store) canonicalizePortableSchema(ctx context.Context, options PortablePruneOptions, stats *PortablePruneStats) error {
	bodyChars := options.BodyChars
	includeSyncFailures := options.IncludeSyncFailures
	retainSanitizedPayloadColumns := options.RetainSanitizedPayloadColumns
	if s.hasColumn(ctx, "threads", "body") && !s.hasColumn(ctx, "threads", "body_excerpt") {
		if _, err := s.db.ExecContext(ctx, `alter table threads add column body_excerpt text`); err != nil {
			return fmt.Errorf("add portable threads.body_excerpt: %w", err)
		}
		if _, err := s.db.ExecContext(ctx, `
			update threads
			   set body_excerpt = case when length(body) > ? then substr(body, 1, ?) else body end
			 where body is not null
		`, bodyChars, bodyChars); err != nil {
			return fmt.Errorf("backfill portable body excerpts: %w", err)
		}
	}
	if !s.hasColumn(ctx, "threads", "body_length") {
		if _, err := s.db.ExecContext(ctx, `alter table threads add column body_length integer not null default 0`); err != nil {
			return fmt.Errorf("add portable threads.body_length: %w", err)
		}
	}
	if retainSanitizedPayloadColumns {
		// Derived exports drop discarded relationship-bearing tables before the
		// single indexed FK proof performed by the threads rebuild.
		reportPortablePruneProgress(options.Progress, PortablePruneStageDisposableTableDrop)
		if err := s.dropPortableDerivedBlobPointerColumns(ctx, stats); err != nil {
			return err
		}
		if err := s.dropCanonicalPortableTablesBulk(ctx, includeSyncFailures, stats, options.tableDropHook); err != nil {
			return err
		}
		droppedIndexes, err := s.rebuildPortableCompatibilityThreadsWithOptions(ctx, options, stats)
		if err != nil {
			return err
		}
		stats.DroppedIndexes = append(stats.DroppedIndexes, droppedIndexes...)
		if err := s.sanitizePortableRepositoryCompatibilityColumn(ctx); err != nil {
			return err
		}
	}
	for _, column := range []struct {
		table string
		name  string
	}{
		{table: "repositories", name: "raw_json"},
		{table: "threads", name: "raw_json"},
		{table: "threads", name: "body"},
	} {
		if retainSanitizedPayloadColumns {
			continue
		}
		if !s.hasColumn(ctx, column.table, column.name) {
			continue
		}
		if _, err := s.db.ExecContext(ctx, `alter table `+sqliteIdentifier(column.table)+` drop column `+sqliteIdentifier(column.name)); err != nil {
			return fmt.Errorf("drop portable column %s.%s: %w", column.table, column.name, err)
		}
		stats.DroppedColumns = append(stats.DroppedColumns, column.table+"."+column.name)
	}
	if !retainSanitizedPayloadColumns {
		if err := s.dropCanonicalPortableTables(ctx, includeSyncFailures, stats); err != nil {
			return err
		}
	}
	if err := s.ensurePortableMetadata(ctx); err != nil {
		return err
	}
	capabilities := "body_excerpts,comment_excerpts,author_association,thread_revisions,thread_fingerprints,thread_key_summaries,pr_details,pr_files,pr_commits,pr_checks,pr_review_threads,workflow_runs,family_tombstones,comment_revisions,pr_review_thread_revisions,raw_json_stripped"
	includes := "repositories,threads,comments,comment_revisions,thread_revisions,thread_fingerprints,thread_key_summaries,pull_request_details,pull_request_files,pull_request_commits,pull_request_checks,pull_request_review_threads,pull_request_review_thread_revisions,pull_request_review_thread_syncs,github_workflow_runs"
	excluded := "raw_json,pull_request_file_patches,documents,fts,vectors,code_snapshots,code_documents,cluster_events,run_history,similarity_edges,blobs,sync_attempt_failures"
	if stats.SyncFailuresIncluded {
		capabilities += ",sync_failure_ledger_redacted"
		includes += ",sync_attempt_failures"
		excluded = strings.ReplaceAll(excluded, ",sync_attempt_failures", "")
	}
	metadata := map[string]string{
		"schema":                portableSchemaFormat,
		"body_chars":            fmt.Sprintf("%d", bodyChars),
		"capabilities":          capabilities,
		"includes":              includes,
		"excluded":              excluded,
		"exported_at":           time.Now().UTC().Format(timeLayout),
		"source_path":           s.path,
		"thread_author_profile": "login,type,association",
	}
	if retainSanitizedPayloadColumns {
		metadata["column_profile"] = PortableColumnProfileSanitizedCompatibility
	} else if _, err := s.db.ExecContext(ctx, `delete from portable_metadata where key = 'column_profile'`); err != nil {
		return fmt.Errorf("clear portable column profile metadata: %w", err)
	}
	for key, value := range metadata {
		if _, err := s.db.ExecContext(ctx, `
			insert into portable_metadata(key, value)
			values(?, ?)
			on conflict(key) do update set value = excluded.value
		`, key, value); err != nil {
			return fmt.Errorf("write portable metadata %s: %w", key, err)
		}
	}
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf(`pragma user_version = %d`, portableSchemaVersion)); err != nil {
		return fmt.Errorf("set portable schema compatibility version: %w", err)
	}
	return nil
}

func (s *Store) dropPortableDerivedBlobPointerColumns(ctx context.Context, stats *PortablePruneStats) error {
	for _, column := range []struct {
		table string
		name  string
	}{
		{table: "comments", name: "raw_json_blob_id"},
		{table: "thread_revisions", name: "raw_json_blob_id"},
	} {
		if !s.hasColumn(ctx, column.table, column.name) {
			continue
		}
		if _, err := s.db.ExecContext(ctx, `alter table `+sqliteIdentifier(column.table)+` drop column `+sqliteIdentifier(column.name)); err != nil {
			return fmt.Errorf("drop derived portable column %s.%s: %w", column.table, column.name, err)
		}
		name := column.table + "." + column.name
		if !slices.Contains(stats.DroppedColumns, name) {
			stats.DroppedColumns = append(stats.DroppedColumns, name)
		}
	}
	return nil
}

func (s *Store) dropCanonicalPortableTables(ctx context.Context, includeSyncFailures bool, stats *PortablePruneStats) error {
	for _, table := range canonicalPortableDroppedTables() {
		if table == "sync_attempt_failures" && includeSyncFailures {
			continue
		}
		if !s.tableExists(ctx, table) {
			continue
		}
		if _, err := s.db.ExecContext(ctx, `drop table if exists `+sqliteIdentifier(table)); err != nil {
			return fmt.Errorf("drop portable table %s: %w", table, err)
		}
		stats.DroppedTables = append(stats.DroppedTables, table)
	}
	return nil
}

func (s *Store) dropCanonicalPortableTablesBulk(ctx context.Context, includeSyncFailures bool, stats *PortablePruneStats, hook portableTableDropHookFunc) (retErr error) {
	if stats == nil {
		stats = &PortablePruneStats{}
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("open portable disposable table drop connection: %w", err)
	}
	defer conn.Close()
	var originalForeignKeys int
	if err := conn.QueryRowContext(ctx, `pragma foreign_keys`).Scan(&originalForeignKeys); err != nil {
		return fmt.Errorf("read portable disposable table foreign keys: %w", err)
	}
	var originalJournalMode string
	if err := conn.QueryRowContext(ctx, `pragma journal_mode`).Scan(&originalJournalMode); err != nil {
		return fmt.Errorf("read portable disposable table journal mode: %w", err)
	}
	restoreForeignKeys := false
	restoreJournalMode := false
	defer func() {
		if restoreJournalMode {
			if err := setPortableJournalMode(context.Background(), conn, originalJournalMode); err != nil {
				retErr = errors.Join(retErr, err)
			}
		}
		if restoreForeignKeys {
			if err := setPortableForeignKeys(context.Background(), conn, originalForeignKeys); err != nil {
				retErr = errors.Join(retErr, err)
			}
		}
	}()
	// The staging database normally uses journal_mode=OFF. MEMORY provides real
	// rollback semantics for this one private DDL transaction without adding a
	// durable journal that survives process failure.
	restoreJournalMode = true
	if err := setPortableJournalMode(ctx, conn, "memory"); err != nil {
		return err
	}
	restoreForeignKeys = true
	if err := setPortableForeignKeys(ctx, conn, 0); err != nil {
		return err
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin portable disposable table drop: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	presentBefore := make(map[string]string)
	for _, table := range canonicalPortableBulkDropOrder() {
		if table == "sync_attempt_failures" && includeSyncFailures {
			continue
		}
		actual, present, err := portableSchemaTableName(ctx, tx, table)
		if err != nil {
			return err
		}
		if present {
			presentBefore[table] = actual
		}
	}
	for _, table := range canonicalPortableBulkDropOrder() {
		if presentBefore[table] == "" {
			continue
		}
		actual, present, err := portableSchemaTableName(ctx, tx, table)
		if err != nil {
			return err
		}
		if !present {
			continue
		}
		if hook != nil {
			if err := hook(ctx, tx, actual); err != nil {
				return fmt.Errorf("portable disposable table drop hook for %s: %w", actual, err)
			}
		}
		if _, err := tx.ExecContext(ctx, `drop table `+sqliteIdentifier(actual)); err != nil {
			return fmt.Errorf("drop portable disposable table %s: %w", actual, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit portable disposable table drop: %w", err)
	}
	if err := setPortableJournalMode(context.Background(), conn, originalJournalMode); err != nil {
		return err
	}
	restoreJournalMode = false
	if err := setPortableForeignKeys(context.Background(), conn, originalForeignKeys); err != nil {
		return err
	}
	restoreForeignKeys = false
	var removed []string
	for _, table := range canonicalPortableBulkDropOrder() {
		if table == "sync_attempt_failures" && includeSyncFailures {
			continue
		}
		_, present, err := portableSchemaTableName(ctx, conn, table)
		if err != nil {
			return err
		}
		if present {
			return fmt.Errorf("portable disposable table %s remains after bulk drop", table)
		}
		removed = append(removed, table)
		actual := presentBefore[table]
		if actual == "" {
			continue
		}
		if !slices.Contains(stats.DroppedTables, actual) {
			stats.DroppedTables = append(stats.DroppedTables, actual)
		}
	}
	if err := validatePortableRetainedSchemaDependencies(ctx, conn, removed); err != nil {
		return err
	}
	return nil
}

func setPortableForeignKeys(ctx context.Context, conn *sql.Conn, enabled int) error {
	if enabled != 0 && enabled != 1 {
		return fmt.Errorf("invalid portable foreign_keys setting %d", enabled)
	}
	if _, err := conn.ExecContext(ctx, fmt.Sprintf(`pragma foreign_keys = %d`, enabled)); err != nil {
		return fmt.Errorf("set portable disposable table foreign keys to %d: %w", enabled, err)
	}
	var actual int
	if err := conn.QueryRowContext(ctx, `pragma foreign_keys`).Scan(&actual); err != nil {
		return fmt.Errorf("verify portable disposable table foreign keys: %w", err)
	}
	if actual != enabled {
		return fmt.Errorf("set portable disposable table foreign keys to %d: pragma remained %d", enabled, actual)
	}
	return nil
}

func setPortableJournalMode(ctx context.Context, conn *sql.Conn, mode string) error {
	want := strings.ToLower(strings.TrimSpace(mode))
	switch want {
	case "delete", "truncate", "persist", "memory", "wal", "off":
	default:
		return fmt.Errorf("invalid portable journal_mode %q", mode)
	}
	var actual string
	if err := conn.QueryRowContext(ctx, `pragma journal_mode = `+want).Scan(&actual); err != nil {
		return fmt.Errorf("set portable disposable table journal mode to %s: %w", want, err)
	}
	if !strings.EqualFold(actual, want) {
		return fmt.Errorf("set portable disposable table journal mode to %s: pragma remained %s", want, actual)
	}
	return nil
}

func portableSchemaTableName(ctx context.Context, q dbQueries, table string) (string, bool, error) {
	var actual string
	err := q.QueryRowContext(ctx, `
		select name
		from sqlite_schema
		where type = 'table' and name collate nocase = ?
	`, table).Scan(&actual)
	if err == sql.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("inspect portable disposable table %s: %w", table, err)
	}
	return actual, true, nil
}

type portableRetainedSchemaObject struct {
	kind string
	name string
	sql  string
}

func validatePortableRetainedSchemaDependencies(ctx context.Context, q dbQueries, dropped []string) error {
	if len(dropped) == 0 {
		return nil
	}
	droppedSet := make(map[string]bool, len(dropped))
	for _, table := range dropped {
		droppedSet[strings.ToLower(table)] = true
	}
	tables, err := portableTableNames(ctx, q)
	if err != nil {
		return err
	}
	for _, table := range tables {
		definitions, err := portableForeignKeysForTable(ctx, q, table)
		if err != nil {
			return err
		}
		for _, definition := range definitions {
			if droppedSet[strings.ToLower(definition.referencedTable)] {
				return fmt.Errorf("retained portable table %s foreign key references dropped table %s", table, definition.referencedTable)
			}
		}
	}
	rows, err := q.QueryContext(ctx, `
		select type, name, sql
		from sqlite_schema
		where type in ('view', 'trigger') and sql is not null
		order by type, name
	`)
	if err != nil {
		return fmt.Errorf("inspect retained portable schema dependencies: %w", err)
	}
	var objects []portableRetainedSchemaObject
	for rows.Next() {
		var object portableRetainedSchemaObject
		if err := rows.Scan(&object.kind, &object.name, &object.sql); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan retained portable schema dependency: %w", err)
		}
		objects = append(objects, object)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("read retained portable schema dependencies: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close retained portable schema dependencies: %w", err)
	}
	for _, object := range objects {
		switch object.kind {
		case "view":
			viewIdentifier, err := portableRuntimeIdentifier(object.name)
			if err != nil {
				return fmt.Errorf("quote retained portable view %s: %w", object.name, err)
			}
			viewRows, err := q.QueryContext(ctx, `select * from `+viewIdentifier+` limit 0`)
			if err != nil {
				return fmt.Errorf("retained portable view %s is invalid after disposable table drop: %w", object.name, err)
			}
			if _, err := viewRows.Columns(); err != nil {
				_ = viewRows.Close()
				return fmt.Errorf("retained portable view %s is invalid after disposable table drop: %w", object.name, err)
			}
			for viewRows.Next() {
			}
			if err := viewRows.Err(); err != nil {
				_ = viewRows.Close()
				return fmt.Errorf("retained portable view %s is invalid after disposable table drop: %w", object.name, err)
			}
			if err := viewRows.Close(); err != nil {
				return fmt.Errorf("close retained portable view %s validation: %w", object.name, err)
			}
		case "trigger":
			dependency, err := portableTriggerDroppedDependency(object.sql, droppedSet)
			if err != nil {
				return fmt.Errorf("inspect retained portable trigger %s: %w", object.name, err)
			}
			if dependency != "" {
				return fmt.Errorf("retained portable trigger %s references dropped table %s", object.name, dependency)
			}
		}
	}
	return nil
}

func (s *Store) sanitizePortableRepositoryCompatibilityColumn(ctx context.Context) error {
	if s.hasColumn(ctx, "repositories", "raw_json") {
		if _, err := s.db.ExecContext(ctx, `update repositories set raw_json = '' where raw_json != ''`); err != nil {
			return fmt.Errorf("sanitize portable repository compatibility column: %w", err)
		}
	}
	return nil
}

func canonicalPortableDroppedTables() []string {
	return []string{
		"code_documents_fts",
		"code_documents_fts_config",
		"code_documents_fts_data",
		"code_documents_fts_docsize",
		"code_documents_fts_idx",
		"code_documents",
		"code_snapshots",
		"documents_fts",
		"documents_fts_config",
		"documents_fts_data",
		"documents_fts_docsize",
		"documents_fts_idx",
		"documents",
		"document_embeddings",
		"document_summaries",
		"thread_vectors",
		"thread_code_snapshots",
		"thread_changed_files",
		"thread_hunk_signatures",
		"cluster_events",
		"cluster_members",
		"clusters",
		"sync_runs",
		"summary_runs",
		"embedding_runs",
		"cluster_runs",
		"similarity_edges",
		"blobs",
		"sync_attempt_failures",
	}
}

func canonicalPortableBulkDropOrder() []string {
	return []string{
		"code_documents_fts",
		"code_documents_fts_config",
		"code_documents_fts_data",
		"code_documents_fts_docsize",
		"code_documents_fts_idx",
		"code_documents",
		"code_snapshots",
		"documents_fts",
		"documents_fts_config",
		"documents_fts_data",
		"documents_fts_docsize",
		"documents_fts_idx",
		"documents",
		"document_embeddings",
		"document_summaries",
		"thread_vectors",
		"thread_changed_files",
		"thread_hunk_signatures",
		"thread_code_snapshots",
		"cluster_events",
		"cluster_members",
		"clusters",
		"similarity_edges",
		"sync_attempt_failures",
		"sync_runs",
		"summary_runs",
		"embedding_runs",
		"cluster_runs",
		"blobs",
	}
}
