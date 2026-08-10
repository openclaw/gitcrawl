package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"
)

const (
	portableSchemaVersion = 4
	portableSchemaFormat  = "gitcrawl-portable-sync-v2"
)

const portableSyncFailureErrorRedaction = "[redacted for portable export]"

const portableSyncFailureScrubPendingKey = "sync_failure_scrub_pending"

const PortableColumnProfileSanitizedCompatibility = "sanitized-compatibility"

type PortablePruneStage string

const (
	PortablePruneStageThreadBodies                PortablePruneStage = "thread bodies"
	PortablePruneStageCommentReviewBodies         PortablePruneStage = "comment and review bodies"
	PortablePruneStageMetadataRawPayloads         PortablePruneStage = "metadata and raw payload cleanup"
	PortablePruneStageFingerprintsSummaries       PortablePruneStage = "fingerprints and summaries"
	PortablePruneStageDiscardedData               PortablePruneStage = "discarded tables and failure ledger"
	PortablePruneStageCanonicalSchemaFinalization PortablePruneStage = "canonical schema finalization"
	PortablePruneStageDisposableTableDrop         PortablePruneStage = "canonical schema finalization: disposable table drop"
	PortablePruneStageThreadsRebuildPreflight     PortablePruneStage = "threads rebuild: preflight"
	PortablePruneStageThreadsRebuildForeignKeys   PortablePruneStage = "threads rebuild: foreign key proof"
	PortablePruneStageThreadsRebuildCompactCopy   PortablePruneStage = "threads rebuild: compact copy"
	PortablePruneStageThreadsRebuildSchemaSwap    PortablePruneStage = "threads rebuild: schema swap"
	PortablePruneStageThreadsRebuildSchemaRestore PortablePruneStage = "threads rebuild: schema restore"
)

type PortablePruneProgressFunc func(PortablePruneStage)

type portableForeignKeyCheckFunc func(context.Context, dbQueries) (int, error)

type portableThreadsRebuildHookStage string

const portableThreadsRebuildHookAfterCopy portableThreadsRebuildHookStage = "after-copy"

type portableThreadsRebuildHookFunc func(portableThreadsRebuildHookStage, *sql.Tx, string) error

type portableTableDropHookFunc func(context.Context, *sql.Tx, string) error

type PortablePruneOptions struct {
	BodyChars           int
	Vacuum              bool
	IncludeSyncFailures bool
	// DeferSecureRewrite guarantees that a derived, non-visible snapshot receives
	// one final VACUUM. It may therefore skip writes to columns or tables that
	// canonical shaping removes. The in-place portable prune command must leave
	// this false so --no-vacuum still scrubs visible payloads before returning.
	DeferSecureRewrite            bool
	RetainSanitizedPayloadColumns bool
	Progress                      PortablePruneProgressFunc `json:"-"`
	foreignKeyCheck               portableForeignKeyCheckFunc
	threadsRebuildHook            portableThreadsRebuildHookFunc
	tableDropHook                 portableTableDropHookFunc
}

type PortablePruneStats struct {
	DBPath                    string   `json:"db_path"`
	ManifestPath              string   `json:"manifest_path,omitempty"`
	SHA256                    string   `json:"sha256,omitempty"`
	BodyChars                 int      `json:"body_chars"`
	BytesBefore               int64    `json:"bytes_before"`
	BytesAfter                int64    `json:"bytes_after"`
	QuickCheck                string   `json:"quick_check,omitempty"`
	ThreadsPruned             int64    `json:"threads_pruned"`
	CommentsPruned            int64    `json:"comments_pruned"`
	ThreadLabelsCompacted     int64    `json:"thread_labels_compacted"`
	ThreadAssigneesCompacted  int64    `json:"thread_assignees_compacted"`
	RepositoriesPruned        int64    `json:"repositories_pruned"`
	RawJSONPruned             int64    `json:"raw_json_pruned"`
	FingerprintsPruned        int64    `json:"fingerprints_pruned"`
	LegacySummariesDeleted    int64    `json:"legacy_summaries_deleted"`
	DocumentsDeleted          int64    `json:"documents_deleted"`
	DocumentsFTSRebuilt       bool     `json:"documents_fts_rebuilt"`
	SyncFailuresIncluded      bool     `json:"sync_failures_included"`
	SyncFailureErrorsRedacted int64    `json:"sync_failure_errors_redacted"`
	SyncFailureVacuumForced   bool     `json:"sync_failure_vacuum_forced"`
	DroppedTables             []string `json:"dropped_tables,omitempty"`
	DroppedColumns            []string `json:"dropped_columns,omitempty"`
	DroppedIndexes            []string `json:"dropped_indexes,omitempty"`
	ForeignKeyValidated       bool     `json:"foreign_key_validated,omitempty"`
	ForeignKeyViolations      int      `json:"foreign_key_violations,omitempty"`
	Vacuumed                  bool     `json:"vacuumed"`
}

type PortableRepositoryScope struct {
	Repository          Repository `json:"repository"`
	RepositoriesBefore  int64      `json:"repositories_before"`
	RepositoriesRemoved int64      `json:"repositories_removed"`
}

type PortableRepositoryScopeOptions struct {
	// DeferForeignKeyValidation is reserved for disposable pipelines that run
	// one equivalent proof after all remaining semantic mutations.
	DeferForeignKeyValidation bool
}

// RestrictPortableRepository reduces a disposable portable snapshot to one
// repository. It combines current foreign-key cascades with column-aware
// deletion so repository-owned rows remain covered as the schema evolves.
func (s *Store) RestrictPortableRepository(ctx context.Context, fullName string) (PortableRepositoryScope, error) {
	return s.RestrictPortableRepositoryWithOptions(ctx, fullName, PortableRepositoryScopeOptions{})
}

func (s *Store) RestrictPortableRepositoryWithOptions(ctx context.Context, fullName string, options PortableRepositoryScopeOptions) (PortableRepositoryScope, error) {
	var result PortableRepositoryScope
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return result, fmt.Errorf("open portable repository scope connection: %w", err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `pragma foreign_keys = on`); err != nil {
		return result, fmt.Errorf("enable portable repository foreign keys: %w", err)
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return result, fmt.Errorf("begin portable repository scope: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var targetCount int64
	if err := tx.QueryRowContext(ctx, `select count(*) from repositories where full_name = ?`, fullName).Scan(&targetCount); err != nil {
		return result, fmt.Errorf("count portable target repository: %w", err)
	}
	if targetCount != 1 {
		return result, fmt.Errorf("portable export target repository %q count is %d, expected 1", fullName, targetCount)
	}
	if err := tx.QueryRowContext(ctx, `
		select id, owner, name, full_name
		from repositories
		where full_name = ?
	`, fullName).Scan(
		&result.Repository.ID,
		&result.Repository.Owner,
		&result.Repository.Name,
		&result.Repository.FullName,
	); err != nil {
		return result, fmt.Errorf("read portable target repository: %w", err)
	}
	if err := tx.QueryRowContext(ctx, `select count(*) from repositories`).Scan(&result.RepositoriesBefore); err != nil {
		return result, fmt.Errorf("count portable repositories before scope: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		create temp table _portable_scope_threads as
		select id from threads where repo_id != ?
	`, result.Repository.ID); err != nil {
		return result, fmt.Errorf("capture out-of-scope portable threads: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		create temp table _portable_scope_revisions as
		select id from thread_revisions where thread_id in (select id from _portable_scope_threads)
	`); err != nil {
		return result, fmt.Errorf("capture out-of-scope portable revisions: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		create temp table _portable_scope_clusters as
		select id from cluster_groups where repo_id != ?
	`, result.Repository.ID); err != nil {
		return result, fmt.Errorf("capture out-of-scope portable clusters: %w", err)
	}
	tables, err := portableTableNames(ctx, tx)
	if err != nil {
		return result, err
	}
	for _, table := range tables {
		switch table {
		case "repositories", "threads", "thread_revisions", "cluster_groups":
			continue
		}
		columns, err := portableTableColumnSet(ctx, tx, table)
		if err != nil {
			return result, err
		}
		var conditions []string
		var args []any
		if columns["thread_revision_id"] {
			conditions = append(conditions, `thread_revision_id in (select id from _portable_scope_revisions)`)
		}
		if columns["thread_id"] {
			conditions = append(conditions, `thread_id in (select id from _portable_scope_threads)`)
		}
		if columns["cluster_id"] {
			conditions = append(conditions, `cluster_id in (select id from _portable_scope_clusters)`)
		}
		if columns["repo_id"] {
			conditions = append(conditions, `repo_id != ?`)
			args = append(args, result.Repository.ID)
		}
		if len(conditions) == 0 {
			continue
		}
		query := `delete from ` + sqliteIdentifier(table) + ` where ` + strings.Join(conditions, " or ")
		if _, err := tx.ExecContext(ctx, query, args...); err != nil {
			return result, fmt.Errorf("delete out-of-scope portable rows from %s: %w", table, err)
		}
	}
	for _, statement := range []string{
		`delete from thread_revisions where id in (select id from _portable_scope_revisions)`,
		`delete from cluster_groups where id in (select id from _portable_scope_clusters)`,
		`delete from threads where id in (select id from _portable_scope_threads)`,
	} {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return result, fmt.Errorf("finalize portable repository scope: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `delete from repositories where id != ?`, result.Repository.ID); err != nil {
		return result, fmt.Errorf("finalize portable repository scope: %w", err)
	}
	var repositoryCount, matchingCount int64
	if err := tx.QueryRowContext(ctx, `select count(*) from repositories`).Scan(&repositoryCount); err != nil {
		return result, fmt.Errorf("verify portable repository count: %w", err)
	}
	if err := tx.QueryRowContext(ctx, `select count(*) from repositories where id = ? and full_name = ?`, result.Repository.ID, fullName).Scan(&matchingCount); err != nil {
		return result, fmt.Errorf("verify portable target repository: %w", err)
	}
	if repositoryCount != 1 || matchingCount != 1 {
		return result, fmt.Errorf("portable repository restriction left %d repositories with %d matching %q", repositoryCount, matchingCount, fullName)
	}
	if err := tx.Commit(); err != nil {
		return result, fmt.Errorf("commit portable repository scope: %w", err)
	}
	if !options.DeferForeignKeyValidation {
		violations, err := portableForeignKeyViolationCount(ctx, conn)
		if err != nil {
			return result, err
		}
		if violations != 0 {
			return result, fmt.Errorf("portable repository restriction left %d foreign-key violations", violations)
		}
	}
	result.RepositoriesRemoved = result.RepositoriesBefore - 1
	return result, nil
}

func portableTableNames(ctx context.Context, q dbQueries) ([]string, error) {
	rows, err := q.QueryContext(ctx, `
		select name
		from sqlite_schema
		where type = 'table' and name not like 'sqlite_%'
		order by name
	`)
	if err != nil {
		return nil, fmt.Errorf("list portable repository scope tables: %w", err)
	}
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, fmt.Errorf("scan portable repository scope table: %w", err)
		}
		tables = append(tables, table)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read portable repository scope tables: %w", err)
	}
	return tables, nil
}

func portableTableColumnSet(ctx context.Context, q dbQueries, table string) (map[string]bool, error) {
	rows, err := q.QueryContext(ctx, `pragma table_info(`+sqliteIdentifier(table)+`)`)
	if err != nil {
		return nil, fmt.Errorf("inspect portable repository scope table %s: %w", table, err)
	}
	defer rows.Close()
	columns := make(map[string]bool)
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, columnType string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			return nil, fmt.Errorf("scan portable repository scope table %s: %w", table, err)
		}
		columns[name] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read portable repository scope table %s: %w", table, err)
	}
	return columns, nil
}

func portableForeignKeyViolationCount(ctx context.Context, q dbQueries) (int, error) {
	rows, err := q.QueryContext(ctx, `pragma foreign_key_check`)
	if err != nil {
		return 0, fmt.Errorf("run portable repository foreign_key_check: %w", err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("read portable repository foreign_key_check: %w", err)
	}
	return count, nil
}

func (s *Store) PrunePortablePayloads(ctx context.Context, options PortablePruneOptions) (PortablePruneStats, error) {
	if options.BodyChars <= 0 {
		options.BodyChars = 256
	}
	stats := PortablePruneStats{
		DBPath:    s.path,
		BodyChars: options.BodyChars,
	}
	if info, err := os.Stat(s.path); err == nil {
		stats.BytesBefore = info.Size()
	}

	reportPortablePruneProgress(options.Progress, PortablePruneStageThreadBodies)
	if err := s.preparePortableThreadPayloads(ctx, options, &stats); err != nil {
		return stats, err
	}
	reportPortablePruneProgress(options.Progress, PortablePruneStageCommentReviewBodies)
	if s.tableExists(ctx, "comments") && s.hasColumn(ctx, "comments", "body") {
		if err := s.ensurePortableExcerptColumns(ctx, "comments"); err != nil {
			return stats, err
		}
		if result, err := s.db.ExecContext(ctx, `
			update comments
			   set body_length = length(body),
			       body_excerpt = case when length(body) > ? then substr(body, 1, ?) else body end,
			       body = case when length(body) > ? then substr(body, 1, ?) else body end
		`, options.BodyChars, options.BodyChars, options.BodyChars, options.BodyChars); err != nil {
			return stats, fmt.Errorf("prune comment bodies: %w", err)
		} else {
			stats.CommentsPruned = rowsAffected(result)
		}
	}
	if s.tableExists(ctx, "comment_revisions") {
		if _, err := s.db.ExecContext(ctx, `
			update comment_revisions
			set body = case when length(body) > ? then substr(body, 1, ?) else body end
		`, options.BodyChars, options.BodyChars); err != nil {
			return stats, fmt.Errorf("prune comment revision bodies: %w", err)
		}
	}
	if err := s.compactPortableReviewThreadBodies(ctx, options.BodyChars); err != nil {
		return stats, err
	}
	reportPortablePruneProgress(options.Progress, PortablePruneStageMetadataRawPayloads)
	if labels, assignees, err := s.compactPortableThreadMetadata(ctx); err != nil {
		return stats, err
	} else {
		stats.ThreadLabelsCompacted = labels
		stats.ThreadAssigneesCompacted = assignees
	}
	if pruned, err := s.clearPortableRawJSON(ctx); err != nil {
		return stats, err
	} else {
		stats.RawJSONPruned = pruned
	}
	if err := s.clearPortablePullRequestFilePatches(ctx); err != nil {
		return stats, err
	}
	reportPortablePruneProgress(options.Progress, PortablePruneStageFingerprintsSummaries)
	if s.tableExists(ctx, "thread_fingerprints") {
		result, err := s.db.ExecContext(ctx, `
			update thread_fingerprints
			   set title_tokens_json = '[]',
			       linked_refs_json = '[]',
			       module_buckets_json = '[]',
			       feature_json = '{}'
		`)
		if err != nil {
			return stats, fmt.Errorf("slim fingerprint details: %w", err)
		}
		stats.FingerprintsPruned = rowsAffected(result)
	}
	if deleted, err := s.pruneEquivalentLegacyKeySummaries(ctx); err != nil {
		return stats, err
	} else {
		stats.LegacySummariesDeleted = deleted
	}
	reportPortablePruneProgress(options.Progress, PortablePruneStageDiscardedData)
	if !options.DeferSecureRewrite && s.tableExists(ctx, "documents") {
		result, err := s.db.ExecContext(ctx, `delete from documents`)
		if err != nil {
			return stats, fmt.Errorf("delete generated documents: %w", err)
		}
		stats.DocumentsDeleted = rowsAffected(result)
	}
	if !options.DeferSecureRewrite && s.tableExists(ctx, "documents_fts") {
		if _, err := s.db.ExecContext(ctx, `insert into documents_fts(documents_fts) values('rebuild')`); err != nil {
			return stats, fmt.Errorf("rebuild document fts: %w", err)
		}
		stats.DocumentsFTSRebuilt = true
	}
	syncFailureScrubRequired := false
	if !(options.DeferSecureRewrite && !options.IncludeSyncFailures) {
		var err error
		syncFailureScrubRequired, err = s.scrubPortableSyncFailures(ctx, options.IncludeSyncFailures, &stats)
		if err != nil {
			return stats, err
		}
	}
	if syncFailureScrubRequired && !options.DeferSecureRewrite {
		if err := s.vacuumPortableDatabase(ctx); err != nil {
			return stats, err
		}
		stats.Vacuumed = true
		stats.SyncFailureVacuumForced = !options.Vacuum
		if _, err := s.db.ExecContext(ctx, `delete from portable_metadata where key = ?`, portableSyncFailureScrubPendingKey); err != nil {
			return stats, fmt.Errorf("clear portable sync failure scrub marker: %w", err)
		}
	}
	reportPortablePruneProgress(options.Progress, PortablePruneStageCanonicalSchemaFinalization)
	if err := s.canonicalizePortableSchema(ctx, options, &stats); err != nil {
		return stats, err
	}
	if options.Vacuum {
		if err := s.vacuumPortableDatabase(ctx); err != nil {
			return stats, err
		}
		stats.Vacuumed = true
	}
	if info, err := os.Stat(s.path); err == nil {
		stats.BytesAfter = info.Size()
	}
	return stats, nil
}

func reportPortablePruneProgress(progress PortablePruneProgressFunc, stage PortablePruneStage) {
	if progress != nil {
		progress(stage)
	}
}

func (s *Store) preparePortableThreadPayloads(ctx context.Context, options PortablePruneOptions, stats *PortablePruneStats) error {
	if s.hasColumn(ctx, "threads", "body") {
		if err := s.ensurePortableExcerptColumns(ctx, "threads"); err != nil {
			return err
		}
		result, err := s.db.ExecContext(ctx, `
			update threads
			   set body_length = case when body is not null then length(body) else body_length end,
			       body_excerpt = case
			         when body is not null and length(body) > ? then substr(body, 1, ?)
			         when body is not null then body
			         else body_excerpt
			       end
			 where body is not null
		`, options.BodyChars, options.BodyChars)
		if err != nil {
			return fmt.Errorf("prune thread body excerpts: %w", err)
		}
		stats.ThreadsPruned += rowsAffected(result)
		if !options.DeferSecureRewrite {
			if _, err := s.db.ExecContext(ctx, `update threads set body = body_excerpt`); err != nil {
				return fmt.Errorf("replace thread bodies with excerpts: %w", err)
			}
		}
	}
	if options.DeferSecureRewrite {
		return nil
	}
	if s.hasColumn(ctx, "threads", "raw_json") {
		if _, err := s.db.ExecContext(ctx, `update threads set raw_json = '' where raw_json is not null and raw_json != ''`); err != nil {
			return fmt.Errorf("clear thread raw json: %w", err)
		}
	}
	if s.hasColumn(ctx, "repositories", "raw_json") {
		result, err := s.db.ExecContext(ctx, `update repositories set raw_json = '' where raw_json is not null and raw_json != ''`)
		if err != nil {
			return fmt.Errorf("clear repository raw json: %w", err)
		}
		stats.RepositoriesPruned = rowsAffected(result)
	}
	return nil
}

func (s *Store) vacuumPortableDatabase(ctx context.Context) error {
	var busy, logFrames, checkpointedFrames int
	if err := s.db.QueryRowContext(ctx, `pragma wal_checkpoint(TRUNCATE)`).Scan(&busy, &logFrames, &checkpointedFrames); err != nil {
		return fmt.Errorf("checkpoint wal: %w", err)
	}
	if busy != 0 {
		return fmt.Errorf("checkpoint wal: busy with %d of %d frames checkpointed", checkpointedFrames, logFrames)
	}
	if _, err := s.db.ExecContext(ctx, `vacuum`); err != nil {
		return fmt.Errorf("vacuum database: %w", err)
	}
	return nil
}

func (s *Store) scrubPortableSyncFailures(ctx context.Context, include bool, stats *PortablePruneStats) (bool, error) {
	ledgerExists := s.tableExists(ctx, "sync_attempt_failures")
	pending, err := s.portableSyncFailureScrubPending(ctx)
	if err != nil {
		return false, err
	}
	if !ledgerExists && !pending {
		return false, nil
	}
	if !pending {
		if err := s.ensurePortableMetadata(ctx); err != nil {
			return false, err
		}
		if _, err := s.db.ExecContext(ctx, `
			insert into portable_metadata(key, value)
			values(?, '1')
			on conflict(key) do update set value = excluded.value
		`, portableSyncFailureScrubPendingKey); err != nil {
			return false, fmt.Errorf("mark portable sync failure scrub pending: %w", err)
		}
	}
	if !ledgerExists {
		return true, nil
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return false, fmt.Errorf("open portable sync failure scrub connection: %w", err)
	}
	defer conn.Close()
	var hasRows bool
	if err := conn.QueryRowContext(ctx, `select exists(select 1 from sync_attempt_failures limit 1)`).Scan(&hasRows); err != nil {
		return false, fmt.Errorf("inspect portable sync failures: %w", err)
	}
	if include {
		stats.SyncFailuresIncluded = true
	}
	if !hasRows {
		return true, nil
	}
	// VACUUM is optional, so securely overwrite private error bytes before the
	// table is dropped or its visible values are replaced. The caller also
	// rewrites the database to remove older retry values already on the freelist.
	if _, err := conn.ExecContext(ctx, `pragma secure_delete = on`); err != nil {
		return false, fmt.Errorf("enable secure deletion for portable sync failures: %w", err)
	}
	if !include {
		if _, err := conn.ExecContext(ctx, `delete from sync_attempt_failures`); err != nil {
			return false, fmt.Errorf("scrub portable sync failures: %w", err)
		}
		return true, nil
	}
	result, err := conn.ExecContext(ctx, `update sync_attempt_failures set error_message = ?`, portableSyncFailureErrorRedaction)
	if err != nil {
		return false, fmt.Errorf("redact portable sync failure errors: %w", err)
	}
	stats.SyncFailureErrorsRedacted = rowsAffected(result)
	return true, nil
}

func (s *Store) portableSyncFailureScrubPending(ctx context.Context) (bool, error) {
	if !s.tableExists(ctx, "portable_metadata") {
		return false, nil
	}
	var value string
	err := s.db.QueryRowContext(ctx, `select value from portable_metadata where key = ?`, portableSyncFailureScrubPendingKey).Scan(&value)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read portable sync failure scrub marker: %w", err)
	}
	return value == "1", nil
}

func (s *Store) ensurePortableMetadata(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `
		create table if not exists portable_metadata (
			key text primary key,
			value text not null
		)
	`); err != nil {
		return fmt.Errorf("ensure portable metadata: %w", err)
	}
	return nil
}

func (s *Store) pruneEquivalentLegacyKeySummaries(ctx context.Context) (int64, error) {
	if !s.hasColumn(ctx, "thread_key_summaries", "summary_kind") {
		return 0, nil
	}
	// Full stores retain the legacy kind for compatibility. Portable snapshots
	// only drop byte-identical copies after the canonical row is present.
	result, err := s.db.ExecContext(ctx, `
		delete from thread_key_summaries
		where summary_kind = ?
			and exists (
				select 1
				from thread_key_summaries as canonical
				where canonical.thread_revision_id = thread_key_summaries.thread_revision_id
					and canonical.summary_kind = ?
					and canonical.prompt_version = thread_key_summaries.prompt_version
					and canonical.provider = thread_key_summaries.provider
					and canonical.model = thread_key_summaries.model
					and canonical.input_hash = thread_key_summaries.input_hash
					and canonical.output_hash = thread_key_summaries.output_hash
					and canonical.key_text = thread_key_summaries.key_text
					and canonical.created_at = thread_key_summaries.created_at
			)
	`, summaryKindLegacyLLMKey3Line, SummaryKindLLMKey)
	if err != nil {
		return 0, fmt.Errorf("prune equivalent legacy key summaries: %w", err)
	}
	return rowsAffected(result), nil
}

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

func portableTriggerDroppedDependency(triggerSQL string, dropped map[string]bool) (string, error) {
	tokens, err := tokenizePortableSQL(triggerSQL)
	if err != nil {
		return "", err
	}
	begin := -1
	start := -1
	for index, token := range tokens {
		if !token.quoted && strings.EqualFold(token.value, "BEGIN") {
			begin = index + 1
			break
		}
		if !token.quoted && strings.EqualFold(token.value, "WHEN") {
			start = index + 1
		}
	}
	if begin < 0 {
		return "", fmt.Errorf("unrecognized trigger body")
	}
	if start >= 0 {
		if dependency := portableDroppedTableDependency(tokens[start:begin-1], dropped); dependency != "" {
			return dependency, nil
		}
	}
	for _, statement := range portableSQLStatements(tokens[begin:]) {
		if dependency := portableDroppedTableDependency(statement, dropped); dependency != "" {
			return dependency, nil
		}
	}
	return "", nil
}

func portableDroppedTableDependency(tokens []portableSQLToken, dropped map[string]bool) string {
	// CTE resolution is deliberately conservative: a removed-table name in a
	// real table position is rejected even when a CTE might shadow it.
	return portableDroppedTableDependencyInList(tokens, dropped, false)
}

func portableDroppedTableDependencyInList(tokens []portableSQLToken, dropped map[string]bool, initialFromList bool) string {
	wantTable := initialFromList
	fromList := initialFromList
	deleteTarget := false
	for index := 0; index < len(tokens); index++ {
		token := tokens[index]
		if wantTable {
			if portableSQLKeyword(token, "OR") && index+1 < len(tokens) {
				index++
				continue
			}
			if token.value == "(" {
				end := portableSQLMatchingParenthesis(tokens, index)
				if dependency := portableDroppedTableDependencyInList(tokens[index+1:end], dropped, true); dependency != "" {
					return dependency
				}
				wantTable = false
				index = end
				continue
			}
			name, consumed := portableSQLTableIdentifier(tokens[index:])
			wantTable = false
			if consumed > 0 {
				index += consumed - 1
			}
			if dropped[strings.ToLower(name)] {
				return name
			}
			continue
		}
		if token.value == "(" {
			end := portableSQLMatchingParenthesis(tokens, index)
			if dependency := portableDroppedTableDependencyInList(tokens[index+1:end], dropped, false); dependency != "" {
				return dependency
			}
			index = end
			continue
		}
		switch {
		case portableSQLKeyword(token, "DELETE"):
			deleteTarget = true
			fromList = false
		case portableSQLKeyword(token, "FROM"):
			wantTable = true
			fromList = !deleteTarget
			deleteTarget = false
		case portableSQLKeyword(token, "JOIN"):
			wantTable = true
		case portableSQLKeyword(token, "UPDATE"),
			portableSQLKeyword(token, "INTO"),
			portableSQLKeyword(token, "REFERENCES"):
			wantTable = true
		case fromList && token.value == ",":
			wantTable = true
		case portableSQLKeyword(token, "WHERE"),
			portableSQLKeyword(token, "GROUP"),
			portableSQLKeyword(token, "HAVING"),
			portableSQLKeyword(token, "ORDER"),
			portableSQLKeyword(token, "LIMIT"),
			portableSQLKeyword(token, "RETURNING"),
			portableSQLKeyword(token, "SET"),
			portableSQLKeyword(token, "VALUES"),
			portableSQLKeyword(token, "UNION"),
			portableSQLKeyword(token, "EXCEPT"),
			portableSQLKeyword(token, "INTERSECT"):
			fromList = false
		}
	}
	return ""
}

func portableSQLMatchingParenthesis(tokens []portableSQLToken, start int) int {
	depth := 0
	for index := start; index < len(tokens); index++ {
		switch tokens[index].value {
		case "(":
			depth++
		case ")":
			depth--
			if depth == 0 {
				return index
			}
		}
	}
	return len(tokens) - 1
}

func portableSQLStatements(tokens []portableSQLToken) [][]portableSQLToken {
	var statements [][]portableSQLToken
	start := 0
	depth := 0
	for index, token := range tokens {
		switch token.value {
		case "(":
			depth++
		case ")":
			if depth > 0 {
				depth--
			}
		case ";":
			if depth == 0 {
				if index > start {
					statements = append(statements, tokens[start:index])
				}
				start = index + 1
			}
		}
	}
	if start < len(tokens) {
		statements = append(statements, tokens[start:])
	}
	return statements
}

func portableSQLTableIdentifier(tokens []portableSQLToken) (string, int) {
	if len(tokens) == 0 || tokens[0].value == "(" {
		return "", 0
	}
	if len(tokens) >= 3 && tokens[1].value == "." {
		return tokens[2].value, 3
	}
	return tokens[0].value, 1
}

func portableSQLKeyword(token portableSQLToken, keyword string) bool {
	return !token.quoted && strings.EqualFold(token.value, keyword)
}

func (s *Store) sanitizePortableRepositoryCompatibilityColumn(ctx context.Context) error {
	if s.hasColumn(ctx, "repositories", "raw_json") {
		if _, err := s.db.ExecContext(ctx, `update repositories set raw_json = '' where raw_json != ''`); err != nil {
			return fmt.Errorf("sanitize portable repository compatibility column: %w", err)
		}
	}
	return nil
}

var portableThreadsCreateTablePattern = regexp.MustCompile("(?i)^(\\s*CREATE\\s+TABLE\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?)(?:\"threads\"|`threads`|\\[threads\\]|threads)(\\s*\\()")

type portableSchemaObject struct {
	name string
	sql  string
}

func (s *Store) rebuildPortableCompatibilityThreads(ctx context.Context) ([]string, error) {
	stats := &PortablePruneStats{}
	return s.rebuildPortableCompatibilityThreadsWithOptions(ctx, PortablePruneOptions{}, stats)
}

func (s *Store) rebuildPortableCompatibilityThreadsWithOptions(ctx context.Context, options PortablePruneOptions, stats *PortablePruneStats) (_ []string, retErr error) {
	if stats == nil {
		stats = &PortablePruneStats{}
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("open portable threads rebuild connection: %w", err)
	}
	defer conn.Close()
	reportPortablePruneProgress(options.Progress, PortablePruneStageThreadsRebuildPreflight)
	var createSQL string
	if err := conn.QueryRowContext(ctx, `select sql from sqlite_schema where type = 'table' and name = 'threads'`).Scan(&createSQL); err != nil {
		return nil, fmt.Errorf("read portable threads schema: %w", err)
	}
	sibling, err := portableThreadsSiblingName()
	if err != nil {
		return nil, err
	}
	siblingCreateSQL, err := rewritePortableThreadsCreateSQL(createSQL, sibling)
	if err != nil {
		return nil, err
	}
	columns, selectExpressions, verbatimColumns, err := portableThreadsRebuildColumns(ctx, conn)
	if err != nil {
		return nil, err
	}
	ordinaryIndexes, uniqueIndexes, err := portableThreadsRebuildIndexes(ctx, conn)
	if err != nil {
		return nil, err
	}
	triggers, err := portableThreadsRebuildTriggers(ctx, conn)
	if err != nil {
		return nil, err
	}
	hasConvergenceTrigger := false
	for _, trigger := range triggers {
		canonicalConvergence := false
		for _, definition := range observationConvergenceTriggers {
			if definition.table == "threads" && definition.name == trigger.name && trigger.sql == sqliteStoredSQL(observationConvergenceTriggerSQL(definition)) {
				canonicalConvergence = true
				hasConvergenceTrigger = true
				break
			}
		}
		if canonicalConvergence {
			continue
		}
		updatesThreads, err := portableTriggerUpdatesThreads(trigger.sql)
		if err != nil {
			return nil, fmt.Errorf("inspect portable threads trigger %s: %w", trigger.name, err)
		}
		if updatesThreads {
			return nil, fmt.Errorf("portable threads rebuild cannot preserve update-trigger semantics for %s", trigger.name)
		}
	}
	originalSequence, err := portableThreadsSequence(ctx, conn)
	if err != nil {
		return nil, err
	}
	if originalSequence.Valid && !strings.Contains(strings.ToUpper(createSQL), "AUTOINCREMENT") {
		return nil, fmt.Errorf("portable threads sqlite_sequence exists without AUTOINCREMENT schema")
	}
	originalIdentity, err := portableThreadsIdentityForTable(ctx, conn, "threads")
	if err != nil {
		return nil, err
	}
	originalThreadForeignKeys, err := portableForeignKeysForTable(ctx, conn, "threads")
	if err != nil {
		return nil, err
	}
	if err := validatePortableThreadsForeignKeyTransformSafety(originalThreadForeignKeys, verbatimColumns); err != nil {
		return nil, err
	}
	originalChildForeignKeys, err := portableChildForeignKeysReferencingThreads(ctx, conn)
	if err != nil {
		return nil, err
	}
	if err := validatePortableChildForeignKeyIdentityTargets(originalChildForeignKeys); err != nil {
		return nil, err
	}
	reportPortablePruneProgress(options.Progress, PortablePruneStageThreadsRebuildForeignKeys)
	checker := options.foreignKeyCheck
	if checker == nil {
		checker = portableForeignKeyViolationCount
	}
	violations, err := checker(ctx, conn)
	if err != nil {
		return nil, err
	}
	stats.ForeignKeyValidated = true
	stats.ForeignKeyViolations = violations
	if violations != 0 {
		return nil, fmt.Errorf("portable threads rebuild found %d foreign-key violations before rebuild", violations)
	}
	foreignKeysOff := false
	legacyAlterChanged := false
	var originalLegacyAlter int
	defer func() {
		if foreignKeysOff {
			if _, err := conn.ExecContext(context.Background(), `pragma foreign_keys = on`); err != nil {
				retErr = errors.Join(retErr, fmt.Errorf("restore portable threads foreign keys: %w", err))
			}
		}
		if legacyAlterChanged {
			if _, err := conn.ExecContext(context.Background(), fmt.Sprintf(`pragma legacy_alter_table = %d`, originalLegacyAlter)); err != nil {
				retErr = errors.Join(retErr, fmt.Errorf("restore portable threads legacy alter mode: %w", err))
			}
		}
	}()
	if err := conn.QueryRowContext(ctx, `pragma legacy_alter_table`).Scan(&originalLegacyAlter); err != nil {
		return nil, fmt.Errorf("read portable threads legacy alter mode: %w", err)
	}
	if originalLegacyAlter != 1 {
		if _, err := conn.ExecContext(ctx, `pragma legacy_alter_table = on`); err != nil {
			return nil, fmt.Errorf("enable portable threads legacy alter mode: %w", err)
		}
		legacyAlterChanged = true
	}
	var legacyAlter int
	if err := conn.QueryRowContext(ctx, `pragma legacy_alter_table`).Scan(&legacyAlter); err != nil {
		return nil, fmt.Errorf("verify portable threads legacy alter mode: %w", err)
	}
	if legacyAlter != 1 {
		return nil, fmt.Errorf("enable portable threads legacy alter mode: pragma remained %d", legacyAlter)
	}
	if _, err := conn.ExecContext(ctx, `pragma foreign_keys = off`); err != nil {
		return nil, fmt.Errorf("disable portable threads foreign keys: %w", err)
	}
	foreignKeysOff = true
	var foreignKeys int
	if err := conn.QueryRowContext(ctx, `pragma foreign_keys`).Scan(&foreignKeys); err != nil {
		return nil, fmt.Errorf("verify disabled portable threads foreign keys: %w", err)
	}
	if foreignKeys != 0 {
		return nil, fmt.Errorf("disable portable threads foreign keys: pragma remained %d", foreignKeys)
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin portable threads rebuild: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	reportPortablePruneProgress(options.Progress, PortablePruneStageThreadsRebuildCompactCopy)
	if _, err := tx.ExecContext(ctx, siblingCreateSQL); err != nil {
		return nil, fmt.Errorf("create portable threads sibling: %w", err)
	}
	insertSQL := `insert or abort into ` + sqliteIdentifier(sibling) + ` (` + strings.Join(columns, ", ") + `) select ` + strings.Join(selectExpressions, ", ") + ` from "threads"`
	if _, err := tx.ExecContext(ctx, insertSQL); err != nil {
		return nil, fmt.Errorf("copy compact portable threads: %w", err)
	}
	if options.threadsRebuildHook != nil {
		if err := options.threadsRebuildHook(portableThreadsRebuildHookAfterCopy, tx, sibling); err != nil {
			return nil, fmt.Errorf("portable threads rebuild test hook: %w", err)
		}
	}
	rebuiltIdentity, err := portableThreadsIdentityForTable(ctx, tx, sibling)
	if err != nil {
		return nil, err
	}
	if rebuiltIdentity != originalIdentity {
		return nil, fmt.Errorf("portable threads rebuild identity mismatch: copied %d of %d rows", rebuiltIdentity.count, originalIdentity.count)
	}
	reportPortablePruneProgress(options.Progress, PortablePruneStageThreadsRebuildSchemaSwap)
	if _, err := tx.ExecContext(ctx, `drop table "threads"`); err != nil {
		return nil, fmt.Errorf("drop uncompact portable threads: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `alter table `+sqliteIdentifier(sibling)+` rename to "threads"`); err != nil {
		return nil, fmt.Errorf("rename compact portable threads: %w", err)
	}
	reportPortablePruneProgress(options.Progress, PortablePruneStageThreadsRebuildSchemaRestore)
	if originalSequence.Valid {
		var rebuiltSequence sql.NullInt64
		if err := tx.QueryRowContext(ctx, `select max(seq) from sqlite_sequence where name in ('threads', ?)`, sibling).Scan(&rebuiltSequence); err != nil {
			return nil, fmt.Errorf("read rebuilt portable threads sequence: %w", err)
		}
		highWater := originalSequence.Int64
		if rebuiltSequence.Valid && rebuiltSequence.Int64 > highWater {
			highWater = rebuiltSequence.Int64
		}
		if _, err := tx.ExecContext(ctx, `delete from sqlite_sequence where name in ('threads', ?)`, sibling); err != nil {
			return nil, fmt.Errorf("clear rebuilt portable threads sequence: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `insert into sqlite_sequence(name, seq) values('threads', ?)`, highWater); err != nil {
			return nil, fmt.Errorf("restore portable threads sequence: %w", err)
		}
	}
	for _, index := range uniqueIndexes {
		if _, err := tx.ExecContext(ctx, index.sql); err != nil {
			return nil, fmt.Errorf("recreate portable unique index %s: %w", index.name, err)
		}
	}
	for _, trigger := range triggers {
		if _, err := tx.ExecContext(ctx, trigger.sql); err != nil {
			return nil, fmt.Errorf("recreate portable threads trigger %s: %w", trigger.name, err)
		}
	}
	// Bulk-copy rows intentionally do not fire table triggers. Definitions are
	// restored for future writes; the known convergence triggers' net effect is
	// applied once below instead of once per copied row.
	var convergenceTable int
	if err := tx.QueryRowContext(ctx, `select exists(select 1 from sqlite_schema where type = 'table' and name = 'observation_schema_convergence')`).Scan(&convergenceTable); err != nil {
		return nil, fmt.Errorf("inspect portable observation convergence table: %w", err)
	}
	if convergenceTable == 1 && hasConvergenceTrigger {
		if _, err := tx.ExecContext(ctx, `update observation_schema_convergence set checked_observation_sequence = -1 where id = 1`); err != nil {
			return nil, fmt.Errorf("invalidate portable observation convergence after threads rebuild: %w", err)
		}
	}
	rebuiltThreadForeignKeys, err := portableForeignKeysForTable(ctx, tx, "threads")
	if err != nil {
		return nil, err
	}
	if !slices.Equal(originalThreadForeignKeys, rebuiltThreadForeignKeys) {
		return nil, fmt.Errorf("portable threads rebuild changed threads foreign-key schema")
	}
	rebuiltChildForeignKeys, err := portableChildForeignKeysReferencingThreads(ctx, tx)
	if err != nil {
		return nil, err
	}
	if !slices.Equal(originalChildForeignKeys, rebuiltChildForeignKeys) {
		return nil, fmt.Errorf("portable threads rebuild changed child foreign-key schema")
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit portable threads rebuild: %w", err)
	}
	if _, err := conn.ExecContext(context.Background(), `pragma foreign_keys = on`); err != nil {
		return nil, fmt.Errorf("restore portable threads foreign keys: %w", err)
	}
	foreignKeysOff = false
	if legacyAlterChanged {
		if _, err := conn.ExecContext(context.Background(), fmt.Sprintf(`pragma legacy_alter_table = %d`, originalLegacyAlter)); err != nil {
			return nil, fmt.Errorf("restore portable threads legacy alter mode: %w", err)
		}
		legacyAlterChanged = false
	}
	if err := conn.QueryRowContext(ctx, `pragma foreign_keys`).Scan(&foreignKeys); err != nil {
		return nil, fmt.Errorf("verify restored portable threads foreign keys: %w", err)
	}
	if foreignKeys != 1 {
		return nil, fmt.Errorf("restore portable threads foreign keys: pragma remained %d", foreignKeys)
	}
	// One indexed full FK proof ran before the rebuild. Child rows and outgoing
	// FK source values are untouched, parent (id, repo_id) identity is exact,
	// and both parent/child FK definitions are equivalent, so zero is preserved.
	return ordinaryIndexes, nil
}

type portableSQLToken struct {
	value   string
	quoted  bool
	literal bool
}

func portableTriggerUpdatesThreads(triggerSQL string) (bool, error) {
	tokens, err := tokenizePortableSQL(triggerSQL)
	if err != nil {
		return false, err
	}
	headerEnd := len(tokens)
	for index, token := range tokens {
		if !token.quoted && strings.EqualFold(token.value, "BEGIN") {
			headerEnd = index
			break
		}
	}
	header := tokens[:headerEnd]
	for index, token := range header {
		if token.quoted || !strings.EqualFold(token.value, "ON") {
			continue
		}
		targetIndex := index + 1
		if targetIndex+2 < len(header) && header[targetIndex+1].value == "." {
			targetIndex += 2
		}
		if targetIndex >= len(header) || !strings.EqualFold(header[targetIndex].value, "threads") {
			continue
		}
		for _, prior := range header[:index] {
			if !prior.quoted && strings.EqualFold(prior.value, "UPDATE") {
				return true, nil
			}
		}
		return false, nil
	}
	return false, fmt.Errorf("unrecognized trigger target")
}

func tokenizePortableSQL(value string) ([]portableSQLToken, error) {
	var tokens []portableSQLToken
	for index := 0; index < len(value); {
		switch {
		case isPortableSQLSpace(value[index]):
			index++
		case index+1 < len(value) && value[index:index+2] == "--":
			index += 2
			for index < len(value) && value[index] != '\n' {
				index++
			}
		case index+1 < len(value) && value[index:index+2] == "/*":
			end := strings.Index(value[index+2:], "*/")
			if end < 0 {
				return nil, fmt.Errorf("unterminated SQL comment")
			}
			index += end + 4
		case value[index] == '\'' || value[index] == '"' || value[index] == '`':
			quote := value[index]
			start := index + 1
			index++
			var text strings.Builder
			for {
				if index >= len(value) {
					return nil, fmt.Errorf("unterminated SQL quote")
				}
				if value[index] == quote {
					if index+1 < len(value) && value[index+1] == quote {
						text.WriteString(value[start:index])
						text.WriteByte(quote)
						index += 2
						start = index
						continue
					}
					text.WriteString(value[start:index])
					index++
					break
				}
				index++
			}
			tokens = append(tokens, portableSQLToken{value: text.String(), quoted: true, literal: quote == '\''})
		case value[index] == '[':
			end := strings.IndexByte(value[index+1:], ']')
			if end < 0 {
				return nil, fmt.Errorf("unterminated SQL bracket identifier")
			}
			tokens = append(tokens, portableSQLToken{value: value[index+1 : index+1+end], quoted: true})
			index += end + 2
		case isPortableSQLWord(value[index]):
			start := index
			for index < len(value) && isPortableSQLWord(value[index]) {
				index++
			}
			tokens = append(tokens, portableSQLToken{value: value[start:index]})
		default:
			tokens = append(tokens, portableSQLToken{value: value[index : index+1]})
			index++
		}
	}
	return tokens, nil
}

func isPortableSQLSpace(value byte) bool {
	return value == ' ' || value == '\t' || value == '\r' || value == '\n' || value == '\f'
}

func isPortableSQLWord(value byte) bool {
	return value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value >= '0' && value <= '9' || value == '_' || value == '$'
}

func portableThreadsSequence(ctx context.Context, q dbQueries) (sql.NullInt64, error) {
	var sequenceTable int
	if err := q.QueryRowContext(ctx, `select exists(select 1 from sqlite_schema where type = 'table' and name = 'sqlite_sequence')`).Scan(&sequenceTable); err != nil {
		return sql.NullInt64{}, fmt.Errorf("inspect portable threads sequence table: %w", err)
	}
	if sequenceTable == 0 {
		return sql.NullInt64{}, nil
	}
	var sequence sql.NullInt64
	if err := q.QueryRowContext(ctx, `select seq from sqlite_sequence where name = 'threads'`).Scan(&sequence); err != nil {
		if err == sql.ErrNoRows {
			return sql.NullInt64{}, nil
		}
		return sql.NullInt64{}, fmt.Errorf("read portable threads sequence: %w", err)
	}
	return sequence, nil
}

type portableThreadsIdentity struct {
	count  int64
	digest [sha256.Size]byte
}

func portableThreadsIdentityForTable(ctx context.Context, q dbQueries, table string) (portableThreadsIdentity, error) {
	rows, err := q.QueryContext(ctx, `select id, repo_id from `+sqliteIdentifier(table)+` order by id`)
	if err != nil {
		return portableThreadsIdentity{}, fmt.Errorf("read portable threads identity from %s: %w", table, err)
	}
	defer rows.Close()
	hash := sha256.New()
	var identity portableThreadsIdentity
	var encoded [16]byte
	for rows.Next() {
		var id, repoID int64
		if err := rows.Scan(&id, &repoID); err != nil {
			return portableThreadsIdentity{}, fmt.Errorf("scan portable threads identity from %s: %w", table, err)
		}
		binary.BigEndian.PutUint64(encoded[:8], uint64(id))
		binary.BigEndian.PutUint64(encoded[8:], uint64(repoID))
		_, _ = hash.Write(encoded[:])
		identity.count++
	}
	if err := rows.Err(); err != nil {
		return portableThreadsIdentity{}, fmt.Errorf("read portable threads identity rows from %s: %w", table, err)
	}
	copy(identity.digest[:], hash.Sum(nil))
	return identity, nil
}

type portableForeignKeyDefinition struct {
	ownerTable      string
	id              int
	sequence        int
	referencedTable string
	fromColumn      string
	toColumn        string
	onUpdate        string
	onDelete        string
	match           string
}

func portableForeignKeysForTable(ctx context.Context, q dbQueries, table string) ([]portableForeignKeyDefinition, error) {
	rows, err := q.QueryContext(ctx, `pragma foreign_key_list(`+sqliteIdentifier(table)+`)`)
	if err != nil {
		return nil, fmt.Errorf("inspect portable foreign keys for %s: %w", table, err)
	}
	defer rows.Close()
	var definitions []portableForeignKeyDefinition
	for rows.Next() {
		var definition portableForeignKeyDefinition
		var toColumn sql.NullString
		definition.ownerTable = table
		if err := rows.Scan(
			&definition.id,
			&definition.sequence,
			&definition.referencedTable,
			&definition.fromColumn,
			&toColumn,
			&definition.onUpdate,
			&definition.onDelete,
			&definition.match,
		); err != nil {
			return nil, fmt.Errorf("scan portable foreign keys for %s: %w", table, err)
		}
		definition.toColumn = toColumn.String
		definitions = append(definitions, definition)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read portable foreign keys for %s: %w", table, err)
	}
	sortPortableForeignKeys(definitions)
	return definitions, nil
}

func portableChildForeignKeysReferencingThreads(ctx context.Context, q dbQueries) ([]portableForeignKeyDefinition, error) {
	tables, err := portableTableNames(ctx, q)
	if err != nil {
		return nil, err
	}
	var definitions []portableForeignKeyDefinition
	for _, table := range tables {
		if table == "threads" {
			continue
		}
		tableDefinitions, err := portableForeignKeysForTable(ctx, q, table)
		if err != nil {
			return nil, err
		}
		for _, definition := range tableDefinitions {
			if strings.EqualFold(definition.referencedTable, "threads") {
				definitions = append(definitions, definition)
			}
		}
	}
	sortPortableForeignKeys(definitions)
	return definitions, nil
}

func sortPortableForeignKeys(definitions []portableForeignKeyDefinition) {
	sort.Slice(definitions, func(left, right int) bool {
		leftKey := fmt.Sprintf("%s\x00%08d\x00%08d\x00%s\x00%s\x00%s\x00%s\x00%s\x00%s", definitions[left].ownerTable, definitions[left].id, definitions[left].sequence, definitions[left].referencedTable, definitions[left].fromColumn, definitions[left].toColumn, definitions[left].onUpdate, definitions[left].onDelete, definitions[left].match)
		rightKey := fmt.Sprintf("%s\x00%08d\x00%08d\x00%s\x00%s\x00%s\x00%s\x00%s\x00%s", definitions[right].ownerTable, definitions[right].id, definitions[right].sequence, definitions[right].referencedTable, definitions[right].fromColumn, definitions[right].toColumn, definitions[right].onUpdate, definitions[right].onDelete, definitions[right].match)
		return leftKey < rightKey
	})
}

func validatePortableChildForeignKeyIdentityTargets(definitions []portableForeignKeyDefinition) error {
	for _, definition := range definitions {
		toColumn := definition.toColumn
		if toColumn == "" {
			toColumn = "id"
		}
		if !strings.EqualFold(toColumn, "id") && !strings.EqualFold(toColumn, "repo_id") {
			return fmt.Errorf("portable threads rebuild cannot preserve child foreign key %s.%s referencing mutable threads column %s", definition.ownerTable, definition.fromColumn, toColumn)
		}
	}
	return nil
}

func validatePortableThreadsForeignKeyTransformSafety(definitions []portableForeignKeyDefinition, verbatimColumns map[string]bool) error {
	for _, definition := range definitions {
		if !verbatimColumns[strings.ToLower(definition.fromColumn)] {
			return fmt.Errorf("portable threads rebuild cannot preserve foreign key from transformed threads column %s", definition.fromColumn)
		}
		if !strings.EqualFold(definition.referencedTable, "threads") {
			continue
		}
		toColumn := definition.toColumn
		if toColumn == "" {
			toColumn = "id"
		}
		if !strings.EqualFold(toColumn, "id") && !strings.EqualFold(toColumn, "repo_id") {
			return fmt.Errorf("portable threads rebuild cannot preserve self-referencing foreign key targeting transformed threads column %s", toColumn)
		}
	}
	return nil
}

func portableThreadsSiblingName() (string, error) {
	var value [8]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("generate portable threads sibling name: %w", err)
	}
	return "threads_portable_" + hex.EncodeToString(value[:]), nil
}

func rewritePortableThreadsCreateSQL(createSQL, sibling string) (string, error) {
	match := portableThreadsCreateTablePattern.FindStringSubmatchIndex(createSQL)
	if match == nil {
		return "", fmt.Errorf("unrecognized portable threads CREATE TABLE SQL")
	}
	return createSQL[:match[3]] + sqliteIdentifier(sibling) + createSQL[match[4]:], nil
}

func portableThreadsRebuildColumns(ctx context.Context, q dbQueries) ([]string, []string, map[string]bool, error) {
	rows, err := q.QueryContext(ctx, `pragma table_xinfo("threads")`)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("inspect portable threads columns: %w", err)
	}
	defer rows.Close()
	var columns, expressions []string
	seen := make(map[string]bool)
	verbatimColumns := make(map[string]bool)
	for rows.Next() {
		var cid, notNull, primaryKey, hidden int
		var name, columnType string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey, &hidden); err != nil {
			return nil, nil, nil, fmt.Errorf("scan portable threads columns: %w", err)
		}
		seen[name] = true
		if hidden != 0 {
			continue
		}
		columns = append(columns, sqliteIdentifier(name))
		switch name {
		case "body":
			expressions = append(expressions, `"body_excerpt"`)
		case "raw_json":
			expressions = append(expressions, `''`)
		default:
			expressions = append(expressions, sqliteIdentifier(name))
			verbatimColumns[strings.ToLower(name)] = true
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, nil, fmt.Errorf("read portable threads columns: %w", err)
	}
	for _, required := range []string{"body", "body_excerpt", "raw_json"} {
		if !seen[required] {
			return nil, nil, nil, fmt.Errorf("portable threads rebuild requires column %s", required)
		}
	}
	return columns, expressions, verbatimColumns, nil
}

func portableThreadsRebuildIndexes(ctx context.Context, q dbQueries) ([]string, []portableSchemaObject, error) {
	rows, err := q.QueryContext(ctx, `pragma index_list("threads")`)
	if err != nil {
		return nil, nil, fmt.Errorf("inspect portable threads indexes: %w", err)
	}
	defer rows.Close()
	var ordinary []string
	var uniqueNames []string
	for rows.Next() {
		var sequence, unique, partial int
		var name, origin string
		if err := rows.Scan(&sequence, &name, &unique, &origin, &partial); err != nil {
			return nil, nil, fmt.Errorf("scan portable threads indexes: %w", err)
		}
		if origin != "c" {
			continue
		}
		if unique == 0 {
			ordinary = append(ordinary, name)
			continue
		}
		uniqueNames = append(uniqueNames, name)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("read portable threads indexes: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, nil, fmt.Errorf("close portable threads indexes: %w", err)
	}
	var uniqueIndexes []portableSchemaObject
	for _, name := range uniqueNames {
		var indexSQL sql.NullString
		if err := q.QueryRowContext(ctx, `select sql from sqlite_schema where type = 'index' and tbl_name = 'threads' and name = ?`, name).Scan(&indexSQL); err != nil {
			return nil, nil, fmt.Errorf("read portable unique index %s: %w", name, err)
		}
		if !indexSQL.Valid || !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(indexSQL.String)), "CREATE UNIQUE INDEX") {
			return nil, nil, fmt.Errorf("portable unique index %s has unrecognized SQL", name)
		}
		uniqueIndexes = append(uniqueIndexes, portableSchemaObject{name: name, sql: indexSQL.String})
	}
	sort.Strings(ordinary)
	sort.Slice(uniqueIndexes, func(left, right int) bool { return uniqueIndexes[left].name < uniqueIndexes[right].name })
	return ordinary, uniqueIndexes, nil
}

func portableThreadsRebuildTriggers(ctx context.Context, q dbQueries) ([]portableSchemaObject, error) {
	rows, err := q.QueryContext(ctx, `select name, sql from sqlite_schema where type = 'trigger' and tbl_name = 'threads' order by name`)
	if err != nil {
		return nil, fmt.Errorf("inspect portable threads triggers: %w", err)
	}
	defer rows.Close()
	var triggers []portableSchemaObject
	for rows.Next() {
		var trigger portableSchemaObject
		var triggerSQL sql.NullString
		if err := rows.Scan(&trigger.name, &triggerSQL); err != nil {
			return nil, fmt.Errorf("scan portable threads trigger: %w", err)
		}
		if !triggerSQL.Valid || !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(triggerSQL.String)), "CREATE TRIGGER") {
			return nil, fmt.Errorf("portable threads trigger %s has unrecognized SQL", trigger.name)
		}
		trigger.sql = triggerSQL.String
		triggers = append(triggers, trigger)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read portable threads triggers: %w", err)
	}
	return triggers, nil
}

func (s *Store) compactPortableThreadMetadata(ctx context.Context) (int64, int64, error) {
	if !s.hasColumn(ctx, "threads", "labels_json") || !s.hasColumn(ctx, "threads", "assignees_json") {
		return 0, 0, nil
	}
	rows, err := s.db.QueryContext(ctx, `select id, labels_json, assignees_json from threads order by id`)
	if err != nil {
		return 0, 0, fmt.Errorf("read portable thread metadata: %w", err)
	}
	type update struct {
		id        int64
		labels    string
		assignees string
	}
	var updates []update
	var labelsCompacted, assigneesCompacted int64
	for rows.Next() {
		var id int64
		var labels, assignees string
		if err := rows.Scan(&id, &labels, &assignees); err != nil {
			_ = rows.Close()
			return 0, 0, fmt.Errorf("scan portable thread metadata: %w", err)
		}
		nextLabels := compactPortableNameList(labels, "name")
		nextAssignees := compactPortableNameList(assignees, "login")
		if nextLabels == labels && nextAssignees == assignees {
			continue
		}
		if nextLabels != labels {
			labelsCompacted++
		}
		if nextAssignees != assignees {
			assigneesCompacted++
		}
		updates = append(updates, update{id: id, labels: nextLabels, assignees: nextAssignees})
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, 0, fmt.Errorf("read portable thread metadata rows: %w", err)
	}
	if err := rows.Close(); err != nil {
		return 0, 0, fmt.Errorf("close portable thread metadata rows: %w", err)
	}
	if len(updates) == 0 {
		return 0, 0, nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, fmt.Errorf("begin portable thread metadata compaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()
	stmt, err := tx.PrepareContext(ctx, `update threads set labels_json = ?, assignees_json = ? where id = ?`)
	if err != nil {
		return 0, 0, fmt.Errorf("prepare portable thread metadata compaction: %w", err)
	}
	defer func() { _ = stmt.Close() }()
	for _, update := range updates {
		if _, err := stmt.ExecContext(ctx, update.labels, update.assignees, update.id); err != nil {
			return 0, 0, fmt.Errorf("compact portable thread metadata: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, 0, fmt.Errorf("commit portable thread metadata compaction: %w", err)
	}
	return labelsCompacted, assigneesCompacted, nil
}

func (s *Store) compactPortableReviewThreadBodies(ctx context.Context, bodyChars int) error {
	for _, table := range []string{"pull_request_review_threads", "pull_request_review_thread_revisions"} {
		if !s.hasColumns(ctx, table, "first_comment_body", "comments_json") {
			continue
		}
		rows, err := s.db.QueryContext(ctx, `
			select rowid, first_comment_body, comments_json
			from `+sqliteIdentifier(table)+`
			order by rowid
		`)
		if err != nil {
			return fmt.Errorf("read portable review bodies from %s: %w", table, err)
		}
		type update struct {
			rowID        int64
			firstBody    sql.NullString
			commentsJSON string
		}
		var updates []update
		for rows.Next() {
			var rowID int64
			var firstBody sql.NullString
			var commentsJSON string
			if err := rows.Scan(&rowID, &firstBody, &commentsJSON); err != nil {
				_ = rows.Close()
				return fmt.Errorf("scan portable review bodies from %s: %w", table, err)
			}
			nextFirst := firstBody
			if nextFirst.Valid {
				nextFirst.String = truncatePortableText(nextFirst.String, bodyChars)
			}
			nextComments := compactPortableReviewComments(commentsJSON, bodyChars)
			if nextFirst == firstBody && nextComments == commentsJSON {
				continue
			}
			updates = append(updates, update{rowID: rowID, firstBody: nextFirst, commentsJSON: nextComments})
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return fmt.Errorf("read portable review bodies from %s: %w", table, err)
		}
		if err := rows.Close(); err != nil {
			return fmt.Errorf("close portable review bodies from %s: %w", table, err)
		}
		for _, update := range updates {
			if _, err := s.db.ExecContext(ctx, `
				update `+sqliteIdentifier(table)+`
				set first_comment_body = ?, comments_json = ?
				where rowid = ?
			`, update.firstBody, update.commentsJSON, update.rowID); err != nil {
				return fmt.Errorf("compact portable review bodies in %s: %w", table, err)
			}
		}
	}
	return nil
}

func compactPortableReviewComments(raw string, bodyChars int) string {
	var value any
	if err := json.Unmarshal([]byte(raw), &value); err != nil {
		return "[]"
	}
	truncatePortableReviewBodies(value, bodyChars)
	compact, err := json.Marshal(value)
	if err != nil {
		return "[]"
	}
	return string(compact)
}

func truncatePortableReviewBodies(value any, bodyChars int) {
	switch typed := value.(type) {
	case []any:
		for _, item := range typed {
			truncatePortableReviewBodies(item, bodyChars)
		}
	case map[string]any:
		for key, item := range typed {
			if key == "body" || key == "bodyText" {
				if text, ok := item.(string); ok {
					typed[key] = truncatePortableText(text, bodyChars)
				}
				continue
			}
			truncatePortableReviewBodies(item, bodyChars)
		}
	}
}

func truncatePortableText(value string, limit int) string {
	if limit <= 0 {
		return ""
	}
	runes := []rune(value)
	if len(runes) <= limit {
		return value
	}
	return string(runes[:limit])
}

func compactPortableNameList(raw, field string) string {
	var values []any
	if err := json.Unmarshal([]byte(raw), &values); err != nil {
		return raw
	}
	names := make([]string, 0, len(values))
	for _, value := range values {
		var name string
		switch typed := value.(type) {
		case string:
			name = typed
		case map[string]any:
			name, _ = typed[field].(string)
		default:
			return raw
		}
		if name = strings.TrimSpace(name); name == "" {
			return raw
		}
		names = append(names, name)
	}
	compact, err := json.Marshal(names)
	if err != nil {
		return raw
	}
	return string(compact)
}

func (s *Store) ensurePortableExcerptColumns(ctx context.Context, table string) error {
	if !s.hasColumn(ctx, table, "body_excerpt") {
		if _, err := s.db.ExecContext(ctx, `alter table `+sqliteIdentifier(table)+` add column body_excerpt text`); err != nil {
			return fmt.Errorf("add portable %s.body_excerpt: %w", table, err)
		}
	}
	if !s.hasColumn(ctx, table, "body_length") {
		if _, err := s.db.ExecContext(ctx, `alter table `+sqliteIdentifier(table)+` add column body_length integer not null default 0`); err != nil {
			return fmt.Errorf("add portable %s.body_length: %w", table, err)
		}
	}
	return nil
}

func (s *Store) clearPortableRawJSON(ctx context.Context) (int64, error) {
	var total int64
	for _, column := range []struct {
		table string
		name  string
	}{
		{table: "comments", name: "raw_json"},
		{table: "comment_revisions", name: "raw_json"},
		{table: "pull_request_details", name: "raw_json"},
		{table: "pull_request_files", name: "raw_json"},
		{table: "pull_request_commits", name: "raw_json"},
		{table: "pull_request_checks", name: "raw_json"},
		{table: "pull_request_review_threads", name: "raw_json"},
		{table: "pull_request_review_thread_revisions", name: "raw_json"},
		{table: "github_workflow_runs", name: "raw_json"},
	} {
		if !s.hasColumn(ctx, column.table, column.name) {
			continue
		}
		result, err := s.db.ExecContext(ctx, `update `+sqliteIdentifier(column.table)+` set `+sqliteIdentifier(column.name)+` = '' where `+sqliteIdentifier(column.name)+` is not null and `+sqliteIdentifier(column.name)+` != ''`)
		if err != nil {
			return total, fmt.Errorf("clear portable raw json %s.%s: %w", column.table, column.name, err)
		}
		total += rowsAffected(result)
	}
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
		if _, err := s.db.ExecContext(ctx, `update `+sqliteIdentifier(column.table)+` set `+sqliteIdentifier(column.name)+` = null where `+sqliteIdentifier(column.name)+` is not null`); err != nil {
			return total, fmt.Errorf("clear portable raw blob pointer %s.%s: %w", column.table, column.name, err)
		}
	}
	return total, nil
}

func (s *Store) clearPortablePullRequestFilePatches(ctx context.Context) error {
	if !s.tableExists(ctx, "pull_request_files") || !s.hasColumn(ctx, "pull_request_files", "patch") {
		return nil
	}
	if _, err := s.db.ExecContext(ctx, `
		update pull_request_files
		set patch = null
		where patch is not null and patch != ''
	`); err != nil {
		return fmt.Errorf("clear portable pull request file patches: %w", err)
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

func sqliteIdentifier(value string) string {
	if value == "" || strings.ContainsAny(value, "\"\x00") {
		panic(fmt.Sprintf("unsafe SQLite identifier: %q", value))
	}
	return `"` + value + `"`
}

func portableRuntimeIdentifier(value string) (string, error) {
	if value == "" || strings.ContainsRune(value, '\x00') {
		return "", fmt.Errorf("unsafe SQLite identifier %q", value)
	}
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`, nil
}

func (s *Store) tableExists(ctx context.Context, table string) bool {
	var name string
	err := s.q().QueryRowContext(ctx, `select name from sqlite_master where type in ('table', 'virtual table') and name = ?`, table).Scan(&name)
	return err == nil && name == table
}

func rowsAffected(result sql.Result) int64 {
	rows, err := result.RowsAffected()
	if err != nil {
		return 0
	}
	return rows
}
