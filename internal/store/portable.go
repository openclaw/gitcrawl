package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
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
