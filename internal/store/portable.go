package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"
)

type PortablePruneOptions struct {
	BodyChars int
	Vacuum    bool
}

type PortablePruneStats struct {
	DBPath              string   `json:"db_path"`
	BodyChars           int      `json:"body_chars"`
	BytesBefore         int64    `json:"bytes_before"`
	BytesAfter          int64    `json:"bytes_after"`
	ThreadsPruned       int64    `json:"threads_pruned"`
	RepositoriesPruned  int64    `json:"repositories_pruned"`
	FingerprintsPruned  int64    `json:"fingerprints_pruned"`
	DocumentsDeleted    int64    `json:"documents_deleted"`
	DocumentsFTSRebuilt bool     `json:"documents_fts_rebuilt"`
	DroppedTables       []string `json:"dropped_tables,omitempty"`
	DroppedColumns      []string `json:"dropped_columns,omitempty"`
	Vacuumed            bool     `json:"vacuumed"`
}

func (s *Store) PrunePortablePayloads(ctx context.Context, options PortablePruneOptions) (PortablePruneStats, error) {
	if options.BodyChars <= 0 {
		options.BodyChars = 256
	}
	stats := PortablePruneStats{
		DBPath:    s.path,
		BodyChars: options.BodyChars,
		Vacuumed:  options.Vacuum,
	}
	if info, err := os.Stat(s.path); err == nil {
		stats.BytesBefore = info.Size()
	}

	if s.hasColumn(ctx, "threads", "body") {
		if s.hasColumn(ctx, "threads", "body_excerpt") && s.hasColumn(ctx, "threads", "body_length") {
			if result, err := s.db.ExecContext(ctx, `
				update threads
				   set body_length = case when body is not null then length(body) else body_length end,
				       body_excerpt = case
				         when body is not null and length(body) > ? then substr(body, 1, ?)
				         when body is not null then body
				         else body_excerpt
				       end
				 where body is not null
			`, options.BodyChars, options.BodyChars); err != nil {
				return stats, fmt.Errorf("prune thread body excerpts: %w", err)
			} else {
				stats.ThreadsPruned += rowsAffected(result)
			}
			if _, err := s.db.ExecContext(ctx, `update threads set body = body_excerpt`); err != nil {
				return stats, fmt.Errorf("replace thread bodies with excerpts: %w", err)
			}
		} else {
			if result, err := s.db.ExecContext(ctx, `
				update threads
				   set body = case when length(body) > ? then substr(body, 1, ?) else body end
				 where body is not null
			`, options.BodyChars, options.BodyChars); err != nil {
				return stats, fmt.Errorf("trim thread bodies: %w", err)
			} else {
				stats.ThreadsPruned += rowsAffected(result)
			}
		}
	}
	if s.hasColumn(ctx, "threads", "raw_json") {
		if _, err := s.db.ExecContext(ctx, `update threads set raw_json = '' where raw_json is not null and raw_json != ''`); err != nil {
			return stats, fmt.Errorf("clear thread raw json: %w", err)
		}
	}
	if s.hasColumn(ctx, "repositories", "raw_json") {
		result, err := s.db.ExecContext(ctx, `update repositories set raw_json = '' where raw_json is not null and raw_json != ''`)
		if err != nil {
			return stats, fmt.Errorf("clear repository raw json: %w", err)
		}
		stats.RepositoriesPruned = rowsAffected(result)
	}
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
	if s.tableExists(ctx, "documents") {
		result, err := s.db.ExecContext(ctx, `delete from documents`)
		if err != nil {
			return stats, fmt.Errorf("delete generated documents: %w", err)
		}
		stats.DocumentsDeleted = rowsAffected(result)
	}
	if s.tableExists(ctx, "documents_fts") {
		if _, err := s.db.ExecContext(ctx, `insert into documents_fts(documents_fts) values('rebuild')`); err != nil {
			return stats, fmt.Errorf("rebuild document fts: %w", err)
		}
		stats.DocumentsFTSRebuilt = true
	}
	if err := s.canonicalizePortableSchema(ctx, options.BodyChars, &stats); err != nil {
		return stats, err
	}
	if options.Vacuum {
		if _, err := s.db.ExecContext(ctx, `pragma wal_checkpoint(TRUNCATE)`); err != nil {
			return stats, fmt.Errorf("checkpoint wal: %w", err)
		}
		if _, err := s.db.ExecContext(ctx, `vacuum`); err != nil {
			return stats, fmt.Errorf("vacuum database: %w", err)
		}
	}
	if info, err := os.Stat(s.path); err == nil {
		stats.BytesAfter = info.Size()
	}
	return stats, nil
}

func (s *Store) canonicalizePortableSchema(ctx context.Context, bodyChars int, stats *PortablePruneStats) error {
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
	for _, column := range []struct {
		table string
		name  string
	}{
		{table: "repositories", name: "raw_json"},
		{table: "threads", name: "raw_json"},
		{table: "threads", name: "body"},
	} {
		if !s.hasColumn(ctx, column.table, column.name) {
			continue
		}
		if _, err := s.db.ExecContext(ctx, `alter table `+sqliteIdentifier(column.table)+` drop column `+sqliteIdentifier(column.name)); err != nil {
			return fmt.Errorf("drop portable column %s.%s: %w", column.table, column.name, err)
		}
		stats.DroppedColumns = append(stats.DroppedColumns, column.table+"."+column.name)
	}
	for _, table := range canonicalPortableDroppedTables() {
		if !s.tableExists(ctx, table) {
			continue
		}
		if _, err := s.db.ExecContext(ctx, `drop table if exists `+sqliteIdentifier(table)); err != nil {
			return fmt.Errorf("drop portable table %s: %w", table, err)
		}
		stats.DroppedTables = append(stats.DroppedTables, table)
	}
	if _, err := s.db.ExecContext(ctx, `
		create table if not exists portable_metadata (
			key text primary key,
			value text not null
		)
	`); err != nil {
		return fmt.Errorf("ensure portable metadata: %w", err)
	}
	metadata := map[string]string{
		"schema":      "ghcrawl-portable-sync-v1",
		"body_chars":  fmt.Sprintf("%d", bodyChars),
		"excluded":    "raw_json,comments,documents,fts,vectors,code_snapshots,cluster_events,run_history,similarity_edges,blobs",
		"exported_at": time.Now().UTC().Format(timeLayout),
		"source_path": s.path,
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
	return nil
}

func canonicalPortableDroppedTables() []string {
	return []string{
		"documents_fts",
		"documents_fts_config",
		"documents_fts_data",
		"documents_fts_docsize",
		"documents_fts_idx",
		"comments",
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
	}
}

func sqliteIdentifier(value string) string {
	if value == "" || strings.ContainsAny(value, "\"\x00") {
		panic(fmt.Sprintf("unsafe SQLite identifier: %q", value))
	}
	return `"` + value + `"`
}

func (s *Store) tableExists(ctx context.Context, table string) bool {
	var name string
	err := s.db.QueryRowContext(ctx, `select name from sqlite_master where type in ('table', 'virtual table') and name = ?`, table).Scan(&name)
	return err == nil && name == table
}

func rowsAffected(result sql.Result) int64 {
	rows, err := result.RowsAffected()
	if err != nil {
		return 0
	}
	return rows
}
