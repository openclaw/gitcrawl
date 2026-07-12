package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

const portableSchemaVersion = 4

type PortablePruneOptions struct {
	BodyChars int
	Vacuum    bool
}

type PortablePruneStats struct {
	DBPath                   string   `json:"db_path"`
	ManifestPath             string   `json:"manifest_path,omitempty"`
	SHA256                   string   `json:"sha256,omitempty"`
	BodyChars                int      `json:"body_chars"`
	BytesBefore              int64    `json:"bytes_before"`
	BytesAfter               int64    `json:"bytes_after"`
	QuickCheck               string   `json:"quick_check,omitempty"`
	ThreadsPruned            int64    `json:"threads_pruned"`
	CommentsPruned           int64    `json:"comments_pruned"`
	ThreadLabelsCompacted    int64    `json:"thread_labels_compacted"`
	ThreadAssigneesCompacted int64    `json:"thread_assignees_compacted"`
	RepositoriesPruned       int64    `json:"repositories_pruned"`
	RawJSONPruned            int64    `json:"raw_json_pruned"`
	FingerprintsPruned       int64    `json:"fingerprints_pruned"`
	DocumentsDeleted         int64    `json:"documents_deleted"`
	DocumentsFTSRebuilt      bool     `json:"documents_fts_rebuilt"`
	DroppedTables            []string `json:"dropped_tables,omitempty"`
	DroppedColumns           []string `json:"dropped_columns,omitempty"`
	Vacuumed                 bool     `json:"vacuumed"`
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
		if err := s.ensurePortableExcerptColumns(ctx, "threads"); err != nil {
			return stats, err
		}
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
		"schema":                "gitcrawl-portable-sync-v2",
		"body_chars":            fmt.Sprintf("%d", bodyChars),
		"capabilities":          "body_excerpts,comment_excerpts,author_association,thread_revisions,thread_fingerprints,thread_key_summaries,pr_details,pr_files,pr_commits,pr_checks,pr_review_threads,workflow_runs,raw_json_stripped",
		"includes":              "repositories,threads,comments,thread_revisions,thread_fingerprints,thread_key_summaries,pull_request_details,pull_request_files,pull_request_commits,pull_request_checks,pull_request_review_threads,pull_request_review_thread_syncs,github_workflow_runs",
		"excluded":              "raw_json,documents,fts,vectors,code_snapshots,code_documents,cluster_events,run_history,similarity_edges,blobs",
		"exported_at":           time.Now().UTC().Format(timeLayout),
		"source_path":           s.path,
		"thread_author_profile": "login,type,association",
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
		{table: "pull_request_details", name: "raw_json"},
		{table: "pull_request_files", name: "raw_json"},
		{table: "pull_request_commits", name: "raw_json"},
		{table: "pull_request_checks", name: "raw_json"},
		{table: "pull_request_review_threads", name: "raw_json"},
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
