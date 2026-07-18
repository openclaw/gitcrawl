package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

func (s *Store) familyTombstoneSchemaHasCurrentShape(ctx context.Context) bool {
	for _, table := range []string{"comments", "pull_request_commits", "pull_request_review_threads"} {
		if !s.hasColumns(ctx, table, "deleted_at", "deletion_reason") ||
			!s.tableHasFamilyTombstoneConstraint(ctx, table) {
			return false
		}
	}
	return s.hasTable(ctx, "comment_revisions") &&
		s.hasTable(ctx, "pull_request_review_thread_revisions")
}

func (s *Store) ensureFamilyTombstoneSchema(ctx context.Context) error {
	for _, table := range []string{"comments", "pull_request_commits", "pull_request_review_threads"} {
		if err := s.ensureColumn(ctx, table, "deleted_at", "text"); err != nil {
			return err
		}
		if err := s.ensureColumn(ctx, table, "deletion_reason", "text"); err != nil {
			return err
		}
	}
	if err := s.ensureFamilyTombstoneConstraints(ctx); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `
		create table if not exists comment_revisions (
			id integer primary key,
			comment_id integer not null references comments(id) on delete cascade,
			author_login text,
			author_type text,
			body text not null,
			is_bot integer not null default 0,
			raw_json text not null,
			created_at_gh text,
			updated_at_gh text,
			deleted_at text,
			deletion_reason text,
			recorded_at text not null
		);
		create index if not exists idx_comment_revisions_comment
			on comment_revisions(comment_id, id);
		create table if not exists pull_request_review_thread_revisions (
			id integer primary key,
			thread_id integer not null,
			review_thread_id text not null,
			path text,
			line integer not null default 0,
			start_line integer not null default 0,
			is_resolved integer not null default 0,
			is_outdated integer not null default 0,
			viewer_can_resolve integer not null default 0,
			viewer_can_unresolve integer not null default 0,
			viewer_can_reply integer not null default 0,
			first_author_login text,
			first_author_type text,
			first_comment_body text,
			first_comment_url text,
			first_comment_created_at text,
			first_comment_updated_at text,
			comments_json text not null,
			raw_json text not null,
			fetched_at text not null,
			deleted_at text,
			deletion_reason text,
			recorded_at text not null,
			foreign key(thread_id, review_thread_id)
				references pull_request_review_threads(thread_id, review_thread_id)
				on delete cascade
		);
		create index if not exists idx_pull_request_review_thread_revisions_thread
			on pull_request_review_thread_revisions(thread_id, review_thread_id, id)
	`); err != nil {
		return fmt.Errorf("ensure family revision tables: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `
		insert into comment_revisions(
			comment_id, author_login, author_type, body, is_bot, raw_json,
			created_at_gh, updated_at_gh, deleted_at, deletion_reason, recorded_at
		)
		select c.id, c.author_login, c.author_type, c.body, c.is_bot, c.raw_json,
			c.created_at_gh, c.updated_at_gh, c.deleted_at, c.deletion_reason,
			coalesce(nullif(c.updated_at_gh, ''), nullif(c.created_at_gh, ''), '1970-01-01T00:00:00Z')
		from comments c
		where not exists (
			select 1 from comment_revisions r where r.comment_id = c.id
		)
	`); err != nil {
		return fmt.Errorf("backfill comment revisions: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `
		insert into pull_request_review_thread_revisions(
			thread_id, review_thread_id, path, line, start_line, is_resolved,
			is_outdated, viewer_can_resolve, viewer_can_unresolve, viewer_can_reply,
			first_author_login, first_author_type, first_comment_body,
			first_comment_url, first_comment_created_at, first_comment_updated_at,
			comments_json, raw_json, fetched_at, deleted_at, deletion_reason, recorded_at
		)
		select rt.thread_id, rt.review_thread_id, rt.path, rt.line, rt.start_line,
			rt.is_resolved, rt.is_outdated, rt.viewer_can_resolve,
			rt.viewer_can_unresolve, rt.viewer_can_reply, rt.first_author_login,
			rt.first_author_type, rt.first_comment_body, rt.first_comment_url,
			rt.first_comment_created_at, rt.first_comment_updated_at,
			rt.comments_json, rt.raw_json, rt.fetched_at, rt.deleted_at,
			rt.deletion_reason, rt.fetched_at
		from pull_request_review_threads rt
		where not exists (
			select 1 from pull_request_review_thread_revisions r
			where r.thread_id = rt.thread_id
				and r.review_thread_id = rt.review_thread_id
		)
	`); err != nil {
		return fmt.Errorf("backfill pull request review thread revisions: %w", err)
	}
	return nil
}

func (s *Store) tableHasFamilyTombstoneConstraint(ctx context.Context, table string) bool {
	var definition string
	if err := s.db.QueryRowContext(ctx, `
		select sql from sqlite_master where type = 'table' and name = ?
	`, table).Scan(&definition); err != nil {
		return false
	}
	normalized := strings.Join(strings.Fields(strings.ToLower(definition)), " ")
	return strings.Contains(normalized, "(deleted_at is null and deletion_reason is null)") &&
		strings.Contains(normalized, "(deleted_at is not null and deletion_reason is not null and trim(deletion_reason) <> '')")
}

func (s *Store) ensureFamilyTombstoneConstraints(ctx context.Context) error {
	tables := []string{"comments", "pull_request_commits", "pull_request_review_threads"}
	rebuild := make(map[string]bool, len(tables))
	for _, table := range tables {
		if !s.tableHasFamilyTombstoneConstraint(ctx, table) {
			rebuild[table] = true
		}
	}
	if len(rebuild) == 0 {
		return nil
	}
	return s.withForeignKeysDisabled(ctx, "family tombstone constraints", func(tx *sql.Tx) error {
		for _, migration := range []struct {
			table      string
			definition string
			columns    string
			indexes    []string
		}{
			{
				table: "comments",
				definition: `(
					id integer primary key,
					thread_id integer not null references threads(id) on delete cascade,
					github_id text not null,
					comment_type text not null,
					author_login text,
					author_type text,
					body text not null,
					is_bot integer not null default 0,
					raw_json text not null,
					raw_json_blob_id integer references blobs(id) on delete set null,
					created_at_gh text,
					updated_at_gh text,
					deleted_at text,
					deletion_reason text,
					check ((deleted_at is null and deletion_reason is null)
						or (deleted_at is not null and deletion_reason is not null and trim(deletion_reason) <> '')),
					unique(thread_id, comment_type, github_id)
				)`,
				columns: "id, thread_id, github_id, comment_type, author_login, author_type, body, is_bot, raw_json, raw_json_blob_id, created_at_gh, updated_at_gh, deleted_at, deletion_reason",
				indexes: []string{
					`create index if not exists idx_comments_thread_type on comments(thread_id, comment_type)`,
				},
			},
			{
				table: "pull_request_commits",
				definition: `(
					thread_id integer not null references threads(id) on delete cascade,
					sha text not null,
					message text,
					author_login text,
					author_name text,
					committed_at text,
					html_url text,
					raw_json text not null,
					fetched_at text not null,
					deleted_at text,
					deletion_reason text,
					check ((deleted_at is null and deletion_reason is null)
						or (deleted_at is not null and deletion_reason is not null and trim(deletion_reason) <> '')),
					primary key(thread_id, sha)
				)`,
				columns: "thread_id, sha, message, author_login, author_name, committed_at, html_url, raw_json, fetched_at, deleted_at, deletion_reason",
			},
			{
				table: "pull_request_review_threads",
				definition: `(
					thread_id integer not null references threads(id) on delete cascade,
					review_thread_id text not null,
					path text,
					line integer not null default 0,
					start_line integer not null default 0,
					is_resolved integer not null default 0,
					is_outdated integer not null default 0,
					viewer_can_resolve integer not null default 0,
					viewer_can_unresolve integer not null default 0,
					viewer_can_reply integer not null default 0,
					first_author_login text,
					first_author_type text,
					first_comment_body text,
					first_comment_url text,
					first_comment_created_at text,
					first_comment_updated_at text,
					comments_json text not null,
					raw_json text not null,
					fetched_at text not null,
					deleted_at text,
					deletion_reason text,
					check ((deleted_at is null and deletion_reason is null)
						or (deleted_at is not null and deletion_reason is not null and trim(deletion_reason) <> '')),
					primary key(thread_id, review_thread_id)
				)`,
				columns: "thread_id, review_thread_id, path, line, start_line, is_resolved, is_outdated, viewer_can_resolve, viewer_can_unresolve, viewer_can_reply, first_author_login, first_author_type, first_comment_body, first_comment_url, first_comment_created_at, first_comment_updated_at, comments_json, raw_json, fetched_at, deleted_at, deletion_reason",
				indexes: []string{
					`create index if not exists idx_pull_request_review_threads_thread_resolved on pull_request_review_threads(thread_id, is_resolved)`,
				},
			},
		} {
			if !rebuild[migration.table] {
				continue
			}
			newTable := migration.table + "_tombstone_new"
			for _, statement := range []string{
				fmt.Sprintf("create table %s %s", newTable, migration.definition),
				fmt.Sprintf("insert into %s(%s) select %s from %s", newTable, migration.columns, migration.columns, migration.table),
				fmt.Sprintf("drop table %s", migration.table),
				fmt.Sprintf("alter table %s rename to %s", newTable, migration.table),
			} {
				if _, err := tx.ExecContext(ctx, statement); err != nil {
					return fmt.Errorf("rebuild %s: %w", migration.table, err)
				}
			}
			for _, statement := range migration.indexes {
				if _, err := tx.ExecContext(ctx, statement); err != nil {
					return fmt.Errorf("recreate %s index: %w", migration.table, err)
				}
			}
		}
		return nil
	})
}
