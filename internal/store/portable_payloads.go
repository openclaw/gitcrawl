package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

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
