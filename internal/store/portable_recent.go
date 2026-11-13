package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// RestrictPortableSince removes only provably old closed threads from a
// disposable copy. All children of retained threads, repository workflow state,
// and observation allocators survive. The export pipeline owns the final FK proof.
func (s *Store) RestrictPortableSince(ctx context.Context, since time.Time) error {
	if since.IsZero() {
		return fmt.Errorf("portable since must be a nonzero timestamp")
	}
	if err := s.CheckPortablePublication(ctx); err != nil {
		return err
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `pragma foreign_keys = on`); err != nil {
		return err
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `create temp table _portable_recent_drop(id integer primary key)`); err != nil {
		return err
	}
	// Parse provider clocks in Go: lexical ordering mishandles offsets and
	// SQLite date functions lose the fractional precision of RFC3339Nano.
	if err := portableRecentCandidates(ctx, tx, since, false); err != nil {
		return err
	}
	if err := portableRecentCandidates(ctx, tx, since, true); err != nil {
		return err
	}
	// Immutable derived generations are omitted by the portable profile. Their
	// triggers would otherwise prevent deletion of their referenced threads.
	for _, table := range clusterCandidateGenerationTables() {
		if _, err := tx.ExecContext(ctx, `drop table if exists `+sqliteIdentifier(table)); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `create temp table _portable_recent_revisions as
		select id from thread_revisions where thread_id in (select id from _portable_recent_drop)`); err != nil {
		return err
	}
	tables, err := portableTableNames(ctx, tx)
	if err != nil {
		return err
	}
	for _, table := range tables {
		if table == "threads" || table == "thread_revisions" {
			continue
		}
		columns, err := portableTableColumnSet(ctx, tx, table)
		if err != nil {
			return err
		}
		var conditions []string
		if columns["thread_id"] {
			conditions = append(conditions, `thread_id in (select id from _portable_recent_drop)`)
		}
		if columns["thread_revision_id"] {
			conditions = append(conditions, `thread_revision_id in (select id from _portable_recent_revisions)`)
		}
		if len(conditions) > 0 {
			if _, err := tx.ExecContext(ctx, `delete from `+sqliteIdentifier(table)+` where `+strings.Join(conditions, " or ")); err != nil {
				return fmt.Errorf("restrict recent portable %s: %w", table, err)
			}
		}
	}
	for _, statement := range []string{
		`delete from thread_revisions where id in (select id from _portable_recent_revisions)`,
		`delete from threads where id in (select id from _portable_recent_drop)`,
		`delete from repo_sync_state`,
		`drop table _portable_recent_revisions`,
		`drop table _portable_recent_drop`,
	} {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("finalize recent portable scope: %w", err)
		}
	}
	return tx.Commit()
}

// Each batch holds only IDs, state and clocks, never payload bodies. Parent
// candidates are mutated only after closing the SELECT that joins them.
func portableRecentCandidates(ctx context.Context, tx *sql.Tx, since time.Time, comments bool) error {
	query := `select id, id, state, created_at_gh, updated_at_gh, closed_at_gh, merged_at_gh, null, null
		from threads where (? or id > ?) order by id limit 500`
	mutation := `insert into _portable_recent_drop(id) values(?)`
	if comments {
		query = `select c.id, c.thread_id, '', c.created_at_gh, c.updated_at_gh, c.deleted_at, c.submitted_at_gh, c.publication_at_gh, null
			from comments c join _portable_recent_drop d on d.id = c.thread_id
			where (? or c.id > ?) order by c.id limit 500`
		mutation = `delete from _portable_recent_drop where id = ?`
	}
	stmt, err := tx.PrepareContext(ctx, mutation)
	if err != nil {
		return err
	}
	defer stmt.Close()
	var last int64
	for first := true; ; first = false {
		rows, err := tx.QueryContext(ctx, query, first, last)
		if err != nil {
			return err
		}
		var selected []int64
		count := 0
		for rows.Next() {
			var parent int64
			var state string
			var dates [6]sql.NullString
			if err := rows.Scan(&last, &parent, &state, &dates[0], &dates[1], &dates[2], &dates[3], &dates[4], &dates[5]); err != nil {
				rows.Close()
				return err
			}
			count++
			old := portableActivityBefore(since, dates[:])
			if !comments && !dates[1].Valid {
				old = false // Missing thread update time cannot prove inactivity.
			}
			if (!comments && state == "closed" && old) || (comments && !old) {
				selected = append(selected, parent)
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		for _, id := range selected {
			if _, err := stmt.ExecContext(ctx, id); err != nil {
				return err
			}
		}
		if count < 500 {
			return nil
		}
	}
}

func portableActivityBefore(since time.Time, dates []sql.NullString) bool {
	// At least one provider creation/update clock is required. Optional absent
	// close/merge/delete/publication clocks do not imply unknown activity.
	if !dates[0].Valid && !dates[1].Valid {
		return false
	}
	for _, date := range dates {
		if !date.Valid {
			continue
		}
		parsed, err := time.Parse(time.RFC3339Nano, date.String)
		if err != nil || !parsed.Before(since) {
			return false
		}
	}
	return true
}
