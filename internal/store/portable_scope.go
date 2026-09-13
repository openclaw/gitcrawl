package store

import (
	"context"
	"fmt"
	"strings"
)

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
