package cli

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

var gitcrawlRepositoryColumns = []string{"id", "full_name", "owner", "name", "html_url", "default_branch", "updated_at"}

const gitcrawlRepositoryExportSQL = `
select id, full_name, owner, name, '' as html_url, '' as default_branch, updated_at
from repositories
order by id`

var gitcrawlThreadColumns = []string{"id", "repo_id", "github_id", "number", "kind", "state", "title", "body", "author_login", "author_type", "html_url", "labels_json", "assignees_json", "is_draft", "created_at_gh", "updated_at_gh", "closed_at_gh", "merged_at_gh", "updated_at"}

func gitcrawlThreadExportSQL(ctx context.Context, db *sql.DB) (string, error) {
	bodyExpr, err := gitcrawlThreadBodyExpr(ctx, db)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`
select id, repo_id, github_id, number, kind, state, title, %s,
       coalesce(author_login, ''), coalesce(author_type, ''), html_url,
       labels_json, assignees_json, is_draft, coalesce(created_at_gh, ''),
       coalesce(updated_at_gh, ''), coalesce(closed_at_gh, ''),
       coalesce(merged_at_gh, ''), updated_at
from threads
order by repo_id, number`, bodyExpr), nil
}

func gitcrawlThreadBodyExpr(ctx context.Context, db *sql.DB) (string, error) {
	hasBody, err := sqliteColumnExists(ctx, db, "threads", "body")
	if err != nil {
		return "", err
	}
	hasExcerpt, err := sqliteColumnExists(ctx, db, "threads", "body_excerpt")
	if err != nil {
		return "", err
	}
	switch {
	case hasBody && hasExcerpt:
		return "coalesce(body, body_excerpt, '')", nil
	case hasBody:
		return "coalesce(body, '')", nil
	case hasExcerpt:
		return "coalesce(body_excerpt, '')", nil
	default:
		return "''", nil
	}
}

func sqliteColumnExists(ctx context.Context, db *sql.DB, table, column string) (bool, error) {
	rows, err := db.QueryContext(ctx, `pragma table_info(`+table+`)`)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, typ string
		var notNull int
		var defaultValue any
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk); err != nil {
			return false, err
		}
		if name == column {
			return true, nil
		}
	}
	return false, rows.Err()
}

func sqliteTableExists(ctx context.Context, db *sql.DB, table string) (bool, error) {
	var name string
	err := db.QueryRowContext(
		ctx,
		`select name from sqlite_schema where type in ('table', 'view') and name = ?`,
		table,
	).Scan(&name)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("inspect cloud snapshot table %s: %w", table, err)
	}
	return name == table, nil
}
