package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	crawlstore "github.com/openclaw/crawlkit/store"
)

type SearchHit struct {
	ThreadID    int64   `json:"thread_id"`
	Number      int     `json:"number"`
	Kind        string  `json:"kind"`
	State       string  `json:"state"`
	Title       string  `json:"title"`
	HTMLURL     string  `json:"html_url"`
	AuthorLogin string  `json:"author_login,omitempty"`
	Snippet     string  `json:"snippet"`
	Score       float64 `json:"score,omitempty"`
}

type ThreadSearchOptions struct {
	RepoID               int64
	Query                string
	Kind                 string
	State                string
	Author               string
	Assignee             string
	Labels               []string
	IncludeLocallyClosed bool
	Limit                int
}

func (s *Store) SearchDocuments(ctx context.Context, repoID int64, query string, limit int) ([]SearchHit, error) {
	if limit <= 0 {
		limit = 20
	}
	matchQuery := crawlstore.FTS5TokenQuery(query)
	if matchQuery == "" {
		return s.searchThreads(ctx, repoID, query, limit)
	}
	rows, err := s.db.QueryContext(ctx, `
		select t.id, t.number, t.kind, t.state, t.title, t.html_url, t.author_login,
			snippet(documents_fts, 3, '[', ']', '...', 18)
		from documents_fts
		join documents d on d.id = documents_fts.rowid
		join threads t on t.id = d.thread_id
		where t.repo_id = ? and documents_fts match ?
		order by bm25(documents_fts)
		limit ?
	`, repoID, matchQuery, limit)
	if err != nil {
		fallback, fallbackErr := s.searchThreads(ctx, repoID, query, limit)
		if fallbackErr == nil {
			return fallback, nil
		}
		return nil, fmt.Errorf("search documents: %w", err)
	}
	defer rows.Close()

	var out []SearchHit
	for rows.Next() {
		var hit SearchHit
		var author sql.NullString
		if err := rows.Scan(&hit.ThreadID, &hit.Number, &hit.Kind, &hit.State, &hit.Title, &hit.HTMLURL, &author, &hit.Snippet); err != nil {
			return nil, fmt.Errorf("scan search hit: %w", err)
		}
		hit.AuthorLogin = author.String
		out = append(out, hit)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate search hits: %w", err)
	}
	if len(out) == 0 {
		return s.searchThreads(ctx, repoID, query, limit)
	}
	return out, nil
}

func (s *Store) searchThreads(ctx context.Context, repoID int64, query string, limit int) ([]SearchHit, error) {
	needle := strings.TrimSpace(strings.ToLower(query))
	if needle == "" {
		return nil, nil
	}
	pattern := "%" + escapeLike(needle) + "%"
	bodyExpr := s.threadBodyExpr(ctx, "")
	rows, err := s.db.QueryContext(ctx, `
		select id, number, kind, state, title, html_url, author_login,
			coalesce(nullif(`+bodyExpr+`, ''), title)
		from threads
		where repo_id = ?
		  and (
			lower(title) like ? escape '\'
			or lower(coalesce(`+bodyExpr+`, '')) like ? escape '\'
		  )
		order by coalesce(updated_at_gh, updated_at) desc, number desc
		limit ?
	`, repoID, pattern, pattern, limit)
	if err != nil {
		return nil, fmt.Errorf("search threads: %w", err)
	}
	defer rows.Close()

	out := make([]SearchHit, 0)
	for rows.Next() {
		var hit SearchHit
		var author sql.NullString
		var snippet sql.NullString
		if err := rows.Scan(&hit.ThreadID, &hit.Number, &hit.Kind, &hit.State, &hit.Title, &hit.HTMLURL, &author, &snippet); err != nil {
			return nil, fmt.Errorf("scan thread search hit: %w", err)
		}
		hit.AuthorLogin = author.String
		hit.Snippet = snippet.String
		out = append(out, hit)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate thread search hits: %w", err)
	}
	return out, nil
}

func (s *Store) SearchThreads(ctx context.Context, options ThreadSearchOptions) ([]Thread, error) {
	if options.Limit <= 0 {
		options.Limit = 20
	}
	query := strings.TrimSpace(options.Query)
	if query == "" {
		return s.searchThreadsFiltered(ctx, options, "")
	}
	matchQuery := crawlstore.FTS5TokenQuery(query)
	if matchQuery != "" {
		out, err := s.searchThreadsFiltered(ctx, options, matchQuery)
		if err == nil && len(out) > 0 {
			return out, nil
		}
	}
	return s.searchThreadsLike(ctx, options)
}

func (s *Store) searchThreadsFiltered(ctx context.Context, options ThreadSearchOptions, matchQuery string) ([]Thread, error) {
	where, args := threadSearchWhere(options)
	from := `threads t`
	if matchQuery != "" {
		from = `documents_fts join documents d on d.id = documents_fts.rowid join threads t on t.id = d.thread_id`
		where = append(where, `documents_fts match ?`)
		args = append(args, matchQuery)
	}
	args = append(args, options.Limit)
	rows, err := s.q().QueryContext(ctx, `
		select `+s.threadSelectColumns(ctx, "t")+`
		from `+from+`
		where `+strings.Join(where, " and ")+`
		order by `+threadSearchOrder(matchQuery != "")+`
		limit ?
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("search threads: %w", err)
	}
	defer rows.Close()
	return scanThreadRows(rows)
}

func (s *Store) searchThreadsLike(ctx context.Context, options ThreadSearchOptions) ([]Thread, error) {
	where, args := threadSearchWhere(options)
	needle := strings.TrimSpace(strings.ToLower(options.Query))
	bodyExpr := s.threadBodyExpr(ctx, "t")
	if needle != "" {
		pattern := "%" + escapeLike(needle) + "%"
		where = append(where, `(lower(t.title) like ? escape '\' or lower(coalesce(`+bodyExpr+`, '')) like ? escape '\')`)
		args = append(args, pattern, pattern)
	}
	args = append(args, options.Limit)
	rows, err := s.q().QueryContext(ctx, `
		select `+s.threadSelectColumns(ctx, "t")+`
		from threads t
		where `+strings.Join(where, " and ")+`
		order by coalesce(t.updated_at_gh, t.updated_at) desc, t.number desc
		limit ?
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("search threads: %w", err)
	}
	defer rows.Close()
	return scanThreadRows(rows)
}

func threadSearchWhere(options ThreadSearchOptions) ([]string, []any) {
	where := []string{`t.repo_id = ?`}
	args := []any{options.RepoID}
	if strings.TrimSpace(options.Kind) != "" {
		where = append(where, `t.kind = ?`)
		args = append(args, strings.TrimSpace(options.Kind))
	}
	if strings.TrimSpace(options.State) != "" && strings.TrimSpace(options.State) != "all" {
		where = append(where, `t.state = ?`)
		args = append(args, strings.TrimSpace(options.State))
	}
	if author := strings.TrimSpace(options.Author); author != "" {
		where = append(where, `lower(coalesce(t.author_login, '')) = lower(?)`)
		args = append(args, author)
	}
	if assignee := strings.TrimSpace(options.Assignee); assignee != "" {
		where = append(where, `exists (
			select 1
			from json_each(case when json_valid(t.assignees_json) then t.assignees_json else '[]' end) a
			where lower(case when json_valid(a.value) then coalesce(json_extract(a.value, '$.login'), a.value) else a.value end) = lower(?)
		)`)
		args = append(args, assignee)
	}
	for _, label := range options.Labels {
		if label = strings.TrimSpace(label); label == "" {
			continue
		}
		where = append(where, `exists (
			select 1
			from json_each(case when json_valid(t.labels_json) then t.labels_json else '[]' end) l
			where lower(case when json_valid(l.value) then coalesce(json_extract(l.value, '$.name'), l.value) else l.value end) = lower(?)
		)`)
		args = append(args, label)
	}
	if !options.IncludeLocallyClosed {
		where = append(where, `t.closed_at_local is null`)
	}
	return where, args
}

func threadSearchOrder(fts bool) string {
	if fts {
		return `bm25(documents_fts), coalesce(t.updated_at_gh, t.updated_at) desc, t.number desc`
	}
	return `coalesce(t.updated_at_gh, t.updated_at) desc, t.number desc`
}

func scanThreadRows(rows *sql.Rows) ([]Thread, error) {
	var out []Thread
	for rows.Next() {
		thread, err := scanThread(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, thread)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate threads: %w", err)
	}
	return out, nil
}

func escapeLike(value string) string {
	var b strings.Builder
	for _, r := range value {
		switch r {
		case '\\', '%', '_':
			b.WriteRune('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}
