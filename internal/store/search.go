package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"unicode"
)

type SearchHit struct {
	ThreadID    int64  `json:"thread_id"`
	Number      int    `json:"number"`
	Kind        string `json:"kind"`
	State       string `json:"state"`
	Title       string `json:"title"`
	HTMLURL     string `json:"html_url"`
	AuthorLogin string `json:"author_login,omitempty"`
	Snippet     string `json:"snippet"`
}

func (s *Store) SearchDocuments(ctx context.Context, repoID int64, query string, limit int) ([]SearchHit, error) {
	if limit <= 0 {
		limit = 20
	}
	matchQuery := ftsQuery(query)
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
	rows, err := s.db.QueryContext(ctx, `
		select id, number, kind, state, title, html_url, author_login,
			coalesce(nullif(body, ''), title)
		from threads
		where repo_id = ?
		  and (
			lower(title) like ? escape '\'
			or lower(coalesce(body, '')) like ? escape '\'
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

func ftsQuery(value string) string {
	terms := make([]string, 0)
	var b strings.Builder
	flush := func() {
		if b.Len() == 0 {
			return
		}
		terms = append(terms, `"`+b.String()+`"`)
		b.Reset()
	}
	for _, r := range value {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' {
			b.WriteRune(r)
			continue
		}
		flush()
	}
	flush()
	return strings.Join(terms, " ")
}
