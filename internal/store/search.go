package store

import (
	"context"
	"database/sql"
	"fmt"
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
	rows, err := s.db.QueryContext(ctx, `
		select t.id, t.number, t.kind, t.state, t.title, t.html_url, t.author_login,
			snippet(documents_fts, 3, '[', ']', '...', 18)
		from documents_fts
		join documents d on d.id = documents_fts.rowid
		join threads t on t.id = d.thread_id
		where t.repo_id = ? and documents_fts match ?
		order by bm25(documents_fts)
		limit ?
	`, repoID, query, limit)
	if err != nil {
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
	return out, nil
}
