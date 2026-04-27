package store

import (
	"context"
	"fmt"
)

type Document struct {
	ID         int64  `json:"id"`
	ThreadID   int64  `json:"thread_id"`
	Title      string `json:"title"`
	Body       string `json:"body,omitempty"`
	RawText    string `json:"raw_text"`
	DedupeText string `json:"dedupe_text"`
	UpdatedAt  string `json:"updated_at"`
}

func (s *Store) UpsertDocument(ctx context.Context, doc Document) (int64, error) {
	var id int64
	err := s.db.QueryRowContext(ctx, `
		insert into documents(thread_id, title, body, raw_text, dedupe_text, updated_at)
		values(?, ?, ?, ?, ?, ?)
		on conflict(thread_id) do update set
			title=excluded.title,
			body=excluded.body,
			raw_text=excluded.raw_text,
			dedupe_text=excluded.dedupe_text,
			updated_at=excluded.updated_at
		returning id
	`, doc.ThreadID, doc.Title, nullString(doc.Body), doc.RawText, doc.DedupeText, doc.UpdatedAt).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("upsert document: %w", err)
	}
	return id, nil
}
