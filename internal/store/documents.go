package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/openclaw/gitcrawl/internal/store/storedb"
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
	id, err := s.qsql().UpsertDocument(ctx, storedb.UpsertDocumentParams{
		ThreadID:   doc.ThreadID,
		Title:      doc.Title,
		Body:       nullString(doc.Body),
		RawText:    doc.RawText,
		DedupeText: doc.DedupeText,
		UpdatedAt:  doc.UpdatedAt,
	})
	if errors.Is(err, sql.ErrNoRows) {
		err = s.q().QueryRowContext(ctx, `select id from documents where thread_id = ?`, doc.ThreadID).Scan(&id)
	}
	if err != nil {
		return 0, fmt.Errorf("upsert document: %w", err)
	}
	return id, nil
}
