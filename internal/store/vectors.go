package store

import (
	"context"
	"encoding/json"
	"fmt"
)

type ThreadVector struct {
	ThreadID    int64     `json:"thread_id"`
	Basis       string    `json:"basis"`
	Model       string    `json:"model"`
	Dimensions  int       `json:"dimensions"`
	ContentHash string    `json:"content_hash"`
	Vector      []float64 `json:"vector"`
	Backend     string    `json:"backend"`
	CreatedAt   string    `json:"created_at"`
	UpdatedAt   string    `json:"updated_at"`
}

func (s *Store) UpsertThreadVector(ctx context.Context, vector ThreadVector) error {
	data, err := json.Marshal(vector.Vector)
	if err != nil {
		return fmt.Errorf("marshal vector: %w", err)
	}
	if vector.Backend == "" {
		vector.Backend = "exact"
	}
	_, err = s.db.ExecContext(ctx, `
		insert into thread_vectors(thread_id, basis, model, dimensions, content_hash, vector_json, vector_backend, created_at, updated_at)
		values(?, ?, ?, ?, ?, ?, ?, ?, ?)
		on conflict(thread_id) do update set
			basis=excluded.basis,
			model=excluded.model,
			dimensions=excluded.dimensions,
			content_hash=excluded.content_hash,
			vector_json=excluded.vector_json,
			vector_backend=excluded.vector_backend,
			updated_at=excluded.updated_at
	`, vector.ThreadID, vector.Basis, vector.Model, vector.Dimensions, vector.ContentHash, string(data), vector.Backend, vector.CreatedAt, vector.UpdatedAt)
	if err != nil {
		return fmt.Errorf("upsert thread vector: %w", err)
	}
	return nil
}

func (s *Store) ListThreadVectors(ctx context.Context, repoID int64) ([]ThreadVector, error) {
	rows, err := s.db.QueryContext(ctx, `
		select tv.thread_id, tv.basis, tv.model, tv.dimensions, tv.content_hash, tv.vector_json, tv.vector_backend, tv.created_at, tv.updated_at
		from thread_vectors tv
		join threads t on t.id = tv.thread_id
		where t.repo_id = ? and t.state = 'open' and t.closed_at_local is null
		order by tv.thread_id
	`, repoID)
	if err != nil {
		return nil, fmt.Errorf("list thread vectors: %w", err)
	}
	defer rows.Close()

	var out []ThreadVector
	for rows.Next() {
		var vector ThreadVector
		var raw string
		if err := rows.Scan(&vector.ThreadID, &vector.Basis, &vector.Model, &vector.Dimensions, &vector.ContentHash, &raw, &vector.Backend, &vector.CreatedAt, &vector.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan thread vector: %w", err)
		}
		if err := json.Unmarshal([]byte(raw), &vector.Vector); err != nil {
			return nil, fmt.Errorf("decode thread vector: %w", err)
		}
		out = append(out, vector)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate thread vectors: %w", err)
	}
	return out, nil
}
