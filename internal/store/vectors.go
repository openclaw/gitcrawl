package store

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"unicode"
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

type ThreadVectorQuery struct {
	RepoID        int64
	Model         string
	Basis         string
	Dimensions    int
	IncludeClosed bool
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
	return s.ListThreadVectorsFiltered(ctx, ThreadVectorQuery{RepoID: repoID})
}

func (s *Store) ListThreadVectorsFiltered(ctx context.Context, query ThreadVectorQuery) ([]ThreadVector, error) {
	if !s.hasTable(ctx, "thread_vectors") {
		return []ThreadVector{}, nil
	}
	where, args := threadVectorWhere(query)
	rows, err := s.db.QueryContext(ctx, `
		select tv.thread_id, tv.basis, tv.model, tv.dimensions, tv.content_hash, tv.vector_json, tv.vector_backend, tv.created_at, tv.updated_at
		from thread_vectors tv
		join threads t on t.id = tv.thread_id
		where `+where+`
		order by tv.thread_id
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("list thread vectors: %w", err)
	}
	defer rows.Close()

	var out []ThreadVector
	for rows.Next() {
		var vector ThreadVector
		var raw []byte
		if err := rows.Scan(&vector.ThreadID, &vector.Basis, &vector.Model, &vector.Dimensions, &vector.ContentHash, &raw, &vector.Backend, &vector.CreatedAt, &vector.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan thread vector: %w", err)
		}
		vector.Vector, err = decodeStoredVector(raw)
		if err != nil {
			return nil, err
		}
		out = append(out, vector)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate thread vectors: %w", err)
	}
	return out, nil
}

func (s *Store) ThreadVectorByNumber(ctx context.Context, query ThreadVectorQuery, number int) (Thread, ThreadVector, error) {
	if !s.hasTable(ctx, "thread_vectors") {
		return Thread{}, ThreadVector{}, fmt.Errorf("thread #%d was not found with an embedding", number)
	}
	where, args := threadVectorWhere(query)
	args = append(args, number)
	row := s.db.QueryRowContext(ctx, `
		select `+s.threadSelectColumns(ctx, "t")+`,
			tv.thread_id, tv.basis, tv.model, tv.dimensions, tv.content_hash, tv.vector_json, tv.vector_backend, tv.created_at, tv.updated_at
		from threads t
		join thread_vectors tv on tv.thread_id = t.id
		where `+where+` and t.number = ?
		limit 1
	`, args...)

	thread, vector, err := scanThreadVector(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Thread{}, ThreadVector{}, fmt.Errorf("thread #%d was not found with an embedding", number)
		}
		return Thread{}, ThreadVector{}, err
	}
	return thread, vector, nil
}

func (s *Store) ThreadsByIDs(ctx context.Context, repoID int64, ids []int64) (map[int64]Thread, error) {
	if len(ids) == 0 {
		return map[int64]Thread{}, nil
	}
	placeholders := make([]string, 0, len(ids))
	args := []any{repoID}
	for _, id := range ids {
		placeholders = append(placeholders, "?")
		args = append(args, id)
	}
	rows, err := s.db.QueryContext(ctx, `
		select `+s.threadSelectColumns(ctx, "")+`
		from threads
		where repo_id = ? and id in (`+strings.Join(placeholders, ",")+`)
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("select threads by id: %w", err)
	}
	defer rows.Close()

	out := make(map[int64]Thread, len(ids))
	for rows.Next() {
		thread, err := scanThread(rows)
		if err != nil {
			return nil, err
		}
		out[thread.ID] = thread
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate threads by id: %w", err)
	}
	return out, nil
}

func threadVectorWhere(query ThreadVectorQuery) (string, []any) {
	where := `t.repo_id = ?`
	args := []any{query.RepoID}
	if !query.IncludeClosed {
		where += ` and t.state = 'open' and t.closed_at_local is null`
	}
	if query.Model != "" {
		where += ` and tv.model = ?`
		args = append(args, query.Model)
	}
	if query.Basis != "" {
		where += ` and tv.basis = ?`
		args = append(args, query.Basis)
	}
	if query.Dimensions > 0 {
		where += ` and tv.dimensions = ?`
		args = append(args, query.Dimensions)
	}
	return where, args
}

func scanThreadVector(row interface {
	Scan(dest ...any) error
}) (Thread, ThreadVector, error) {
	var thread Thread
	var vector ThreadVector
	var body, authorLogin, authorType, rawJSON, createdAt, updatedAtGH, closedAt, mergedAt, firstPulled, lastPulled, closedLocal, closeReason sql.NullString
	var isDraft int
	var raw []byte
	if err := row.Scan(&thread.ID, &thread.RepoID, &thread.GitHubID, &thread.Number, &thread.Kind, &thread.State, &thread.Title,
		&body, &authorLogin, &authorType, &thread.HTMLURL, &thread.LabelsJSON, &thread.AssigneesJSON, &rawJSON,
		&thread.ContentHash, &isDraft, &createdAt, &updatedAtGH, &closedAt, &mergedAt, &firstPulled, &lastPulled, &thread.UpdatedAt,
		&closedLocal, &closeReason,
		&vector.ThreadID, &vector.Basis, &vector.Model, &vector.Dimensions, &vector.ContentHash, &raw, &vector.Backend, &vector.CreatedAt, &vector.UpdatedAt); err != nil {
		return Thread{}, ThreadVector{}, fmt.Errorf("scan thread vector: %w", err)
	}
	thread.Body = body.String
	thread.AuthorLogin = authorLogin.String
	thread.AuthorType = authorType.String
	thread.CreatedAtGitHub = createdAt.String
	thread.UpdatedAtGitHub = updatedAtGH.String
	thread.ClosedAtGitHub = closedAt.String
	thread.MergedAtGitHub = mergedAt.String
	thread.FirstPulledAt = firstPulled.String
	thread.LastPulledAt = lastPulled.String
	thread.ClosedAtLocal = closedLocal.String
	thread.CloseReasonLocal = closeReason.String
	thread.RawJSON = rawJSON.String
	thread.IsDraft = isDraft != 0
	decoded, err := decodeStoredVector(raw)
	if err != nil {
		return Thread{}, ThreadVector{}, err
	}
	vector.Vector = decoded
	return thread, vector, nil
}

func decodeStoredVector(raw []byte) ([]float64, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("stored vector payload is empty")
	}
	for _, value := range string(raw) {
		if unicode.IsSpace(value) {
			continue
		}
		if value == '[' {
			var vector []float64
			if err := json.Unmarshal(raw, &vector); err != nil {
				return nil, fmt.Errorf("decode JSON thread vector: %w", err)
			}
			return vector, nil
		}
		break
	}
	if len(raw)%4 != 0 {
		return nil, fmt.Errorf("decode binary thread vector: byte length %d is not divisible by 4", len(raw))
	}
	vector := make([]float64, 0, len(raw)/4)
	for offset := 0; offset < len(raw); offset += 4 {
		bits := binary.LittleEndian.Uint32(raw[offset : offset+4])
		vector = append(vector, float64(math.Float32frombits(bits)))
	}
	return vector, nil
}
