package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

type EmbeddingTask struct {
	ThreadID    int64  `json:"thread_id"`
	Number      int    `json:"number"`
	Kind        string `json:"kind"`
	Title       string `json:"title"`
	Text        string `json:"-"`
	ContentHash string `json:"content_hash"`
}

type EmbeddingTaskOptions struct {
	RepoID        int64
	Basis         string
	Model         string
	Number        int
	Limit         int
	Force         bool
	IncludeClosed bool
}

func (s *Store) ListEmbeddingTasks(ctx context.Context, options EmbeddingTaskOptions) ([]EmbeddingTask, error) {
	basis := strings.TrimSpace(options.Basis)
	if basis == "" {
		basis = "title_original"
	}
	model := strings.TrimSpace(options.Model)
	where := []string{`t.repo_id = ?`}
	args := []any{options.RepoID}
	if !options.IncludeClosed {
		where = append(where, `t.state = 'open'`, `t.closed_at_local is null`)
	}
	if options.Number > 0 {
		where = append(where, `t.number = ?`)
		args = append(args, options.Number)
	}
	limitSQL := ``
	if options.Limit > 0 {
		limitSQL = ` limit ?`
		args = append(args, options.Limit)
	}
	rows, err := s.q().QueryContext(ctx, `
		select t.id, t.number, t.kind, t.title, coalesce(d.body, ''), coalesce(d.raw_text, ''), coalesce(d.dedupe_text, ''),
		       coalesce(tv.content_hash, '')
		from threads t
		join documents d on d.thread_id = t.id
		left join thread_vectors tv on tv.thread_id = t.id and tv.basis = ? and tv.model = ?
		where `+strings.Join(where, " and ")+`
		order by coalesce(t.updated_at_gh, t.updated_at) desc, t.number desc`+limitSQL,
		append([]any{basis, model}, args...)...)
	if err != nil {
		return nil, fmt.Errorf("list embedding tasks: %w", err)
	}
	defer rows.Close()

	out := make([]EmbeddingTask, 0)
	for rows.Next() {
		var task EmbeddingTask
		var body, rawText, dedupeText, existingHash string
		if err := rows.Scan(&task.ThreadID, &task.Number, &task.Kind, &task.Title, &body, &rawText, &dedupeText, &existingHash); err != nil {
			return nil, fmt.Errorf("scan embedding task: %w", err)
		}
		text, err := embeddingTextForBasis(basis, task.Title, body, rawText, dedupeText)
		if err != nil {
			return nil, err
		}
		task.Text = text
		task.ContentHash = embeddingContentHash(basis, model, text)
		if !options.Force && existingHash == task.ContentHash {
			continue
		}
		out = append(out, task)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate embedding tasks: %w", err)
	}
	return out, nil
}

func embeddingTextForBasis(basis, title, body, rawText, dedupeText string) (string, error) {
	switch basis {
	case "", "title_original":
		parts := []string{strings.TrimSpace(title)}
		if strings.TrimSpace(body) != "" {
			parts = append(parts, strings.TrimSpace(body))
		} else if strings.TrimSpace(rawText) != "" {
			parts = append(parts, strings.TrimSpace(rawText))
		}
		return strings.TrimSpace(strings.Join(parts, "\n\n")), nil
	case "dedupe_text":
		return strings.TrimSpace(dedupeText), nil
	default:
		return "", fmt.Errorf("embedding basis %q is not supported yet", basis)
	}
}

func embeddingContentHash(basis, model, text string) string {
	sum := sha256.Sum256([]byte("embedding:" + basis + ":" + model + "\n" + text))
	return hex.EncodeToString(sum[:])
}
