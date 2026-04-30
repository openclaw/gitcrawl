package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

type EmbeddingTask struct {
	ThreadID          int64  `json:"thread_id"`
	Number            int    `json:"number"`
	Kind              string `json:"kind"`
	Title             string `json:"title"`
	Text              string `json:"-"`
	ContentHash       string `json:"content_hash"`
	TextTruncated     bool   `json:"text_truncated,omitempty"`
	OriginalTextRunes int    `json:"original_text_runes,omitempty"`
	TextRunes         int    `json:"text_runes,omitempty"`
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

const (
	MaxEmbeddingTextRunes       = 6_000
	embeddingContentHashVersion = "embedding:v2:max_runes=24000"
)

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
		select t.id, t.number, t.kind, t.title, coalesce(d.body, t.body, ''), coalesce(d.raw_text, t.body, ''), coalesce(d.dedupe_text, t.title || ' ' || coalesce(t.body, '')),
		       coalesce((
		         select tks.key_text
		         from thread_key_summaries tks
		         join thread_revisions tr on tr.id = tks.thread_revision_id
		         where tr.thread_id = t.id
		           and tks.summary_kind in ('llm_key_summary', 'llm_key_3line')
		         order by tks.created_at desc, tr.created_at desc, tks.id desc
		         limit 1
		       ), ''),
		       coalesce(tv.content_hash, '')
		from threads t
		left join documents d on d.thread_id = t.id
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
		var body, rawText, dedupeText, keySummary, existingHash string
		if err := rows.Scan(&task.ThreadID, &task.Number, &task.Kind, &task.Title, &body, &rawText, &dedupeText, &keySummary, &existingHash); err != nil {
			return nil, fmt.Errorf("scan embedding task: %w", err)
		}
		text, meta, err := embeddingTextForBasisWithMeta(basis, task.Title, body, rawText, dedupeText, keySummary)
		if err != nil {
			return nil, err
		}
		if strings.TrimSpace(text) == "" {
			continue
		}
		task.Text = text
		task.TextTruncated = meta.Truncated
		task.OriginalTextRunes = meta.OriginalRunes
		task.TextRunes = meta.Runes
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

func embeddingTextForBasis(basis, title, body, rawText, dedupeText, keySummary string) (string, error) {
	text, _, err := embeddingTextForBasisWithMeta(basis, title, body, rawText, dedupeText, keySummary)
	return text, err
}

type embeddingTextMeta struct {
	Truncated     bool
	OriginalRunes int
	Runes         int
}

func embeddingTextForBasisWithMeta(basis, title, body, rawText, dedupeText, keySummary string) (string, embeddingTextMeta, error) {
	var text string
	switch basis {
	case "", "title_original":
		parts := []string{strings.TrimSpace(title)}
		if strings.TrimSpace(body) != "" {
			parts = append(parts, strings.TrimSpace(body))
		} else if strings.TrimSpace(rawText) != "" {
			parts = append(parts, strings.TrimSpace(rawText))
		}
		text = strings.TrimSpace(strings.Join(parts, "\n\n"))
	case "dedupe_text":
		text = strings.TrimSpace(dedupeText)
	case "llm_key_summary":
		keySummary = strings.TrimSpace(keySummary)
		if keySummary == "" {
			return "", embeddingTextMeta{}, nil
		}
		text = strings.TrimSpace("title: " + strings.TrimSpace(title) + "\n\nkey_summary:\n" + keySummary)
	default:
		return "", embeddingTextMeta{}, fmt.Errorf("embedding basis %q is not supported yet", basis)
	}
	text, meta := capEmbeddingText(text)
	return text, meta, nil
}

func capEmbeddingText(text string) (string, embeddingTextMeta) {
	runes := []rune(strings.TrimSpace(text))
	meta := embeddingTextMeta{OriginalRunes: len(runes), Runes: len(runes)}
	if len(runes) <= MaxEmbeddingTextRunes {
		return string(runes), meta
	}
	meta.Truncated = true
	meta.Runes = MaxEmbeddingTextRunes
	return string(runes[:MaxEmbeddingTextRunes]), meta
}

func embeddingContentHash(basis, model, text string) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s:%s:%s\n%s", embeddingContentHashVersion, basis, model, text)))
	return hex.EncodeToString(sum[:])
}
