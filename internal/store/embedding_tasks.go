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
	MaxEmbeddingTextBytes       = 7_000
	embeddingContentHashVersion = "embedding:v5"
)

func (s *Store) ListEmbeddingTasks(ctx context.Context, options EmbeddingTaskOptions) ([]EmbeddingTask, error) {
	basis := strings.TrimSpace(options.Basis)
	if basis == "" {
		basis = "title_original"
	}
	model := strings.TrimSpace(options.Model)
	var number any
	if options.Number > 0 {
		number = options.Number
	}
	revisionOrder := s.latestThreadRevisionConsumerOrder(ctx, "latest", "t")
	summaryFresh := s.threadRevisionFreshnessPredicate(ctx, "tr", "t")
	eligibleSummaryFresh := s.threadRevisionFreshnessPredicate(ctx, "eligible_revision", "t")
	rows, err := s.q().QueryContext(ctx, `
		select t.id, t.number, t.kind, t.title,
			coalesce(d.body, t.body, '') as body,
			coalesce(d.raw_text, t.body, '') as raw_text,
			coalesce(d.dedupe_text, t.title || ' ' || coalesce(t.body, '')) as dedupe_text,
			cast(coalesce((
				select tks.key_text
				from thread_key_summaries tks
				join thread_revisions tr on tr.id = tks.thread_revision_id
				where tr.thread_id = t.id
					and tr.id = (
						select latest.id
						from thread_revisions latest
						where latest.thread_id = t.id
						order by `+revisionOrder+`
						limit 1
					)
					and `+summaryFresh+`
					and tks.summary_kind = 'llm_key_summary'
				order by tks.created_at desc, tks.id desc
				limit 1
			), '') as text) as key_summary,
			coalesce(tv.content_hash, '') as existing_hash
		from threads t
		left join documents d on d.thread_id = t.id
		left join thread_vectors tv on tv.thread_id = t.id and tv.basis = ?1 and tv.model = ?2
		where t.repo_id = ?3
			and (?4 != 0 or (t.state = 'open' and t.closed_at_local is null))
			and (?5 is null or t.number = ?5)
			and (
				?1 != 'llm_key_summary'
				or exists (
					select 1
					from thread_key_summaries eligible_summary
					join thread_revisions eligible_revision
						on eligible_revision.id = eligible_summary.thread_revision_id
					where eligible_revision.thread_id = t.id
						and eligible_revision.id = (
							select latest.id
							from thread_revisions latest
							where latest.thread_id = t.id
							order by `+revisionOrder+`
							limit 1
						)
						and `+eligibleSummaryFresh+`
						and eligible_summary.summary_kind = 'llm_key_summary'
				)
			)
		order by coalesce(t.updated_at_gh, t.updated_at) desc, t.number desc
		limit case when ?6 <= 0 then -1 else ?6 end
	`, basis, model, options.RepoID, boolInt(options.IncludeClosed), number, options.Limit)
	if err != nil {
		return nil, fmt.Errorf("list embedding tasks: %w", err)
	}
	defer rows.Close()

	out := make([]EmbeddingTask, 0)
	for rows.Next() {
		var row struct {
			id           int64
			number       int
			kind         string
			title        string
			body         string
			rawText      string
			dedupeText   string
			keySummary   string
			existingHash string
		}
		if err := rows.Scan(
			&row.id,
			&row.number,
			&row.kind,
			&row.title,
			&row.body,
			&row.rawText,
			&row.dedupeText,
			&row.keySummary,
			&row.existingHash,
		); err != nil {
			return nil, fmt.Errorf("scan embedding task: %w", err)
		}
		task := EmbeddingTask{
			ThreadID: row.id,
			Number:   row.number,
			Kind:     row.kind,
			Title:    row.title,
		}
		text, meta, err := embeddingTextForBasisWithMeta(
			basis,
			task.Title,
			row.body,
			row.rawText,
			row.dedupeText,
			row.keySummary,
		)
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
		if !options.Force && row.existingHash == task.ContentHash {
			continue
		}
		out = append(out, task)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate embedding tasks: %w", err)
	}
	return out, nil
}

func SupportsEmbeddingBasis(basis string) bool {
	switch strings.TrimSpace(basis) {
	case "", "title_original", "dedupe_text", "llm_key_summary":
		return true
	default:
		return false
	}
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
		if strings.TrimSpace(rawText) != "" {
			text = strings.TrimSpace(rawText)
		} else {
			parts := []string{strings.TrimSpace(title)}
			if strings.TrimSpace(body) != "" {
				parts = append(parts, strings.TrimSpace(body))
			}
			text = strings.TrimSpace(strings.Join(parts, "\n\n"))
		}
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
	text = strings.TrimSpace(text)
	runes := []rune(text)
	meta := embeddingTextMeta{OriginalRunes: len(runes), Runes: len(runes)}
	capped := capStringByRunesAndBytes(text, MaxEmbeddingTextRunes, MaxEmbeddingTextBytes)
	if capped == text {
		return text, meta
	}
	meta.Truncated = true
	meta.Runes = len([]rune(capped))
	return capped, meta
}

func capStringByRunesAndBytes(text string, maxRunes, maxBytes int) string {
	runes := 0
	bytes := 0
	for end, r := range text {
		runeBytes := len(string(r))
		if runes >= maxRunes || bytes+runeBytes > maxBytes {
			return text[:end]
		}
		runes++
		bytes += runeBytes
	}
	return text
}

func embeddingContentHash(basis, model, text string) string {
	sum := sha256.Sum256([]byte(embeddingContentHashMaterial(basis, model, text)))
	return hex.EncodeToString(sum[:])
}

func embeddingContentHashMaterial(basis, model, text string) string {
	return fmt.Sprintf("%s:max_runes=%d:max_bytes=%d:%s:%s\n%s", embeddingContentHashVersion, MaxEmbeddingTextRunes, MaxEmbeddingTextBytes, basis, model, text)
}
