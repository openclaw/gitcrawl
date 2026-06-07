package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/openclaw/gitcrawl/internal/store/storedb"
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
	rows, err := s.qsql().ListEmbeddingTasks(ctx, storedb.ListEmbeddingTasksParams{
		Basis:         basis,
		Model:         model,
		RepoID:        options.RepoID,
		IncludeClosed: boolInt(options.IncludeClosed),
		Number:        number,
		RowLimit:      options.Limit,
	})
	if err != nil {
		return nil, fmt.Errorf("list embedding tasks: %w", err)
	}

	out := make([]EmbeddingTask, 0)
	for _, row := range rows {
		task := EmbeddingTask{
			ThreadID: row.ID,
			Number:   int(row.Number),
			Kind:     row.Kind,
			Title:    row.Title,
		}
		text, meta, err := embeddingTextForBasisWithMeta(basis, task.Title, row.Body, row.RawText, row.DedupeText, row.KeySummary)
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
		if !options.Force && row.ExistingHash == task.ContentHash {
			continue
		}
		out = append(out, task)
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
