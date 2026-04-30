package store

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestEmbeddingTextForBasisTruncatesOversizedBody(t *testing.T) {
	title := "Big issue"
	body := strings.Repeat("a", 100_000)

	for _, basis := range []string{"title_original", "dedupe_text", "llm_key_summary"} {
		text, err := embeddingTextForBasis(basis, title, body, body, body, body)
		if err != nil {
			t.Fatalf("%s: embedding text: %v", basis, err)
		}
		if got := utf8.RuneCountInString(text); got > 30_000 {
			t.Fatalf("%s: embedding text not truncated: got %d runes, want <= 30000", basis, got)
		}
	}
}

func TestListEmbeddingTasksUsesLatestLLMKeySummary(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repoID, err := st.UpsertRepository(ctx, Repository{
		Owner:     "openclaw",
		Name:      "gitcrawl",
		FullName:  "openclaw/gitcrawl",
		RawJSON:   "{}",
		UpdatedAt: "2026-04-26T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	threadID, err := st.UpsertThread(ctx, Thread{
		RepoID:        repoID,
		GitHubID:      "1",
		Number:        7,
		Kind:          "issue",
		State:         "open",
		Title:         "Download stalls",
		Body:          "Large download stalls near completion.",
		HTMLURL:       "https://github.com/openclaw/gitcrawl/issues/7",
		LabelsJSON:    "[]",
		AssigneesJSON: "[]",
		RawJSON:       "{}",
		ContentHash:   "hash",
		UpdatedAt:     "2026-04-26T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("thread: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `
		insert into thread_revisions(id, thread_id, content_hash, title_hash, body_hash, labels_hash, created_at)
		values(1, ?, 'hash', 'title', 'body', 'labels', '2026-04-26T00:00:00Z');
		insert into thread_key_summaries(thread_revision_id, summary_kind, prompt_version, provider, model, input_hash, output_hash, key_text, created_at)
		values(1, 'llm_key_3line', 'v1', 'openai', 'gpt-5-mini', 'input', 'output', 'intent: fix downloads\nsurface: downloader\nmechanism: retry stalled stream', '2026-04-26T00:01:00Z');
	`, threadID); err != nil {
		t.Fatalf("seed summary: %v", err)
	}

	tasks, err := st.ListEmbeddingTasks(ctx, EmbeddingTaskOptions{
		RepoID: repoID,
		Basis:  "llm_key_summary",
		Model:  "text-embedding-3-large",
	})
	if err != nil {
		t.Fatalf("tasks: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("tasks = %d, want 1", len(tasks))
	}
	if !strings.Contains(tasks[0].Text, "title: Download stalls") || !strings.Contains(tasks[0].Text, "key_summary:") {
		t.Fatalf("unexpected embedding text: %q", tasks[0].Text)
	}
}
