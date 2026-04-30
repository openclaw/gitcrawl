package store

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

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

func TestEmbeddingTextForBasisCapsLongInputs(t *testing.T) {
	body := strings.Repeat("x", MaxEmbeddingTextRunes+500)
	text, meta, err := embeddingTextForBasisWithMeta("title_original", "oversized issue", body, "", "", "")
	if err != nil {
		t.Fatalf("embedding text: %v", err)
	}
	if !meta.Truncated {
		t.Fatal("long embedding text should be marked truncated")
	}
	if got := len([]rune(text)); got != MaxEmbeddingTextRunes {
		t.Fatalf("truncated runes = %d, want %d", got, MaxEmbeddingTextRunes)
	}
	if meta.OriginalRunes <= MaxEmbeddingTextRunes || meta.Runes != MaxEmbeddingTextRunes {
		t.Fatalf("meta = %+v", meta)
	}
	if !strings.HasPrefix(text, "oversized issue\n\n") {
		t.Fatalf("truncated text lost title prefix: %q", text[:40])
	}
}

func TestListEmbeddingTasksIncludeClosed(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repoID, err := st.UpsertRepository(ctx, Repository{
		Owner:     "openclaw",
		Name:      "clawhub",
		FullName:  "openclaw/clawhub",
		RawJSON:   "{}",
		UpdatedAt: "2026-04-30T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	for _, thread := range []Thread{
		{RepoID: repoID, GitHubID: "11", Number: 11, Kind: "issue", State: "open", Title: "Open skill bug", Body: "still broken", HTMLURL: "https://github.com/openclaw/clawhub/issues/11", LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "h11", UpdatedAt: "2026-04-30T00:00:00Z"},
		{RepoID: repoID, GitHubID: "12", Number: 12, Kind: "issue", State: "closed", Title: "Closed skill bug", Body: "fixed already", HTMLURL: "https://github.com/openclaw/clawhub/issues/12", LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "h12", UpdatedAt: "2026-04-30T00:00:00Z"},
	} {
		if _, err := st.UpsertThread(ctx, thread); err != nil {
			t.Fatalf("thread %d: %v", thread.Number, err)
		}
	}

	tasks, err := st.ListEmbeddingTasks(ctx, EmbeddingTaskOptions{RepoID: repoID, Basis: "title_original", Model: "test"})
	if err != nil {
		t.Fatalf("open tasks: %v", err)
	}
	if len(tasks) != 1 || tasks[0].Number != 11 {
		t.Fatalf("open tasks = %+v, want only #11", tasks)
	}
	tasks, err = st.ListEmbeddingTasks(ctx, EmbeddingTaskOptions{RepoID: repoID, Basis: "title_original", Model: "test", IncludeClosed: true})
	if err != nil {
		t.Fatalf("all tasks: %v", err)
	}
	if len(tasks) != 2 {
		t.Fatalf("include-closed tasks = %+v, want 2", tasks)
	}
}
