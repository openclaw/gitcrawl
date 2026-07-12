package store

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"
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
		RepoID:          repoID,
		GitHubID:        "1",
		Number:          7,
		Kind:            "issue",
		State:           "open",
		Title:           "Download stalls",
		Body:            "Large download stalls near completion.",
		HTMLURL:         "https://github.com/openclaw/gitcrawl/issues/7",
		LabelsJSON:      "[]",
		AssigneesJSON:   "[]",
		RawJSON:         "{}",
		ContentHash:     "hash",
		UpdatedAtGitHub: "2026-04-26T00:00:00Z",
		UpdatedAt:       "2026-04-26T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("thread: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `
		insert into thread_revisions(id, thread_id, source_updated_at, content_hash, title_hash, body_hash, labels_hash, created_at)
		values(1, ?, '2026-04-26T00:00:00Z', 'hash', 'title', 'body', 'labels', '2026-04-26T00:00:00Z');
		insert into thread_key_summaries(thread_revision_id, summary_kind, prompt_version, provider, model, input_hash, output_hash, key_text, created_at)
		values(1, 'llm_key_summary', 'v1', 'openai', 'gpt-5-mini', 'input', 'output', 'intent: fix downloads\nsurface: downloader\nmechanism: retry stalled stream', '2026-04-26T00:01:00Z');
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

	if _, err := st.DB().ExecContext(ctx, `
		update threads
		set updated_at_gh = '2026-04-27T00:00:00Z',
			updated_at = '2026-04-27T00:00:00Z'
		where id = ?
	`, threadID); err != nil {
		t.Fatalf("advance thread without revision hydration: %v", err)
	}
	tasks, err = st.ListEmbeddingTasks(ctx, EmbeddingTaskOptions{
		RepoID: repoID,
		Basis:  "llm_key_summary",
		Model:  "text-embedding-3-large",
		Force:  true,
	})
	if err != nil {
		t.Fatalf("stale revision tasks: %v", err)
	}
	if len(tasks) != 0 {
		t.Fatalf("stale revision summary was embedded: %+v", tasks)
	}
}

func TestListEmbeddingTasksUsesLatestObservedRevision(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repoID, err := st.UpsertRepository(ctx, Repository{
		Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl", RawJSON: "{}", UpdatedAt: "2026-07-12T00:02:00Z",
	})
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	threadID, err := st.UpsertThread(ctx, Thread{
		RepoID: repoID, GitHubID: "observed", Number: 71, Kind: "issue", State: "open",
		Title: "Use observed summary", Body: "Current evidence has the lower revision id.",
		HTMLURL: "https://github.com/openclaw/gitcrawl/issues/71", LabelsJSON: "[]", AssigneesJSON: "[]",
		RawJSON: "{}", ContentHash: "current", UpdatedAtGitHub: "2026-07-12T00:02:00Z", UpdatedAt: "2026-07-12T00:02:00Z",
	})
	if err != nil {
		t.Fatalf("thread: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `
		insert into thread_revisions(id, thread_id, source_updated_at, content_hash, title_hash, body_hash, labels_hash, created_at)
		values
			(710, ?, '2026-07-12T00:02:00Z', 'current', 'title', 'body', 'labels', '2026-07-12T00:02:00Z'),
			(711, ?, '2026-07-12T00:01:00Z', 'stale', 'title', 'body', 'labels', '2026-07-12T00:03:00Z');
		insert into thread_key_summaries(thread_revision_id, summary_kind, prompt_version, provider, model, input_hash, output_hash, key_text, created_at)
		values(710, 'llm_key_summary', 'v1', 'test', 'test', 'input', 'output', 'current observed summary', '2026-07-12T00:04:00Z');
	`, threadID, threadID); err != nil {
		t.Fatalf("seed observed revisions: %v", err)
	}

	tasks, err := st.ListEmbeddingTasks(ctx, EmbeddingTaskOptions{
		RepoID: repoID,
		Basis:  "llm_key_summary",
		Model:  "text-embedding-3-large",
		Force:  true,
	})
	if err != nil {
		t.Fatalf("tasks: %v", err)
	}
	if len(tasks) != 1 || !strings.Contains(tasks[0].Text, "current observed summary") {
		t.Fatalf("observed summary tasks = %+v", tasks)
	}
}

func TestListEmbeddingTasksRejectsSummaryFromOlderRevision(t *testing.T) {
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
		UpdatedAt: "2026-07-12T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	threadID, err := st.UpsertThread(ctx, Thread{
		RepoID:        repoID,
		GitHubID:      "2",
		Number:        8,
		Kind:          "pull_request",
		State:         "open",
		Title:         "Refresh review evidence",
		Body:          "The title remains stable while review evidence changes.",
		HTMLURL:       "https://github.com/openclaw/gitcrawl/pull/8",
		LabelsJSON:    "[]",
		AssigneesJSON: "[]",
		RawJSON:       "{}",
		ContentHash:   "thread",
		UpdatedAt:     "2026-07-12T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("thread: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `
		insert into thread_revisions(id, thread_id, content_hash, title_hash, body_hash, labels_hash, created_at)
		values
			(1, ?, 'old', 'title', 'body', 'labels', '2026-07-12T00:00:00Z'),
			(2, ?, 'new', 'title', 'body', 'labels', '2026-07-12T00:02:00Z');
		insert into thread_key_summaries(thread_revision_id, summary_kind, prompt_version, provider, model, input_hash, output_hash, key_text, created_at)
		values(1, 'llm_key_summary', 'v1', 'openai', 'gpt-5-mini', 'input', 'output', 'stale review evidence', '2026-07-12T00:01:00Z');
	`, threadID, threadID); err != nil {
		t.Fatalf("seed revisions: %v", err)
	}

	tasks, err := st.ListEmbeddingTasks(ctx, EmbeddingTaskOptions{
		RepoID: repoID,
		Basis:  "llm_key_summary",
		Model:  "text-embedding-3-large",
		Force:  true,
	})
	if err != nil {
		t.Fatalf("tasks: %v", err)
	}
	if len(tasks) != 0 {
		t.Fatalf("tasks = %+v, want no task until the latest revision is summarized", tasks)
	}
}

func TestListEmbeddingTasksAppliesSummaryLimitAfterEligibility(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repoID, err := st.UpsertRepository(ctx, Repository{
		Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl", RawJSON: "{}", UpdatedAt: "2026-07-12T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	var oldestRevisionID int64
	for _, number := range []int{1, 2, 3} {
		updatedAt := fmt.Sprintf("2026-07-12T00:0%d:00Z", number)
		thread := Thread{
			RepoID: repoID, GitHubID: fmt.Sprintf("%d", number), Number: number, Kind: "issue", State: "open",
			Title: fmt.Sprintf("Issue %d", number), Body: "body",
			HTMLURL:    fmt.Sprintf("https://github.com/openclaw/gitcrawl/issues/%d", number),
			LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: fmt.Sprintf("thread-%d", number),
			UpdatedAtGitHub: updatedAt, UpdatedAt: updatedAt,
		}
		thread.ID, err = st.UpsertThread(ctx, thread)
		if err != nil {
			t.Fatalf("thread %d: %v", number, err)
		}
		enrichment, err := st.UpsertThreadRevisionAndFingerprint(ctx, ThreadEvidence{Thread: thread}, updatedAt)
		if err != nil {
			t.Fatalf("enrichment %d: %v", number, err)
		}
		if number == 1 {
			oldestRevisionID = enrichment.RevisionID
		}
	}
	if err := st.UpsertThreadKeySummary(ctx, ThreadKeySummary{
		ThreadRevisionID: oldestRevisionID,
		SummaryKind:      SummaryKindLLMKey,
		PromptVersion:    SummaryPromptVersionV1,
		Provider:         "openai",
		Model:            "summary-test",
		InputHash:        "input",
		OutputHash:       "output",
		KeyText:          "Only the oldest thread is currently eligible.",
		CreatedAt:        "2026-07-12T00:04:00Z",
	}); err != nil {
		t.Fatalf("summary: %v", err)
	}

	tasks, err := st.ListEmbeddingTasks(ctx, EmbeddingTaskOptions{
		RepoID: repoID, Basis: "llm_key_summary", Model: "embedding-test", Limit: 1, Force: true,
	})
	if err != nil {
		t.Fatalf("embedding tasks: %v", err)
	}
	if len(tasks) != 1 || tasks[0].Number != 1 {
		t.Fatalf("eligible task was starved by newer unsummarized threads: %+v", tasks)
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

func TestTitleOriginalEmbeddingPrefersCanonicalDocumentText(t *testing.T) {
	rawText := "# Refresh cache\n\nOriginal body\n\nChanged files:\n- internal/cache/store.go\n\nCommits:\n- fix: refresh manifest"
	text, err := embeddingTextForBasis("title_original", "Refresh cache", "Original body", rawText, "", "")
	if err != nil {
		t.Fatalf("embedding text: %v", err)
	}
	if text != rawText {
		t.Fatalf("embedding text = %q, want canonical document", text)
	}
}

func TestEmbeddingTextForBasisCapsTokenDenseInputsByBytes(t *testing.T) {
	body := strings.Repeat("界", MaxEmbeddingTextRunes)
	text, meta, err := embeddingTextForBasisWithMeta("title_original", "oversized unicode", body, "", "", "")
	if err != nil {
		t.Fatalf("embedding text: %v", err)
	}
	if !meta.Truncated {
		t.Fatal("token-dense embedding text should be marked truncated")
	}
	if got := len([]byte(text)); got > MaxEmbeddingTextBytes {
		t.Fatalf("truncated bytes = %d, want <= %d", got, MaxEmbeddingTextBytes)
	}
	if !utf8.ValidString(text) {
		t.Fatal("truncated text is not valid UTF-8")
	}
	if got := len([]rune(text)); got >= MaxEmbeddingTextRunes {
		t.Fatalf("truncated runes = %d, want byte cap to apply before rune cap %d", got, MaxEmbeddingTextRunes)
	}
	if meta.OriginalRunes <= meta.Runes {
		t.Fatalf("meta = %+v", meta)
	}
}

func TestEmbeddingContentHashVersionTracksCurrentInputCaps(t *testing.T) {
	if embeddingContentHash("title_original", "test", "body") == "" {
		t.Fatal("embedding content hash should be non-empty")
	}
	material := embeddingContentHashMaterial("title_original", "test", "body")
	if want := fmt.Sprintf("max_runes=%d", MaxEmbeddingTextRunes); !strings.Contains(material, want) {
		t.Fatalf("embedding hash material should include %s", want)
	}
	if want := fmt.Sprintf("max_bytes=%d", MaxEmbeddingTextBytes); !strings.Contains(material, want) {
		t.Fatalf("embedding hash material should include %s", want)
	}
	if strings.Contains(material, "max_runes=24000") {
		t.Fatal("embedding hash material still carries stale 24000 rune cap")
	}
}

func TestSupportsEmbeddingBasis(t *testing.T) {
	for _, basis := range []string{"", "title_original", "dedupe_text", "llm_key_summary"} {
		if !SupportsEmbeddingBasis(basis) {
			t.Fatalf("supported basis rejected: %q", basis)
		}
	}
	if SupportsEmbeddingBasis("missing") {
		t.Fatal("unsupported basis accepted")
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
