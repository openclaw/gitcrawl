package store

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func TestListSummaryTasksSkipsCurrentSummaryAndSupportsForce(t *testing.T) {
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
		t.Fatalf("repository: %v", err)
	}
	thread := Thread{
		RepoID:          repoID,
		GitHubID:        "1",
		Number:          1,
		Kind:            "issue",
		State:           "open",
		Title:           "Gateway reconnect loop",
		Body:            "Reconnect loops after a websocket timeout.",
		HTMLURL:         "https://github.com/openclaw/gitcrawl/issues/1",
		LabelsJSON:      "[]",
		AssigneesJSON:   "[]",
		RawJSON:         "{}",
		ContentHash:     "thread",
		UpdatedAtGitHub: "2026-07-12T00:00:00Z",
		UpdatedAt:       "2026-07-12T00:00:00Z",
	}
	threadID, err := st.UpsertThread(ctx, thread)
	if err != nil {
		t.Fatalf("thread: %v", err)
	}
	thread.ID = threadID
	if _, err := st.UpsertDocument(ctx, Document{
		ThreadID:   threadID,
		Title:      thread.Title,
		Body:       thread.Body,
		RawText:    thread.Title + "\n\n" + thread.Body,
		DedupeText: thread.Title + " " + thread.Body,
		UpdatedAt:  thread.UpdatedAt,
	}); err != nil {
		t.Fatalf("document: %v", err)
	}
	enrichment, err := st.UpsertThreadRevisionAndFingerprint(ctx, ThreadEvidence{Thread: thread}, "2026-07-12T00:00:00Z")
	if err != nil {
		t.Fatalf("enrichment: %v", err)
	}
	options := SummaryTaskOptions{
		RepoID:        repoID,
		Provider:      "openai",
		Model:         "summary-test",
		SummaryKind:   SummaryKindLLMKey,
		PromptVersion: SummaryPromptVersionV1,
	}
	tasks, err := st.ListSummaryTasks(ctx, options)
	if err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	if len(tasks) != 1 || tasks[0].RevisionID != enrichment.RevisionID || tasks[0].InputHash == "" {
		t.Fatalf("tasks = %+v", tasks)
	}
	if err := st.UpsertThreadKeySummary(ctx, ThreadKeySummary{
		ThreadRevisionID: enrichment.RevisionID,
		SummaryKind:      SummaryKindLLMKey,
		PromptVersion:    SummaryPromptVersionV1,
		Provider:         "openai",
		Model:            "summary-test",
		InputHash:        tasks[0].InputHash,
		OutputHash:       StableHash("summary"),
		KeyText:          "Gateway reconnects loop after websocket timeout.",
		CreatedAt:        "2026-07-12T00:01:00Z",
	}); err != nil {
		t.Fatalf("upsert summary: %v", err)
	}
	tasks, err = st.ListSummaryTasks(ctx, options)
	if err != nil {
		t.Fatalf("list current tasks: %v", err)
	}
	if len(tasks) != 0 {
		t.Fatalf("current summary was selected again: %+v", tasks)
	}
	olderThread := Thread{
		RepoID:          repoID,
		GitHubID:        "2",
		Number:          2,
		Kind:            "issue",
		State:           "open",
		Title:           "Older unsummarized issue",
		Body:            "This task must survive the post-filter limit.",
		HTMLURL:         "https://github.com/openclaw/gitcrawl/issues/2",
		LabelsJSON:      "[]",
		AssigneesJSON:   "[]",
		RawJSON:         "{}",
		ContentHash:     "older-thread",
		UpdatedAtGitHub: "2026-07-11T00:00:00Z",
		UpdatedAt:       "2026-07-11T00:00:00Z",
	}
	olderThread.ID, err = st.UpsertThread(ctx, olderThread)
	if err != nil {
		t.Fatalf("older thread: %v", err)
	}
	if _, err := st.UpsertDocument(ctx, Document{
		ThreadID:   olderThread.ID,
		Title:      olderThread.Title,
		Body:       olderThread.Body,
		RawText:    olderThread.Title + "\n\n" + olderThread.Body,
		DedupeText: olderThread.Title + " " + olderThread.Body,
		UpdatedAt:  olderThread.UpdatedAt,
	}); err != nil {
		t.Fatalf("older document: %v", err)
	}
	if _, err := st.UpsertThreadRevisionAndFingerprint(ctx, ThreadEvidence{Thread: olderThread}, "2026-07-11T00:00:00Z"); err != nil {
		t.Fatalf("older enrichment: %v", err)
	}
	options.Limit = 1
	tasks, err = st.ListSummaryTasks(ctx, options)
	if err != nil {
		t.Fatalf("list limited tasks: %v", err)
	}
	if len(tasks) != 1 || tasks[0].Number != olderThread.Number {
		t.Fatalf("post-filter limited tasks = %+v", tasks)
	}
	options.Force = true
	tasks, err = st.ListSummaryTasks(ctx, options)
	if err != nil {
		t.Fatalf("list forced tasks: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("forced tasks = %+v", tasks)
	}

	thread.UpdatedAtGitHub = "2026-07-13T00:00:00Z"
	thread.UpdatedAt = "2026-07-13T00:00:00Z"
	if _, err := st.UpsertThread(ctx, thread); err != nil {
		t.Fatalf("update thread without hydration: %v", err)
	}
	options.Number = thread.Number
	tasks, err = st.ListSummaryTasks(ctx, options)
	if err != nil {
		t.Fatalf("list stale revision tasks: %v", err)
	}
	if len(tasks) != 0 {
		t.Fatalf("stale revision paired with current document: %+v", tasks)
	}
}

func TestSummaryTasksValidateAndBoundStoredEvidence(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if _, err := st.ListSummaryTasks(ctx, SummaryTaskOptions{}); err == nil || !strings.Contains(err.Error(), "provider") {
		t.Fatalf("missing summary options error = %v", err)
	}
	if err := st.UpsertThreadKeySummary(ctx, ThreadKeySummary{}); err == nil || !strings.Contains(err.Error(), "positive") {
		t.Fatalf("missing revision error = %v", err)
	}
	if err := st.UpsertThreadKeySummary(ctx, ThreadKeySummary{ThreadRevisionID: 1}); err == nil || !strings.Contains(err.Error(), "required") {
		t.Fatalf("missing summary fields error = %v", err)
	}

	repoID, err := st.UpsertRepository(ctx, Repository{
		Owner:     "openclaw",
		Name:      "gitcrawl",
		FullName:  "openclaw/gitcrawl",
		RawJSON:   "{}",
		UpdatedAt: "2026-07-12T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("repository: %v", err)
	}
	thread := Thread{
		RepoID:          repoID,
		GitHubID:        "9",
		Number:          9,
		Kind:            "issue",
		State:           "closed",
		Title:           "Bound summary evidence",
		Body:            "fallback",
		HTMLURL:         "https://github.com/openclaw/gitcrawl/issues/9",
		LabelsJSON:      "[]",
		AssigneesJSON:   "[]",
		RawJSON:         "{}",
		ContentHash:     "thread",
		UpdatedAtGitHub: "2026-07-12T00:00:00Z",
		UpdatedAt:       "2026-07-12T00:00:00Z",
	}
	threadID, err := st.UpsertThread(ctx, thread)
	if err != nil {
		t.Fatalf("thread: %v", err)
	}
	thread.ID = threadID
	if _, err := st.UpsertDocument(ctx, Document{
		ThreadID:   threadID,
		Title:      thread.Title,
		Body:       thread.Body,
		RawText:    strings.Repeat("evidence ", MaxSummaryTextRunes),
		DedupeText: thread.Title,
		UpdatedAt:  thread.UpdatedAt,
	}); err != nil {
		t.Fatalf("document: %v", err)
	}
	enrichment, err := st.UpsertThreadRevisionAndFingerprint(ctx, ThreadEvidence{Thread: thread}, "2026-07-12T00:00:00Z")
	if err != nil {
		t.Fatalf("enrichment: %v", err)
	}
	options := SummaryTaskOptions{
		RepoID:        repoID,
		Provider:      "openai",
		Model:         "summary-test",
		SummaryKind:   SummaryKindLLMKey,
		PromptVersion: SummaryPromptVersionV1,
		Number:        9,
		Limit:         1,
		IncludeClosed: true,
	}
	tasks, err := st.ListSummaryTasks(ctx, options)
	if err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	if len(tasks) != 1 || !tasks[0].TextTruncated || len([]rune(tasks[0].Text)) > MaxSummaryTextRunes {
		t.Fatalf("bounded task = %+v", tasks)
	}
	summary := ThreadKeySummary{
		ThreadRevisionID: enrichment.RevisionID,
		SummaryKind:      SummaryKindLLMKey,
		PromptVersion:    SummaryPromptVersionV1,
		Provider:         "openai",
		Model:            "summary-test",
		InputHash:        tasks[0].InputHash,
		OutputHash:       StableHash("first"),
		KeyText:          " First summary. ",
	}
	if err := st.UpsertThreadKeySummary(ctx, summary); err != nil {
		t.Fatalf("insert summary: %v", err)
	}
	summary.OutputHash = StableHash("second")
	summary.KeyText = "Second summary."
	if err := st.UpsertThreadKeySummary(ctx, summary); err != nil {
		t.Fatalf("update summary: %v", err)
	}
	var keyText, createdAt string
	if err := st.DB().QueryRowContext(ctx, `
		select key_text, created_at
		from thread_key_summaries
		where thread_revision_id = ?
	`, enrichment.RevisionID).Scan(&keyText, &createdAt); err != nil {
		t.Fatalf("read summary: %v", err)
	}
	if keyText != "Second summary." || strings.TrimSpace(createdAt) == "" {
		t.Fatalf("stored summary = %q at %q", keyText, createdAt)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	if _, err := st.ListSummaryTasks(ctx, options); err == nil || !strings.Contains(err.Error(), "list summary tasks") {
		t.Fatalf("closed list error = %v", err)
	}
	if err := st.UpsertThreadKeySummary(ctx, summary); err == nil || !strings.Contains(err.Error(), "upsert thread key summary") {
		t.Fatalf("closed upsert error = %v", err)
	}
}
