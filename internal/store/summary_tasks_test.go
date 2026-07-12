package store

import (
	"context"
	"path/filepath"
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
	options.Force = true
	tasks, err = st.ListSummaryTasks(ctx, options)
	if err != nil {
		t.Fatalf("list forced tasks: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("forced tasks = %+v", tasks)
	}
}
