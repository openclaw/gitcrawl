package store

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func TestClocklessRevisionConsumersUseObservationSequence(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repoID, err := st.UpsertRepository(ctx, Repository{
		Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl",
		RawJSON: "{}", UpdatedAt: "2026-07-12T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("repository: %v", err)
	}
	thread := Thread{
		RepoID: repoID, GitHubID: "clockless", Number: 88, Kind: "issue", State: "open",
		Title: "Clockless revision", Body: "Use durable observation order.",
		HTMLURL:    "https://github.com/openclaw/gitcrawl/issues/88",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "thread",
		UpdatedAt: "2026-07-12T00:00:00Z",
	}
	thread.ID, err = st.UpsertThread(ctx, thread)
	if err != nil {
		t.Fatalf("thread: %v", err)
	}
	var observationSequence int64
	if err := st.DB().QueryRowContext(
		ctx,
		`select observation_sequence from threads where id = ?`,
		thread.ID,
	).Scan(&observationSequence); err != nil {
		t.Fatalf("thread observation sequence: %v", err)
	}
	enrichment, err := st.UpsertThreadRevisionAndFingerprint(ctx, ThreadEvidence{
		Thread:              thread,
		ObservationSequence: observationSequence,
	}, "2026-07-12T00:01:00Z")
	if err != nil {
		t.Fatalf("revision: %v", err)
	}
	if err := st.UpsertThreadKeySummary(ctx, ThreadKeySummary{
		ThreadRevisionID: enrichment.RevisionID,
		SummaryKind:      SummaryKindLLMKey,
		PromptVersion:    SummaryPromptVersionV1,
		Provider:         "test",
		Model:            "test",
		InputHash:        "input",
		OutputHash:       "output",
		KeyText:          "clockless current summary",
		CreatedAt:        "2026-07-12T00:02:00Z",
	}); err != nil {
		t.Fatalf("summary: %v", err)
	}

	summaryTasks, err := st.ListSummaryTasks(ctx, SummaryTaskOptions{
		RepoID: repoID, Provider: "test", Model: "test",
		SummaryKind: SummaryKindLLMKey, PromptVersion: SummaryPromptVersionV1,
		Number: thread.Number, Force: true,
	})
	if err != nil {
		t.Fatalf("summary tasks: %v", err)
	}
	if len(summaryTasks) != 1 || summaryTasks[0].RevisionID != enrichment.RevisionID {
		t.Fatalf("clockless summary tasks = %+v", summaryTasks)
	}
	embeddingTasks, err := st.ListEmbeddingTasks(ctx, EmbeddingTaskOptions{
		RepoID: repoID, Basis: "llm_key_summary", Model: "test",
		Number: thread.Number, Force: true,
	})
	if err != nil {
		t.Fatalf("embedding tasks: %v", err)
	}
	if len(embeddingTasks) != 1 ||
		!strings.Contains(embeddingTasks[0].Text, "clockless current summary") {
		t.Fatalf("clockless embedding tasks = %+v", embeddingTasks)
	}
	summaries, err := st.summariesByThreadIDs(ctx, []int64{thread.ID})
	if err != nil {
		t.Fatalf("cluster summaries: %v", err)
	}
	if summaries[thread.ID][SummaryKindLLMKey] != "clockless current summary" {
		t.Fatalf("clockless cluster summaries = %+v", summaries)
	}

	thread.Title = "Clockless thread advanced"
	thread.UpdatedAt = "2026-07-12T00:03:00Z"
	if _, err := st.UpsertThread(ctx, thread); err != nil {
		t.Fatalf("advance thread without revision: %v", err)
	}
	summaryTasks, err = st.ListSummaryTasks(ctx, SummaryTaskOptions{
		RepoID: repoID, Provider: "test", Model: "test",
		SummaryKind: SummaryKindLLMKey, PromptVersion: SummaryPromptVersionV1,
		Number: thread.Number, Force: true,
	})
	if err != nil {
		t.Fatalf("stale summary tasks: %v", err)
	}
	if len(summaryTasks) != 0 {
		t.Fatalf("stale clockless summary tasks = %+v", summaryTasks)
	}
	embeddingTasks, err = st.ListEmbeddingTasks(ctx, EmbeddingTaskOptions{
		RepoID: repoID, Basis: "llm_key_summary", Model: "test",
		Number: thread.Number, Force: true,
	})
	if err != nil {
		t.Fatalf("stale embedding tasks: %v", err)
	}
	if len(embeddingTasks) != 0 {
		t.Fatalf("stale clockless embedding tasks = %+v", embeddingTasks)
	}
	summaries, err = st.summariesByThreadIDs(ctx, []int64{thread.ID})
	if err != nil {
		t.Fatalf("stale cluster summaries: %v", err)
	}
	if summaries[thread.ID][SummaryKindLLMKey] != "" {
		t.Fatalf("stale clockless cluster summaries = %+v", summaries)
	}
}

func TestRevisionConsumersRejectMalformedClocksAtCurrentSequence(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repoID, err := st.UpsertRepository(ctx, Repository{
		Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl",
		RawJSON: "{}", UpdatedAt: "2026-07-12T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("repository: %v", err)
	}
	thread := Thread{
		RepoID: repoID, GitHubID: "malformed", Number: 89, Kind: "issue", State: "open",
		Title: "Malformed clock", Body: "Malformed clocks must fail closed.",
		HTMLURL:         "https://github.com/openclaw/gitcrawl/issues/89",
		LabelsJSON:      "[]",
		AssigneesJSON:   "[]",
		RawJSON:         "{}",
		ContentHash:     "thread",
		UpdatedAtGitHub: "2026-07-12T00:00:00Z",
		UpdatedAt:       "2026-07-12T00:00:00Z",
	}
	thread.ID, err = st.UpsertThread(ctx, thread)
	if err != nil {
		t.Fatalf("thread: %v", err)
	}
	var observationSequence int64
	if err := st.DB().QueryRowContext(
		ctx,
		`select observation_sequence from threads where id = ?`,
		thread.ID,
	).Scan(&observationSequence); err != nil {
		t.Fatalf("thread observation sequence: %v", err)
	}
	enrichment, err := st.UpsertThreadRevisionAndFingerprint(ctx, ThreadEvidence{
		Thread:              thread,
		ObservationSequence: observationSequence,
	}, "2026-07-12T00:01:00Z")
	if err != nil {
		t.Fatalf("revision: %v", err)
	}
	if err := st.UpsertThreadKeySummary(ctx, ThreadKeySummary{
		ThreadRevisionID: enrichment.RevisionID,
		SummaryKind:      SummaryKindLLMKey,
		PromptVersion:    SummaryPromptVersionV1,
		Provider:         "test",
		Model:            "test",
		InputHash:        "input",
		OutputHash:       "output",
		KeyText:          "must not surface",
		CreatedAt:        "2026-07-12T00:02:00Z",
	}); err != nil {
		t.Fatalf("summary: %v", err)
	}

	assertConsumersStale := func(label string) {
		t.Helper()
		summaryTasks, err := st.ListSummaryTasks(ctx, SummaryTaskOptions{
			RepoID: repoID, Provider: "test", Model: "test",
			SummaryKind: SummaryKindLLMKey, PromptVersion: SummaryPromptVersionV1,
			Number: thread.Number, Force: true,
		})
		if err != nil {
			t.Fatalf("%s summary tasks: %v", label, err)
		}
		if len(summaryTasks) != 0 {
			t.Fatalf("%s summary tasks = %+v", label, summaryTasks)
		}
		embeddingTasks, err := st.ListEmbeddingTasks(ctx, EmbeddingTaskOptions{
			RepoID: repoID, Basis: "llm_key_summary", Model: "test",
			Number: thread.Number, Force: true,
		})
		if err != nil {
			t.Fatalf("%s embedding tasks: %v", label, err)
		}
		if len(embeddingTasks) != 0 {
			t.Fatalf("%s embedding tasks = %+v", label, embeddingTasks)
		}
		summaries, err := st.summariesByThreadIDs(ctx, []int64{thread.ID})
		if err != nil {
			t.Fatalf("%s cluster summaries: %v", label, err)
		}
		if summaries[thread.ID][SummaryKindLLMKey] != "" {
			t.Fatalf("%s cluster summaries = %+v", label, summaries)
		}
		coverage, err := st.ArchiveCoverage(ctx, ArchiveCoverageOptions{})
		if err != nil {
			t.Fatalf("%s coverage: %v", label, err)
		}
		for name, metric := range map[string]EnrichmentCoverageMetric{
			"revisions":    coverage.Rows[0].Enrichment.Revisions,
			"fingerprints": coverage.Rows[0].Enrichment.Fingerprints,
			"summaries":    coverage.Rows[0].Enrichment.Summaries,
		} {
			if metric.Fresh != 0 {
				t.Fatalf("%s %s coverage = %+v", label, name, metric)
			}
		}
	}

	if _, err := st.DB().ExecContext(
		ctx,
		`update thread_revisions set source_updated_at = 'malformed' where id = ?`,
		enrichment.RevisionID,
	); err != nil {
		t.Fatalf("malform revision clock: %v", err)
	}
	assertConsumersStale("malformed revision clock")

	if _, err := st.DB().ExecContext(
		ctx,
		`update thread_revisions set source_updated_at = ?, observation_sequence = ? where id = ?`,
		thread.UpdatedAtGitHub,
		observationSequence,
		enrichment.RevisionID,
	); err != nil {
		t.Fatalf("restore revision clock: %v", err)
	}
	if _, err := st.DB().ExecContext(
		ctx,
		`update threads set updated_at_gh = 'malformed' where id = ?`,
		thread.ID,
	); err != nil {
		t.Fatalf("malform thread clock: %v", err)
	}
	assertConsumersStale("malformed thread clock")
}
