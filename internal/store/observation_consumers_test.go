package store

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func TestRevisionFreshnessKeepsLegacyRowsScoped(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	type seededThread struct {
		repoID     int64
		threadID   int64
		revisionID int64
		number     int
		summary    string
	}
	seed := func(owner, name string, number int, summary, createdAt string) seededThread {
		t.Helper()
		repoID, err := st.UpsertRepository(ctx, Repository{
			Owner: owner, Name: name, FullName: owner + "/" + name,
			RawJSON: "{}", UpdatedAt: "2026-07-12T00:00:00Z",
		})
		if err != nil {
			t.Fatalf("repository %s/%s: %v", owner, name, err)
		}
		thread := Thread{
			RepoID: repoID, GitHubID: name, Number: number, Kind: "issue", State: "open",
			Title: summary, Body: summary,
			HTMLURL:       "https://github.com/" + owner + "/" + name + "/issues/1",
			LabelsJSON:    "[]",
			AssigneesJSON: "[]",
			RawJSON:       "{}",
			ContentHash:   name + "-thread",
			UpdatedAt:     "2026-07-12T00:00:00Z",
		}
		thread.ID, err = st.UpsertThread(ctx, thread)
		if err != nil {
			t.Fatalf("thread %s/%s: %v", owner, name, err)
		}
		var sequence int64
		if err := st.DB().QueryRowContext(
			ctx,
			`select observation_sequence from threads where id = ?`,
			thread.ID,
		).Scan(&sequence); err != nil {
			t.Fatalf("thread sequence %s/%s: %v", owner, name, err)
		}
		enrichment, err := st.UpsertThreadRevisionAndFingerprint(ctx, ThreadEvidence{
			Thread: thread, ObservationSequence: sequence,
		}, createdAt)
		if err != nil {
			t.Fatalf("revision %s/%s: %v", owner, name, err)
		}
		if err := st.UpsertThreadKeySummary(ctx, ThreadKeySummary{
			ThreadRevisionID: enrichment.RevisionID,
			SummaryKind:      SummaryKindLLMKey,
			PromptVersion:    SummaryPromptVersionV1,
			Provider:         "test",
			Model:            "test",
			InputHash:        name + "-input",
			OutputHash:       name + "-output",
			KeyText:          summary,
			CreatedAt:        createdAt,
		}); err != nil {
			t.Fatalf("summary %s/%s: %v", owner, name, err)
		}
		if _, err := st.DB().ExecContext(ctx, `
			update threads
			set observation_sequence = 0, evidence_observation_sequence = 0
			where id = ?;
			update thread_revisions
			set observation_sequence = 0
			where id = ?
		`, thread.ID, enrichment.RevisionID); err != nil {
			t.Fatalf("legacy sequences %s/%s: %v", owner, name, err)
		}
		return seededThread{
			repoID: repoID, threadID: thread.ID, revisionID: enrichment.RevisionID,
			number: number, summary: summary,
		}
	}

	requested := seed("openclaw", "requested", 41, "requested summary", "2026-07-12T00:01:00Z")
	unrelated := seed("openclaw", "unrelated", 42, "unrelated summary", "2026-07-12T00:02:00Z")

	summaryTasks, err := st.ListSummaryTasks(ctx, SummaryTaskOptions{
		RepoID: requested.repoID, Provider: "test", Model: "test",
		SummaryKind: SummaryKindLLMKey, PromptVersion: SummaryPromptVersionV1,
		Number: requested.number, Force: true,
	})
	if err != nil {
		t.Fatalf("summary tasks: %v", err)
	}
	if len(summaryTasks) != 1 || summaryTasks[0].ThreadID != requested.threadID {
		t.Fatalf("scoped summary tasks = %+v", summaryTasks)
	}

	embeddingTasks, err := st.ListEmbeddingTasks(ctx, EmbeddingTaskOptions{
		RepoID: requested.repoID, Basis: "llm_key_summary", Model: "test",
		Number: requested.number, Force: true,
	})
	if err != nil {
		t.Fatalf("embedding tasks: %v", err)
	}
	if len(embeddingTasks) != 1 ||
		embeddingTasks[0].ThreadID != requested.threadID ||
		!strings.Contains(embeddingTasks[0].Text, requested.summary) ||
		strings.Contains(embeddingTasks[0].Text, unrelated.summary) {
		t.Fatalf("scoped embedding tasks = %+v", embeddingTasks)
	}

	summaries, err := st.summariesByThreadIDs(ctx, []int64{requested.threadID})
	if err != nil {
		t.Fatalf("cluster summaries: %v", err)
	}
	if summaries[requested.threadID][SummaryKindLLMKey] != requested.summary {
		t.Fatalf("requested cluster summaries = %+v", summaries)
	}
	if _, ok := summaries[unrelated.threadID]; ok {
		t.Fatalf("unrelated cluster summary escaped scope: %+v", summaries)
	}
}

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

func TestRevisionConsumersRequireAcceptedFreshSourceAfterNewerFetch(t *testing.T) {
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
	staleThread := Thread{
		RepoID: repoID, GitHubID: "fresh-source", Number: 90, Kind: "issue", State: "open",
		Title: "Stale source", Body: "Fetched second from the older source revision.",
		HTMLURL:         "https://github.com/openclaw/gitcrawl/issues/90",
		LabelsJSON:      "[]",
		AssigneesJSON:   "[]",
		RawJSON:         `{"revision":"stale"}`,
		ContentHash:     "stale-thread",
		UpdatedAtGitHub: "2026-07-12T00:00:00Z",
		UpdatedAt:       "2026-07-12T00:02:00Z",
	}
	staleUpsert, err := st.UpsertThreadObservation(
		ctx,
		staleThread,
		UpsertThreadOptions{ObservationSequence: 2},
	)
	if err != nil {
		t.Fatalf("stale thread observation: %v", err)
	}
	staleThread.ID = staleUpsert.ID
	staleRevision, err := st.UpsertThreadRevisionAndFingerprint(ctx, ThreadEvidence{
		Thread:              staleThread,
		ObservationSequence: 2,
	}, "2026-07-12T00:02:00Z")
	if err != nil {
		t.Fatalf("stale revision: %v", err)
	}
	if err := st.UpsertThreadKeySummary(ctx, ThreadKeySummary{
		ThreadRevisionID: staleRevision.RevisionID,
		SummaryKind:      SummaryKindLLMKey,
		PromptVersion:    SummaryPromptVersionV1,
		Provider:         "test",
		Model:            "test",
		InputHash:        "stale-input",
		OutputHash:       "stale-output",
		KeyText:          "stale source summary",
		CreatedAt:        "2026-07-12T00:02:01Z",
	}); err != nil {
		t.Fatalf("stale summary: %v", err)
	}

	freshThread := staleThread
	freshThread.Title = "Fresh source"
	freshThread.Body = "Fetched first from the newer source revision."
	freshThread.RawJSON = `{"revision":"fresh"}`
	freshThread.ContentHash = "fresh-thread"
	freshThread.UpdatedAtGitHub = "2026-07-12T00:01:00Z"
	freshThread.UpdatedAt = "2026-07-12T00:03:00Z"
	freshUpsert, err := st.UpsertThreadObservation(
		ctx,
		freshThread,
		UpsertThreadOptions{ObservationSequence: 1},
	)
	if err != nil {
		t.Fatalf("fresh thread observation: %v", err)
	}
	if !freshUpsert.Applied {
		t.Fatal("newer source observation was not applied")
	}
	if !freshUpsert.EvidenceApplied ||
		freshUpsert.EvidenceObservationSequence != 1 {
		t.Fatalf("newer source evidence was not accepted: %+v", freshUpsert)
	}
	freshThread.ID = freshUpsert.ID
	initialFreshRevision, err := st.UpsertThreadRevisionAndFingerprint(ctx, ThreadEvidence{
		Thread:              freshThread,
		ObservationSequence: 1,
	}, "2026-07-12T00:03:00Z")
	if err != nil {
		t.Fatalf("fresh revision: %v", err)
	}
	if err := st.UpsertThreadKeySummary(ctx, ThreadKeySummary{
		ThreadRevisionID: initialFreshRevision.RevisionID,
		SummaryKind:      SummaryKindLLMKey,
		PromptVersion:    SummaryPromptVersionV1,
		Provider:         "test",
		Model:            "test",
		InputHash:        "fresh-input",
		OutputHash:       "fresh-output",
		KeyText:          "fresh source summary",
		CreatedAt:        "2026-07-12T00:03:01Z",
	}); err != nil {
		t.Fatalf("fresh summary: %v", err)
	}

	summaryTasks, err := st.ListSummaryTasks(ctx, SummaryTaskOptions{
		RepoID: repoID, Provider: "test", Model: "test",
		SummaryKind: SummaryKindLLMKey, PromptVersion: SummaryPromptVersionV1,
		Number: freshThread.Number, Force: true,
	})
	if err != nil {
		t.Fatalf("summary tasks: %v", err)
	}
	if len(summaryTasks) != 1 ||
		summaryTasks[0].RevisionID != initialFreshRevision.RevisionID {
		t.Fatalf(
			"newer-source summary tasks = %+v, want revision %d",
			summaryTasks,
			initialFreshRevision.RevisionID,
		)
	}

	acceptedUpsert, err := st.UpsertThreadObservation(
		ctx,
		freshThread,
		UpsertThreadOptions{ObservationSequence: 3},
	)
	if err != nil {
		t.Fatalf("accepted fresh thread observation: %v", err)
	}
	if !acceptedUpsert.Applied || !acceptedUpsert.EvidenceApplied ||
		acceptedUpsert.EvidenceObservationSequence != 3 {
		t.Fatalf("accepted fresh thread observation = %+v", acceptedUpsert)
	}
	freshRevision, err := st.UpsertThreadRevisionAndFingerprint(ctx, ThreadEvidence{
		Thread:              freshThread,
		ObservationSequence: 3,
	}, "2026-07-12T00:04:00Z")
	if err != nil {
		t.Fatalf("accepted fresh revision: %v", err)
	}
	if err := st.UpsertThreadKeySummary(ctx, ThreadKeySummary{
		ThreadRevisionID: freshRevision.RevisionID,
		SummaryKind:      SummaryKindLLMKey,
		PromptVersion:    SummaryPromptVersionV1,
		Provider:         "test",
		Model:            "test",
		InputHash:        "accepted-fresh-input",
		OutputHash:       "accepted-fresh-output",
		KeyText:          "accepted fresh source summary",
		CreatedAt:        "2026-07-12T00:04:01Z",
	}); err != nil {
		t.Fatalf("accepted fresh summary: %v", err)
	}
	summaryTasks, err = st.ListSummaryTasks(ctx, SummaryTaskOptions{
		RepoID: repoID, Provider: "test", Model: "test",
		SummaryKind: SummaryKindLLMKey, PromptVersion: SummaryPromptVersionV1,
		Number: freshThread.Number, Force: true,
	})
	if err != nil {
		t.Fatalf("accepted summary tasks: %v", err)
	}
	if len(summaryTasks) != 1 || summaryTasks[0].RevisionID != freshRevision.RevisionID {
		t.Fatalf("summary tasks = %+v, want fresh revision %d", summaryTasks, freshRevision.RevisionID)
	}
	embeddingTasks, err := st.ListEmbeddingTasks(ctx, EmbeddingTaskOptions{
		RepoID: repoID, Basis: "llm_key_summary", Model: "test",
		Number: freshThread.Number, Force: true,
	})
	if err != nil {
		t.Fatalf("embedding tasks: %v", err)
	}
	if len(embeddingTasks) != 1 ||
		!strings.Contains(embeddingTasks[0].Text, "accepted fresh source summary") ||
		strings.Contains(embeddingTasks[0].Text, "stale source summary") {
		t.Fatalf("embedding tasks = %+v, want fresh summary", embeddingTasks)
	}
	summaries, err := st.summariesByThreadIDs(ctx, []int64{freshThread.ID})
	if err != nil {
		t.Fatalf("cluster summaries: %v", err)
	}
	if summaries[freshThread.ID][SummaryKindLLMKey] != "accepted fresh source summary" {
		t.Fatalf("cluster summaries = %+v", summaries)
	}
	coverage, err := st.ArchiveCoverage(ctx, ArchiveCoverageOptions{})
	if err != nil {
		t.Fatalf("archive coverage: %v", err)
	}
	for name, metric := range map[string]EnrichmentCoverageMetric{
		"revisions":    coverage.Rows[0].Enrichment.Revisions,
		"fingerprints": coverage.Rows[0].Enrichment.Fingerprints,
		"summaries":    coverage.Rows[0].Enrichment.Summaries,
	} {
		if metric.Fresh != 1 {
			t.Fatalf("%s coverage = %+v, want fresh source revision", name, metric)
		}
	}
}
