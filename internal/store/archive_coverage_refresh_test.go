package store

import (
	"context"
	"path/filepath"
	"testing"
)

func TestArchiveCoverageFreshnessUsesSuccessfulRefreshRuns(t *testing.T) {
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
		Title:           "Stable evidence",
		Body:            "The evidence does not change during the quiet period.",
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
	enrichment, err := st.UpsertThreadRevisionAndFingerprint(ctx, ThreadEvidence{Thread: thread}, "2026-07-12T00:00:00Z")
	if err != nil {
		t.Fatalf("initial enrichment: %v", err)
	}
	if err := st.UpsertThreadKeySummary(ctx, ThreadKeySummary{
		ThreadRevisionID: enrichment.RevisionID,
		SummaryKind:      SummaryKindLLMKey,
		PromptVersion:    SummaryPromptVersionV1,
		Provider:         "openai",
		Model:            "summary-test",
		InputHash:        "input",
		OutputHash:       "output",
		KeyText:          "Stable evidence remains current.",
		CreatedAt:        "2026-07-12T00:01:00Z",
	}); err != nil {
		t.Fatalf("summary: %v", err)
	}

	thread.LastPulledAt = "2026-07-13T00:00:00Z"
	thread.UpdatedAt = "2026-07-13T00:00:00Z"
	if _, err := st.UpsertThread(ctx, thread); err != nil {
		t.Fatalf("refresh thread: %v", err)
	}
	refreshed, err := st.UpsertThreadRevisionAndFingerprint(ctx, ThreadEvidence{Thread: thread}, "2026-07-13T00:00:00Z")
	if err != nil {
		t.Fatalf("refresh enrichment: %v", err)
	}
	if refreshed.RevisionCreated || refreshed.RevisionID != enrichment.RevisionID {
		t.Fatalf("unchanged evidence created a revision: initial=%+v refreshed=%+v", enrichment, refreshed)
	}
	if _, err := st.RecordRun(ctx, RunRecord{
		RepoID:     repoID,
		Kind:       "sync",
		Scope:      "open",
		Status:     "success",
		StartedAt:  "2026-07-13T00:00:00Z",
		FinishedAt: "2026-07-13T00:01:00Z",
		StatsJSON:  `{"metadata_only":false,"evidence_observed":1}`,
	}); err != nil {
		t.Fatalf("sync run: %v", err)
	}
	if _, err := st.RecordRun(ctx, RunRecord{
		RepoID:     repoID,
		Kind:       "sync",
		Scope:      "open",
		Status:     "success",
		StartedAt:  "2026-07-13T00:02:00Z",
		FinishedAt: "2026-07-13T00:03:00Z",
		StatsJSON:  `{"metadata_only":false,"evidence_observed":0}`,
	}); err != nil {
		t.Fatalf("empty hydration sync run: %v", err)
	}
	if _, err := st.RecordRun(ctx, RunRecord{
		RepoID:     repoID,
		Kind:       "summary",
		Scope:      "repo",
		Status:     "success",
		StartedAt:  "2026-07-13T00:01:00Z",
		FinishedAt: "2026-07-13T00:02:00Z",
	}); err != nil {
		t.Fatalf("summary run: %v", err)
	}
	if _, err := st.RecordRun(ctx, RunRecord{
		RepoID:     repoID,
		Kind:       "summary",
		Scope:      "repo",
		Status:     "error",
		StartedAt:  "2026-07-13T00:03:00Z",
		FinishedAt: "2026-07-13T00:04:00Z",
	}); err != nil {
		t.Fatalf("failed summary run: %v", err)
	}
	if _, err := st.RecordRun(ctx, RunRecord{
		RepoID:     repoID,
		Kind:       "sync",
		Scope:      "open",
		Status:     "success",
		StartedAt:  "2026-07-14T00:00:00Z",
		FinishedAt: "2026-07-14T00:01:00Z",
		StatsJSON:  `{"metadata_only":true}`,
	}); err != nil {
		t.Fatalf("metadata-only sync run: %v", err)
	}

	coverage, err := st.ArchiveCoverage(ctx, ArchiveCoverageOptions{})
	if err != nil {
		t.Fatalf("coverage: %v", err)
	}
	if len(coverage.Rows) != 1 {
		t.Fatalf("coverage rows = %d", len(coverage.Rows))
	}
	if metric := coverage.Rows[0].Enrichment.Revisions; metric.Fresh != 1 || metric.LatestAt != "2026-07-13T00:01:00.000000000Z" {
		t.Fatalf("revision coverage = %+v", metric)
	}
	if metric := coverage.Rows[0].Enrichment.Fingerprints; metric.Fresh != 1 || metric.LatestAt != "2026-07-13T00:01:00.000000000Z" {
		t.Fatalf("fingerprint coverage = %+v", metric)
	}
	if metric := coverage.Rows[0].Enrichment.Summaries; metric.Fresh != 1 || metric.LatestAt != "2026-07-13T00:02:00.000000000Z" {
		t.Fatalf("summary coverage = %+v", metric)
	}
}

func TestArchiveCoverageSummariesRequireKeySummaryKind(t *testing.T) {
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
		GitHubID:        "2",
		Number:          2,
		Kind:            "issue",
		State:           "open",
		Title:           "Generic summary",
		Body:            "Only key summaries satisfy the producer contract.",
		HTMLURL:         "https://github.com/openclaw/gitcrawl/issues/2",
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
	enrichment, err := st.UpsertThreadRevisionAndFingerprint(ctx, ThreadEvidence{Thread: thread}, "2026-07-12T00:00:00Z")
	if err != nil {
		t.Fatalf("enrichment: %v", err)
	}
	if err := st.UpsertThreadKeySummary(ctx, ThreadKeySummary{
		ThreadRevisionID: enrichment.RevisionID,
		SummaryKind:      "generic",
		PromptVersion:    "v1",
		Provider:         "test",
		Model:            "test",
		InputHash:        "input",
		OutputHash:       "output",
		KeyText:          "Generic summary.",
		CreatedAt:        "2026-07-12T00:01:00Z",
	}); err != nil {
		t.Fatalf("summary: %v", err)
	}

	coverage, err := st.ArchiveCoverage(ctx, ArchiveCoverageOptions{})
	if err != nil {
		t.Fatalf("coverage: %v", err)
	}
	metric := coverage.Rows[0].Enrichment.Summaries
	if metric.Eligible != 1 || metric.Covered != 0 || metric.Fresh != 0 {
		t.Fatalf("summary coverage = %+v", metric)
	}
}

func TestArchiveCoverageUsesSummaryCreationTimeWithoutRunHistory(t *testing.T) {
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
		t.Fatalf("repository: %v", err)
	}
	thread := Thread{
		RepoID: repoID, GitHubID: "3", Number: 3, Kind: "issue", State: "open",
		Title: "Portable summary time", HTMLURL: "https://github.com/openclaw/gitcrawl/issues/3",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "thread",
		UpdatedAtGitHub: "2026-07-12T00:00:00Z", UpdatedAt: "2026-07-12T00:00:00Z",
	}
	thread.ID, err = st.UpsertThread(ctx, thread)
	if err != nil {
		t.Fatalf("thread: %v", err)
	}
	enrichment, err := st.UpsertThreadRevisionAndFingerprint(ctx, ThreadEvidence{Thread: thread}, "2026-07-12T00:01:00Z")
	if err != nil {
		t.Fatalf("enrichment: %v", err)
	}
	if err := st.UpsertThreadKeySummary(ctx, ThreadKeySummary{
		ThreadRevisionID: enrichment.RevisionID,
		SummaryKind:      SummaryKindLLMKey,
		PromptVersion:    SummaryPromptVersionV1,
		Provider:         "openai",
		Model:            "summary-test",
		InputHash:        "input",
		OutputHash:       "output",
		KeyText:          "Summary created after the revision.",
		CreatedAt:        "2026-07-12T00:05:00Z",
	}); err != nil {
		t.Fatalf("summary: %v", err)
	}
	metric, err := st.archiveRevisionChildCoverage(ctx, repoID, "thread_key_summaries", "thread_revision_id", "summary_kind", SummaryKindLLMKey)
	if err != nil {
		t.Fatalf("summary coverage: %v", err)
	}
	if metric.LatestAt != "2026-07-12T00:05:00.000000000Z" {
		t.Fatalf("summary latest observation = %+v", metric)
	}
}

func TestArchiveCoverageUsesLatestObservedRevision(t *testing.T) {
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
		UpdatedAt: "2026-07-12T00:02:00Z",
	})
	if err != nil {
		t.Fatalf("repository: %v", err)
	}
	current := Thread{
		RepoID:          repoID,
		GitHubID:        "3",
		Number:          3,
		Kind:            "issue",
		State:           "open",
		Title:           "Current evidence",
		Body:            "The lower revision id has the newest source observation.",
		HTMLURL:         "https://github.com/openclaw/gitcrawl/issues/3",
		LabelsJSON:      "[]",
		AssigneesJSON:   "[]",
		RawJSON:         "{}",
		ContentHash:     "thread-current",
		UpdatedAtGitHub: "2026-07-12T00:02:00Z",
		UpdatedAt:       "2026-07-12T00:02:00Z",
	}
	current.ID, err = st.UpsertThread(ctx, current)
	if err != nil {
		t.Fatalf("thread: %v", err)
	}
	currentEnrichment, err := st.UpsertThreadRevisionAndFingerprint(
		ctx,
		ThreadEvidence{Thread: current},
		"2026-07-12T00:02:00Z",
	)
	if err != nil {
		t.Fatalf("current enrichment: %v", err)
	}

	stale := current
	stale.Title = "Stale intermediate evidence"
	stale.ContentHash = "thread-stale"
	stale.UpdatedAtGitHub = "2026-07-12T00:01:00Z"
	stale.UpdatedAt = "2026-07-12T00:01:00Z"
	staleEnrichment, err := st.UpsertThreadRevisionAndFingerprint(
		ctx,
		ThreadEvidence{Thread: stale},
		"2026-07-12T00:01:00Z",
	)
	if err != nil {
		t.Fatalf("stale enrichment: %v", err)
	}
	if staleEnrichment.RevisionID <= currentEnrichment.RevisionID {
		t.Fatalf("stale revision id = %d, current revision id = %d", staleEnrichment.RevisionID, currentEnrichment.RevisionID)
	}
	if err := st.UpsertThreadKeySummary(ctx, ThreadKeySummary{
		ThreadRevisionID: currentEnrichment.RevisionID,
		SummaryKind:      SummaryKindLLMKey,
		PromptVersion:    SummaryPromptVersionV1,
		Provider:         "openai",
		Model:            "summary-test",
		InputHash:        "input",
		OutputHash:       "output",
		KeyText:          "Current observed evidence.",
		CreatedAt:        "2026-07-12T00:02:00Z",
	}); err != nil {
		t.Fatalf("summary: %v", err)
	}

	coverage, err := st.ArchiveCoverage(ctx, ArchiveCoverageOptions{})
	if err != nil {
		t.Fatalf("coverage: %v", err)
	}
	if metric := coverage.Rows[0].Enrichment.Revisions; metric.Fresh != 1 {
		t.Fatalf("revision coverage = %+v", metric)
	}
	if metric := coverage.Rows[0].Enrichment.Fingerprints; metric.Fresh != 1 {
		t.Fatalf("fingerprint coverage = %+v", metric)
	}
	if metric := coverage.Rows[0].Enrichment.Summaries; metric.Fresh != 1 {
		t.Fatalf("summary coverage = %+v", metric)
	}
}
