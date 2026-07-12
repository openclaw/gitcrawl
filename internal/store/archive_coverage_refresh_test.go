package store

import (
	"context"
	"path/filepath"
	"testing"
)

func TestArchiveCoverageChildFreshnessUsesRevisionObservation(t *testing.T) {
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

	thread.UpdatedAtGitHub = "2026-07-13T00:00:00Z"
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

	coverage, err := st.ArchiveCoverage(ctx, ArchiveCoverageOptions{})
	if err != nil {
		t.Fatalf("coverage: %v", err)
	}
	if len(coverage.Rows) != 1 {
		t.Fatalf("coverage rows = %d", len(coverage.Rows))
	}
	for name, metric := range map[string]EnrichmentCoverageMetric{
		"fingerprints": coverage.Rows[0].Enrichment.Fingerprints,
		"summaries":    coverage.Rows[0].Enrichment.Summaries,
	} {
		if metric.Fresh != 1 || metric.LatestAt != "2026-07-13T00:00:00.000000000Z" {
			t.Fatalf("%s coverage = %+v", name, metric)
		}
	}
}
