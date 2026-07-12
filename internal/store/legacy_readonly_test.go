package store

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLegacyReadOnlyArchiveUsesAvailableRevisionOrderColumns(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "gitcrawl.db")
	st, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	repoID, err := st.UpsertRepository(ctx, Repository{
		Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl",
		RawJSON: "{}", UpdatedAt: "2026-07-12T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("repository: %v", err)
	}
	thread := Thread{
		RepoID: repoID, GitHubID: "1", Number: 1, Kind: "issue", State: "open",
		Title: "legacy read only", Body: "preserve portable archive reads",
		HTMLURL:    "https://github.com/openclaw/gitcrawl/issues/1",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "thread",
		UpdatedAtGitHub: "2026-07-12T00:00:00Z", UpdatedAt: "2026-07-12T00:00:00Z",
	}
	thread.ID, err = st.UpsertThread(ctx, thread)
	if err != nil {
		t.Fatalf("thread: %v", err)
	}
	enrichment, err := st.UpsertThreadRevisionAndFingerprint(
		ctx,
		ThreadEvidence{Thread: thread},
		"2026-07-12T00:01:00Z",
	)
	if err != nil {
		t.Fatalf("enrichment: %v", err)
	}
	if err := st.UpsertThreadKeySummary(ctx, ThreadKeySummary{
		ThreadRevisionID: enrichment.RevisionID,
		SummaryKind:      SummaryKindLLMKey,
		PromptVersion:    SummaryPromptVersionV1,
		Provider:         "test",
		Model:            "test",
		InputHash:        "input",
		OutputHash:       "output",
		KeyText:          "legacy summary",
		CreatedAt:        "2026-07-12T00:02:00Z",
	}); err != nil {
		t.Fatalf("summary: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	raw, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open raw store: %v", err)
	}
	for _, statement := range []string{
		`alter table thread_revisions drop column observation_sequence`,
		`alter table threads drop column observation_sequence`,
		`drop table thread_observation_sequence`,
		`pragma user_version = 6`,
	} {
		if _, err := raw.ExecContext(ctx, statement); err != nil {
			_ = raw.Close()
			t.Fatalf("build legacy fixture with %q: %v", statement, err)
		}
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close raw store: %v", err)
	}
	before, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("read legacy store before: %v", err)
	}

	readOnly, err := OpenReadOnly(ctx, dbPath)
	if err != nil {
		t.Fatalf("open legacy store read only: %v", err)
	}
	coverage, err := readOnly.ArchiveCoverage(ctx, ArchiveCoverageOptions{})
	if err != nil {
		_ = readOnly.Close()
		t.Fatalf("archive coverage: %v", err)
	}
	if len(coverage.Rows) != 1 || coverage.Rows[0].Enrichment.Revisions.Covered != 1 {
		_ = readOnly.Close()
		t.Fatalf("legacy coverage = %+v", coverage)
	}
	summaries, err := readOnly.summariesByThreadIDs(ctx, []int64{thread.ID})
	if err != nil {
		_ = readOnly.Close()
		t.Fatalf("legacy summaries: %v", err)
	}
	if summaries[thread.ID][SummaryKindLLMKey] != "legacy summary" {
		_ = readOnly.Close()
		t.Fatalf("legacy summaries = %+v", summaries)
	}
	embeddingTasks, err := readOnly.ListEmbeddingTasks(ctx, EmbeddingTaskOptions{
		RepoID: repoID, Basis: "llm_key_summary", Model: "test", Force: true,
	})
	if err != nil {
		_ = readOnly.Close()
		t.Fatalf("legacy embedding tasks: %v", err)
	}
	if len(embeddingTasks) != 1 ||
		!strings.Contains(embeddingTasks[0].Text, "legacy summary") {
		_ = readOnly.Close()
		t.Fatalf("legacy embedding tasks = %+v", embeddingTasks)
	}
	if err := readOnly.Close(); err != nil {
		t.Fatalf("close read-only store: %v", err)
	}

	after, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("read legacy store after: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("read-only legacy queries mutated database bytes")
	}
}
