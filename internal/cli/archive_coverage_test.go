package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/openclaw/gitcrawl/internal/store"
)

func TestCoverageCommandJSONAndTable(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "gitcrawl.db")
	if err := New().Run(ctx, []string{"--config", configPath, "init", "--db", dbPath}); err != nil {
		t.Fatalf("init: %v", err)
	}
	seedCoverageStore(t, ctx, dbPath)

	jsonRun := New()
	var jsonOut bytes.Buffer
	jsonRun.Stdout = &jsonOut
	if err := jsonRun.Run(ctx, []string{"--config", configPath, "coverage", "openclaw/gitcrawl", "--min-missing-pr-details", "2", "--json"}); err != nil {
		t.Fatalf("coverage json: %v", err)
	}
	var payload struct {
		Repositories []store.ArchiveCoverageRow `json:"repositories"`
		Totals       store.ArchiveCoverageRow   `json:"totals"`
	}
	if err := json.Unmarshal(jsonOut.Bytes(), &payload); err != nil {
		t.Fatalf("decode coverage json: %v\n%s", err, jsonOut.String())
	}
	if len(payload.Repositories) != 1 {
		t.Fatalf("repositories = %+v", payload.Repositories)
	}
	row := payload.Repositories[0]
	if row.Repository != "openclaw/gitcrawl" || row.PullRequests != 3 || row.PullRequestsWithDetails != 1 || row.MissingPRDetails != 2 {
		t.Fatalf("coverage row = %+v", row)
	}
	if row.HydrationFailuresSupported || row.KnownFailedHydrations != nil {
		t.Fatalf("failure ledger fields = %+v", row)
	}

	tableRun := New()
	var tableOut bytes.Buffer
	tableRun.Stdout = &tableOut
	if err := tableRun.Run(ctx, []string{"--config", configPath, "coverage"}); err != nil {
		t.Fatalf("coverage table: %v", err)
	}
	if !strings.Contains(tableOut.String(), "MISSING_PR_DETAILS") || !strings.Contains(tableOut.String(), "openclaw/gitcrawl") || !strings.Contains(tableOut.String(), "openclaw/other") {
		t.Fatalf("coverage table output = %q", tableOut.String())
	}
}

func seedCoverageStore(t *testing.T, ctx context.Context, dbPath string) {
	t.Helper()
	st, err := store.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	gitcrawlID, err := st.UpsertRepository(ctx, store.Repository{Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl", RawJSON: "{}", UpdatedAt: "2026-07-03T00:00:00Z"})
	if err != nil {
		t.Fatalf("gitcrawl repo: %v", err)
	}
	otherID, err := st.UpsertRepository(ctx, store.Repository{Owner: "openclaw", Name: "other", FullName: "openclaw/other", RawJSON: "{}", UpdatedAt: "2026-07-03T00:00:00Z"})
	if err != nil {
		t.Fatalf("other repo: %v", err)
	}
	_, err = st.UpsertThread(ctx, coverageThread(gitcrawlID, 1, "issue"))
	if err != nil {
		t.Fatalf("issue thread: %v", err)
	}
	var detailedPRID int64
	for _, number := range []int{2, 3, 4} {
		id, err := st.UpsertThread(ctx, coverageThread(gitcrawlID, number, "pull_request"))
		if err != nil {
			t.Fatalf("gitcrawl pr %d: %v", number, err)
		}
		if number == 3 {
			detailedPRID = id
		}
	}
	if _, err := st.UpsertThread(ctx, coverageThread(otherID, 9, "pull_request")); err != nil {
		t.Fatalf("other pr: %v", err)
	}
	if err := st.UpsertPullRequestCache(ctx, store.PullRequestDetail{
		ThreadID:  detailedPRID,
		RepoID:    gitcrawlID,
		Number:    3,
		RawJSON:   "{}",
		FetchedAt: "2026-07-03T00:00:00Z",
		UpdatedAt: "2026-07-03T00:00:00Z",
	}, []store.PullRequestFile{{
		ThreadID:  detailedPRID,
		Path:      "README.md",
		RawJSON:   "{}",
		FetchedAt: "2026-07-03T00:00:00Z",
	}}, []store.PullRequestCommit{{
		ThreadID:  detailedPRID,
		SHA:       "abc",
		RawJSON:   "{}",
		FetchedAt: "2026-07-03T00:00:00Z",
	}}, []store.PullRequestCheck{{
		ThreadID:  detailedPRID,
		Name:      "test",
		RawJSON:   "{}",
		FetchedAt: "2026-07-03T00:00:00Z",
	}}, []store.WorkflowRun{{
		RepoID:    gitcrawlID,
		RunID:     "99",
		RawJSON:   "{}",
		FetchedAt: "2026-07-03T00:00:00Z",
	}}); err != nil {
		t.Fatalf("pr details: %v", err)
	}
	if err := st.UpsertPullRequestReviewThreads(ctx, detailedPRID, "2026-07-03T00:00:00Z", []store.PullRequestReviewThread{{
		ThreadID:       detailedPRID,
		ReviewThreadID: "thread-1",
		RawJSON:        "{}",
		CommentsJSON:   "[]",
		FetchedAt:      "2026-07-03T00:00:00Z",
	}}); err != nil {
		t.Fatalf("review thread: %v", err)
	}
}

func coverageThread(repoID int64, number int, kind string) store.Thread {
	return store.Thread{
		RepoID:        repoID,
		GitHubID:      fmt.Sprintf("gid-%d", number),
		Number:        number,
		Kind:          kind,
		State:         "open",
		Title:         "thread",
		HTMLURL:       "https://github.com/openclaw/gitcrawl/issues/1",
		LabelsJSON:    "[]",
		AssigneesJSON: "[]",
		RawJSON:       "{}",
		ContentHash:   "hash",
		UpdatedAt:     "2026-07-03T00:00:00Z",
	}
}
