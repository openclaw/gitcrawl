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
		RepositoryFilters []string                   `json:"repository_filters"`
		Repositories      []store.ArchiveCoverageRow `json:"repositories"`
		Totals            store.ArchiveCoverageRow   `json:"totals"`
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
	if row.Comments != 2 || row.PRReviews != 1 {
		t.Fatalf("comment coverage = %+v", row)
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

	multiRun := New()
	var multiOut bytes.Buffer
	multiRun.Stdout = &multiOut
	if err := multiRun.Run(ctx, []string{"--config", configPath, "coverage", "--repos", "openclaw/gitcrawl,openclaw/other,openclaw/gitcrawl", "--json"}); err != nil {
		t.Fatalf("coverage multi-repo json: %v", err)
	}
	if err := json.Unmarshal(multiOut.Bytes(), &payload); err != nil {
		t.Fatalf("decode multi-repo coverage json: %v\n%s", err, multiOut.String())
	}
	if got := payload.RepositoryFilters; len(got) != 2 || got[0] != "openclaw/gitcrawl" || got[1] != "openclaw/other" {
		t.Fatalf("repository filters = %v", got)
	}
	if len(payload.Repositories) != 2 {
		t.Fatalf("filtered repositories = %+v", payload.Repositories)
	}

	filteredRun := New()
	var filteredOut bytes.Buffer
	filteredRun.Stdout = &filteredOut
	if err := filteredRun.Run(ctx, []string{"--config", configPath, "coverage", "--repos", "openclaw/gitcrawl,openclaw/other", "--min-missing-pr-details", "4", "--json"}); err != nil {
		t.Fatalf("coverage filtered json: %v", err)
	}
	payload.Repositories = nil
	if err := json.Unmarshal(filteredOut.Bytes(), &payload); err != nil {
		t.Fatalf("decode filtered coverage json: %v\n%s", err, filteredOut.String())
	}
	if payload.Repositories == nil || len(payload.Repositories) != 0 {
		t.Fatalf("empty repositories should encode as an array: %+v", payload.Repositories)
	}

	for _, args := range [][]string{
		{"--config", configPath, "coverage", "openclaw/gitcrawl", "--repos", "openclaw/other", "--json"},
		{"--config", configPath, "coverage", "--repos", "openclaw/gitcrawl,,openclaw/other", "--json"},
	} {
		if err := New().Run(ctx, args); err == nil {
			t.Fatalf("coverage args should fail: %v", args)
		}
	}
}

func TestCoverageCommandSupportsCanonicalPortableArchive(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "gitcrawl.db")
	if err := New().Run(ctx, []string{"--config", configPath, "init", "--db", dbPath}); err != nil {
		t.Fatalf("init: %v", err)
	}
	seedCoverageStore(t, ctx, dbPath)

	st, err := store.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if _, err := st.PrunePortablePayloads(ctx, store.PortablePruneOptions{BodyChars: 2000, Vacuum: false}); err != nil {
		st.Close()
		t.Fatalf("prune portable archive: %v", err)
	}
	var syncRuns int
	if err := st.DB().QueryRowContext(ctx, `select count(*) from sqlite_master where type = 'table' and name = 'sync_runs'`).Scan(&syncRuns); err != nil {
		st.Close()
		t.Fatalf("inspect portable archive: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close portable archive: %v", err)
	}
	if syncRuns != 0 {
		t.Fatalf("portable archive retained sync_runs")
	}

	run := New()
	var out bytes.Buffer
	run.Stdout = &out
	if err := run.Run(ctx, []string{"--config", configPath, "coverage", "--json"}); err != nil {
		t.Fatalf("coverage portable archive: %v", err)
	}
	var payload struct {
		Repositories []store.ArchiveCoverageRow `json:"repositories"`
	}
	if err := json.Unmarshal(out.Bytes(), &payload); err != nil {
		t.Fatalf("decode portable coverage json: %v\n%s", err, out.String())
	}
	if len(payload.Repositories) != 2 {
		t.Fatalf("portable coverage rows = %+v", payload.Repositories)
	}
	for _, row := range payload.Repositories {
		if row.LastSyncAt == "" {
			t.Fatalf("portable coverage missing last sync: %+v", row)
		}
	}
}

func TestCoverageCommandSupportsArchiveWithoutOptionalCoverageTables(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "gitcrawl.db")
	if err := New().Run(ctx, []string{"--config", configPath, "init", "--db", dbPath}); err != nil {
		t.Fatalf("init: %v", err)
	}
	seedCoverageStore(t, ctx, dbPath)

	st, err := store.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	for _, table := range []string{
		"comments",
		"pull_request_files",
		"pull_request_commits",
		"pull_request_checks",
		"pull_request_review_threads",
		"pull_request_details",
		"github_workflow_runs",
		"sync_runs",
		"repo_sync_state",
	} {
		if _, err := st.DB().ExecContext(ctx, `drop table `+table); err != nil {
			st.Close()
			t.Fatalf("drop %s: %v", table, err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close compatibility archive: %v", err)
	}

	run := New()
	var out bytes.Buffer
	run.Stdout = &out
	if err := run.Run(ctx, []string{"--config", configPath, "coverage", "--json"}); err != nil {
		t.Fatalf("coverage compatibility archive: %v", err)
	}
	var payload struct {
		Repositories []store.ArchiveCoverageRow `json:"repositories"`
	}
	if err := json.Unmarshal(out.Bytes(), &payload); err != nil {
		t.Fatalf("decode compatibility coverage json: %v\n%s", err, out.String())
	}
	if len(payload.Repositories) != 2 {
		t.Fatalf("compatibility coverage rows = %+v", payload.Repositories)
	}
	for _, row := range payload.Repositories {
		if row.PullRequestsWithDetails != 0 || row.MissingPRDetails != row.PullRequests || row.Comments != 0 || row.PRFiles != 0 || row.WorkflowRuns != 0 {
			t.Fatalf("compatibility coverage row = %+v", row)
		}
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
	issueID, err := st.UpsertThread(ctx, coverageThread(gitcrawlID, 1, "issue"))
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
	for _, comment := range []store.Comment{
		{ThreadID: issueID, GitHubID: "issue-comment", CommentType: "issue_comment", Body: "comment", RawJSON: "{}"},
		{ThreadID: detailedPRID, GitHubID: "review", CommentType: "pull_review", Body: "review", RawJSON: "{}"},
	} {
		if _, err := st.UpsertComment(ctx, comment); err != nil {
			t.Fatalf("comment: %v", err)
		}
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
