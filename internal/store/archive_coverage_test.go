package store

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

func TestArchiveCoverageReportsAndFiltersCurrentStore(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	primaryID, secondaryID := seedArchiveCoverageRows(t, ctx, st)

	coverage, err := st.ArchiveCoverage(ctx, ArchiveCoverageOptions{})
	if err != nil {
		t.Fatalf("archive coverage: %v", err)
	}
	if len(coverage.Rows) != 2 || coverage.Totals.PullRequests != 4 || coverage.Totals.MissingPRDetails != 3 {
		t.Fatalf("coverage = %+v", coverage)
	}
	primary := coverage.Rows[0]
	if primary.Repository != "openclaw/gitcrawl" || primary.Comments != 2 || primary.PRReviews != 1 || primary.PullRequestsWithDetails != 1 || primary.PRFiles != 1 || primary.PRCommits != 1 || primary.PRChecks != 1 || primary.PRReviewThreads != 1 || primary.WorkflowRuns != 1 || primary.LastSyncAt == "" {
		t.Fatalf("primary coverage = %+v", primary)
	}
	if !primary.Enrichment.Revisions.Supported ||
		primary.Enrichment.Revisions.Eligible != 4 ||
		primary.Enrichment.Revisions.Fresh != 1 ||
		primary.Enrichment.Fingerprints.Fresh != 1 ||
		primary.Enrichment.Summaries.Fresh != 1 ||
		primary.Enrichment.Clusters.Fresh != 1 ||
		primary.Enrichment.PRDetails.Eligible != 3 ||
		primary.Enrichment.PRDetails.Fresh != 1 ||
		primary.Enrichment.PRDetails.Complete {
		t.Fatalf("primary enrichment coverage = %+v", primary.Enrichment)
	}

	filtered, err := st.ArchiveCoverage(ctx, ArchiveCoverageOptions{RepoIDs: []int64{primaryID, secondaryID}, MinMissingPRDetails: 2})
	if err != nil {
		t.Fatalf("filtered archive coverage: %v", err)
	}
	if len(filtered.Rows) != 1 || filtered.Rows[0].Repository != "openclaw/gitcrawl" {
		t.Fatalf("filtered coverage = %+v", filtered)
	}
}

func TestArchiveCoveragePRDetailFreshnessParsesTimestamps(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name            string
		sourceUpdatedAt string
		fetchedAt       string
		wantFresh       int
		wantLatestAt    string
	}{
		{
			name:            "fractional fetch after whole-second source update",
			sourceUpdatedAt: "2026-07-12T12:00:00Z",
			fetchedAt:       "2026-07-12T12:00:00.500Z",
			wantFresh:       1,
			wantLatestAt:    "2026-07-12T12:00:00.500000000Z",
		},
		{
			name:            "whole-second fetch before fractional source update",
			sourceUpdatedAt: "2026-07-12T12:00:00.500Z",
			fetchedAt:       "2026-07-12T12:00:00Z",
			wantFresh:       0,
			wantLatestAt:    "2026-07-12T12:00:00.000000000Z",
		},
		{
			name:            "malformed fetch timestamp is stale",
			sourceUpdatedAt: "2026-07-12T12:00:00Z",
			fetchedAt:       "not-a-timestamp",
			wantFresh:       0,
		},
		{
			name:            "malformed source timestamp is stale",
			sourceUpdatedAt: "not-a-timestamp",
			fetchedAt:       "2026-07-12T12:00:00.500Z",
			wantFresh:       0,
			wantLatestAt:    "2026-07-12T12:00:00.500000000Z",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
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
				UpdatedAt: "2026-07-12T12:00:00Z",
			})
			if err != nil {
				t.Fatalf("repository: %v", err)
			}
			thread := archiveCoverageThread(repoID, 1, "pull_request")
			thread.UpdatedAtGitHub = test.sourceUpdatedAt
			threadID, err := st.UpsertThread(ctx, thread)
			if err != nil {
				t.Fatalf("thread: %v", err)
			}
			if err := st.UpsertPullRequestCache(ctx, PullRequestDetail{
				ThreadID:  threadID,
				RepoID:    repoID,
				Number:    thread.Number,
				RawJSON:   "{}",
				FetchedAt: test.fetchedAt,
				UpdatedAt: test.fetchedAt,
			}, nil, nil, nil, nil); err != nil {
				t.Fatalf("PR detail: %v", err)
			}

			coverage, err := st.ArchiveCoverage(ctx, ArchiveCoverageOptions{})
			if err != nil {
				t.Fatalf("archive coverage: %v", err)
			}
			if len(coverage.Rows) != 1 {
				t.Fatalf("coverage rows = %d, want 1", len(coverage.Rows))
			}
			metric := coverage.Rows[0].Enrichment.PRDetails
			if metric.Eligible != 1 || metric.Covered != 1 || metric.Fresh != test.wantFresh ||
				metric.Stale != 1-test.wantFresh || metric.LatestAt != test.wantLatestAt {
				t.Fatalf("PR detail coverage = %+v", metric)
			}
		})
	}
}

func TestArchiveCoverageSupportsPortableAndOptionalTableDrift(t *testing.T) {
	ctx := context.Background()

	t.Run("portable", func(t *testing.T) {
		st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
		if err != nil {
			t.Fatalf("open store: %v", err)
		}
		defer st.Close()
		seedArchiveCoverageRows(t, ctx, st)
		if _, err := st.PrunePortablePayloads(ctx, PortablePruneOptions{BodyChars: 2000, Vacuum: false}); err != nil {
			t.Fatalf("prune portable store: %v", err)
		}
		coverage, err := st.ArchiveCoverage(ctx, ArchiveCoverageOptions{})
		if err != nil {
			t.Fatalf("portable archive coverage: %v", err)
		}
		if len(coverage.Rows) != 2 || coverage.Rows[0].LastSyncAt == "" {
			t.Fatalf("portable coverage = %+v", coverage)
		}
	})

	t.Run("optional tables absent", func(t *testing.T) {
		st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
		if err != nil {
			t.Fatalf("open store: %v", err)
		}
		defer st.Close()
		seedArchiveCoverageRows(t, ctx, st)
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
			"thread_fingerprints",
			"thread_key_summaries",
			"thread_revisions",
			"cluster_memberships",
			"cluster_groups",
			"cluster_runs",
		} {
			if _, err := st.DB().ExecContext(ctx, `drop table `+table); err != nil {
				t.Fatalf("drop %s: %v", table, err)
			}
		}
		coverage, err := st.ArchiveCoverage(ctx, ArchiveCoverageOptions{})
		if err != nil {
			t.Fatalf("compatibility archive coverage: %v", err)
		}
		if len(coverage.Rows) != 2 ||
			coverage.Rows[0].PullRequestsWithDetails != 0 ||
			coverage.Rows[0].MissingPRDetails != coverage.Rows[0].PullRequests ||
			coverage.Rows[0].Comments != 0 ||
			coverage.Rows[0].LastSyncAt != "" ||
			coverage.Rows[0].Enrichment.Revisions.Supported ||
			coverage.Rows[0].Enrichment.Clusters.Supported {
			t.Fatalf("compatibility coverage = %+v", coverage)
		}
	})
}

func TestArchiveCoverageRejectsIncompatibleCoreTables(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	if _, err := st.DB().ExecContext(ctx, `alter table threads rename to legacy_threads`); err != nil {
		t.Fatalf("rename threads: %v", err)
	}
	if _, err := st.ArchiveCoverage(ctx, ArchiveCoverageOptions{}); err == nil || !strings.Contains(err.Error(), "gitcrawl doctor --json") {
		t.Fatalf("incompatible core error = %v", err)
	}
}

func seedArchiveCoverageRows(t *testing.T, ctx context.Context, st *Store) (int64, int64) {
	t.Helper()
	primaryID, err := st.UpsertRepository(ctx, Repository{Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl", RawJSON: "{}", UpdatedAt: "2026-07-06T00:00:00Z"})
	if err != nil {
		t.Fatalf("primary repository: %v", err)
	}
	secondaryID, err := st.UpsertRepository(ctx, Repository{Owner: "openclaw", Name: "other", FullName: "openclaw/other", RawJSON: "{}", UpdatedAt: "2026-07-06T00:00:00Z"})
	if err != nil {
		t.Fatalf("secondary repository: %v", err)
	}
	issueID, err := st.UpsertThread(ctx, archiveCoverageThread(primaryID, 1, "issue"))
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	var detailedPRID int64
	for _, number := range []int{2, 3, 4} {
		threadID, err := st.UpsertThread(ctx, archiveCoverageThread(primaryID, number, "pull_request"))
		if err != nil {
			t.Fatalf("primary PR %d: %v", number, err)
		}
		if number == 2 {
			detailedPRID = threadID
		}
	}
	if _, err := st.UpsertThread(ctx, archiveCoverageThread(secondaryID, 5, "pull_request")); err != nil {
		t.Fatalf("secondary PR: %v", err)
	}
	for _, comment := range []Comment{
		{ThreadID: issueID, GitHubID: "comment", CommentType: "issue_comment", Body: "comment", RawJSON: "{}"},
		{ThreadID: detailedPRID, GitHubID: "review", CommentType: "pull_review", Body: "review", RawJSON: "{}"},
	} {
		if _, err := st.UpsertComment(ctx, comment); err != nil {
			t.Fatalf("comment: %v", err)
		}
	}
	if err := st.UpsertPullRequestCache(ctx, PullRequestDetail{ThreadID: detailedPRID, RepoID: primaryID, Number: 2, RawJSON: "{}", FetchedAt: "2026-07-06T00:01:00Z", UpdatedAt: "2026-07-06T00:01:00Z"},
		[]PullRequestFile{{ThreadID: detailedPRID, Path: "README.md", RawJSON: "{}", FetchedAt: "2026-07-06T00:01:00Z"}},
		[]PullRequestCommit{{ThreadID: detailedPRID, SHA: "abc", RawJSON: "{}", FetchedAt: "2026-07-06T00:01:00Z"}},
		[]PullRequestCheck{{ThreadID: detailedPRID, Name: "test", RawJSON: "{}", FetchedAt: "2026-07-06T00:01:00Z"}},
		[]WorkflowRun{{RepoID: primaryID, RunID: "1", RawJSON: "{}", FetchedAt: "2026-07-06T00:01:00Z"}},
	); err != nil {
		t.Fatalf("PR cache: %v", err)
	}
	if err := st.UpsertPullRequestReviewThreads(ctx, detailedPRID, "2026-07-06T00:01:00Z", []PullRequestReviewThread{{ThreadID: detailedPRID, ReviewThreadID: "thread", CommentsJSON: "[]", RawJSON: "{}", FetchedAt: "2026-07-06T00:01:00Z"}}); err != nil {
		t.Fatalf("review threads: %v", err)
	}
	detailedThread := archiveCoverageThread(primaryID, 2, "pull_request")
	detailedThread.ID = detailedPRID
	enrichment, err := st.UpsertThreadRevisionAndFingerprint(ctx, ThreadEvidence{Thread: detailedThread}, "2026-07-06T00:01:00Z")
	if err != nil {
		t.Fatalf("thread enrichment: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `
		insert into thread_key_summaries(thread_revision_id, summary_kind, prompt_version, provider, model, input_hash, output_hash, key_text, created_at)
		values(?, 'llm_key_summary', 'v1', 'test', 'test', 'input', 'output', 'summary', '2026-07-06T00:01:00Z')
	`, enrichment.RevisionID); err != nil {
		t.Fatalf("key summary: %v", err)
	}
	clusterRun, err := st.DB().ExecContext(ctx, `
		insert into cluster_runs(repo_id, scope, status, started_at, finished_at)
		values(?, 'durable', 'success', '2026-07-06T00:01:00Z', '2026-07-06T00:01:00Z')
	`, primaryID)
	if err != nil {
		t.Fatalf("cluster run: %v", err)
	}
	clusterRunID, err := clusterRun.LastInsertId()
	if err != nil {
		t.Fatalf("cluster run id: %v", err)
	}
	clusterGroup, err := st.DB().ExecContext(ctx, `
		insert into cluster_groups(repo_id, stable_key, stable_slug, status, cluster_type, representative_thread_id, title, created_at, updated_at)
		values(?, 'coverage', 'coverage', 'active', 'duplicate_candidate', ?, 'coverage', '2026-07-06T00:01:00Z', '2026-07-06T00:01:00Z')
	`, primaryID, detailedPRID)
	if err != nil {
		t.Fatalf("cluster group: %v", err)
	}
	clusterGroupID, err := clusterGroup.LastInsertId()
	if err != nil {
		t.Fatalf("cluster group id: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `
		insert into cluster_memberships(
			cluster_id, thread_id, role, state, first_seen_run_id, last_seen_run_id,
			added_by, added_reason_json, created_at, updated_at
		)
		values(?, ?, 'canonical', 'active', ?, ?, 'test', '{}', '2026-07-06T00:01:00Z', '2026-07-06T00:01:00Z')
	`, clusterGroupID, detailedPRID, clusterRunID, clusterRunID); err != nil {
		t.Fatalf("cluster membership: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `insert into sync_runs(repo_id, scope, status, started_at, finished_at) values(?, 'open', 'success', '2026-07-06T00:00:00Z', '2026-07-06T00:02:00Z')`, primaryID); err != nil {
		t.Fatalf("sync run: %v", err)
	}
	return primaryID, secondaryID
}

func archiveCoverageThread(repoID int64, number int, kind string) Thread {
	return Thread{
		RepoID:        repoID,
		GitHubID:      fmt.Sprintf("gid-%d", number),
		Number:        number,
		Kind:          kind,
		State:         "open",
		Title:         "thread",
		HTMLURL:       fmt.Sprintf("https://github.com/openclaw/gitcrawl/issues/%d", number),
		LabelsJSON:    "[]",
		AssigneesJSON: "[]",
		RawJSON:       "{}",
		ContentHash:   fmt.Sprintf("hash-%d", number),
		UpdatedAt:     "2026-07-06T00:00:00Z",
	}
}
