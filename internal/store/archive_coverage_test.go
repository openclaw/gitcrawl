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

func TestArchiveCoveragePRDetailFreshnessUsesAcceptedSourceObservation(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repoID, err := st.UpsertRepository(ctx, Repository{
		Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl",
		RawJSON: "{}", UpdatedAt: "2026-07-12T12:00:00Z",
	})
	if err != nil {
		t.Fatalf("repository: %v", err)
	}
	thread := archiveCoverageThread(repoID, 1, "pull_request")
	thread.UpdatedAtGitHub = "2026-07-12T12:00:00Z"
	thread.RawJSON = `{"version":1}`
	thread.ContentHash = "version-1"
	initial, err := st.UpsertThreadObservation(ctx, thread, UpsertThreadOptions{
		ObservationSequence: 1,
	})
	if err != nil {
		t.Fatalf("initial thread: %v", err)
	}
	thread.ID = initial.ID
	if applied, err := st.ReserveThreadChildObservation(
		ctx,
		thread.ID,
		ThreadChildPullRequestDetails,
		thread.UpdatedAtGitHub,
		1,
	); err != nil || !applied {
		t.Fatalf("reserve PR detail observation = %t, %v", applied, err)
	}
	if err := st.UpsertPullRequestCache(ctx, PullRequestDetail{
		ThreadID:  thread.ID,
		RepoID:    repoID,
		Number:    thread.Number,
		RawJSON:   "{}",
		FetchedAt: "2026-07-12T12:05:00Z",
		UpdatedAt: "2026-07-12T12:05:00Z",
	}, nil, nil, nil, nil); err != nil {
		t.Fatalf("PR detail: %v", err)
	}

	thread.UpdatedAtGitHub = "2026-07-12T12:03:00Z"
	thread.RawJSON = `{"version":2}`
	thread.ContentHash = "version-2"
	if current, err := st.UpsertThreadObservation(ctx, thread, UpsertThreadOptions{
		IncompleteEvidence:  true,
		ObservationSequence: 2,
	}); err != nil || !current.Applied {
		t.Fatalf("newer metadata observation = %+v, %v", current, err)
	}

	coverage, err := st.ArchiveCoverage(ctx, ArchiveCoverageOptions{})
	if err != nil {
		t.Fatalf("archive coverage: %v", err)
	}
	metric := coverage.Rows[0].Enrichment.PRDetails
	if metric.Covered != 1 || metric.Fresh != 1 || metric.Stale != 0 ||
		metric.LatestAt != "2026-07-12T12:05:00.000000000Z" {
		t.Fatalf("PR detail coverage after metadata-only parent = %+v", metric)
	}

	if accepted, err := st.UpsertThreadObservation(ctx, thread, UpsertThreadOptions{
		ObservationSequence: 3,
	}); err != nil || !accepted.EvidenceApplied {
		t.Fatalf("newer accepted evidence = %+v, %v", accepted, err)
	}
	coverage, err = st.ArchiveCoverage(ctx, ArchiveCoverageOptions{})
	if err != nil {
		t.Fatalf("archive coverage after accepted evidence: %v", err)
	}
	metric = coverage.Rows[0].Enrichment.PRDetails
	if metric.Covered != 1 || metric.Fresh != 0 || metric.Stale != 1 {
		t.Fatalf("PR detail coverage after accepted evidence advanced = %+v", metric)
	}
}

func TestArchiveCoveragePRFilesRequireCurrentObservation(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repoID, err := st.UpsertRepository(ctx, Repository{
		Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl",
		RawJSON: "{}", UpdatedAt: "2026-07-12T12:00:00Z",
	})
	if err != nil {
		t.Fatalf("repository: %v", err)
	}
	thread := archiveCoverageThread(repoID, 1, "pull_request")
	thread.UpdatedAtGitHub = "2026-07-12T12:00:00Z"
	thread.RawJSON = `{"version":1}`
	thread.ContentHash = "version-1"
	initial, err := st.UpsertThreadObservation(ctx, thread, UpsertThreadOptions{
		ObservationSequence: 1,
	})
	if err != nil {
		t.Fatalf("initial thread: %v", err)
	}
	thread.ID = initial.ID

	assertMetric := func(wantCovered, wantFresh, wantMissing, wantStale int, wantComplete bool) {
		t.Helper()
		coverage, err := st.ArchiveCoverage(ctx, ArchiveCoverageOptions{})
		if err != nil {
			t.Fatalf("archive coverage: %v", err)
		}
		if len(coverage.Rows) != 1 {
			t.Fatalf("coverage rows = %d, want 1", len(coverage.Rows))
		}
		metric := coverage.Rows[0].Enrichment.PRFiles
		if !metric.Supported || metric.Eligible != 1 ||
			metric.Covered != wantCovered || metric.Fresh != wantFresh ||
			metric.Missing != wantMissing || metric.Stale != wantStale ||
			metric.Complete != wantComplete {
			t.Fatalf("PR file coverage = %+v", metric)
		}
	}

	assertMetric(0, 0, 1, 0, false)
	if applied, err := st.ReserveThreadChildObservation(
		ctx,
		thread.ID,
		ThreadChildPullRequestFiles,
		thread.UpdatedAtGitHub,
		1,
	); err != nil || !applied {
		t.Fatalf("reserve PR file observation = %t, %v", applied, err)
	}
	assertMetric(1, 1, 0, 0, true)

	thread.UpdatedAtGitHub = "2026-07-12T12:03:00Z"
	thread.RawJSON = `{"version":2}`
	thread.ContentHash = "version-2"
	if accepted, err := st.UpsertThreadObservation(ctx, thread, UpsertThreadOptions{
		ObservationSequence: 2,
	}); err != nil || !accepted.EvidenceApplied {
		t.Fatalf("newer accepted evidence = %+v, %v", accepted, err)
	}
	assertMetric(1, 0, 0, 1, false)
}

func TestArchiveObservationFreshnessUsesSequenceForEqualSource(t *testing.T) {
	const source = "2026-07-12T12:00:00Z"
	if archiveObservationAtOrAfter(source, 10, source, 11) {
		t.Fatal("older sequence at the same source timestamp was reported fresh")
	}
	if !archiveObservationAtOrAfter(source, 11, source, 11) {
		t.Fatal("equal source and sequence should be fresh")
	}
	if !archiveObservationAtOrAfter(source, 12, source, 11) {
		t.Fatal("newer sequence at the same source timestamp should be fresh")
	}
}

func TestArchiveCoverageRevisionFreshnessParsesTimestamps(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name              string
		sourceUpdatedAt   string
		revisionUpdatedAt string
		wantFresh         int
	}{
		{
			name:              "fractional revision after whole-second source update",
			sourceUpdatedAt:   "2026-07-12T12:00:00Z",
			revisionUpdatedAt: "2026-07-12T12:00:00.500Z",
			wantFresh:         1,
		},
		{
			name:              "whole-second revision before fractional source update",
			sourceUpdatedAt:   "2026-07-12T12:00:00.500Z",
			revisionUpdatedAt: "2026-07-12T12:00:00Z",
			wantFresh:         0,
		},
		{
			name:              "malformed revision timestamp is stale",
			sourceUpdatedAt:   "2026-07-12T12:00:00Z",
			revisionUpdatedAt: "not-a-timestamp",
			wantFresh:         0,
		},
		{
			name:              "malformed source timestamp is stale",
			sourceUpdatedAt:   "not-a-timestamp",
			revisionUpdatedAt: "2026-07-12T12:00:00.500Z",
			wantFresh:         0,
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
			if _, err := st.DB().ExecContext(ctx, `
				update threads
				set observation_sequence = 0,
					evidence_observation_sequence = 0
				where id = ?
			`, threadID); err != nil {
				t.Fatalf("mark legacy thread: %v", err)
			}
			if _, err := st.DB().ExecContext(ctx, `
				insert into thread_revisions(
					thread_id, source_updated_at, content_hash, title_hash, body_hash, labels_hash, created_at
				) values(?, ?, 'content', 'title', 'body', 'labels', '2026-07-12T12:00:01Z')
			`, threadID, test.revisionUpdatedAt); err != nil {
				t.Fatalf("revision: %v", err)
			}

			coverage, err := st.ArchiveCoverage(ctx, ArchiveCoverageOptions{})
			if err != nil {
				t.Fatalf("archive coverage: %v", err)
			}
			if len(coverage.Rows) != 1 {
				t.Fatalf("coverage rows = %d, want 1", len(coverage.Rows))
			}
			metric := coverage.Rows[0].Enrichment.Revisions
			if metric.Eligible != 1 || metric.Covered != 1 || metric.Fresh != test.wantFresh ||
				metric.Stale != 1-test.wantFresh || metric.LatestAt != "2026-07-12T12:00:01.000000000Z" {
				t.Fatalf("revision coverage = %+v", metric)
			}
		})
	}
}

func TestArchiveCoverageClocklessFreshnessUsesObservationSequence(t *testing.T) {
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
		UpdatedAt: "2026-07-12T12:00:00Z",
	})
	if err != nil {
		t.Fatalf("repository: %v", err)
	}
	thread := archiveCoverageThread(repoID, 1, "issue")
	thread.UpdatedAtGitHub = ""
	thread.UpdatedAt = "2026-07-12T12:00:00Z"
	thread.ID, err = st.UpsertThread(ctx, thread)
	if err != nil {
		t.Fatalf("thread: %v", err)
	}
	enrichment, err := st.UpsertThreadRevisionAndFingerprint(
		ctx,
		ThreadEvidence{Thread: thread},
		"2026-07-12T12:00:01Z",
	)
	if err != nil {
		t.Fatalf("revision and fingerprint: %v", err)
	}
	if err := st.UpsertThreadKeySummary(ctx, ThreadKeySummary{
		ThreadRevisionID: enrichment.RevisionID,
		SummaryKind:      SummaryKindLLMKey,
		PromptVersion:    SummaryPromptVersionV1,
		Provider:         "openai",
		Model:            "summary-test",
		InputHash:        "input",
		OutputHash:       "output",
		KeyText:          "Current clockless evidence.",
		CreatedAt:        "2026-07-12T12:00:02Z",
	}); err != nil {
		t.Fatalf("summary: %v", err)
	}

	assertFresh := func(want int) {
		t.Helper()
		coverage, err := st.ArchiveCoverage(ctx, ArchiveCoverageOptions{})
		if err != nil {
			t.Fatalf("archive coverage: %v", err)
		}
		if len(coverage.Rows) != 1 {
			t.Fatalf("coverage rows = %d, want 1", len(coverage.Rows))
		}
		for name, metric := range map[string]EnrichmentCoverageMetric{
			"revisions":    coverage.Rows[0].Enrichment.Revisions,
			"fingerprints": coverage.Rows[0].Enrichment.Fingerprints,
			"summaries":    coverage.Rows[0].Enrichment.Summaries,
		} {
			if metric.Eligible != 1 || metric.Covered != 1 || metric.Fresh != want ||
				metric.Stale != 1-want {
				t.Fatalf("%s coverage = %+v", name, metric)
			}
		}
	}
	assertFresh(1)

	thread.Title = "Advanced without a source clock"
	thread.ContentHash = "clockless-advanced"
	thread.UpdatedAt = "2026-07-12T12:00:03Z"
	if _, err := st.UpsertThread(ctx, thread); err != nil {
		t.Fatalf("advance thread: %v", err)
	}
	assertFresh(0)
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
	if err := st.UpsertPullRequestCache(ctx, PullRequestDetail{ThreadID: detailedPRID, RepoID: primaryID, Number: 2, HeadSHA: "coverage-head", RawJSON: "{}", FetchedAt: "2026-07-06T00:01:00Z", UpdatedAt: "2026-07-06T00:01:00Z"},
		[]PullRequestFile{{ThreadID: detailedPRID, Path: "README.md", RawJSON: "{}", FetchedAt: "2026-07-06T00:01:00Z"}},
		[]PullRequestCommit{{ThreadID: detailedPRID, SHA: "abc", RawJSON: "{}", FetchedAt: "2026-07-06T00:01:00Z"}},
		[]PullRequestCheck{{ThreadID: detailedPRID, Name: "test", RawJSON: "{}", FetchedAt: "2026-07-06T00:01:00Z"}},
		[]WorkflowRun{{RepoID: primaryID, RunID: "1", HeadSHA: "coverage-head", RawJSON: "{}", FetchedAt: "2026-07-06T00:01:00Z"}},
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
		RepoID:          repoID,
		GitHubID:        fmt.Sprintf("gid-%d", number),
		Number:          number,
		Kind:            kind,
		State:           "open",
		Title:           "thread",
		HTMLURL:         fmt.Sprintf("https://github.com/openclaw/gitcrawl/issues/%d", number),
		LabelsJSON:      "[]",
		AssigneesJSON:   "[]",
		RawJSON:         "{}",
		ContentHash:     fmt.Sprintf("hash-%d", number),
		UpdatedAtGitHub: "2026-07-06T00:00:00Z",
		UpdatedAt:       "2026-07-06T00:00:00Z",
	}
}
