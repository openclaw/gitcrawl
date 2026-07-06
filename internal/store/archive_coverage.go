package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

type ArchiveCoverageOptions struct {
	RepoIDs             []int64
	MinMissingPRDetails int
}

type ArchiveCoverageRow struct {
	RepoID                     int64  `json:"repo_id"`
	Repository                 string `json:"repository"`
	Issues                     int    `json:"issues"`
	PullRequests               int    `json:"pull_requests"`
	OpenPullRequests           int    `json:"open_pull_requests"`
	Comments                   int    `json:"comments"`
	PRReviews                  int    `json:"pr_reviews"`
	PullRequestsWithDetails    int    `json:"pull_requests_with_details"`
	MissingPRDetails           int    `json:"missing_pr_details"`
	PRFiles                    int    `json:"pr_files"`
	PRCommits                  int    `json:"pr_commits"`
	PRChecks                   int    `json:"pr_checks"`
	PRReviewThreads            int    `json:"pr_review_threads"`
	WorkflowRuns               int    `json:"workflow_runs"`
	LastSyncAt                 string `json:"last_sync_at,omitempty"`
	HydrationFailuresSupported bool   `json:"hydration_failures_supported"`
	KnownFailedHydrations      *int   `json:"known_failed_hydrations"`
}

type ArchiveCoverage struct {
	Rows   []ArchiveCoverageRow `json:"repositories"`
	Totals ArchiveCoverageRow   `json:"totals"`
}

func (s *Store) ArchiveCoverage(ctx context.Context, opts ArchiveCoverageOptions) (ArchiveCoverage, error) {
	if !s.hasTable(ctx, "repositories") {
		return ArchiveCoverage{Rows: []ArchiveCoverageRow{}}, nil
	}
	if !s.archiveCoverageHasColumns(ctx, "repositories", "id", "full_name") ||
		!s.archiveCoverageHasColumns(ctx, "threads", "id", "repo_id", "kind", "state") {
		return ArchiveCoverage{}, fmt.Errorf("archive coverage requires compatible repositories and threads tables; run gitcrawl doctor --json")
	}
	commentsExpression := "0"
	prReviewsExpression := "0"
	if s.archiveCoverageHasColumns(ctx, "comments", "thread_id", "comment_type") {
		commentsExpression = `(
			select count(*)
			from comments c
			join threads ct on ct.id = c.thread_id
			where ct.repo_id = r.id
		)`
		prReviewsExpression = `(
			select count(*)
			from comments c
			join threads ct on ct.id = c.thread_id
			where ct.repo_id = r.id and c.comment_type = 'pull_review'
		)`
	}
	pullRequestsWithDetailsExpression := "0"
	missingPRDetailsExpression := "coalesce(sum(case when t.kind = 'pull_request' then 1 else 0 end), 0)"
	pullRequestDetailsJoin := ""
	if s.archiveCoverageHasColumns(ctx, "pull_request_details", "thread_id") {
		pullRequestsWithDetailsExpression = "count(distinct prd.thread_id)"
		missingPRDetailsExpression += " - count(distinct prd.thread_id)"
		pullRequestDetailsJoin = "left join pull_request_details prd on prd.thread_id = t.id\n"
	}
	prFilesExpression := s.archiveCoverageThreadChildCountExpression(ctx, "pull_request_files")
	prCommitsExpression := s.archiveCoverageThreadChildCountExpression(ctx, "pull_request_commits")
	prChecksExpression := s.archiveCoverageThreadChildCountExpression(ctx, "pull_request_checks")
	prReviewThreadsExpression := s.archiveCoverageThreadChildCountExpression(ctx, "pull_request_review_threads")
	workflowRunsExpression := "0"
	if s.archiveCoverageHasColumns(ctx, "github_workflow_runs", "repo_id") {
		workflowRunsExpression = `(
			select count(*)
			from github_workflow_runs gwr
			where gwr.repo_id = r.id
		)`
	}
	lastSyncExpression := s.archiveCoverageLastSyncExpression(ctx)
	query := `
		select
		  r.id,
		  r.full_name,
		  coalesce(sum(case when t.kind = 'issue' then 1 else 0 end), 0) as issues,
		  coalesce(sum(case when t.kind = 'pull_request' then 1 else 0 end), 0) as pull_requests,
		  coalesce(sum(case when t.kind = 'pull_request' and t.state = 'open' then 1 else 0 end), 0) as open_pull_requests,
		  ` + commentsExpression + ` as comments,
		  ` + prReviewsExpression + ` as pr_reviews,
		  ` + pullRequestsWithDetailsExpression + ` as pull_requests_with_details,
		  ` + missingPRDetailsExpression + ` as missing_pr_details,
		  ` + prFilesExpression + ` as pr_files,
		  ` + prCommitsExpression + ` as pr_commits,
		  ` + prChecksExpression + ` as pr_checks,
		  ` + prReviewThreadsExpression + ` as pr_review_threads,
		  ` + workflowRunsExpression + ` as workflow_runs,
		  ` + lastSyncExpression + ` as last_sync_at
		from repositories r
		left join threads t on t.repo_id = r.id
	` + pullRequestDetailsJoin
	args := make([]any, 0, len(opts.RepoIDs)+1)
	if len(opts.RepoIDs) > 0 {
		query += "where r.id in (" + strings.TrimSuffix(strings.Repeat("?,", len(opts.RepoIDs)), ",") + ")\n"
		for _, repoID := range opts.RepoIDs {
			args = append(args, repoID)
		}
	}
	query += `
		group by r.id, r.full_name
		having missing_pr_details >= ?
		order by r.full_name
	`
	args = append(args, opts.MinMissingPRDetails)
	rows, err := s.q().QueryContext(ctx, query, args...)
	if err != nil {
		return ArchiveCoverage{}, fmt.Errorf("archive coverage: %w", err)
	}
	defer rows.Close()

	coverage := ArchiveCoverage{Rows: []ArchiveCoverageRow{}}
	for rows.Next() {
		var row ArchiveCoverageRow
		var lastSync sql.NullString
		if err := rows.Scan(
			&row.RepoID,
			&row.Repository,
			&row.Issues,
			&row.PullRequests,
			&row.OpenPullRequests,
			&row.Comments,
			&row.PRReviews,
			&row.PullRequestsWithDetails,
			&row.MissingPRDetails,
			&row.PRFiles,
			&row.PRCommits,
			&row.PRChecks,
			&row.PRReviewThreads,
			&row.WorkflowRuns,
			&lastSync,
		); err != nil {
			return ArchiveCoverage{}, fmt.Errorf("scan archive coverage: %w", err)
		}
		row.LastSyncAt = lastSync.String
		row.HydrationFailuresSupported = false
		coverage.Rows = append(coverage.Rows, row)
		addArchiveCoverageTotals(&coverage.Totals, row)
	}
	if err := rows.Err(); err != nil {
		return ArchiveCoverage{}, fmt.Errorf("iterate archive coverage: %w", err)
	}
	coverage.Totals.Repository = "total"
	coverage.Totals.HydrationFailuresSupported = false
	return coverage, nil
}

func (s *Store) archiveCoverageHasColumns(ctx context.Context, table string, columns ...string) bool {
	if !s.hasTable(ctx, table) {
		return false
	}
	for _, column := range columns {
		if !s.hasColumn(ctx, table, column) {
			return false
		}
	}
	return true
}

func (s *Store) archiveCoverageThreadChildCountExpression(ctx context.Context, table string) string {
	if !s.archiveCoverageHasColumns(ctx, table, "thread_id") {
		return "0"
	}
	return `(
		select count(*)
		from ` + sqliteIdentifier(table) + ` child
		join threads pt on pt.id = child.thread_id
		where pt.repo_id = r.id
	)`
}

func (s *Store) archiveCoverageLastSyncExpression(ctx context.Context) string {
	candidates := make([]string, 0, 4)
	if s.archiveCoverageHasColumns(ctx, "sync_runs", "repo_id", "status", "finished_at") {
		candidates = append(candidates, `(
			select max(sr.finished_at)
			from sync_runs sr
			where sr.repo_id = r.id and sr.status in ('success', 'completed')
		)`)
	}
	if s.archiveCoverageHasColumns(ctx, "repo_sync_state", "repo_id", "last_open_close_reconciled_at", "last_overlapping_open_scan_completed_at", "last_non_overlapping_scan_completed_at", "last_full_open_scan_started_at", "updated_at") {
		candidates = append(candidates, `(
			select coalesce(
				max(rss.last_open_close_reconciled_at),
				max(rss.last_overlapping_open_scan_completed_at),
				max(rss.last_non_overlapping_scan_completed_at),
				max(rss.last_full_open_scan_started_at),
				max(rss.updated_at)
			)
			from repo_sync_state rss
			where rss.repo_id = r.id
		)`)
	}
	if s.archiveCoverageHasColumns(ctx, "portable_metadata", "key", "value") {
		candidates = append(candidates, `(
			select pm.value
			from portable_metadata pm
			where pm.key = 'exported_at'
		)`)
	}
	candidates = append(candidates, `''`)
	if len(candidates) == 1 {
		return candidates[0]
	}
	return "coalesce(" + strings.Join(candidates, ",\n") + ")"
}

func addArchiveCoverageTotals(total *ArchiveCoverageRow, row ArchiveCoverageRow) {
	total.Issues += row.Issues
	total.PullRequests += row.PullRequests
	total.OpenPullRequests += row.OpenPullRequests
	total.Comments += row.Comments
	total.PRReviews += row.PRReviews
	total.PullRequestsWithDetails += row.PullRequestsWithDetails
	total.MissingPRDetails += row.MissingPRDetails
	total.PRFiles += row.PRFiles
	total.PRCommits += row.PRCommits
	total.PRChecks += row.PRChecks
	total.PRReviewThreads += row.PRReviewThreads
	total.WorkflowRuns += row.WorkflowRuns
}
