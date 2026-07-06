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
	query := `
		select
		  r.id,
		  r.full_name,
		  coalesce(sum(case when t.kind = 'issue' then 1 else 0 end), 0) as issues,
		  coalesce(sum(case when t.kind = 'pull_request' then 1 else 0 end), 0) as pull_requests,
		  coalesce(sum(case when t.kind = 'pull_request' and t.state = 'open' then 1 else 0 end), 0) as open_pull_requests,
		  (
		    select count(*)
		    from comments c
		    join threads ct on ct.id = c.thread_id
		    where ct.repo_id = r.id
		  ) as comments,
		  (
		    select count(*)
		    from comments c
		    join threads ct on ct.id = c.thread_id
		    where ct.repo_id = r.id and c.comment_type = 'pull_review'
		  ) as pr_reviews,
		  count(distinct prd.thread_id) as pull_requests_with_details,
		  coalesce(sum(case when t.kind = 'pull_request' then 1 else 0 end), 0) - count(distinct prd.thread_id) as missing_pr_details,
		  (
		    select count(*)
		    from pull_request_files pf
		    join threads pt on pt.id = pf.thread_id
		    where pt.repo_id = r.id
		  ) as pr_files,
		  (
		    select count(*)
		    from pull_request_commits pc
		    join threads pt on pt.id = pc.thread_id
		    where pt.repo_id = r.id
		  ) as pr_commits,
		  (
		    select count(*)
		    from pull_request_checks pc
		    join threads pt on pt.id = pc.thread_id
		    where pt.repo_id = r.id
		  ) as pr_checks,
		  (
		    select count(*)
		    from pull_request_review_threads prt
		    join threads pt on pt.id = prt.thread_id
		    where pt.repo_id = r.id
		  ) as pr_review_threads,
		  (
		    select count(*)
		    from github_workflow_runs gwr
		    where gwr.repo_id = r.id
		  ) as workflow_runs,
		  (
		    select coalesce(max(finished_at), '')
		    from sync_runs sr
		    where sr.repo_id = r.id and sr.status in ('success', 'completed')
		  ) as last_sync_at
		from repositories r
		left join threads t on t.repo_id = r.id
		left join pull_request_details prd on prd.thread_id = t.id
	`
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
