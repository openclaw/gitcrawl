package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

const archiveCoverageTimestampLayout = "2006-01-02T15:04:05.000000000Z07:00"

type ArchiveCoverageOptions struct {
	RepoIDs             []int64
	MinMissingPRDetails int
}

type EnrichmentCoverageMetric struct {
	Supported      bool    `json:"supported"`
	Eligible       int     `json:"eligible"`
	Covered        int     `json:"covered"`
	Fresh          int     `json:"fresh"`
	Missing        int     `json:"missing"`
	Stale          int     `json:"stale"`
	CoverageRatio  float64 `json:"coverage_ratio"`
	FreshnessRatio float64 `json:"freshness_ratio"`
	Complete       bool    `json:"complete"`
	LatestAt       string  `json:"latest_at,omitempty"`
}

type EnrichmentCoverage struct {
	Revisions    EnrichmentCoverageMetric `json:"revisions"`
	Fingerprints EnrichmentCoverageMetric `json:"fingerprints"`
	Summaries    EnrichmentCoverageMetric `json:"summaries"`
	Clusters     EnrichmentCoverageMetric `json:"clusters"`
	PRDetails    EnrichmentCoverageMetric `json:"pr_details"`
}

type ArchiveCoverageRow struct {
	RepoID                     int64              `json:"repo_id"`
	Repository                 string             `json:"repository"`
	Issues                     int                `json:"issues"`
	PullRequests               int                `json:"pull_requests"`
	OpenPullRequests           int                `json:"open_pull_requests"`
	Comments                   int                `json:"comments"`
	PRReviews                  int                `json:"pr_reviews"`
	PullRequestsWithDetails    int                `json:"pull_requests_with_details"`
	MissingPRDetails           int                `json:"missing_pr_details"`
	PRFiles                    int                `json:"pr_files"`
	PRCommits                  int                `json:"pr_commits"`
	PRChecks                   int                `json:"pr_checks"`
	PRReviewThreads            int                `json:"pr_review_threads"`
	WorkflowRuns               int                `json:"workflow_runs"`
	LastSyncAt                 string             `json:"last_sync_at,omitempty"`
	HydrationFailuresSupported bool               `json:"hydration_failures_supported"`
	KnownFailedHydrations      *int               `json:"known_failed_hydrations"`
	Enrichment                 EnrichmentCoverage `json:"enrichment"`
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
	}
	if err := rows.Err(); err != nil {
		return ArchiveCoverage{}, fmt.Errorf("iterate archive coverage: %w", err)
	}
	if err := rows.Close(); err != nil {
		return ArchiveCoverage{}, fmt.Errorf("close archive coverage rows: %w", err)
	}
	for index := range coverage.Rows {
		coverage.Rows[index].Enrichment, err = s.archiveEnrichmentCoverage(ctx, coverage.Rows[index].RepoID)
		if err != nil {
			return ArchiveCoverage{}, err
		}
		addArchiveCoverageTotals(&coverage.Totals, coverage.Rows[index])
	}
	coverage.Totals.Repository = "total"
	coverage.Totals.HydrationFailuresSupported = false
	finalizeEnrichmentCoverage(&coverage.Totals.Enrichment)
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
	addEnrichmentCoverage(&total.Enrichment, row.Enrichment)
}

func (s *Store) archiveEnrichmentCoverage(ctx context.Context, repoID int64) (EnrichmentCoverage, error) {
	var coverage EnrichmentCoverage
	var err error
	coverage.Revisions, err = s.archiveRevisionCoverage(ctx, repoID)
	if err != nil {
		return EnrichmentCoverage{}, err
	}
	coverage.Fingerprints, err = s.archiveRevisionChildCoverage(ctx, repoID, "thread_fingerprints", "thread_revision_id", "algorithm_version", ThreadFingerprintAlgorithmVersion)
	if err != nil {
		return EnrichmentCoverage{}, err
	}
	coverage.Summaries, err = s.archiveRevisionChildCoverage(ctx, repoID, "thread_key_summaries", "thread_revision_id", "summary_kind", SummaryKindLLMKey)
	if err != nil {
		return EnrichmentCoverage{}, err
	}
	coverage.Clusters, err = s.archiveClusterCoverage(ctx, repoID)
	if err != nil {
		return EnrichmentCoverage{}, err
	}
	coverage.PRDetails, err = s.archivePRDetailCoverage(ctx, repoID)
	if err != nil {
		return EnrichmentCoverage{}, err
	}
	syncObservedAt, ok, err := s.archiveLatestSuccessfulHydrationRunAt(ctx, repoID)
	if err != nil {
		return EnrichmentCoverage{}, err
	}
	if ok {
		coverage.Revisions.LatestAt = formatArchiveCoverageTimestamp(syncObservedAt)
		coverage.Fingerprints.LatestAt = formatArchiveCoverageTimestamp(syncObservedAt)
	}
	summaryObservedAt, ok, err := s.archiveLatestSuccessfulRunAt(ctx, repoID, "summary_runs")
	if err != nil {
		return EnrichmentCoverage{}, err
	}
	if ok {
		coverage.Summaries.LatestAt = formatArchiveCoverageTimestamp(summaryObservedAt)
	}
	return coverage, nil
}

func (s *Store) archiveLatestSuccessfulHydrationRunAt(ctx context.Context, repoID int64) (time.Time, bool, error) {
	if !s.archiveCoverageHasColumns(ctx, "sync_runs", "repo_id", "status", "finished_at", "stats_json") {
		return time.Time{}, false, nil
	}
	var value sql.NullString
	if err := s.q().QueryRowContext(ctx, `
		select max(finished_at)
		from sync_runs
		where repo_id = ?
		  and status in ('success', 'completed')
		  and case
			when json_valid(stats_json) then
			  coalesce(json_extract(stats_json, '$.evidence_observed'), 0) > 0
			  or coalesce(json_extract(stats_json, '$.revisions_created'), 0) > 0
			  or coalesce(json_extract(stats_json, '$.fingerprints_upserted'), 0) > 0
			else 0
		  end
	`, repoID).Scan(&value); err != nil {
		return time.Time{}, false, fmt.Errorf("read latest successful sync hydration observation: %w", err)
	}
	if !value.Valid || strings.TrimSpace(value.String) == "" {
		return time.Time{}, false, nil
	}
	parsed, ok := parseArchiveCoverageTimestamp(value.String)
	if !ok {
		return time.Time{}, false, fmt.Errorf("latest successful sync hydration observation is invalid: %q", value.String)
	}
	return parsed, true, nil
}

func (s *Store) archiveLatestSuccessfulRunAt(ctx context.Context, repoID int64, table string) (time.Time, bool, error) {
	if !s.archiveCoverageHasColumns(ctx, table, "repo_id", "status", "finished_at") {
		return time.Time{}, false, nil
	}
	var value sql.NullString
	if err := s.q().QueryRowContext(ctx, `
		select max(finished_at)
		from `+sqliteIdentifier(table)+`
		where repo_id = ? and status in ('success', 'completed')
	`, repoID).Scan(&value); err != nil {
		return time.Time{}, false, fmt.Errorf("read latest successful %s observation: %w", table, err)
	}
	if !value.Valid || strings.TrimSpace(value.String) == "" {
		return time.Time{}, false, nil
	}
	parsed, ok := parseArchiveCoverageTimestamp(value.String)
	if !ok {
		return time.Time{}, false, fmt.Errorf("latest successful %s observation is invalid: %q", table, value.String)
	}
	return parsed, true, nil
}

func (s *Store) archiveRevisionCoverage(ctx context.Context, repoID int64) (EnrichmentCoverageMetric, error) {
	if !s.archiveCoverageHasColumns(ctx, "thread_revisions", "id", "thread_id", "source_updated_at", "created_at") {
		return EnrichmentCoverageMetric{}, nil
	}
	threadUpdatedAt := archiveThreadUpdatedAtExpression(s, ctx, "t")
	rows, err := s.q().QueryContext(ctx, `
		select case when tr.id is null then 0 else 1 end,
			coalesce(nullif(tr.source_updated_at, ''), tr.created_at, ''),
			coalesce(tr.created_at, ''),
			`+threadUpdatedAt+`
		from threads t
		left join thread_revisions tr on tr.id = (
			select latest.id
			from thread_revisions latest
			where latest.thread_id = t.id
			order by latest.id desc
			limit 1
		)
		where t.repo_id = ?
	`, repoID)
	if err != nil {
		return EnrichmentCoverageMetric{}, fmt.Errorf("archive revision coverage: %w", err)
	}
	defer rows.Close()

	metric := EnrichmentCoverageMetric{Supported: true}
	var latestCreatedAt time.Time
	for rows.Next() {
		var hasRevision int
		var revisionUpdatedAt, createdAt, sourceUpdatedAt string
		if err := rows.Scan(&hasRevision, &revisionUpdatedAt, &createdAt, &sourceUpdatedAt); err != nil {
			return EnrichmentCoverageMetric{}, fmt.Errorf("scan archive revision coverage: %w", err)
		}
		metric.Eligible++
		if hasRevision == 0 {
			continue
		}
		metric.Covered++
		if parsed, ok := parseArchiveCoverageTimestamp(createdAt); ok && (latestCreatedAt.IsZero() || parsed.After(latestCreatedAt)) {
			latestCreatedAt = parsed
		}
		if archiveCoverageTimestampAtOrAfter(revisionUpdatedAt, sourceUpdatedAt) {
			metric.Fresh++
		}
	}
	if err := rows.Err(); err != nil {
		return EnrichmentCoverageMetric{}, fmt.Errorf("iterate archive revision coverage: %w", err)
	}
	if !latestCreatedAt.IsZero() {
		metric.LatestAt = formatArchiveCoverageTimestamp(latestCreatedAt)
	}
	finalizeEnrichmentCoverageMetric(&metric)
	return metric, nil
}

func (s *Store) archiveRevisionChildCoverage(ctx context.Context, repoID int64, table, revisionColumn, conditionColumn, conditionValue string) (EnrichmentCoverageMetric, error) {
	if !s.archiveCoverageHasColumns(ctx, "thread_revisions", "id", "thread_id", "source_updated_at", "created_at") ||
		!s.archiveCoverageHasColumns(ctx, table, "id", revisionColumn, "created_at") {
		return EnrichmentCoverageMetric{}, nil
	}
	tableName := sqliteIdentifier(table)
	revisionColumnName := sqliteIdentifier(revisionColumn)
	condition := ""
	args := []any{}
	if conditionColumn != "" {
		if !s.hasColumn(ctx, table, conditionColumn) {
			return EnrichmentCoverageMetric{}, nil
		}
		condition = " and latest_child." + sqliteIdentifier(conditionColumn) + " = ?"
		args = append(args, conditionValue)
	}
	threadUpdatedAt := archiveThreadUpdatedAtExpression(s, ctx, "t")
	args = append(args, repoID)
	rows, err := s.q().QueryContext(ctx, `
		select case when child.id is null then 0 else 1 end,
			coalesce(nullif(tr.source_updated_at, ''), tr.created_at, ''),
			coalesce(child.created_at, ''),
			`+threadUpdatedAt+`
		from threads t
		left join thread_revisions tr on tr.id = (
			select latest.id
			from thread_revisions latest
			where latest.thread_id = t.id
			order by latest.id desc
			limit 1
		)
		left join `+tableName+` child on child.id = (
			select latest_child.id
			from `+tableName+` latest_child
			where latest_child.`+revisionColumnName+` = tr.id`+condition+`
			order by latest_child.id desc
			limit 1
		)
		where t.repo_id = ?
	`, args...)
	if err != nil {
		return EnrichmentCoverageMetric{}, fmt.Errorf("archive revision child coverage: %w", err)
	}
	defer rows.Close()

	metric := EnrichmentCoverageMetric{Supported: true}
	var latestObservationAt time.Time
	for rows.Next() {
		var hasChild int
		var revisionUpdatedAt, childCreatedAt, sourceUpdatedAt string
		if err := rows.Scan(&hasChild, &revisionUpdatedAt, &childCreatedAt, &sourceUpdatedAt); err != nil {
			return EnrichmentCoverageMetric{}, fmt.Errorf("scan archive revision child coverage: %w", err)
		}
		metric.Eligible++
		if hasChild == 0 {
			continue
		}
		metric.Covered++
		if parsed, ok := parseArchiveCoverageTimestamp(childCreatedAt); ok && (latestObservationAt.IsZero() || parsed.After(latestObservationAt)) {
			latestObservationAt = parsed
		}
		if archiveCoverageTimestampAtOrAfter(revisionUpdatedAt, sourceUpdatedAt) {
			metric.Fresh++
		}
	}
	if err := rows.Err(); err != nil {
		return EnrichmentCoverageMetric{}, fmt.Errorf("iterate archive revision child coverage: %w", err)
	}
	if !latestObservationAt.IsZero() {
		metric.LatestAt = formatArchiveCoverageTimestamp(latestObservationAt)
	}
	finalizeEnrichmentCoverageMetric(&metric)
	return metric, nil
}

func (s *Store) archiveClusterCoverage(ctx context.Context, repoID int64) (EnrichmentCoverageMetric, error) {
	if !s.archiveCoverageHasColumns(ctx, "cluster_runs", "id", "repo_id", "status", "finished_at") ||
		!s.archiveCoverageHasColumns(ctx, "cluster_groups", "id", "repo_id", "status") ||
		!s.archiveCoverageHasColumns(ctx, "cluster_memberships", "cluster_id", "thread_id", "state", "last_seen_run_id") {
		return EnrichmentCoverageMetric{}, nil
	}
	openThread := "t.state = 'open'"
	if s.hasColumn(ctx, "threads", "closed_at_local") {
		openThread += " and t.closed_at_local is null"
	}
	return s.scanEnrichmentCoverageMetric(ctx, `
		with latest_run as (
			select id, finished_at
			from cluster_runs
			where repo_id = ? and status in ('success', 'completed') and finished_at is not null
			order by id desc
			limit 1
		)
		select count(*),
			coalesce(sum(case when exists(
				select 1
				from cluster_memberships cm
				join cluster_groups cg on cg.id = cm.cluster_id
				where cm.thread_id = t.id and cm.state = 'active' and cg.repo_id = t.repo_id and cg.status = 'active'
			) then 1 else 0 end), 0),
			coalesce(sum(case when exists(
				select 1
				from cluster_memberships cm
				join cluster_groups cg on cg.id = cm.cluster_id
				where cm.thread_id = t.id and cm.state = 'active' and cg.repo_id = t.repo_id and cg.status = 'active'
				  and cm.last_seen_run_id = (select id from latest_run)
			) then 1 else 0 end), 0),
			coalesce((select finished_at from latest_run), '')
		from threads t
		where t.repo_id = ? and `+openThread+`
	`, repoID, repoID)
}

func (s *Store) archivePRDetailCoverage(ctx context.Context, repoID int64) (EnrichmentCoverageMetric, error) {
	if !s.archiveCoverageHasColumns(ctx, "pull_request_details", "thread_id", "fetched_at") {
		return EnrichmentCoverageMetric{}, nil
	}
	threadUpdatedAt := archiveThreadUpdatedAtExpression(s, ctx, "t")
	rows, err := s.q().QueryContext(ctx, `
		select case when prd.thread_id is null then 0 else 1 end,
			coalesce(prd.fetched_at, ''),
			`+threadUpdatedAt+`
		from threads t
		left join pull_request_details prd on prd.thread_id = t.id
		where t.repo_id = ? and t.kind = 'pull_request'
	`, repoID)
	if err != nil {
		return EnrichmentCoverageMetric{}, fmt.Errorf("archive PR detail coverage: %w", err)
	}
	defer rows.Close()

	metric := EnrichmentCoverageMetric{Supported: true}
	var latestFetchedAt time.Time
	for rows.Next() {
		var hasDetail int
		var fetchedAt, sourceUpdatedAt string
		if err := rows.Scan(&hasDetail, &fetchedAt, &sourceUpdatedAt); err != nil {
			return EnrichmentCoverageMetric{}, fmt.Errorf("scan archive PR detail coverage: %w", err)
		}
		metric.Eligible++
		if hasDetail == 0 {
			continue
		}
		metric.Covered++

		fetched, fetchedOK := parseArchiveCoverageTimestamp(fetchedAt)
		if fetchedOK && (latestFetchedAt.IsZero() || fetched.After(latestFetchedAt)) {
			latestFetchedAt = fetched
		}
		if fetchedOK && archiveCoverageTimestampAtOrAfter(fetchedAt, sourceUpdatedAt) {
			metric.Fresh++
		}
	}
	if err := rows.Err(); err != nil {
		return EnrichmentCoverageMetric{}, fmt.Errorf("iterate archive PR detail coverage: %w", err)
	}
	if !latestFetchedAt.IsZero() {
		metric.LatestAt = formatArchiveCoverageTimestamp(latestFetchedAt)
	}
	finalizeEnrichmentCoverageMetric(&metric)
	return metric, nil
}

func parseArchiveCoverageTimestamp(value string) (time.Time, bool) {
	parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(value))
	if err != nil {
		return time.Time{}, false
	}
	return parsed.UTC(), true
}

func formatArchiveCoverageTimestamp(value time.Time) string {
	return value.UTC().Format(archiveCoverageTimestampLayout)
}

func archiveCoverageTimestampAtOrAfter(value, baseline string) bool {
	parsedValue, ok := parseArchiveCoverageTimestamp(value)
	if !ok {
		return false
	}
	if strings.TrimSpace(baseline) == "" {
		return true
	}
	parsedBaseline, ok := parseArchiveCoverageTimestamp(baseline)
	return ok && !parsedValue.Before(parsedBaseline)
}

func archiveThreadUpdatedAtExpression(s *Store, ctx context.Context, alias string) string {
	candidates := make([]string, 0, 2)
	for _, column := range []string{"updated_at_gh", "updated_at"} {
		if s.hasColumn(ctx, "threads", column) {
			candidates = append(candidates, "nullif("+qualifiedColumn(alias, column)+", '')")
		}
	}
	if len(candidates) == 0 {
		return "''"
	}
	return "coalesce(" + strings.Join(candidates, ", ") + ", '')"
}

func (s *Store) scanEnrichmentCoverageMetric(ctx context.Context, query string, args ...any) (EnrichmentCoverageMetric, error) {
	metric := EnrichmentCoverageMetric{Supported: true}
	if err := s.q().QueryRowContext(ctx, query, args...).Scan(&metric.Eligible, &metric.Covered, &metric.Fresh, &metric.LatestAt); err != nil {
		return EnrichmentCoverageMetric{}, fmt.Errorf("archive enrichment coverage: %w", err)
	}
	finalizeEnrichmentCoverageMetric(&metric)
	return metric, nil
}

func finalizeEnrichmentCoverageMetric(metric *EnrichmentCoverageMetric) {
	if !metric.Supported {
		return
	}
	metric.Missing = max(0, metric.Eligible-metric.Covered)
	metric.Stale = max(0, metric.Covered-metric.Fresh)
	metric.CoverageRatio = enrichmentRatio(metric.Covered, metric.Eligible)
	metric.FreshnessRatio = enrichmentRatio(metric.Fresh, metric.Eligible)
	metric.Complete = metric.Missing == 0 && metric.Stale == 0
}

func enrichmentRatio(value, eligible int) float64 {
	if eligible == 0 {
		return 1
	}
	return float64(value) / float64(eligible)
}

func addEnrichmentCoverage(total *EnrichmentCoverage, row EnrichmentCoverage) {
	addEnrichmentCoverageMetric(&total.Revisions, row.Revisions)
	addEnrichmentCoverageMetric(&total.Fingerprints, row.Fingerprints)
	addEnrichmentCoverageMetric(&total.Summaries, row.Summaries)
	addEnrichmentCoverageMetric(&total.Clusters, row.Clusters)
	addEnrichmentCoverageMetric(&total.PRDetails, row.PRDetails)
}

func addEnrichmentCoverageMetric(total *EnrichmentCoverageMetric, row EnrichmentCoverageMetric) {
	total.Supported = row.Supported
	total.Eligible += row.Eligible
	total.Covered += row.Covered
	total.Fresh += row.Fresh
	rowLatest, rowLatestOK := parseArchiveCoverageTimestamp(row.LatestAt)
	totalLatest, totalLatestOK := parseArchiveCoverageTimestamp(total.LatestAt)
	if rowLatestOK && (!totalLatestOK || rowLatest.After(totalLatest)) {
		total.LatestAt = formatArchiveCoverageTimestamp(rowLatest)
	}
}

func finalizeEnrichmentCoverage(coverage *EnrichmentCoverage) {
	finalizeEnrichmentCoverageMetric(&coverage.Revisions)
	finalizeEnrichmentCoverageMetric(&coverage.Fingerprints)
	finalizeEnrichmentCoverageMetric(&coverage.Summaries)
	finalizeEnrichmentCoverageMetric(&coverage.Clusters)
	finalizeEnrichmentCoverageMetric(&coverage.PRDetails)
}
