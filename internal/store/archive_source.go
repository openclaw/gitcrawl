package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

type ArchiveObservationState string

const (
	ArchiveObservationUnsupported ArchiveObservationState = "unsupported"
	ArchiveObservationUnknown     ArchiveObservationState = "unknown"
	ArchiveObservationMissing     ArchiveObservationState = "missing"
	ArchiveObservationStale       ArchiveObservationState = "stale"
	ArchiveObservationPartial     ArchiveObservationState = "partial"
	ArchiveObservationComplete    ArchiveObservationState = "complete"
	ArchiveObservationEmpty       ArchiveObservationState = "empty"
)

type ArchiveInventoryObservation struct {
	State      ArchiveObservationState `json:"state"`
	Successful bool                    `json:"successful"`
	RunID      int64                   `json:"run_id,omitempty"`
	FinishedAt string                  `json:"finished_at,omitempty"`
}

type ArchiveChildObservations struct {
	State    ArchiveObservationState `json:"state"`
	Eligible int                     `json:"eligible"`
	Observed int                     `json:"observed"`
	Missing  int                     `json:"missing"`
	Stale    int                     `json:"stale"`
	Unknown  int                     `json:"unknown"`
}

type ArchiveRepositoryObservations struct {
	RepoID            int64                               `json:"repo_id"`
	Inventory         ArchiveInventoryObservation         `json:"inventory"`
	Children          map[string]ArchiveChildObservations `json:"children"`
	PRDetails         EnrichmentCoverageMetric            `json:"pr_details"`
	PRFiles           EnrichmentCoverageMetric            `json:"pr_files"`
	WorkflowFreshness ArchiveObservationState             `json:"workflow_freshness"`
	FailedHydrations  *int                                `json:"failed_hydrations"`
}

type ArchiveSourceAssessment struct {
	RemoteFreshness ArchiveObservationState         `json:"remote_freshness"`
	Repositories    []ArchiveRepositoryObservations `json:"repositories"`
}

// ArchiveSourceObservations describes persisted observations, not current GitHub
// state. The caller must use the same frozen database for coverage and this read.
func (s *Store) ArchiveSourceObservations(ctx context.Context, coverage ArchiveCoverage) (ArchiveSourceAssessment, error) {
	result := ArchiveSourceAssessment{
		RemoteFreshness: ArchiveObservationUnknown,
		Repositories:    make([]ArchiveRepositoryObservations, 0, len(coverage.Rows)),
	}
	for _, row := range coverage.Rows {
		inventory, err := s.archiveInventoryObservation(ctx, row.RepoID)
		if err != nil {
			return ArchiveSourceAssessment{}, err
		}
		repo := ArchiveRepositoryObservations{
			RepoID: row.RepoID, Inventory: inventory,
			Children:  make(map[string]ArchiveChildObservations),
			PRDetails: row.Enrichment.PRDetails, PRFiles: row.Enrichment.PRFiles,
			WorkflowFreshness: ArchiveObservationUnknown,
			FailedHydrations:  row.KnownFailedHydrations,
		}
		// Workflow observations are per head SHA, not a repository-wide list or
		// a thread clock. Their presence cannot prove repository freshness.
		if !s.archiveCoverageHasColumns(ctx, "workflow_run_observation_reservations", "repo_id", "head_sha", "observation_sequence") {
			repo.WorkflowFreshness = ArchiveObservationUnsupported
		}
		for _, family := range threadChildObservationFamilies {
			metric, err := s.archiveChildObservations(ctx, row.RepoID, family)
			if err != nil {
				return ArchiveSourceAssessment{}, err
			}
			repo.Children[string(family)] = metric
		}
		result.Repositories = append(result.Repositories, repo)
	}
	return result, nil
}

func (s *Store) archiveInventoryObservation(ctx context.Context, repoID int64) (ArchiveInventoryObservation, error) {
	result := ArchiveInventoryObservation{State: ArchiveObservationUnsupported}
	if !s.archiveCoverageHasColumns(ctx, "sync_runs", "id", "repo_id", "scope", "status", "started_at", "finished_at", "stats_json") {
		return result, nil
	}
	var scope, status, started, finished, encoded string
	err := s.q().QueryRowContext(ctx, `
		select id, scope, status, started_at, coalesce(finished_at, ''), coalesce(stats_json, '')
		from sync_runs where repo_id = ? order by id desc limit 1
	`, repoID).Scan(&result.RunID, &scope, &status, &started, &finished, &encoded)
	if err == sql.ErrNoRows {
		result.State = ArchiveObservationMissing
		return result, nil
	}
	if err != nil {
		return result, fmt.Errorf("read archive inventory observation: %w", err)
	}
	result.State = ArchiveObservationUnknown
	if value, ok := parseArchiveCoverageTimestamp(finished); ok {
		result.FinishedAt = formatArchiveCoverageTimestamp(value)
	}
	if status != "success" && status != "completed" {
		result.State = ArchiveObservationPartial
		return result, nil
	}
	result.Successful = true
	// Syncer.Stats records bounds and these mandatory fields at commit time.
	// Missing/sanitized stats are unknown, never an implicit unbounded run.
	var stats struct {
		StartedAt           string `json:"started_at"`
		FinishedAt          string `json:"finished_at"`
		MetadataOnly        *bool  `json:"metadata_only"`
		ThreadsSynced       *int   `json:"threads_synced"`
		ThreadsSkippedStale int    `json:"threads_skipped_stale"`
		RequestedSince      string `json:"requested_since"`
		Limit               int    `json:"limit"`
		Numbers             []int  `json:"numbers"`
	}
	if json.Unmarshal([]byte(encoded), &stats) != nil ||
		stats.MetadataOnly == nil || stats.ThreadsSynced == nil || *stats.ThreadsSynced < 0 ||
		stats.StartedAt != started || stats.FinishedAt != finished || result.FinishedAt == "" {
		return result, nil
	}
	if _, ok := parseArchiveCoverageTimestamp(started); !ok {
		return result, nil
	}
	result.State = ArchiveObservationPartial
	if scope == "all" && stats.Limit <= 0 && len(stats.Numbers) == 0 && stats.RequestedSince == "" {
		result.State = ArchiveObservationComplete
		if *stats.ThreadsSynced == 0 && stats.ThreadsSkippedStale == 0 {
			result.State = ArchiveObservationEmpty
		}
	}
	return result, nil
}

func (s *Store) archiveChildObservations(ctx context.Context, repoID int64, family ThreadChildObservationFamily) (ArchiveChildObservations, error) {
	metric := ArchiveChildObservations{State: ArchiveObservationUnsupported}
	if !s.archiveCoverageHasColumns(ctx, "thread_child_observation_reservations",
		"thread_id", "family", "source_updated_at", "observation_sequence") ||
		!s.archiveCoverageHasColumns(ctx, "threads", "updated_at_gh", "observation_sequence") {
		return metric, nil
	}
	filter := ""
	if family != ThreadChildComments {
		filter = " and t.kind = 'pull_request'"
	}
	rows, err := s.q().QueryContext(ctx, `
		select coalesce(t.updated_at_gh, ''), t.observation_sequence,
			r.thread_id is not null, coalesce(r.source_updated_at, ''), coalesce(r.observation_sequence, 0)
		from threads t
		left join thread_child_observation_reservations r on r.thread_id = t.id and r.family = ?
		where t.repo_id = ?`+filter, string(family), repoID)
	if err != nil {
		return metric, fmt.Errorf("read archive child observations: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var source, observedSource string
		var sequence, observedSequence int64
		var found bool
		if err := rows.Scan(&source, &sequence, &found, &observedSource, &observedSequence); err != nil {
			return metric, fmt.Errorf("scan archive child observations: %w", err)
		}
		metric.Eligible++
		if !found {
			metric.Missing++
			continue
		}
		metric.Observed++
		_, sourceValid := parseArchiveCoverageTimestamp(source)
		_, observedValid := parseArchiveCoverageTimestamp(observedSource)
		if (!sourceValid && strings.TrimSpace(source) != "") ||
			(!observedValid && strings.TrimSpace(observedSource) != "") ||
			observedSequence <= 0 || sequence == 0 {
			metric.Unknown++
		} else if !archiveObservationAtOrAfter(observedSource, observedSequence, source, observationSequenceOrderValue(sequence)) {
			metric.Stale++
		}
	}
	if err := rows.Err(); err != nil {
		return metric, fmt.Errorf("iterate archive child observations: %w", err)
	}
	switch {
	case metric.Eligible == 0:
		metric.State = ArchiveObservationEmpty
	case metric.Missing == metric.Eligible:
		metric.State = ArchiveObservationMissing
	case metric.Stale == metric.Eligible:
		metric.State = ArchiveObservationStale
	case metric.Unknown == metric.Eligible:
		metric.State = ArchiveObservationUnknown
	case metric.Missing+metric.Stale+metric.Unknown > 0:
		metric.State = ArchiveObservationPartial
	default:
		metric.State = ArchiveObservationComplete
	}
	return metric, nil
}
