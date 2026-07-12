package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

type ThreadChildObservationFamily string

const (
	ThreadChildComments           ThreadChildObservationFamily = "comments"
	ThreadChildPullRequestDetails ThreadChildObservationFamily = "pull_request_details"
	ThreadChildPullRequestFiles   ThreadChildObservationFamily = "pull_request_files"
	ThreadChildPullRequestCommits ThreadChildObservationFamily = "pull_request_commits"
	ThreadChildPullRequestChecks  ThreadChildObservationFamily = "pull_request_checks"
	ThreadChildReviewThreads      ThreadChildObservationFamily = "pull_request_review_threads"
)

var threadChildObservationFamilies = []ThreadChildObservationFamily{
	ThreadChildComments,
	ThreadChildPullRequestDetails,
	ThreadChildPullRequestFiles,
	ThreadChildPullRequestCommits,
	ThreadChildPullRequestChecks,
	ThreadChildReviewThreads,
}

func (s *Store) NextThreadObservationSequence(ctx context.Context, startedAt string) (int64, error) {
	if strings.TrimSpace(startedAt) == "" {
		startedAt = time.Now().UTC().Format(timeLayout)
	}
	if _, err := s.q().ExecContext(ctx, `
		insert into thread_observation_sequence(id, value, last_started_at)
		values(1, 0, '')
		on conflict(id) do nothing
	`); err != nil {
		return 0, fmt.Errorf("initialize thread observation sequence: %w", err)
	}
	var sequence int64
	if err := s.q().QueryRowContext(ctx, `
			update thread_observation_sequence
			set value = value + 1,
				last_started_at = ?
			where id = 1
			returning value
	`, startedAt).Scan(&sequence); err != nil {
		return 0, fmt.Errorf("advance thread observation sequence at %s: %w", startedAt, err)
	}
	return sequence, nil
}

func (s *Store) ReserveThreadChildObservation(
	ctx context.Context,
	threadID int64,
	family ThreadChildObservationFamily,
	sourceUpdatedAt string,
	sequence int64,
) (bool, error) {
	if threadID <= 0 {
		return false, fmt.Errorf("thread id must be positive")
	}
	if !validThreadChildObservationFamily(family) {
		return false, fmt.Errorf("unsupported thread child observation family %q", family)
	}
	if sequence <= 0 {
		return false, fmt.Errorf("observation sequence must be positive")
	}
	if s.queries == nil {
		var applied bool
		err := s.WithTx(ctx, func(tx *Store) error {
			var err error
			applied, err = tx.ReserveThreadChildObservation(
				ctx,
				threadID,
				family,
				sourceUpdatedAt,
				sequence,
			)
			return err
		})
		return applied, err
	}
	var currentSourceUpdatedAt string
	var currentSequence int64
	err := s.q().QueryRowContext(ctx, `
		select source_updated_at, observation_sequence
		from thread_child_observation_reservations
		where thread_id = ? and family = ?
	`, threadID, family).Scan(&currentSourceUpdatedAt, &currentSequence)
	if err != nil && err != sql.ErrNoRows {
		return false, fmt.Errorf("read thread child observation %q: %w", family, err)
	}
	if err == nil {
		order, err := compareReservationObservationOrder(
			observationOrder{
				SourceUpdatedAt:     sourceUpdatedAt,
				ObservationSequence: sequence,
			},
			observationOrder{
				SourceUpdatedAt:     currentSourceUpdatedAt,
				ObservationSequence: currentSequence,
			},
		)
		if err != nil {
			return false, fmt.Errorf("compare thread child observation %q order: %w", family, err)
		}
		if order <= 0 {
			return false, nil
		}
	}
	if _, err := s.q().ExecContext(ctx, `
		insert into thread_child_observation_reservations(
			thread_id, family, source_updated_at, observation_sequence
		)
		values(?, ?, ?, ?)
		on conflict(thread_id, family) do update set
			source_updated_at = excluded.source_updated_at,
			observation_sequence = excluded.observation_sequence
	`, threadID, family, sourceUpdatedAt, sequence); err != nil {
		return false, fmt.Errorf("reserve thread child observation %q: %w", family, err)
	}
	return true, nil
}

func validThreadChildObservationFamily(family ThreadChildObservationFamily) bool {
	for _, candidate := range threadChildObservationFamilies {
		if family == candidate {
			return true
		}
	}
	return false
}

func (s *Store) WorkflowRunObservationReservation(
	ctx context.Context,
	repoID int64,
	headSHA string,
) (sourceUpdatedAt string, sequence int64, found bool, err error) {
	if repoID <= 0 {
		return "", 0, false, fmt.Errorf("repository id must be positive")
	}
	headSHA = strings.TrimSpace(headSHA)
	if headSHA == "" {
		return "", 0, false, fmt.Errorf("head SHA must not be empty")
	}
	err = s.q().QueryRowContext(ctx, `
		select source_updated_at, observation_sequence
		from workflow_run_observation_reservations
		where repo_id = ? and head_sha = ?
	`, repoID, headSHA).Scan(&sourceUpdatedAt, &sequence)
	if err == sql.ErrNoRows {
		return "", 0, false, nil
	}
	if err != nil {
		return "", 0, false, fmt.Errorf(
			"read workflow run observation for %s: %w",
			headSHA,
			err,
		)
	}
	return sourceUpdatedAt, sequence, true, nil
}

func (s *Store) ReserveWorkflowRunObservation(
	ctx context.Context,
	repoID int64,
	headSHA string,
	sourceUpdatedAt string,
	sequence int64,
) (bool, error) {
	if repoID <= 0 {
		return false, fmt.Errorf("repository id must be positive")
	}
	headSHA = strings.TrimSpace(headSHA)
	if headSHA == "" {
		return false, fmt.Errorf("head SHA must not be empty")
	}
	if sequence <= 0 {
		return false, fmt.Errorf("observation sequence must be positive")
	}
	if s.queries == nil {
		var applied bool
		err := s.WithTx(ctx, func(tx *Store) error {
			var err error
			applied, err = tx.ReserveWorkflowRunObservation(
				ctx,
				repoID,
				headSHA,
				sourceUpdatedAt,
				sequence,
			)
			return err
		})
		return applied, err
	}
	var currentSourceUpdatedAt string
	var currentSequence int64
	err := s.q().QueryRowContext(ctx, `
		select source_updated_at, observation_sequence
		from workflow_run_observation_reservations
		where repo_id = ? and head_sha = ?
	`, repoID, headSHA).Scan(&currentSourceUpdatedAt, &currentSequence)
	if err != nil && err != sql.ErrNoRows {
		return false, fmt.Errorf("read workflow run observation for %s: %w", headSHA, err)
	}
	if err == nil {
		order, err := compareWorkflowObservationOrder(
			observationOrder{
				SourceUpdatedAt:     sourceUpdatedAt,
				ObservationSequence: sequence,
			},
			observationOrder{
				SourceUpdatedAt:     currentSourceUpdatedAt,
				ObservationSequence: currentSequence,
			},
		)
		if err != nil {
			return false, fmt.Errorf("compare workflow run observation for %s order: %w", headSHA, err)
		}
		if order < 0 {
			return false, nil
		}
		if order == 0 {
			return true, nil
		}
	}
	if _, err := s.q().ExecContext(ctx, `
		insert into workflow_run_observation_reservations(
			repo_id, head_sha, source_updated_at, observation_sequence
		)
		values(?, ?, ?, ?)
		on conflict(repo_id, head_sha) do update set
			source_updated_at = excluded.source_updated_at,
			observation_sequence = excluded.observation_sequence
	`, repoID, headSHA, sourceUpdatedAt, sequence); err != nil {
		return false, fmt.Errorf("reserve workflow run observation for %s: %w", headSHA, err)
	}
	return true, nil
}

func compareReservationObservationOrder(incoming, current observationOrder) (int, error) {
	if strings.TrimSpace(current.SourceUpdatedAt) == "" && current.ObservationSequence > 0 {
		return compareObservationSequence(incoming.ObservationSequence, current.ObservationSequence), nil
	}
	return compareObservationOrder(incoming, current)
}

func compareWorkflowObservationOrder(incoming, current observationOrder) (int, error) {
	if strings.TrimSpace(incoming.SourceUpdatedAt) == "" ||
		strings.TrimSpace(current.SourceUpdatedAt) == "" {
		return compareObservationSequence(incoming.ObservationSequence, current.ObservationSequence), nil
	}
	return compareObservationOrder(incoming, current)
}

func compareObservationSequence(incoming, current int64) int {
	switch {
	case incoming < current:
		return -1
	case incoming > current:
		return 1
	default:
		return 0
	}
}
