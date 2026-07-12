package store

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

type WorkflowRunSnapshotState struct {
	SourceUpdatedAt     string
	ObservationSequence int64
	ReservationFound    bool
	Runs                []WorkflowRun
}

type WorkflowRunSnapshotResult struct {
	Applied    bool
	RowsSynced int
}

func (s *Store) ReadWorkflowRunSnapshotState(
	ctx context.Context,
	repoID int64,
	headSHA string,
) (WorkflowRunSnapshotState, error) {
	if repoID <= 0 {
		return WorkflowRunSnapshotState{}, fmt.Errorf("repository id must be positive")
	}
	headSHA = strings.TrimSpace(headSHA)
	if headSHA == "" {
		return WorkflowRunSnapshotState{}, fmt.Errorf("head SHA must not be empty")
	}
	if s.queries == nil {
		var state WorkflowRunSnapshotState
		err := s.WithTx(ctx, func(tx *Store) error {
			var err error
			state, err = tx.ReadWorkflowRunSnapshotState(ctx, repoID, headSHA)
			return err
		})
		return state, err
	}

	sourceUpdatedAt, sequence, found, err := s.WorkflowRunObservationReservation(
		ctx,
		repoID,
		headSHA,
	)
	if err != nil {
		return WorkflowRunSnapshotState{}, err
	}
	runs, err := s.ListWorkflowRuns(ctx, repoID, WorkflowRunListOptions{
		HeadSHA: headSHA,
		Limit:   -1,
	})
	if err != nil {
		return WorkflowRunSnapshotState{}, err
	}
	for _, run := range runs {
		if _, err := workflowRunSourceTimestamp(run); err != nil {
			return WorkflowRunSnapshotState{}, fmt.Errorf(
				"validate stored workflow run %s source: %w",
				run.RunID,
				err,
			)
		}
	}
	return normalizeWorkflowRunSnapshotState(WorkflowRunSnapshotState{
		SourceUpdatedAt:     sourceUpdatedAt,
		ObservationSequence: sequence,
		ReservationFound:    found,
		Runs:                runs,
	}), nil
}

func (s *Store) ApplyWorkflowRunSnapshot(
	ctx context.Context,
	repoID int64,
	headSHA string,
	sourceUpdatedAt string,
	sequence int64,
	expected WorkflowRunSnapshotState,
	runs []WorkflowRun,
) (WorkflowRunSnapshotResult, error) {
	if repoID <= 0 {
		return WorkflowRunSnapshotResult{}, fmt.Errorf("repository id must be positive")
	}
	headSHA = strings.TrimSpace(headSHA)
	if headSHA == "" {
		return WorkflowRunSnapshotResult{}, fmt.Errorf("head SHA must not be empty")
	}
	if sequence <= 0 {
		return WorkflowRunSnapshotResult{}, fmt.Errorf("observation sequence must be positive")
	}
	normalizedRuns, err := validateWorkflowRunSnapshot(repoID, headSHA, runs)
	if err != nil {
		return WorkflowRunSnapshotResult{}, err
	}
	sourceUpdatedAt = strings.TrimSpace(sourceUpdatedAt)
	if sourceUpdatedAt == "" && len(normalizedRuns) > 0 {
		return WorkflowRunSnapshotResult{}, fmt.Errorf(
			"workflow snapshot source must not be empty when runs are present",
		)
	}
	if sourceUpdatedAt != "" {
		if _, ok := timestampOrderKey(sourceUpdatedAt); !ok {
			return WorkflowRunSnapshotResult{}, fmt.Errorf(
				"workflow snapshot source is invalid: %q",
				sourceUpdatedAt,
			)
		}
	}
	expected = normalizeWorkflowRunSnapshotState(expected)
	if s.queries == nil {
		var result WorkflowRunSnapshotResult
		err := s.WithTx(ctx, func(tx *Store) error {
			var err error
			result, err = tx.ApplyWorkflowRunSnapshot(
				ctx,
				repoID,
				headSHA,
				sourceUpdatedAt,
				sequence,
				expected,
				normalizedRuns,
			)
			return err
		})
		return result, err
	}

	current, err := s.ReadWorkflowRunSnapshotState(ctx, repoID, headSHA)
	if err != nil {
		return WorkflowRunSnapshotResult{}, err
	}
	incomingOrder := observationOrder{
		SourceUpdatedAt:     sourceUpdatedAt,
		ObservationSequence: sequence,
	}
	order := 1
	if current.ReservationFound {
		order, err = compareWorkflowObservationOrder(
			incomingOrder,
			observationOrder{
				SourceUpdatedAt:     current.SourceUpdatedAt,
				ObservationSequence: current.ObservationSequence,
			},
		)
		if err != nil {
			return WorkflowRunSnapshotResult{}, fmt.Errorf(
				"compare workflow run observation for %s order: %w",
				headSHA,
				err,
			)
		}
	}

	if !workflowRunSnapshotStatesEqual(expected, current) {
		if !current.ReservationFound || order != 0 {
			return WorkflowRunSnapshotResult{}, nil
		}
		return s.mergeEqualWorkflowRunSnapshot(ctx, current, normalizedRuns)
	}
	if current.ReservationFound {
		switch {
		case order < 0:
			return WorkflowRunSnapshotResult{}, nil
		case order == 0:
			return s.mergeEqualWorkflowRunSnapshot(ctx, current, normalizedRuns)
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
		return WorkflowRunSnapshotResult{}, fmt.Errorf(
			"reserve workflow run observation for %s: %w",
			headSHA,
			err,
		)
	}
	if err := s.replaceWorkflowRuns(ctx, repoID, headSHA, normalizedRuns); err != nil {
		return WorkflowRunSnapshotResult{}, err
	}
	return WorkflowRunSnapshotResult{
		Applied:    true,
		RowsSynced: len(normalizedRuns),
	}, nil
}

func (s *Store) mergeEqualWorkflowRunSnapshot(
	ctx context.Context,
	current WorkflowRunSnapshotState,
	incoming []WorkflowRun,
) (WorkflowRunSnapshotResult, error) {
	currentByID := make(map[string]WorkflowRun, len(current.Runs))
	for _, run := range current.Runs {
		currentByID[run.RunID] = run
	}
	changed := make([]WorkflowRun, 0, len(incoming))
	for _, run := range incoming {
		existing, found := currentByID[run.RunID]
		if !found {
			changed = append(changed, run)
			continue
		}
		incomingSource, err := workflowRunSourceTimestamp(run)
		if err != nil {
			return WorkflowRunSnapshotResult{}, fmt.Errorf(
				"validate incoming workflow run %s source: %w",
				run.RunID,
				err,
			)
		}
		currentSource, err := workflowRunSourceTimestamp(existing)
		if err != nil {
			return WorkflowRunSnapshotResult{}, fmt.Errorf(
				"validate stored workflow run %s source: %w",
				run.RunID,
				err,
			)
		}
		order, err := compareObservationOrder(
			observationOrder{SourceUpdatedAt: incomingSource},
			observationOrder{SourceUpdatedAt: currentSource},
		)
		if err != nil {
			return WorkflowRunSnapshotResult{}, fmt.Errorf(
				"compare workflow run %s source: %w",
				run.RunID,
				err,
			)
		}
		switch {
		case order > 0:
			changed = append(changed, run)
		case order == 0 && !workflowRunsEquivalent(existing, run):
			return WorkflowRunSnapshotResult{}, fmt.Errorf(
				"conflicting workflow run %s observations share one snapshot tuple",
				run.RunID,
			)
		}
	}
	if len(changed) == 0 {
		return WorkflowRunSnapshotResult{}, nil
	}
	if err := s.upsertWorkflowRuns(ctx, changed); err != nil {
		return WorkflowRunSnapshotResult{}, err
	}
	return WorkflowRunSnapshotResult{
		Applied:    true,
		RowsSynced: len(changed),
	}, nil
}

func validateWorkflowRunSnapshot(
	repoID int64,
	headSHA string,
	runs []WorkflowRun,
) ([]WorkflowRun, error) {
	normalized := append([]WorkflowRun(nil), runs...)
	seen := make(map[string]struct{}, len(normalized))
	for index := range normalized {
		run := &normalized[index]
		run.RunID = strings.TrimSpace(run.RunID)
		if run.RunID == "" {
			return nil, fmt.Errorf("workflow run id must not be empty")
		}
		if _, found := seen[run.RunID]; found {
			return nil, fmt.Errorf("workflow snapshot contains duplicate run %s", run.RunID)
		}
		seen[run.RunID] = struct{}{}
		if run.RepoID != repoID {
			return nil, fmt.Errorf(
				"workflow run %s repository id = %d, want %d",
				run.RunID,
				run.RepoID,
				repoID,
			)
		}
		if strings.TrimSpace(run.HeadSHA) != headSHA {
			return nil, fmt.Errorf(
				"workflow run %s head SHA = %q, want %q",
				run.RunID,
				run.HeadSHA,
				headSHA,
			)
		}
		if _, err := workflowRunSourceTimestamp(*run); err != nil {
			return nil, fmt.Errorf("workflow run %s source: %w", run.RunID, err)
		}
	}
	sort.Slice(normalized, func(i, j int) bool {
		return normalized[i].RunID < normalized[j].RunID
	})
	return normalized, nil
}

func workflowRunSourceTimestamp(run WorkflowRun) (string, error) {
	latestValue := ""
	latestKey := ""
	for _, value := range []string{run.UpdatedAtGH, run.CreatedAtGH} {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		key, ok := timestampOrderKey(value)
		if !ok {
			return "", fmt.Errorf("invalid timestamp %q", value)
		}
		if latestValue == "" || key > latestKey {
			latestValue = value
			latestKey = key
		}
	}
	if latestValue == "" {
		return "", fmt.Errorf("missing created_at and updated_at")
	}
	return latestValue, nil
}

func normalizeWorkflowRunSnapshotState(state WorkflowRunSnapshotState) WorkflowRunSnapshotState {
	state.Runs = append([]WorkflowRun(nil), state.Runs...)
	sort.Slice(state.Runs, func(i, j int) bool {
		return state.Runs[i].RunID < state.Runs[j].RunID
	})
	return state
}

func workflowRunSnapshotStatesEqual(left, right WorkflowRunSnapshotState) bool {
	left = normalizeWorkflowRunSnapshotState(left)
	right = normalizeWorkflowRunSnapshotState(right)
	if left.SourceUpdatedAt != right.SourceUpdatedAt ||
		left.ObservationSequence != right.ObservationSequence ||
		left.ReservationFound != right.ReservationFound ||
		len(left.Runs) != len(right.Runs) {
		return false
	}
	for index := range left.Runs {
		if left.Runs[index] != right.Runs[index] {
			return false
		}
	}
	return true
}

func workflowRunsEquivalent(left, right WorkflowRun) bool {
	left.FetchedAt = ""
	right.FetchedAt = ""
	return left == right
}
