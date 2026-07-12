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

func (s *Store) applyLegacyWorkflowRunCache(
	ctx context.Context,
	detail PullRequestDetail,
	runs []WorkflowRun,
) error {
	targetHeadSHA := strings.TrimSpace(detail.HeadSHA)
	runsByHead := make(map[string][]WorkflowRun)
	if targetHeadSHA != "" {
		runsByHead[targetHeadSHA] = nil
	}
	for _, input := range runs {
		run := input
		if run.RepoID == 0 {
			run.RepoID = detail.RepoID
		}
		run.HeadSHA = strings.TrimSpace(run.HeadSHA)
		if run.HeadSHA == "" {
			if targetHeadSHA == "" {
				return fmt.Errorf(
					"legacy workflow run %s has no head SHA and pull request detail has no head SHA",
					run.RunID,
				)
			}
			run.HeadSHA = targetHeadSHA
		}
		if err := normalizeLegacyWorkflowRunSource(&run, detail); err != nil {
			return fmt.Errorf("normalize legacy workflow run %s source: %w", run.RunID, err)
		}
		runsByHead[run.HeadSHA] = append(runsByHead[run.HeadSHA], run)
	}
	if len(runsByHead) == 0 {
		return nil
	}

	sourceByHead := make(map[string]string, len(runsByHead))
	for headSHA, incoming := range runsByHead {
		sourceUpdatedAt, err := legacyWorkflowSnapshotSource(detail, incoming)
		if err != nil {
			return err
		}
		sourceByHead[headSHA] = sourceUpdatedAt
	}
	sequence, err := s.NextThreadObservationSequence(ctx, detail.FetchedAt)
	if err != nil {
		return err
	}
	headSHAs := make([]string, 0, len(runsByHead))
	for headSHA := range runsByHead {
		headSHAs = append(headSHAs, headSHA)
	}
	sort.Strings(headSHAs)
	for _, headSHA := range headSHAs {
		incoming := runsByHead[headSHA]
		expected, err := s.ReadWorkflowRunSnapshotState(ctx, detail.RepoID, headSHA)
		if err != nil {
			return err
		}
		desired := incoming
		if headSHA != targetHeadSHA {
			desired = mergeLegacyWorkflowRuns(expected.Runs, incoming)
		}
		if _, err := s.ApplyWorkflowRunSnapshot(
			ctx,
			detail.RepoID,
			headSHA,
			sourceByHead[headSHA],
			sequence,
			expected,
			desired,
		); err != nil {
			return err
		}
	}
	return nil
}

func normalizeLegacyWorkflowRunSource(
	run *WorkflowRun,
	detail PullRequestDetail,
) error {
	if strings.TrimSpace(run.UpdatedAtGH) != "" ||
		strings.TrimSpace(run.CreatedAtGH) != "" {
		_, err := workflowRunSourceTimestamp(*run)
		return err
	}
	for _, value := range []string{run.FetchedAt, detail.FetchedAt, detail.UpdatedAt} {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := timestampOrderKey(value); !ok {
			return fmt.Errorf("invalid fallback timestamp %q", value)
		}
		run.UpdatedAtGH = value
		return nil
	}
	return fmt.Errorf("missing created_at, updated_at, and fetched_at")
}

func legacyWorkflowSnapshotSource(
	detail PullRequestDetail,
	runs []WorkflowRun,
) (string, error) {
	if len(runs) > 0 {
		return workflowRunSnapshotSource(runs)
	}
	latestSource := ""
	latestKey := ""
	for _, candidate := range []struct {
		name  string
		value string
	}{
		{name: "fetched_at", value: detail.FetchedAt},
		{name: "updated_at", value: detail.UpdatedAt},
	} {
		value := strings.TrimSpace(candidate.value)
		if value == "" {
			continue
		}
		key, ok := timestampOrderKey(value)
		if !ok {
			return "", fmt.Errorf(
				"empty workflow snapshot source %s is invalid: %q",
				candidate.name,
				value,
			)
		}
		if latestSource == "" || key > latestKey {
			latestSource = value
			latestKey = key
		}
	}
	if latestSource == "" {
		return "", fmt.Errorf(
			"empty workflow snapshot source requires valid pull request fetched_at or updated_at",
		)
	}
	return latestSource, nil
}

func workflowRunSnapshotSource(runs []WorkflowRun) (string, error) {
	latestSource := ""
	latestKey := ""
	for _, run := range runs {
		source, err := workflowRunSourceTimestamp(run)
		if err != nil {
			return "", fmt.Errorf("workflow run %s source: %w", run.RunID, err)
		}
		key, _ := timestampOrderKey(source)
		if latestSource == "" || key > latestKey {
			latestSource = source
			latestKey = key
		}
	}
	return latestSource, nil
}

func mergeLegacyWorkflowRuns(current, incoming []WorkflowRun) []WorkflowRun {
	mergedByID := make(map[string]WorkflowRun, len(current)+len(incoming))
	for _, run := range current {
		mergedByID[run.RunID] = run
	}
	for _, run := range incoming {
		mergedByID[run.RunID] = run
	}
	merged := make([]WorkflowRun, 0, len(mergedByID))
	for _, run := range mergedByID {
		merged = append(merged, run)
	}
	return merged
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
