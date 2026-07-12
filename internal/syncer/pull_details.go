package syncer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/openclaw/gitcrawl/internal/documents"
	gh "github.com/openclaw/gitcrawl/internal/github"
	"github.com/openclaw/gitcrawl/internal/store"
)

type pullDetailStats struct {
	details bool
	files   int
	commits int
	checks  int
	runs    int
}

type pullRequestDetailRows struct {
	fetchedAt                   string
	workflowSourceUpdatedAt     string
	workflowSnapshotFresh       bool
	workflowBaseline            store.WorkflowRunSnapshotState
	workflowDeletedRunIDs       []string
	workflowObservationOrder    int
	workflowObservationSequence int64
	pull                        map[string]any
	filesRaw                    []map[string]any
	commitsRaw                  []map[string]any
	checksRaw                   []map[string]any
	runsRaw                     []map[string]any
}

type workflowRunLookupClient interface {
	GetWorkflowRun(
		ctx context.Context,
		owner string,
		repo string,
		runID string,
		reporter gh.Reporter,
	) (map[string]any, error)
}

func (s *Syncer) fetchPullRequestDetails(ctx context.Context, options Options, number int) (pullRequestDetailRows, error) {
	fetchedAt := s.now().Format(time.RFC3339Nano)
	pull, err := s.client.GetPull(ctx, options.Owner, options.Repo, number, options.Reporter)
	if err != nil {
		return pullRequestDetailRows{}, err
	}
	filesRaw, err := s.client.ListPullFiles(ctx, options.Owner, options.Repo, number, options.Reporter)
	if err != nil {
		return pullRequestDetailRows{}, err
	}
	commitsRaw, err := s.client.ListPullCommits(ctx, options.Owner, options.Repo, number, options.Reporter)
	if err != nil {
		return pullRequestDetailRows{}, err
	}
	headSHA := nestedString(pull, "head", "sha")
	var checksRaw []map[string]any
	if headSHA != "" {
		checksRaw, err = s.client.ListCommitCheckRuns(ctx, options.Owner, options.Repo, headSHA, options.Reporter)
		if err != nil {
			return pullRequestDetailRows{}, err
		}
	}
	var runsRaw []map[string]any
	if headSHA != "" {
		runsRaw, err = s.client.ListWorkflowRuns(ctx, options.Owner, options.Repo, gh.ListWorkflowRunsOptions{HeadSHA: headSHA}, options.Reporter)
		if err != nil {
			return pullRequestDetailRows{}, err
		}
	}
	workflowSourceUpdatedAt, workflowSnapshotFresh, workflowBaseline, workflowDeletedRunIDs, err := s.workflowSnapshotObservation(
		ctx,
		options,
		headSHA,
		runsRaw,
	)
	if err != nil {
		return pullRequestDetailRows{}, err
	}
	workflowObservationSequence := int64(0)
	if headSHA != "" && workflowSnapshotFresh {
		workflowObservationSequence, err = s.store.NextThreadObservationSequence(
			ctx,
			s.now().Format(time.RFC3339Nano),
		)
		if err != nil {
			return pullRequestDetailRows{}, err
		}
	}
	return pullRequestDetailRows{
		fetchedAt:                   fetchedAt,
		workflowSourceUpdatedAt:     workflowSourceUpdatedAt,
		workflowSnapshotFresh:       workflowSnapshotFresh,
		workflowBaseline:            workflowBaseline,
		workflowDeletedRunIDs:       workflowDeletedRunIDs,
		workflowObservationSequence: workflowObservationSequence,
		pull:                        pull,
		filesRaw:                    filesRaw,
		commitsRaw:                  commitsRaw,
		checksRaw:                   checksRaw,
		runsRaw:                     runsRaw,
	}, nil
}

func (s *Syncer) workflowSnapshotObservation(
	ctx context.Context,
	options Options,
	headSHA string,
	rows []map[string]any,
) (
	sourceUpdatedAt string,
	fresh bool,
	baseline store.WorkflowRunSnapshotState,
	deletedRunIDs []string,
	err error,
) {
	sourceUpdatedAt, incoming, err := workflowSnapshotOrder(rows)
	if err != nil {
		return "", false, store.WorkflowRunSnapshotState{}, nil, err
	}
	if headSHA == "" {
		return sourceUpdatedAt, true, store.WorkflowRunSnapshotState{}, nil, nil
	}
	repo, err := s.store.RepositoryByFullName(ctx, options.Owner+"/"+options.Repo)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return sourceUpdatedAt, true, store.WorkflowRunSnapshotState{}, nil, nil
		}
		return "", false, store.WorkflowRunSnapshotState{}, nil, err
	}
	baseline, err = s.store.ReadWorkflowRunSnapshotState(ctx, repo.ID, headSHA)
	if err != nil {
		return "", false, store.WorkflowRunSnapshotState{}, nil, err
	}
	currentRuns := baseline.Runs
	reservationSource := baseline.SourceUpdatedAt
	found := baseline.ReservationFound
	if found {
		if _, err = latestWorkflowTimestamp(reservationSource); err != nil {
			return "", false, store.WorkflowRunSnapshotState{}, nil, fmt.Errorf(
				"validate workflow reservation source: %w",
				err,
			)
		}
	}
	currentRunIDs := make(map[string]struct{}, len(currentRuns))
	for _, current := range currentRuns {
		currentRunIDs[current.RunID] = struct{}{}
		currentSource, err := workflowRunTimestamp(current.UpdatedAtGH, current.CreatedAtGH)
		if err != nil {
			return "", false, store.WorkflowRunSnapshotState{}, nil, fmt.Errorf(
				"validate stored workflow run %s source: %w",
				current.RunID,
				err,
			)
		}
		incomingSource, present := incoming[current.RunID]
		if present {
			if workflowTimestampBefore(incomingSource, currentSource) {
				return sourceUpdatedAt, false, baseline, nil, nil
			}
			continue
		}
		lookup, ok := s.client.(workflowRunLookupClient)
		if !ok {
			if workflowTimestampBefore(sourceUpdatedAt, reservationSource) {
				return sourceUpdatedAt, false, baseline, nil, nil
			}
			return "", false, store.WorkflowRunSnapshotState{}, nil, fmt.Errorf(
				"cannot verify missing workflow run %s before replacing head %s",
				current.RunID,
				headSHA,
			)
		}
		_, lookupErr := lookup.GetWorkflowRun(
			ctx,
			options.Owner,
			options.Repo,
			current.RunID,
			options.Reporter,
		)
		var requestErr *gh.RequestError
		if lookupErr == nil {
			return sourceUpdatedAt, false, baseline, nil, nil
		}
		if !errors.As(lookupErr, &requestErr) || requestErr.Status != 404 {
			return "", false, store.WorkflowRunSnapshotState{}, nil, fmt.Errorf(
				"verify missing workflow run %s: %w",
				current.RunID,
				lookupErr,
			)
		}
		deletedRunIDs = append(deletedRunIDs, current.RunID)
		sourceUpdatedAt, err = latestWorkflowTimestamp(sourceUpdatedAt, currentSource)
		if err != nil {
			return "", false, store.WorkflowRunSnapshotState{}, nil, err
		}
	}
	for runID, incomingSource := range incoming {
		if _, present := currentRunIDs[runID]; present {
			continue
		}
		if !found || workflowTimestampBefore(reservationSource, incomingSource) {
			continue
		}
		lookup, ok := s.client.(workflowRunLookupClient)
		if !ok {
			return "", false, store.WorkflowRunSnapshotState{}, nil, fmt.Errorf(
				"cannot verify reappearing workflow run %s for head %s",
				runID,
				headSHA,
			)
		}
		exact, lookupErr := lookup.GetWorkflowRun(
			ctx,
			options.Owner,
			options.Repo,
			runID,
			options.Reporter,
		)
		var requestErr *gh.RequestError
		if errors.As(lookupErr, &requestErr) && requestErr.Status == 404 {
			return sourceUpdatedAt, false, baseline, nil, nil
		}
		if lookupErr != nil {
			return "", false, store.WorkflowRunSnapshotState{}, nil, fmt.Errorf(
				"verify reappearing workflow run %s: %w",
				runID,
				lookupErr,
			)
		}
		if exactRunID := jsonID(exact["id"]); exactRunID != runID {
			return "", false, store.WorkflowRunSnapshotState{}, nil, fmt.Errorf(
				"verify reappearing workflow run %s: exact lookup returned %s",
				runID,
				exactRunID,
			)
		}
		exactSource, err := workflowRunTimestamp(
			stringValue(exact["updated_at"]),
			stringValue(exact["created_at"]),
		)
		if err != nil {
			return "", false, store.WorkflowRunSnapshotState{}, nil, fmt.Errorf(
				"verify reappearing workflow run %s source: %w",
				runID,
				err,
			)
		}
		if workflowTimestampBefore(incomingSource, exactSource) {
			return sourceUpdatedAt, false, baseline, nil, nil
		}
	}
	if found {
		sourceUpdatedAt, err = latestWorkflowTimestamp(sourceUpdatedAt, reservationSource)
		if err != nil {
			return "", false, store.WorkflowRunSnapshotState{}, nil, err
		}
	}
	sort.Strings(deletedRunIDs)
	return sourceUpdatedAt, true, baseline, deletedRunIDs, nil
}

func (s *Syncer) consolidateWorkflowSnapshots(
	ctx context.Context,
	options Options,
	payloads []threadSyncPayload,
) error {
	type snapshotGroup struct {
		headSHA  string
		baseline store.WorkflowRunSnapshotState
		indices  []int
	}

	groups := make([]snapshotGroup, 0)
	groupIndicesByHead := make(map[string][]int)
	for index := range payloads {
		rows := &payloads[index].pullDetails
		if !payloads[index].hasPullDetails || !rows.workflowSnapshotFresh {
			continue
		}
		headSHA := nestedString(rows.pull, "head", "sha")
		if headSHA == "" {
			continue
		}
		groupIndex := -1
		for _, candidate := range groupIndicesByHead[headSHA] {
			if groups[candidate].headSHA == headSHA &&
				workflowSnapshotBaselinesEqual(groups[candidate].baseline, rows.workflowBaseline) {
				groupIndex = candidate
				break
			}
		}
		if groupIndex < 0 {
			groupIndex = len(groups)
			groups = append(groups, snapshotGroup{
				headSHA:  headSHA,
				baseline: rows.workflowBaseline,
				indices:  []int{index},
			})
			groupIndicesByHead[headSHA] = append(groupIndicesByHead[headSHA], groupIndex)
			continue
		}
		groups[groupIndex].indices = append(groups[groupIndex].indices, index)
	}

	for _, group := range groups {
		if len(group.indices) < 2 {
			continue
		}
		observationIndices := append([]int(nil), group.indices...)
		sort.SliceStable(observationIndices, func(i, j int) bool {
			left := payloads[observationIndices[i]].pullDetails.workflowObservationOrder
			right := payloads[observationIndices[j]].pullDetails.workflowObservationOrder
			if left <= 0 {
				left = observationIndices[i] + 1
			}
			if right <= 0 {
				right = observationIndices[j] + 1
			}
			return left < right
		})
		sourceUpdatedAt := ""
		rowsByID := make(map[string]map[string]any)
		deletedRunIDs := make(map[string]struct{})
		observedRunIDs := make(map[string]struct{})
		laterAbsentRunIDs := make(map[string]struct{})
		for _, index := range observationIndices {
			rows := &payloads[index].pullDetails
			var err error
			sourceUpdatedAt, err = latestWorkflowTimestamp(
				sourceUpdatedAt,
				rows.workflowSourceUpdatedAt,
			)
			if err != nil {
				return err
			}
			for _, runID := range rows.workflowDeletedRunIDs {
				if runID != "" {
					deletedRunIDs[runID] = struct{}{}
				}
			}
			presentRunIDs := make(map[string]struct{}, len(rows.runsRaw))
			for _, row := range rows.runsRaw {
				runID := jsonID(row["id"])
				if runID == "" {
					continue
				}
				presentRunIDs[runID] = struct{}{}
				existing, found := rowsByID[runID]
				if !found {
					rowsByID[runID] = row
					continue
				}
				incomingSource, err := workflowRunTimestamp(
					stringValue(row["updated_at"]),
					stringValue(row["created_at"]),
				)
				if err != nil {
					return fmt.Errorf("workflow run %s source: %w", runID, err)
				}
				existingSource, err := workflowRunTimestamp(
					stringValue(existing["updated_at"]),
					stringValue(existing["created_at"]),
				)
				if err != nil {
					return fmt.Errorf("workflow run %s source: %w", runID, err)
				}
				switch {
				case workflowTimestampBefore(existingSource, incomingSource):
					rowsByID[runID] = row
				case workflowTimestampBefore(incomingSource, existingSource):
				default:
					if mustJSON(existing) != mustJSON(row) {
						return fmt.Errorf(
							"conflicting workflow run %s observations share one sync generation",
							runID,
						)
					}
				}
			}
			for runID := range observedRunIDs {
				if _, present := presentRunIDs[runID]; !present {
					laterAbsentRunIDs[runID] = struct{}{}
				}
			}
			for runID := range presentRunIDs {
				observedRunIDs[runID] = struct{}{}
			}
		}
		for runID := range laterAbsentRunIDs {
			if _, deleted := deletedRunIDs[runID]; deleted {
				continue
			}
			deleted, err := s.verifySiblingWorkflowRunDeletion(
				ctx,
				options,
				group.headSHA,
				runID,
			)
			if err != nil {
				return err
			}
			if deleted {
				deletedRunIDs[runID] = struct{}{}
			}
		}
		for runID := range deletedRunIDs {
			delete(rowsByID, runID)
		}

		runIDs := make([]string, 0, len(rowsByID))
		for runID := range rowsByID {
			runIDs = append(runIDs, runID)
		}
		sort.Strings(runIDs)
		consolidatedRows := make([]map[string]any, 0, len(runIDs))
		for _, runID := range runIDs {
			consolidatedRows = append(consolidatedRows, rowsByID[runID])
		}
		tombstones := make([]string, 0, len(deletedRunIDs))
		for runID := range deletedRunIDs {
			tombstones = append(tombstones, runID)
		}
		sort.Strings(tombstones)
		for _, index := range group.indices {
			rows := &payloads[index].pullDetails
			rows.workflowSourceUpdatedAt = sourceUpdatedAt
			rows.workflowDeletedRunIDs = append([]string(nil), tombstones...)
			rows.runsRaw = append([]map[string]any(nil), consolidatedRows...)
		}
	}
	return nil
}

func (s *Syncer) verifySiblingWorkflowRunDeletion(
	ctx context.Context,
	options Options,
	headSHA string,
	runID string,
) (bool, error) {
	lookup, ok := s.client.(workflowRunLookupClient)
	if !ok {
		return false, fmt.Errorf(
			"cannot verify workflow run %s absent from later sibling for head %s",
			runID,
			headSHA,
		)
	}
	exact, lookupErr := lookup.GetWorkflowRun(
		ctx,
		options.Owner,
		options.Repo,
		runID,
		options.Reporter,
	)
	var requestErr *gh.RequestError
	if errors.As(lookupErr, &requestErr) && requestErr.Status == 404 {
		return true, nil
	}
	if lookupErr != nil {
		return false, fmt.Errorf(
			"verify workflow run %s absent from later sibling: %w",
			runID,
			lookupErr,
		)
	}
	if exactRunID := jsonID(exact["id"]); exactRunID != runID {
		return false, fmt.Errorf(
			"verify workflow run %s absent from later sibling: exact lookup returned %s",
			runID,
			exactRunID,
		)
	}
	if _, err := workflowRunTimestamp(
		stringValue(exact["updated_at"]),
		stringValue(exact["created_at"]),
	); err != nil {
		return false, fmt.Errorf(
			"verify workflow run %s absent from later sibling source: %w",
			runID,
			err,
		)
	}
	return false, nil
}

func workflowSnapshotBaselinesEqual(
	left store.WorkflowRunSnapshotState,
	right store.WorkflowRunSnapshotState,
) bool {
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

func (s *Syncer) persistPullRequestDetails(
	ctx context.Context,
	st *store.Store,
	thread store.Thread,
	rows pullRequestDetailRows,
	families store.PullRequestHydrationFamilies,
) (pullDetailStats, error) {
	fetchedAt := rows.fetchedAt
	if fetchedAt == "" {
		fetchedAt = s.now().Format(time.RFC3339Nano)
	}
	detail := mapPullDetail(thread, rows.pull, fetchedAt)
	files := mapPullFiles(thread.ID, rows.filesRaw, fetchedAt)
	commits := mapPullCommits(thread.ID, rows.commitsRaw, fetchedAt)
	checks := mapPullChecks(thread.ID, rows.checksRaw, fetchedAt)
	runs := mapWorkflowRuns(thread.RepoID, rows.runsRaw, fetchedAt)
	workflowRowsSynced := 0
	if families.WorkflowRuns {
		if rows.workflowObservationSequence <= 0 {
			return pullDetailStats{}, fmt.Errorf(
				"workflow observation sequence must be positive",
			)
		}
		result, err := st.ApplyWorkflowRunSnapshot(
			ctx,
			thread.RepoID,
			detail.HeadSHA,
			rows.workflowSourceUpdatedAt,
			rows.workflowObservationSequence,
			rows.workflowBaseline,
			runs,
		)
		if err != nil {
			return pullDetailStats{}, err
		}
		workflowRowsSynced = result.RowsSynced
		families.WorkflowRuns = false
	}
	if err := st.UpsertPullRequestCacheFamilies(
		ctx,
		detail,
		files,
		commits,
		checks,
		runs,
		families,
	); err != nil {
		return pullDetailStats{}, err
	}
	comments, err := st.ListComments(ctx, thread.ID)
	if err != nil {
		return pullDetailStats{}, err
	}
	storedFiles, err := st.PullRequestFiles(ctx, thread.ID)
	if err != nil {
		return pullDetailStats{}, err
	}
	storedCommits, err := st.PullRequestCommits(ctx, thread.ID)
	if err != nil {
		return pullDetailStats{}, err
	}
	if _, err := st.UpsertDocument(
		ctx,
		documents.BuildWithContext(thread, comments, storedFiles, storedCommits),
	); err != nil {
		return pullDetailStats{}, err
	}
	stats := pullDetailStats{details: families.Details}
	if families.Files {
		stats.files = len(files)
	}
	if families.Commits {
		stats.commits = len(commits)
	}
	if families.Checks {
		stats.checks = len(checks)
	}
	stats.runs = workflowRowsSynced
	return stats, nil
}

func mapPullDetail(thread store.Thread, pull map[string]any, fetchedAt string) store.PullRequestDetail {
	return store.PullRequestDetail{
		ThreadID:         thread.ID,
		RepoID:           thread.RepoID,
		Number:           thread.Number,
		BaseSHA:          nestedString(pull, "base", "sha"),
		HeadSHA:          nestedString(pull, "head", "sha"),
		HeadRef:          nestedString(pull, "head", "ref"),
		HeadRepoFullName: nestedString(pull, "head", "repo", "full_name"),
		MergeableState:   stringValue(pull["mergeable_state"]),
		Additions:        intValue(pull["additions"]),
		Deletions:        intValue(pull["deletions"]),
		ChangedFiles:     intValue(pull["changed_files"]),
		RawJSON:          mustJSON(pull),
		FetchedAt:        fetchedAt,
		UpdatedAt:        fetchedAt,
	}
}

func mapPullFiles(threadID int64, rows []map[string]any, fetchedAt string) []store.PullRequestFile {
	out := make([]store.PullRequestFile, 0, len(rows))
	for _, row := range rows {
		filename := stringValue(row["filename"])
		if filename == "" {
			continue
		}
		out = append(out, store.PullRequestFile{
			ThreadID:     threadID,
			Path:         filename,
			Status:       stringValue(row["status"]),
			Additions:    intValue(row["additions"]),
			Deletions:    intValue(row["deletions"]),
			Changes:      intValue(row["changes"]),
			PreviousPath: stringValue(row["previous_filename"]),
			Patch:        stringValue(row["patch"]),
			RawJSON:      mustJSON(row),
			FetchedAt:    fetchedAt,
		})
	}
	return out
}

func mapPullCommits(threadID int64, rows []map[string]any, fetchedAt string) []store.PullRequestCommit {
	out := make([]store.PullRequestCommit, 0, len(rows))
	for _, row := range rows {
		sha := stringValue(row["sha"])
		if sha == "" {
			continue
		}
		out = append(out, store.PullRequestCommit{
			ThreadID:    threadID,
			SHA:         sha,
			Message:     nestedString(row, "commit", "message"),
			AuthorLogin: nestedString(row, "author", "login"),
			AuthorName:  nestedString(row, "commit", "author", "name"),
			CommittedAt: nestedString(row, "commit", "author", "date"),
			HTMLURL:     stringValue(row["html_url"]),
			RawJSON:     mustJSON(row),
			FetchedAt:   fetchedAt,
		})
	}
	return out
}
