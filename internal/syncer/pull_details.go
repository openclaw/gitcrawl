package syncer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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
	fetchedAt               string
	workflowSourceUpdatedAt string
	workflowSnapshotFresh   bool
	pull                    map[string]any
	filesRaw                []map[string]any
	commitsRaw              []map[string]any
	checksRaw               []map[string]any
	runsRaw                 []map[string]any
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
	workflowSourceUpdatedAt, workflowSnapshotFresh, err := s.workflowSnapshotObservation(
		ctx,
		options,
		headSHA,
		runsRaw,
	)
	if err != nil {
		return pullRequestDetailRows{}, err
	}
	return pullRequestDetailRows{
		fetchedAt:               fetchedAt,
		workflowSourceUpdatedAt: workflowSourceUpdatedAt,
		workflowSnapshotFresh:   workflowSnapshotFresh,
		pull:                    pull,
		filesRaw:                filesRaw,
		commitsRaw:              commitsRaw,
		checksRaw:               checksRaw,
		runsRaw:                 runsRaw,
	}, nil
}

func (s *Syncer) workflowSnapshotObservation(
	ctx context.Context,
	options Options,
	headSHA string,
	rows []map[string]any,
) (sourceUpdatedAt string, fresh bool, err error) {
	sourceUpdatedAt, incoming, err := workflowSnapshotOrder(rows)
	if err != nil {
		return "", false, err
	}
	if headSHA == "" {
		return sourceUpdatedAt, true, nil
	}
	repo, err := s.store.RepositoryByFullName(ctx, options.Owner+"/"+options.Repo)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return sourceUpdatedAt, true, nil
		}
		return "", false, err
	}
	currentRuns, err := s.store.ListWorkflowRuns(ctx, repo.ID, store.WorkflowRunListOptions{
		HeadSHA: headSHA,
		Limit:   -1,
	})
	if err != nil {
		return "", false, err
	}
	reservationSource, _, found, err := s.store.WorkflowRunObservationReservation(
		ctx,
		repo.ID,
		headSHA,
	)
	if err != nil {
		return "", false, err
	}
	if found {
		if _, err = latestWorkflowTimestamp(reservationSource); err != nil {
			return "", false, fmt.Errorf("validate workflow reservation source: %w", err)
		}
	}
	for _, current := range currentRuns {
		currentSource, err := latestWorkflowTimestamp(current.UpdatedAtGH, current.CreatedAtGH)
		if err != nil {
			return "", false, fmt.Errorf(
				"validate stored workflow run %s source: %w",
				current.RunID,
				err,
			)
		}
		incomingSource, present := incoming[current.RunID]
		if present {
			if workflowTimestampBefore(incomingSource, currentSource) {
				return sourceUpdatedAt, false, nil
			}
			continue
		}
		lookup, ok := s.client.(workflowRunLookupClient)
		if !ok {
			if workflowTimestampBefore(sourceUpdatedAt, reservationSource) {
				return sourceUpdatedAt, false, nil
			}
			return "", false, fmt.Errorf(
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
			return sourceUpdatedAt, false, nil
		}
		if !errors.As(lookupErr, &requestErr) || requestErr.Status != 404 {
			return "", false, fmt.Errorf(
				"verify missing workflow run %s: %w",
				current.RunID,
				lookupErr,
			)
		}
		sourceUpdatedAt, err = latestWorkflowTimestamp(sourceUpdatedAt, currentSource)
		if err != nil {
			return "", false, err
		}
	}
	if found {
		sourceUpdatedAt, err = latestWorkflowTimestamp(sourceUpdatedAt, reservationSource)
		if err != nil {
			return "", false, err
		}
	}
	return sourceUpdatedAt, true, nil
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
	if families.WorkflowRuns {
		stats.runs = len(runs)
	}
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
