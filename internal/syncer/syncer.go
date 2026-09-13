package syncer

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/openclaw/crawlkit/progress"
	"github.com/openclaw/gitcrawl/internal/documents"
	gh "github.com/openclaw/gitcrawl/internal/github"
	"github.com/openclaw/gitcrawl/internal/store"
)

type GitHubClient interface {
	GetRepo(ctx context.Context, owner, repo string, reporter gh.Reporter) (map[string]any, error)
	GetIssue(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) (map[string]any, error)
	GetPull(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) (map[string]any, error)
	ListRepositoryIssues(ctx context.Context, owner, repo string, options gh.ListIssuesOptions, reporter gh.Reporter) ([]map[string]any, error)
	ListIssueComments(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) ([]map[string]any, error)
	ListPullReviews(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) ([]map[string]any, error)
	ListPullReviewComments(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) ([]map[string]any, error)
	ListPullReviewThreads(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) ([]map[string]any, error)
	ListPullFiles(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) ([]map[string]any, error)
	ListPullCommits(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) ([]map[string]any, error)
	ListCommitCheckRuns(ctx context.Context, owner, repo, ref string, reporter gh.Reporter) ([]map[string]any, error)
	ListWorkflowRuns(ctx context.Context, owner, repo string, options gh.ListWorkflowRunsOptions, reporter gh.Reporter) ([]map[string]any, error)
}

type Syncer struct {
	client        GitHubClient
	store         *store.Store
	now           func() time.Time
	beforePersist func()
}

type Options struct {
	Owner             string
	Repo              string
	State             string
	Since             string
	Limit             int
	Numbers           []int
	IncludeComments   bool
	IncludePRMetadata bool
	IncludePRDetails  bool
	Reporter          gh.Reporter
	Logger            *slog.Logger
	Progress          SyncProgressReporter
}

type Stats struct {
	Repository           string `json:"repository"`
	ThreadsSynced        int    `json:"threads_synced"`
	IssuesSynced         int    `json:"issues_synced"`
	PullRequestsSynced   int    `json:"pull_requests_synced"`
	CommentsSynced       int    `json:"comments_synced"`
	ReviewThreadsSynced  int    `json:"review_threads_synced"`
	PRDetailsSynced      int    `json:"pr_details_synced"`
	PRFilesSynced        int    `json:"pr_files_synced"`
	PRCommitsSynced      int    `json:"pr_commits_synced"`
	PRChecksSynced       int    `json:"pr_checks_synced"`
	WorkflowRunsSynced   int    `json:"workflow_runs_synced"`
	EvidenceObserved     int    `json:"evidence_observed"`
	RevisionsCreated     int    `json:"revisions_created"`
	FingerprintsUpserted int    `json:"fingerprints_upserted"`
	ThreadsClosed        int    `json:"threads_closed"`
	ThreadsSkippedStale  int    `json:"threads_skipped_stale"`
	RequestedSince       string `json:"requested_since,omitempty"`
	ClosedSweepThrough   string `json:"closed_sweep_through,omitempty"`
	Limit                int    `json:"limit,omitempty"`
	Numbers              []int  `json:"numbers,omitempty"`
	MetadataOnly         bool   `json:"metadata_only"`
	StartedAt            string `json:"started_at"`
	FinishedAt           string `json:"finished_at"`
}

type syncPersistStats struct {
	ThreadsSynced        int
	IssuesSynced         int
	PullRequestsSynced   int
	CommentsSynced       int
	ReviewThreadsSynced  int
	PRDetailsSynced      int
	PRFilesSynced        int
	PRCommitsSynced      int
	PRChecksSynced       int
	WorkflowRunsSynced   int
	EvidenceObserved     int
	RevisionsCreated     int
	FingerprintsUpserted int
	ThreadsClosed        int
	ThreadsSkippedStale  int
}

type threadSyncPayload struct {
	row                    map[string]any
	commentRows            []commentRow
	reviewThreads          []map[string]any
	reviewThreadsFetchedAt string
	pullDetails            pullRequestDetailRows
	hasPullDetails         bool
}

func New(client GitHubClient, st *store.Store) *Syncer {
	return &Syncer{
		client: client,
		store:  st,
		now:    func() time.Time { return time.Now().UTC() },
	}
}

func (s *Syncer) Sync(ctx context.Context, options Options) (Stats, error) {
	startedAt := s.now()
	started := startedAt.Format(time.RFC3339Nano)
	if err := reportSyncProgress(options.Progress, SyncProgress{
		Stage: SyncProgressConnecting,
	}); err != nil {
		return Stats{}, err
	}
	since, err := normalizeSince(options.Since, startedAt)
	if err != nil {
		return Stats{}, err
	}
	state, err := normalizeState(options.State)
	if err != nil {
		return Stats{}, err
	}
	repoRaw, err := s.client.GetRepo(ctx, options.Owner, options.Repo, options.Reporter)
	if err != nil {
		return Stats{}, err
	}
	if err := s.store.WithTx(ctx, func(st *store.Store) error {
		repoID, err := s.upsertRepository(ctx, st, options, repoRaw)
		if err != nil {
			return err
		}
		return st.PreserveClosedSweepWatermark(ctx, repoID, startedAt)
	}); err != nil {
		return Stats{}, err
	}
	var failures []error
	var reserveErr *gh.RateLimitReserveError
	recordFailure := func(number int, row map[string]any, cause error, operations ...string) error {
		failures = append(failures, fmt.Errorf("%s #%d: %w", strings.Join(operations, ", "), number, cause))
		errors.As(cause, &reserveErr)
		if err := s.recordSyncFailure(ctx, options, repoRaw, row, number, cause, operations...); err != nil {
			failures = append(failures, fmt.Errorf("record sync attempt failure: %w", err))
			return errors.Join(failures...)
		}
		if ctx.Err() != nil {
			return errors.Join(failures...)
		}
		return nil
	}
	numbers := uniquePositiveNumbers(options.Numbers)
	rows := make([]map[string]any, 0, len(numbers))
	if len(numbers) > 0 {
		for _, number := range numbers {
			if reserveErr != nil {
				break
			}
			row, err := s.client.GetIssue(ctx, options.Owner, options.Repo, number, options.Reporter)
			if err != nil {
				if err := recordFailure(number, nil, err, "issue"); err != nil {
					return Stats{}, err
				}
				continue
			}
			rows = append(rows, row)
		}
	} else {
		var err error
		rows, err = s.client.ListRepositoryIssues(ctx, options.Owner, options.Repo, gh.ListIssuesOptions{
			State:         state,
			Since:         since,
			Limit:         options.Limit,
			ExpectedTotal: expectedIssueTotal(repoRaw, state, since, options.Limit),
		}, options.Reporter)
		if err != nil {
			return Stats{}, err
		}
	}

	var closedOverlapRows []map[string]any
	needsClosedOverlap := len(numbers) == 0 && state == "open" && options.Limit <= 0
	if needsClosedOverlap {
		closedSince := since
		if closedSince == "" {
			watermark := startedAt.Add(-24 * time.Hour)
			repo, lookupErr := s.store.RepositoryByFullName(ctx, options.Owner+"/"+options.Repo)
			if lookupErr != nil && !errors.Is(lookupErr, sql.ErrNoRows) {
				return Stats{}, lookupErr
			}
			if lookupErr == nil {
				previous, err := s.store.ClosedSweepWatermark(ctx, repo.ID)
				if err != nil {
					return Stats{}, err
				}
				if !previous.IsZero() {
					watermark = previous
				}
			}
			if watermark.After(startedAt) {
				watermark = startedAt
			}
			closedSince = watermark.Add(-time.Minute).Format(time.RFC3339Nano)
		}
		closedOverlapRows, err = s.fetchClosedOverlapRows(ctx, options, closedSince)
		if err != nil {
			return Stats{}, err
		}
		rows = mergeIssueRows(rows, closedOverlapRows)
	}
	received := receivedThreadCounts(rows)
	if err := reportSyncProgress(options.Progress, received); err != nil {
		return Stats{}, err
	}
	closedOverlapNumbers := make(map[int]struct{}, len(closedOverlapRows))
	for _, row := range closedOverlapRows {
		closedOverlapNumbers[intValue(row["number"])] = struct{}{}
	}

	payloads := make([]threadSyncPayload, 0, len(rows))
	workflowObservationOrder := 0
	for _, row := range rows {
		payload := threadSyncPayload{row: row}
		number := intValue(row["number"])
		kind := issueKind(row)
		if reserveErr != nil && (options.IncludeComments ||
			kind == "pull_request" && (options.IncludePRMetadata || options.IncludePRDetails)) {
			continue
		}
		if options.IncludeComments {
			commentRows, operation, err := s.fetchCommentRows(ctx, options, kind, number)
			if err != nil {
				if err := recordFailure(number, row, err, operation); err != nil {
					return Stats{}, err
				}
				continue
			}
			payload.commentRows = commentRows
			received.CommentsReceived += len(commentRows)
		}
		if options.IncludePRDetails && kind == "pull_request" {
			reviewThreads, reviewThreadsFetchedAt, err := s.fetchPullReviewThreadRows(ctx, options, number)
			if err != nil {
				if err := recordFailure(number, row, err, "pull_review_threads"); err != nil {
					return Stats{}, err
				}
				continue
			}
			payload.reviewThreads = reviewThreads
			payload.reviewThreadsFetchedAt = reviewThreadsFetchedAt
			pullDetails, err := s.fetchPullRequestDetails(ctx, options, number)
			if err != nil {
				if err := recordFailure(number, row, err, "pull_request_details"); err != nil {
					return Stats{}, err
				}
				continue
			}
			workflowObservationOrder++
			pullDetails.workflowObservationOrder = workflowObservationOrder
			payload.pullDetails = pullDetails
			payload.hasPullDetails = true
		} else if options.IncludePRMetadata && kind == "pull_request" {
			pullDetails, err := s.fetchPullRequestMetadata(ctx, options, number)
			if err != nil {
				if err := recordFailure(number, row, err, "pull_request_metadata"); err != nil {
					return Stats{}, err
				}
				continue
			}
			payload.pullDetails = pullDetails
			payload.hasPullDetails = true
		}
		payloads = append(payloads, payload)
		if err := reportSyncProgress(options.Progress, received); err != nil {
			return Stats{}, err
		}
	}
	groupFailures := s.consolidateWorkflowSnapshots(ctx, options, payloads, reserveErr == nil)
	completed := payloads[:0]
	for index, payload := range payloads {
		if cause, excluded := groupFailures[index]; excluded {
			if cause != nil {
				if err := recordFailure(intValue(payload.row["number"]), payload.row, cause, "pull_request_details"); err != nil {
					return Stats{}, err
				}
			}
			continue
		}
		completed = append(completed, payload)
	}
	payloads = completed
	// Order replace-all child snapshots when their complete observations are available.
	observationSequence, err := s.store.NextThreadObservationSequence(ctx, started)
	if err != nil {
		return Stats{}, err
	}
	for index := range payloads {
		if payloads[index].hasPullDetails &&
			payloads[index].pullDetails.workflowSnapshotFresh {
			payloads[index].pullDetails.workflowObservationSequence = observationSequence
		}
	}
	if s.beforePersist != nil {
		s.beforePersist()
	}
	received.Stage = SyncProgressFinalizing
	if err := reportSyncProgress(options.Progress, received); err != nil {
		return Stats{}, err
	}
	stats := Stats{
		Repository:     options.Owner + "/" + options.Repo,
		RequestedSince: since,
		Limit:          options.Limit,
		Numbers:        numbers,
		MetadataOnly:   !options.IncludeComments && !options.IncludePRMetadata && !options.IncludePRDetails,
		StartedAt:      started,
	}
	tracker := progress.New(options.Logger, progress.Options{
		Name:  "sync",
		Unit:  "threads",
		Total: int64(len(rows)),
		Attrs: []any{
			"repository", stats.Repository,
			"state", state,
		},
	})
	// Each complete item owns its transaction, but shared-head observations above
	// retain one consolidated snapshot and generation across those transactions.
	for _, payload := range payloads {
		var attempt syncPersistStats
		err := s.store.WithTx(ctx, func(st *store.Store) error {
			// WithTx can retry a busy transaction; only its committed attempt counts.
			attempt = syncPersistStats{}
			repoID, err := s.upsertRepository(ctx, st, options, repoRaw)
			if err != nil {
				return err
			}
			thread := mapIssueToThread(repoID, payload.row, s.now().Format(time.RFC3339Nano))
			_, hasIssueDraft := payload.row["draft"]
			if payload.hasPullDetails {
				thread.IsDraft = boolValue(payload.pullDetails.pull["draft"])
			}
			workflowHeadSHA := ""
			workflowRunsEligible := false
			if options.IncludePRDetails &&
				thread.Kind == "pull_request" &&
				payload.hasPullDetails &&
				payload.pullDetails.workflowSnapshotFresh {
				workflowHeadSHA = nestedString(payload.pullDetails.pull, "head", "sha")
				workflowRunsEligible = workflowHeadSHA != ""
			}
			upsert, err := st.UpsertThreadObservation(ctx, thread, store.UpsertThreadOptions{
				PreserveDraft:       thread.Kind == "pull_request" && !payload.hasPullDetails && !hasIssueDraft,
				IncompleteEvidence:  !hasFreshThreadEvidence(options, thread),
				ObservationSequence: observationSequence,
			})
			if err != nil {
				return err
			}
			if !upsert.Applied && !upsert.EvidenceApplied {
				if workflowRunsEligible {
					count, err := s.persistWorkflowRunSnapshot(
						ctx,
						st,
						thread.RepoID,
						workflowHeadSHA,
						payload.pullDetails,
					)
					if err != nil {
						return err
					}
					attempt.WorkflowRunsSynced += count
				}
				attempt.ThreadsSkippedStale++
				return nil
			}
			thread.ID = upsert.ID
			completeEvidence := hasFreshThreadEvidence(options, thread)
			evidenceApplied := completeEvidence && upsert.EvidenceApplied
			childWritesAllowed := !completeEvidence || evidenceApplied
			childReservations := make(map[store.ThreadChildObservationFamily]bool)
			reserveChild := func(family store.ThreadChildObservationFamily) error {
				if !childWritesAllowed {
					return nil
				}
				applied, err := st.ReserveThreadChildObservation(
					ctx,
					thread.ID,
					family,
					thread.UpdatedAtGitHub,
					observationSequence,
				)
				if err != nil {
					return err
				}
				childReservations[family] = applied
				return nil
			}
			if options.IncludeComments {
				if err := reserveChild(store.ThreadChildComments); err != nil {
					return err
				}
			}
			var resolvedOperations []string
			if upsert.Applied {
				resolvedOperations = append(resolvedOperations, "issue")
			}
			if childReservations[store.ThreadChildComments] {
				resolvedOperations = append(resolvedOperations, "issue_comments")
				if thread.Kind == "pull_request" {
					resolvedOperations = append(resolvedOperations, "pull_reviews", "pull_review_comments")
				}
			}
			if payload.hasPullDetails {
				if err := reserveChild(store.ThreadChildPullRequestDetails); err != nil {
					return err
				}
			}
			if options.IncludePRDetails && thread.Kind == "pull_request" {
				for _, family := range []store.ThreadChildObservationFamily{
					store.ThreadChildPullRequestFiles,
					store.ThreadChildPullRequestCommits,
					store.ThreadChildPullRequestChecks,
					store.ThreadChildReviewThreads,
				} {
					if err := reserveChild(family); err != nil {
						return err
					}
				}
			}
			if upsert.Applied {
				if _, overlap := closedOverlapNumbers[thread.Number]; overlap &&
					upsert.PreviousState == "open" &&
					thread.State == "closed" {
					attempt.ThreadsClosed++
				}
			}
			var comments []store.Comment
			if options.IncludeComments && childReservations[store.ThreadChildComments] {
				var synced int
				comments, synced, err = persistComments(
					ctx,
					st,
					thread,
					payload.commentRows,
					observationSequence,
				)
				if err != nil {
					return err
				}
				attempt.CommentsSynced += synced
			} else {
				var err error
				comments, err = st.ListComments(ctx, thread.ID)
				if err != nil {
					return err
				}
			}
			if payload.hasPullDetails {
				if childReservations[store.ThreadChildReviewThreads] {
					count, err := s.persistPullReviewThreads(ctx, st, thread, payload.reviewThreads, payload.reviewThreadsFetchedAt)
					if err != nil {
						return err
					}
					attempt.ReviewThreadsSynced += count
					resolvedOperations = append(resolvedOperations, "pull_review_threads")
				}
				detailStats, err := s.persistPullRequestDetails(
					ctx,
					st,
					thread,
					payload.pullDetails,
					store.PullRequestHydrationFamilies{
						Details:      childReservations[store.ThreadChildPullRequestDetails],
						Files:        childReservations[store.ThreadChildPullRequestFiles],
						Commits:      childReservations[store.ThreadChildPullRequestCommits],
						Checks:       childReservations[store.ThreadChildPullRequestChecks],
						WorkflowRuns: workflowRunsEligible,
					},
				)
				if err != nil {
					return err
				}
				if detailStats.details {
					attempt.PRDetailsSynced++
					resolvedOperations = append(resolvedOperations, "pull_request_metadata")
				}
				if detailStats.details &&
					childReservations[store.ThreadChildPullRequestFiles] &&
					childReservations[store.ThreadChildPullRequestCommits] &&
					childReservations[store.ThreadChildPullRequestChecks] &&
					payload.pullDetails.workflowSnapshotFresh {
					resolvedOperations = append(resolvedOperations, "pull_request_details")
				}
				attempt.PRFilesSynced += detailStats.files
				attempt.PRCommitsSynced += detailStats.commits
				attempt.PRChecksSynced += detailStats.checks
				attempt.WorkflowRunsSynced += detailStats.runs
			}
			var pullFiles []store.PullRequestFile
			var pullCommits []store.PullRequestCommit
			if thread.Kind == "pull_request" {
				pullFiles, err = st.PullRequestFiles(ctx, thread.ID)
				if err != nil {
					return err
				}
				pullCommits, err = st.PullRequestCommits(ctx, thread.ID)
				if err != nil {
					return err
				}
			}
			if _, err := st.UpsertDocument(ctx, documents.BuildWithContext(thread, comments, pullFiles, pullCommits)); err != nil {
				return err
			}
			if evidenceApplied {
				attempt.EvidenceObserved++
				enrichment, err := persistThreadEnrichment(
					ctx,
					st,
					thread,
					comments,
					pullFiles,
					pullCommits,
					s.now().Format(time.RFC3339Nano),
					upsert.EvidenceObservationSequence,
				)
				if err != nil {
					return err
				}
				if enrichment.RevisionCreated {
					attempt.RevisionsCreated++
				}
				if enrichment.FingerprintUpserted {
					attempt.FingerprintsUpserted++
				}
			}
			attempt.ThreadsSynced++
			if thread.Kind == "pull_request" {
				attempt.PullRequestsSynced++
			} else {
				attempt.IssuesSynced++
			}
			// Resolve only observed families, atomically with their persisted data.
			// An empty operation filter would resolve every family in the store API.
			if len(resolvedOperations) > 0 {
				_, err = st.ResolveSyncAttemptFailures(ctx, repoID, thread.Number, s.now().Format(time.RFC3339Nano), resolvedOperations...)
			}
			return err
		})
		if err != nil {
			// The failed transaction is gone. Record only its existing parent,
			// under each requested family so a shallow retry cannot clear children.
			operations := requestedSyncOperations(options, issueKind(payload.row))
			if recordErr := recordFailure(intValue(payload.row["number"]), nil, err, operations...); recordErr != nil {
				break
			}
			continue
		}
		addSyncPersistStats(&stats, attempt)
		tracker.Add(1,
			"number", intValue(payload.row["number"]),
			"kind", issueKind(payload.row),
			"thread_state", stringValue(payload.row["state"]),
			"stale_skipped", attempt.ThreadsSkippedStale > 0,
		)
	}
	stats.FinishedAt = s.now().Format(time.RFC3339Nano)
	if stats.ThreadsClosed > 0 {
		options.Reporter.Printf(
			"[sync] closed overlap sweep matched %d stale open thread(s)",
			stats.ThreadsClosed,
		)
	}
	if err := errors.Join(failures...); err != nil {
		tracker.Finish(err)
		return stats, err
	}
	if len(numbers) == 0 && options.Limit <= 0 && since == "" {
		stats.ClosedSweepThrough = started
	}
	// Partial commits are useful evidence, never a successful list checkpoint.
	err = s.store.WithTx(ctx, func(st *store.Store) error {
		repoID, err := s.upsertRepository(ctx, st, options, repoRaw)
		if err != nil {
			return err
		}
		_, err = st.RecordRun(ctx, store.RunRecord{
			RepoID:     repoID,
			Kind:       "sync",
			Scope:      syncRunScope(state, numbers),
			Status:     "success",
			StartedAt:  stats.StartedAt,
			FinishedAt: stats.FinishedAt,
			StatsJSON:  mustJSON(stats),
		})
		return err
	})
	if err != nil {
		stats.ClosedSweepThrough = ""
		tracker.Finish(err)
		return stats, err
	}
	tracker.Finish(nil)
	return stats, nil
}

func receivedThreadCounts(rows []map[string]any) SyncProgress {
	result := SyncProgress{Stage: SyncProgressThreads}
	for _, row := range rows {
		if issueKind(row) == "pull_request" {
			result.PullRequestsReceived++
		} else {
			result.IssuesReceived++
		}
	}
	return result
}

func reportSyncProgress(
	reporter SyncProgressReporter,
	snapshot SyncProgress,
) error {
	if reporter == nil {
		return nil
	}
	return reporter(snapshot)
}

func (s *Syncer) upsertRepository(ctx context.Context, st *store.Store, options Options, repoRaw map[string]any) (int64, error) {
	return st.UpsertRepository(ctx, store.Repository{
		Owner:        options.Owner,
		Name:         options.Repo,
		FullName:     options.Owner + "/" + options.Repo,
		GitHubRepoID: jsonID(repoRaw["id"]),
		RawJSON:      mustJSON(repoRaw),
		UpdatedAt:    s.now().Format(time.RFC3339Nano),
	})
}

func (s *Syncer) recordSyncFailure(ctx context.Context, options Options, repoRaw, row map[string]any, number int, syncErr error, operations ...string) error {
	if syncErr == nil {
		return nil
	}
	recordCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return s.store.WithTx(recordCtx, func(st *store.Store) error {
		now := s.now().Format(time.RFC3339Nano)
		repoID, err := s.upsertRepository(recordCtx, st, options, repoRaw)
		if err != nil {
			return err
		}
		var threadID int64
		if row != nil {
			thread := mapIssueToThread(repoID, row, now)
			_, hasDraft := row["draft"]
			upsert, err := st.UpsertThreadObservation(recordCtx, thread, store.UpsertThreadOptions{
				IncompleteEvidence: true,
				PreserveDraft:      thread.Kind == "pull_request" && !hasDraft,
			})
			if err != nil {
				return err
			}
			threadID = upsert.ID
			// The parent is retained even when children fail. Resolve only its
			// applied observation, atomically with the failed-child bookkeeping.
			if upsert.Applied {
				if _, err := st.ResolveSyncAttemptFailures(recordCtx, repoID, number, now, "issue"); err != nil {
					return err
				}
			}
		} else {
			threads, err := st.ListThreadsFiltered(recordCtx, store.ThreadListOptions{
				RepoID: repoID, IncludeClosed: true, Numbers: []int{number}, Limit: 1,
			})
			if err != nil {
				return err
			}
			if len(threads) > 0 {
				threadID = threads[0].ID
			}
		}
		for _, operation := range operations {
			if _, err := st.RecordSyncAttemptFailure(recordCtx, store.SyncAttemptFailure{
				RepoID:       repoID,
				ThreadID:     threadID,
				Number:       number,
				Operation:    operation,
				ErrorClass:   syncAttemptErrorClass(syncErr),
				ErrorMessage: syncErr.Error(),
				FirstSeenAt:  now,
				LastSeenAt:   now,
			}); err != nil {
				return err
			}
		}
		return nil
	})
}

func syncAttemptErrorClass(err error) string {
	switch {
	case errors.Is(err, context.Canceled):
		return "context_canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	default:
		return "error"
	}
}

func addSyncPersistStats(stats *Stats, persisted syncPersistStats) {
	stats.ThreadsSynced += persisted.ThreadsSynced
	stats.IssuesSynced += persisted.IssuesSynced
	stats.PullRequestsSynced += persisted.PullRequestsSynced
	stats.CommentsSynced += persisted.CommentsSynced
	stats.ReviewThreadsSynced += persisted.ReviewThreadsSynced
	stats.PRDetailsSynced += persisted.PRDetailsSynced
	stats.PRFilesSynced += persisted.PRFilesSynced
	stats.PRCommitsSynced += persisted.PRCommitsSynced
	stats.PRChecksSynced += persisted.PRChecksSynced
	stats.WorkflowRunsSynced += persisted.WorkflowRunsSynced
	stats.EvidenceObserved += persisted.EvidenceObserved
	stats.RevisionsCreated += persisted.RevisionsCreated
	stats.FingerprintsUpserted += persisted.FingerprintsUpserted
	stats.ThreadsClosed += persisted.ThreadsClosed
	stats.ThreadsSkippedStale += persisted.ThreadsSkippedStale
}

func uniquePositiveNumbers(numbers []int) []int {
	if len(numbers) == 0 {
		return nil
	}
	seen := make(map[int]struct{}, len(numbers))
	out := make([]int, 0, len(numbers))
	for _, number := range numbers {
		if number <= 0 {
			continue
		}
		if _, ok := seen[number]; ok {
			continue
		}
		seen[number] = struct{}{}
		out = append(out, number)
	}
	return out
}

func mergeIssueRows(primary, overlap []map[string]any) []map[string]any {
	if len(overlap) == 0 {
		return primary
	}
	merged := append(make([]map[string]any, 0, len(primary)+len(overlap)), primary...)
	indexByNumber := make(map[int]int, len(merged))
	for index, row := range merged {
		indexByNumber[intValue(row["number"])] = index
	}
	for _, row := range overlap {
		number := intValue(row["number"])
		if index, ok := indexByNumber[number]; ok {
			merged[index] = row
			continue
		}
		indexByNumber[number] = len(merged)
		merged = append(merged, row)
	}
	return merged
}

func syncRunScope(state string, numbers []int) string {
	if len(numbers) == 0 {
		return state
	}
	parts := make([]string, 0, len(numbers))
	for _, number := range numbers {
		parts = append(parts, strconv.Itoa(number))
	}
	return "numbers:" + strings.Join(parts, ",")
}

func issueKind(row map[string]any) string {
	if _, ok := row["pull_request"]; ok {
		return "pull_request"
	}
	return "issue"
}

func normalizeState(value string) (string, error) {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" {
		return "open", nil
	}
	switch value {
	case "open", "closed", "all":
		return value, nil
	default:
		return "", fmt.Errorf("invalid state %q: use open, closed, or all", value)
	}
}

func (s *Syncer) fetchClosedOverlapRows(ctx context.Context, options Options, since string) ([]map[string]any, error) {
	return s.client.ListRepositoryIssues(ctx, options.Owner, options.Repo, gh.ListIssuesOptions{
		State: "closed",
		Since: since,
	}, options.Reporter)
}

func hasFreshThreadEvidence(options Options, thread store.Thread) bool {
	return options.IncludeComments && (thread.Kind != "pull_request" || options.IncludePRDetails)
}

func requestedSyncOperations(options Options, kind string) []string {
	operations := []string{"issue"}
	if options.IncludeComments {
		operations = append(operations, "issue_comments")
		if kind == "pull_request" {
			operations = append(operations, "pull_reviews", "pull_review_comments")
		}
	}
	if kind == "pull_request" {
		if options.IncludePRMetadata || options.IncludePRDetails {
			operations = append(operations, "pull_request_metadata")
		}
		if options.IncludePRDetails {
			operations = append(operations, "pull_review_threads", "pull_request_details")
		}
	}
	return operations
}

func normalizeSince(value string, now time.Time) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", nil
	}
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return parsed.UTC().Format(time.RFC3339Nano), nil
	}
	units := []struct {
		suffix string
		scale  time.Duration
	}{
		{"mo", 30 * 24 * time.Hour},
		{"w", 7 * 24 * time.Hour},
		{"d", 24 * time.Hour},
		{"h", time.Hour},
		{"m", time.Minute},
		{"s", time.Second},
	}
	for _, unit := range units {
		if !strings.HasSuffix(value, unit.suffix) {
			continue
		}
		raw := strings.TrimSuffix(value, unit.suffix)
		amount, err := strconv.Atoi(raw)
		if err != nil || amount <= 0 {
			return "", fmt.Errorf("invalid --since %q: expected ISO timestamp or relative duration like 15m, 2h, 7d, 1mo", value)
		}
		return now.Add(-time.Duration(amount) * unit.scale).UTC().Format(time.RFC3339Nano), nil
	}
	return "", fmt.Errorf("invalid --since %q: expected ISO timestamp or relative duration like 15m, 2h, 7d, 1mo", value)
}

func mapIssueToThread(repoID int64, row map[string]any, pulledAt string) store.Thread {
	kind := issueKind(row)
	labelsJSON := mustJSON(row["labels"])
	if labelsJSON == "null" {
		labelsJSON = "[]"
	}
	assigneesJSON := mustJSON(row["assignees"])
	if assigneesJSON == "null" {
		assigneesJSON = "[]"
	}
	title := stringValue(row["title"])
	body := stringValue(row["body"])
	return store.Thread{
		RepoID:            repoID,
		GitHubID:          jsonID(row["id"]),
		Number:            intValue(row["number"]),
		Kind:              kind,
		State:             stringValue(row["state"]),
		Title:             title,
		Body:              body,
		AuthorLogin:       loginFromUser(row["user"]),
		AuthorType:        typeFromUser(row["user"]),
		AuthorAssociation: stringValue(row["author_association"]),
		HTMLURL:           stringValue(row["html_url"]),
		LabelsJSON:        labelsJSON,
		AssigneesJSON:     assigneesJSON,
		RawJSON:           mustJSON(row),
		ContentHash:       contentHash(title, body, labelsJSON),
		IsDraft:           boolValue(row["draft"]),
		CreatedAtGitHub:   stringValue(row["created_at"]),
		UpdatedAtGitHub:   stringValue(row["updated_at"]),
		ClosedAtGitHub:    stringValue(row["closed_at"]),
		MergedAtGitHub:    stringValue(mapValue(row["pull_request"])["merged_at"]),
		FirstPulledAt:     pulledAt,
		LastPulledAt:      pulledAt,
		UpdatedAt:         pulledAt,
	}
}

func persistThreadEnrichment(
	ctx context.Context,
	st *store.Store,
	thread store.Thread,
	comments []store.Comment,
	files []store.PullRequestFile,
	commits []store.PullRequestCommit,
	createdAt string,
	observationSequence int64,
) (store.ThreadEnrichmentResult, error) {
	evidence := store.ThreadEvidence{
		Thread:              thread,
		ObservationSequence: observationSequence,
		Comments:            comments,
		Files:               files,
		Commits:             commits,
	}
	var err error
	if evidence.Comments == nil {
		evidence.Comments, err = st.ListComments(ctx, thread.ID)
		if err != nil {
			return store.ThreadEnrichmentResult{}, err
		}
	}
	if thread.Kind == "pull_request" {
		detail, ok, err := st.PullRequestDetailByThread(ctx, thread.ID)
		if err != nil {
			return store.ThreadEnrichmentResult{}, err
		}
		if ok {
			evidence.Detail = &detail
		}
		if len(evidence.Files) == 0 {
			evidence.Files, err = st.PullRequestFiles(ctx, thread.ID)
			if err != nil {
				return store.ThreadEnrichmentResult{}, err
			}
		}
		if len(evidence.Commits) == 0 {
			evidence.Commits, err = st.PullRequestCommits(ctx, thread.ID)
			if err != nil {
				return store.ThreadEnrichmentResult{}, err
			}
		}
		evidence.ReviewThreads, err = st.PullRequestReviewThreads(ctx, thread.ID)
		if err != nil {
			return store.ThreadEnrichmentResult{}, err
		}
		evidence.Checks, err = st.PullRequestChecks(ctx, thread.ID)
		if err != nil {
			return store.ThreadEnrichmentResult{}, err
		}
		headSHA := ""
		if evidence.Detail != nil {
			headSHA = evidence.Detail.HeadSHA
		}
		if headSHA != "" {
			evidence.WorkflowRuns, err = st.ListWorkflowRuns(ctx, thread.RepoID, store.WorkflowRunListOptions{
				HeadSHA: headSHA,
				Limit:   -1,
			})
			if err != nil {
				return store.ThreadEnrichmentResult{}, err
			}
		}
	}
	return st.UpsertThreadRevisionAndFingerprint(ctx, evidence, createdAt)
}

func (s *Syncer) fetchCommentRows(ctx context.Context, options Options, threadKind string, number int) ([]commentRow, string, error) {
	var rows []commentRow
	issueComments, err := s.client.ListIssueComments(ctx, options.Owner, options.Repo, number, options.Reporter)
	if err != nil {
		return nil, "issue_comments", err
	}
	for _, row := range issueComments {
		rows = append(rows, commentRow{kind: "issue_comment", raw: row})
	}
	if threadKind == "pull_request" {
		reviews, err := s.client.ListPullReviews(ctx, options.Owner, options.Repo, number, options.Reporter)
		if err != nil {
			return nil, "pull_reviews", err
		}
		for _, row := range reviews {
			rows = append(rows, commentRow{kind: "pull_review", raw: row})
		}
		reviewComments, err := s.client.ListPullReviewComments(ctx, options.Owner, options.Repo, number, options.Reporter)
		if err != nil {
			return nil, "pull_review_comments", err
		}
		for _, row := range reviewComments {
			rows = append(rows, commentRow{kind: "pull_review_comment", raw: row})
		}
	}
	return rows, "", nil
}

func persistComments(
	ctx context.Context,
	st *store.Store,
	thread store.Thread,
	rows []commentRow,
	observationSequence int64,
) ([]store.Comment, int, error) {
	synced := 0
	observedIDs := make([]int64, 0, len(rows))
	for _, row := range rows {
		comment := mapComment(thread.ID, row.kind, row.raw)
		if comment.Body == "" && row.kind != "pull_review" && comment.DeletedAt == "" {
			continue
		}
		commentID, err := st.UpsertComment(ctx, comment)
		if err != nil {
			return nil, 0, err
		}
		if comment.DeletedAt == "" {
			observedIDs = append(observedIDs, commentID)
		}
		synced++
	}
	if observationSequence > 0 {
		if err := st.ReplaceThreadChildObservationMembers(
			ctx,
			thread.ID,
			store.ThreadChildComments,
			observationSequence,
			observedIDs,
		); err != nil {
			return nil, 0, err
		}
	}
	comments, err := st.ListComments(ctx, thread.ID)
	if err != nil {
		return nil, 0, err
	}
	return comments, synced, nil
}

func (s *Syncer) fetchPullReviewThreadRows(ctx context.Context, options Options, number int) ([]map[string]any, string, error) {
	fetchedAt := s.now().Format(time.RFC3339Nano)
	rows, err := s.client.ListPullReviewThreads(ctx, options.Owner, options.Repo, number, options.Reporter)
	if err != nil {
		return nil, "", fmt.Errorf("list pull request review threads for #%d: %w", number, err)
	}
	return rows, fetchedAt, nil
}

func (s *Syncer) persistPullReviewThreads(ctx context.Context, st *store.Store, thread store.Thread, rows []map[string]any, fetchedAt string) (int, error) {
	if fetchedAt == "" {
		fetchedAt = s.now().Format(time.RFC3339Nano)
	}
	threads := make([]store.PullRequestReviewThread, 0, len(rows))
	for _, row := range rows {
		mapped := mapPullReviewThread(thread.ID, row, fetchedAt)
		if mapped.ReviewThreadID == "" {
			continue
		}
		threads = append(threads, mapped)
	}
	if err := st.UpsertPullRequestReviewThreads(ctx, thread.ID, fetchedAt, threads); err != nil {
		return 0, err
	}
	return len(threads), nil
}

func mapPullReviewThread(threadID int64, row map[string]any, fetchedAt string) store.PullRequestReviewThread {
	comments := mapAnySlice(row["comments"], "nodes")
	first := map[string]any{}
	if len(comments) > 0 {
		first = comments[0]
	}
	firstAuthor := mapValue(first["author"])
	return store.PullRequestReviewThread{
		ThreadID:              threadID,
		ReviewThreadID:        stringValue(row["id"]),
		Path:                  stringValue(row["path"]),
		Line:                  intValue(row["line"]),
		StartLine:             intValue(row["startLine"]),
		IsResolved:            boolValue(row["isResolved"]),
		IsOutdated:            boolValue(row["isOutdated"]),
		ViewerCanResolve:      boolValue(row["viewerCanResolve"]),
		ViewerCanUnresolve:    boolValue(row["viewerCanUnresolve"]),
		ViewerCanReply:        boolValue(row["viewerCanReply"]),
		FirstAuthorLogin:      stringValue(firstAuthor["login"]),
		FirstAuthorType:       stringValue(firstAuthor["__typename"]),
		FirstCommentBody:      stringValue(first["body"]),
		FirstCommentURL:       stringValue(first["url"]),
		FirstCommentCreatedAt: stringValue(first["createdAt"]),
		FirstCommentUpdatedAt: stringValue(first["updatedAt"]),
		CommentsJSON:          mustJSON(comments),
		RawJSON:               mustJSON(row),
		FetchedAt:             fetchedAt,
		DeletedAt:             stringValue(row["deleted_at"]),
		DeletionReason:        stringValue(row["deletion_reason"]),
	}
}

type commentRow struct {
	kind string
	raw  map[string]any
}

func mapComment(threadID int64, kind string, row map[string]any) store.Comment {
	authorLogin := loginFromUser(row["user"])
	authorType := typeFromUser(row["user"])
	return store.Comment{
		ThreadID:        threadID,
		GitHubID:        jsonID(row["id"]),
		CommentType:     kind,
		AuthorLogin:     authorLogin,
		AuthorType:      authorType,
		Body:            stringValue(row["body"]),
		IsBot:           isBot(authorLogin, authorType),
		ReviewState:     stringValue(row["state"]),
		RawJSON:         mustJSON(row),
		CreatedAtGitHub: stringValue(row["created_at"]),
		UpdatedAtGitHub: stringValue(row["updated_at"]),
		DeletedAt:       stringValue(row["deleted_at"]),
		DeletionReason:  stringValue(row["deletion_reason"]),
	}
}

func isBot(login, authorType string) bool {
	return strings.EqualFold(authorType, "Bot") || strings.HasSuffix(strings.ToLower(login), "[bot]")
}

func loginFromUser(value any) string {
	user, ok := value.(map[string]any)
	if !ok {
		return ""
	}
	return stringValue(user["login"])
}

func typeFromUser(value any) string {
	user, ok := value.(map[string]any)
	if !ok {
		return ""
	}
	return stringValue(user["type"])
}

func boolValue(value any) bool {
	typed, ok := value.(bool)
	return ok && typed
}

func mapValue(value any) map[string]any {
	typed, ok := value.(map[string]any)
	if !ok {
		return map[string]any{}
	}
	return typed
}

func mapAnySlice(value any, path ...string) []map[string]any {
	current := value
	for _, key := range path {
		current = mapValue(current)[key]
	}
	raw, ok := current.([]map[string]any)
	if ok {
		return raw
	}
	items, ok := current.([]any)
	if !ok {
		return nil
	}
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		if mapped, ok := item.(map[string]any); ok {
			out = append(out, mapped)
		}
	}
	return out
}

func contentHash(values ...string) string {
	hash := sha256.New()
	for _, value := range values {
		_, _ = hash.Write([]byte(value))
		_, _ = hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func mustJSON(value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		return "{}"
	}
	return string(data)
}

func jsonID(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case float64:
		return strconv.FormatInt(int64(typed), 10)
	case int:
		return strconv.Itoa(typed)
	case int64:
		return strconv.FormatInt(typed, 10)
	case json.Number:
		return typed.String()
	default:
		return ""
	}
}

func expectedIssueTotal(repoRaw map[string]any, state, since string, limit int) int {
	if state != "open" || since != "" {
		return 0
	}
	count := intValue(repoRaw["open_issues_count"])
	if count <= 0 {
		return 0
	}
	if limit > 0 && limit < count {
		return limit
	}
	return count
}

func intValue(value any) int {
	switch typed := value.(type) {
	case float64:
		return int(typed)
	case int:
		return typed
	case int64:
		return int(typed)
	case json.Number:
		parsed, _ := strconv.Atoi(typed.String())
		return parsed
	default:
		return 0
	}
}

func stringValue(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case fmt.Stringer:
		return typed.String()
	default:
		return ""
	}
}
