package syncer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
	client GitHubClient
	store  *store.Store
	now    func() time.Time
}

type Options struct {
	Owner            string
	Repo             string
	State            string
	Since            string
	Limit            int
	Numbers          []int
	IncludeComments  bool
	IncludePRDetails bool
	Reporter         gh.Reporter
	Logger           *slog.Logger
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
	FinishedAt           string
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
	started := s.now().Format(time.RFC3339Nano)
	since, err := normalizeSince(options.Since, s.now())
	if err != nil {
		return Stats{}, err
	}
	state, err := normalizeState(options.State)
	if err != nil {
		return Stats{}, err
	}
	observationSequence, err := s.store.NextThreadObservationSequence(ctx, started)
	if err != nil {
		return Stats{}, err
	}
	repoRaw, err := s.client.GetRepo(ctx, options.Owner, options.Repo, options.Reporter)
	if err != nil {
		return Stats{}, err
	}
	numbers := uniquePositiveNumbers(options.Numbers)
	rows := make([]map[string]any, 0, len(numbers))
	if len(numbers) > 0 {
		for _, number := range numbers {
			row, err := s.client.GetIssue(ctx, options.Owner, options.Repo, number, options.Reporter)
			if err != nil {
				return Stats{}, err
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
	needsClosedOverlap := len(numbers) == 0 && state == "open" && since != "" && options.Limit <= 0
	if needsClosedOverlap {
		closedOverlapRows, err = s.fetchClosedOverlapRows(ctx, options, since)
		if err != nil {
			return Stats{}, err
		}
		rows = mergeIssueRows(rows, closedOverlapRows)
	}
	closedOverlapNumbers := make(map[int]struct{}, len(closedOverlapRows))
	for _, row := range closedOverlapRows {
		closedOverlapNumbers[intValue(row["number"])] = struct{}{}
	}

	payloads := make([]threadSyncPayload, 0, len(rows))
	for _, row := range rows {
		payload := threadSyncPayload{row: row}
		number := intValue(row["number"])
		kind := issueKind(row)
		if options.IncludeComments {
			commentRows, err := s.fetchCommentRows(ctx, options, kind, number)
			if err != nil {
				return Stats{}, err
			}
			payload.commentRows = commentRows
		}
		if options.IncludePRDetails && kind == "pull_request" {
			reviewThreads, reviewThreadsFetchedAt, err := s.fetchPullReviewThreadRows(ctx, options, number)
			if err != nil {
				return Stats{}, err
			}
			payload.reviewThreads = reviewThreads
			payload.reviewThreadsFetchedAt = reviewThreadsFetchedAt
			pullDetails, err := s.fetchPullRequestDetails(ctx, options, number)
			if err != nil {
				return Stats{}, err
			}
			payload.pullDetails = pullDetails
			payload.hasPullDetails = true
		}
		payloads = append(payloads, payload)
	}
	stats := Stats{
		Repository:     options.Owner + "/" + options.Repo,
		RequestedSince: since,
		Limit:          options.Limit,
		Numbers:        numbers,
		MetadataOnly:   !options.IncludeComments && !options.IncludePRDetails,
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
	var persisted syncPersistStats
	persist := func(st *store.Store) error {
		attempt := syncPersistStats{}
		repoID, err := st.UpsertRepository(ctx, store.Repository{
			Owner:        options.Owner,
			Name:         options.Repo,
			FullName:     options.Owner + "/" + options.Repo,
			GitHubRepoID: jsonID(repoRaw["id"]),
			RawJSON:      mustJSON(repoRaw),
			UpdatedAt:    s.now().Format(time.RFC3339Nano),
		})
		if err != nil {
			return err
		}
		for _, payload := range payloads {
			thread := mapIssueToThread(repoID, payload.row, s.now().Format(time.RFC3339Nano))
			_, hasIssueDraft := payload.row["draft"]
			if payload.hasPullDetails {
				thread.IsDraft = boolValue(payload.pullDetails.pull["draft"])
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
				attempt.ThreadsSkippedStale++
				tracker.Add(1,
					"number", thread.Number,
					"kind", thread.Kind,
					"thread_state", thread.State,
					"result", "stale_skipped",
				)
				continue
			}
			thread.ID = upsert.ID
			completeEvidence := hasFreshThreadEvidence(options, thread)
			evidenceApplied := completeEvidence && upsert.EvidenceApplied
			childWritesAllowed := !completeEvidence || evidenceApplied
			if upsert.Applied {
				if _, overlap := closedOverlapNumbers[thread.Number]; overlap &&
					upsert.PreviousState == "open" &&
					thread.State == "closed" {
					attempt.ThreadsClosed++
				}
			}
			var comments []store.Comment
			if options.IncludeComments && childWritesAllowed {
				comments, err = persistComments(ctx, st, thread, payload.commentRows)
				if err != nil {
					return err
				}
				attempt.CommentsSynced += len(comments)
			} else {
				var err error
				comments, err = st.ListComments(ctx, thread.ID)
				if err != nil {
					return err
				}
			}
			if options.IncludePRDetails && thread.Kind == "pull_request" && childWritesAllowed {
				count, err := s.persistPullReviewThreads(ctx, st, thread, payload.reviewThreads, payload.reviewThreadsFetchedAt)
				if err != nil {
					return err
				}
				attempt.ReviewThreadsSynced += count
				if payload.hasPullDetails {
					detailStats, err := s.persistPullRequestDetails(ctx, st, thread, payload.pullDetails)
					if err != nil {
						return err
					}
					attempt.PRDetailsSynced++
					attempt.PRFilesSynced += detailStats.files
					attempt.PRCommitsSynced += detailStats.commits
					attempt.PRChecksSynced += detailStats.checks
					attempt.WorkflowRunsSynced += detailStats.runs
				}
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
			tracker.Add(1,
				"number", thread.Number,
				"kind", thread.Kind,
				"thread_state", thread.State,
			)
		}
		if attempt.ThreadsClosed > 0 {
			options.Reporter.Printf(
				"[sync] closed overlap sweep matched %d stale open thread(s)",
				attempt.ThreadsClosed,
			)
		}
		attempt.FinishedAt = s.now().Format(time.RFC3339Nano)
		runStats := stats
		applySyncPersistStats(&runStats, attempt)
		if _, err := st.RecordRun(ctx, store.RunRecord{
			RepoID:     repoID,
			Kind:       "sync",
			Scope:      syncRunScope(state, numbers),
			Status:     "success",
			StartedAt:  runStats.StartedAt,
			FinishedAt: runStats.FinishedAt,
			StatsJSON:  mustJSON(runStats),
		}); err != nil {
			return err
		}
		persisted = attempt
		return nil
	}
	if err := s.store.WithTx(ctx, persist); err != nil {
		tracker.Finish(err)
		return Stats{}, err
	}
	applySyncPersistStats(&stats, persisted)
	tracker.Finish(nil)
	return stats, nil
}

func applySyncPersistStats(stats *Stats, persisted syncPersistStats) {
	stats.ThreadsSynced = persisted.ThreadsSynced
	stats.IssuesSynced = persisted.IssuesSynced
	stats.PullRequestsSynced = persisted.PullRequestsSynced
	stats.CommentsSynced = persisted.CommentsSynced
	stats.ReviewThreadsSynced = persisted.ReviewThreadsSynced
	stats.PRDetailsSynced = persisted.PRDetailsSynced
	stats.PRFilesSynced = persisted.PRFilesSynced
	stats.PRCommitsSynced = persisted.PRCommitsSynced
	stats.PRChecksSynced = persisted.PRChecksSynced
	stats.WorkflowRunsSynced = persisted.WorkflowRunsSynced
	stats.EvidenceObserved = persisted.EvidenceObserved
	stats.RevisionsCreated = persisted.RevisionsCreated
	stats.FingerprintsUpserted = persisted.FingerprintsUpserted
	stats.ThreadsClosed = persisted.ThreadsClosed
	stats.ThreadsSkippedStale = persisted.ThreadsSkippedStale
	stats.FinishedAt = persisted.FinishedAt
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

func (s *Syncer) fetchCommentRows(ctx context.Context, options Options, threadKind string, number int) ([]commentRow, error) {
	var rows []commentRow
	issueComments, err := s.client.ListIssueComments(ctx, options.Owner, options.Repo, number, options.Reporter)
	if err != nil {
		return nil, err
	}
	for _, row := range issueComments {
		rows = append(rows, commentRow{kind: "issue_comment", raw: row})
	}
	if threadKind == "pull_request" {
		reviews, err := s.client.ListPullReviews(ctx, options.Owner, options.Repo, number, options.Reporter)
		if err != nil {
			return nil, err
		}
		for _, row := range reviews {
			rows = append(rows, commentRow{kind: "pull_review", raw: row})
		}
		reviewComments, err := s.client.ListPullReviewComments(ctx, options.Owner, options.Repo, number, options.Reporter)
		if err != nil {
			return nil, err
		}
		for _, row := range reviewComments {
			rows = append(rows, commentRow{kind: "pull_review_comment", raw: row})
		}
	}
	return rows, nil
}

func persistComments(ctx context.Context, st *store.Store, thread store.Thread, rows []commentRow) ([]store.Comment, error) {
	if err := st.DeleteCommentsForThread(ctx, thread.ID); err != nil {
		return nil, err
	}
	comments := make([]store.Comment, 0, len(rows))
	for _, row := range rows {
		comment := mapComment(thread.ID, row.kind, row.raw)
		if comment.Body == "" && row.kind != "pull_review" {
			continue
		}
		if _, err := st.UpsertComment(ctx, comment); err != nil {
			return nil, err
		}
		comments = append(comments, comment)
	}
	return comments, nil
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
