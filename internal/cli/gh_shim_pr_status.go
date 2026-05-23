package cli

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/openclaw/gitcrawl/internal/store"
)

type ghPRStatusResult struct {
	Repo               string                  `json:"repo"`
	Number             int                     `json:"number"`
	Title              string                  `json:"title"`
	URL                string                  `json:"url"`
	State              string                  `json:"state"`
	IsDraft            bool                    `json:"is_draft"`
	HeadSHA            string                  `json:"head_sha,omitempty"`
	HeadRef            string                  `json:"head_ref,omitempty"`
	MergeableState     string                  `json:"mergeable_state,omitempty"`
	Cache              ghPRStatusCache         `json:"cache"`
	Checks             ghPRStatusChecks        `json:"checks"`
	Reviews            ghPRStatusReviews       `json:"reviews"`
	ReviewThreads      ghPRStatusReviewThreads `json:"review_threads"`
	IsMergeReady       bool                    `json:"is_merge_ready"`
	BlockingReasons    []string                `json:"blocking_reasons,omitempty"`
	SuggestedNextSteps []string                `json:"suggested_next_steps,omitempty"`
}

type ghPRStatusCache struct {
	AgeSeconds             int    `json:"age_seconds,omitempty"`
	LastPulledAt           string `json:"last_pulled_at,omitempty"`
	PRDetailsFetchedAt     string `json:"pr_details_fetched_at,omitempty"`
	ReviewThreadsFetchedAt string `json:"review_threads_fetched_at,omitempty"`
	HasPRDetails           bool   `json:"has_pr_details"`
	HasReviewThreads       bool   `json:"has_review_threads"`
	ReviewThreadsKnown     bool   `json:"review_threads_known"`
}

type ghPRStatusChecks struct {
	OverallStatus string               `json:"overall_status"`
	Total         int                  `json:"total"`
	Pass          int                  `json:"pass"`
	Fail          int                  `json:"fail"`
	Pending       int                  `json:"pending"`
	Checks        []ghPRStatusCheckRun `json:"checks,omitempty"`
}

type ghPRStatusCheckRun struct {
	Name        string `json:"name"`
	Status      string `json:"status,omitempty"`
	Conclusion  string `json:"conclusion,omitempty"`
	Workflow    string `json:"workflow,omitempty"`
	DetailsURL  string `json:"details_url,omitempty"`
	CompletedAt string `json:"completed_at,omitempty"`
}

type ghPRStatusReviews struct {
	Total                 int                `json:"total"`
	Approvals             int                `json:"approvals"`
	ChangesRequested      int                `json:"changes_requested"`
	StaleChangesRequested int                `json:"stale_changes_requested"`
	Comments              int                `json:"comments"`
	Latest                []ghPRStatusReview `json:"latest,omitempty"`
}

type ghPRStatusReview struct {
	ID          string `json:"id,omitempty"`
	Author      string `json:"author,omitempty"`
	AuthorType  string `json:"author_type,omitempty"`
	State       string `json:"state,omitempty"`
	CommitID    string `json:"commit_id,omitempty"`
	IsStale     bool   `json:"is_stale,omitempty"`
	SubmittedAt string `json:"submitted_at,omitempty"`
}

type ghPRStatusReviewThreads struct {
	KnownResolution bool                     `json:"known_resolution"`
	Total           int                      `json:"total"`
	Unresolved      int                      `json:"unresolved"`
	Resolved        int                      `json:"resolved"`
	Unknown         int                      `json:"unknown"`
	Threads         []ghPRStatusReviewThread `json:"threads,omitempty"`
}

type ghPRStatusReviewThread struct {
	ID              string                    `json:"id"`
	Path            string                    `json:"path,omitempty"`
	Line            int                       `json:"line,omitempty"`
	IsResolved      bool                      `json:"is_resolved,omitempty"`
	IsOutdated      bool                      `json:"is_outdated,omitempty"`
	ResolutionKnown bool                      `json:"resolution_known"`
	Author          string                    `json:"author,omitempty"`
	IsBot           bool                      `json:"is_bot,omitempty"`
	Body            string                    `json:"body,omitempty"`
	URL             string                    `json:"url,omitempty"`
	Comments        []ghPRStatusThreadComment `json:"comments,omitempty"`
}

type ghPRStatusThreadComment struct {
	ID         string `json:"id,omitempty"`
	Author     string `json:"author,omitempty"`
	AuthorType string `json:"author_type,omitempty"`
	IsBot      bool   `json:"is_bot,omitempty"`
	Body       string `json:"body,omitempty"`
	CreatedAt  string `json:"created_at,omitempty"`
	URL        string `json:"url,omitempty"`
}

type ghPRStatusCompact struct {
	Repo                 string   `json:"repo"`
	Number               int      `json:"number"`
	Title                string   `json:"title"`
	URL                  string   `json:"url"`
	IsMergeReady         bool     `json:"is_merge_ready"`
	Checks               string   `json:"checks"`
	FailedChecks         int      `json:"failed_checks"`
	PendingChecks        int      `json:"pending_checks"`
	UnresolvedThreads    int      `json:"unresolved_threads"`
	UnknownReviewThreads int      `json:"unknown_review_threads"`
	ChangesRequested     int      `json:"changes_requested"`
	Approvals            int      `json:"approvals"`
	CacheAgeSeconds      int      `json:"cache_age_seconds,omitempty"`
	BlockingReasons      []string `json:"blocking_reasons,omitempty"`
}

func (a *App) runGHPRStatus(ctx context.Context, args []string, controls ghShimControls) error {
	fs := flag.NewFlagSet("pr status", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	repoShort := fs.String("R", "", "repository")
	repoLong := fs.String("repo", "", "repository")
	jsonFieldsRaw := fs.String("json", "", "comma-separated JSON fields")
	jqRaw := fs.String("jq", "", "jq filter")
	compact := fs.Bool("compact", false, "write compact JSON")
	solo := fs.Bool("solo", false, "skip approval requirement")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"R": true, "repo": true, "json": true, "jq": true})); err != nil {
		return usageErr(err)
	}
	if fs.NArg() != 1 {
		return localGHUnsupported(fmt.Errorf("cached gh pr status requires a number or GitHub URL"))
	}
	ref, _ := parseThreadReference(fs.Arg(0))
	number, err := parseThreadNumber(fs.Arg(0))
	if err != nil {
		return usageErr(err)
	}
	repoArg := firstNonEmpty(*repoShort, *repoLong)
	if repoArg == "" {
		repoArg = ref.FullName()
	}
	repoValue, err := a.resolveGHRepo(ctx, repoArg)
	if err != nil {
		return localGHUnsupported(err)
	}
	if controls.Live {
		if err := a.hydrateGHPRStatus(ctx, repoValue, number); err != nil {
			return err
		}
	}
	result, err := a.localGHPRStatus(ctx, repoValue, number, *solo)
	if err != nil && !controls.Cached && a.shouldAutoHydrateGHThread(err) {
		if hydrateErr := a.hydrateGHPRStatus(ctx, repoValue, number); hydrateErr != nil {
			return hydrateErr
		}
		result, err = a.localGHPRStatus(ctx, repoValue, number, *solo)
	}
	if err == nil && !controls.Cached && !controls.Live && ghPRStatusNeedsHydration(result) && a.shouldAutoHydrateGHThread(nil) {
		if hydrateErr := a.hydrateGHPRStatus(ctx, repoValue, number); hydrateErr != nil {
			return hydrateErr
		}
		result, err = a.localGHPRStatus(ctx, repoValue, number, *solo)
	}
	if err != nil {
		return localGHUnsupported(err)
	}
	output := any(result)
	if *compact {
		output = compactGHPRStatus(result)
	}
	jsonFields := strings.TrimSpace(*jsonFieldsRaw)
	if jsonFields != "" {
		output = selectGHPRStatusFields(output, jsonFields)
	}
	if *compact || jsonFields != "" || strings.TrimSpace(*jqRaw) != "" || a.format == FormatJSON {
		if err := a.writeJSONValue(output, strings.TrimSpace(*jqRaw)); err != nil {
			return err
		}
	} else {
		if err := writeGHPRStatusText(a.Stdout, result); err != nil {
			return err
		}
	}
	switch ghPRStatusExitCode(result) {
	case 0:
		return nil
	case 3:
		return exitErr(3, fmt.Errorf("pull request checks pending"))
	default:
		return exitErr(1, fmt.Errorf("pull request not ready"))
	}
}

func (a *App) hydrateGHPRStatus(ctx context.Context, repoValue string, number int) error {
	owner, repoName, err := parseOwnerRepo(repoValue)
	if err != nil {
		return err
	}
	if _, err := a.syncRepository(ctx, owner, repoName, syncOptions{
		Numbers:          []int{number},
		IncludeComments:  true,
		IncludePRDetails: true,
		Quiet:            true,
	}); err != nil {
		return localGHUnsupported(err)
	}
	return nil
}

func (a *App) localGHPRStatus(ctx context.Context, repoValue string, number int, solo bool) (ghPRStatusResult, error) {
	owner, repoName, err := parseOwnerRepo(repoValue)
	if err != nil {
		return ghPRStatusResult{}, err
	}
	rt, err := a.openLocalRuntimeReadOnly(ctx)
	if err != nil {
		return ghPRStatusResult{}, localGHUnsupported(err)
	}
	defer rt.Store.Close()
	repo, err := rt.repository(ctx, owner, repoName)
	if err != nil {
		return ghPRStatusResult{}, localGHUnsupported(err)
	}
	threads, err := rt.Store.ListThreadsFiltered(ctx, store.ThreadListOptions{
		RepoID:        repo.ID,
		IncludeClosed: true,
		Numbers:       []int{number},
	})
	if err != nil {
		return ghPRStatusResult{}, err
	}
	var thread store.Thread
	for _, candidate := range threads {
		if candidate.Number == number && candidate.Kind == "pull_request" {
			thread = candidate
			break
		}
	}
	if thread.ID == 0 {
		return ghPRStatusResult{}, localGHUnsupported(fmt.Errorf("pull request #%d was not found in local cache", number))
	}
	cache, cacheErr := rt.Store.PullRequestCache(ctx, repo.ID, number)
	comments, err := rt.Store.ListComments(ctx, thread.ID)
	if err != nil {
		return ghPRStatusResult{}, err
	}
	cachedReviewThreads, err := rt.Store.PullRequestReviewThreads(ctx, thread.ID)
	if err != nil {
		return ghPRStatusResult{}, err
	}
	reviewThreadsFetchedAt, err := rt.Store.PullRequestReviewThreadsFetchedAt(ctx, thread.ID)
	if err != nil {
		return ghPRStatusResult{}, err
	}
	result := buildGHPRStatus(repoValue, thread, cache, cacheErr == nil, comments, cachedReviewThreads, reviewThreadsFetchedAt, solo)
	return result, nil
}

func buildGHPRStatus(repoValue string, thread store.Thread, cache store.PullRequestCache, hasCache bool, comments []store.Comment, cachedThreads []store.PullRequestReviewThread, reviewThreadsFetchedAt string, solo bool) ghPRStatusResult {
	reviewThreadsKnown := reviewThreadsFetchedAt != "" || len(cachedThreads) > 0
	result := ghPRStatusResult{
		Repo:    repoValue,
		Number:  thread.Number,
		Title:   thread.Title,
		URL:     thread.HTMLURL,
		State:   thread.State,
		IsDraft: thread.IsDraft,
		Cache: ghPRStatusCache{
			LastPulledAt:       thread.LastPulledAt,
			HasPRDetails:       hasCache,
			HasReviewThreads:   len(cachedThreads) > 0,
			ReviewThreadsKnown: reviewThreadsKnown,
		},
	}
	cacheTimes := []string{thread.LastPulledAt}
	if hasCache {
		result.HeadSHA = cache.Detail.HeadSHA
		result.HeadRef = cache.Detail.HeadRef
		result.MergeableState = cache.Detail.MergeableState
		result.Cache.PRDetailsFetchedAt = cache.Detail.FetchedAt
		cacheTimes = append(cacheTimes, cache.Detail.FetchedAt)
		result.Checks = summarizePRChecks(cache.Checks)
	} else {
		result.Checks.OverallStatus = "unknown"
	}
	if reviewThreadsFetchedAt != "" {
		result.Cache.ReviewThreadsFetchedAt = reviewThreadsFetchedAt
		cacheTimes = append(cacheTimes, reviewThreadsFetchedAt)
	} else if len(cachedThreads) > 0 {
		result.Cache.ReviewThreadsFetchedAt = cachedThreads[0].FetchedAt
		cacheTimes = append(cacheTimes, cachedThreads[0].FetchedAt)
	}
	result.Cache.AgeSeconds = cacheAgeSeconds(cacheTimes...)
	result.Reviews = summarizePRReviews(comments, result.HeadSHA)
	result.ReviewThreads = summarizePRReviewThreads(comments, cachedThreads, reviewThreadsKnown)
	result.BlockingReasons = ghPRStatusBlockingReasons(result, solo)
	result.SuggestedNextSteps = ghPRStatusNextSteps(result)
	result.IsMergeReady = len(result.BlockingReasons) == 0
	return result
}

func summarizePRChecks(checks []store.PullRequestCheck) ghPRStatusChecks {
	out := ghPRStatusChecks{OverallStatus: "pass", Total: len(checks)}
	if len(checks) == 0 {
		out.OverallStatus = "unknown"
		return out
	}
	for _, check := range checks {
		status := classifyPRCheck(check.Status, check.Conclusion)
		switch status {
		case "failure":
			out.Fail++
		case "pending":
			out.Pending++
		default:
			out.Pass++
		}
		out.Checks = append(out.Checks, ghPRStatusCheckRun{
			Name:        check.Name,
			Status:      check.Status,
			Conclusion:  check.Conclusion,
			Workflow:    check.WorkflowName,
			DetailsURL:  check.DetailsURL,
			CompletedAt: check.CompletedAt,
		})
	}
	if out.Fail > 0 {
		out.OverallStatus = "failure"
	} else if out.Pending > 0 {
		out.OverallStatus = "pending"
	}
	return out
}

func classifyPRCheck(status, conclusion string) string {
	if !strings.EqualFold(strings.TrimSpace(status), "completed") {
		return "pending"
	}
	switch strings.ToLower(strings.TrimSpace(conclusion)) {
	case "success", "neutral", "skipped":
		return "pass"
	case "failure", "timed_out", "action_required", "startup_failure", "stale", "cancelled":
		return "failure"
	default:
		return "pending"
	}
}

func summarizePRReviews(comments []store.Comment, headSHA string) ghPRStatusReviews {
	var reviews []ghPRStatusReview
	for _, comment := range comments {
		if comment.CommentType != "pull_review" {
			continue
		}
		raw := decodeRawJSON(comment.RawJSON)
		state := strings.ToUpper(firstNonEmpty(rawString(raw, "state"), rawString(raw, "event")))
		review := ghPRStatusReview{
			ID:          comment.GitHubID,
			Author:      comment.AuthorLogin,
			AuthorType:  comment.AuthorType,
			State:       state,
			CommitID:    rawString(raw, "commit_id"),
			SubmittedAt: firstNonEmpty(rawString(raw, "submitted_at"), comment.CreatedAtGitHub),
		}
		review.IsStale = headSHA != "" && review.CommitID != "" && review.CommitID != headSHA
		reviews = append(reviews, review)
	}
	sort.SliceStable(reviews, func(i, j int) bool {
		return reviews[i].SubmittedAt > reviews[j].SubmittedAt
	})
	out := ghPRStatusReviews{Total: len(reviews), Latest: reviews}
	if len(out.Latest) > 10 {
		out.Latest = out.Latest[:10]
	}
	for _, review := range reviews {
		if review.IsStale {
			if review.State == "CHANGES_REQUESTED" {
				out.StaleChangesRequested++
			}
			continue
		}
		switch review.State {
		case "COMMENTED", "COMMENT":
			out.Comments++
		}
	}
	seenDecision := map[string]bool{}
	for _, review := range reviews {
		if review.IsStale {
			continue
		}
		if review.State != "APPROVED" && review.State != "CHANGES_REQUESTED" {
			continue
		}
		key := firstNonEmpty(review.Author, review.ID)
		if key == "" || seenDecision[key] {
			continue
		}
		seenDecision[key] = true
		switch review.State {
		case "APPROVED":
			out.Approvals++
		case "CHANGES_REQUESTED":
			out.ChangesRequested++
		}
	}
	return out
}

func summarizePRReviewThreads(comments []store.Comment, cachedThreads []store.PullRequestReviewThread, knownResolution bool) ghPRStatusReviewThreads {
	if len(cachedThreads) > 0 {
		out := ghPRStatusReviewThreads{KnownResolution: true, Total: len(cachedThreads)}
		for _, thread := range cachedThreads {
			item := ghPRStatusReviewThread{
				ID:              thread.ReviewThreadID,
				Path:            thread.Path,
				Line:            thread.Line,
				IsResolved:      thread.IsResolved,
				IsOutdated:      thread.IsOutdated,
				ResolutionKnown: true,
				Author:          thread.FirstAuthorLogin,
				IsBot:           isGHBot(thread.FirstAuthorLogin, thread.FirstAuthorType),
				Body:            thread.FirstCommentBody,
				URL:             thread.FirstCommentURL,
				Comments:        decodeThreadComments(thread.CommentsJSON),
			}
			if thread.IsResolved {
				out.Resolved++
			} else {
				out.Unresolved++
			}
			out.Threads = append(out.Threads, item)
		}
		return out
	}
	if knownResolution {
		return ghPRStatusReviewThreads{KnownResolution: true}
	}
	reconstructed := reconstructReviewThreads(comments)
	return ghPRStatusReviewThreads{
		KnownResolution: false,
		Total:           len(reconstructed),
		Unknown:         len(reconstructed),
		Threads:         reconstructed,
	}
}

func reconstructReviewThreads(comments []store.Comment) []ghPRStatusReviewThread {
	byID := map[string]*ghPRStatusReviewThread{}
	var order []string
	for _, comment := range comments {
		if comment.CommentType != "pull_review_comment" && comment.CommentType != "review_comment" {
			continue
		}
		raw := decodeRawJSON(comment.RawJSON)
		parentID := rawString(raw, "in_reply_to_id")
		threadID := comment.GitHubID
		if parentID != "" && parentID != "0" {
			threadID = parentID
		}
		item, ok := byID[threadID]
		if !ok {
			item = &ghPRStatusReviewThread{
				ID:              threadID,
				Path:            rawString(raw, "path"),
				Line:            rawInt(raw, "line"),
				ResolutionKnown: false,
			}
			byID[threadID] = item
			order = append(order, threadID)
		}
		if item.Path == "" {
			item.Path = rawString(raw, "path")
		}
		if item.Line == 0 {
			item.Line = rawInt(raw, "line")
		}
		c := ghPRStatusThreadComment{
			ID:         comment.GitHubID,
			Author:     comment.AuthorLogin,
			AuthorType: comment.AuthorType,
			IsBot:      comment.IsBot,
			Body:       comment.Body,
			CreatedAt:  comment.CreatedAtGitHub,
			URL:        rawString(raw, "html_url"),
		}
		item.Comments = append(item.Comments, c)
		if item.Author == "" {
			item.Author = c.Author
			item.IsBot = c.IsBot
			item.Body = c.Body
			item.URL = c.URL
		}
	}
	out := make([]ghPRStatusReviewThread, 0, len(order))
	for _, id := range order {
		out = append(out, *byID[id])
	}
	return out
}

func decodeThreadComments(raw string) []ghPRStatusThreadComment {
	var rows []map[string]any
	if err := json.Unmarshal([]byte(raw), &rows); err != nil {
		return nil
	}
	out := make([]ghPRStatusThreadComment, 0, len(rows))
	for _, row := range rows {
		author := rawMap(row, "author")
		login := rawString(author, "login")
		authorType := firstNonEmpty(rawString(author, "__typename"), rawString(author, "type"))
		out = append(out, ghPRStatusThreadComment{
			ID:         rawString(row, "id"),
			Author:     login,
			AuthorType: authorType,
			IsBot:      isGHBot(login, authorType),
			Body:       rawString(row, "body"),
			CreatedAt:  firstNonEmpty(rawString(row, "createdAt"), rawString(row, "created_at")),
			URL:        rawString(row, "url"),
		})
	}
	return out
}

func ghPRStatusBlockingReasons(result ghPRStatusResult, solo bool) []string {
	var reasons []string
	if !strings.EqualFold(strings.TrimSpace(result.State), "open") {
		reasons = append(reasons, "not open")
	}
	if result.IsDraft {
		reasons = append(reasons, "draft")
	}
	if reason := ghPRMergeabilityBlocker(result.MergeableState); reason != "" {
		reasons = append(reasons, reason)
	}
	switch result.Checks.OverallStatus {
	case "failure":
		reasons = append(reasons, "checks failing")
	case "pending":
		reasons = append(reasons, "checks pending")
	case "unknown":
		reasons = append(reasons, "checks unknown")
	}
	if result.ReviewThreads.Unresolved > 0 {
		reasons = append(reasons, "unresolved review threads")
	}
	if result.ReviewThreads.Unknown > 0 {
		reasons = append(reasons, "review thread resolution unknown")
	}
	if result.Reviews.ChangesRequested > 0 {
		reasons = append(reasons, "changes requested")
	}
	if !solo && result.Reviews.Approvals == 0 {
		reasons = append(reasons, "no approval")
	}
	return reasons
}

func ghPRMergeabilityBlocker(state string) string {
	switch strings.ToLower(strings.TrimSpace(state)) {
	case "", "clean", "has_hooks":
		return ""
	case "dirty":
		return "merge conflicts"
	case "blocked":
		return "merge blocked"
	case "unknown", "unchecked":
		return "mergeability unknown"
	default:
		return "not mergeable"
	}
}

func ghPRStatusNeedsHydration(result ghPRStatusResult) bool {
	return !result.Cache.HasPRDetails || !result.Cache.ReviewThreadsKnown
}

func ghPRStatusNextSteps(result ghPRStatusResult) []string {
	var steps []string
	for _, reason := range result.BlockingReasons {
		switch reason {
		case "checks failing":
			steps = append(steps, "inspect failed checks")
		case "checks pending":
			steps = append(steps, "wait for checks")
		case "checks unknown":
			steps = append(steps, "hydrate PR details")
		case "merge blocked", "merge conflicts", "mergeability unknown", "not mergeable":
			steps = append(steps, "inspect mergeability")
		case "not open":
			steps = append(steps, "verify PR state")
		case "unresolved review threads", "review thread resolution unknown":
			steps = append(steps, "inspect review threads")
		case "changes requested":
			steps = append(steps, "address or dismiss stale requested changes")
		case "no approval":
			steps = append(steps, "wait for approval or retry with --solo")
		}
	}
	return uniqueStrings(steps)
}

func ghPRStatusExitCode(result ghPRStatusResult) int {
	if result.IsMergeReady {
		return 0
	}
	if result.Checks.OverallStatus == "pending" && len(result.BlockingReasons) == 1 {
		return 3
	}
	return 1
}

func compactGHPRStatus(result ghPRStatusResult) ghPRStatusCompact {
	return ghPRStatusCompact{
		Repo:                 result.Repo,
		Number:               result.Number,
		Title:                result.Title,
		URL:                  result.URL,
		IsMergeReady:         result.IsMergeReady,
		Checks:               result.Checks.OverallStatus,
		FailedChecks:         result.Checks.Fail,
		PendingChecks:        result.Checks.Pending,
		UnresolvedThreads:    result.ReviewThreads.Unresolved,
		UnknownReviewThreads: result.ReviewThreads.Unknown,
		ChangesRequested:     result.Reviews.ChangesRequested,
		Approvals:            result.Reviews.Approvals,
		CacheAgeSeconds:      result.Cache.AgeSeconds,
		BlockingReasons:      result.BlockingReasons,
	}
}

func selectGHPRStatusFields(value any, fieldsRaw string) map[string]any {
	data, _ := json.Marshal(value)
	var row map[string]any
	_ = json.Unmarshal(data, &row)
	out := make(map[string]any)
	for _, field := range parseJSONFields(fieldsRaw) {
		if value, ok := row[field]; ok {
			out[field] = value
			continue
		}
		out[field] = row[ghPRStatusFieldAlias(field)]
	}
	return out
}

func ghPRStatusFieldAlias(field string) string {
	switch field {
	case "blockingReasons":
		return "blocking_reasons"
	case "headRef":
		return "head_ref"
	case "headSha", "headSHA":
		return "head_sha"
	case "isDraft":
		return "is_draft"
	case "isMergeReady":
		return "is_merge_ready"
	case "mergeableState":
		return "mergeable_state"
	case "reviewThreads":
		return "review_threads"
	case "suggestedNextSteps":
		return "suggested_next_steps"
	default:
		return field
	}
}

func writeGHPRStatusText(w io.Writer, result ghPRStatusResult) error {
	_, err := fmt.Fprintf(w, "PR #%d %s\nstate: %s\nready: %t\nchecks: %s pass=%d fail=%d pending=%d\nreview_threads: unresolved=%d resolved=%d unknown=%d\nreviews: approvals=%d changes_requested=%d\ncache_age_seconds: %d\n",
		result.Number, result.Title, result.State, result.IsMergeReady,
		result.Checks.OverallStatus, result.Checks.Pass, result.Checks.Fail, result.Checks.Pending,
		result.ReviewThreads.Unresolved, result.ReviewThreads.Resolved, result.ReviewThreads.Unknown,
		result.Reviews.Approvals, result.Reviews.ChangesRequested,
		result.Cache.AgeSeconds)
	return err
}

func cacheAgeSeconds(values ...string) int {
	var newest time.Time
	for _, value := range values {
		if strings.TrimSpace(value) == "" {
			continue
		}
		parsed, err := time.Parse(time.RFC3339Nano, value)
		if err != nil {
			parsed, err = time.Parse(time.RFC3339, value)
		}
		if err == nil && parsed.After(newest) {
			newest = parsed
		}
	}
	if newest.IsZero() {
		return 0
	}
	age := int(time.Since(newest).Seconds())
	if age < 0 {
		return 0
	}
	return age
}

func decodeRawJSON(raw string) map[string]any {
	var out map[string]any
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return map[string]any{}
	}
	return out
}

func rawMap(row map[string]any, key string) map[string]any {
	typed, ok := row[key].(map[string]any)
	if !ok {
		return map[string]any{}
	}
	return typed
}

func rawString(row map[string]any, key string) string {
	switch typed := row[key].(type) {
	case string:
		return typed
	case float64:
		if typed == float64(int64(typed)) {
			return strconv.FormatInt(int64(typed), 10)
		}
		return strconv.FormatFloat(typed, 'f', -1, 64)
	default:
		return ""
	}
}

func rawInt(row map[string]any, key string) int {
	switch typed := row[key].(type) {
	case float64:
		return int(typed)
	case int:
		return typed
	case string:
		value, _ := strconv.Atoi(typed)
		return value
	default:
		return 0
	}
}

func rawBool(row map[string]any, key string) bool {
	switch typed := row[key].(type) {
	case bool:
		return typed
	case string:
		return strings.EqualFold(typed, "true")
	default:
		return false
	}
}

func isGHBot(login, authorType string) bool {
	return strings.EqualFold(authorType, "Bot") || strings.HasSuffix(strings.ToLower(login), "[bot]")
}
