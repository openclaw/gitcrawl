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

	"github.com/openclaw/gitcrawl/internal/documents"
	gh "github.com/openclaw/gitcrawl/internal/github"
	"github.com/openclaw/gitcrawl/internal/store"
	"github.com/vincentkoc/crawlkit/progress"
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
	Repository          string `json:"repository"`
	ThreadsSynced       int    `json:"threads_synced"`
	IssuesSynced        int    `json:"issues_synced"`
	PullRequestsSynced  int    `json:"pull_requests_synced"`
	CommentsSynced      int    `json:"comments_synced"`
	ReviewThreadsSynced int    `json:"review_threads_synced"`
	PRDetailsSynced     int    `json:"pr_details_synced"`
	PRFilesSynced       int    `json:"pr_files_synced"`
	PRCommitsSynced     int    `json:"pr_commits_synced"`
	PRChecksSynced      int    `json:"pr_checks_synced"`
	WorkflowRunsSynced  int    `json:"workflow_runs_synced"`
	ThreadsClosed       int    `json:"threads_closed"`
	RequestedSince      string `json:"requested_since,omitempty"`
	Limit               int    `json:"limit,omitempty"`
	Numbers             []int  `json:"numbers,omitempty"`
	MetadataOnly        bool   `json:"metadata_only"`
	StartedAt           string `json:"started_at"`
	FinishedAt          string `json:"finished_at"`
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
	repoRaw, err := s.client.GetRepo(ctx, options.Owner, options.Repo, options.Reporter)
	if err != nil {
		return Stats{}, err
	}
	repoID, err := s.store.UpsertRepository(ctx, store.Repository{
		Owner:        options.Owner,
		Name:         options.Repo,
		FullName:     options.Owner + "/" + options.Repo,
		GitHubRepoID: jsonID(repoRaw["id"]),
		RawJSON:      mustJSON(repoRaw),
		UpdatedAt:    s.now().Format(time.RFC3339Nano),
	})
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

	stats := Stats{
		Repository:     options.Owner + "/" + options.Repo,
		RequestedSince: since,
		Limit:          options.Limit,
		Numbers:        numbers,
		MetadataOnly:   !options.IncludeComments,
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
	persist := func(st *store.Store) error {
		for _, row := range rows {
			thread := mapIssueToThread(repoID, row, s.now().Format(time.RFC3339Nano))
			threadID, err := st.UpsertThread(ctx, thread)
			if err != nil {
				return err
			}
			thread.ID = threadID
			var comments []store.Comment
			if options.IncludeComments {
				var err error
				comments, err = s.syncComments(ctx, options, thread)
				if err != nil {
					return err
				}
				stats.CommentsSynced += len(comments)
			}
			if options.IncludePRDetails && thread.Kind == "pull_request" {
				count, err := s.syncPullReviewThreads(ctx, st, options, thread)
				if err != nil {
					return err
				}
				stats.ReviewThreadsSynced += count
				detailStats, err := s.syncPullRequestDetails(ctx, st, options, thread)
				if err != nil {
					return err
				}
				stats.PRDetailsSynced++
				stats.PRFilesSynced += detailStats.files
				stats.PRCommitsSynced += detailStats.commits
				stats.PRChecksSynced += detailStats.checks
				stats.WorkflowRunsSynced += detailStats.runs
			}
			if _, err := st.UpsertDocument(ctx, documents.BuildWithComments(thread, comments)); err != nil {
				return err
			}
			stats.ThreadsSynced++
			if thread.Kind == "pull_request" {
				stats.PullRequestsSynced++
			} else {
				stats.IssuesSynced++
			}
			tracker.Add(1,
				"number", thread.Number,
				"kind", thread.Kind,
				"thread_state", thread.State,
			)
		}
		if len(numbers) == 0 && state == "open" && since != "" && options.Limit <= 0 {
			closed, err := s.applyClosedOverlapSweep(ctx, st, repoID, options, since)
			if err != nil {
				return err
			}
			stats.ThreadsClosed = closed
		}
		stats.FinishedAt = s.now().Format(time.RFC3339Nano)
		if _, err := st.RecordRun(ctx, store.RunRecord{
			RepoID:     repoID,
			Kind:       "sync",
			Scope:      syncRunScope(state, numbers),
			Status:     "success",
			StartedAt:  stats.StartedAt,
			FinishedAt: stats.FinishedAt,
			StatsJSON:  mustJSON(stats),
		}); err != nil {
			return err
		}
		return nil
	}
	if !options.IncludeComments && !options.IncludePRDetails {
		if err := s.store.WithTx(ctx, persist); err != nil {
			tracker.Finish(err)
			return Stats{}, err
		}
		tracker.Finish(nil)
		return stats, nil
	}
	if err := persist(s.store); err != nil {
		tracker.Finish(err)
		return Stats{}, err
	}
	tracker.Finish(nil)
	return stats, nil
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

func (s *Syncer) applyClosedOverlapSweep(ctx context.Context, st *store.Store, repoID int64, options Options, since string) (int, error) {
	rows, err := s.client.ListRepositoryIssues(ctx, options.Owner, options.Repo, gh.ListIssuesOptions{
		State: "closed",
		Since: since,
	}, options.Reporter)
	if err != nil {
		return 0, err
	}
	closed := 0
	for _, row := range rows {
		thread := mapIssueToThread(repoID, row, s.now().Format(time.RFC3339Nano))
		updated, err := st.MarkOpenThreadClosedFromGitHub(ctx, thread)
		if err != nil {
			return 0, err
		}
		if updated {
			closed++
		}
	}
	if closed > 0 {
		options.Reporter.Printf("[sync] closed overlap sweep matched %d stale open thread(s)", closed)
	}
	return closed, nil
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
	kind := "issue"
	if _, ok := row["pull_request"]; ok {
		kind = "pull_request"
	}
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
		RepoID:          repoID,
		GitHubID:        jsonID(row["id"]),
		Number:          intValue(row["number"]),
		Kind:            kind,
		State:           stringValue(row["state"]),
		Title:           title,
		Body:            body,
		AuthorLogin:     loginFromUser(row["user"]),
		AuthorType:      typeFromUser(row["user"]),
		HTMLURL:         stringValue(row["html_url"]),
		LabelsJSON:      labelsJSON,
		AssigneesJSON:   assigneesJSON,
		RawJSON:         mustJSON(row),
		ContentHash:     contentHash(title, body, labelsJSON),
		CreatedAtGitHub: stringValue(row["created_at"]),
		UpdatedAtGitHub: stringValue(row["updated_at"]),
		ClosedAtGitHub:  stringValue(row["closed_at"]),
		FirstPulledAt:   pulledAt,
		LastPulledAt:    pulledAt,
		UpdatedAt:       pulledAt,
	}
}

func (s *Syncer) syncComments(ctx context.Context, options Options, thread store.Thread) ([]store.Comment, error) {
	var rows []commentRow
	issueComments, err := s.client.ListIssueComments(ctx, options.Owner, options.Repo, thread.Number, options.Reporter)
	if err != nil {
		return nil, err
	}
	for _, row := range issueComments {
		rows = append(rows, commentRow{kind: "issue_comment", raw: row})
	}
	if thread.Kind == "pull_request" {
		reviews, err := s.client.ListPullReviews(ctx, options.Owner, options.Repo, thread.Number, options.Reporter)
		if err != nil {
			return nil, err
		}
		for _, row := range reviews {
			rows = append(rows, commentRow{kind: "pull_review", raw: row})
		}
		reviewComments, err := s.client.ListPullReviewComments(ctx, options.Owner, options.Repo, thread.Number, options.Reporter)
		if err != nil {
			return nil, err
		}
		for _, row := range reviewComments {
			rows = append(rows, commentRow{kind: "pull_review_comment", raw: row})
		}
	}
	var comments []store.Comment
	for _, row := range rows {
		comment := mapComment(thread.ID, row.kind, row.raw)
		if comment.Body == "" && row.kind != "pull_review" {
			continue
		}
		if _, err := s.store.UpsertComment(ctx, comment); err != nil {
			return nil, err
		}
		comments = append(comments, comment)
	}
	return comments, nil
}

func (s *Syncer) syncPullReviewThreads(ctx context.Context, st *store.Store, options Options, thread store.Thread) (int, error) {
	rows, err := s.client.ListPullReviewThreads(ctx, options.Owner, options.Repo, thread.Number, options.Reporter)
	if err != nil {
		return 0, fmt.Errorf("list pull request review threads for #%d: %w", thread.Number, err)
	}
	fetchedAt := s.now().Format(time.RFC3339Nano)
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
