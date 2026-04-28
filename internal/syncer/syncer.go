package syncer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/openclaw/gitcrawl/internal/documents"
	gh "github.com/openclaw/gitcrawl/internal/github"
	"github.com/openclaw/gitcrawl/internal/store"
)

type GitHubClient interface {
	GetRepo(ctx context.Context, owner, repo string, reporter gh.Reporter) (map[string]any, error)
	ListRepositoryIssues(ctx context.Context, owner, repo string, options gh.ListIssuesOptions, reporter gh.Reporter) ([]map[string]any, error)
	ListIssueComments(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) ([]map[string]any, error)
	ListPullReviews(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) ([]map[string]any, error)
	ListPullReviewComments(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) ([]map[string]any, error)
}

type Syncer struct {
	client GitHubClient
	store  *store.Store
	now    func() time.Time
}

type Options struct {
	Owner           string
	Repo            string
	State           string
	Since           string
	Limit           int
	IncludeComments bool
	Reporter        gh.Reporter
}

type Stats struct {
	Repository         string `json:"repository"`
	ThreadsSynced      int    `json:"threads_synced"`
	IssuesSynced       int    `json:"issues_synced"`
	PullRequestsSynced int    `json:"pull_requests_synced"`
	CommentsSynced     int    `json:"comments_synced"`
	ThreadsClosed      int    `json:"threads_closed"`
	RequestedSince     string `json:"requested_since,omitempty"`
	Limit              int    `json:"limit,omitempty"`
	MetadataOnly       bool   `json:"metadata_only"`
	StartedAt          string `json:"started_at"`
	FinishedAt         string `json:"finished_at"`
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

	rows, err := s.client.ListRepositoryIssues(ctx, options.Owner, options.Repo, gh.ListIssuesOptions{
		State: state,
		Since: since,
		Limit: options.Limit,
	}, options.Reporter)
	if err != nil {
		return Stats{}, err
	}

	stats := Stats{
		Repository:     options.Owner + "/" + options.Repo,
		RequestedSince: since,
		Limit:          options.Limit,
		MetadataOnly:   !options.IncludeComments,
		StartedAt:      started,
	}
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
			if _, err := st.UpsertDocument(ctx, documents.BuildWithComments(thread, comments)); err != nil {
				return err
			}
			stats.ThreadsSynced++
			if thread.Kind == "pull_request" {
				stats.PullRequestsSynced++
			} else {
				stats.IssuesSynced++
			}
		}
		if state == "open" && since != "" && options.Limit <= 0 {
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
			Scope:      state,
			Status:     "success",
			StartedAt:  stats.StartedAt,
			FinishedAt: stats.FinishedAt,
			StatsJSON:  mustJSON(stats),
		}); err != nil {
			return err
		}
		return nil
	}
	if !options.IncludeComments {
		if err := s.store.WithTx(ctx, persist); err != nil {
			return Stats{}, err
		}
		return stats, nil
	}
	if err := persist(s.store); err != nil {
		return Stats{}, err
	}
	return stats, nil
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
		if comment.Body == "" {
			continue
		}
		if _, err := s.store.UpsertComment(ctx, comment); err != nil {
			return nil, err
		}
		comments = append(comments, comment)
	}
	return comments, nil
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
