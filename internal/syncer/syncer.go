package syncer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/openclaw/gitcrawl/internal/documents"
	gh "github.com/openclaw/gitcrawl/internal/github"
	"github.com/openclaw/gitcrawl/internal/store"
)

type GitHubClient interface {
	GetRepo(ctx context.Context, owner, repo string, reporter gh.Reporter) (map[string]any, error)
	ListRepositoryIssues(ctx context.Context, owner, repo string, options gh.ListIssuesOptions, reporter gh.Reporter) ([]map[string]any, error)
}

type Syncer struct {
	client GitHubClient
	store  *store.Store
	now    func() time.Time
}

type Options struct {
	Owner    string
	Repo     string
	Since    string
	Limit    int
	Reporter gh.Reporter
}

type Stats struct {
	Repository         string `json:"repository"`
	ThreadsSynced      int    `json:"threads_synced"`
	IssuesSynced       int    `json:"issues_synced"`
	PullRequestsSynced int    `json:"pull_requests_synced"`
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
		State: "open",
		Since: options.Since,
		Limit: options.Limit,
	}, options.Reporter)
	if err != nil {
		return Stats{}, err
	}

	stats := Stats{
		Repository:     options.Owner + "/" + options.Repo,
		RequestedSince: options.Since,
		Limit:          options.Limit,
		MetadataOnly:   true,
		StartedAt:      started,
	}
	for _, row := range rows {
		thread := mapIssueToThread(repoID, row, s.now().Format(time.RFC3339Nano))
		threadID, err := s.store.UpsertThread(ctx, thread)
		if err != nil {
			return Stats{}, err
		}
		thread.ID = threadID
		if _, err := s.store.UpsertDocument(ctx, documents.Build(thread)); err != nil {
			return Stats{}, err
		}
		stats.ThreadsSynced++
		if thread.Kind == "pull_request" {
			stats.PullRequestsSynced++
		} else {
			stats.IssuesSynced++
		}
	}
	stats.FinishedAt = s.now().Format(time.RFC3339Nano)
	if _, err := s.store.RecordRun(ctx, store.RunRecord{
		RepoID:     repoID,
		Kind:       "sync",
		Scope:      "open",
		Status:     "success",
		StartedAt:  stats.StartedAt,
		FinishedAt: stats.FinishedAt,
		StatsJSON:  mustJSON(stats),
	}); err != nil {
		return Stats{}, err
	}
	return stats, nil
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
