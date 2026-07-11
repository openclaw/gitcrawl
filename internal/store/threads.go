package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/openclaw/gitcrawl/internal/store/storedb"
)

type Thread struct {
	ID                int64  `json:"id"`
	RepoID            int64  `json:"repo_id"`
	GitHubID          string `json:"github_id"`
	Number            int    `json:"number"`
	Kind              string `json:"kind"`
	State             string `json:"state"`
	Title             string `json:"title"`
	Body              string `json:"body,omitempty"`
	AuthorLogin       string `json:"author_login,omitempty"`
	AuthorType        string `json:"author_type,omitempty"`
	AuthorAssociation string `json:"author_association,omitempty"`
	HTMLURL           string `json:"html_url"`
	LabelsJSON        string `json:"labels_json"`
	AssigneesJSON     string `json:"assignees_json"`
	RawJSON           string `json:"-"`
	ContentHash       string `json:"content_hash"`
	IsDraft           bool   `json:"is_draft"`
	CreatedAtGitHub   string `json:"created_at_gh,omitempty"`
	UpdatedAtGitHub   string `json:"updated_at_gh,omitempty"`
	ClosedAtGitHub    string `json:"closed_at_gh,omitempty"`
	MergedAtGitHub    string `json:"merged_at_gh,omitempty"`
	FirstPulledAt     string `json:"first_pulled_at,omitempty"`
	LastPulledAt      string `json:"last_pulled_at,omitempty"`
	UpdatedAt         string `json:"updated_at"`
	ClosedAtLocal     string `json:"closed_at_local,omitempty"`
	CloseReasonLocal  string `json:"close_reason_local,omitempty"`
}

func (s *Store) UpsertThread(ctx context.Context, thread Thread) (int64, error) {
	id, err := s.qsql().UpsertThread(ctx, storedb.UpsertThreadParams{
		RepoID:            thread.RepoID,
		GithubID:          thread.GitHubID,
		Number:            int64(thread.Number),
		Kind:              thread.Kind,
		State:             thread.State,
		Title:             thread.Title,
		Body:              nullString(thread.Body),
		AuthorLogin:       nullString(thread.AuthorLogin),
		AuthorType:        nullString(thread.AuthorType),
		AuthorAssociation: nullString(thread.AuthorAssociation),
		HtmlUrl:           thread.HTMLURL,
		LabelsJson:        thread.LabelsJSON,
		AssigneesJson:     thread.AssigneesJSON,
		RawJson:           thread.RawJSON,
		ContentHash:       thread.ContentHash,
		IsDraft:           int64(boolInt(thread.IsDraft)),
		CreatedAtGh:       nullString(thread.CreatedAtGitHub),
		UpdatedAtGh:       nullString(thread.UpdatedAtGitHub),
		ClosedAtGh:        nullString(thread.ClosedAtGitHub),
		MergedAtGh:        nullString(thread.MergedAtGitHub),
		FirstPulledAt:     nullString(thread.FirstPulledAt),
		LastPulledAt:      nullString(thread.LastPulledAt),
		UpdatedAt:         thread.UpdatedAt,
	})
	if err != nil {
		return 0, fmt.Errorf("upsert thread: %w", err)
	}
	return id, nil
}

func (s *Store) MarkOpenThreadClosedFromGitHub(ctx context.Context, thread Thread) (bool, error) {
	if thread.RepoID <= 0 {
		return false, fmt.Errorf("repo id must be positive")
	}
	if thread.Number <= 0 {
		return false, fmt.Errorf("thread number must be positive")
	}
	if thread.Kind == "" {
		return false, fmt.Errorf("thread kind is required")
	}
	if thread.State == "" {
		thread.State = "closed"
	}
	affected, err := s.qsql().MarkOpenThreadClosedFromGitHub(ctx, storedb.MarkOpenThreadClosedFromGitHubParams{
		GithubID:          thread.GitHubID,
		State:             thread.State,
		Title:             thread.Title,
		Body:              nullString(thread.Body),
		AuthorLogin:       nullString(thread.AuthorLogin),
		AuthorType:        nullString(thread.AuthorType),
		AuthorAssociation: nullString(thread.AuthorAssociation),
		HtmlUrl:           thread.HTMLURL,
		LabelsJson:        thread.LabelsJSON,
		AssigneesJson:     thread.AssigneesJSON,
		RawJson:           thread.RawJSON,
		ContentHash:       thread.ContentHash,
		IsDraft:           int64(boolInt(thread.IsDraft)),
		CreatedAtGh:       nullString(thread.CreatedAtGitHub),
		UpdatedAtGh:       nullString(thread.UpdatedAtGitHub),
		ClosedAtGh:        nullString(thread.ClosedAtGitHub),
		MergedAtGh:        nullString(thread.MergedAtGitHub),
		LastPulledAt:      nullString(thread.LastPulledAt),
		UpdatedAt:         thread.UpdatedAt,
		RepoID:            thread.RepoID,
		Kind:              thread.Kind,
		Number:            int64(thread.Number),
	})
	if err != nil {
		return false, fmt.Errorf("mark open thread closed from github: %w", err)
	}
	return affected > 0, nil
}

func (s *Store) ListThreads(ctx context.Context, repoID int64, includeClosed bool) ([]Thread, error) {
	return s.ListThreadsFiltered(ctx, ThreadListOptions{RepoID: repoID, IncludeClosed: includeClosed})
}

type ThreadListOptions struct {
	RepoID        int64
	IncludeClosed bool
	Numbers       []int
	Limit         int
}

func (s *Store) ListThreadsFiltered(ctx context.Context, options ThreadListOptions) ([]Thread, error) {
	if len(options.Numbers) == 0 &&
		s.hasColumn(ctx, "threads", "body") &&
		s.hasColumn(ctx, "threads", "raw_json") &&
		s.hasColumn(ctx, "threads", "author_association") {
		rows, err := s.qsql().ListThreadsCurrentSchema(ctx, storedb.ListThreadsCurrentSchemaParams{
			RepoID:        options.RepoID,
			IncludeClosed: boolInt(options.IncludeClosed),
			RowLimit:      options.Limit,
		})
		if err != nil {
			return nil, fmt.Errorf("list threads: %w", err)
		}
		out := make([]Thread, 0, len(rows))
		for _, row := range rows {
			out = append(out, threadFromCurrentSchemaDB(row))
		}
		return out, nil
	}
	where := `repo_id = ?`
	args := []any{options.RepoID}
	if !options.IncludeClosed {
		where += ` and closed_at_local is null`
	}
	if len(options.Numbers) > 0 {
		placeholders := make([]string, 0, len(options.Numbers))
		for _, number := range options.Numbers {
			placeholders = append(placeholders, "?")
			args = append(args, number)
		}
		where += ` and number in (` + strings.Join(placeholders, ",") + `)`
	}
	limitSQL := ``
	if options.Limit > 0 {
		limitSQL = ` limit ?`
		args = append(args, options.Limit)
	}
	rows, err := s.q().QueryContext(ctx, `
		select `+s.threadSelectColumns(ctx, "")+`
		from threads
		where `+where+`
		order by number`+limitSQL, args...)
	if err != nil {
		return nil, fmt.Errorf("list threads: %w", err)
	}
	defer rows.Close()

	var out []Thread
	for rows.Next() {
		thread, err := scanThread(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, thread)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate threads: %w", err)
	}
	return out, nil
}

func (s *Store) CloseThreadLocally(ctx context.Context, repoID int64, number int, reason string) error {
	if repoID <= 0 {
		return fmt.Errorf("repo id must be positive")
	}
	if number <= 0 {
		return fmt.Errorf("thread number must be positive")
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "local close"
	}
	closedAt := time.Now().UTC().Format(timeLayout)
	affected, err := s.qsql().CloseThreadLocally(ctx, storedb.CloseThreadLocallyParams{
		ClosedAt: sql.NullString{String: closedAt, Valid: true},
		Reason:   sql.NullString{String: reason, Valid: true},
		RepoID:   repoID,
		Number:   int64(number),
	})
	if err != nil {
		return fmt.Errorf("close thread locally: %w", err)
	}
	if affected == 0 {
		return fmt.Errorf("thread #%d was not found", number)
	}
	return nil
}

func (s *Store) ReopenThreadLocally(ctx context.Context, repoID int64, number int) error {
	if repoID <= 0 {
		return fmt.Errorf("repo id must be positive")
	}
	if number <= 0 {
		return fmt.Errorf("thread number must be positive")
	}
	updatedAt := time.Now().UTC().Format(timeLayout)
	affected, err := s.qsql().ReopenThreadLocally(ctx, storedb.ReopenThreadLocallyParams{
		UpdatedAt: updatedAt,
		RepoID:    repoID,
		Number:    int64(number),
	})
	if err != nil {
		return fmt.Errorf("reopen thread locally: %w", err)
	}
	if affected == 0 {
		return fmt.Errorf("thread #%d was not found", number)
	}
	return nil
}

func scanThread(rows interface {
	Scan(dest ...any) error
}) (Thread, error) {
	var thread Thread
	var body, authorLogin, authorType, authorAssociation, rawJSON, createdAt, updatedAtGH, closedAt, mergedAt, firstPulled, lastPulled, closedLocal, closeReason sql.NullString
	var isDraft int
	if err := rows.Scan(&thread.ID, &thread.RepoID, &thread.GitHubID, &thread.Number, &thread.Kind, &thread.State, &thread.Title,
		&body, &authorLogin, &authorType, &authorAssociation, &thread.HTMLURL, &thread.LabelsJSON, &thread.AssigneesJSON, &rawJSON,
		&thread.ContentHash, &isDraft, &createdAt, &updatedAtGH, &closedAt, &mergedAt, &firstPulled, &lastPulled, &thread.UpdatedAt,
		&closedLocal, &closeReason); err != nil {
		return Thread{}, fmt.Errorf("scan thread: %w", err)
	}
	thread.Body = body.String
	thread.AuthorLogin = authorLogin.String
	thread.AuthorType = authorType.String
	thread.AuthorAssociation = authorAssociation.String
	thread.CreatedAtGitHub = createdAt.String
	thread.UpdatedAtGitHub = updatedAtGH.String
	thread.ClosedAtGitHub = closedAt.String
	thread.MergedAtGitHub = mergedAt.String
	thread.FirstPulledAt = firstPulled.String
	thread.LastPulledAt = lastPulled.String
	thread.ClosedAtLocal = closedLocal.String
	thread.CloseReasonLocal = closeReason.String
	thread.RawJSON = rawJSON.String
	thread.IsDraft = isDraft != 0
	return thread, nil
}

func (s *Store) threadSelectColumns(ctx context.Context, alias string) string {
	column := func(name string) string {
		if alias == "" {
			return name
		}
		return alias + "." + name
	}
	return strings.Join([]string{
		column("id"),
		column("repo_id"),
		column("github_id"),
		column("number"),
		column("kind"),
		column("state"),
		column("title"),
		s.threadBodyExpr(ctx, alias),
		column("author_login"),
		column("author_type"),
		s.threadOptionalColumnExpr(ctx, alias, "author_association"),
		column("html_url"),
		column("labels_json"),
		column("assignees_json"),
		s.threadRawJSONExpr(ctx, alias),
		column("content_hash"),
		column("is_draft"),
		column("created_at_gh"),
		column("updated_at_gh"),
		column("closed_at_gh"),
		column("merged_at_gh"),
		column("first_pulled_at"),
		column("last_pulled_at"),
		column("updated_at"),
		column("closed_at_local"),
		column("close_reason_local"),
	}, ", ")
}

func (s *Store) threadOptionalColumnExpr(ctx context.Context, alias, name string) string {
	if s.hasColumn(ctx, "threads", name) {
		return qualifiedColumn(alias, name)
	}
	return "''"
}

func (s *Store) threadBodyExpr(ctx context.Context, alias string) string {
	if s.hasColumn(ctx, "threads", "body") {
		return qualifiedColumn(alias, "body")
	}
	if s.hasColumn(ctx, "threads", "body_excerpt") {
		return qualifiedColumn(alias, "body_excerpt")
	}
	return "''"
}

func (s *Store) threadRawJSONExpr(ctx context.Context, alias string) string {
	if s.hasColumn(ctx, "threads", "raw_json") {
		return qualifiedColumn(alias, "raw_json")
	}
	return "''"
}

func qualifiedColumn(alias, name string) string {
	if alias == "" {
		return name
	}
	return alias + "." + name
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
