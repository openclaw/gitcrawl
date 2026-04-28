package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

type Thread struct {
	ID               int64  `json:"id"`
	RepoID           int64  `json:"repo_id"`
	GitHubID         string `json:"github_id"`
	Number           int    `json:"number"`
	Kind             string `json:"kind"`
	State            string `json:"state"`
	Title            string `json:"title"`
	Body             string `json:"body,omitempty"`
	AuthorLogin      string `json:"author_login,omitempty"`
	AuthorType       string `json:"author_type,omitempty"`
	HTMLURL          string `json:"html_url"`
	LabelsJSON       string `json:"labels_json"`
	AssigneesJSON    string `json:"assignees_json"`
	RawJSON          string `json:"-"`
	ContentHash      string `json:"content_hash"`
	IsDraft          bool   `json:"is_draft"`
	CreatedAtGitHub  string `json:"created_at_gh,omitempty"`
	UpdatedAtGitHub  string `json:"updated_at_gh,omitempty"`
	ClosedAtGitHub   string `json:"closed_at_gh,omitempty"`
	MergedAtGitHub   string `json:"merged_at_gh,omitempty"`
	FirstPulledAt    string `json:"first_pulled_at,omitempty"`
	LastPulledAt     string `json:"last_pulled_at,omitempty"`
	UpdatedAt        string `json:"updated_at"`
	ClosedAtLocal    string `json:"closed_at_local,omitempty"`
	CloseReasonLocal string `json:"close_reason_local,omitempty"`
}

func (s *Store) UpsertThread(ctx context.Context, thread Thread) (int64, error) {
	var id int64
	err := s.q().QueryRowContext(ctx, `
		insert into threads(
			repo_id, github_id, number, kind, state, title, body, author_login, author_type, html_url,
			labels_json, assignees_json, raw_json, content_hash, is_draft,
			created_at_gh, updated_at_gh, closed_at_gh, merged_at_gh,
			first_pulled_at, last_pulled_at, updated_at
		)
		values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		on conflict(repo_id, kind, number) do update set
			github_id=excluded.github_id,
			state=excluded.state,
			title=excluded.title,
			body=excluded.body,
			author_login=excluded.author_login,
			author_type=excluded.author_type,
			html_url=excluded.html_url,
			labels_json=excluded.labels_json,
			assignees_json=excluded.assignees_json,
			raw_json=excluded.raw_json,
			content_hash=excluded.content_hash,
			is_draft=excluded.is_draft,
			created_at_gh=excluded.created_at_gh,
			updated_at_gh=excluded.updated_at_gh,
			closed_at_gh=excluded.closed_at_gh,
			merged_at_gh=excluded.merged_at_gh,
			last_pulled_at=excluded.last_pulled_at,
			updated_at=excluded.updated_at
		returning id
	`, thread.RepoID, thread.GitHubID, thread.Number, thread.Kind, thread.State, thread.Title, nullString(thread.Body),
		nullString(thread.AuthorLogin), nullString(thread.AuthorType), thread.HTMLURL, thread.LabelsJSON, thread.AssigneesJSON,
		thread.RawJSON, thread.ContentHash, boolInt(thread.IsDraft), nullString(thread.CreatedAtGitHub), nullString(thread.UpdatedAtGitHub),
		nullString(thread.ClosedAtGitHub), nullString(thread.MergedAtGitHub), nullString(thread.FirstPulledAt), nullString(thread.LastPulledAt),
		thread.UpdatedAt).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("upsert thread: %w", err)
	}
	return id, nil
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
	result, err := s.q().ExecContext(ctx, `
		update threads
		set closed_at_local = ?, close_reason_local = ?, updated_at = ?
		where repo_id = ? and number = ?
	`, closedAt, reason, closedAt, repoID, number)
	if err != nil {
		return fmt.Errorf("close thread locally: %w", err)
	}
	if affected, err := result.RowsAffected(); err == nil && affected == 0 {
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
	result, err := s.q().ExecContext(ctx, `
		update threads
		set closed_at_local = null, close_reason_local = null, updated_at = ?
		where repo_id = ? and number = ?
	`, updatedAt, repoID, number)
	if err != nil {
		return fmt.Errorf("reopen thread locally: %w", err)
	}
	if affected, err := result.RowsAffected(); err == nil && affected == 0 {
		return fmt.Errorf("thread #%d was not found", number)
	}
	return nil
}

func scanThread(rows interface {
	Scan(dest ...any) error
}) (Thread, error) {
	var thread Thread
	var body, authorLogin, authorType, rawJSON, createdAt, updatedAtGH, closedAt, mergedAt, firstPulled, lastPulled, closedLocal, closeReason sql.NullString
	var isDraft int
	if err := rows.Scan(&thread.ID, &thread.RepoID, &thread.GitHubID, &thread.Number, &thread.Kind, &thread.State, &thread.Title,
		&body, &authorLogin, &authorType, &thread.HTMLURL, &thread.LabelsJSON, &thread.AssigneesJSON, &rawJSON,
		&thread.ContentHash, &isDraft, &createdAt, &updatedAtGH, &closedAt, &mergedAt, &firstPulled, &lastPulled, &thread.UpdatedAt,
		&closedLocal, &closeReason); err != nil {
		return Thread{}, fmt.Errorf("scan thread: %w", err)
	}
	thread.Body = body.String
	thread.AuthorLogin = authorLogin.String
	thread.AuthorType = authorType.String
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
