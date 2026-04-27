package store

import (
	"context"
	"fmt"
)

type Comment struct {
	ID              int64  `json:"id"`
	ThreadID        int64  `json:"thread_id"`
	GitHubID        string `json:"github_id"`
	CommentType     string `json:"comment_type"`
	AuthorLogin     string `json:"author_login,omitempty"`
	AuthorType      string `json:"author_type,omitempty"`
	Body            string `json:"body"`
	IsBot           bool   `json:"is_bot"`
	RawJSON         string `json:"-"`
	CreatedAtGitHub string `json:"created_at_gh,omitempty"`
	UpdatedAtGitHub string `json:"updated_at_gh,omitempty"`
}

func (s *Store) UpsertComment(ctx context.Context, comment Comment) (int64, error) {
	var id int64
	err := s.q().QueryRowContext(ctx, `
		insert into comments(thread_id, github_id, comment_type, author_login, author_type, body, is_bot, raw_json, created_at_gh, updated_at_gh)
		values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		on conflict(thread_id, comment_type, github_id) do update set
			author_login=excluded.author_login,
			author_type=excluded.author_type,
			body=excluded.body,
			is_bot=excluded.is_bot,
			raw_json=excluded.raw_json,
			created_at_gh=excluded.created_at_gh,
			updated_at_gh=excluded.updated_at_gh
		returning id
	`, comment.ThreadID, comment.GitHubID, comment.CommentType, nullString(comment.AuthorLogin), nullString(comment.AuthorType), comment.Body,
		boolInt(comment.IsBot), comment.RawJSON, nullString(comment.CreatedAtGitHub), nullString(comment.UpdatedAtGitHub)).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("upsert comment: %w", err)
	}
	return id, nil
}
