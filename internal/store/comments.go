package store

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/openclaw/gitcrawl/internal/store/storedb"
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
	ReviewState     string `json:"review_state,omitempty"`
	RawJSON         string `json:"-"`
	CreatedAtGitHub string `json:"created_at_gh,omitempty"`
	UpdatedAtGitHub string `json:"updated_at_gh,omitempty"`
}

func (s *Store) UpsertComment(ctx context.Context, comment Comment) (int64, error) {
	id, err := s.qsql().UpsertComment(ctx, storedb.UpsertCommentParams{
		ThreadID:    comment.ThreadID,
		GithubID:    comment.GitHubID,
		CommentType: comment.CommentType,
		AuthorLogin: nullString(comment.AuthorLogin),
		AuthorType:  nullString(comment.AuthorType),
		Body:        comment.Body,
		IsBot:       int64(boolInt(comment.IsBot)),
		RawJson:     comment.RawJSON,
		CreatedAtGh: nullString(comment.CreatedAtGitHub),
		UpdatedAtGh: nullString(comment.UpdatedAtGitHub),
	})
	if err != nil {
		return 0, fmt.Errorf("upsert comment: %w", err)
	}
	return id, nil
}

func (s *Store) DeleteCommentsForThread(ctx context.Context, threadID int64) error {
	if _, err := s.q().ExecContext(ctx, `delete from comments where thread_id = ?`, threadID); err != nil {
		return fmt.Errorf("delete comments for thread: %w", err)
	}
	return nil
}

func (s *Store) ListComments(ctx context.Context, threadID int64) ([]Comment, error) {
	if !s.tableExists(ctx, "comments") {
		return nil, nil
	}
	rows, err := s.qsql().ListComments(ctx, threadID)
	if err != nil {
		return nil, fmt.Errorf("list comments: %w", err)
	}
	comments := make([]Comment, 0, len(rows))
	for _, row := range rows {
		comments = append(comments, Comment{
			ID:              row.ID,
			ThreadID:        row.ThreadID,
			GitHubID:        row.GithubID,
			CommentType:     row.CommentType,
			AuthorLogin:     stringValue(row.AuthorLogin),
			AuthorType:      stringValue(row.AuthorType),
			Body:            row.Body,
			IsBot:           int64Bool(row.IsBot),
			ReviewState:     reviewStateFromRawJSON(row.RawJson),
			RawJSON:         row.RawJson,
			CreatedAtGitHub: stringValue(row.CreatedAtGh),
			UpdatedAtGitHub: stringValue(row.UpdatedAtGh),
		})
	}
	return comments, nil
}

func reviewStateFromRawJSON(raw string) string {
	var value struct {
		State string `json:"state"`
	}
	if json.Unmarshal([]byte(raw), &value) != nil {
		return ""
	}
	return value.State
}
