package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

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
	DeletedAt       string `json:"deleted_at,omitempty"`
	DeletionReason  string `json:"deletion_reason,omitempty"`
}

func (s *Store) UpsertComment(ctx context.Context, comment Comment) (int64, error) {
	if err := validateTombstone(comment.DeletedAt, comment.DeletionReason); err != nil {
		return 0, fmt.Errorf("upsert comment: %w", err)
	}
	if s.queries == nil {
		var id int64
		err := s.WithTx(ctx, func(tx *Store) error {
			var err error
			id, err = tx.upsertComment(ctx, comment)
			return err
		})
		return id, err
	}
	return s.upsertComment(ctx, comment)
}

func (s *Store) upsertComment(ctx context.Context, comment Comment) (int64, error) {
	if comment.DeletedAt != "" {
		id, applied, err := s.tombstoneComment(ctx, comment.ThreadID, comment.CommentType, comment.GitHubID, comment.DeletedAt, comment.DeletionReason)
		if err != nil {
			return 0, err
		}
		if applied {
			return id, nil
		}
	}
	id, err := s.qsql().UpsertComment(ctx, storedb.UpsertCommentParams{
		ThreadID:       comment.ThreadID,
		GithubID:       comment.GitHubID,
		CommentType:    comment.CommentType,
		AuthorLogin:    nullString(comment.AuthorLogin),
		AuthorType:     nullString(comment.AuthorType),
		Body:           comment.Body,
		IsBot:          int64(boolInt(comment.IsBot)),
		RawJson:        comment.RawJSON,
		CreatedAtGh:    nullString(comment.CreatedAtGitHub),
		UpdatedAtGh:    nullString(comment.UpdatedAtGitHub),
		DeletedAt:      nullString(comment.DeletedAt),
		DeletionReason: nullString(comment.DeletionReason),
	})
	if err != nil {
		return 0, fmt.Errorf("upsert comment: %w", err)
	}
	if err := s.recordCommentRevision(ctx, id, time.Now().UTC().Format(timeLayout)); err != nil {
		return 0, err
	}
	return id, nil
}

func (s *Store) TombstoneComment(ctx context.Context, threadID int64, commentType, githubID, deletedAt, reason string) (bool, error) {
	if err := validateTombstone(deletedAt, reason); err != nil {
		return false, fmt.Errorf("tombstone comment: %w", err)
	}
	if deletedAt == "" {
		return false, fmt.Errorf("tombstone comment: deleted_at is required")
	}
	if s.queries == nil {
		var applied bool
		err := s.WithTx(ctx, func(tx *Store) error {
			var err error
			_, applied, err = tx.tombstoneComment(ctx, threadID, commentType, githubID, deletedAt, reason)
			return err
		})
		return applied, err
	}
	_, applied, err := s.tombstoneComment(ctx, threadID, commentType, githubID, deletedAt, reason)
	return applied, err
}

func (s *Store) tombstoneComment(ctx context.Context, threadID int64, commentType, githubID, deletedAt, reason string) (int64, bool, error) {
	var id int64
	err := s.q().QueryRowContext(ctx, `
		update comments
		set deleted_at = ?, deletion_reason = ?
		where thread_id = ? and comment_type = ? and github_id = ?
		returning id
	`, deletedAt, reason, threadID, commentType, githubID).Scan(&id)
	if err == sql.ErrNoRows {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("tombstone comment: %w", err)
	}
	if err := s.recordCommentRevision(ctx, id, time.Now().UTC().Format(timeLayout)); err != nil {
		return 0, false, err
	}
	return id, true, nil
}

func (s *Store) recordCommentRevision(ctx context.Context, commentID int64, recordedAt string) error {
	if _, err := s.q().ExecContext(ctx, `
		insert into comment_revisions(
			comment_id, author_login, author_type, body, is_bot, raw_json,
			created_at_gh, updated_at_gh, deleted_at, deletion_reason, recorded_at
		)
		select c.id, c.author_login, c.author_type, c.body, c.is_bot, c.raw_json,
			c.created_at_gh, c.updated_at_gh, c.deleted_at, c.deletion_reason, ?
		from comments c
		where c.id = ?
			and not exists (
				select 1
				from comment_revisions r
				where r.id = (
					select max(latest.id) from comment_revisions latest
					where latest.comment_id = c.id
				)
					and r.author_login is c.author_login
					and r.author_type is c.author_type
					and r.body is c.body
					and r.is_bot is c.is_bot
					and r.raw_json is c.raw_json
					and r.created_at_gh is c.created_at_gh
					and r.updated_at_gh is c.updated_at_gh
					and r.deleted_at is c.deleted_at
					and r.deletion_reason is c.deletion_reason
			)
	`, recordedAt, commentID); err != nil {
		return fmt.Errorf("record comment revision: %w", err)
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
			DeletedAt:       stringValue(row.DeletedAt),
			DeletionReason:  stringValue(row.DeletionReason),
		})
	}
	return comments, nil
}

func validateTombstone(deletedAt, reason string) error {
	normalizedDeletedAt := strings.TrimSpace(deletedAt)
	normalizedReason := strings.TrimSpace(reason)
	if deletedAt != normalizedDeletedAt || reason != normalizedReason {
		return fmt.Errorf("deleted_at and deletion_reason must not have surrounding whitespace")
	}
	if (normalizedDeletedAt == "") != (normalizedReason == "") {
		return fmt.Errorf("deleted_at and deletion_reason must be set together")
	}
	if normalizedDeletedAt == "" {
		return nil
	}
	if _, err := time.Parse(timeLayout, normalizedDeletedAt); err != nil {
		return fmt.Errorf("deleted_at must be RFC3339: %w", err)
	}
	return nil
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
