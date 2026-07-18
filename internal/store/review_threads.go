package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/openclaw/gitcrawl/internal/store/storedb"
)

type PullRequestReviewThread struct {
	ThreadID              int64  `json:"thread_id"`
	ReviewThreadID        string `json:"id"`
	Path                  string `json:"path,omitempty"`
	Line                  int    `json:"line,omitempty"`
	StartLine             int    `json:"start_line,omitempty"`
	IsResolved            bool   `json:"is_resolved"`
	IsOutdated            bool   `json:"is_outdated"`
	ViewerCanResolve      bool   `json:"viewer_can_resolve"`
	ViewerCanUnresolve    bool   `json:"viewer_can_unresolve"`
	ViewerCanReply        bool   `json:"viewer_can_reply"`
	FirstAuthorLogin      string `json:"first_author_login,omitempty"`
	FirstAuthorType       string `json:"first_author_type,omitempty"`
	FirstCommentBody      string `json:"first_comment_body,omitempty"`
	FirstCommentURL       string `json:"first_comment_url,omitempty"`
	FirstCommentCreatedAt string `json:"first_comment_created_at,omitempty"`
	FirstCommentUpdatedAt string `json:"first_comment_updated_at,omitempty"`
	CommentsJSON          string `json:"comments_json"`
	RawJSON               string `json:"-"`
	FetchedAt             string `json:"fetched_at"`
	DeletedAt             string `json:"deleted_at,omitempty"`
	DeletionReason        string `json:"deletion_reason,omitempty"`
}

func (s *Store) UpsertPullRequestReviewThreads(ctx context.Context, threadID int64, fetchedAt string, threads []PullRequestReviewThread) error {
	for _, thread := range threads {
		if err := validateTombstone(thread.DeletedAt, thread.DeletionReason); err != nil {
			return fmt.Errorf("upsert pull request review thread %q: %w", thread.ReviewThreadID, err)
		}
	}
	if s.queries == nil {
		return s.WithTx(ctx, func(tx *Store) error {
			return tx.upsertPullRequestReviewThreads(ctx, threadID, fetchedAt, threads)
		})
	}
	return s.upsertPullRequestReviewThreads(ctx, threadID, fetchedAt, threads)
}

func (s *Store) upsertPullRequestReviewThreads(ctx context.Context, threadID int64, fetchedAt string, threads []PullRequestReviewThread) error {
	if err := s.qsql().UpsertPullRequestReviewThreadSync(ctx, storedb.UpsertPullRequestReviewThreadSyncParams{
		ThreadID:  threadID,
		FetchedAt: fetchedAt,
	}); err != nil {
		return fmt.Errorf("mark pull request review threads fetched: %w", err)
	}
	for _, thread := range threads {
		if thread.ReviewThreadID == "" {
			continue
		}
		if thread.DeletedAt != "" {
			applied, err := s.tombstonePullRequestReviewThread(ctx, threadID, thread.ReviewThreadID, thread.DeletedAt, thread.DeletionReason)
			if err != nil {
				return err
			}
			if applied {
				continue
			}
		}
		if err := s.qsql().UpsertPullRequestReviewThread(ctx, storedb.UpsertPullRequestReviewThreadParams{
			ThreadID:              threadID,
			ReviewThreadID:        thread.ReviewThreadID,
			Path:                  nullString(thread.Path),
			Line:                  int64(thread.Line),
			StartLine:             int64(thread.StartLine),
			IsResolved:            int64(boolInt(thread.IsResolved)),
			IsOutdated:            int64(boolInt(thread.IsOutdated)),
			ViewerCanResolve:      int64(boolInt(thread.ViewerCanResolve)),
			ViewerCanUnresolve:    int64(boolInt(thread.ViewerCanUnresolve)),
			ViewerCanReply:        int64(boolInt(thread.ViewerCanReply)),
			FirstAuthorLogin:      nullString(thread.FirstAuthorLogin),
			FirstAuthorType:       nullString(thread.FirstAuthorType),
			FirstCommentBody:      nullString(thread.FirstCommentBody),
			FirstCommentUrl:       nullString(thread.FirstCommentURL),
			FirstCommentCreatedAt: nullString(thread.FirstCommentCreatedAt),
			FirstCommentUpdatedAt: nullString(thread.FirstCommentUpdatedAt),
			CommentsJson:          thread.CommentsJSON,
			RawJson:               thread.RawJSON,
			FetchedAt:             thread.FetchedAt,
			DeletedAt:             nullString(thread.DeletedAt),
			DeletionReason:        nullString(thread.DeletionReason),
		}); err != nil {
			return fmt.Errorf("upsert pull request review thread: %w", err)
		}
		if err := s.recordPullRequestReviewThreadRevision(ctx, threadID, thread.ReviewThreadID, time.Now().UTC().Format(timeLayout)); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) TombstonePullRequestReviewThread(ctx context.Context, threadID int64, reviewThreadID, deletedAt, reason string) (bool, error) {
	if err := validateTombstone(deletedAt, reason); err != nil {
		return false, fmt.Errorf("tombstone pull request review thread: %w", err)
	}
	if deletedAt == "" {
		return false, fmt.Errorf("tombstone pull request review thread: deleted_at is required")
	}
	if s.queries == nil {
		var applied bool
		err := s.WithTx(ctx, func(tx *Store) error {
			var err error
			applied, err = tx.tombstonePullRequestReviewThread(ctx, threadID, reviewThreadID, deletedAt, reason)
			return err
		})
		return applied, err
	}
	return s.tombstonePullRequestReviewThread(ctx, threadID, reviewThreadID, deletedAt, reason)
}

func (s *Store) tombstonePullRequestReviewThread(ctx context.Context, threadID int64, reviewThreadID, deletedAt, reason string) (bool, error) {
	result, err := s.q().ExecContext(ctx, `
		update pull_request_review_threads
		set deleted_at = ?, deletion_reason = ?
		where thread_id = ? and review_thread_id = ?
	`, deletedAt, reason, threadID, reviewThreadID)
	if err != nil {
		return false, fmt.Errorf("tombstone pull request review thread: %w", err)
	}
	if rowsAffected(result) == 0 {
		return false, nil
	}
	if err := s.recordPullRequestReviewThreadRevision(ctx, threadID, reviewThreadID, time.Now().UTC().Format(timeLayout)); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) recordPullRequestReviewThreadRevision(ctx context.Context, threadID int64, reviewThreadID, recordedAt string) error {
	if _, err := s.q().ExecContext(ctx, `
		insert into pull_request_review_thread_revisions(
			thread_id, review_thread_id, path, line, start_line, is_resolved,
			is_outdated, viewer_can_resolve, viewer_can_unresolve, viewer_can_reply,
			first_author_login, first_author_type, first_comment_body,
			first_comment_url, first_comment_created_at, first_comment_updated_at,
			comments_json, raw_json, fetched_at, deleted_at, deletion_reason, recorded_at
		)
		select rt.thread_id, rt.review_thread_id, rt.path, rt.line, rt.start_line,
			rt.is_resolved, rt.is_outdated, rt.viewer_can_resolve,
			rt.viewer_can_unresolve, rt.viewer_can_reply, rt.first_author_login,
			rt.first_author_type, rt.first_comment_body, rt.first_comment_url,
			rt.first_comment_created_at, rt.first_comment_updated_at,
			rt.comments_json, rt.raw_json, rt.fetched_at, rt.deleted_at,
			rt.deletion_reason, ?
		from pull_request_review_threads rt
		where rt.thread_id = ? and rt.review_thread_id = ?
			and not exists (
				select 1
				from pull_request_review_thread_revisions r
				where r.id = (
					select max(latest.id)
					from pull_request_review_thread_revisions latest
					where latest.thread_id = rt.thread_id
						and latest.review_thread_id = rt.review_thread_id
				)
					and r.path is rt.path
					and r.line is rt.line
					and r.start_line is rt.start_line
					and r.is_resolved is rt.is_resolved
					and r.is_outdated is rt.is_outdated
					and r.viewer_can_resolve is rt.viewer_can_resolve
					and r.viewer_can_unresolve is rt.viewer_can_unresolve
					and r.viewer_can_reply is rt.viewer_can_reply
					and r.first_author_login is rt.first_author_login
					and r.first_author_type is rt.first_author_type
					and r.first_comment_body is rt.first_comment_body
					and r.first_comment_url is rt.first_comment_url
					and r.first_comment_created_at is rt.first_comment_created_at
					and r.first_comment_updated_at is rt.first_comment_updated_at
					and r.comments_json is rt.comments_json
					and r.raw_json is rt.raw_json
					and r.deleted_at is rt.deleted_at
					and r.deletion_reason is rt.deletion_reason
			)
	`, recordedAt, threadID, reviewThreadID); err != nil {
		return fmt.Errorf("record pull request review thread revision: %w", err)
	}
	return nil
}

func (s *Store) PullRequestReviewThreads(ctx context.Context, threadID int64) ([]PullRequestReviewThread, error) {
	if !s.tableExists(ctx, "pull_request_review_threads") {
		return nil, nil
	}
	rows, err := s.qsql().PullRequestReviewThreads(ctx, threadID)
	if err != nil {
		return nil, fmt.Errorf("list pull request review threads: %w", err)
	}
	threads := make([]PullRequestReviewThread, 0, len(rows))
	for _, row := range rows {
		threads = append(threads, PullRequestReviewThread{
			ThreadID:              row.ThreadID,
			ReviewThreadID:        row.ReviewThreadID,
			Path:                  stringValue(row.Path),
			Line:                  int(row.Line),
			StartLine:             int(row.StartLine),
			IsResolved:            int64Bool(row.IsResolved),
			IsOutdated:            int64Bool(row.IsOutdated),
			ViewerCanResolve:      int64Bool(row.ViewerCanResolve),
			ViewerCanUnresolve:    int64Bool(row.ViewerCanUnresolve),
			ViewerCanReply:        int64Bool(row.ViewerCanReply),
			FirstAuthorLogin:      stringValue(row.FirstAuthorLogin),
			FirstAuthorType:       stringValue(row.FirstAuthorType),
			FirstCommentBody:      stringValue(row.FirstCommentBody),
			FirstCommentURL:       stringValue(row.FirstCommentUrl),
			FirstCommentCreatedAt: stringValue(row.FirstCommentCreatedAt),
			FirstCommentUpdatedAt: stringValue(row.FirstCommentUpdatedAt),
			CommentsJSON:          row.CommentsJson,
			RawJSON:               row.RawJson,
			FetchedAt:             row.FetchedAt,
			DeletedAt:             stringValue(row.DeletedAt),
			DeletionReason:        stringValue(row.DeletionReason),
		})
	}
	return threads, nil
}

func (s *Store) PullRequestReviewThreadsFetchedAt(ctx context.Context, threadID int64) (string, error) {
	if !s.tableExists(ctx, "pull_request_review_thread_syncs") {
		return "", nil
	}
	fetchedAt, err := s.qsql().PullRequestReviewThreadsFetchedAt(ctx, threadID)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("pull request review threads fetched marker: %w", err)
	}
	return fetchedAt, nil
}
