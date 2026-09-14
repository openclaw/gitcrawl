package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// IssueCommentReuse identifies the exact saved discussion validated by a fresh
// parent timestamp and comment count. PR reviews are deliberately excluded.
type IssueCommentReuse struct {
	ThreadID            int64
	ObservationSequence int64
	CommentIDs          []int64
}

func (s *Store) ReusableIssueComments(ctx context.Context, repoID int64, number int, updatedAt string, count int) (*IssueCommentReuse, error) {
	updated, err := time.Parse(time.RFC3339Nano, updatedAt)
	if err != nil || count < 0 {
		return nil, nil
	}
	var snapshot IssueCommentReuse
	var source string
	err = s.q().QueryRowContext(ctx, `
		select t.id, r.source_updated_at, r.observation_sequence
		from threads t
		join thread_child_observation_reservations r on r.thread_id = t.id and r.family = 'comments'
		where t.repo_id = ? and t.number = ?
			and not exists (
				select 1 from sync_attempt_failures f
				where f.repo_id = t.repo_id and f.number = t.number
					and f.operation = 'issue_comments' and f.resolved_at is null
			)
	`, repoID, number).Scan(&snapshot.ThreadID, &source, &snapshot.ObservationSequence)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read reusable issue comments: %w", err)
	}
	observed, err := time.Parse(time.RFC3339Nano, source)
	if err != nil || !observed.Equal(updated) || snapshot.ObservationSequence <= 0 {
		return nil, nil
	}
	memberIDs, found, err := s.ThreadChildObservationMemberIDs(ctx, snapshot.ThreadID, ThreadChildComments, snapshot.ObservationSequence)
	if err != nil || !found {
		return nil, err
	}
	comments, err := s.ListComments(ctx, snapshot.ThreadID)
	if err != nil {
		return nil, err
	}
	byID := make(map[int64]Comment, len(comments))
	for _, comment := range comments {
		byID[comment.ID] = comment
	}
	for _, id := range memberIDs {
		comment, found := byID[id]
		if !found {
			return nil, nil
		}
		if comment.CommentType == "issue_comment" {
			// Portable pruning keeps observation membership but strips raw payloads
			// and may truncate bodies. Fetch again before claiming full evidence.
			if comment.RawJSON == "" || comment.Body == "" {
				return nil, nil
			}
			snapshot.CommentIDs = append(snapshot.CommentIDs, id)
		}
	}
	if len(snapshot.CommentIDs) != count {
		return nil, nil
	}
	return &snapshot, nil
}
