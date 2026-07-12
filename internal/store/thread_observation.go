package store

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/openclaw/gitcrawl/internal/store/storedb"
)

type ThreadChildObservationFamily string

const (
	ThreadChildComments           ThreadChildObservationFamily = "comments"
	ThreadChildPullRequestDetails ThreadChildObservationFamily = "pull_request_details"
	ThreadChildPullRequestFiles   ThreadChildObservationFamily = "pull_request_files"
	ThreadChildPullRequestCommits ThreadChildObservationFamily = "pull_request_commits"
	ThreadChildPullRequestChecks  ThreadChildObservationFamily = "pull_request_checks"
	ThreadChildReviewThreads      ThreadChildObservationFamily = "pull_request_review_threads"
)

var threadChildObservationFamilies = []ThreadChildObservationFamily{
	ThreadChildComments,
	ThreadChildPullRequestDetails,
	ThreadChildPullRequestFiles,
	ThreadChildPullRequestCommits,
	ThreadChildPullRequestChecks,
	ThreadChildReviewThreads,
}

func (s *Store) NextThreadObservationSequence(ctx context.Context, startedAt string) (int64, error) {
	if strings.TrimSpace(startedAt) == "" {
		startedAt = time.Now().UTC().Format(timeLayout)
	}
	if _, err := s.q().ExecContext(ctx, `
		insert into thread_observation_sequence(id, value, last_started_at)
		values(1, 0, '')
		on conflict(id) do nothing
	`); err != nil {
		return 0, fmt.Errorf("initialize thread observation sequence: %w", err)
	}
	var sequence int64
	if err := s.q().QueryRowContext(ctx, `
			update thread_observation_sequence
			set value = value + 1,
				last_started_at = ?
			where id = 1
			returning value
	`, startedAt).Scan(&sequence); err != nil {
		return 0, fmt.Errorf("advance thread observation sequence at %s: %w", startedAt, err)
	}
	return sequence, nil
}

func (s *Store) ReserveThreadChildObservation(
	ctx context.Context,
	threadID int64,
	family ThreadChildObservationFamily,
	sequence int64,
) (bool, error) {
	if threadID <= 0 {
		return false, fmt.Errorf("thread id must be positive")
	}
	if !validThreadChildObservationFamily(family) {
		return false, fmt.Errorf("unsupported thread child observation family %q", family)
	}
	if sequence <= 0 {
		return false, fmt.Errorf("observation sequence must be positive")
	}
	updated, err := s.qsql().ReserveThreadChildObservation(
		ctx,
		storedb.ReserveThreadChildObservationParams{
			ThreadID:            threadID,
			Family:              string(family),
			ObservationSequence: sequence,
		},
	)
	if err != nil {
		return false, fmt.Errorf("reserve thread child observation %q: %w", family, err)
	}
	return updated > 0, nil
}

func validThreadChildObservationFamily(family ThreadChildObservationFamily) bool {
	for _, candidate := range threadChildObservationFamilies {
		if family == candidate {
			return true
		}
	}
	return false
}

func (s *Store) ReserveWorkflowRunObservation(
	ctx context.Context,
	repoID int64,
	headSHA string,
	sequence int64,
) (bool, error) {
	if repoID <= 0 {
		return false, fmt.Errorf("repository id must be positive")
	}
	headSHA = strings.TrimSpace(headSHA)
	if headSHA == "" {
		return false, fmt.Errorf("head SHA must not be empty")
	}
	if sequence <= 0 {
		return false, fmt.Errorf("observation sequence must be positive")
	}
	updated, err := s.qsql().ReserveWorkflowRunObservation(
		ctx,
		storedb.ReserveWorkflowRunObservationParams{
			RepoID:              repoID,
			HeadSha:             headSHA,
			ObservationSequence: sequence,
		},
	)
	if err != nil {
		return false, fmt.Errorf("reserve workflow run observation for %s: %w", headSHA, err)
	}
	return updated > 0, nil
}
