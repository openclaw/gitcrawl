package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

const syncAttemptFailuresSchemaVersion = 12

type SyncAttemptFailure struct {
	ID           int64  `json:"id"`
	RepoID       int64  `json:"repo_id"`
	ThreadID     int64  `json:"thread_id,omitempty"`
	Number       int    `json:"number"`
	Operation    string `json:"operation"`
	ErrorClass   string `json:"error_class"`
	ErrorMessage string `json:"error_message"`
	FirstSeenAt  string `json:"first_seen_at"`
	LastSeenAt   string `json:"last_seen_at"`
	RetryCount   int    `json:"retry_count"`
	ResolvedAt   string `json:"resolved_at,omitempty"`
}

type SyncAttemptFailureListOptions struct {
	RepoID          int64
	IncludeResolved bool
	Limit           int
}

func (s *Store) RecordSyncAttemptFailure(ctx context.Context, failure SyncAttemptFailure) (int64, error) {
	if failure.RepoID == 0 {
		return 0, fmt.Errorf("record sync attempt failure: missing repo id")
	}
	if failure.Number <= 0 {
		return 0, fmt.Errorf("record sync attempt failure: missing issue or pull request number")
	}
	operation := strings.TrimSpace(failure.Operation)
	if operation == "" {
		return 0, fmt.Errorf("record sync attempt failure: missing operation")
	}
	errorClass := strings.TrimSpace(failure.ErrorClass)
	if errorClass == "" {
		errorClass = "error"
	}
	message := strings.TrimSpace(failure.ErrorMessage)
	if message == "" {
		message = errorClass
	}
	firstSeen := strings.TrimSpace(failure.FirstSeenAt)
	if firstSeen == "" {
		firstSeen = failure.LastSeenAt
	}
	lastSeen := strings.TrimSpace(failure.LastSeenAt)
	if lastSeen == "" {
		lastSeen = firstSeen
	}
	var threadID any
	if failure.ThreadID != 0 {
		threadID = failure.ThreadID
	}
	if _, err := s.q().ExecContext(ctx, `
insert into sync_attempt_failures(repo_id, thread_id, number, operation, error_class, error_message, first_seen_at, last_seen_at, retry_count, resolved_at)
values(?, ?, ?, ?, ?, ?, ?, ?, 0, null)
on conflict(repo_id, number, operation, error_class) do update set
  thread_id=coalesce(excluded.thread_id, sync_attempt_failures.thread_id),
  error_message=excluded.error_message,
  last_seen_at=excluded.last_seen_at,
  retry_count=sync_attempt_failures.retry_count + 1,
  resolved_at=null
`, failure.RepoID, threadID, failure.Number, operation, errorClass, message, firstSeen, lastSeen); err != nil {
		return 0, fmt.Errorf("record sync attempt failure: %w", err)
	}
	var id int64
	if err := s.q().QueryRowContext(ctx, `
select id
from sync_attempt_failures
where repo_id = ? and number = ? and operation = ? and error_class = ?
`, failure.RepoID, failure.Number, operation, errorClass).Scan(&id); err != nil {
		return 0, fmt.Errorf("read sync attempt failure id: %w", err)
	}
	return id, nil
}

func (s *Store) ResolveSyncAttemptFailures(ctx context.Context, repoID int64, number int, resolvedAt string) (int, error) {
	if repoID == 0 || number <= 0 {
		return 0, nil
	}
	result, err := s.q().ExecContext(ctx, `
update sync_attempt_failures
set resolved_at = ?, last_seen_at = ?
where repo_id = ?
  and number = ?
  and resolved_at is null
`, resolvedAt, resolvedAt, repoID, number)
	if err != nil {
		return 0, fmt.Errorf("resolve sync attempt failures: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("count resolved sync attempt failures: %w", err)
	}
	return int(affected), nil
}

func (s *Store) ListSyncAttemptFailures(ctx context.Context, options SyncAttemptFailureListOptions) ([]SyncAttemptFailure, error) {
	if options.RepoID == 0 {
		return nil, fmt.Errorf("list sync attempt failures: missing repo id")
	}
	if !s.hasTable(ctx, "sync_attempt_failures") {
		current, err := s.schemaVersion(ctx)
		if err != nil {
			return nil, fmt.Errorf("list sync attempt failures: %w", err)
		}
		if current < syncAttemptFailuresSchemaVersion {
			return []SyncAttemptFailure{}, nil
		}
		return nil, fmt.Errorf("list sync attempt failures: missing sync_attempt_failures table")
	}
	limit := options.Limit
	if limit <= 0 {
		limit = 50
	}
	whereResolved := "and resolved_at is null"
	if options.IncludeResolved {
		whereResolved = ""
	}
	rows, err := s.q().QueryContext(ctx, `
select id, repo_id, thread_id, number, operation, error_class, error_message, first_seen_at, last_seen_at, retry_count, resolved_at
from sync_attempt_failures
where repo_id = ?
`+whereResolved+`
order by resolved_at is not null, last_seen_at desc, id desc
limit ?`, options.RepoID, limit)
	if err != nil {
		return nil, fmt.Errorf("list sync attempt failures: %w", err)
	}
	defer rows.Close()
	out := make([]SyncAttemptFailure, 0)
	for rows.Next() {
		var failure SyncAttemptFailure
		var threadID sql.NullInt64
		var resolvedAt sql.NullString
		if err := rows.Scan(
			&failure.ID,
			&failure.RepoID,
			&threadID,
			&failure.Number,
			&failure.Operation,
			&failure.ErrorClass,
			&failure.ErrorMessage,
			&failure.FirstSeenAt,
			&failure.LastSeenAt,
			&failure.RetryCount,
			&resolvedAt,
		); err != nil {
			return nil, fmt.Errorf("scan sync attempt failure: %w", err)
		}
		if threadID.Valid {
			failure.ThreadID = threadID.Int64
		}
		if resolvedAt.Valid {
			failure.ResolvedAt = resolvedAt.String
		}
		out = append(out, failure)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scan sync attempt failures: %w", err)
	}
	return out, nil
}
