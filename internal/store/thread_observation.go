package store

import (
	"context"
	"fmt"
	"strings"
	"time"
)

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
		set value = max(
			value,
			coalesce((select max(observation_sequence) from thread_revisions), 0)
		) + 1,
			last_started_at = ?
		where id = 1
		returning value
	`, startedAt).Scan(&sequence); err != nil {
		return 0, fmt.Errorf("advance thread observation sequence at %s: %w", startedAt, err)
	}
	return sequence, nil
}
