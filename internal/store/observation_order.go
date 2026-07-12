package store

import (
	"context"
	"fmt"
	"strings"
)

type observationOrder struct {
	SourceUpdatedAt     string
	ObservationSequence int64
}

func compareObservationOrder(incoming, current observationOrder) (int, error) {
	incomingTimestamp := strings.TrimSpace(incoming.SourceUpdatedAt)
	currentTimestamp := strings.TrimSpace(current.SourceUpdatedAt)
	incomingKey, incomingValid := timestampOrderKey(incomingTimestamp)
	currentKey, currentValid := timestampOrderKey(currentTimestamp)

	switch {
	case incomingValid && currentValid:
		switch {
		case incomingKey < currentKey:
			return -1, nil
		case incomingKey > currentKey:
			return 1, nil
		}
	case incomingValid:
		return 1, nil
	case currentValid:
		return -1, nil
	case incomingTimestamp != currentTimestamp:
		return 0, fmt.Errorf(
			"ambiguous malformed observation timestamps %q and %q",
			incomingTimestamp,
			currentTimestamp,
		)
	}

	switch {
	case incoming.ObservationSequence < current.ObservationSequence:
		return -1, nil
	case incoming.ObservationSequence > current.ObservationSequence:
		return 1, nil
	default:
		return 0, nil
	}
}

func (s *Store) latestThreadRevisionOrder(ctx context.Context, alias string) string {
	qualified := sqliteIdentifier(alias) + "."
	parts := make([]string, 0, 3)
	if s.hasColumn(ctx, "thread_revisions", "source_updated_at") {
		parts = append(parts, "gitcrawl_timestamp_key(nullif("+qualified+"source_updated_at, '')) desc")
	}
	if s.hasColumn(ctx, "thread_revisions", "observation_sequence") {
		parts = append(parts, qualified+"observation_sequence desc")
	}
	parts = append(parts, qualified+"id desc")
	return strings.Join(parts, ", ")
}
