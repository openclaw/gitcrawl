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

func observationSequenceOrderValue(sequence int64) int64 {
	// Negative thread sequences mark metadata-only observations; their absolute
	// value still participates in durable fetch ordering.
	if sequence < 0 {
		return -sequence
	}
	return sequence
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

func (s *Store) threadRevisionFreshnessPredicate(
	ctx context.Context,
	revisionAlias string,
	threadAlias string,
) string {
	revision := sqliteIdentifier(revisionAlias) + "."
	thread := sqliteIdentifier(threadAlias) + "."
	hasRevisionSource := s.hasColumn(ctx, "thread_revisions", "source_updated_at")
	hasThreadSource := s.hasColumn(ctx, "threads", "updated_at_gh")
	hasSequence := s.hasColumn(ctx, "thread_revisions", "observation_sequence") &&
		s.hasColumn(ctx, "threads", "observation_sequence")

	if hasSequence {
		sequenceFresh := thread + "observation_sequence >= 0 and (" +
			revision + "observation_sequence <= 0 or " +
			thread + "observation_sequence = 0 or " +
			revision + "observation_sequence >= " + thread + "observation_sequence)"
		if !hasRevisionSource || !hasThreadSource {
			return sequenceFresh
		}
		revisionTimestamp := "gitcrawl_timestamp_key(nullif(" + revision + "source_updated_at, ''))"
		threadTimestamp := "gitcrawl_timestamp_key(nullif(" + thread + "updated_at_gh, ''))"
		revisionClockUsable := revisionTimestamp + " is not null or trim(coalesce(" +
			revision + "source_updated_at, '')) = ''"
		threadClockUsable := threadTimestamp + " is not null or trim(coalesce(" +
			thread + "updated_at_gh, '')) = ''"
		return "((" + revisionTimestamp + " is not null and " + threadTimestamp + " is not null and (" +
			sequenceFresh + ") and " + revisionTimestamp + " >= " + threadTimestamp + ") or ((" +
			revisionTimestamp + " is null or " + threadTimestamp + " is null) and " +
			"(" + revisionClockUsable + ") and (" + threadClockUsable + ") and " +
			sequenceFresh + "))"
	}

	if !hasRevisionSource {
		return "0 = 1"
	}
	threadTimestamp := "gitcrawl_timestamp_key(" + thread + "updated_at)"
	if hasThreadSource {
		threadTimestamp = "gitcrawl_timestamp_key(coalesce(nullif(" + thread + "updated_at_gh, ''), " +
			thread + "updated_at))"
	}
	return "gitcrawl_timestamp_key(nullif(" + revision + "source_updated_at, '')) >= " + threadTimestamp
}
