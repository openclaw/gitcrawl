package store

import (
	"context"
	"fmt"
	"math"
	"strings"
)

type observationOrder struct {
	SourceUpdatedAt     string
	ObservationSequence int64
}

func observationSequenceOrderValue(sequence int64) int64 {
	// Negative thread sequences mark metadata-only observations; their absolute
	// value still participates in durable fetch ordering.
	if sequence == math.MinInt64 {
		return math.MaxInt64
	}
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

func compareRevisionObservationOrder(incoming, current observationOrder) (int, error) {
	incomingTimestamp := strings.TrimSpace(incoming.SourceUpdatedAt)
	currentTimestamp := strings.TrimSpace(current.SourceUpdatedAt)
	_, incomingValid := timestampOrderKey(incomingTimestamp)
	_, currentValid := timestampOrderKey(currentTimestamp)
	if !incomingValid && !currentValid && incomingTimestamp != currentTimestamp {
		return 0, fmt.Errorf(
			"ambiguous malformed observation timestamps %q and %q",
			incomingTimestamp,
			currentTimestamp,
		)
	}

	switch {
	case incoming.ObservationSequence > 0 && current.ObservationSequence > 0:
		switch {
		case incoming.ObservationSequence < current.ObservationSequence:
			return -1, nil
		case incoming.ObservationSequence > current.ObservationSequence:
			return 1, nil
		}
	case incoming.ObservationSequence > 0:
		return 1, nil
	case current.ObservationSequence > 0:
		return -1, nil
	}
	return compareObservationOrder(incoming, current)
}

func observationOrderSQL(sourceExpression, sequenceExpression string) string {
	source := "trim(coalesce(" + sourceExpression + ", ''))"
	key := "gitcrawl_timestamp_key(nullif(" + source + ", ''))"
	return "case when " + key + " is not null then 1 else 0 end desc, " +
		key + " desc, case when " + key + " is null then " + source +
		" else '' end desc, " + sequenceExpression + " desc"
}

func workflowObservationOrderSQL(
	sourceExpression string,
	sequenceExpression string,
	hasUnknownSourceExpression string,
) string {
	source := "trim(coalesce(" + sourceExpression + ", ''))"
	key := "gitcrawl_timestamp_key(nullif(" + source + ", ''))"
	hasUnknownSource := "(" + hasUnknownSourceExpression + " <> 0)"
	return "case when " + hasUnknownSource + " then " + sequenceExpression + " end desc, " +
		"case when " + hasUnknownSource + " and " + source + " = '' then 1 else 0 end desc, " +
		"case when not " + hasUnknownSource + " and " + key + " is not null then 1 else 0 end desc, " +
		"case when not " + hasUnknownSource + " then " + key + " end desc, " +
		"case when not " + hasUnknownSource + " and " + key + " is null then " + source +
		" else '' end desc, " + sequenceExpression + " desc"
}

func observationSourceEquivalentSQL(leftExpression, rightExpression string) string {
	left := "trim(coalesce(" + leftExpression + ", ''))"
	right := "trim(coalesce(" + rightExpression + ", ''))"
	leftKey := "gitcrawl_timestamp_key(nullif(" + left + ", ''))"
	rightKey := "gitcrawl_timestamp_key(nullif(" + right + ", ''))"
	return "((" + leftKey + " is not null and " + rightKey + " is not null and " +
		leftKey + " = " + rightKey + ") or (" + leftKey + " is null and " +
		rightKey + " is null and " + left + " = " + right + "))"
}

func threadEvidenceRevisionOrderSQL(revisionAlias, threadAlias string) string {
	revision := sqliteIdentifier(revisionAlias) + "."
	thread := sqliteIdentifier(threadAlias) + "."
	return "case when " + observationSourceEquivalentSQL(
		revision+"source_updated_at",
		thread+"updated_at_gh",
	) + " then 1 else 0 end desc, " +
		observationOrderSQL(revision+"source_updated_at", revision+"observation_sequence") +
		", " + revision + "id desc"
}

func threadEvidenceTupleExistsSQL(revisionAlias, threadAlias string) string {
	revision := sqliteIdentifier(revisionAlias) + "."
	thread := sqliteIdentifier(threadAlias) + "."
	return `exists(
			select 1
			from thread_revisions ` + sqliteIdentifier(revisionAlias) + `
			where ` + revision + `thread_id = ` + thread + `id
				and ` + revision + `observation_sequence = ` + thread + `evidence_observation_sequence
		)`
}

func (s *Store) latestThreadRevisionOrder(ctx context.Context, alias string) string {
	qualified := sqliteIdentifier(alias) + "."
	parts := make([]string, 0, 3)
	if s.hasColumn(ctx, "thread_revisions", "observation_sequence") {
		parts = append(parts, qualified+"observation_sequence desc")
	}
	if s.hasColumn(ctx, "thread_revisions", "source_updated_at") {
		parts = append(parts, "gitcrawl_timestamp_key(nullif("+qualified+"source_updated_at, '')) desc")
	}
	parts = append(parts, qualified+"id desc")
	return strings.Join(parts, ", ")
}

func (s *Store) latestThreadRevisionConsumerOrder(
	ctx context.Context,
	revisionAlias string,
	threadAlias string,
) string {
	freshness := s.threadRevisionFreshnessPredicate(ctx, revisionAlias, threadAlias)
	return "case when (" + freshness + ") then 1 else 0 end desc, " +
		s.latestThreadRevisionOrder(ctx, revisionAlias)
}

func (s *Store) threadRevisionFreshnessPredicate(
	ctx context.Context,
	revisionAlias string,
	threadAlias string,
) string {
	revision := sqliteIdentifier(revisionAlias) + "."
	thread := sqliteIdentifier(threadAlias) + "."
	sequenceFloorColumn := ""
	if s.hasColumn(ctx, "thread_revisions", "observation_sequence") {
		switch {
		case s.hasColumn(ctx, "threads", "evidence_observation_sequence"):
			sequenceFloorColumn = "evidence_observation_sequence"
		case s.hasColumn(ctx, "threads", "observation_sequence"):
			sequenceFloorColumn = "observation_sequence"
		}
	}
	revisionLegacyTimestamp := "gitcrawl_timestamp_key(" + revision + "created_at)"
	revisionSourceTimestamp := "null"
	revisionSourceUsable := "1 = 1"
	if s.hasColumn(ctx, "thread_revisions", "source_updated_at") {
		revisionLegacyTimestamp = "gitcrawl_timestamp_key(coalesce(nullif(" +
			revision + "source_updated_at, ''), " + revision + "created_at))"
		revisionSourceTimestamp = "gitcrawl_timestamp_key(nullif(" +
			revision + "source_updated_at, ''))"
		revisionSourceUsable = revisionSourceTimestamp + " is not null or trim(coalesce(" +
			revision + "source_updated_at, '')) = ''"
	}
	threadLegacyTimestamp := "gitcrawl_timestamp_key(" + thread + "updated_at)"
	threadSourceTimestamp := "null"
	threadSourceUsable := "1 = 1"
	if s.hasColumn(ctx, "threads", "updated_at_gh") {
		threadLegacyTimestamp = "gitcrawl_timestamp_key(coalesce(nullif(" +
			thread + "updated_at_gh, ''), " + thread + "updated_at))"
		threadSourceTimestamp = "gitcrawl_timestamp_key(nullif(" + thread + "updated_at_gh, ''))"
		threadSourceUsable = threadSourceTimestamp + " is not null or trim(coalesce(" +
			thread + "updated_at_gh, '')) = ''"
	}
	legacyClockFresh := revisionLegacyTimestamp + " is not null and " +
		threadLegacyTimestamp + " is not null and " +
		revisionLegacyTimestamp + " >= " + threadLegacyTimestamp

	if sequenceFloorColumn != "" {
		sequenceFloor := thread + sequenceFloorColumn
		sourceClockFresh := "(" + revisionSourceUsable + ") and (" + threadSourceUsable +
			") and (" + revisionSourceTimestamp + " is null or " +
			threadSourceTimestamp + " is null or " +
			revisionSourceTimestamp + " >= " + threadSourceTimestamp + ")"
		sequenceFresh := sequenceFloor + " > 0 and " +
			revision + "observation_sequence > 0 and " +
			revision + "observation_sequence >= " + sequenceFloor
		return "(((" + sequenceFresh + ") and (" + sourceClockFresh + ")) or (" +
			sequenceFloor + " = 0 and (" + legacyClockFresh + ")))"
	}

	return legacyClockFresh
}
