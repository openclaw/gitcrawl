package store

import (
	"context"
	"database/sql"
	"fmt"
)

// threadContentColumns are the stored thread columns, other than the payload
// fields compared by samePayload, whose change makes a new content version.
type threadContentColumns struct {
	githubID          string
	title             string
	body              string
	authorLogin       string
	authorType        string
	authorAssociation string
	htmlURL           string
	labelsJSON        string
	assigneesJSON     string
	createdAt         string
	closedAt          string
	mergedAt          string
}

func (c threadContentColumns) equal(thread Thread, storedSourceUpdatedAt string) bool {
	return c.githubID == thread.GitHubID &&
		c.title == thread.Title &&
		c.body == thread.Body &&
		c.authorLogin == thread.AuthorLogin &&
		c.authorType == thread.AuthorType &&
		c.authorAssociation == thread.AuthorAssociation &&
		c.htmlURL == thread.HTMLURL &&
		c.labelsJSON == thread.LabelsJSON &&
		c.assigneesJSON == thread.AssigneesJSON &&
		c.createdAt == thread.CreatedAtGitHub &&
		c.closedAt == thread.ClosedAtGitHub &&
		c.mergedAt == thread.MergedAtGitHub &&
		storedSourceUpdatedAt == thread.UpdatedAtGitHub
}

// threadPayloadEqual compares thread envelopes. With record checks enabled,
// merge state is excluded: it is recorded as events, never thread content.
func threadPayloadEqual(recordChecks bool, stored, incoming string) bool {
	if stored == incoming {
		return true
	}
	if !recordChecks {
		return false
	}
	return threadContentRaw(stored) == threadContentRaw(incoming)
}

func threadCheckTime(thread Thread) string {
	if thread.LastPulledAt != "" {
		return thread.LastPulledAt
	}
	return thread.UpdatedAt
}

// ensureThreadCheckRow seeds a thread's check row from its content row, so the
// effective bookkeeping is unchanged until the first check is recorded.
func (s *Store) ensureThreadCheckRow(ctx context.Context, threadID int64) error {
	if _, err := s.q().ExecContext(ctx, `
		insert or ignore into thread_checks(
			thread_id, first_pulled_at, last_pulled_at, observation_sequence, evidence_complete,
			evidence_observation_sequence, evidence_source_updated_at, checked_at
		)
		select id, first_pulled_at, last_pulled_at, abs(observation_sequence), observation_sequence > 0,
			evidence_observation_sequence, evidence_source_updated_at, updated_at
		from threads
		where id = ?
	`, threadID); err != nil {
		return fmt.Errorf("seed thread check: %w", err)
	}
	return nil
}

// effectiveThreadObservationSequence returns the signed observation sequence
// of the latest accepted observation. The check row wins when it is at least
// as new as the content row; an older binary that rewrote the content row
// after a rollback therefore still orders correctly.
func (s *Store) effectiveThreadObservationSequence(ctx context.Context, threadID, contentSequence int64) (int64, error) {
	var sequence int64
	var complete bool
	err := s.q().QueryRowContext(ctx, `
		select observation_sequence, evidence_complete from thread_checks where thread_id = ?
	`, threadID).Scan(&sequence, &complete)
	if err == sql.ErrNoRows {
		return contentSequence, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read thread check: %w", err)
	}
	if sequence < observationSequenceOrderValue(contentSequence) {
		return contentSequence, nil
	}
	if !complete {
		return -sequence, nil
	}
	return sequence, nil
}

// recordThreadCheck records one accepted thread observation. storedSequence
// carries the legacy sign: negative means incomplete child evidence.
func (s *Store) recordThreadCheck(ctx context.Context, threadID int64, thread Thread, storedSequence int64) error {
	if err := s.ensureThreadCheckRow(ctx, threadID); err != nil {
		return err
	}
	if _, err := s.q().ExecContext(ctx, `
		update thread_checks
		set first_pulled_at = coalesce(first_pulled_at, nullif(?1, '')),
			last_pulled_at = coalesce(nullif(?2, ''), last_pulled_at),
			observation_sequence = ?3,
			evidence_complete = ?4,
			checked_at = coalesce(nullif(?5, ''), checked_at)
		where thread_id = ?6
	`, thread.FirstPulledAt, thread.LastPulledAt, observationSequenceOrderValue(storedSequence),
		boolInt(storedSequence > 0), thread.UpdatedAt, threadID); err != nil {
		return fmt.Errorf("record thread check: %w", err)
	}
	return nil
}

// reserveThreadCheckEvidence is reserveThreadEvidenceObservation for the
// split layout: the evidence pointer lives in thread_checks.
func (s *Store) reserveThreadCheckEvidence(ctx context.Context, threadID int64, sourceUpdatedAt string, sequence int64) (bool, error) {
	if err := s.ensureThreadCheckRow(ctx, threadID); err != nil {
		return false, err
	}
	var contentSource, checkSource string
	var contentSequence, checkSequence int64
	if err := s.q().QueryRowContext(ctx, `
		select t.evidence_source_updated_at, t.evidence_observation_sequence,
			c.evidence_source_updated_at, c.evidence_observation_sequence
		from threads t join thread_checks c on c.thread_id = t.id
		where t.id = ?
	`, threadID).Scan(&contentSource, &contentSequence, &checkSource, &checkSequence); err != nil {
		return false, fmt.Errorf("read thread evidence observation: %w", err)
	}
	currentSource, currentSequence := checkSource, checkSequence
	order, err := compareObservationOrder(observationOrder{SourceUpdatedAt: contentSource, ObservationSequence: contentSequence}, observationOrder{SourceUpdatedAt: checkSource, ObservationSequence: checkSequence})
	if err != nil {
		return false, err
	}
	if order > 0 {
		currentSource, currentSequence = contentSource, contentSequence
	}
	if currentSequence != 0 {
		order, err := compareObservationOrder(
			observationOrder{SourceUpdatedAt: sourceUpdatedAt, ObservationSequence: sequence},
			observationOrder{SourceUpdatedAt: currentSource, ObservationSequence: currentSequence},
		)
		if err != nil {
			return false, fmt.Errorf("compare thread evidence observation order: %w", err)
		}
		if order <= 0 {
			return false, nil
		}
	}
	if _, err := s.q().ExecContext(ctx, `
		update thread_checks
		set evidence_source_updated_at = ?, evidence_observation_sequence = ?
		where thread_id = ?
	`, sourceUpdatedAt, sequence, threadID); err != nil {
		return false, fmt.Errorf("reserve thread evidence observation: %w", err)
	}
	return true, nil
}
