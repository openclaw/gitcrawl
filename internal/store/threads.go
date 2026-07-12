package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/openclaw/gitcrawl/internal/store/storedb"
)

type Thread struct {
	ID                int64  `json:"id"`
	RepoID            int64  `json:"repo_id"`
	GitHubID          string `json:"github_id"`
	Number            int    `json:"number"`
	Kind              string `json:"kind"`
	State             string `json:"state"`
	Title             string `json:"title"`
	Body              string `json:"body,omitempty"`
	AuthorLogin       string `json:"author_login,omitempty"`
	AuthorType        string `json:"author_type,omitempty"`
	AuthorAssociation string `json:"author_association,omitempty"`
	HTMLURL           string `json:"html_url"`
	LabelsJSON        string `json:"labels_json"`
	AssigneesJSON     string `json:"assignees_json"`
	RawJSON           string `json:"-"`
	ContentHash       string `json:"content_hash"`
	IsDraft           bool   `json:"is_draft"`
	CreatedAtGitHub   string `json:"created_at_gh,omitempty"`
	UpdatedAtGitHub   string `json:"updated_at_gh,omitempty"`
	ClosedAtGitHub    string `json:"closed_at_gh,omitempty"`
	MergedAtGitHub    string `json:"merged_at_gh,omitempty"`
	FirstPulledAt     string `json:"first_pulled_at,omitempty"`
	LastPulledAt      string `json:"last_pulled_at,omitempty"`
	UpdatedAt         string `json:"updated_at"`
	ClosedAtLocal     string `json:"closed_at_local,omitempty"`
	CloseReasonLocal  string `json:"close_reason_local,omitempty"`
}

type UpsertThreadOptions struct {
	PreserveDraft bool
	// IncompleteEvidence stores the sequence as negative until full evidence for
	// the same source revision arrives.
	IncompleteEvidence  bool
	ObservationSequence int64
}

type UpsertThreadResult struct {
	ID              int64
	Applied         bool
	EvidenceApplied bool
	PreviousState   string
	// ObservationSequence is the effective absolute parent snapshot generation.
	ObservationSequence int64
	// EvidenceObservationSequence is the accepted complete child-evidence generation.
	EvidenceObservationSequence int64
}

func (s *Store) UpsertThread(ctx context.Context, thread Thread, options ...UpsertThreadOptions) (int64, error) {
	result, err := s.UpsertThreadObservation(ctx, thread, options...)
	return result.ID, err
}

func (s *Store) UpsertThreadObservation(ctx context.Context, thread Thread, options ...UpsertThreadOptions) (UpsertThreadResult, error) {
	var upsertOptions UpsertThreadOptions
	if len(options) > 0 {
		upsertOptions = options[0]
	}
	if s.queries != nil {
		return s.upsertThreadObservation(ctx, thread, upsertOptions)
	}
	var result UpsertThreadResult
	err := s.WithTx(ctx, func(tx *Store) error {
		var err error
		result, err = tx.upsertThreadObservation(ctx, thread, upsertOptions)
		return err
	})
	return result, err
}

func (s *Store) upsertThreadObservation(ctx context.Context, thread Thread, options UpsertThreadOptions) (UpsertThreadResult, error) {
	if options.ObservationSequence <= 0 {
		sequence, err := s.NextThreadObservationSequence(ctx, thread.UpdatedAt)
		if err != nil {
			return UpsertThreadResult{}, err
		}
		options.ObservationSequence = sequence
	}

	var existing struct {
		id                  int64
		sourceUpdatedAt     string
		observationSequence int64
		rawJSON             string
		contentHash         string
		state               string
		isDraft             int64
	}
	err := s.q().QueryRowContext(ctx, `
		select id, coalesce(updated_at_gh, ''), observation_sequence,
			coalesce(raw_json, ''), content_hash, state, is_draft
		from threads
		where repo_id = ? and kind = ? and number = ?
	`, thread.RepoID, thread.Kind, thread.Number).Scan(
		&existing.id,
		&existing.sourceUpdatedAt,
		&existing.observationSequence,
		&existing.rawJSON,
		&existing.contentHash,
		&existing.state,
		&existing.isDraft,
	)
	if err != nil && err != sql.ErrNoRows {
		return UpsertThreadResult{}, fmt.Errorf("read current thread observation: %w", err)
	}
	exists := err == nil
	expectedDraft := int64(boolInt(thread.IsDraft))
	if exists && options.PreserveDraft {
		expectedDraft = existing.isDraft
	}
	samePayload := exists &&
		existing.rawJSON == thread.RawJSON &&
		existing.contentHash == thread.ContentHash &&
		existing.state == thread.State &&
		existing.isDraft == expectedDraft
	storedObservationSequence := options.ObservationSequence
	if options.IncompleteEvidence {
		storedObservationSequence = -storedObservationSequence
		if samePayload {
			storedObservationSequence = existing.observationSequence
		}
	}
	if exists && samePayload && !options.IncompleteEvidence && existing.observationSequence < 0 {
		storedObservationSequence = max(
			storedObservationSequence,
			observationSequenceOrderValue(existing.observationSequence),
		)
	}
	if exists {
		sourceOrder, err := compareObservationOrder(
			observationOrder{SourceUpdatedAt: thread.UpdatedAtGitHub},
			observationOrder{SourceUpdatedAt: existing.sourceUpdatedAt},
		)
		if err != nil {
			return UpsertThreadResult{}, fmt.Errorf("compare thread observation source order: %w", err)
		}
		order, err := compareObservationOrder(
			observationOrder{
				SourceUpdatedAt:     thread.UpdatedAtGitHub,
				ObservationSequence: observationSequenceOrderValue(storedObservationSequence),
			},
			observationOrder{
				SourceUpdatedAt:     existing.sourceUpdatedAt,
				ObservationSequence: observationSequenceOrderValue(existing.observationSequence),
			},
		)
		if err != nil {
			return UpsertThreadResult{}, fmt.Errorf("compare thread observation order: %w", err)
		}
		if sourceOrder == 0 && samePayload && !options.IncompleteEvidence && existing.observationSequence < 0 {
			order = 1
		}
		if order < 0 {
			evidenceApplied := false
			if sourceOrder == 0 && samePayload && !options.IncompleteEvidence {
				evidenceApplied, err = s.reserveThreadEvidenceObservation(
					ctx,
					existing.id,
					options.ObservationSequence,
				)
				if err != nil {
					return UpsertThreadResult{}, err
				}
			}
			return UpsertThreadResult{
				ID:                          existing.id,
				Applied:                     false,
				EvidenceApplied:             evidenceApplied,
				PreviousState:               existing.state,
				ObservationSequence:         observationSequenceOrderValue(existing.observationSequence),
				EvidenceObservationSequence: evidenceObservationSequence(evidenceApplied, options.ObservationSequence),
			}, nil
		}
		if order == 0 {
			if samePayload {
				if !options.IncompleteEvidence {
					evidenceApplied, err := s.reserveThreadEvidenceObservation(
						ctx,
						existing.id,
						options.ObservationSequence,
					)
					if err != nil {
						return UpsertThreadResult{}, err
					}
					return UpsertThreadResult{
						ID:                          existing.id,
						Applied:                     false,
						EvidenceApplied:             evidenceApplied,
						PreviousState:               existing.state,
						ObservationSequence:         observationSequenceOrderValue(existing.observationSequence),
						EvidenceObservationSequence: evidenceObservationSequence(evidenceApplied, options.ObservationSequence),
					}, nil
				}
			} else {
				return UpsertThreadResult{}, fmt.Errorf(
					"conflicting thread observations share sequence %d",
					observationSequenceOrderValue(storedObservationSequence),
				)
			}
		}
	}
	params := storedb.UpsertThreadParams{
		RepoID:              thread.RepoID,
		GithubID:            thread.GitHubID,
		Number:              int64(thread.Number),
		Kind:                thread.Kind,
		State:               thread.State,
		Title:               thread.Title,
		Body:                nullString(thread.Body),
		AuthorLogin:         nullString(thread.AuthorLogin),
		AuthorType:          nullString(thread.AuthorType),
		AuthorAssociation:   nullString(thread.AuthorAssociation),
		HtmlUrl:             thread.HTMLURL,
		LabelsJson:          thread.LabelsJSON,
		AssigneesJson:       thread.AssigneesJSON,
		RawJson:             thread.RawJSON,
		ContentHash:         thread.ContentHash,
		IsDraft:             int64(boolInt(thread.IsDraft)),
		CreatedAtGh:         nullString(thread.CreatedAtGitHub),
		UpdatedAtGh:         nullString(thread.UpdatedAtGitHub),
		ClosedAtGh:          nullString(thread.ClosedAtGitHub),
		MergedAtGh:          nullString(thread.MergedAtGitHub),
		FirstPulledAt:       nullString(thread.FirstPulledAt),
		LastPulledAt:        nullString(thread.LastPulledAt),
		ObservationSequence: storedObservationSequence,
		UpdatedAt:           thread.UpdatedAt,
	}
	var id int64
	if options.PreserveDraft {
		id, err = s.qsql().UpsertThreadPreservingDraft(ctx, storedb.UpsertThreadPreservingDraftParams(params))
	} else {
		id, err = s.qsql().UpsertThread(ctx, params)
	}
	if err != nil {
		return UpsertThreadResult{}, fmt.Errorf("upsert thread: %w", err)
	}
	evidenceApplied := false
	if !options.IncompleteEvidence {
		evidenceApplied, err = s.reserveThreadEvidenceObservation(
			ctx,
			id,
			options.ObservationSequence,
		)
		if err != nil {
			return UpsertThreadResult{}, err
		}
	}
	return UpsertThreadResult{
		ID:                          id,
		Applied:                     true,
		EvidenceApplied:             evidenceApplied,
		PreviousState:               existing.state,
		ObservationSequence:         observationSequenceOrderValue(storedObservationSequence),
		EvidenceObservationSequence: evidenceObservationSequence(evidenceApplied, options.ObservationSequence),
	}, nil
}

func (s *Store) reserveThreadEvidenceObservation(ctx context.Context, threadID, sequence int64) (bool, error) {
	updated, err := s.qsql().ReserveThreadEvidenceObservation(
		ctx,
		storedb.ReserveThreadEvidenceObservationParams{
			ID:                          threadID,
			EvidenceObservationSequence: sequence,
		},
	)
	if err != nil {
		return false, fmt.Errorf("reserve thread evidence observation: %w", err)
	}
	return updated > 0, nil
}

func evidenceObservationSequence(applied bool, sequence int64) int64 {
	if !applied {
		return 0
	}
	return sequence
}

func (s *Store) MarkOpenThreadClosedFromGitHub(ctx context.Context, thread Thread) (bool, error) {
	if thread.RepoID <= 0 {
		return false, fmt.Errorf("repo id must be positive")
	}
	if thread.Number <= 0 {
		return false, fmt.Errorf("thread number must be positive")
	}
	if thread.Kind == "" {
		return false, fmt.Errorf("thread kind is required")
	}
	if thread.State == "" {
		thread.State = "closed"
	}
	if s.queries == nil {
		var updated bool
		err := s.WithTx(ctx, func(tx *Store) error {
			var err error
			updated, err = tx.MarkOpenThreadClosedFromGitHub(ctx, thread)
			return err
		})
		return updated, err
	}
	var state string
	var closedAtLocal sql.NullString
	err := s.q().QueryRowContext(ctx, `
		select state, closed_at_local
		from threads
		where repo_id = ? and kind = ? and number = ?
	`, thread.RepoID, thread.Kind, thread.Number).Scan(&state, &closedAtLocal)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read open thread before github close: %w", err)
	}
	if state != "open" || closedAtLocal.Valid {
		return false, nil
	}
	result, err := s.upsertThreadObservation(ctx, thread, UpsertThreadOptions{
		PreserveDraft: thread.Kind == "pull_request",
	})
	if err != nil {
		return false, fmt.Errorf("mark open thread closed from github: %w", err)
	}
	return result.Applied, nil
}

func (s *Store) ListThreads(ctx context.Context, repoID int64, includeClosed bool) ([]Thread, error) {
	return s.ListThreadsFiltered(ctx, ThreadListOptions{RepoID: repoID, IncludeClosed: includeClosed})
}

type ThreadListOptions struct {
	RepoID        int64
	IncludeClosed bool
	Numbers       []int
	Limit         int
}

func (s *Store) ListThreadsFiltered(ctx context.Context, options ThreadListOptions) ([]Thread, error) {
	if len(options.Numbers) == 0 &&
		s.hasColumn(ctx, "threads", "body") &&
		s.hasColumn(ctx, "threads", "raw_json") &&
		s.hasColumn(ctx, "threads", "author_association") {
		rows, err := s.qsql().ListThreadsCurrentSchema(ctx, storedb.ListThreadsCurrentSchemaParams{
			RepoID:        options.RepoID,
			IncludeClosed: boolInt(options.IncludeClosed),
			RowLimit:      options.Limit,
		})
		if err != nil {
			return nil, fmt.Errorf("list threads: %w", err)
		}
		out := make([]Thread, 0, len(rows))
		for _, row := range rows {
			out = append(out, threadFromCurrentSchemaDB(row))
		}
		return out, nil
	}
	where := `repo_id = ?`
	args := []any{options.RepoID}
	if !options.IncludeClosed {
		where += ` and closed_at_local is null`
	}
	if len(options.Numbers) > 0 {
		placeholders := make([]string, 0, len(options.Numbers))
		for _, number := range options.Numbers {
			placeholders = append(placeholders, "?")
			args = append(args, number)
		}
		where += ` and number in (` + strings.Join(placeholders, ",") + `)`
	}
	limitSQL := ``
	if options.Limit > 0 {
		limitSQL = ` limit ?`
		args = append(args, options.Limit)
	}
	rows, err := s.q().QueryContext(ctx, `
		select `+s.threadSelectColumns(ctx, "")+`
		from threads
		where `+where+`
		order by number`+limitSQL, args...)
	if err != nil {
		return nil, fmt.Errorf("list threads: %w", err)
	}
	defer rows.Close()

	var out []Thread
	for rows.Next() {
		thread, err := scanThread(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, thread)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate threads: %w", err)
	}
	return out, nil
}

func (s *Store) CloseThreadLocally(ctx context.Context, repoID int64, number int, reason string) error {
	if repoID <= 0 {
		return fmt.Errorf("repo id must be positive")
	}
	if number <= 0 {
		return fmt.Errorf("thread number must be positive")
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "local close"
	}
	closedAt := time.Now().UTC().Format(timeLayout)
	affected, err := s.qsql().CloseThreadLocally(ctx, storedb.CloseThreadLocallyParams{
		ClosedAt: sql.NullString{String: closedAt, Valid: true},
		Reason:   sql.NullString{String: reason, Valid: true},
		RepoID:   repoID,
		Number:   int64(number),
	})
	if err != nil {
		return fmt.Errorf("close thread locally: %w", err)
	}
	if affected == 0 {
		return fmt.Errorf("thread #%d was not found", number)
	}
	return nil
}

func (s *Store) ReopenThreadLocally(ctx context.Context, repoID int64, number int) error {
	if repoID <= 0 {
		return fmt.Errorf("repo id must be positive")
	}
	if number <= 0 {
		return fmt.Errorf("thread number must be positive")
	}
	updatedAt := time.Now().UTC().Format(timeLayout)
	affected, err := s.qsql().ReopenThreadLocally(ctx, storedb.ReopenThreadLocallyParams{
		UpdatedAt: updatedAt,
		RepoID:    repoID,
		Number:    int64(number),
	})
	if err != nil {
		return fmt.Errorf("reopen thread locally: %w", err)
	}
	if affected == 0 {
		return fmt.Errorf("thread #%d was not found", number)
	}
	return nil
}

func scanThread(rows interface {
	Scan(dest ...any) error
}) (Thread, error) {
	var thread Thread
	var body, authorLogin, authorType, authorAssociation, rawJSON, createdAt, updatedAtGH, closedAt, mergedAt, firstPulled, lastPulled, closedLocal, closeReason sql.NullString
	var isDraft int
	if err := rows.Scan(&thread.ID, &thread.RepoID, &thread.GitHubID, &thread.Number, &thread.Kind, &thread.State, &thread.Title,
		&body, &authorLogin, &authorType, &authorAssociation, &thread.HTMLURL, &thread.LabelsJSON, &thread.AssigneesJSON, &rawJSON,
		&thread.ContentHash, &isDraft, &createdAt, &updatedAtGH, &closedAt, &mergedAt, &firstPulled, &lastPulled, &thread.UpdatedAt,
		&closedLocal, &closeReason); err != nil {
		return Thread{}, fmt.Errorf("scan thread: %w", err)
	}
	thread.Body = body.String
	thread.AuthorLogin = authorLogin.String
	thread.AuthorType = authorType.String
	thread.AuthorAssociation = authorAssociation.String
	thread.CreatedAtGitHub = createdAt.String
	thread.UpdatedAtGitHub = updatedAtGH.String
	thread.ClosedAtGitHub = closedAt.String
	thread.MergedAtGitHub = mergedAt.String
	thread.FirstPulledAt = firstPulled.String
	thread.LastPulledAt = lastPulled.String
	thread.ClosedAtLocal = closedLocal.String
	thread.CloseReasonLocal = closeReason.String
	thread.RawJSON = rawJSON.String
	thread.IsDraft = isDraft != 0
	return thread, nil
}

func (s *Store) threadSelectColumns(ctx context.Context, alias string) string {
	column := func(name string) string {
		if alias == "" {
			return name
		}
		return alias + "." + name
	}
	return strings.Join([]string{
		column("id"),
		column("repo_id"),
		column("github_id"),
		column("number"),
		column("kind"),
		column("state"),
		column("title"),
		s.threadBodyExpr(ctx, alias),
		column("author_login"),
		column("author_type"),
		s.threadOptionalColumnExpr(ctx, alias, "author_association"),
		column("html_url"),
		column("labels_json"),
		column("assignees_json"),
		s.threadRawJSONExpr(ctx, alias),
		column("content_hash"),
		column("is_draft"),
		column("created_at_gh"),
		column("updated_at_gh"),
		column("closed_at_gh"),
		column("merged_at_gh"),
		column("first_pulled_at"),
		column("last_pulled_at"),
		column("updated_at"),
		column("closed_at_local"),
		column("close_reason_local"),
	}, ", ")
}

func (s *Store) threadOptionalColumnExpr(ctx context.Context, alias, name string) string {
	if s.hasColumn(ctx, "threads", name) {
		return qualifiedColumn(alias, name)
	}
	return "''"
}

func (s *Store) threadBodyExpr(ctx context.Context, alias string) string {
	if s.hasColumn(ctx, "threads", "body") {
		return qualifiedColumn(alias, "body")
	}
	if s.hasColumn(ctx, "threads", "body_excerpt") {
		return qualifiedColumn(alias, "body_excerpt")
	}
	return "''"
}

func (s *Store) threadRawJSONExpr(ctx context.Context, alias string) string {
	if s.hasColumn(ctx, "threads", "raw_json") {
		return qualifiedColumn(alias, "raw_json")
	}
	return "''"
}

func qualifiedColumn(alias, name string) string {
	if alias == "" {
		return name
	}
	return alias + "." + name
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
