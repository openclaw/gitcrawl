package store

import (
	"context"
	"math"
	"path/filepath"
	"strings"
	"testing"
)

func TestObservationSequenceOrderValueHandlesMinInt64(t *testing.T) {
	if got := observationSequenceOrderValue(math.MinInt64); got != math.MaxInt64 {
		t.Fatalf("minimum sequence order value = %d, want %d", got, int64(math.MaxInt64))
	}
}

func TestCompareObservationOrder(t *testing.T) {
	tests := []struct {
		name     string
		incoming observationOrder
		current  observationOrder
		want     int
		wantErr  string
	}{
		{
			name: "newer source timestamp wins",
			incoming: observationOrder{
				SourceUpdatedAt: "2026-07-12T00:00:01Z", ObservationSequence: 1,
			},
			current: observationOrder{
				SourceUpdatedAt: "2026-07-12T00:00:00Z", ObservationSequence: 2,
			},
			want: 1,
		},
		{
			name: "equivalent source timestamps use sequence",
			incoming: observationOrder{
				SourceUpdatedAt: "2026-07-12T01:00:00+01:00", ObservationSequence: 3,
			},
			current: observationOrder{
				SourceUpdatedAt: "2026-07-12T00:00:00Z", ObservationSequence: 2,
			},
			want: 1,
		},
		{
			name: "missing source timestamps use sequence",
			incoming: observationOrder{
				ObservationSequence: 1,
			},
			current: observationOrder{
				ObservationSequence: 2,
			},
			want: -1,
		},
		{
			name: "equal malformed source timestamps use sequence",
			incoming: observationOrder{
				SourceUpdatedAt: "not-a-time", ObservationSequence: 3,
			},
			current: observationOrder{
				SourceUpdatedAt: "not-a-time", ObservationSequence: 2,
			},
			want: 1,
		},
		{
			name: "valid source timestamp outranks malformed source timestamp",
			incoming: observationOrder{
				SourceUpdatedAt: "2026-07-12T00:00:00Z", ObservationSequence: 1,
			},
			current: observationOrder{
				SourceUpdatedAt: "not-a-time", ObservationSequence: 2,
			},
			want: 1,
		},
		{
			name: "different malformed source timestamps are ambiguous",
			incoming: observationOrder{
				SourceUpdatedAt: "not-a-time-a", ObservationSequence: 2,
			},
			current: observationOrder{
				SourceUpdatedAt: "not-a-time-b", ObservationSequence: 1,
			},
			wantErr: "ambiguous malformed observation timestamps",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := compareObservationOrder(test.incoming, test.current)
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("error = %v, want %q", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("compare: %v", err)
			}
			if got != test.want {
				t.Fatalf("order = %d, want %d", got, test.want)
			}
		})
	}
}

func TestCompareRevisionObservationOrder(t *testing.T) {
	tests := []struct {
		name     string
		incoming observationOrder
		current  observationOrder
		want     int
		wantErr  string
	}{
		{
			name: "newer fetch wins after evidence clock decreases",
			incoming: observationOrder{
				SourceUpdatedAt: "2026-07-12T00:00:00Z", ObservationSequence: 2,
			},
			current: observationOrder{
				SourceUpdatedAt: "2026-07-12T00:00:01Z", ObservationSequence: 1,
			},
			want: 1,
		},
		{
			name: "older fetch loses despite newer evidence clock",
			incoming: observationOrder{
				SourceUpdatedAt: "2026-07-12T00:00:01Z", ObservationSequence: 1,
			},
			current: observationOrder{
				SourceUpdatedAt: "2026-07-12T00:00:00Z", ObservationSequence: 2,
			},
			want: -1,
		},
		{
			name: "legacy observations fall back to source clock",
			incoming: observationOrder{
				SourceUpdatedAt: "2026-07-12T00:00:01Z",
			},
			current: observationOrder{
				SourceUpdatedAt: "2026-07-12T00:00:00Z",
			},
			want: 1,
		},
		{
			name: "different malformed clocks remain ambiguous",
			incoming: observationOrder{
				SourceUpdatedAt: "not-a-time-a", ObservationSequence: 2,
			},
			current: observationOrder{
				SourceUpdatedAt: "not-a-time-b", ObservationSequence: 1,
			},
			wantErr: "ambiguous malformed observation timestamps",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := compareRevisionObservationOrder(test.incoming, test.current)
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("error = %v, want %q", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("compare: %v", err)
			}
			if got != test.want {
				t.Fatalf("order = %d, want %d", got, test.want)
			}
		})
	}
}

func TestThreadChildObservationReservationsAdvanceIndependently(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repoID, err := st.UpsertRepository(ctx, Repository{
		Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl",
		RawJSON: "{}", UpdatedAt: "2026-07-12T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("repository: %v", err)
	}
	threadID, err := st.UpsertThread(ctx, Thread{
		RepoID: repoID, GitHubID: "1", Number: 1, Kind: "pull_request", State: "open",
		Title: "family reservations", HTMLURL: "https://github.com/openclaw/gitcrawl/pull/1",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "thread",
		UpdatedAt: "2026-07-12T00:00:00Z",
	}, UpsertThreadOptions{IncompleteEvidence: true, ObservationSequence: 1})
	if err != nil {
		t.Fatalf("thread: %v", err)
	}

	for _, reservation := range []struct {
		family          ThreadChildObservationFamily
		sourceUpdatedAt string
		sequence        int64
		want            bool
	}{
		{
			family: ThreadChildComments, sourceUpdatedAt: "2026-07-12T00:00:00Z",
			sequence: 3, want: true,
		},
		{
			family: ThreadChildPullRequestDetails, sourceUpdatedAt: "2026-07-12T00:00:00Z",
			sequence: 2, want: true,
		},
		{
			family: ThreadChildComments, sourceUpdatedAt: "2026-07-12T00:00:00Z",
			sequence: 2, want: false,
		},
		{
			family: ThreadChildComments, sourceUpdatedAt: "2026-07-12T00:01:00Z",
			sequence: 1, want: true,
		},
		{
			family: ThreadChildComments, sourceUpdatedAt: "2026-07-12T00:00:00Z",
			sequence: 9, want: false,
		},
		{
			family: ThreadChildPullRequestDetails, sourceUpdatedAt: "2026-07-12T00:00:00Z",
			sequence: 4, want: true,
		},
	} {
		applied, err := st.ReserveThreadChildObservation(
			ctx,
			threadID,
			reservation.family,
			reservation.sourceUpdatedAt,
			reservation.sequence,
		)
		if err != nil || applied != reservation.want {
			t.Fatalf(
				"reserve %s at %d = %t, %v; want %t",
				reservation.family,
				reservation.sequence,
				applied,
				err,
				reservation.want,
			)
		}
	}
	var commentsSource string
	var commentsSequence, detailsSequence int64
	if err := st.DB().QueryRowContext(ctx, `
		select source_updated_at, observation_sequence
		from thread_child_observation_reservations
		where thread_id = ? and family = 'comments'
	`, threadID).Scan(&commentsSource, &commentsSequence); err != nil {
		t.Fatalf("comments reservation: %v", err)
	}
	if err := st.DB().QueryRowContext(ctx, `
		select observation_sequence
		from thread_child_observation_reservations
		where thread_id = ? and family = 'pull_request_details'
	`, threadID).Scan(&detailsSequence); err != nil {
		t.Fatalf("details reservation: %v", err)
	}
	if commentsSource != "2026-07-12T00:01:00Z" || commentsSequence != 1 ||
		detailsSequence != 4 {
		t.Fatalf(
			"reservations = comments %s/%d, details %d",
			commentsSource,
			commentsSequence,
			detailsSequence,
		)
	}
	if _, err := st.ReserveThreadChildObservation(
		ctx,
		threadID,
		ThreadChildObservationFamily("unbounded"),
		"2026-07-12T00:00:00Z",
		5,
	); err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("invalid family error = %v", err)
	}
}

func TestWorkflowRunObservationReservationsAreRepositoryHeadScoped(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repoID, err := st.UpsertRepository(ctx, Repository{
		Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl",
		RawJSON: "{}", UpdatedAt: "2026-07-12T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("repository: %v", err)
	}
	for _, reservation := range []struct {
		headSHA         string
		sourceUpdatedAt string
		sequence        int64
		want            bool
	}{
		{
			headSHA: "shared-head", sourceUpdatedAt: "2026-07-12T00:00:00Z",
			sequence: 3, want: true,
		},
		{
			headSHA: "shared-head", sourceUpdatedAt: "2026-07-12T00:00:00Z",
			sequence: 2, want: false,
		},
		{
			headSHA: "shared-head", sourceUpdatedAt: "2026-07-12T00:00:00Z",
			sequence: 3, want: true,
		},
		{
			headSHA: "shared-head", sourceUpdatedAt: "2026-07-12T00:01:00Z",
			sequence: 1, want: true,
		},
		{
			headSHA: "shared-head", sourceUpdatedAt: "2026-07-12T00:00:00Z",
			sequence: 9, want: false,
		},
		{
			headSHA: "other-head", sourceUpdatedAt: "2026-07-12T00:00:00Z",
			sequence: 1, want: true,
		},
	} {
		applied, err := st.ReserveWorkflowRunObservation(
			ctx,
			repoID,
			reservation.headSHA,
			reservation.sourceUpdatedAt,
			reservation.sequence,
		)
		if err != nil || applied != reservation.want {
			t.Fatalf(
				"reserve %s at %d = %t, %v; want %t",
				reservation.headSHA,
				reservation.sequence,
				applied,
				err,
				reservation.want,
			)
		}
	}
	var sharedSource string
	var sharedSequence, otherSequence int64
	if err := st.DB().QueryRowContext(ctx, `
		select source_updated_at, observation_sequence
		from workflow_run_observation_reservations
		where repo_id = ? and head_sha = 'shared-head'
	`, repoID).Scan(&sharedSource, &sharedSequence); err != nil {
		t.Fatalf("shared reservation: %v", err)
	}
	if err := st.DB().QueryRowContext(ctx, `
		select observation_sequence
		from workflow_run_observation_reservations
		where repo_id = ? and head_sha = 'other-head'
	`, repoID).Scan(&otherSequence); err != nil {
		t.Fatalf("other reservation: %v", err)
	}
	if sharedSource != "2026-07-12T00:01:00Z" || sharedSequence != 1 ||
		otherSequence != 1 {
		t.Fatalf(
			"reservations = shared %s/%d, other %d",
			sharedSource,
			sharedSequence,
			otherSequence,
		)
	}
	for _, invalid := range []struct {
		repoID   int64
		headSHA  string
		sequence int64
	}{
		{repoID: 0, headSHA: "head", sequence: 1},
		{repoID: repoID, headSHA: " ", sequence: 1},
		{repoID: repoID, headSHA: "head", sequence: 0},
	} {
		if _, err := st.ReserveWorkflowRunObservation(
			ctx,
			invalid.repoID,
			invalid.headSHA,
			"2026-07-12T00:00:00Z",
			invalid.sequence,
		); err == nil {
			t.Fatalf("invalid reservation %+v succeeded", invalid)
		}
	}
}

func TestUpsertThreadObservationRejectsDelayedCanonicalOverwrite(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repoID, err := st.UpsertRepository(ctx, Repository{
		Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl",
		RawJSON: "{}", UpdatedAt: "2026-07-12T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("repository: %v", err)
	}
	thread := Thread{
		RepoID: repoID, GitHubID: "1", Number: 1, Kind: "issue", State: "open",
		Title: "state a", HTMLURL: "https://github.com/openclaw/gitcrawl/issues/1",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: `{"state":"a"}`,
		ContentHash: "state-a", UpdatedAtGitHub: "2026-07-12T00:00:00Z",
		UpdatedAt: "2026-07-12T00:01:00Z",
	}
	first, err := st.UpsertThreadObservation(ctx, thread, UpsertThreadOptions{ObservationSequence: 1})
	if err != nil || !first.Applied {
		t.Fatalf("first observation = %+v, %v", first, err)
	}

	thread.Title = "state b"
	thread.RawJSON = `{"state":"b"}`
	thread.ContentHash = "state-b"
	current, err := st.UpsertThreadObservation(ctx, thread, UpsertThreadOptions{ObservationSequence: 2})
	if err != nil || !current.Applied {
		t.Fatalf("current observation = %+v, %v", current, err)
	}

	thread.Title = "state a"
	thread.RawJSON = `{"state":"a"}`
	thread.ContentHash = "state-a"
	stale, err := st.UpsertThreadObservation(ctx, thread, UpsertThreadOptions{ObservationSequence: 1})
	if err != nil {
		t.Fatalf("stale observation: %v", err)
	}
	if stale.Applied || stale.ID != current.ID {
		t.Fatalf("stale observation = %+v, want skipped id %d", stale, current.ID)
	}

	threads, err := st.ListThreads(ctx, repoID, true)
	if err != nil {
		t.Fatalf("list threads: %v", err)
	}
	if len(threads) != 1 || threads[0].Title != "state b" {
		t.Fatalf("canonical threads = %+v", threads)
	}
}

func TestUpsertThreadObservationIsIdempotentButRejectsTiedConflicts(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repoID, err := st.UpsertRepository(ctx, Repository{
		Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl",
		RawJSON: "{}", UpdatedAt: "2026-07-12T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("repository: %v", err)
	}
	thread := Thread{
		RepoID: repoID, GitHubID: "1", Number: 1, Kind: "issue", State: "open",
		Title: "same", HTMLURL: "https://github.com/openclaw/gitcrawl/issues/1",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: `{"state":"same"}`,
		ContentHash: "same", UpdatedAtGitHub: "2026-07-12T00:00:00Z",
		UpdatedAt: "2026-07-12T00:01:00Z",
	}
	if result, err := st.UpsertThreadObservation(ctx, thread, UpsertThreadOptions{ObservationSequence: 1}); err != nil || !result.Applied {
		t.Fatalf("first observation = %+v, %v", result, err)
	}
	repeated, err := st.UpsertThreadObservation(ctx, thread, UpsertThreadOptions{ObservationSequence: 1})
	if err != nil {
		t.Fatalf("repeat observation: %v", err)
	}
	if repeated.Applied {
		t.Fatalf("repeat observation applied: %+v", repeated)
	}

	thread.Title = "conflict"
	thread.RawJSON = `{"state":"conflict"}`
	thread.ContentHash = "conflict"
	if _, err := st.UpsertThreadObservation(ctx, thread, UpsertThreadOptions{ObservationSequence: 1}); err == nil ||
		!strings.Contains(err.Error(), "conflicting thread observations share sequence 1") {
		t.Fatalf("tied conflict error = %v", err)
	}
}

func TestUpsertThreadObservationTracksIncompleteEvidenceGeneration(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repoID, err := st.UpsertRepository(ctx, Repository{
		Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl",
		RawJSON: "{}", UpdatedAt: "2026-07-12T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("repository: %v", err)
	}
	thread := Thread{
		RepoID: repoID, GitHubID: "1", Number: 1, Kind: "issue", State: "open",
		Title: "hydrated", HTMLURL: "https://github.com/openclaw/gitcrawl/issues/1",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: `{"state":"hydrated"}`,
		ContentHash: "hydrated", UpdatedAtGitHub: "2026-07-12T00:00:00Z",
		UpdatedAt: "2026-07-12T00:01:00Z",
	}
	initial, err := st.UpsertThreadObservation(ctx, thread, UpsertThreadOptions{ObservationSequence: 1})
	if err != nil || !initial.Applied {
		t.Fatalf("initial observation = %+v, %v", initial, err)
	}
	readSequence := func() int64 {
		t.Helper()
		var sequence int64
		if err := st.DB().QueryRowContext(ctx, `
			select observation_sequence
			from threads
			where id = ?
		`, initial.ID).Scan(&sequence); err != nil {
			t.Fatalf("read observation sequence: %v", err)
		}
		return sequence
	}

	repeated, err := st.UpsertThreadObservation(ctx, thread, UpsertThreadOptions{
		IncompleteEvidence:  true,
		ObservationSequence: 3,
	})
	if err != nil || !repeated.Applied {
		t.Fatalf("repeated incomplete observation = %+v, %v", repeated, err)
	}
	if sequence := readSequence(); sequence != 3 {
		t.Fatalf("repeated incomplete sequence = %d, want 3", sequence)
	}

	thread.Title = "metadata update"
	thread.RawJSON = `{"state":"metadata-update"}`
	thread.ContentHash = "metadata-update"
	thread.UpdatedAtGitHub = "2026-07-12T00:00:01Z"
	updated, err := st.UpsertThreadObservation(ctx, thread, UpsertThreadOptions{
		IncompleteEvidence:  true,
		ObservationSequence: 4,
	})
	if err != nil || !updated.Applied {
		t.Fatalf("newer incomplete observation = %+v, %v", updated, err)
	}
	if sequence := readSequence(); sequence != -4 {
		t.Fatalf("newer incomplete sequence = %d, want -4", sequence)
	}

	hydrated, err := st.UpsertThreadObservation(ctx, thread, UpsertThreadOptions{
		ObservationSequence: 2,
	})
	if err != nil || !hydrated.Applied {
		t.Fatalf("same-source hydration = %+v, %v", hydrated, err)
	}
	if !hydrated.EvidenceApplied || hydrated.ObservationSequence != 4 ||
		hydrated.EvidenceObservationSequence != 2 {
		t.Fatalf("same-source hydration result = %+v, want parent 4 and evidence 2", hydrated)
	}
	if sequence := readSequence(); sequence != 4 {
		t.Fatalf("same-source hydration sequence = %d, want 4", sequence)
	}

	thread.Title = "same-clock conflict"
	thread.RawJSON = `{"state":"same-clock-conflict"}`
	thread.ContentHash = "same-clock-conflict"
	conflict, err := st.UpsertThreadObservation(ctx, thread, UpsertThreadOptions{
		IncompleteEvidence:  true,
		ObservationSequence: 5,
	})
	if err != nil || !conflict.Applied {
		t.Fatalf("same-clock incomplete conflict = %+v, %v", conflict, err)
	}
	if sequence := readSequence(); sequence != -5 {
		t.Fatalf("same-clock conflict sequence = %d, want -5", sequence)
	}

	delayed := thread
	delayed.Title = "metadata update"
	delayed.RawJSON = `{"state":"metadata-update"}`
	delayed.ContentHash = "metadata-update"
	staleHydration, err := st.UpsertThreadObservation(ctx, delayed, UpsertThreadOptions{
		ObservationSequence: 3,
	})
	if err != nil {
		t.Fatalf("delayed conflicting hydration: %v", err)
	}
	if staleHydration.Applied {
		t.Fatalf("delayed conflicting hydration applied: %+v", staleHydration)
	}
	if sequence := readSequence(); sequence != -5 {
		t.Fatalf("sequence after delayed hydration = %d, want -5", sequence)
	}
}

func TestUpsertThreadObservationRejectsDelayedIntermediateParentAfterIncompleteReplay(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repoID, err := st.UpsertRepository(ctx, Repository{
		Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl",
		RawJSON: "{}", UpdatedAt: "2026-07-12T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("repository: %v", err)
	}
	thread := Thread{
		RepoID: repoID, GitHubID: "1", Number: 1, Kind: "issue", State: "open",
		Title: "canonical", Body: "canonical body",
		HTMLURL:         "https://github.com/openclaw/gitcrawl/issues/1",
		LabelsJSON:      "[]",
		AssigneesJSON:   "[]",
		RawJSON:         `{"state":"canonical"}`,
		ContentHash:     "canonical",
		UpdatedAtGitHub: "2026-07-12T00:00:00Z",
		UpdatedAt:       "2026-07-12T00:01:00Z",
	}
	initial, err := st.UpsertThreadObservation(ctx, thread, UpsertThreadOptions{
		ObservationSequence: 1,
	})
	if err != nil || !initial.Applied || !initial.EvidenceApplied {
		t.Fatalf("initial complete observation = %+v, %v", initial, err)
	}
	replayed, err := st.UpsertThreadObservation(ctx, thread, UpsertThreadOptions{
		IncompleteEvidence:  true,
		ObservationSequence: 3,
	})
	if err != nil || !replayed.Applied || replayed.ObservationSequence != 3 ||
		replayed.EvidenceApplied {
		t.Fatalf("same-payload incomplete replay = %+v, %v", replayed, err)
	}

	delayed := thread
	delayed.Title = "delayed conflict"
	delayed.Body = "must not replace canonical"
	delayed.RawJSON = `{"state":"delayed-conflict"}`
	delayed.ContentHash = "delayed-conflict"
	conflict, err := st.UpsertThreadObservation(ctx, delayed, UpsertThreadOptions{
		IncompleteEvidence:  true,
		ObservationSequence: 2,
	})
	if err != nil {
		t.Fatalf("delayed intermediate conflict: %v", err)
	}
	if conflict.Applied || conflict.EvidenceApplied || conflict.ObservationSequence != 3 {
		t.Fatalf("delayed intermediate conflict = %+v, want rejected below parent high-water 3", conflict)
	}

	var title string
	var parentSequence, evidenceSequence int64
	if err := st.DB().QueryRowContext(ctx, `
		select title, observation_sequence, evidence_observation_sequence
		from threads
		where id = ?
	`, initial.ID).Scan(&title, &parentSequence, &evidenceSequence); err != nil {
		t.Fatalf("read canonical parent: %v", err)
	}
	if title != "canonical" || parentSequence != 3 || evidenceSequence != 1 {
		t.Fatalf(
			"canonical parent = title %q, parent %d, evidence %d; want canonical/3/1",
			title,
			parentSequence,
			evidenceSequence,
		)
	}
}

func TestUpsertThreadObservationCompletionPreservesGenerationHighWaterMark(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repoID, err := st.UpsertRepository(ctx, Repository{
		Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl",
		RawJSON: "{}", UpdatedAt: "2026-07-12T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("repository: %v", err)
	}
	thread := Thread{
		RepoID: repoID, GitHubID: "1", Number: 1, Kind: "issue", State: "open",
		Title: "complete payload", Body: "complete body",
		HTMLURL:    "https://github.com/openclaw/gitcrawl/issues/1",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: `{"state":"complete"}`,
		ContentHash: "complete", UpdatedAtGitHub: "2026-07-12T00:00:00Z",
		UpdatedAt: "2026-07-12T00:01:00Z",
	}
	incomplete, err := st.UpsertThreadObservation(ctx, thread, UpsertThreadOptions{
		IncompleteEvidence:  true,
		ObservationSequence: 4,
	})
	if err != nil || !incomplete.Applied || incomplete.ObservationSequence != 4 {
		t.Fatalf("incomplete observation = %+v, %v", incomplete, err)
	}
	readEvidenceSequence := func() int64 {
		t.Helper()
		var sequence int64
		if err := st.DB().QueryRowContext(ctx, `
			select evidence_observation_sequence
			from threads
			where id = ?
		`, incomplete.ID).Scan(&sequence); err != nil {
			t.Fatalf("read evidence observation sequence: %v", err)
		}
		return sequence
	}
	if sequence := readEvidenceSequence(); sequence != 0 {
		t.Fatalf("incomplete evidence sequence = %d, want 0", sequence)
	}

	completed, err := st.UpsertThreadObservation(ctx, thread, UpsertThreadOptions{
		ObservationSequence: 2,
	})
	if err != nil || !completed.Applied || !completed.EvidenceApplied ||
		completed.ObservationSequence != 4 || completed.EvidenceObservationSequence != 2 {
		t.Fatalf("completed observation = %+v, %v", completed, err)
	}
	if sequence := readEvidenceSequence(); sequence != 2 {
		t.Fatalf("completed evidence sequence = %d, want 2", sequence)
	}

	newerEvidence, err := st.UpsertThreadObservation(ctx, thread, UpsertThreadOptions{
		ObservationSequence: 3,
	})
	if err != nil || newerEvidence.Applied || !newerEvidence.EvidenceApplied ||
		newerEvidence.ObservationSequence != 4 || newerEvidence.EvidenceObservationSequence != 3 {
		t.Fatalf("newer complete evidence = %+v, %v", newerEvidence, err)
	}
	if sequence := readEvidenceSequence(); sequence != 3 {
		t.Fatalf("newer evidence sequence = %d, want 3", sequence)
	}
	replayedEvidence, err := st.UpsertThreadObservation(ctx, thread, UpsertThreadOptions{
		ObservationSequence: 2,
	})
	if err != nil || replayedEvidence.Applied || replayedEvidence.EvidenceApplied ||
		replayedEvidence.ObservationSequence != 4 {
		t.Fatalf("replayed evidence = %+v, %v", replayedEvidence, err)
	}
	thread.ID = completed.ID
	comment := Comment{
		ThreadID: thread.ID, GitHubID: "comment-1", CommentType: "issue_comment",
		Body: "sequence three child", RawJSON: `{"body":"sequence three child"}`,
		CreatedAtGitHub: "2026-07-12T00:00:00Z",
		UpdatedAtGitHub: "2026-07-12T00:00:01Z",
	}
	if _, err := st.UpsertComment(ctx, comment); err != nil {
		t.Fatalf("newer comment: %v", err)
	}
	enrichment, err := st.UpsertThreadRevisionAndFingerprint(ctx, ThreadEvidence{
		Thread:              thread,
		ObservationSequence: newerEvidence.EvidenceObservationSequence,
		Comments:            []Comment{comment},
	}, "2026-07-12T00:03:00Z")
	if err != nil {
		t.Fatalf("newer enrichment: %v", err)
	}
	if !enrichment.RevisionCreated || !enrichment.FingerprintUpserted {
		t.Fatalf("newer enrichment = %+v", enrichment)
	}

	delayed := thread
	delayed.Title = "delayed conflict"
	delayed.Body = "delayed body"
	delayed.RawJSON = `{"state":"delayed"}`
	delayed.ContentHash = "delayed"
	conflict, err := st.UpsertThreadObservation(ctx, delayed, UpsertThreadOptions{
		ObservationSequence: 3,
	})
	if err != nil {
		t.Fatalf("delayed conflict: %v", err)
	}
	if conflict.Applied || conflict.EvidenceApplied || conflict.ObservationSequence != 4 {
		t.Fatalf("delayed conflict = %+v, want skipped at sequence 4", conflict)
	}

	var canonicalTitle string
	var canonicalSequence, evidenceSequence, revisionSequence, revisions, fingerprints int64
	if err := st.DB().QueryRowContext(ctx, `
		select title, observation_sequence, evidence_observation_sequence
		from threads
		where id = ?
	`, thread.ID).Scan(&canonicalTitle, &canonicalSequence, &evidenceSequence); err != nil {
		t.Fatalf("canonical observation: %v", err)
	}
	if err := st.DB().QueryRowContext(ctx, `
		select observation_sequence
		from thread_revisions
		where thread_id = ?
		order by observation_sequence desc
		limit 1
	`, thread.ID).Scan(&revisionSequence); err != nil {
		t.Fatalf("revision observation: %v", err)
	}
	if err := st.DB().QueryRowContext(ctx, `
		select count(*), count(tf.id)
		from thread_revisions tr
		left join thread_fingerprints tf on tf.thread_revision_id = tr.id
		where tr.thread_id = ?
	`, thread.ID).Scan(&revisions, &fingerprints); err != nil {
		t.Fatalf("fingerprint child: %v", err)
	}
	comments, err := st.ListComments(ctx, thread.ID)
	if err != nil {
		t.Fatalf("comments: %v", err)
	}
	if canonicalTitle != "complete payload" || canonicalSequence != 4 ||
		evidenceSequence != 3 || revisionSequence != 3 || revisions != 1 || fingerprints != 1 ||
		len(comments) != 1 || comments[0].Body != "sequence three child" {
		t.Fatalf(
			"persisted state = title %q, canonical %d, evidence %d, revision %d, revisions %d, fingerprints %d, comments %+v",
			canonicalTitle,
			canonicalSequence,
			evidenceSequence,
			revisionSequence,
			revisions,
			fingerprints,
			comments,
		)
	}
}

func TestUpsertThreadObservationCompletesNewerSourceBelowEvidenceSequence(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repoID, err := st.UpsertRepository(ctx, Repository{
		Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl",
		RawJSON: "{}", UpdatedAt: "2026-07-12T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("repository: %v", err)
	}
	thread := Thread{
		RepoID: repoID, GitHubID: "1", Number: 1, Kind: "issue", State: "open",
		Title: "old source", Body: "old body",
		HTMLURL:    "https://github.com/openclaw/gitcrawl/issues/1",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: `{"version":1}`,
		ContentHash: "old", UpdatedAtGitHub: "2026-07-12T00:00:00Z",
		UpdatedAt: "2026-07-12T00:01:00Z",
	}
	old, err := st.UpsertThreadObservation(ctx, thread, UpsertThreadOptions{
		ObservationSequence: 5,
	})
	if err != nil || !old.Applied || !old.EvidenceApplied {
		t.Fatalf("old complete observation = %+v, %v", old, err)
	}

	thread.Title = "new source"
	thread.Body = "new body"
	thread.RawJSON = `{"version":2}`
	thread.ContentHash = "new"
	thread.UpdatedAtGitHub = "2026-07-12T00:02:00Z"
	metadata, err := st.UpsertThreadObservation(ctx, thread, UpsertThreadOptions{
		IncompleteEvidence:  true,
		ObservationSequence: 8,
	})
	if err != nil || !metadata.Applied || metadata.EvidenceApplied {
		t.Fatalf("newer metadata observation = %+v, %v", metadata, err)
	}
	complete, err := st.UpsertThreadObservation(ctx, thread, UpsertThreadOptions{
		ObservationSequence: 2,
	})
	if err != nil || !complete.Applied || !complete.EvidenceApplied ||
		complete.ObservationSequence != 8 ||
		complete.EvidenceObservationSequence != 2 {
		t.Fatalf("newer complete observation = %+v, %v", complete, err)
	}

	var title, evidenceSource string
	var parentSequence, evidenceSequence int64
	if err := st.DB().QueryRowContext(ctx, `
		select title, observation_sequence, evidence_source_updated_at,
			evidence_observation_sequence
		from threads
		where id = ?
	`, complete.ID).Scan(
		&title,
		&parentSequence,
		&evidenceSource,
		&evidenceSequence,
	); err != nil {
		t.Fatalf("read completed observation: %v", err)
	}
	if title != "new source" || parentSequence != 8 ||
		evidenceSource != "2026-07-12T00:02:00Z" || evidenceSequence != 2 {
		t.Fatalf(
			"completed observation = title %q, parent %d, evidence %s/%d",
			title,
			parentSequence,
			evidenceSource,
			evidenceSequence,
		)
	}
}

func TestUpsertThreadObservationStartsIncompletePayloadWithoutEvidenceGeneration(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "gitcrawl.db")
	st, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	repoID, err := st.UpsertRepository(ctx, Repository{
		Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl",
		RawJSON: "{}", UpdatedAt: "2026-07-12T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("repository: %v", err)
	}
	result, err := st.UpsertThreadObservation(ctx, Thread{
		RepoID: repoID, GitHubID: "1", Number: 1, Kind: "issue", State: "open",
		Title: "metadata", HTMLURL: "https://github.com/openclaw/gitcrawl/issues/1",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: `{}`,
		ContentHash: "metadata", UpdatedAtGitHub: "2026-07-12T00:00:00Z",
		UpdatedAt: "2026-07-12T00:01:00Z",
	}, UpsertThreadOptions{
		IncompleteEvidence:  true,
		ObservationSequence: 7,
	})
	if err != nil || !result.Applied {
		t.Fatalf("incomplete observation = %+v, %v", result, err)
	}
	var sequence int64
	if err := st.DB().QueryRowContext(ctx, `
		select observation_sequence
		from threads
		where id = ?
	`, result.ID).Scan(&sequence); err != nil {
		t.Fatalf("read observation sequence: %v", err)
	}
	if sequence != -7 {
		t.Fatalf("new incomplete sequence = %d, want -7", sequence)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	st, err = Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer st.Close()
	if err := st.DB().QueryRowContext(ctx, `
		select observation_sequence
		from threads
		where id = ?
	`, result.ID).Scan(&sequence); err != nil {
		t.Fatalf("read reopened observation sequence: %v", err)
	}
	if sequence != -7 {
		t.Fatalf("reopened incomplete sequence = %d, want -7", sequence)
	}
}

func TestPortableReopenPreservesIncompleteObservationGeneration(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "gitcrawl.db")
	st, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	repoID, err := st.UpsertRepository(ctx, Repository{
		Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl",
		RawJSON: "{}", UpdatedAt: "2026-07-12T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("repository: %v", err)
	}
	result, err := st.UpsertThreadObservation(ctx, Thread{
		RepoID: repoID, GitHubID: "1", Number: 1, Kind: "issue", State: "open",
		Title: "metadata", HTMLURL: "https://github.com/openclaw/gitcrawl/issues/1",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: `{}`,
		ContentHash: "metadata", UpdatedAtGitHub: "2026-07-12T00:00:00Z",
		UpdatedAt: "2026-07-12T00:01:00Z",
	}, UpsertThreadOptions{
		IncompleteEvidence:  true,
		ObservationSequence: 7,
	})
	if err != nil || !result.Applied {
		t.Fatalf("incomplete observation = %+v, %v", result, err)
	}
	if _, err := st.PrunePortablePayloads(ctx, PortablePruneOptions{BodyChars: 64}); err != nil {
		t.Fatalf("portable prune: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close portable store: %v", err)
	}

	st, err = Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("reopen portable store: %v", err)
	}
	defer st.Close()
	var sequence int64
	if err := st.DB().QueryRowContext(ctx, `
		select observation_sequence
		from threads
		where id = ?
	`, result.ID).Scan(&sequence); err != nil {
		t.Fatalf("read portable observation sequence: %v", err)
	}
	if sequence != -7 {
		t.Fatalf("portable observation sequence = %d, want -7", sequence)
	}
}

func TestUpsertThreadObservationUsesSequenceWithoutSourceTimestamp(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repoID, err := st.UpsertRepository(ctx, Repository{
		Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl",
		RawJSON: "{}", UpdatedAt: "2026-07-12T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("repository: %v", err)
	}
	thread := Thread{
		RepoID: repoID, GitHubID: "1", Number: 1, Kind: "issue", State: "open",
		Title: "newer", HTMLURL: "https://github.com/openclaw/gitcrawl/issues/1",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: `{"state":"newer"}`,
		ContentHash: "newer", UpdatedAt: "2026-07-12T00:01:00Z",
	}
	if result, err := st.UpsertThreadObservation(ctx, thread, UpsertThreadOptions{ObservationSequence: 2}); err != nil || !result.Applied {
		t.Fatalf("newer observation = %+v, %v", result, err)
	}
	thread.Title = "older"
	thread.RawJSON = `{"state":"older"}`
	thread.ContentHash = "older"
	result, err := st.UpsertThreadObservation(ctx, thread, UpsertThreadOptions{ObservationSequence: 1})
	if err != nil {
		t.Fatalf("older observation: %v", err)
	}
	if result.Applied {
		t.Fatalf("older observation applied: %+v", result)
	}
}

func TestUpsertThreadObservationRejectsAmbiguousMalformedClocks(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repoID, err := st.UpsertRepository(ctx, Repository{
		Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl",
		RawJSON: "{}", UpdatedAt: "2026-07-12T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("repository: %v", err)
	}
	thread := Thread{
		RepoID: repoID, GitHubID: "1", Number: 1, Kind: "issue", State: "open",
		Title: "first", HTMLURL: "https://github.com/openclaw/gitcrawl/issues/1",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: `{"state":"first"}`,
		ContentHash: "first", UpdatedAtGitHub: "malformed-a",
		UpdatedAt: "2026-07-12T00:01:00Z",
	}
	if _, err := st.UpsertThreadObservation(ctx, thread, UpsertThreadOptions{ObservationSequence: 1}); err != nil {
		t.Fatalf("first observation: %v", err)
	}
	thread.Title = "second"
	thread.RawJSON = `{"state":"second"}`
	thread.ContentHash = "second"
	thread.UpdatedAtGitHub = "malformed-b"
	if _, err := st.UpsertThreadObservation(ctx, thread, UpsertThreadOptions{ObservationSequence: 2}); err == nil ||
		!strings.Contains(err.Error(), "ambiguous malformed observation timestamps") {
		t.Fatalf("ambiguous observation error = %v", err)
	}
}
