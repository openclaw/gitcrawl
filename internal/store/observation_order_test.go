package store

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

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
	if sequence := readSequence(); sequence != 1 {
		t.Fatalf("repeated incomplete sequence = %d, want 1", sequence)
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
	if hydrated.ObservationSequence != 4 {
		t.Fatalf("same-source hydration result = %+v, want sequence 4", hydrated)
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

	completed, err := st.UpsertThreadObservation(ctx, thread, UpsertThreadOptions{
		ObservationSequence: 2,
	})
	if err != nil || !completed.Applied || completed.ObservationSequence != 4 {
		t.Fatalf("completed observation = %+v, %v", completed, err)
	}
	thread.ID = completed.ID
	comment := Comment{
		ThreadID: thread.ID, GitHubID: "comment-1", CommentType: "issue_comment",
		Body: "complete child", RawJSON: `{"body":"complete child"}`,
		CreatedAtGitHub: "2026-07-12T00:00:00Z",
		UpdatedAtGitHub: "2026-07-12T00:00:00Z",
	}
	if _, err := st.UpsertComment(ctx, comment); err != nil {
		t.Fatalf("comment: %v", err)
	}
	enrichment, err := st.UpsertThreadRevisionAndFingerprint(ctx, ThreadEvidence{
		Thread:              thread,
		ObservationSequence: completed.ObservationSequence,
		Comments:            []Comment{comment},
	}, "2026-07-12T00:02:00Z")
	if err != nil {
		t.Fatalf("enrichment: %v", err)
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
	if conflict.Applied || conflict.ObservationSequence != 4 {
		t.Fatalf("delayed conflict = %+v, want skipped at sequence 4", conflict)
	}

	var canonicalTitle string
	var canonicalSequence, revisionSequence, fingerprints int64
	if err := st.DB().QueryRowContext(ctx, `
		select title, observation_sequence
		from threads
		where id = ?
	`, thread.ID).Scan(&canonicalTitle, &canonicalSequence); err != nil {
		t.Fatalf("canonical observation: %v", err)
	}
	if err := st.DB().QueryRowContext(ctx, `
		select observation_sequence
		from thread_revisions
		where id = ?
	`, enrichment.RevisionID).Scan(&revisionSequence); err != nil {
		t.Fatalf("revision observation: %v", err)
	}
	if err := st.DB().QueryRowContext(ctx, `
		select count(*)
		from thread_fingerprints
		where thread_revision_id = ?
	`, enrichment.RevisionID).Scan(&fingerprints); err != nil {
		t.Fatalf("fingerprint child: %v", err)
	}
	comments, err := st.ListComments(ctx, thread.ID)
	if err != nil {
		t.Fatalf("comments: %v", err)
	}
	if canonicalTitle != "complete payload" || canonicalSequence != 4 ||
		revisionSequence != 4 || fingerprints != 1 ||
		len(comments) != 1 || comments[0].Body != "complete child" {
		t.Fatalf(
			"persisted state = title %q, canonical %d, revision %d, fingerprints %d, comments %+v",
			canonicalTitle,
			canonicalSequence,
			revisionSequence,
			fingerprints,
			comments,
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
