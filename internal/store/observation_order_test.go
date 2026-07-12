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

func TestUpsertThreadObservationPreservesEvidenceGenerationForIncompletePayloads(t *testing.T) {
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
		PreserveObservationSequence: true,
		ObservationSequence:         3,
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
		PreserveObservationSequence: true,
		ObservationSequence:         4,
	})
	if err != nil || !updated.Applied {
		t.Fatalf("newer incomplete observation = %+v, %v", updated, err)
	}
	if sequence := readSequence(); sequence != 1 {
		t.Fatalf("newer incomplete sequence = %d, want 1", sequence)
	}

	hydrated, err := st.UpsertThreadObservation(ctx, thread, UpsertThreadOptions{
		ObservationSequence: 2,
	})
	if err != nil || !hydrated.Applied {
		t.Fatalf("same-source hydration = %+v, %v", hydrated, err)
	}
	if sequence := readSequence(); sequence != 2 {
		t.Fatalf("same-source hydration sequence = %d, want 2", sequence)
	}

	thread.Title = "same-clock conflict"
	thread.RawJSON = `{"state":"same-clock-conflict"}`
	thread.ContentHash = "same-clock-conflict"
	conflict, err := st.UpsertThreadObservation(ctx, thread, UpsertThreadOptions{
		PreserveObservationSequence: true,
		ObservationSequence:         5,
	})
	if err != nil || !conflict.Applied {
		t.Fatalf("same-clock incomplete conflict = %+v, %v", conflict, err)
	}
	if sequence := readSequence(); sequence != 5 {
		t.Fatalf("same-clock conflict sequence = %d, want 5", sequence)
	}
}

func TestUpsertThreadObservationStartsIncompletePayloadWithoutEvidenceGeneration(t *testing.T) {
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
	result, err := st.UpsertThreadObservation(ctx, Thread{
		RepoID: repoID, GitHubID: "1", Number: 1, Kind: "issue", State: "open",
		Title: "metadata", HTMLURL: "https://github.com/openclaw/gitcrawl/issues/1",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: `{}`,
		ContentHash: "metadata", UpdatedAtGitHub: "2026-07-12T00:00:00Z",
		UpdatedAt: "2026-07-12T00:01:00Z",
	}, UpsertThreadOptions{
		PreserveObservationSequence: true,
		ObservationSequence:         7,
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
	if sequence != 0 {
		t.Fatalf("new incomplete sequence = %d, want 0", sequence)
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
