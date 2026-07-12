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
