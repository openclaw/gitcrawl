package store

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
)

func TestArchiveInventoryObservation(t *testing.T) {
	ctx := context.Background()
	start := "2026-09-01T00:00:00Z"
	finish := "2026-09-01T00:01:00Z"
	for _, test := range []struct {
		name, scope, status, stats string
		drop, noRun                bool
		want                       ArchiveObservationState
	}{
		{name: "unsupported", drop: true, want: ArchiveObservationUnsupported},
		{name: "missing", noRun: true, want: ArchiveObservationMissing},
		{name: "unknown sanitized stats", scope: "all", status: "success", want: ArchiveObservationUnknown},
		{name: "unknown malformed stats", scope: "all", status: "success", stats: "{", want: ArchiveObservationUnknown},
		{name: "partial state", scope: "open", status: "success", stats: `"threads_synced":1`, want: ArchiveObservationPartial},
		{name: "partial bounded", scope: "all", status: "success", stats: `"threads_synced":1,"limit":1`, want: ArchiveObservationPartial},
		{name: "partial since", scope: "all", status: "success", stats: `"threads_synced":1,"requested_since":"2026-08-01T00:00:00Z"`, want: ArchiveObservationPartial},
		{name: "partial targeted", scope: "all", status: "success", stats: `"threads_synced":1,"numbers":[1]`, want: ArchiveObservationPartial},
		{name: "failed", scope: "all", status: "failed", want: ArchiveObservationPartial},
		{name: "complete", scope: "all", status: "success", stats: `"threads_synced":1`, want: ArchiveObservationComplete},
		{name: "stale skipped is not empty", scope: "all", status: "success", stats: `"threads_synced":0,"threads_skipped_stale":1`, want: ArchiveObservationComplete},
		{name: "empty", scope: "all", status: "success", stats: `"threads_synced":0`, want: ArchiveObservationEmpty},
	} {
		t.Run(test.name, func(t *testing.T) {
			st, err := Open(ctx, filepath.Join(t.TempDir(), "source.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			repoID, err := st.UpsertRepository(ctx, Repository{Owner: "example", Name: "repo", FullName: "example/repo", UpdatedAt: start})
			if err != nil {
				t.Fatal(err)
			}
			if test.drop {
				if _, err := st.DB().ExecContext(ctx, `drop table sync_runs`); err != nil {
					t.Fatal(err)
				}
			} else if !test.noRun {
				stats := test.stats
				if stats != "" && stats != "{" {
					stats = `{"started_at":"` + start + `","finished_at":"` + finish + `","metadata_only":false,` + stats + `}`
				}
				if _, err := st.RecordRun(ctx, RunRecord{RepoID: repoID, Kind: "sync", Scope: test.scope,
					Status: test.status, StartedAt: start, FinishedAt: finish, StatsJSON: stats}); err != nil {
					t.Fatal(err)
				}
			}
			got, err := st.archiveInventoryObservation(ctx, repoID)
			if err != nil {
				t.Fatal(err)
			}
			if got.State != test.want {
				t.Fatalf("state = %q, want %q (%+v)", got.State, test.want, got)
			}
		})
	}
}

func TestArchiveChildObservations(t *testing.T) {
	ctx := context.Background()
	for _, test := range []struct {
		name                       string
		sequence, observedSequence int64
		source, observedSource     string
		empty, missing, drop       bool
		want                       ArchiveObservationState
	}{
		{name: "unsupported", drop: true, want: ArchiveObservationUnsupported},
		{name: "empty", empty: true, want: ArchiveObservationEmpty},
		{name: "missing", missing: true, want: ArchiveObservationMissing},
		{name: "stale sequence", sequence: 2, observedSequence: 1, want: ArchiveObservationStale},
		{name: "clockless complete", sequence: 2, observedSequence: 2, want: ArchiveObservationComplete},
		{name: "negative metadata sequence", sequence: -3, observedSequence: 2, want: ArchiveObservationStale},
		{name: "unknown legacy sequence", sequence: 0, observedSequence: 2, want: ArchiveObservationUnknown},
		{name: "unknown invalid clock", sequence: 2, observedSequence: 2, source: "invalid", want: ArchiveObservationUnknown},
		{name: "stale source", sequence: 2, observedSequence: 3, source: "2026-09-02T00:00:00Z", observedSource: "2026-09-01T00:00:00Z", want: ArchiveObservationStale},
	} {
		t.Run(test.name, func(t *testing.T) {
			st, err := Open(ctx, filepath.Join(t.TempDir(), "source.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			repoID, err := st.UpsertRepository(ctx, Repository{Owner: "example", Name: "repo", FullName: "example/repo", UpdatedAt: "2026-09-01T00:00:00Z"})
			if err != nil {
				t.Fatal(err)
			}
			if !test.empty {
				thread := archiveCoverageThread(repoID, 1, "issue")
				id, err := st.UpsertThread(ctx, thread)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := st.DB().ExecContext(ctx, `update threads set updated_at_gh = ?, observation_sequence = ? where id = ?`, test.source, test.sequence, id); err != nil {
					t.Fatal(err)
				}
				if !test.missing && !test.drop {
					if _, err := st.DB().ExecContext(ctx, `insert into thread_child_observation_reservations(thread_id, family, source_updated_at, observation_sequence) values(?, 'comments', ?, ?)`, id, test.observedSource, test.observedSequence); err != nil {
						t.Fatal(err)
					}
				}
			}
			if test.drop {
				if _, err := st.DB().ExecContext(ctx, `drop table thread_child_observation_reservations`); err != nil {
					t.Fatal(err)
				}
			}
			got, err := st.archiveChildObservations(ctx, repoID, ThreadChildComments)
			if err != nil {
				t.Fatal(err)
			}
			if got.State != test.want {
				t.Fatalf("metric = %+v, want %q", got, test.want)
			}
		})
	}
}

func TestArchiveSourceObservationsKeepRepositoryScopeAndUnknownFreshness(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "source.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	first, second := seedArchiveCoverageRows(t, ctx, st)
	if _, err := st.RecordRun(ctx, RunRecord{RepoID: second, Kind: "sync", Scope: "all", Status: "success",
		StartedAt: "2026-09-01T00:00:00Z", FinishedAt: "2026-09-01T00:01:00Z",
		StatsJSON: `{"started_at":"2026-09-01T00:00:00Z","finished_at":"2026-09-01T00:01:00Z","metadata_only":false,"threads_synced":1}`}); err != nil {
		t.Fatal(err)
	}
	if err := st.ensurePortableMetadata(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := st.DB().ExecContext(ctx, `insert into portable_metadata(key,value) values('exported_at','2099-01-01T00:00:00Z')`); err != nil {
		t.Fatal(err)
	}
	coverage, err := st.ArchiveCoverage(ctx, ArchiveCoverageOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got, err := st.ArchiveSourceObservations(ctx, coverage)
	if err != nil {
		t.Fatal(err)
	}
	if got.RemoteFreshness != ArchiveObservationUnknown || len(got.Repositories) != 2 {
		t.Fatalf("source = %+v", got)
	}
	for _, repo := range got.Repositories {
		if repo.RepoID == first && repo.Inventory.State == ArchiveObservationComplete {
			t.Fatal("another repo established inventory coverage")
		}
		if repo.RepoID == second && repo.Inventory.State != ArchiveObservationComplete {
			t.Fatalf("second repository: %+v", repo)
		}
	}
	encoded, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	if !json.Valid(encoded) {
		t.Fatal("invalid source assessment")
	}
}
