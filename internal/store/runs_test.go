package store

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestRecordAndListRuns(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t, ctx)
	defer st.Close()

	repoID, err := st.UpsertRepository(ctx, Repository{
		Owner:     "openclaw",
		Name:      "gitcrawl",
		FullName:  "openclaw/gitcrawl",
		RawJSON:   "{}",
		UpdatedAt: "2026-04-26T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	if _, err := st.RecordRun(ctx, RunRecord{
		RepoID:     repoID,
		Kind:       "sync",
		Scope:      "open",
		Status:     "success",
		StartedAt:  "2026-04-26T00:00:00Z",
		FinishedAt: "2026-04-26T00:00:01Z",
		StatsJSON:  `{"threads_synced":1}`,
	}); err != nil {
		t.Fatalf("record run: %v", err)
	}

	runs, err := st.ListRuns(ctx, repoID, "sync", 10)
	if err != nil {
		t.Fatalf("list runs: %v", err)
	}
	if len(runs) != 1 || runs[0].Kind != "sync" || runs[0].Status != "success" {
		t.Fatalf("unexpected runs: %#v", runs)
	}
}

func TestStatusAcceptsCompletedSyncRuns(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t, ctx)
	defer st.Close()

	repoID, err := st.UpsertRepository(ctx, Repository{
		Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl", RawJSON: "{}", UpdatedAt: "2026-04-26T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	if _, err := st.RecordRun(ctx, RunRecord{
		RepoID: repoID, Kind: "sync", Scope: "open", Status: "completed",
		StartedAt: "2026-04-26T00:00:00Z", FinishedAt: "2026-04-26T00:00:01Z",
	}); err != nil {
		t.Fatalf("record run: %v", err)
	}
	status, err := st.Status(ctx)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.LastSyncAt.IsZero() {
		t.Fatalf("expected last sync time, got %#v", status)
	}
}

func TestLastSuccessfulSyncAt(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t, ctx)
	defer st.Close()

	repoID, err := st.UpsertRepository(ctx, Repository{
		Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl", RawJSON: "{}", UpdatedAt: "2026-04-26T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	for _, step := range []struct {
		status     string
		startedAt  string
		finishedAt string
		want       string
	}{
		{"failed", "2026-04-26T00:00:00Z", "2026-04-26T00:00:30Z", ""},
		{"success", "2026-04-26T00:01:00Z", "2026-04-26T00:01:30Z", "2026-04-26T00:01:30Z"},
		{"failed", "2026-04-26T00:02:00Z", "2026-04-26T00:02:30Z", "2026-04-26T00:01:30Z"},
		{"completed", "2026-04-26T00:03:00Z", "2026-04-26T00:03:30Z", "2026-04-26T00:03:30Z"},
		{"checkpoint", "2026-04-26T00:04:00Z", "2026-04-26T00:04:30Z", "2026-04-26T00:03:30Z"},
	} {
		if _, err := st.RecordRun(ctx, RunRecord{
			RepoID: repoID, Kind: "sync", Scope: "open", Status: step.status,
			StartedAt: step.startedAt, FinishedAt: step.finishedAt,
		}); err != nil {
			t.Fatalf("record %s run at %s: %v", step.status, step.finishedAt, err)
		}
		lastSync, err := st.LastSuccessfulSyncAt(ctx, repoID)
		if err != nil {
			t.Fatalf("last sync after %s at %s: %v", step.status, step.finishedAt, err)
		}
		if step.want == "" {
			if !lastSync.IsZero() {
				t.Fatalf("failed-only history returned last sync %s", lastSync)
			}
			continue
		}
		if got := lastSync.UTC().Format(time.RFC3339Nano); got != step.want {
			t.Fatalf("last sync after %s at %s = %s, want %s", step.status, step.finishedAt, got, step.want)
		}
	}
}

func TestLastSuccessfulListSyncAtIgnoresTargetedRuns(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t, ctx)
	defer st.Close()

	repoID, err := st.UpsertRepository(ctx, Repository{
		Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl", RawJSON: "{}", UpdatedAt: "2026-04-26T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	if _, err := st.RecordRun(ctx, RunRecord{
		RepoID: repoID, Kind: "sync", Scope: "numbers:13", Status: "success",
		StartedAt: "2026-04-26T00:03:00Z", FinishedAt: "2026-04-26T00:03:30Z",
	}); err != nil {
		t.Fatalf("record targeted run: %v", err)
	}
	if lastSync, err := st.LastSuccessfulListSyncAt(ctx, repoID, "open"); err != nil || !lastSync.IsZero() {
		t.Fatalf("targeted run should not count as broad list sync: last=%s err=%v", lastSync, err)
	}
	if _, err := st.RecordRun(ctx, RunRecord{
		RepoID: repoID, Kind: "sync", Scope: "all", Status: "success",
		StartedAt: "2026-04-26T00:04:00Z", FinishedAt: "2026-04-26T00:04:30Z",
	}); err != nil {
		t.Fatalf("record all run: %v", err)
	}
	lastSync, err := st.LastSuccessfulListSyncAt(ctx, repoID, "open")
	if err != nil {
		t.Fatalf("last broad sync: %v", err)
	}
	want, _ := time.Parse(time.RFC3339Nano, "2026-04-26T00:04:30Z")
	if !lastSync.Equal(want) {
		t.Fatalf("last broad sync = %s, want %s", lastSync, want)
	}
	runs, err := st.SuccessfulListSyncRuns(ctx, repoID, "all")
	if err != nil {
		t.Fatalf("list successful all-state runs: %v", err)
	}
	if len(runs) != 1 || runs[0].Scope != "all" ||
		runs[0].FinishedAt != "2026-04-26T00:04:30Z" {
		t.Fatalf("successful all-state runs = %+v", runs)
	}
	if _, err := st.SuccessfulListSyncRuns(ctx, repoID, "invalid"); err == nil {
		t.Fatal("invalid list sync state succeeded")
	}
}

func TestOctopoolSyncCheckpointIntervals(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t, ctx)
	defer st.Close()
	repo, err := st.UpsertRepository(ctx, Repository{Owner: "fixture", Name: "repo", FullName: "fixture/repo", RawJSON: "{}"})
	if err != nil {
		t.Fatal(err)
	}
	at := func(v string) time.Time {
		x, err := time.Parse(time.RFC3339Nano, v)
		if err != nil {
			t.Fatal(err)
		}
		return x
	}
	record := func(since, start, finish string, abort bool) *OctopoolSyncCheckpoint {
		t.Helper()
		var cp *OctopoolSyncCheckpoint
		stop := errors.New("fixture rollback")
		err := st.WithTx(ctx, func(tx *Store) error {
			var err error
			cp, err = tx.QualifyOctopoolSyncCheckpoint(ctx, repo, "123", since, at(start))
			if err != nil {
				return err
			}
			stats := map[string]any{"requested_since": since, "metadata_only": false}
			if cp != nil {
				stats["octopool_checkpoint"] = cp
			}
			body, _ := json.Marshal(stats)
			_, err = tx.RecordRun(ctx, RunRecord{RepoID: repo, Kind: "sync", Scope: "all", Status: "success", StartedAt: start, FinishedAt: finish, StatsJSON: string(body)})
			if err == nil && abort {
				return stop
			}
			return err
		})
		if (abort && !errors.Is(err, stop)) || (!abort && err != nil) {
			t.Fatal(err)
		}
		return cp
	}
	read := func() *OctopoolSyncCheckpoint {
		t.Helper()
		cp, err := st.ReadOctopoolSyncCheckpoint(ctx, repo)
		if err != nil {
			t.Fatal(err)
		}
		return cp
	}
	first := record("2026-04-01T00:00:00Z", "2026-04-02T00:00:00Z", "2026-04-02T02:00:00Z", false)
	if first == nil || !reflect.DeepEqual(first, read()) || first.Through != "2026-04-02T00:00:00Z" {
		t.Fatal("checkpoint used finish time", first)
	}
	if gap := record("2026-04-03T00:00:00Z", "2026-04-04T00:00:00Z", "2026-04-04T01:00:00Z", false); gap != nil || !reflect.DeepEqual(first, read()) {
		t.Fatal("disjoint window skipped gap")
	}
	record("2026-04-01T23:59:00Z", "2026-04-05T00:00:00Z", "2026-04-05T01:00:00Z", true)
	if !reflect.DeepEqual(first, read()) {
		t.Fatal("rollback advanced checkpoint")
	}
	next := record("2026-04-01T23:59:00Z", "2026-04-05T00:00:00Z", "2026-04-05T01:00:00Z", false)
	if next == nil || next.CoverageSince != first.CoverageSince || next.Through != "2026-04-05T00:00:00Z" {
		t.Fatal("coverage lower bound changed", next)
	}
	if changed, err := st.QualifyOctopoolSyncCheckpoint(ctx, repo, "456", next.RequestSince, at("2026-04-06T00:00:00Z")); err != nil || changed != nil {
		t.Fatal("provider identity rebound", changed, err)
	}
	next.IncludeComments = false
	body, _ := json.Marshal(map[string]any{"requested_since": next.RequestSince, "metadata_only": false, "octopool_checkpoint": next})
	if _, err := st.RecordRun(ctx, RunRecord{RepoID: repo, Kind: "sync", Scope: "all", Status: "success", StartedAt: next.Through, FinishedAt: "2026-04-05T01:00:00Z", StatsJSON: string(body)}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.ReadOctopoolSyncCheckpoint(ctx, repo); !errors.Is(err, ErrOctopoolSyncCheckpoint) {
		t.Fatal("malformed qualification fell back", err)
	}
}
