package store

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func TestApplyWorkflowRunSnapshotMergesEqualTuplesContentAware(t *testing.T) {
	for _, test := range []struct {
		name       string
		first      []WorkflowRun
		second     []WorkflowRun
		wantFirst  int
		wantSecond int
	}{
		{
			name:       "subset then superset",
			first:      workflowSnapshotRuns("901"),
			second:     workflowSnapshotRuns("900", "901"),
			wantFirst:  1,
			wantSecond: 1,
		},
		{
			name:       "superset then subset",
			first:      workflowSnapshotRuns("900", "901"),
			second:     workflowSnapshotRuns("901"),
			wantFirst:  2,
			wantSecond: 0,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
			if err != nil {
				t.Fatalf("open store: %v", err)
			}
			defer st.Close()

			repoID := workflowSnapshotRepository(t, ctx, st)
			setWorkflowSnapshotRepoID(test.first, repoID)
			setWorkflowSnapshotRepoID(test.second, repoID)
			baseline, err := st.ReadWorkflowRunSnapshotState(ctx, repoID, "shared-head")
			if err != nil {
				t.Fatalf("read baseline: %v", err)
			}
			first, err := st.ApplyWorkflowRunSnapshot(
				ctx,
				repoID,
				"shared-head",
				"2026-07-12T00:02:00Z",
				1,
				baseline,
				test.first,
			)
			if err != nil {
				t.Fatalf("apply first snapshot: %v", err)
			}
			second, err := st.ApplyWorkflowRunSnapshot(
				ctx,
				repoID,
				"shared-head",
				"2026-07-12T00:02:00Z",
				1,
				baseline,
				test.second,
			)
			if err != nil {
				t.Fatalf("apply second snapshot: %v", err)
			}
			if first.RowsSynced != test.wantFirst || second.RowsSynced != test.wantSecond {
				t.Fatalf(
					"rows synced = first %d second %d, want %d/%d",
					first.RowsSynced,
					second.RowsSynced,
					test.wantFirst,
					test.wantSecond,
				)
			}
			assertWorkflowSnapshotRunIDs(t, ctx, st, repoID, "900", "901")
		})
	}
}

func TestApplyWorkflowRunSnapshotRejectsStaleCASAfterDeletion(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repoID := workflowSnapshotRepository(t, ctx, st)
	initialRuns := workflowSnapshotRuns("900", "901")
	setWorkflowSnapshotRepoID(initialRuns, repoID)
	empty, err := st.ReadWorkflowRunSnapshotState(ctx, repoID, "shared-head")
	if err != nil {
		t.Fatalf("read empty snapshot: %v", err)
	}
	if _, err := st.ApplyWorkflowRunSnapshot(
		ctx,
		repoID,
		"shared-head",
		"2026-07-12T00:02:00Z",
		1,
		empty,
		initialRuns,
	); err != nil {
		t.Fatalf("seed workflow snapshot: %v", err)
	}
	validated, err := st.ReadWorkflowRunSnapshotState(ctx, repoID, "shared-head")
	if err != nil {
		t.Fatalf("read validated snapshot: %v", err)
	}

	staleReady := make(chan struct{})
	releaseStale := make(chan struct{})
	type applyResult struct {
		result WorkflowRunSnapshotResult
		err    error
	}
	staleResult := make(chan applyResult, 1)
	go func() {
		close(staleReady)
		<-releaseStale
		result, err := st.ApplyWorkflowRunSnapshot(
			ctx,
			repoID,
			"shared-head",
			"2026-07-12T00:02:00Z",
			3,
			validated,
			initialRuns,
		)
		staleResult <- applyResult{result: result, err: err}
	}()
	<-staleReady

	deletedRuns := workflowSnapshotRuns("900")
	setWorkflowSnapshotRepoID(deletedRuns, repoID)
	deleted, err := st.ApplyWorkflowRunSnapshot(
		ctx,
		repoID,
		"shared-head",
		"2026-07-12T00:02:00Z",
		2,
		validated,
		deletedRuns,
	)
	if err != nil {
		t.Fatalf("apply verified deletion: %v", err)
	}
	if !deleted.Applied {
		t.Fatal("verified deletion was not applied")
	}
	close(releaseStale)
	stale := <-staleResult
	if stale.err != nil {
		t.Fatalf("apply stale snapshot: %v", stale.err)
	}
	if stale.result.Applied || stale.result.RowsSynced != 0 {
		t.Fatalf("stale snapshot result = %+v, want skipped", stale.result)
	}
	assertWorkflowSnapshotRunIDs(t, ctx, st, repoID, "900")
	source, sequence, found, err := st.WorkflowRunObservationReservation(
		ctx,
		repoID,
		"shared-head",
	)
	if err != nil || !found {
		t.Fatalf("workflow reservation = %q/%d found=%t err=%v", source, sequence, found, err)
	}
	if source != "2026-07-12T00:02:00Z" || sequence != 2 {
		t.Fatalf("workflow reservation after stale replay = %s/%d", source, sequence)
	}
}

func TestReadWorkflowRunSnapshotStateRejectsClocklessStoredRun(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repoID := workflowSnapshotRepository(t, ctx, st)
	if _, err := st.DB().ExecContext(ctx, `
		insert into github_workflow_runs(
			repo_id, run_id, head_sha, raw_json, fetched_at
		) values(?, '900', 'shared-head', '{}', '2026-07-12T01:00:00Z')
	`, repoID); err != nil {
		t.Fatalf("seed clockless workflow run: %v", err)
	}
	if _, err := st.ReadWorkflowRunSnapshotState(
		ctx,
		repoID,
		"shared-head",
	); err == nil || !strings.Contains(err.Error(), "missing created_at and updated_at") {
		t.Fatalf("clockless stored workflow run error = %v", err)
	}
}

func workflowSnapshotRepository(t *testing.T, ctx context.Context, st *Store) int64 {
	t.Helper()
	repoID, err := st.UpsertRepository(ctx, Repository{
		Owner:     "openclaw",
		Name:      "gitcrawl",
		FullName:  "openclaw/gitcrawl",
		RawJSON:   "{}",
		UpdatedAt: "2026-07-12T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("repository: %v", err)
	}
	return repoID
}

func workflowSnapshotRuns(runIDs ...string) []WorkflowRun {
	runs := make([]WorkflowRun, 0, len(runIDs))
	for _, runID := range runIDs {
		run := WorkflowRun{
			RunID:        runID,
			HeadBranch:   "shared-branch",
			HeadSHA:      "shared-head",
			Status:       "completed",
			Conclusion:   "success",
			WorkflowName: "workflow-" + runID,
			Event:        "pull_request",
			RawJSON:      `{"id":"` + runID + `"}`,
			FetchedAt:    "2026-07-12T01:00:00Z",
		}
		if runID == "900" {
			run.RunNumber = 1
			run.CreatedAtGH = "2026-07-12T00:00:00Z"
			run.UpdatedAtGH = "2026-07-12T00:01:00Z"
		} else {
			run.RunNumber = 2
			run.CreatedAtGH = "2026-07-12T00:01:00Z"
			run.UpdatedAtGH = "2026-07-12T00:02:00Z"
		}
		runs = append(runs, run)
	}
	return runs
}

func setWorkflowSnapshotRepoID(runs []WorkflowRun, repoID int64) {
	for index := range runs {
		runs[index].RepoID = repoID
	}
}

func assertWorkflowSnapshotRunIDs(
	t *testing.T,
	ctx context.Context,
	st *Store,
	repoID int64,
	want ...string,
) {
	t.Helper()
	runs, err := st.ListWorkflowRuns(ctx, repoID, WorkflowRunListOptions{
		HeadSHA: "shared-head",
		Limit:   -1,
	})
	if err != nil {
		t.Fatalf("list workflow runs: %v", err)
	}
	got := make(map[string]bool, len(runs))
	for _, run := range runs {
		got[run.RunID] = true
	}
	if len(got) != len(want) {
		t.Fatalf("workflow run ids = %v, want %v", got, want)
	}
	for _, runID := range want {
		if !got[runID] {
			t.Fatalf("workflow run ids = %v, missing %s", got, runID)
		}
	}
}
