package store

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMigrationRepairsPartialV10ObservationBackfills(t *testing.T) {
	ctx := context.Background()
	dbPath, repoID, threadID := seedMigrationPullRequest(t, "partial-v10-head", 11)

	raw := openRawMigrationDB(t, dbPath)
	for _, statement := range []string{
		`delete from thread_child_observation_reservations`,
		`delete from workflow_run_observation_reservations`,
		`pragma user_version = 10`,
	} {
		if _, err := raw.ExecContext(ctx, statement); err != nil {
			_ = raw.Close()
			t.Fatalf("prepare partial v10 state with %q: %v", statement, err)
		}
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close partial v10 store: %v", err)
	}

	st, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("reopen partial v10 store: %v", err)
	}
	defer st.Close()

	var count, minimum, maximum int64
	if err := st.DB().QueryRowContext(ctx, `
		select count(*), min(observation_sequence), max(observation_sequence)
		from thread_child_observation_reservations
		where thread_id = ?
	`, threadID).Scan(&count, &minimum, &maximum); err != nil {
		t.Fatalf("read repaired child reservations: %v", err)
	}
	if count != 6 || minimum != 11 || maximum != 11 {
		t.Fatalf("child reservations = count %d, min %d, max %d; want six at 11", count, minimum, maximum)
	}

	var workflowSequence int64
	if err := st.DB().QueryRowContext(ctx, `
		select observation_sequence
		from workflow_run_observation_reservations
		where repo_id = ? and head_sha = 'partial-v10-head'
	`, repoID).Scan(&workflowSequence); err != nil {
		t.Fatalf("read repaired workflow reservation: %v", err)
	}
	if workflowSequence != 11 {
		t.Fatalf("workflow reservation = %d, want 11", workflowSequence)
	}
}

func TestMigrationReconcilesAllocatorFromEveryPersistedSequenceTable(t *testing.T) {
	ctx := context.Background()
	dbPath, repoID, threadID := seedMigrationPullRequest(t, "allocator-head", 5)

	raw := openRawMigrationDB(t, dbPath)
	statements := []struct {
		query string
		args  []any
	}{
		{`update threads set observation_sequence = -17, evidence_observation_sequence = 19 where id = ?`, []any{threadID}},
		{`update thread_revisions set observation_sequence = 23 where thread_id = ?`, []any{threadID}},
		{`insert into thread_child_observation_reservations(thread_id, family, observation_sequence)
			values(?, 'comments', 29)
			on conflict(thread_id, family) do update set observation_sequence = excluded.observation_sequence`, []any{threadID}},
		{`insert into workflow_run_observation_reservations(repo_id, head_sha, observation_sequence)
			values(?, 'allocator-head', 31)
			on conflict(repo_id, head_sha) do update set observation_sequence = excluded.observation_sequence`, []any{repoID}},
		{`drop table thread_observation_sequence`, nil},
		{`pragma user_version = 10`, nil},
	}
	for _, statement := range statements {
		if _, err := raw.ExecContext(ctx, statement.query, statement.args...); err != nil {
			_ = raw.Close()
			t.Fatalf("prepare allocator state with %q: %v", statement.query, err)
		}
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close allocator store: %v", err)
	}

	st, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("reopen allocator store: %v", err)
	}
	defer st.Close()

	var floor int64
	if err := st.DB().QueryRowContext(ctx, `
		select value from thread_observation_sequence where id = 1
	`).Scan(&floor); err != nil {
		t.Fatalf("read allocator floor: %v", err)
	}
	if floor != 31 {
		t.Fatalf("allocator floor = %d, want 31", floor)
	}
	next, err := st.NextThreadObservationSequence(ctx, "2026-07-12T08:00:00Z")
	if err != nil {
		t.Fatalf("advance allocator: %v", err)
	}
	if next != 32 {
		t.Fatalf("next allocator sequence = %d, want 32", next)
	}
}

func TestMigrationNormalizesLegacyV10ChildReservationShape(t *testing.T) {
	ctx := context.Background()
	dbPath, repoID, threadID := seedMigrationPullRequest(t, "legacy-shared-head", 7)

	raw := openRawMigrationDB(t, dbPath)
	if _, err := raw.ExecContext(ctx, `
		drop table thread_child_observation_reservations;
		create table thread_child_observation_reservations (
			thread_id integer not null references threads(id) on delete cascade,
			family text not null check (family in (
				'comments',
				'pull_request_details',
				'pull_request_files',
				'pull_request_commits',
				'pull_request_checks',
				'workflow_runs',
				'pull_request_review_threads'
			)),
			observation_sequence integer not null check (observation_sequence > 0),
			primary key(thread_id, family)
		);
		insert into thread_child_observation_reservations(
			thread_id, family, observation_sequence
		)
		values(?, 'workflow_runs', 37);
		update workflow_run_observation_reservations
		set observation_sequence = 31
		where repo_id = ? and head_sha = 'legacy-shared-head';
		pragma user_version = 10;
	`, threadID, repoID); err != nil {
		_ = raw.Close()
		t.Fatalf("prepare legacy child reservation schema: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close legacy child reservation store: %v", err)
	}

	before, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("read legacy db before diagnostics: %v", err)
	}
	diag := InspectSchema(ctx, dbPath)
	if diag.ChildReservationsCurrent ||
		!containsString(diag.PendingMigrations, "thread_child_observation_reservations_shape") {
		t.Fatalf("legacy child reservation diagnostics = %#v", diag)
	}
	after, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("read legacy db after diagnostics: %v", err)
	}
	if !bytes.Equal(after, before) {
		t.Fatal("schema diagnostics mutated legacy child reservation database")
	}

	st, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("reopen legacy child reservation store: %v", err)
	}
	defer st.Close()

	if !st.threadChildObservationReservationsHaveCurrentShape(ctx) {
		t.Fatal("migrated child reservation table does not have the current family constraint")
	}
	var createSQL string
	if err := st.DB().QueryRowContext(ctx, `
		select sql from sqlite_schema
		where type = 'table' and name = 'thread_child_observation_reservations'
	`).Scan(&createSQL); err != nil {
		t.Fatalf("read migrated child reservation schema: %v", err)
	}
	if strings.Contains(createSQL, "'workflow_runs'") {
		t.Fatalf("migrated child reservation schema still permits workflow_runs:\n%s", createSQL)
	}
	if _, err := st.DB().ExecContext(ctx, `
		insert into thread_child_observation_reservations(
			thread_id, family, observation_sequence
		)
		values(?, 'workflow_runs', 41)
	`, threadID); err == nil || !strings.Contains(err.Error(), "CHECK constraint failed") {
		t.Fatalf("legacy workflow family insert error = %v, want CHECK constraint failure", err)
	}

	var workflowSequence int64
	if err := st.DB().QueryRowContext(ctx, `
		select observation_sequence
		from workflow_run_observation_reservations
		where repo_id = ? and head_sha = 'legacy-shared-head'
	`, repoID).Scan(&workflowSequence); err != nil {
		t.Fatalf("read migrated workflow reservation: %v", err)
	}
	if workflowSequence != 37 {
		t.Fatalf("migrated workflow reservation = %d, want 37", workflowSequence)
	}

	currentDiag := InspectSchema(ctx, dbPath)
	if !currentDiag.ChildReservationsCurrent ||
		containsString(currentDiag.PendingMigrations, "thread_child_observation_reservations_shape") {
		t.Fatalf("current child reservation diagnostics = %#v", currentDiag)
	}
}

func seedMigrationPullRequest(t *testing.T, headSHA string, sequence int64) (string, int64, int64) {
	t.Helper()
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "gitcrawl.db")
	st, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open seed store: %v", err)
	}
	repoID, err := st.UpsertRepository(ctx, Repository{
		Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl",
		RawJSON: "{}", UpdatedAt: "2026-07-12T00:00:00Z",
	})
	if err != nil {
		_ = st.Close()
		t.Fatalf("seed repository: %v", err)
	}
	thread := Thread{
		RepoID: repoID, GitHubID: headSHA, Number: 101, Kind: "pull_request", State: "open",
		Title: "migration seed", HTMLURL: "https://github.com/openclaw/gitcrawl/pull/101",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: headSHA,
		UpdatedAtGitHub: "2026-07-12T00:00:00Z",
		UpdatedAt:       "2026-07-12T00:00:00Z",
	}
	upsert, err := st.UpsertThreadObservation(ctx, thread, UpsertThreadOptions{ObservationSequence: sequence})
	if err != nil {
		_ = st.Close()
		t.Fatalf("seed thread observation: %v", err)
	}
	thread.ID = upsert.ID
	if _, err := st.UpsertThreadRevisionAndFingerprint(ctx, ThreadEvidence{
		Thread:              thread,
		ObservationSequence: sequence,
	}, "2026-07-12T00:01:00Z"); err != nil {
		_ = st.Close()
		t.Fatalf("seed thread revision: %v", err)
	}
	if err := st.UpsertPullRequestCacheFamilies(
		ctx,
		PullRequestDetail{
			ThreadID: upsert.ID, RepoID: repoID, Number: 101, HeadSHA: headSHA,
			RawJSON: "{}", FetchedAt: "2026-07-12T00:00:00Z", UpdatedAt: "2026-07-12T00:00:00Z",
		},
		nil,
		nil,
		nil,
		nil,
		PullRequestHydrationFamilies{Details: true},
	); err != nil {
		_ = st.Close()
		t.Fatalf("seed pull request detail: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close seed store: %v", err)
	}
	return dbPath, repoID, upsert.ID
}

func openRawMigrationDB(t *testing.T, dbPath string) *sql.DB {
	t.Helper()
	raw, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open raw migration store: %v", err)
	}
	return raw
}
