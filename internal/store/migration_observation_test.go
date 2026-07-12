package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestHealthyV10WritableReopenIsByteStable(t *testing.T) {
	ctx := context.Background()
	dbPath, _, _ := seedMigrationPullRequest(t, "stable-reopen-head", 11)

	st, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("converge seeded store: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close converged store: %v", err)
	}
	checkpointMigrationDB(t, dbPath)
	before := snapshotMigrationDBFiles(t, dbPath)

	st, err = Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("healthy writable reopen: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close healthy writable reopen: %v", err)
	}
	after := snapshotMigrationDBFiles(t, dbPath)
	if before != after {
		t.Fatalf("healthy writable reopen changed database files:\nbefore=%s\nafter=%s", before, after)
	}
}

func TestOpenRepairsCurrentV10MissingSchemaObject(t *testing.T) {
	ctx := context.Background()
	dbPath, _, _ := seedMigrationPullRequest(t, "missing-schema-object-head", 11)
	st, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("converge seeded store: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close converged store: %v", err)
	}

	raw := openRawMigrationDB(t, dbPath)
	if _, err := raw.ExecContext(ctx, `drop table comments`); err != nil {
		_ = raw.Close()
		t.Fatalf("drop required comments table: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close missing-schema-object store: %v", err)
	}

	st, err = Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("repair missing schema object: %v", err)
	}
	defer st.Close()
	if !st.hasTable(ctx, "comments") {
		t.Fatal("comments table was not restored")
	}
	var indexCount int64
	if err := st.DB().QueryRowContext(ctx, `
		select count(*)
		from sqlite_schema
		where type = 'index' and name = 'idx_comments_thread_type'
	`).Scan(&indexCount); err != nil {
		t.Fatalf("read restored comments index: %v", err)
	}
	if indexCount != 1 {
		t.Fatalf("restored comments index count = %d, want 1", indexCount)
	}
}

func TestInspectSchemaReportsSemanticObservationDriftWithoutMutation(t *testing.T) {
	ctx := context.Background()
	dbPath, _, threadID := seedMigrationPullRequest(t, "semantic-drift-head", 11)
	st, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("converge seeded store: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close converged store: %v", err)
	}

	raw := openRawMigrationDB(t, dbPath)
	for _, statement := range []string{
		`update thread_revisions set observation_sequence = 23 where thread_id = ?`,
		`delete from thread_child_observation_reservations where thread_id = ?`,
		`delete from workflow_run_observation_reservations`,
		`update thread_observation_sequence set value = 0 where id = 1`,
		`pragma user_version = 10`,
	} {
		args := []any{}
		if strings.Contains(statement, "thread_id = ?") {
			args = append(args, threadID)
		}
		if _, err := raw.ExecContext(ctx, statement, args...); err != nil {
			_ = raw.Close()
			t.Fatalf("prepare semantic drift with %q: %v", statement, err)
		}
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close semantic drift store: %v", err)
	}
	checkpointMigrationDB(t, dbPath)
	before := snapshotMigrationDBFiles(t, dbPath)

	diag := InspectSchema(ctx, dbPath)
	if diag.State != "pending_migration" || diag.Current || !diag.PendingMigration {
		t.Fatalf("semantic drift diagnostics = %#v", diag)
	}
	for _, migration := range []string{
		migrationThreadEvidenceObservationSequence,
		migrationThreadChildReservationsBackfill,
		migrationWorkflowRunReservationsBackfill,
		migrationThreadObservationSequenceFloor,
	} {
		if !containsString(diag.PendingMigrations, migration) {
			t.Fatalf("semantic drift migrations = %#v, missing %q", diag.PendingMigrations, migration)
		}
	}
	if len(diag.NextSteps) < 2 || !strings.Contains(diag.NextSteps[0], "repair") {
		t.Fatalf("semantic drift next steps = %#v", diag.NextSteps)
	}
	after := snapshotMigrationDBFiles(t, dbPath)
	if before.database != after.database {
		t.Fatalf("read-only semantic diagnostics changed database bytes:\nbefore=%s\nafter=%s", before, after)
	}

	st, err = Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("repair semantic drift: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close repaired store: %v", err)
	}
	repaired := InspectSchema(ctx, dbPath)
	if repaired.State != "current" || len(repaired.PendingMigrations) != 0 {
		t.Fatalf("repaired semantic diagnostics = %#v", repaired)
	}
}

func TestMigrationRebuildsMalformedReservationLookalikes(t *testing.T) {
	ctx := context.Background()
	dbPath, repoID, threadID := seedMigrationPullRequest(t, "malformed-shared-head", 11)
	st, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("converge seeded store: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close converged store: %v", err)
	}

	raw := openRawMigrationDB(t, dbPath)
	if _, err := raw.ExecContext(ctx, `
		drop table thread_child_observation_reservations;
		create table thread_child_observation_reservations (
			thread_id integer,
			family text not null check (family in (
				'comments',
				'pull_request_details',
				'pull_request_files',
				'pull_request_commits',
				'pull_request_checks',
				'pull_request_review_threads'
			)),
			observation_sequence integer
		);
		insert into thread_child_observation_reservations(
			thread_id, family, observation_sequence
		)
		values
			(?, 'comments', 7),
			(?, 'comments', 17);

		drop table workflow_run_observation_reservations;
		create table workflow_run_observation_reservations (
			repo_id integer,
			head_sha text not null check (trim(head_sha) <> ''),
			observation_sequence integer
		);
		insert into workflow_run_observation_reservations(
			repo_id, head_sha, observation_sequence
		)
		values
			(?, 'malformed-shared-head', 13),
			(?, 'malformed-shared-head', 19);
		pragma user_version = 10;
	`, threadID, threadID, repoID, repoID); err != nil {
		_ = raw.Close()
		t.Fatalf("prepare malformed reservation lookalikes: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close malformed reservation store: %v", err)
	}

	diag := InspectSchema(ctx, dbPath)
	for _, migration := range []string{
		migrationThreadChildReservationsShape,
		migrationWorkflowRunReservationsShape,
	} {
		if !containsString(diag.PendingMigrations, migration) {
			t.Fatalf("malformed diagnostics = %#v, missing %q", diag, migration)
		}
	}

	st, err = Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("rebuild malformed reservation lookalikes: %v", err)
	}
	defer st.Close()
	if !st.threadChildObservationReservationsHaveCurrentShape(ctx) {
		t.Fatal("child reservation table was not rebuilt to canonical shape")
	}
	if !st.workflowRunObservationReservationsHaveCurrentShape(ctx) {
		t.Fatal("workflow reservation table was not rebuilt to canonical shape")
	}
	var childSequence, childRows, workflowSequence int64
	if err := st.DB().QueryRowContext(ctx, `
		select max(observation_sequence), count(*)
		from thread_child_observation_reservations
		where thread_id = ? and family = 'comments'
	`, threadID).Scan(&childSequence, &childRows); err != nil {
		t.Fatalf("read rebuilt child reservation: %v", err)
	}
	if childRows != 1 || childSequence != 17 {
		t.Fatalf("rebuilt child reservation = rows %d sequence %d, want one row at 17", childRows, childSequence)
	}
	if err := st.DB().QueryRowContext(ctx, `
		select observation_sequence
		from workflow_run_observation_reservations
		where repo_id = ? and head_sha = 'malformed-shared-head'
	`, repoID).Scan(&workflowSequence); err != nil {
		t.Fatalf("read rebuilt workflow reservation: %v", err)
	}
	if workflowSequence != 19 {
		t.Fatalf("rebuilt workflow reservation = %d, want 19", workflowSequence)
	}
}

func TestObservationSchemaMigrationsMatchFreshV10Exactly(t *testing.T) {
	ctx := context.Background()
	freshPath := filepath.Join(t.TempDir(), "fresh.db")
	fresh, err := Open(ctx, freshPath)
	if err != nil {
		t.Fatalf("open fresh store: %v", err)
	}
	if err := fresh.Close(); err != nil {
		t.Fatalf("close fresh store: %v", err)
	}
	freshFingerprint := migrationSchemaFingerprint(t, freshPath)

	for _, version := range []int{4, 6, 8, 9} {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			dbPath, _, _ := seedMigrationPullRequest(
				t,
				fmt.Sprintf("schema-v%d-head", version),
				11,
			)
			downgradeObservationSchema(t, dbPath, version)

			st, err := Open(ctx, dbPath)
			if err != nil {
				t.Fatalf("migrate schema v%d: %v", version, err)
			}
			if err := st.Close(); err != nil {
				t.Fatalf("close migrated schema v%d: %v", version, err)
			}
			migratedFingerprint := migrationSchemaFingerprint(t, dbPath)
			if freshFingerprint != migratedFingerprint {
				t.Fatalf(
					"schema v%d migration differs from fresh v10:\n%s",
					version,
					firstFingerprintDifference(freshFingerprint, migratedFingerprint),
				)
			}
		})
	}
}

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
	if err := st.Close(); err != nil {
		t.Fatalf("close repaired partial v10 store: %v", err)
	}
	checkpointMigrationDB(t, dbPath)
	before := snapshotMigrationDBFiles(t, dbPath)
	st, err = Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("reopen repaired partial v10 store: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close idempotent partial v10 reopen: %v", err)
	}
	after := snapshotMigrationDBFiles(t, dbPath)
	if before != after {
		t.Fatalf("recovered partial v10 reopen changed database files:\nbefore=%s\nafter=%s", before, after)
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

type migrationDBFiles struct {
	database [sha256.Size]byte
	wal      [sha256.Size]byte
	shm      [sha256.Size]byte
	walSize  int64
	shmSize  int64
	walFound bool
	shmFound bool
}

func (state migrationDBFiles) String() string {
	return fmt.Sprintf(
		"db=%x wal=%t/%d/%x shm=%t/%d/%x",
		state.database,
		state.walFound,
		state.walSize,
		state.wal,
		state.shmFound,
		state.shmSize,
		state.shm,
	)
}

func checkpointMigrationDB(t *testing.T, dbPath string) {
	t.Helper()
	raw := openRawMigrationDB(t, dbPath)
	if _, err := raw.Exec(`pragma wal_checkpoint(truncate)`); err != nil {
		_ = raw.Close()
		t.Fatalf("checkpoint migration db: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close checkpointed migration db: %v", err)
	}
}

func snapshotMigrationDBFiles(t *testing.T, dbPath string) migrationDBFiles {
	t.Helper()
	state := migrationDBFiles{database: hashMigrationFile(t, dbPath)}
	for _, companion := range []struct {
		path  string
		hash  *[sha256.Size]byte
		size  *int64
		found *bool
	}{
		{path: dbPath + "-wal", hash: &state.wal, size: &state.walSize, found: &state.walFound},
		{path: dbPath + "-shm", hash: &state.shm, size: &state.shmSize, found: &state.shmFound},
	} {
		info, err := os.Stat(companion.path)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			t.Fatalf("stat %s: %v", companion.path, err)
		}
		*companion.found = true
		*companion.size = info.Size()
		*companion.hash = hashMigrationFile(t, companion.path)
	}
	return state
}

func hashMigrationFile(t *testing.T, path string) [sha256.Size]byte {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return sha256.Sum256(content)
}

func migrationSchemaFingerprint(t *testing.T, dbPath string) string {
	t.Helper()
	raw := openRawMigrationDB(t, dbPath)
	defer raw.Close()
	rows, err := raw.Query(`
		select type, name, tbl_name, coalesce(sql, '')
		from sqlite_schema
		where name not like 'sqlite_%'
		order by type, name
	`)
	if err != nil {
		t.Fatalf("query schema fingerprint: %v", err)
	}
	defer rows.Close()
	var fingerprint strings.Builder
	for rows.Next() {
		var objectType, name, table, createSQL string
		if err := rows.Scan(&objectType, &name, &table, &createSQL); err != nil {
			t.Fatalf("scan schema fingerprint: %v", err)
		}
		fmt.Fprintf(&fingerprint, "%s\t%s\t%s\t%s\n", objectType, name, table, createSQL)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read schema fingerprint: %v", err)
	}
	return fingerprint.String()
}

func firstFingerprintDifference(fresh, migrated string) string {
	freshLines := strings.Split(fresh, "\n")
	migratedLines := strings.Split(migrated, "\n")
	limit := len(freshLines)
	if len(migratedLines) < limit {
		limit = len(migratedLines)
	}
	for i := 0; i < limit; i++ {
		if freshLines[i] != migratedLines[i] {
			return fmt.Sprintf("line %d\nfresh:    %s\nmigrated: %s", i+1, freshLines[i], migratedLines[i])
		}
	}
	return fmt.Sprintf("line counts differ: fresh=%d migrated=%d", len(freshLines), len(migratedLines))
}

func downgradeObservationSchema(t *testing.T, dbPath string, version int) {
	t.Helper()
	ctx := context.Background()
	raw := openRawMigrationDB(t, dbPath)
	defer raw.Close()
	var statements []string
	switch version {
	case 4:
		statements = []string{
			`drop index if exists idx_thread_revisions_thread_observation`,
			`create table thread_revisions_legacy (
				id integer primary key,
				thread_id integer not null references threads(id) on delete cascade,
				source_updated_at text,
				content_hash text not null,
				title_hash text not null,
				body_hash text not null,
				labels_hash text not null,
				created_at text not null,
				unique(thread_id, content_hash)
			)`,
			`insert into thread_revisions_legacy(
				id, thread_id, source_updated_at, content_hash, title_hash,
				body_hash, labels_hash, created_at
			)
			select id, thread_id, source_updated_at, content_hash, title_hash,
				body_hash, labels_hash, created_at
			from thread_revisions`,
			`drop table thread_revisions`,
			`alter table thread_revisions_legacy rename to thread_revisions`,
			`alter table threads drop column evidence_observation_sequence`,
			`alter table threads drop column observation_sequence`,
			`drop table thread_observation_sequence`,
			`drop table thread_child_observation_reservations`,
			`drop table workflow_run_observation_reservations`,
		}
	case 6:
		statements = []string{
			`drop index if exists idx_thread_revisions_thread_observation`,
			`alter table thread_revisions drop column observation_sequence`,
			`alter table threads drop column evidence_observation_sequence`,
			`alter table threads drop column observation_sequence`,
			`drop table thread_observation_sequence`,
			`drop table thread_child_observation_reservations`,
			`drop table workflow_run_observation_reservations`,
		}
	case 8:
		statements = []string{
			`alter table threads drop column evidence_observation_sequence`,
			`drop table thread_child_observation_reservations`,
			`drop table workflow_run_observation_reservations`,
		}
	case 9:
		statements = []string{
			`drop table thread_child_observation_reservations`,
			`drop table workflow_run_observation_reservations`,
		}
	default:
		t.Fatalf("unsupported legacy schema version %d", version)
	}
	statements = append(statements, fmt.Sprintf(`pragma user_version = %d`, version))
	for _, statement := range statements {
		if _, err := raw.ExecContext(ctx, statement); err != nil {
			t.Fatalf("downgrade schema v%d with %q: %v", version, statement, err)
		}
	}
}
