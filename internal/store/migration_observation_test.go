package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestOpenRepairsMinimumThreadObservationSequence(t *testing.T) {
	ctx := context.Background()
	dbPath, _, threadID := seedMigrationPullRequest(t, "minimum-sequence-head", 11)
	st, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("converge seeded store: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close converged store: %v", err)
	}

	raw := openRawMigrationDB(t, dbPath)
	if _, err := raw.ExecContext(ctx, `pragma ignore_check_constraints = on`); err != nil {
		_ = raw.Close()
		t.Fatalf("disable check constraints: %v", err)
	}
	if _, err := raw.ExecContext(
		ctx,
		`update threads set observation_sequence = ? where id = ?`,
		int64(math.MinInt64),
		threadID,
	); err != nil {
		_ = raw.Close()
		t.Fatalf("poison thread observation sequence: %v", err)
	}
	if _, err := raw.ExecContext(ctx, `pragma ignore_check_constraints = off`); err != nil {
		_ = raw.Close()
		t.Fatalf("restore check constraints: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close poisoned store: %v", err)
	}

	diag := InspectSchema(ctx, dbPath)
	if !containsString(diag.PendingMigrations, migrationThreadObservationSequenceValue) {
		t.Fatalf("minimum sequence diagnostics = %#v", diag)
	}

	st, err = Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("repair minimum sequence: %v", err)
	}
	defer st.Close()
	var sequence int64
	if err := st.DB().QueryRowContext(
		ctx,
		`select observation_sequence from threads where id = ?`,
		threadID,
	).Scan(&sequence); err != nil {
		t.Fatalf("read repaired sequence: %v", err)
	}
	if sequence != 0 {
		t.Fatalf("repaired sequence = %d, want 0", sequence)
	}
	if _, err := st.DB().ExecContext(
		ctx,
		`update threads set observation_sequence = ? where id = ?`,
		int64(math.MinInt64),
		threadID,
	); err == nil || !strings.Contains(err.Error(), "CHECK constraint failed") {
		t.Fatalf("minimum canonical sequence update error = %v", err)
	}
}

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

func TestFreshSchemaDeclaresCanonicalObservationShapes(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open fresh schema database: %v", err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, schemaSQL); err != nil {
		t.Fatalf("apply fresh schema: %v", err)
	}

	st := &Store{db: db}
	checks := []struct {
		name string
		run  func(context.Context) bool
	}{
		{name: "threads", run: st.threadsHaveCanonicalShape},
		{
			name: "thread revisions",
			run: func(ctx context.Context) bool {
				return st.tableHasCanonicalSQL(
					ctx,
					"thread_revisions",
					canonicalThreadRevisionsCreateSQL,
				)
			},
		},
		{name: "thread observation allocator", run: st.threadObservationSequenceHasCurrentShape},
		{name: "thread child reservations", run: st.threadChildObservationReservationsHaveCurrentShape},
		{name: "workflow reservations", run: st.workflowRunObservationReservationsHaveCurrentShape},
	}
	for _, check := range checks {
		if !check.run(ctx) {
			t.Fatalf("fresh %s schema is not canonical", check.name)
		}
	}
	if _, err := db.ExecContext(ctx, `pragma foreign_keys = off`); err != nil {
		t.Fatalf("disable foreign keys for family constraint proof: %v", err)
	}
	for _, family := range threadChildObservationFamilies {
		if _, err := db.ExecContext(ctx, `
			insert into thread_child_observation_reservations(
				thread_id, family, observation_sequence
			)
			values(1, ?, 1)
		`, family); err != nil {
			t.Fatalf("canonical schema rejected family %q: %v", family, err)
		}
	}
	if _, err := db.ExecContext(ctx, `
		insert into thread_child_observation_reservations(
			thread_id, family, observation_sequence
		)
		values(1, 'unsupported', 1)
	`); err == nil || !strings.Contains(err.Error(), "CHECK constraint failed") {
		t.Fatalf("unsupported canonical family error = %v", err)
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
		`update threads
		 set evidence_source_updated_at = '', evidence_observation_sequence = 0
		 where id = ?`,
		`delete from thread_child_observation_reservations where thread_id = ?`,
		`delete from workflow_run_observation_reservations`,
		`update thread_observation_sequence set value = 0 where id = 1`,
		`pragma user_version = 10`,
	} {
		args := []any{}
		if strings.Contains(statement, "?") {
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
			source_updated_at text,
			observation_sequence integer
		);
		insert into thread_child_observation_reservations(
			thread_id, family, source_updated_at, observation_sequence
		)
			values
				(?, 'comments', '2026-07-12T00:02:00Z', 7),
				(?, 'comments', '2026-07-12T00:01:00Z', 17),
				(?, 'comments', '2026-07-12T00:03:00Z', 'poison'),
				(?, 'pull_request_files', '2026-07-12T00:02:00Z', 5),
				(?, 'pull_request_files', '2026-07-12T02:02:00+02:00', 9);

		drop table workflow_run_observation_reservations;
		create table workflow_run_observation_reservations (
			repo_id integer,
			head_sha text not null check (trim(head_sha) <> ''),
			source_updated_at text,
			observation_sequence integer
		);
		insert into workflow_run_observation_reservations(
			repo_id, head_sha, source_updated_at, observation_sequence
		)
			values
				(?, 'malformed-shared-head', '2026-07-12T00:02:00Z', 13),
				(?, 'malformed-shared-head', '2026-07-12T00:01:00Z', 19),
				(?, 'malformed-shared-head', '2026-07-12T00:03:00Z', 'poison'),
				(?, 'equivalent-head', '2026-07-12T00:02:00Z', 11),
				(?, 'equivalent-head', '2026-07-12T02:02:00+02:00', 15);
			pragma user_version = 10;
		`,
		threadID, threadID, threadID, threadID, threadID,
		repoID, repoID, repoID, repoID, repoID,
	); err != nil {
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
	var childSource, workflowSource string
	var childSequence, childRows, workflowSequence int64
	if err := st.DB().QueryRowContext(ctx, `
		select source_updated_at, observation_sequence, count(*)
		from thread_child_observation_reservations
		where thread_id = ? and family = 'comments'
	`, threadID).Scan(&childSource, &childSequence, &childRows); err != nil {
		t.Fatalf("read rebuilt child reservation: %v", err)
	}
	if childRows != 1 || childSource != "2026-07-12T00:02:00Z" || childSequence != 7 {
		t.Fatalf(
			"rebuilt child reservation = rows %d order %s/%d",
			childRows,
			childSource,
			childSequence,
		)
	}
	if err := st.DB().QueryRowContext(ctx, `
		select source_updated_at, observation_sequence
		from workflow_run_observation_reservations
		where repo_id = ? and head_sha = 'malformed-shared-head'
	`, repoID).Scan(&workflowSource, &workflowSequence); err != nil {
		t.Fatalf("read rebuilt workflow reservation: %v", err)
	}
	if workflowSource != "2026-07-12T00:02:00Z" || workflowSequence != 13 {
		t.Fatalf(
			"rebuilt workflow reservation = %s/%d",
			workflowSource,
			workflowSequence,
		)
	}
	var equivalentChildSource, equivalentWorkflowSource string
	var equivalentChildSequence, equivalentWorkflowSequence int64
	if err := st.DB().QueryRowContext(ctx, `
		select source_updated_at, observation_sequence
		from thread_child_observation_reservations
		where thread_id = ? and family = 'pull_request_files'
	`, threadID).Scan(&equivalentChildSource, &equivalentChildSequence); err != nil {
		t.Fatalf("read equivalent child reservation: %v", err)
	}
	if equivalentChildSource != "2026-07-12T02:02:00+02:00" ||
		equivalentChildSequence != 9 {
		t.Fatalf(
			"equivalent child reservation = %s/%d",
			equivalentChildSource,
			equivalentChildSequence,
		)
	}
	if err := st.DB().QueryRowContext(ctx, `
		select source_updated_at, observation_sequence
		from workflow_run_observation_reservations
		where repo_id = ? and head_sha = 'equivalent-head'
	`, repoID).Scan(&equivalentWorkflowSource, &equivalentWorkflowSequence); err != nil {
		t.Fatalf("read equivalent workflow reservation: %v", err)
	}
	if equivalentWorkflowSource != "2026-07-12T02:02:00+02:00" ||
		equivalentWorkflowSequence != 15 {
		t.Fatalf(
			"equivalent workflow reservation = %s/%d",
			equivalentWorkflowSource,
			equivalentWorkflowSequence,
		)
	}
	for _, statement := range []string{
		`update thread_child_observation_reservations
		 set observation_sequence = 'poison'
		 where thread_id = ? and family = 'comments'`,
		`update workflow_run_observation_reservations
		 set observation_sequence = 'poison'
		 where repo_id = ? and head_sha = 'malformed-shared-head'`,
	} {
		var arg any = threadID
		if strings.Contains(statement, "workflow_run") {
			arg = repoID
		}
		if _, err := st.DB().ExecContext(ctx, statement, arg); err == nil ||
			!strings.Contains(err.Error(), "CHECK constraint failed") {
			t.Fatalf("poisoned canonical reservation insert error = %v", err)
		}
	}
}

func TestMigrationRebuildsMalformedAllocatorAndConvergenceLookalikes(t *testing.T) {
	ctx := context.Background()
	dbPath, _, _ := seedMigrationPullRequest(t, "malformed-allocator-head", 11)
	st, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("converge seeded store: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close converged store: %v", err)
	}

	raw := openRawMigrationDB(t, dbPath)
	if _, err := raw.ExecContext(ctx, `
		drop table observation_schema_convergence;
		create table observation_schema_convergence (
			id integer,
			checked_observation_sequence integer
		);
		insert into observation_schema_convergence(id, checked_observation_sequence)
		values(1, 7), (1, 13), (1, 'poison');

		drop table thread_observation_sequence;
		create table thread_observation_sequence (
			id integer,
			value integer,
			last_started_at text
		);
		insert into thread_observation_sequence(id, value, last_started_at)
		values
			(1, 13, '2026-07-12T00:00:00Z'),
			(2, 17, '2026-07-12T01:00:00Z'),
			(3, 'poison', 42);
		pragma user_version = 10;
	`); err != nil {
		_ = raw.Close()
		t.Fatalf("prepare malformed allocator and convergence lookalikes: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close malformed allocator store: %v", err)
	}

	diag := InspectSchema(ctx, dbPath)
	for _, migration := range []string{
		migrationThreadObservationSequenceShape,
		migrationObservationSchemaConvergence,
	} {
		if !containsString(diag.PendingMigrations, migration) {
			t.Fatalf("malformed diagnostics = %#v, missing %q", diag, migration)
		}
	}

	st, err = Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("rebuild malformed allocator and convergence lookalikes: %v", err)
	}
	defer st.Close()
	if !st.threadObservationSequenceHasCurrentShape(ctx) {
		t.Fatal("allocator table was not rebuilt to canonical shape")
	}
	if !st.observationSchemaConvergenceHasCurrentShape(ctx) {
		t.Fatal("convergence table and triggers were not rebuilt to canonical shape")
	}
	var allocator, checked int64
	if err := st.DB().QueryRowContext(ctx, `
		select
			thread_observation_sequence.value,
			observation_schema_convergence.checked_observation_sequence
		from thread_observation_sequence
		join observation_schema_convergence using (id)
		where thread_observation_sequence.id = 1
	`).Scan(&allocator, &checked); err != nil {
		t.Fatalf("read rebuilt allocator and convergence watermark: %v", err)
	}
	if allocator != 17 || checked != allocator {
		t.Fatalf(
			"rebuilt allocator/convergence = %d/%d, want 17/17",
			allocator,
			checked,
		)
	}
	for _, statement := range []string{
		`update thread_observation_sequence set value = 'poison' where id = 1`,
		`update observation_schema_convergence
		 set checked_observation_sequence = 'poison' where id = 1`,
	} {
		if _, err := st.DB().ExecContext(ctx, statement); err == nil ||
			!strings.Contains(err.Error(), "CHECK constraint failed") {
			t.Fatalf("poisoned canonical allocator insert error = %v", err)
		}
	}
}

func TestMigrationRepairsMissingCanonicalConvergenceRow(t *testing.T) {
	ctx := context.Background()
	dbPath, _, _ := seedMigrationPullRequest(t, "missing-convergence-row-head", 11)
	st, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("converge seeded store: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close converged store: %v", err)
	}

	raw := openRawMigrationDB(t, dbPath)
	if _, err := raw.ExecContext(ctx, `
		delete from observation_schema_convergence where id = 1;
		pragma user_version = 10;
	`); err != nil {
		_ = raw.Close()
		t.Fatalf("remove canonical convergence row: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close missing convergence row store: %v", err)
	}

	st, err = Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("repair missing canonical convergence row: %v", err)
	}
	defer st.Close()
	var allocator, checked int64
	if err := st.DB().QueryRowContext(ctx, `
		select
			thread_observation_sequence.value,
			observation_schema_convergence.checked_observation_sequence
		from thread_observation_sequence
		join observation_schema_convergence using (id)
		where thread_observation_sequence.id = 1
	`).Scan(&allocator, &checked); err != nil {
		t.Fatalf("read repaired convergence row: %v", err)
	}
	if checked != allocator {
		t.Fatalf("repaired convergence watermark = %d, want allocator %d", checked, allocator)
	}
}

func TestThreadRevisionTransitionIndexDetection(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "gitcrawl.db")
	st, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open fresh store: %v", err)
	}
	defer st.Close()

	if st.threadRevisionsHaveUniqueContentHash(ctx) {
		t.Fatal("fresh schema unexpectedly has the legacy unique content-hash index")
	}
	if _, err := st.DB().ExecContext(ctx, `
		create unique index legacy_thread_revision_content
		on thread_revisions(thread_id, content_hash)
	`); err != nil {
		t.Fatalf("create legacy transition index: %v", err)
	}
	if !st.threadRevisionsHaveUniqueContentHash(ctx) {
		t.Fatal("legacy unique content-hash index was not detected")
	}
}

func TestSQLiteStoredSQLNormalizesSupportedDDL(t *testing.T) {
	tests := map[string]string{
		"create table sample(id integer)":                                             "CREATE TABLE sample(id integer)",
		"create index if not exists idx on sample(id)":                                "CREATE INDEX idx on sample(id)",
		"create index idx on sample(id)":                                              "CREATE INDEX idx on sample(id)",
		"create trigger if not exists trg after insert on sample begin select 1; end": "CREATE TRIGGER trg after insert on sample begin select 1; end",
		"create trigger trg after insert on sample begin select 1; end":               "CREATE TRIGGER trg after insert on sample begin select 1; end",
		"CREATE VIEW sample_view AS SELECT 1":                                         "CREATE VIEW sample_view AS SELECT 1",
	}
	for input, want := range tests {
		t.Run(input, func(t *testing.T) {
			if got := sqliteStoredSQL(input); got != want {
				t.Fatalf("sqliteStoredSQL(%q) = %q, want %q", input, got, want)
			}
		})
	}
}

func TestSchemaNextStepsCoversDiagnosticStates(t *testing.T) {
	tests := []struct {
		state       string
		pending     []string
		wantContain string
		wantCount   int
	}{
		{state: "missing", wantContain: "gitcrawl init", wantCount: 1},
		{state: "newer", wantContain: "Upgrade", wantCount: 1},
		{
			state:       "pending_migration",
			pending:     []string{"allocator", "convergence"},
			wantContain: "allocator, convergence",
			wantCount:   2,
		},
		{state: "error", wantContain: "SQLite file health", wantCount: 1},
		{state: "current", wantCount: 0},
	}
	for _, test := range tests {
		t.Run(test.state, func(t *testing.T) {
			got := schemaNextSteps(SchemaDiagnostics{
				State:             test.state,
				PendingMigrations: test.pending,
			})
			if len(got) != test.wantCount {
				t.Fatalf("schemaNextSteps(%q) = %#v, want %d entries", test.state, got, test.wantCount)
			}
			if test.wantContain != "" && !strings.Contains(strings.Join(got, "\n"), test.wantContain) {
				t.Fatalf("schemaNextSteps(%q) = %#v, missing %q", test.state, got, test.wantContain)
			}
		})
	}
}

func TestInspectSchemaClassifiesBoundaryStates(t *testing.T) {
	ctx := context.Background()
	if diag := InspectSchema(ctx, ""); diag.State != "missing" ||
		len(diag.NextSteps) != 1 ||
		!strings.Contains(diag.NextSteps[0], "configured db_path") {
		t.Fatalf("empty-path diagnostics = %#v", diag)
	}
	if diag := InspectSchema(ctx, string([]byte{0})); diag.State != "error" ||
		diag.Error == "" ||
		len(diag.NextSteps) != 1 {
		t.Fatalf("invalid-path diagnostics = %#v", diag)
	}

	tempDir := t.TempDir()
	missingPath := filepath.Join(tempDir, "missing.db")
	if diag := InspectSchema(ctx, missingPath); diag.State != "missing" ||
		diag.Exists ||
		len(diag.NextSteps) != 1 {
		t.Fatalf("missing-file diagnostics = %#v", diag)
	}

	corruptPath := filepath.Join(tempDir, "corrupt.db")
	if err := os.WriteFile(corruptPath, []byte("not a sqlite database"), 0o600); err != nil {
		t.Fatalf("write corrupt database fixture: %v", err)
	}
	if diag := InspectSchema(ctx, corruptPath); diag.State != "error" ||
		!diag.Exists ||
		diag.Error == "" ||
		len(diag.NextSteps) != 1 {
		t.Fatalf("corrupt-file diagnostics = %#v", diag)
	}
	if _, err := Open(ctx, corruptPath); err == nil {
		t.Fatal("writable open unexpectedly accepted corrupt database")
	}
	if _, err := OpenReadOnly(ctx, corruptPath); err == nil {
		t.Fatal("read-only open unexpectedly accepted corrupt database")
	}

	newerPath := filepath.Join(tempDir, "newer.db")
	st, err := Open(ctx, newerPath)
	if err != nil {
		t.Fatalf("open newer-schema fixture: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `pragma user_version = 11`); err != nil {
		_ = st.Close()
		t.Fatalf("mark newer-schema fixture: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close newer-schema fixture: %v", err)
	}
	if _, err := st.schemaVersion(ctx); err == nil ||
		!strings.Contains(err.Error(), "read schema version") {
		t.Fatalf("closed-store schema version error = %v", err)
	}
	if err := st.withForeignKeysDisabled(ctx, "closed store", func(*sql.Tx) error {
		return nil
	}); err == nil || !strings.Contains(err.Error(), "open closed store migration connection") {
		t.Fatalf("closed-store migration error = %v", err)
	}
	if diag := InspectSchema(ctx, newerPath); diag.State != "newer" ||
		!diag.Newer ||
		diag.PendingMigration ||
		len(diag.NextSteps) != 1 ||
		!strings.Contains(diag.NextSteps[0], "Upgrade") {
		t.Fatalf("newer-schema diagnostics = %#v", diag)
	}
	if _, err := OpenReadOnly(ctx, newerPath); err == nil ||
		!strings.Contains(err.Error(), "newer than supported") {
		t.Fatalf("newer read-only open error = %v", err)
	}
	if _, err := Open(ctx, newerPath); err == nil ||
		!strings.Contains(err.Error(), "newer than supported") {
		t.Fatalf("newer writable open error = %v", err)
	}
}

func TestObservationConvergenceWatermarkStates(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "gitcrawl.db")
	st, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open fresh store: %v", err)
	}
	defer st.Close()

	current, err := st.observationSchemaConvergenceIsCurrent(ctx)
	if err != nil || !current {
		t.Fatalf("fresh convergence state = %v, %v; want current", current, err)
	}
	if _, err := st.DB().ExecContext(ctx, `
		delete from observation_schema_convergence where id = 1
	`); err != nil {
		t.Fatalf("delete convergence watermark: %v", err)
	}
	current, err = st.observationSchemaConvergenceIsCurrent(ctx)
	if err != nil || current {
		t.Fatalf("missing convergence state = %v, %v; want not current", current, err)
	}
	if err := st.markObservationSchemaConverged(ctx); err != nil {
		t.Fatalf("restore convergence watermark: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `drop table observation_schema_convergence`); err != nil {
		t.Fatalf("drop convergence table: %v", err)
	}
	if _, err := st.observationSchemaConvergenceIsCurrent(ctx); err == nil ||
		!strings.Contains(err.Error(), "read observation schema convergence") {
		t.Fatalf("missing convergence table error = %v", err)
	}
}

func TestForeignKeyDisabledMigrationRejectsFailuresAndViolations(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "gitcrawl.db")
	st, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open fresh store: %v", err)
	}
	defer st.Close()

	sentinel := errors.New("forced migration failure")
	err = st.withForeignKeysDisabled(ctx, "forced callback", func(*sql.Tx) error {
		return sentinel
	})
	if !errors.Is(err, sentinel) || !strings.Contains(err.Error(), "migrate forced callback") {
		t.Fatalf("callback failure = %v", err)
	}

	err = st.withForeignKeysDisabled(ctx, "foreign key proof", func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `
			insert into thread_revisions(
				id, thread_id, source_updated_at, content_hash, title_hash,
				body_hash, labels_hash, raw_json_blob_id,
				observation_sequence, created_at
			)
			values(999, 999, '', 'content', 'title', 'body', 'labels', null, 0, '')
		`)
		return err
	})
	if err == nil || !strings.Contains(err.Error(), "introduced foreign key violations") {
		t.Fatalf("foreign key violation result = %v", err)
	}
	var orphanCount int64
	if err := st.DB().QueryRowContext(ctx, `
		select count(*) from thread_revisions where id = 999
	`).Scan(&orphanCount); err != nil {
		t.Fatalf("read rolled-back foreign key proof row: %v", err)
	}
	if orphanCount != 0 {
		t.Fatalf("foreign key proof committed %d orphan rows", orphanCount)
	}
	var foreignKeys int
	if err := st.DB().QueryRowContext(ctx, `pragma foreign_keys`).Scan(&foreignKeys); err != nil {
		t.Fatalf("read restored foreign key mode: %v", err)
	}
	if foreignKeys != 1 {
		t.Fatalf("foreign key mode = %d, want enabled", foreignKeys)
	}
}

func TestObservationMigrationHelpersPropagateSchemaErrors(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name      string
		dropTable string
		run       func(*Store) error
		want      string
	}{
		{
			name:      "legacy portable columns",
			dropTable: "repositories",
			run:       func(st *Store) error { return st.ensureLegacyPortableColumns(ctx) },
			want:      "no such table",
		},
		{
			name:      "legacy revision columns",
			dropTable: "thread_revisions",
			run:       func(st *Store) error { return st.ensureLegacyPortableColumns(ctx) },
			want:      "no such table",
		},
		{
			name:      "legacy thread columns",
			dropTable: "threads",
			run:       func(st *Store) error { return st.ensureLegacyPortableColumns(ctx) },
			want:      "no such table",
		},
		{
			name:      "evidence sequence",
			dropTable: "threads",
			run:       func(st *Store) error { return st.ensureThreadEvidenceObservationSequence(ctx) },
			want:      "backfill thread evidence observation order",
		},
		{
			name:      "child reservations",
			dropTable: "thread_child_observation_reservations",
			run:       func(st *Store) error { return st.ensureThreadChildObservationReservations(ctx) },
			want:      "backfill comment observation reservations",
		},
		{
			name:      "workflow reservations",
			dropTable: "workflow_run_observation_reservations",
			run:       func(st *Store) error { return st.ensureWorkflowRunObservationReservations(ctx) },
			want:      "backfill workflow run observation reservations",
		},
		{
			name:      "allocator floor",
			dropTable: "thread_observation_sequence",
			run:       func(st *Store) error { return st.ensureThreadObservationSequenceFloor(ctx) },
			want:      "reconcile thread observation sequence",
		},
		{
			name:      "convergence watermark",
			dropTable: "observation_schema_convergence",
			run:       func(st *Store) error { return st.markObservationSchemaConverged(ctx) },
			want:      "mark observation schema converged",
		},
		{
			name:      "allocator inspection",
			dropTable: "thread_observation_sequence",
			run: func(st *Store) error {
				_, _, err := st.threadObservationSequenceNeedsRepair(ctx)
				return err
			},
			want: "inspect thread observation allocator row",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dbPath := filepath.Join(t.TempDir(), "gitcrawl.db")
			st, err := Open(ctx, dbPath)
			if err != nil {
				t.Fatalf("open fresh store: %v", err)
			}
			defer st.Close()
			if _, err := st.DB().ExecContext(
				ctx,
				`drop table `+sqliteIdentifier(test.dropTable),
			); err != nil {
				t.Fatalf("drop %s: %v", test.dropTable, err)
			}
			err = test.run(st)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("%s error = %v, want %q", test.name, err, test.want)
			}
		})
	}
}

func TestSQLiteBusyRetryStopsOnCanceledContext(t *testing.T) {
	if IsTransientSQLiteBusy(nil) {
		t.Fatal("nil error classified as SQLite busy")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := 0
	err := withSQLiteBusyRetry(ctx, []time.Duration{time.Hour}, func() error {
		calls++
		return errors.New("database is locked")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled retry error = %v", err)
	}
	if calls != 1 {
		t.Fatalf("canceled retry calls = %d, want 1", calls)
	}
}

func TestQSQLBuildsFallbackQueries(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "gitcrawl.db")
	st, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open fresh store: %v", err)
	}
	defer st.Close()

	fallbackQueries := (&Store{db: st.DB()}).qsql()
	if fallbackQueries == nil {
		t.Fatal("qsql fallback did not construct generated queries")
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
		delete from workflow_run_observation_reservations
		where repo_id = ? and head_sha = 'legacy-shared-head';
		insert into workflow_run_observation_reservations(
			repo_id, head_sha, source_updated_at, observation_sequence
		)
		values(?, 'legacy-shared-head', '2026-07-12T00:00:31Z', 31);
		pragma user_version = 10;
	`, threadID, repoID, repoID); err != nil {
		_ = raw.Close()
		t.Fatalf("prepare legacy child reservation schema: %v", err)
	}
	var canonicalSequence, legacySequence int64
	if err := raw.QueryRowContext(ctx, `
		select observation_sequence
		from workflow_run_observation_reservations
		where repo_id = ? and head_sha = 'legacy-shared-head'
	`, repoID).Scan(&canonicalSequence); err != nil {
		_ = raw.Close()
		t.Fatalf("read seeded canonical workflow reservation: %v", err)
	}
	if err := raw.QueryRowContext(ctx, `
		select observation_sequence
		from thread_child_observation_reservations
		where thread_id = ? and family = 'workflow_runs'
	`, threadID).Scan(&legacySequence); err != nil {
		_ = raw.Close()
		t.Fatalf("read seeded legacy workflow reservation: %v", err)
	}
	if canonicalSequence != 31 || legacySequence != 37 {
		_ = raw.Close()
		t.Fatalf(
			"seeded workflow reservations = canonical %d, legacy %d; want 31 and 37",
			canonicalSequence,
			legacySequence,
		)
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

	var workflowSource string
	var workflowSequence int64
	if err := st.DB().QueryRowContext(ctx, `
		select source_updated_at, observation_sequence
		from workflow_run_observation_reservations
		where repo_id = ? and head_sha = 'legacy-shared-head'
	`, repoID).Scan(&workflowSource, &workflowSequence); err != nil {
		t.Fatalf("read migrated workflow reservation: %v", err)
	}
	if workflowSource != "" || workflowSequence != 37 {
		t.Fatalf(
			"migrated workflow reservation = %q/%d, want unknown source at 37",
			workflowSource,
			workflowSequence,
		)
	}
	if applied, err := st.ReserveWorkflowRunObservation(
		ctx,
		repoID,
		"legacy-shared-head",
		"2026-07-12T00:01:00Z",
		36,
	); err != nil || applied {
		t.Fatalf("lower legacy workflow observation = %t, %v", applied, err)
	}
	if applied, err := st.ReserveWorkflowRunObservation(
		ctx,
		repoID,
		"legacy-shared-head",
		"2026-07-12T00:01:00Z",
		38,
	); err != nil || !applied {
		t.Fatalf("newer legacy workflow observation = %t, %v", applied, err)
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
