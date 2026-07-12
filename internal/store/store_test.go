package store

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type fakeSQLiteCodeError int

func (e fakeSQLiteCodeError) Error() string {
	return fmt.Sprintf("sqlite code %d", e)
}

func (e fakeSQLiteCodeError) Code() int {
	return int(e)
}

func TestSQLiteBusyRetryRecovers(t *testing.T) {
	ctx := context.Background()
	attempts := 0
	err := withSQLiteBusyRetry(ctx, []time.Duration{0, 0}, func() error {
		attempts++
		if attempts < 3 {
			return fmt.Errorf("upsert thread: %w", fakeSQLiteCodeError(5))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("retry should recover: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
}

func TestSQLiteBusyRetryExhausts(t *testing.T) {
	ctx := context.Background()
	attempts := 0
	err := withSQLiteBusyRetry(ctx, []time.Duration{0, 0}, func() error {
		attempts++
		return fmt.Errorf("commit transaction: %w", fakeSQLiteCodeError(6))
	})
	if err == nil || !strings.Contains(err.Error(), "sqlite busy after 3 attempts") {
		t.Fatalf("expected exhausted busy error, got %v", err)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
}

func TestSQLiteBusyRetryDoesNotRetryOtherErrors(t *testing.T) {
	ctx := context.Background()
	attempts := 0
	want := errors.New("database disk image is malformed")
	err := withSQLiteBusyRetry(ctx, []time.Duration{0, 0}, func() error {
		attempts++
		return want
	})
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
}

func TestOpenMigratesSchema(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	var version int
	if err := st.DB().QueryRowContext(ctx, `pragma user_version`).Scan(&version); err != nil {
		t.Fatalf("read user_version: %v", err)
	}
	if version != schemaVersion {
		t.Fatalf("schema version: got %d want %d", version, schemaVersion)
	}
}

func TestOpenMigratesAuthorAssociationFromVersionFour(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "gitcrawl.db")
	st, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `
		insert into repositories(id, owner, name, full_name, raw_json, updated_at)
		values(1, 'openclaw', 'gitcrawl', 'openclaw/gitcrawl', '{}', '2026-07-12T00:00:00Z');
		insert into threads(
			id, repo_id, github_id, number, kind, state, title, html_url,
			labels_json, assignees_json, raw_json, content_hash, updated_at
		) values(
			1, 1, '101', 101, 'issue', 'open', 'migration fixture',
			'https://github.com/openclaw/gitcrawl/issues/101',
			'[]', '[]', '{}', 'hash', '2026-07-12T00:00:00Z'
		);
	`); err != nil {
		t.Fatalf("seed store: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	raw, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open raw store: %v", err)
	}
	if _, err := raw.ExecContext(ctx, `
		alter table threads drop column author_association;
		pragma user_version = 4;
	`); err != nil {
		_ = raw.Close()
		t.Fatalf("downgrade schema fixture: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close raw store: %v", err)
	}

	st, err = Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("reopen migrated store: %v", err)
	}
	defer st.Close()
	if !st.hasColumn(ctx, "threads", "author_association") {
		t.Fatal("author_association column was not restored")
	}
	var version int
	var title string
	if err := st.DB().QueryRowContext(ctx, `pragma user_version`).Scan(&version); err != nil {
		t.Fatalf("read user_version: %v", err)
	}
	if version != schemaVersion {
		t.Fatalf("schema version: got %d want %d", version, schemaVersion)
	}
	if err := st.DB().QueryRowContext(ctx, `select title from threads where id = 1`).Scan(&title); err != nil {
		t.Fatalf("read preserved thread: %v", err)
	}
	if title != "migration fixture" {
		t.Fatalf("thread title = %q, want migration fixture", title)
	}
}

func TestOpenMigratesV071PortableVersionFourThreadRevisionHistory(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "gitcrawl.db")
	st, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
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
	threadID, err := st.UpsertThread(ctx, Thread{
		RepoID:        repoID,
		GitHubID:      "101",
		Number:        101,
		Kind:          "issue",
		State:         "open",
		Title:         "revision migration",
		HTMLURL:       "https://github.com/openclaw/gitcrawl/issues/101",
		LabelsJSON:    "[]",
		AssigneesJSON: "[]",
		RawJSON:       "{}",
		ContentHash:   "thread",
		UpdatedAt:     "2026-07-12T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("thread: %v", err)
	}
	result, err := st.UpsertThreadRevisionAndFingerprint(ctx, ThreadEvidence{Thread: Thread{
		ID:            threadID,
		RepoID:        repoID,
		Number:        101,
		Kind:          "issue",
		State:         "open",
		Title:         "revision migration",
		LabelsJSON:    "[]",
		AssigneesJSON: "[]",
		UpdatedAt:     "2026-07-12T00:00:00Z",
	}}, "2026-07-12T00:01:00Z")
	if err != nil {
		t.Fatalf("thread enrichment: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	raw, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open raw store: %v", err)
	}
	if _, err := raw.ExecContext(ctx, `pragma foreign_keys = off`); err != nil {
		_ = raw.Close()
		t.Fatalf("disable foreign keys: %v", err)
	}
	tx, err := raw.BeginTx(ctx, nil)
	if err != nil {
		_ = raw.Close()
		t.Fatalf("begin legacy schema fixture: %v", err)
	}
	migratedThreadID := threadID + 1000
	if _, err := tx.ExecContext(
		ctx,
		`update thread_revisions set thread_id = ? where thread_id = ?`,
		migratedThreadID,
		threadID,
	); err != nil {
		_ = tx.Rollback()
		_ = raw.Close()
		t.Fatalf("move legacy revision to high thread id: %v", err)
	}
	if _, err := tx.ExecContext(
		ctx,
		`update threads set id = ? where id = ?`,
		migratedThreadID,
		threadID,
	); err != nil {
		_ = tx.Rollback()
		_ = raw.Close()
		t.Fatalf("move legacy thread above revision id: %v", err)
	}
	for _, stmt := range []string{
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
			id, thread_id, source_updated_at, content_hash, title_hash, body_hash,
			labels_hash, created_at
		)
		select id, thread_id, source_updated_at, content_hash, title_hash, body_hash,
			labels_hash, created_at
		from thread_revisions`,
		`drop table thread_revisions`,
		`alter table thread_revisions_legacy rename to thread_revisions`,
		`alter table threads drop column evidence_observation_sequence`,
		`alter table threads drop column observation_sequence`,
		`drop table thread_observation_sequence`,
		`drop table thread_child_observation_reservations`,
		`pragma user_version = 4`,
	} {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			_ = tx.Rollback()
			_ = raw.Close()
			t.Fatalf("build legacy schema fixture: %v", err)
		}
	}
	if err := tx.Commit(); err != nil {
		_ = raw.Close()
		t.Fatalf("commit legacy schema fixture: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close raw store: %v", err)
	}

	st, err = Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("reopen migrated store: %v", err)
	}
	defer st.Close()
	if st.threadRevisionsHaveUniqueContentHash(ctx) {
		t.Fatal("legacy thread revision uniqueness was not removed")
	}
	if !st.hasColumn(ctx, "thread_revisions", "raw_json_blob_id") {
		t.Fatal("portable-pruned raw_json_blob_id column was not restored")
	}
	if !st.hasColumn(ctx, "thread_revisions", "observation_sequence") {
		t.Fatal("thread revision observation sequence column was not restored")
	}
	if !st.hasColumn(ctx, "threads", "observation_sequence") {
		t.Fatal("thread observation sequence column was not restored")
	}
	if !st.hasColumn(ctx, "threads", "evidence_observation_sequence") {
		t.Fatal("thread evidence observation sequence column was not restored")
	}
	if !st.hasTable(ctx, "thread_observation_sequence") {
		t.Fatal("thread observation sequence table was not restored")
	}
	if !st.hasTable(ctx, "thread_child_observation_reservations") {
		t.Fatal("thread child observation reservation table was not restored")
	}
	var version, fingerprints int
	var observationSequence, threadObservationSequence, evidenceObservationSequence, childReservations int64
	if err := st.DB().QueryRowContext(ctx, `pragma user_version`).Scan(&version); err != nil {
		t.Fatalf("read user_version: %v", err)
	}
	if version != schemaVersion {
		t.Fatalf("schema version: got %d want %d", version, schemaVersion)
	}
	if err := st.DB().QueryRowContext(ctx, `select count(*) from thread_fingerprints where thread_revision_id = ?`, result.RevisionID).Scan(&fingerprints); err != nil {
		t.Fatalf("read preserved fingerprint: %v", err)
	}
	if fingerprints != 1 {
		t.Fatalf("preserved fingerprints = %d, want 1", fingerprints)
	}
	if err := st.DB().QueryRowContext(ctx, `
		select observation_sequence
		from thread_revisions
		where id = ?
	`, result.RevisionID).Scan(&observationSequence); err != nil {
		t.Fatalf("read migrated observation sequence: %v", err)
	}
	if observationSequence != 0 {
		t.Fatalf("migrated observation sequence = %d, want legacy marker 0", observationSequence)
	}
	if err := st.DB().QueryRowContext(ctx, `
		select observation_sequence, evidence_observation_sequence
		from threads
		where id = ?
	`, migratedThreadID).Scan(&threadObservationSequence, &evidenceObservationSequence); err != nil {
		t.Fatalf("read migrated thread observation sequence: %v", err)
	}
	if threadObservationSequence != 0 || evidenceObservationSequence != 0 {
		t.Fatalf(
			"migrated thread sequences = parent %d, evidence %d; want legacy markers",
			threadObservationSequence,
			evidenceObservationSequence,
		)
	}
	if err := st.DB().QueryRowContext(ctx, `
		select count(*)
		from thread_child_observation_reservations
		where thread_id = ?
	`, migratedThreadID).Scan(&childReservations); err != nil {
		t.Fatalf("read migrated child reservations: %v", err)
	}
	if childReservations != 0 {
		t.Fatalf("v0.7.1 child reservations = %d, want legacy marker absence", childReservations)
	}
	coverage, err := st.ArchiveCoverage(ctx, ArchiveCoverageOptions{})
	if err != nil {
		t.Fatalf("archive coverage after migration: %v", err)
	}
	if len(coverage.Rows) != 1 ||
		coverage.Rows[0].Enrichment.Revisions.Fresh != 1 ||
		coverage.Rows[0].Enrichment.Fingerprints.Fresh != 1 {
		t.Fatalf("migrated enrichment freshness = %+v", coverage.Rows)
	}
	nextObservationSequence, err := st.NextThreadObservationSequence(
		ctx,
		"2026-07-12T00:03:00Z",
	)
	if err != nil {
		t.Fatalf("next observation sequence: %v", err)
	}
	if nextObservationSequence <= observationSequence {
		t.Fatalf(
			"next observation sequence = %d, want > %d",
			nextObservationSequence,
			observationSequence,
		)
	}
	if _, err := st.DB().ExecContext(ctx, `
		insert into thread_revisions(
			thread_id, source_updated_at, content_hash, title_hash, body_hash, labels_hash, created_at
		)
		select thread_id, source_updated_at, content_hash, title_hash, body_hash, labels_hash, '2026-07-12T00:02:00Z'
		from thread_revisions
		where id = ?
	`, result.RevisionID); err != nil {
		t.Fatalf("insert repeated content hash after migration: %v", err)
	}
	rows, err := st.DB().QueryContext(ctx, `pragma foreign_key_check`)
	if err != nil {
		t.Fatalf("foreign key check: %v", err)
	}
	defer rows.Close()
	if rows.Next() {
		t.Fatal("migration left a foreign key violation")
	}
}

func TestOpenMigrationPreservesClocklessStaleRevision(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "gitcrawl.db")
	st, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
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
	thread := Thread{
		RepoID:        repoID,
		GitHubID:      "101",
		Number:        101,
		Kind:          "issue",
		State:         "open",
		Title:         "clockless migration",
		HTMLURL:       "https://github.com/openclaw/gitcrawl/issues/101",
		LabelsJSON:    "[]",
		AssigneesJSON: "[]",
		RawJSON:       "{}",
		ContentHash:   "clockless",
		UpdatedAt:     "2026-07-12T00:00:00Z",
	}
	thread.ID, err = st.UpsertThread(ctx, thread)
	if err != nil {
		t.Fatalf("thread: %v", err)
	}
	enrichment, err := st.UpsertThreadRevisionAndFingerprint(
		ctx,
		ThreadEvidence{Thread: thread},
		"2026-07-12T00:01:00Z",
	)
	if err != nil {
		t.Fatalf("thread enrichment: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `
		update threads
		set updated_at = '2026-07-12T00:02:00Z'
		where id = ?
	`, thread.ID); err != nil {
		t.Fatalf("advance thread after revision: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	raw, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open raw store: %v", err)
	}
	for _, stmt := range []string{
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
			id, thread_id, source_updated_at, content_hash, title_hash, body_hash,
			labels_hash, created_at
		)
		select id, thread_id, source_updated_at, content_hash, title_hash, body_hash,
			labels_hash, created_at
		from thread_revisions`,
		`drop table thread_revisions`,
		`alter table thread_revisions_legacy rename to thread_revisions`,
		`alter table threads drop column evidence_observation_sequence`,
		`alter table threads drop column observation_sequence`,
		`drop table thread_observation_sequence`,
		`drop table thread_child_observation_reservations`,
		`pragma user_version = 4`,
	} {
		if _, err := raw.ExecContext(ctx, stmt); err != nil {
			_ = raw.Close()
			t.Fatalf("build stale legacy fixture: %v", err)
		}
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close raw store: %v", err)
	}

	st, err = Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("reopen migrated store: %v", err)
	}
	defer st.Close()
	var revisionSequence, threadSequence, evidenceSequence int64
	if err := st.DB().QueryRowContext(ctx, `
		select observation_sequence
		from thread_revisions
		where id = ?
	`, enrichment.RevisionID).Scan(&revisionSequence); err != nil {
		t.Fatalf("read migrated revision sequence: %v", err)
	}
	if err := st.DB().QueryRowContext(ctx, `
		select observation_sequence, evidence_observation_sequence
		from threads
		where id = ?
	`, thread.ID).Scan(&threadSequence, &evidenceSequence); err != nil {
		t.Fatalf("read migrated thread sequence: %v", err)
	}
	if revisionSequence != 0 || threadSequence != 0 || evidenceSequence != 0 {
		t.Fatalf(
			"migrated stale sequences = revision %d, thread %d, evidence %d; want legacy markers",
			revisionSequence,
			threadSequence,
			evidenceSequence,
		)
	}
	coverage, err := st.ArchiveCoverage(ctx, ArchiveCoverageOptions{})
	if err != nil {
		t.Fatalf("archive coverage after migration: %v", err)
	}
	if len(coverage.Rows) != 1 ||
		coverage.Rows[0].Enrichment.Revisions.Stale != 1 ||
		coverage.Rows[0].Enrichment.Fingerprints.Stale != 1 {
		t.Fatalf("migrated stale enrichment coverage = %+v", coverage.Rows)
	}
}

func TestMigrationReconcilesThreadObservationSequenceOnce(t *testing.T) {
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
	thread := Thread{
		RepoID: repoID, GitHubID: "sequence-floor", Number: 102, Kind: "issue", State: "open",
		Title: "sequence floor", Body: "reconcile once during migration",
		HTMLURL:         "https://github.com/openclaw/gitcrawl/issues/102",
		LabelsJSON:      "[]",
		AssigneesJSON:   "[]",
		RawJSON:         "{}",
		ContentHash:     "sequence-floor",
		UpdatedAtGitHub: "2026-07-12T00:00:00Z",
		UpdatedAt:       "2026-07-12T00:00:00Z",
	}
	upsert, err := st.UpsertThreadObservation(
		ctx,
		thread,
		UpsertThreadOptions{ObservationSequence: 19},
	)
	if err != nil {
		t.Fatalf("thread observation: %v", err)
	}
	thread.ID = upsert.ID
	if _, err := st.UpsertThreadRevisionAndFingerprint(ctx, ThreadEvidence{
		Thread:              thread,
		ObservationSequence: 17,
	}, "2026-07-12T00:01:00Z"); err != nil {
		t.Fatalf("thread revision: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	raw, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open raw store: %v", err)
	}
	if _, err := raw.ExecContext(
		ctx,
		`update threads set observation_sequence = -19 where id = ?`,
		thread.ID,
	); err != nil {
		_ = raw.Close()
		t.Fatalf("prepare negative thread sequence: %v", err)
	}
	for _, statement := range []string{
		`update thread_observation_sequence set value = 1 where id = 1`,
		`pragma user_version = 7`,
	} {
		if _, err := raw.ExecContext(ctx, statement); err != nil {
			_ = raw.Close()
			t.Fatalf("prepare sequence migration with %q: %v", statement, err)
		}
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close raw store: %v", err)
	}

	st, err = Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("reopen migrated store: %v", err)
	}
	defer st.Close()
	var floor int64
	if err := st.DB().QueryRowContext(ctx, `
		select value
		from thread_observation_sequence
		where id = 1
	`).Scan(&floor); err != nil {
		t.Fatalf("read reconciled sequence floor: %v", err)
	}
	if floor != 19 {
		t.Fatalf("sequence floor = %d, want 19", floor)
	}
	next, err := st.NextThreadObservationSequence(ctx, "2026-07-12T00:02:00Z")
	if err != nil {
		t.Fatalf("next observation sequence: %v", err)
	}
	if next != 20 {
		t.Fatalf("next observation sequence = %d, want 20", next)
	}
}

func TestMigrationBackfillsThreadEvidenceObservationSequence(t *testing.T) {
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
	thread := Thread{
		RepoID: repoID, GitHubID: "evidence-migration", Number: 103, Kind: "issue", State: "open",
		Title: "evidence migration", Body: "backfill durable reservation",
		HTMLURL:         "https://github.com/openclaw/gitcrawl/issues/103",
		LabelsJSON:      "[]",
		AssigneesJSON:   "[]",
		RawJSON:         "{}",
		ContentHash:     "evidence-migration",
		UpdatedAtGitHub: "2026-07-12T00:00:00Z",
		UpdatedAt:       "2026-07-12T00:00:00Z",
	}
	upsert, err := st.UpsertThreadObservation(ctx, thread, UpsertThreadOptions{
		IncompleteEvidence:  true,
		ObservationSequence: 9,
	})
	if err != nil {
		t.Fatalf("thread observation: %v", err)
	}
	thread.ID = upsert.ID
	if _, err := st.UpsertThreadRevisionAndFingerprint(ctx, ThreadEvidence{
		Thread:              thread,
		ObservationSequence: 7,
	}, "2026-07-12T00:01:00Z"); err != nil {
		t.Fatalf("thread revision: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	raw, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open raw store: %v", err)
	}
	for _, statement := range []string{
		`alter table threads drop column evidence_observation_sequence`,
		`pragma user_version = 8`,
	} {
		if _, err := raw.ExecContext(ctx, statement); err != nil {
			_ = raw.Close()
			t.Fatalf("prepare evidence migration with %q: %v", statement, err)
		}
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close raw store: %v", err)
	}

	st, err = Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("reopen migrated store: %v", err)
	}
	defer st.Close()
	var version int
	var evidenceSource string
	var parentSequence, evidenceSequence, childSequence int64
	if err := st.DB().QueryRowContext(ctx, `pragma user_version`).Scan(&version); err != nil {
		t.Fatalf("read user_version: %v", err)
	}
	if err := st.DB().QueryRowContext(ctx, `
		select observation_sequence, evidence_source_updated_at,
			evidence_observation_sequence
		from threads
		where id = ?
	`, thread.ID).Scan(&parentSequence, &evidenceSource, &evidenceSequence); err != nil {
		t.Fatalf("read migrated evidence sequence: %v", err)
	}
	if version != schemaVersion || parentSequence != -9 ||
		evidenceSource != "2026-07-12T00:00:00Z" || evidenceSequence != 7 {
		t.Fatalf(
			"migrated evidence state = version %d, parent %d, evidence %s/%d",
			version,
			parentSequence,
			evidenceSource,
			evidenceSequence,
		)
	}
	if err := st.DB().QueryRowContext(ctx, `
		select observation_sequence
		from thread_child_observation_reservations
		where thread_id = ? and family = 'comments'
	`, thread.ID).Scan(&childSequence); err != nil {
		t.Fatalf("read migrated comment reservation: %v", err)
	}
	if childSequence != 7 {
		t.Fatalf("migrated comment reservation = %d, want 7", childSequence)
	}
}

func TestMigrationBackfillsActualThreadEvidenceTuple(t *testing.T) {
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
	thread := Thread{
		RepoID: repoID, GitHubID: "evidence-tuple", Number: 104, Kind: "issue", State: "open",
		Title: "evidence tuple", Body: "preserve one actual revision order",
		HTMLURL:         "https://github.com/openclaw/gitcrawl/issues/104",
		LabelsJSON:      "[]",
		AssigneesJSON:   "[]",
		RawJSON:         `{"version":1}`,
		ContentHash:     "version-1",
		UpdatedAtGitHub: "2026-07-12T00:01:00Z",
		UpdatedAt:       "2026-07-12T00:01:00Z",
	}
	old, err := st.UpsertThreadObservation(ctx, thread, UpsertThreadOptions{
		ObservationSequence: 2,
	})
	if err != nil {
		t.Fatalf("old thread observation: %v", err)
	}
	thread.ID = old.ID
	if _, err := st.UpsertThreadRevisionAndFingerprint(ctx, ThreadEvidence{
		Thread: thread, ObservationSequence: 2,
	}, "2026-07-12T00:01:01Z"); err != nil {
		t.Fatalf("old revision: %v", err)
	}
	thread.UpdatedAtGitHub = "2026-07-12T00:02:00Z"
	thread.UpdatedAt = "2026-07-12T00:02:00Z"
	thread.RawJSON = `{"version":2}`
	thread.ContentHash = "version-2"
	current, err := st.UpsertThreadObservation(ctx, thread, UpsertThreadOptions{
		ObservationSequence: 1,
	})
	if err != nil || !current.EvidenceApplied {
		t.Fatalf("newer-source thread observation = %+v, %v", current, err)
	}
	if _, err := st.UpsertThreadRevisionAndFingerprint(ctx, ThreadEvidence{
		Thread: thread, ObservationSequence: 1,
	}, "2026-07-12T00:02:01Z"); err != nil {
		t.Fatalf("newer-source revision: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	raw, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open raw store: %v", err)
	}
	if _, err := raw.ExecContext(ctx, `
		update threads
		set evidence_source_updated_at = '2026-07-12T00:02:00Z',
			evidence_observation_sequence = 2
		where id = ?
	`, thread.ID); err != nil {
		_ = raw.Close()
		t.Fatalf("fabricate evidence tuple: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close raw store: %v", err)
	}

	st, err = Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("reopen migrated store: %v", err)
	}
	defer st.Close()
	var source string
	var sequence, matchingRevisions int64
	if err := st.DB().QueryRowContext(ctx, `
		select evidence_source_updated_at, evidence_observation_sequence
		from threads
		where id = ?
	`, thread.ID).Scan(&source, &sequence); err != nil {
		t.Fatalf("read repaired evidence tuple: %v", err)
	}
	if source != "2026-07-12T00:02:00Z" || sequence != 1 {
		t.Fatalf("repaired evidence tuple = %s/%d, want newer source at sequence 1", source, sequence)
	}
	if err := st.DB().QueryRowContext(ctx, `
		select count(*)
		from thread_revisions
		where thread_id = ? and source_updated_at = ? and observation_sequence = ?
	`, thread.ID, source, sequence).Scan(&matchingRevisions); err != nil {
		t.Fatalf("match repaired evidence tuple: %v", err)
	}
	if matchingRevisions != 1 {
		t.Fatalf("repaired evidence tuple matched %d revisions, want exactly one", matchingRevisions)
	}
}

func TestMigrationBackfillsV9CompleteEvidenceFamilies(t *testing.T) {
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
	upsert, err := st.UpsertThreadObservation(ctx, Thread{
		RepoID: repoID, GitHubID: "104", Number: 104, Kind: "pull_request", State: "open",
		Title: "v9 evidence", HTMLURL: "https://github.com/openclaw/gitcrawl/pull/104",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "v9-evidence",
		UpdatedAtGitHub: "2026-07-12T00:00:00Z",
		UpdatedAt:       "2026-07-12T00:00:00Z",
	}, UpsertThreadOptions{ObservationSequence: 7})
	if err != nil || !upsert.EvidenceApplied {
		t.Fatalf("thread observation = %+v, %v", upsert, err)
	}
	if err := st.UpsertPullRequestCacheFamilies(
		ctx,
		PullRequestDetail{
			ThreadID: upsert.ID, RepoID: repoID, Number: 104, HeadSHA: "v9-head",
			RawJSON: "{}", FetchedAt: "2026-07-12T00:00:00Z", UpdatedAt: "2026-07-12T00:00:00Z",
		},
		nil,
		nil,
		nil,
		nil,
		PullRequestHydrationFamilies{Details: true},
	); err != nil {
		t.Fatalf("pull request detail: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	raw, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open raw store: %v", err)
	}
	if _, err := raw.ExecContext(ctx, `
		drop table thread_child_observation_reservations;
		drop table workflow_run_observation_reservations;
		pragma user_version = 9;
	`); err != nil {
		_ = raw.Close()
		t.Fatalf("prepare v9 migration: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close raw store: %v", err)
	}

	st, err = Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("reopen migrated store: %v", err)
	}
	defer st.Close()
	var count, minimum, maximum, workflowSequence int64
	if err := st.DB().QueryRowContext(ctx, `
		select count(*), min(observation_sequence), max(observation_sequence)
		from thread_child_observation_reservations
		where thread_id = ?
	`, upsert.ID).Scan(&count, &minimum, &maximum); err != nil {
		t.Fatalf("read migrated reservations: %v", err)
	}
	if count != 6 || minimum != 7 || maximum != 7 {
		t.Fatalf(
			"migrated reservations = count %d, min %d, max %d; want six families at 7",
			count,
			minimum,
			maximum,
		)
	}
	if err := st.DB().QueryRowContext(ctx, `
		select observation_sequence
		from workflow_run_observation_reservations
		where repo_id = ? and head_sha = 'v9-head'
	`, repoID).Scan(&workflowSequence); err != nil {
		t.Fatalf("read migrated workflow reservation: %v", err)
	}
	if workflowSequence != 7 {
		t.Fatalf("migrated workflow reservation = %d, want 7", workflowSequence)
	}
}

func TestMigrationBackfillsLegacyV10WorkflowRunReservation(t *testing.T) {
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
	upsert, err := st.UpsertThreadObservation(ctx, Thread{
		RepoID: repoID, GitHubID: "105", Number: 105, Kind: "pull_request", State: "open",
		Title: "v10 workflow fence", HTMLURL: "https://github.com/openclaw/gitcrawl/pull/105",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "v10-workflow",
		UpdatedAtGitHub: "2026-07-12T00:00:00Z",
		UpdatedAt:       "2026-07-12T00:00:00Z",
	}, UpsertThreadOptions{ObservationSequence: 7})
	if err != nil {
		t.Fatalf("thread observation: %v", err)
	}
	if err := st.UpsertPullRequestCacheFamilies(
		ctx,
		PullRequestDetail{
			ThreadID: upsert.ID, RepoID: repoID, Number: 105, HeadSHA: "shared-v10-head",
			RawJSON: "{}", FetchedAt: "2026-07-12T00:00:00Z", UpdatedAt: "2026-07-12T00:00:00Z",
		},
		nil,
		nil,
		nil,
		nil,
		PullRequestHydrationFamilies{Details: true},
	); err != nil {
		t.Fatalf("pull request detail: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	raw, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open raw store: %v", err)
	}
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
		values
			(?, 'workflow_runs', 10),
			(?, 'pull_request_files', 10);
		drop table workflow_run_observation_reservations;
		pragma user_version = 10;
	`, upsert.ID, upsert.ID); err != nil {
		_ = raw.Close()
		t.Fatalf("prepare legacy v10 migration: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close raw store: %v", err)
	}

	st, err = Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("reopen migrated store: %v", err)
	}
	defer st.Close()
	var workflowSequence int64
	if err := st.DB().QueryRowContext(ctx, `
		select observation_sequence
		from workflow_run_observation_reservations
		where repo_id = ? and head_sha = 'shared-v10-head'
	`, repoID).Scan(&workflowSequence); err != nil {
		t.Fatalf("read migrated workflow reservation: %v", err)
	}
	if workflowSequence != 10 {
		t.Fatalf("migrated workflow reservation = %d, want legacy high-water mark 10", workflowSequence)
	}
	var childSource string
	var childSequence int64
	if err := st.DB().QueryRowContext(ctx, `
		select source_updated_at, observation_sequence
		from thread_child_observation_reservations
		where thread_id = ? and family = 'pull_request_files'
	`, upsert.ID).Scan(&childSource, &childSequence); err != nil {
		t.Fatalf("read migrated child reservation: %v", err)
	}
	if childSource != "" || childSequence != 10 {
		t.Fatalf(
			"migrated child reservation = %q/%d, want unknown source at 10",
			childSource,
			childSequence,
		)
	}
	if applied, err := st.ReserveThreadChildObservation(
		ctx,
		upsert.ID,
		ThreadChildPullRequestFiles,
		"2026-07-12T00:01:00Z",
		9,
	); err != nil || applied {
		t.Fatalf("lower migrated child observation = %t, %v", applied, err)
	}
	if applied, err := st.ReserveThreadChildObservation(
		ctx,
		upsert.ID,
		ThreadChildPullRequestFiles,
		"2026-07-12T00:01:00Z",
		11,
	); err != nil || !applied {
		t.Fatalf("newer migrated child observation = %t, %v", applied, err)
	}
	var legacyReservations int64
	if err := st.DB().QueryRowContext(ctx, `
		select count(*)
		from thread_child_observation_reservations
		where family = 'workflow_runs'
	`).Scan(&legacyReservations); err != nil {
		t.Fatalf("read legacy workflow reservations: %v", err)
	}
	if legacyReservations != 0 {
		t.Fatalf("legacy workflow reservations = %d, want migrated rows removed", legacyReservations)
	}
}

func TestInspectSchemaReportsCurrentStoreWithoutMutation(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "gitcrawl.db")
	st, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	before, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("read db before: %v", err)
	}

	diag := InspectSchema(ctx, dbPath)
	if diag.State != "current" || !diag.Current || diag.PendingMigration || diag.Newer {
		t.Fatalf("schema diag = %#v, want current", diag)
	}
	if diag.CurrentVersion != schemaVersion || diag.SupportedVersion != schemaVersion {
		t.Fatalf("schema versions = current %d supported %d, want %d", diag.CurrentVersion, diag.SupportedVersion, schemaVersion)
	}
	if diag.PRDetails.State != "supported" || !diag.PRDetails.DuplicatePathFilesSupported {
		t.Fatalf("pr detail diag = %#v, want supported", diag.PRDetails)
	}
	if !diag.ChildReservations || !diag.WorkflowRunReservations {
		t.Fatalf("reservation diagnostics = %#v, want supported", diag)
	}
	after, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("read db after: %v", err)
	}
	if !bytes.Equal(after, before) {
		t.Fatal("schema diagnostics mutated current database bytes")
	}
}

func TestInspectSchemaReportsMissingStore(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "missing.db")

	diag := InspectSchema(ctx, dbPath)
	if diag.State != "missing" || diag.Exists || diag.SupportedVersion != schemaVersion {
		t.Fatalf("schema diag = %#v, want missing", diag)
	}
	if len(diag.NextSteps) == 0 {
		t.Fatalf("missing db next steps are empty: %#v", diag)
	}
	if _, err := os.Stat(dbPath); !os.IsNotExist(err) {
		t.Fatalf("schema diagnostics created missing db: %v", err)
	}
}

func TestInspectSchemaReportsEmptyPath(t *testing.T) {
	diag := InspectSchema(context.Background(), "")
	if diag.State != "missing" || diag.Exists || len(diag.NextSteps) == 0 {
		t.Fatalf("schema diag = %#v, want missing path guidance", diag)
	}
}

func TestInspectSchemaReportsEmptyDatabaseMigration(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "empty.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open empty db: %v", err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		t.Fatalf("ping empty db: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close empty db: %v", err)
	}

	diag := InspectSchema(ctx, dbPath)
	if diag.State != "pending_migration" || diag.CurrentVersion != 0 || !containsString(diag.PendingMigrations, "schema_version_0_to_10") {
		t.Fatalf("schema diag = %#v, want empty database migration", diag)
	}
}

func TestInspectSchemaReportsInvalidDatabasePath(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "database-directory")
	if err := os.Mkdir(dbPath, 0o755); err != nil {
		t.Fatalf("create database directory: %v", err)
	}

	diag := InspectSchema(context.Background(), dbPath)
	if diag.State != "error" || !diag.Exists || diag.Error == "" || len(diag.NextSteps) == 0 {
		t.Fatalf("schema diag = %#v, want invalid database path error", diag)
	}
}

func TestInspectSchemaReportsCurrentVersionCompatibilityDriftWithoutMutation(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "gitcrawl.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open seed db: %v", err)
	}
	_, err = db.ExecContext(ctx, `
		create table repositories (id integer primary key);
		create table threads (id integer primary key);
		create table thread_vectors (
			thread_id integer primary key,
			basis text,
			model text
		);
		create table pull_request_details (thread_id integer primary key);
		pragma user_version = 10;
	`)
	if err != nil {
		_ = db.Close()
		t.Fatalf("seed compatibility drift: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close seed db: %v", err)
	}
	before, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("read db before: %v", err)
	}

	diag := InspectSchema(ctx, dbPath)
	if diag.State != "pending_migration" || !diag.PendingMigration || !diag.Legacy || diag.Current || diag.Newer {
		t.Fatalf("schema diag = %#v, want current-version compatibility drift", diag)
	}
	if diag.CurrentVersion != schemaVersion || diag.PRDetails.State != "partial" {
		t.Fatalf("schema version/details = %d/%#v, want %d/partial", diag.CurrentVersion, diag.PRDetails, schemaVersion)
	}
	for _, want := range []string{
		"repositories_raw_json_column",
		"threads_body_column",
		"threads_raw_json_column",
		"threads_author_association_column",
		"threads_observation_sequence",
		"threads_evidence_observation_sequence",
		"thread_child_observation_reservations_table",
		"workflow_run_observation_reservations_table",
		"thread_vectors_composite_key",
		"pull_request_files_table",
	} {
		if !containsString(diag.PendingMigrations, want) {
			t.Fatalf("pending migrations = %#v, missing %q", diag.PendingMigrations, want)
		}
	}
	after, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("read db after: %v", err)
	}
	if !bytes.Equal(after, before) {
		t.Fatal("schema diagnostics mutated compatibility-drift database bytes")
	}
}

func TestInspectSchemaReportsNewerStoreWithoutMutation(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "gitcrawl.db")
	st, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `pragma user_version = 99`); err != nil {
		_ = st.Close()
		t.Fatalf("set newer schema: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	before, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("read db before: %v", err)
	}

	diag := InspectSchema(ctx, dbPath)
	if diag.State != "newer" || !diag.Newer || diag.PendingMigration {
		t.Fatalf("schema diag = %#v, want newer", diag)
	}
	if diag.CurrentVersion != 99 || diag.SupportedVersion != schemaVersion {
		t.Fatalf("schema versions = current %d supported %d", diag.CurrentVersion, diag.SupportedVersion)
	}
	if len(diag.NextSteps) == 0 {
		t.Fatalf("newer db next steps are empty: %#v", diag)
	}
	after, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("read db after: %v", err)
	}
	if !bytes.Equal(after, before) {
		t.Fatal("schema diagnostics mutated newer database bytes")
	}
}

func TestInspectSchemaReportsLegacyPendingMigrationWithoutMutation(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "gitcrawl.db")
	st, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	seedLegacyPullRequestFilesSchema(t, ctx, dbPath)
	before, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("read db before: %v", err)
	}

	diag := InspectSchema(ctx, dbPath)
	if diag.State != "pending_migration" || !diag.PendingMigration || !diag.Legacy || diag.Newer {
		t.Fatalf("schema diag = %#v, want pending migration", diag)
	}
	if diag.CurrentVersion != 3 || diag.SupportedVersion != schemaVersion {
		t.Fatalf("schema versions = current %d supported %d", diag.CurrentVersion, diag.SupportedVersion)
	}
	if diag.PRDetails.State != "legacy" || diag.PRDetails.FilesPositionKey || diag.PRDetails.DuplicatePathFilesSupported {
		t.Fatalf("pr detail diag = %#v, want legacy", diag.PRDetails)
	}
	if !containsString(diag.PendingMigrations, "schema_version_3_to_10") ||
		!containsString(diag.PendingMigrations, "pull_request_files_position_key") ||
		!containsString(diag.PendingMigrations, "thread_child_observation_reservations_table") ||
		!containsString(diag.PendingMigrations, "workflow_run_observation_reservations_table") {
		t.Fatalf("pending migrations = %#v", diag.PendingMigrations)
	}
	if len(diag.NextSteps) == 0 {
		t.Fatalf("pending db next steps are empty: %#v", diag)
	}
	after, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("read db after: %v", err)
	}
	if !bytes.Equal(after, before) {
		t.Fatal("schema diagnostics mutated legacy database bytes")
	}

	raw, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open raw db: %v", err)
	}
	defer raw.Close()
	var version int
	if err := raw.QueryRowContext(ctx, `pragma user_version`).Scan(&version); err != nil {
		t.Fatalf("read user_version: %v", err)
	}
	if version != 3 {
		t.Fatalf("user_version = %d, want 3", version)
	}
}

func TestStatusOnEmptyStore(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	status, err := st.Status(ctx)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.RepositoryCount != 0 || status.ThreadCount != 0 || status.ClusterCount != 0 {
		t.Fatalf("expected empty status, got %#v", status)
	}
}

func TestOpenReadOnlyDoesNotMutateStore(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "gitcrawl.db")
	st, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if _, err := st.UpsertRepository(ctx, Repository{
		Owner:     "openclaw",
		Name:      "openclaw",
		FullName:  "openclaw/openclaw",
		RawJSON:   "{}",
		UpdatedAt: "2026-04-27T00:00:00Z",
	}); err != nil {
		t.Fatalf("seed repository: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	before, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("read db before: %v", err)
	}

	readOnly, err := OpenReadOnly(ctx, dbPath)
	if err != nil {
		t.Fatalf("open readonly: %v", err)
	}
	status, err := readOnly.Status(ctx)
	if err != nil {
		t.Fatalf("readonly status: %v", err)
	}
	if status.RepositoryCount != 1 {
		t.Fatalf("repository count: got %d want 1", status.RepositoryCount)
	}
	if err := readOnly.Close(); err != nil {
		t.Fatalf("close readonly: %v", err)
	}
	after, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("read db after: %v", err)
	}
	if !bytes.Equal(after, before) {
		t.Fatal("readonly open mutated database bytes")
	}
}

func seedLegacyPullRequestFilesSchema(t *testing.T, ctx context.Context, dbPath string) {
	t.Helper()
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open raw db: %v", err)
	}
	defer db.Close()
	_, err = db.ExecContext(ctx, `
		drop index if exists idx_pull_request_files_path;
		drop index if exists idx_pull_request_files_thread_path;
		alter table pull_request_files rename to pull_request_files_current;
		create table pull_request_files (
			thread_id integer not null references threads(id) on delete cascade,
			path text not null,
			status text,
			additions integer not null default 0,
			deletions integer not null default 0,
			changes integer not null default 0,
			previous_path text,
			patch text,
			raw_json text not null,
			fetched_at text not null,
			primary key(thread_id, path)
		);
		insert into pull_request_files(thread_id, path, status, additions, deletions, changes, previous_path, patch, raw_json, fetched_at)
			select thread_id, path, status, additions, deletions, changes, previous_path, patch, raw_json, fetched_at
			from pull_request_files_current;
		drop table pull_request_files_current;
		create index if not exists idx_pull_request_files_path on pull_request_files(path);
		drop table thread_child_observation_reservations;
		drop table workflow_run_observation_reservations;
		pragma user_version = 3;
	`)
	if err != nil {
		t.Fatalf("seed legacy pull_request_files table: %v", err)
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func TestOpenReadOnlySupportsCanonicalPortableStore(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "portable.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open seed db: %v", err)
	}
	_, err = db.ExecContext(ctx, `
		create table repositories (
			id integer primary key,
			owner text not null,
			name text not null,
			full_name text not null,
			github_repo_id text,
			updated_at text not null
		);
		create table threads (
			id integer primary key,
			repo_id integer not null,
			github_id text not null,
			number integer not null,
			kind text not null,
			state text not null,
			title text not null,
			body_excerpt text,
			body_length integer not null default 0,
			author_login text,
			author_type text,
			html_url text not null,
			labels_json text not null,
			assignees_json text not null,
			content_hash text not null,
			is_draft integer not null default 0,
			created_at_gh text,
			updated_at_gh text,
			closed_at_gh text,
			merged_at_gh text,
			first_pulled_at text,
			last_pulled_at text,
			updated_at text not null,
			closed_at_local text,
			close_reason_local text
		);
		create table repo_sync_state (
			repo_id integer primary key,
			last_full_open_scan_started_at text,
			last_overlapping_open_scan_completed_at text,
			last_non_overlapping_scan_completed_at text,
			last_open_close_reconciled_at text,
			updated_at text not null
		);
		create table cluster_groups (
			id integer primary key,
			repo_id integer not null,
			stable_key text not null,
			stable_slug text not null,
			status text not null,
			cluster_type text not null,
			representative_thread_id integer,
			title text,
			created_at text not null,
			updated_at text not null,
			closed_at text
		);
		insert into repositories(id, owner, name, full_name, updated_at)
		values(1, 'openclaw', 'openclaw', 'openclaw/openclaw', '2026-04-28T00:00:00Z');
		insert into threads(id, repo_id, github_id, number, kind, state, title, body_excerpt, html_url, labels_json, assignees_json, content_hash, updated_at)
		values(1, 1, '1', 42, 'issue', 'open', 'portable issue', 'portable body', 'https://github.com/openclaw/openclaw/issues/42', '[]', '[]', 'hash', '2026-04-28T00:00:00Z');
		insert into repo_sync_state(repo_id, last_open_close_reconciled_at, updated_at)
		values(1, '2026-04-28T01:02:03Z', '2026-04-28T01:02:03Z');
		insert into cluster_groups(id, repo_id, stable_key, stable_slug, status, cluster_type, representative_thread_id, title, created_at, updated_at)
		values(1, 1, 'stable', 'stable', 'active', 'similarity', 1, 'portable cluster', '2026-04-28T00:00:00Z', '2026-04-28T00:00:00Z');
	`)
	if err != nil {
		t.Fatalf("seed portable db: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close seed db: %v", err)
	}
	before, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("read db before: %v", err)
	}

	st, err := OpenReadOnly(ctx, dbPath)
	if err != nil {
		t.Fatalf("open readonly portable: %v", err)
	}
	defer st.Close()

	status, err := st.Status(ctx)
	if err != nil {
		t.Fatalf("portable status: %v", err)
	}
	if status.RepositoryCount != 1 || status.ThreadCount != 1 || status.OpenThreadCount != 1 || status.ClusterCount != 1 {
		t.Fatalf("unexpected portable status: %#v", status)
	}
	if status.LastSyncAt.IsZero() {
		t.Fatalf("portable last sync was not read from repo_sync_state: %#v", status)
	}
	repo, err := st.RepositoryByFullName(ctx, "openclaw/openclaw")
	if err != nil {
		t.Fatalf("portable repository: %v", err)
	}
	if repo.RawJSON != "" {
		t.Fatalf("portable raw json: got %q want empty", repo.RawJSON)
	}
	threads, err := st.ListThreadsFiltered(ctx, ThreadListOptions{RepoID: repo.ID, Numbers: []int{42}})
	if err != nil {
		t.Fatalf("portable threads: %v", err)
	}
	if len(threads) != 1 || threads[0].Body != "portable body" || threads[0].RawJSON != "" {
		t.Fatalf("unexpected portable thread: %#v", threads)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close portable readonly: %v", err)
	}
	after, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("read db after: %v", err)
	}
	if !bytes.Equal(after, before) {
		t.Fatal("readonly portable open mutated database bytes")
	}
}

func TestStatusPrefersPortableExportedAt(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "portable.sync.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open seed db: %v", err)
	}
	_, err = db.ExecContext(ctx, `
		create table repositories (
			id integer primary key,
			owner text not null,
			name text not null,
			full_name text not null,
			github_repo_id text,
			updated_at text not null
		);
		create table threads (
			id integer primary key,
			repo_id integer not null,
			github_id text not null,
			number integer not null,
			kind text not null,
			state text not null,
			title text not null,
			body_excerpt text,
			body_length integer not null default 0,
			author_login text,
			author_type text,
			html_url text not null,
			labels_json text not null,
			assignees_json text not null,
			content_hash text not null,
			is_draft integer not null default 0,
			created_at_gh text,
			updated_at_gh text,
			closed_at_gh text,
			merged_at_gh text,
			first_pulled_at text,
			last_pulled_at text,
			updated_at text not null,
			closed_at_local text,
			close_reason_local text
		);
		create table repo_sync_state (
			repo_id integer primary key,
			last_full_open_scan_started_at text,
			last_overlapping_open_scan_completed_at text,
			last_non_overlapping_scan_completed_at text,
			last_open_close_reconciled_at text,
			updated_at text not null
		);
		create table cluster_groups (
			id integer primary key,
			repo_id integer not null,
			stable_key text not null,
			stable_slug text not null,
			status text not null,
			cluster_type text not null,
			representative_thread_id integer,
			title text,
			created_at text not null,
			updated_at text not null,
			closed_at text
		);
		create table portable_metadata (
			key text primary key,
			value text not null
		);
		insert into repositories(id, owner, name, full_name, updated_at)
		values(1, 'openclaw', 'openclaw', 'openclaw/openclaw', '2026-04-28T00:00:00Z');
		insert into repo_sync_state(repo_id, last_open_close_reconciled_at, updated_at)
		values(1, '2026-04-28T01:02:03Z', '2026-04-28T01:02:03Z');
		insert into portable_metadata(key, value)
		values('exported_at', '2026-04-30T01:11:27.830908426Z');
	`)
	if err != nil {
		t.Fatalf("seed portable db: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close seed db: %v", err)
	}

	st, err := OpenReadOnly(ctx, dbPath)
	if err != nil {
		t.Fatalf("open readonly portable: %v", err)
	}
	defer st.Close()
	status, err := st.Status(ctx)
	if err != nil {
		t.Fatalf("portable status: %v", err)
	}
	want := "2026-04-30T01:11:27.830908426Z"
	if got := status.LastSyncAt.Format(time.RFC3339Nano); got != want {
		t.Fatalf("last sync = %q, want portable exported_at %q", got, want)
	}
}

func TestOpenMigratesPortableStoreColumns(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "portable.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open seed db: %v", err)
	}
	_, err = db.ExecContext(ctx, `
		create table repositories (
			id integer primary key,
			owner text not null,
			name text not null,
			full_name text not null,
			github_repo_id text,
			updated_at text not null
		);
		create table threads (
			id integer primary key,
			repo_id integer not null,
			github_id text not null,
			number integer not null,
			kind text not null,
			state text not null,
			title text not null,
			body_excerpt text,
			body_length integer not null default 0,
			author_login text,
			author_type text,
			html_url text not null,
			labels_json text not null,
			assignees_json text not null,
			content_hash text not null,
			is_draft integer not null default 0,
			created_at_gh text,
			updated_at_gh text,
			closed_at_gh text,
			merged_at_gh text,
			first_pulled_at text,
			last_pulled_at text,
			updated_at text not null,
			closed_at_local text,
			close_reason_local text
		);
		insert into repositories(id, owner, name, full_name, updated_at)
		values(1, 'openclaw', 'openclaw', 'openclaw/openclaw', '2026-04-26T00:00:00Z');
		insert into threads(id, repo_id, github_id, number, kind, state, title, body_excerpt, body_length, html_url, labels_json, assignees_json, content_hash, updated_at)
		values(1, 1, '1', 42, 'issue', 'open', 'portable issue', 'portable body', 26, 'https://github.com/openclaw/openclaw/issues/42', '[]', '[]', 'hash', '2026-04-26T00:00:00Z');
	`)
	if err != nil {
		t.Fatalf("seed portable db: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close seed db: %v", err)
	}

	st, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	repo, err := st.RepositoryByFullName(ctx, "openclaw/openclaw")
	if err != nil {
		_ = st.Close()
		t.Fatalf("repository: %v", err)
	}
	threads, err := st.ListThreadsFiltered(ctx, ThreadListOptions{RepoID: repo.ID, Numbers: []int{42}})
	if err != nil {
		_ = st.Close()
		t.Fatalf("threads: %v", err)
	}
	if len(threads) != 1 || threads[0].Body != "portable body" {
		_ = st.Close()
		t.Fatalf("unexpected portable thread: %#v", threads)
	}
	assertPortableBodyMetadata := func(st *Store) {
		t.Helper()
		var body, excerpt string
		var bodyLength int
		if err := st.DB().QueryRowContext(ctx, `
			select body, body_excerpt, body_length
			from threads
			where id = 1
		`).Scan(&body, &excerpt, &bodyLength); err != nil {
			t.Fatalf("read portable truncation metadata: %v", err)
		}
		if body != "portable body" || excerpt != "portable body" || bodyLength != 26 {
			t.Fatalf(
				"portable truncation metadata = body %q excerpt %q length %d",
				body,
				excerpt,
				bodyLength,
			)
		}
	}
	assertPortableBodyMetadata(st)
	if err := st.Close(); err != nil {
		t.Fatalf("close migrated portable store: %v", err)
	}
	st, err = Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("reopen migrated portable store: %v", err)
	}
	defer st.Close()
	assertPortableBodyMetadata(st)
}

func TestDocumentsFTSWorks(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	_, err = st.DB().ExecContext(ctx, `
		insert into repositories(owner, name, full_name, raw_json, updated_at)
		values('openclaw', 'gitcrawl', 'openclaw/gitcrawl', '{}', '2026-04-26T00:00:00Z');
		insert into threads(repo_id, github_id, number, kind, state, title, body, html_url, labels_json, assignees_json, raw_json, content_hash, updated_at)
		values(1, '1', 1, 'issue', 'open', 'download stalls', 'body', 'https://github.com/openclaw/gitcrawl/issues/1', '[]', '[]', '{}', 'hash', '2026-04-26T00:00:00Z');
		insert into documents(thread_id, title, body, raw_text, dedupe_text, updated_at)
		values(1, 'download stalls', 'body', 'download stalls body', 'download stalls', '2026-04-26T00:00:00Z');
	`)
	if err != nil {
		t.Fatalf("seed documents: %v", err)
	}

	var count int
	if err := st.DB().QueryRowContext(ctx, `select count(*) from documents_fts where documents_fts match 'download'`).Scan(&count); err != nil {
		t.Fatalf("query fts: %v", err)
	}
	if count != 1 {
		t.Fatalf("fts count: got %d want 1", count)
	}
}

func TestSearchFallsBackToThreadPayloadsWhenDocumentsArePruned(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	_, err = st.DB().ExecContext(ctx, `
		insert into repositories(id, owner, name, full_name, raw_json, updated_at)
		values(1, 'openclaw', 'openclaw', 'openclaw/openclaw', '{}', '2026-04-26T00:00:00Z');
		insert into threads(repo_id, github_id, number, kind, state, title, body, html_url, labels_json, assignees_json, raw_json, content_hash, updated_at)
		values(1, '1', 73038, 'pull_request', 'open', 'feat(providers): add DeepInfra provider plugin', 'DeepInfra provider plugin', 'https://github.com/openclaw/openclaw/pull/73038', '[]', '[]', '{}', 'hash', '2026-04-27T00:00:00Z');
	`)
	if err != nil {
		t.Fatalf("seed threads: %v", err)
	}

	hits, err := st.SearchDocuments(ctx, 1, "DeepInfra", 10)
	if err != nil {
		t.Fatalf("search documents: %v", err)
	}
	if len(hits) != 1 || hits[0].Number != 73038 {
		t.Fatalf("hits = %#v, want PR 73038", hits)
	}
}

func TestPrunePortablePayloads(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	_, err = st.DB().ExecContext(ctx, `
		insert into repositories(id, owner, name, full_name, raw_json, updated_at)
		values(1, 'openclaw', 'gitcrawl', 'openclaw/gitcrawl', '{"id":1}', '2026-04-26T00:00:00Z');
		insert into threads(id, repo_id, github_id, number, kind, state, title, body, author_association, html_url, labels_json, assignees_json, raw_json, content_hash, updated_at)
		values(1, 1, '1', 1, 'pull_request', 'open', 'download stalls', 'abcdefghijklmnopqrstuvwxyz', 'MEMBER', 'https://github.com/openclaw/gitcrawl/pull/1', '[{"name":"bug","color":"d73a4a","url":"https://api.github.com/labels/bug"}]', '[{"login":"alice","avatar_url":"https://avatars.githubusercontent.com/u/1"}]', '{"body":"abcdefghijklmnopqrstuvwxyz"}', 'hash', '2026-04-26T00:00:00Z');
		insert into comments(id, thread_id, github_id, comment_type, author_login, author_type, body, is_bot, raw_json, created_at_gh, updated_at_gh)
		values(1, 1, 'c1', 'issue_comment', 'alice', 'User', 'comment abcdefghijklmnopqrstuvwxyz', 0, '{"body":"comment abcdefghijklmnopqrstuvwxyz"}', '2026-04-26T00:00:00Z', '2026-04-26T00:00:00Z');
		insert into pull_request_details(thread_id, repo_id, number, base_sha, head_sha, additions, deletions, changed_files, raw_json, fetched_at, updated_at)
		values(1, 1, 1, 'base', 'head', 10, 2, 1, '{"mergeable":true}', '2026-04-26T00:00:00Z', '2026-04-26T00:00:00Z');
		insert into pull_request_files(thread_id, path, status, additions, deletions, changes, patch, raw_json, fetched_at)
		values(1, 'README.md', 'modified', 10, 2, 12, '@@ patch', '{"filename":"README.md"}', '2026-04-26T00:00:00Z');
		insert into pull_request_commits(thread_id, sha, message, raw_json, fetched_at)
		values(1, 'abc123', 'fix download stall', '{"sha":"abc123"}', '2026-04-26T00:00:00Z');
		insert into pull_request_checks(thread_id, name, status, conclusion, details_url, raw_json, fetched_at)
		values(1, 'CI', 'completed', 'success', 'https://example.test/check', '{"name":"CI"}', '2026-04-26T00:00:00Z');
		insert into github_workflow_runs(repo_id, run_id, run_number, head_branch, head_sha, status, conclusion, workflow_name, html_url, raw_json, fetched_at)
		values(1, '99', 99, 'main', 'head', 'completed', 'success', 'CI', 'https://example.test/run', '{"id":99}', '2026-04-26T00:00:00Z');
		insert into documents(thread_id, title, body, raw_text, dedupe_text, updated_at)
		values(1, 'download stalls', 'abcdefghijklmnopqrstuvwxyz', 'download stalls abcdefghijklmnopqrstuvwxyz', 'download stalls', '2026-04-26T00:00:00Z');
		insert into thread_revisions(thread_id, source_updated_at, content_hash, title_hash, body_hash, labels_hash, created_at)
		values(1, '2026-04-26T00:00:00Z', 'hash', 'title-hash', 'body-hash', 'labels-hash', '2026-04-26T00:00:00Z');
		insert into thread_fingerprints(thread_revision_id, algorithm_version, fingerprint_hash, fingerprint_slug, title_tokens_json, body_token_hash, linked_refs_json, file_set_hash, module_buckets_json, simhash64, feature_json, created_at)
		values(1, 'v1', 'fp-hash', 'fp-slug', '["download","stalls"]', 'body-token-hash', '["#1"]', 'files', '["runtime"]', '123', '{"tokens":["download"]}', '2026-04-26T00:00:00Z');
	`)
	if err != nil {
		t.Fatalf("seed prune data: %v", err)
	}

	stats, err := st.PrunePortablePayloads(ctx, PortablePruneOptions{BodyChars: 8})
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if stats.DocumentsDeleted != 1 || stats.FingerprintsPruned != 1 || stats.CommentsPruned != 1 || stats.RawJSONPruned == 0 || stats.ThreadLabelsCompacted != 1 || stats.ThreadAssigneesCompacted != 1 {
		t.Fatalf("unexpected stats: %#v", stats)
	}

	var bodyExcerpt, labelsJSON, assigneesJSON, titleTokens, linkedRefs, buckets, features string
	if st.hasColumn(ctx, "repositories", "raw_json") {
		t.Fatal("repositories.raw_json was not dropped")
	}
	if st.hasColumn(ctx, "threads", "raw_json") {
		t.Fatal("threads.raw_json was not dropped")
	}
	if st.hasColumn(ctx, "threads", "body") {
		t.Fatal("threads.body was not dropped")
	}
	if err := st.DB().QueryRowContext(ctx, `select body_excerpt from threads where id = 1`).Scan(&bodyExcerpt); err != nil {
		t.Fatalf("thread body excerpt: %v", err)
	}
	if err := st.DB().QueryRowContext(ctx, `select labels_json, assignees_json from threads where id = 1`).Scan(&labelsJSON, &assigneesJSON); err != nil {
		t.Fatalf("thread metadata: %v", err)
	}
	if labelsJSON != `["bug"]` || assigneesJSON != `["alice"]` {
		t.Fatalf("thread metadata not compacted: labels=%q assignees=%q", labelsJSON, assigneesJSON)
	}
	var bodyLength int
	if err := st.DB().QueryRowContext(ctx, `select body_length from threads where id = 1`).Scan(&bodyLength); err != nil {
		t.Fatalf("thread body length: %v", err)
	}
	if bodyLength != 26 {
		t.Fatalf("thread body_length = %d, want 26", bodyLength)
	}
	if err := st.DB().QueryRowContext(ctx, `select title_tokens_json, linked_refs_json, module_buckets_json, feature_json from thread_fingerprints where id = 1`).Scan(&titleTokens, &linkedRefs, &buckets, &features); err != nil {
		t.Fatalf("fingerprint payload: %v", err)
	}
	if st.tableExists(ctx, "documents") {
		t.Fatal("documents table was not dropped")
	}
	if !st.tableExists(ctx, "comments") {
		t.Fatal("comments table was dropped")
	}
	var commentBody, commentExcerpt, commentRawJSON string
	var commentBodyLength int
	if err := st.DB().QueryRowContext(ctx, `select body, body_excerpt, body_length, raw_json from comments where id = 1`).Scan(&commentBody, &commentExcerpt, &commentBodyLength, &commentRawJSON); err != nil {
		t.Fatalf("comment portable payload: %v", err)
	}
	if commentBody != "comment " || commentExcerpt != "comment " || commentBodyLength != 34 || commentRawJSON != "" {
		t.Fatalf("comment not pruned: body=%q excerpt=%q length=%d raw=%q", commentBody, commentExcerpt, commentBodyLength, commentRawJSON)
	}
	var prDetailCount, prFileCount, prCommitCount, prCheckCount, runCount int
	if err := st.DB().QueryRowContext(ctx, `select count(*) from pull_request_details where raw_json = ''`).Scan(&prDetailCount); err != nil {
		t.Fatalf("pr detail count: %v", err)
	}
	if err := st.DB().QueryRowContext(ctx, `select count(*) from pull_request_files where raw_json = ''`).Scan(&prFileCount); err != nil {
		t.Fatalf("pr file count: %v", err)
	}
	if err := st.DB().QueryRowContext(ctx, `select count(*) from pull_request_commits where raw_json = ''`).Scan(&prCommitCount); err != nil {
		t.Fatalf("pr commit count: %v", err)
	}
	if err := st.DB().QueryRowContext(ctx, `select count(*) from pull_request_checks where raw_json = ''`).Scan(&prCheckCount); err != nil {
		t.Fatalf("pr check count: %v", err)
	}
	if err := st.DB().QueryRowContext(ctx, `select count(*) from github_workflow_runs where raw_json = ''`).Scan(&runCount); err != nil {
		t.Fatalf("workflow run count: %v", err)
	}
	if prDetailCount != 1 || prFileCount != 1 || prCommitCount != 1 || prCheckCount != 1 || runCount != 1 {
		t.Fatalf("pr/run rows not retained: detail=%d files=%d commits=%d checks=%d runs=%d", prDetailCount, prFileCount, prCommitCount, prCheckCount, runCount)
	}
	var portableSchema, capabilities, authorProfile, authorAssociation string
	if err := st.DB().QueryRowContext(ctx, `select value from portable_metadata where key = 'schema'`).Scan(&portableSchema); err != nil {
		t.Fatalf("portable schema metadata: %v", err)
	}
	if err := st.DB().QueryRowContext(ctx, `select value from portable_metadata where key = 'capabilities'`).Scan(&capabilities); err != nil {
		t.Fatalf("portable capabilities metadata: %v", err)
	}
	if err := st.DB().QueryRowContext(ctx, `select value from portable_metadata where key = 'thread_author_profile'`).Scan(&authorProfile); err != nil {
		t.Fatalf("portable author profile metadata: %v", err)
	}
	if err := st.DB().QueryRowContext(ctx, `select author_association from threads where id = 1`).Scan(&authorAssociation); err != nil {
		t.Fatalf("portable author association: %v", err)
	}
	if portableSchema != "gitcrawl-portable-sync-v2" ||
		!strings.Contains(capabilities, "comment_excerpts") ||
		!strings.Contains(capabilities, "workflow_runs") ||
		!strings.Contains(capabilities, "author_association") ||
		!strings.Contains(capabilities, "thread_revisions") ||
		authorProfile != "login,type,association" ||
		authorAssociation != "MEMBER" {
		t.Fatalf("portable metadata schema=%q capabilities=%q author_profile=%q association=%q", portableSchema, capabilities, authorProfile, authorAssociation)
	}
	if bodyExcerpt != "abcdefgh" || titleTokens != "[]" || linkedRefs != "[]" || buckets != "[]" || features != "{}" {
		t.Fatalf("payloads not pruned: bodyExcerpt=%q titleTokens=%q linkedRefs=%q buckets=%q features=%q", bodyExcerpt, titleTokens, linkedRefs, buckets, features)
	}
}
