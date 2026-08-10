package store

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestCanonicalPortableBulkDropOrderMatchesKnownSet(t *testing.T) {
	want := append([]string(nil), canonicalPortableDroppedTables()...)
	got := append([]string(nil), canonicalPortableBulkDropOrder()...)
	slices.Sort(want)
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Fatalf("bulk drop set = %v, want %v", got, want)
	}
	for index := 1; index < len(got); index++ {
		if got[index] == got[index-1] {
			t.Fatalf("bulk drop table %s appears more than once", got[index])
		}
	}
}

func TestPortableBulkDropResolvesCaseVariantTableNames(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "case-variant.db"))
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if _, err := db.ExecContext(ctx, `create table "Documents"(id integer primary key, payload text)`); err != nil {
		t.Fatalf("create case-variant disposable table: %v", err)
	}
	st := &Store{db: db, path: "case-variant.db"}
	stats := &PortablePruneStats{}
	if err := st.dropCanonicalPortableTablesBulk(ctx, false, stats, nil); err != nil {
		t.Fatalf("drop case-variant portable table: %v", err)
	}
	if tableExistsForPortableTest(t, db, "Documents") {
		t.Fatal("case-variant disposable table remains")
	}
	if !slices.Contains(stats.DroppedTables, "Documents") {
		t.Fatalf("case-variant dropped tables = %v", stats.DroppedTables)
	}
}

func TestDropPortableDerivedBlobPointerColumnsRemovesForeignKeys(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "blob-pointers.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	stats := &PortablePruneStats{}
	if err := st.dropPortableDerivedBlobPointerColumns(ctx, stats); err != nil {
		t.Fatalf("drop derived blob pointer columns: %v", err)
	}
	for _, table := range []string{"comments", "thread_revisions"} {
		if st.hasColumn(ctx, table, "raw_json_blob_id") {
			t.Fatalf("derived blob pointer remains on %s", table)
		}
		definitions, err := portableForeignKeysForTable(ctx, st.DB(), table)
		if err != nil {
			t.Fatalf("inspect %s foreign keys: %v", table, err)
		}
		for _, definition := range definitions {
			if strings.EqualFold(definition.referencedTable, "blobs") {
				t.Fatalf("derived %s schema still references blobs: %+v", table, definition)
			}
		}
	}
	want := []string{"comments.raw_json_blob_id", "thread_revisions.raw_json_blob_id"}
	if !slices.Equal(stats.DroppedColumns, want) {
		t.Fatalf("dropped blob pointer columns = %v, want %v", stats.DroppedColumns, want)
	}
}

func TestPortableBulkDropValidatesQuotedRetainedViewName(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "quoted-view.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	if _, err := st.DB().ExecContext(ctx, `create view "report""view" as select id from repositories`); err != nil {
		t.Fatalf("create quoted retained view: %v", err)
	}
	stats := &PortablePruneStats{}
	preparePortableBulkDropTest(t, ctx, st, stats)
	if err := st.dropCanonicalPortableTablesBulk(ctx, false, stats, nil); err != nil {
		t.Fatalf("bulk drop with quoted retained view: %v", err)
	}
}

func TestPortableBulkDropRemovesLargeDisposableGraphAndRestoresForeignKeys(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "bulk-drop.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	if _, err := st.DB().ExecContext(ctx, `
		pragma foreign_keys = on;
		insert into repositories(id, owner, name, full_name, raw_json, updated_at)
		values(1, 'openclaw', 'gitcrawl', 'openclaw/gitcrawl', '{}', '2026-08-09T00:00:00Z');
		insert into code_snapshots(id, repo_id, source_root, git_sha, worktree_dirty, file_count, byte_count, indexed_at)
		values(1, 1, '/source', 'sha', 0, 2000, 4096000, '2026-08-09T00:00:00Z');
		with recursive sequence(value) as (
			select 1
			union all
			select value + 1 from sequence where value < 2000
		)
		insert into code_documents(snapshot_id, repo_id, path, language, content_hash, text_content, byte_size, updated_at)
		select 1, 1, printf('file-%04d.go', value), 'go', printf('hash-%d', value),
		       hex(zeroblob(1024)), 2048, '2026-08-09T00:00:00Z'
		from sequence;
	`); err != nil {
		t.Fatalf("seed large disposable graph: %v", err)
	}
	var before int
	if err := st.DB().QueryRowContext(ctx, `select count(*) from code_documents`).Scan(&before); err != nil || before != 2000 {
		t.Fatalf("seeded code documents=%d err=%v", before, err)
	}
	var expectedDropped []string
	for _, table := range canonicalPortableBulkDropOrder() {
		if tableExistsForPortableTest(t, st.DB(), table) {
			expectedDropped = append(expectedDropped, table)
		}
	}
	stats := &PortablePruneStats{}
	preparePortableBulkDropTest(t, ctx, st, stats)
	if err := st.dropCanonicalPortableTablesBulk(ctx, false, stats, nil); err != nil {
		t.Fatalf("bulk drop portable tables: %v", err)
	}
	if got := portableTestForeignKeys(t, st.DB()); got != 1 {
		t.Fatalf("foreign_keys after bulk drop = %d, want 1", got)
	}
	for _, table := range canonicalPortableBulkDropOrder() {
		if tableExistsForPortableTest(t, st.DB(), table) {
			t.Fatalf("bulk-dropped table %s still exists", table)
		}
	}
	if !slices.Equal(stats.DroppedTables, expectedDropped) {
		t.Fatalf("bulk drop stats = %v, want %v", stats.DroppedTables, expectedDropped)
	}
	seen := make(map[string]bool)
	for _, table := range stats.DroppedTables {
		if seen[table] {
			t.Fatalf("bulk-dropped table %s reported twice: %v", table, stats.DroppedTables)
		}
		seen[table] = true
	}
	for _, table := range []string{"code_documents_fts", "code_documents", "code_snapshots"} {
		if !seen[table] {
			t.Fatalf("bulk drop stats missing %s: %v", table, stats.DroppedTables)
		}
	}
	violations, err := portableForeignKeyViolationCount(ctx, st.DB())
	if err != nil || violations != 0 {
		t.Fatalf("bulk-dropped store FK violations=%d err=%v", violations, err)
	}
}

func TestPortableBulkDropRollsBackAndRestoresForeignKeys(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "bulk-drop-rollback.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	st.DB().SetMaxOpenConns(1)
	st.DB().SetMaxIdleConns(1)
	var journalMode string
	if err := st.DB().QueryRowContext(ctx, `pragma journal_mode = off`).Scan(&journalMode); err != nil || journalMode != "off" {
		t.Fatalf("set rollback fixture journal mode=%q err=%v", journalMode, err)
	}
	if _, err := st.DB().ExecContext(ctx, `
		pragma foreign_keys = on;
		insert into repositories(id, owner, name, full_name, raw_json, updated_at)
		values(1, 'openclaw', 'gitcrawl', 'openclaw/gitcrawl', '{}', '2026-08-09T00:00:00Z');
		insert into code_snapshots(id, repo_id, source_root, git_sha, worktree_dirty, file_count, byte_count, indexed_at)
		values(1, 1, '/source', 'sha', 0, 1, 4, '2026-08-09T00:00:00Z');
		insert into code_documents(id, snapshot_id, repo_id, path, language, content_hash, text_content, byte_size, updated_at)
		values(1, 1, 1, 'file.go', 'go', 'hash', 'body', 4, '2026-08-09T00:00:00Z');
	`); err != nil {
		t.Fatalf("seed rollback graph: %v", err)
	}
	stats := &PortablePruneStats{}
	preparePortableBulkDropTest(t, ctx, st, stats)
	transactionJournalMode := ""
	err = st.dropCanonicalPortableTablesBulk(ctx, false, stats, func(_ context.Context, tx *sql.Tx, table string) error {
		if table == "code_snapshots" {
			if err := tx.QueryRowContext(ctx, `pragma journal_mode`).Scan(&transactionJournalMode); err != nil {
				return err
			}
			return errors.New("injected drop failure")
		}
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "injected drop failure") {
		t.Fatalf("bulk drop rollback error = %v", err)
	}
	if len(stats.DroppedTables) != 0 {
		t.Fatalf("rolled-back drops reported as committed: %v", stats.DroppedTables)
	}
	for _, table := range []string{"code_documents_fts", "code_documents", "code_snapshots"} {
		if !tableExistsForPortableTest(t, st.DB(), table) {
			t.Fatalf("rollback did not restore %s", table)
		}
	}
	var documents int
	if err := st.DB().QueryRowContext(ctx, `select count(*) from code_documents`).Scan(&documents); err != nil || documents != 1 {
		t.Fatalf("code documents after rollback=%d err=%v", documents, err)
	}
	if got := portableTestForeignKeys(t, st.DB()); got != 1 {
		t.Fatalf("foreign_keys after rollback = %d, want 1", got)
	}
	if transactionJournalMode != "memory" {
		t.Fatalf("bulk drop transaction journal_mode = %q, want memory", transactionJournalMode)
	}
	if err := st.DB().QueryRowContext(ctx, `pragma journal_mode`).Scan(&journalMode); err != nil || journalMode != "off" {
		t.Fatalf("journal mode after rollback=%q err=%v", journalMode, err)
	}
}

func TestPortableBulkDropRespectsSyncFailuresAndRestoresDisabledForeignKeys(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "bulk-drop-sync-failures.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	st.DB().SetMaxOpenConns(1)
	st.DB().SetMaxIdleConns(1)
	if _, err := st.DB().ExecContext(ctx, `pragma foreign_keys = off`); err != nil {
		t.Fatalf("disable fixture foreign keys: %v", err)
	}
	stats := &PortablePruneStats{}
	preparePortableBulkDropTest(t, ctx, st, stats)
	if err := st.dropCanonicalPortableTablesBulk(ctx, true, stats, nil); err != nil {
		t.Fatalf("bulk drop retaining sync failures: %v", err)
	}
	if !tableExistsForPortableTest(t, st.DB(), "sync_attempt_failures") {
		t.Fatal("bulk drop removed included sync_attempt_failures")
	}
	if slices.Contains(stats.DroppedTables, "sync_attempt_failures") {
		t.Fatalf("included sync_attempt_failures reported dropped: %v", stats.DroppedTables)
	}
	if got := portableTestForeignKeys(t, st.DB()); got != 0 {
		t.Fatalf("foreign_keys after disabled-state bulk drop = %d, want 0", got)
	}
}

func TestPortableBulkDropRejectsRetainedSchemaDependencies(t *testing.T) {
	for _, test := range []struct {
		name      string
		statement string
		wantError string
	}{
		{
			name:      "foreign key",
			statement: `create table retained_documents_fk(id integer primary key, document_id integer references documents(id))`,
			wantError: "retained portable table retained_documents_fk foreign key references dropped table documents",
		},
		{
			name:      "view",
			statement: `create view retained_documents_view as select id from documents`,
			wantError: "retained portable view retained_documents_view is invalid",
		},
		{
			name:      "trigger",
			statement: `create trigger retained_documents_trigger after update on repositories begin select count(*) from documents; end`,
			wantError: "retained portable trigger retained_documents_trigger references dropped table documents",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			st, err := Open(ctx, filepath.Join(t.TempDir(), "dependency.db"))
			if err != nil {
				t.Fatalf("open store: %v", err)
			}
			defer st.Close()
			if _, err := st.DB().ExecContext(ctx, test.statement); err != nil {
				t.Fatalf("seed retained dependency: %v", err)
			}
			stats := &PortablePruneStats{}
			preparePortableBulkDropTest(t, ctx, st, stats)
			err = st.dropCanonicalPortableTablesBulk(ctx, false, stats, nil)
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("retained dependency error = %v", err)
			}
			if got := portableTestForeignKeys(t, st.DB()); got != 1 {
				t.Fatalf("foreign_keys after dependency failure = %d, want 1", got)
			}
		})
	}
}

func TestPortableTriggerDroppedDependencyUsesTablePositions(t *testing.T) {
	dropped := map[string]bool{"documents": true}
	dependency, err := portableTriggerDroppedDependency(`
		create trigger retained_literal after update on repositories
		begin
			select 'documents';
		end
	`, dropped)
	if err != nil || dependency != "" {
		t.Fatalf("literal dependency=%q err=%v", dependency, err)
	}
	dependency, err = portableTriggerDroppedDependency(`
		create trigger retained_identifier after update on repositories
		begin
			select count(*) from "documents";
		end
	`, dropped)
	if err != nil || dependency != "documents" {
		t.Fatalf("quoted identifier dependency=%q err=%v", dependency, err)
	}
	dependency, err = portableTriggerDroppedDependency(`
		create trigger retained_single_quoted after update on repositories
		begin
			select count(*) from 'documents';
		end
	`, dropped)
	if err != nil || dependency != "documents" {
		t.Fatalf("single-quoted identifier dependency=%q err=%v", dependency, err)
	}
	dependency, err = portableTriggerDroppedDependency(`
		create trigger retained_column after update on repositories
		begin
			insert into retained_audit(documents) values('column value');
		end
	`, dropped)
	if err != nil || dependency != "" {
		t.Fatalf("column-name dependency=%q err=%v", dependency, err)
	}
	dependency, err = portableTriggerDroppedDependency(`
		create trigger retained_when after update on repositories
		when exists(select 1 from documents)
		begin
			select 1;
		end
	`, dropped)
	if err != nil || dependency != "documents" {
		t.Fatalf("WHEN dependency=%q err=%v", dependency, err)
	}
	dependency, err = portableTriggerDroppedDependency(`
		create trigger retained_cte after update on repositories
		begin
			with documents as (select 1 as id)
			select id from documents;
		end
	`, dropped)
	if err != nil || dependency != "documents" {
		t.Fatalf("CTE dependency=%q err=%v", dependency, err)
	}
	dependency, err = portableTriggerDroppedDependency(`
		create trigger retained_cte_scope after update on repositories
		begin
			with documents as (select 1 as id)
			select id from documents;
			select count(*) from documents;
		end
	`, dropped)
	if err != nil || dependency != "documents" {
		t.Fatalf("later-statement dependency=%q err=%v", dependency, err)
	}
	dependency, err = portableTriggerDroppedDependency(`
		create trigger retained_cte_target after update on repositories
		begin
			with documents as (select 1 as id)
			delete from documents;
		end
	`, dropped)
	if err != nil || dependency != "documents" {
		t.Fatalf("CTE DML target dependency=%q err=%v", dependency, err)
	}
	dependency, err = portableTriggerDroppedDependency(`
		create trigger retained_grouped_source after update on repositories
		begin
			select * from (select 1 where true), documents;
		end
	`, dropped)
	if err != nil || dependency != "documents" {
		t.Fatalf("grouped source dependency=%q err=%v", dependency, err)
	}
	dependency, err = portableTriggerDroppedDependency(`
		create trigger retained_join_comma after update on repositories
		begin
			select *
			from repositories r
			join threads t on coalesce(r.id, t.repo_id) = t.repo_id,
			     documents d;
		end
	`, dropped)
	if err != nil || dependency != "documents" {
		t.Fatalf("JOIN comma dependency=%q err=%v", dependency, err)
	}
}

func portableTestForeignKeys(t *testing.T, db *sql.DB) int {
	t.Helper()
	var enabled int
	if err := db.QueryRow(`pragma foreign_keys`).Scan(&enabled); err != nil {
		t.Fatalf("read foreign_keys: %v", err)
	}
	return enabled
}

func preparePortableBulkDropTest(t *testing.T, ctx context.Context, st *Store, stats *PortablePruneStats) {
	t.Helper()
	if err := st.dropPortableDerivedBlobPointerColumns(ctx, stats); err != nil {
		t.Fatalf("drop derived blob pointer columns: %v", err)
	}
}
