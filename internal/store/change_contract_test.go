package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func changeContractFixture(t testing.TB, retention int64) *Store {
	t.Helper()
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	if _, err := s.DB().Exec(`
INSERT INTO repositories(id,owner,name,full_name,github_repo_id,raw_json,updated_at) VALUES(1,'fixture','repo','fixture/repo','100','{}','2026-01-01');
INSERT INTO threads(id,repo_id,github_id,number,kind,state,title,body,html_url,labels_json,assignees_json,raw_json,content_hash,updated_at) VALUES
 (1,1,'gh10',10,'pull_request','open','one','body','https://github.com/fixture/repo/pull/10','[]','[]','{}','hash10','2026-01-01'),
 (2,1,'gh20',20,'pull_request','open','two','body','https://github.com/fixture/repo/pull/20','[]','[]','{}','hash20','2026-01-01');
INSERT INTO thread_revisions(id,thread_id,content_hash,title_hash,body_hash,labels_hash,created_at) VALUES
 (1,1,'r1','t','b','l','2026-01-01'),(2,2,'r2','t','b','l','2026-01-01');
`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.EnableChangeContract(ctx, ChangeContractEnableOptions{Retention: retention}); err != nil {
		t.Fatal(err)
	}
	return s
}

type changeContractEntry struct {
	Seq   int64
	Table string
	Op    int64
	Key   string
}

func changeContractEntriesAfter(t testing.TB, db *sql.DB, after int64) []changeContractEntry {
	t.Helper()
	rows, err := db.Query(`select l.seq, t.table_name, l.op, cast(l.row_key as text) from change_contract_log l join change_contract_tables t using(table_id) where l.seq > ? order by l.seq`, after)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []changeContractEntry
	for rows.Next() {
		var e changeContractEntry
		if err := rows.Scan(&e.Seq, &e.Table, &e.Op, &e.Key); err != nil {
			t.Fatal(err)
		}
		out = append(out, e)
	}
	return out
}

func changeContractSequence(t testing.TB, db *sql.DB) int64 {
	t.Helper()
	var value int64
	if err := db.QueryRow(`select value from change_contract_sequence where id = 1`).Scan(&value); err != nil {
		t.Fatal(err)
	}
	return value
}

// readerCursor and readerCheck are the reference consumer algorithm from
// docs/change-contract.md, run in one read transaction.
type readerCursor struct {
	Epoch         string
	Generation    int64
	SchemaVersion int64
	Seq           int64
}

type readerVerdict struct {
	Full    bool
	Changed bool
	Rows    []changeContractEntry
	Next    readerCursor
}

func readerCheck(t testing.TB, db *sql.DB, family string, cursor *readerCursor) readerVerdict {
	t.Helper()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var next readerCursor
	if err := tx.QueryRow(`pragma schema_version`).Scan(&next.SchemaVersion); err != nil {
		t.Fatal(err)
	}
	var version int64
	var prunedThrough int64
	if err := tx.QueryRow(`select s.contract_version, s.epoch, s.generation, q.value, q.pruned_through from change_contract_state s, change_contract_sequence q`).Scan(&version, &next.Epoch, &next.Generation, &next.Seq, &prunedThrough); err != nil {
		t.Fatal(err)
	}
	var unarmed int
	if err := tx.QueryRow(`
		select count(*) from change_contract_families f
		join sqlite_schema m on m.type = 'table' and m.name = f.table_name
		left join change_contract_tables t on t.table_name = f.table_name
		where f.family = ?
		  and (t.table_id is null or (select count(*) from sqlite_schema s
			where s.type = 'trigger' and s.tbl_name = t.table_name
			  and s.sql in (t.insert_trigger, t.update_trigger, t.delete_trigger)) <> 3)`, family).Scan(&unarmed); err != nil {
		t.Fatal(err)
	}
	var prune int
	if err := tx.QueryRow(`select count(*) from sqlite_schema where type = 'trigger' and name = 'change_contract_log_prune'`).Scan(&prune); err != nil {
		t.Fatal(err)
	}
	verdict := readerVerdict{Next: next}
	if version != ChangeContractVersion || unarmed != 0 || prune != 1 || cursor == nil ||
		cursor.Epoch != next.Epoch || cursor.Generation != next.Generation ||
		cursor.SchemaVersion != next.SchemaVersion || cursor.Seq > next.Seq {
		verdict.Full = true
		return verdict
	}
	var last int64
	if err := tx.QueryRow(`select coalesce(max(t.last_seq), 0) from change_contract_families f join change_contract_tables t on t.table_name = f.table_name where f.family = ?`, family).Scan(&last); err != nil {
		t.Fatal(err)
	}
	verdict.Changed = last > cursor.Seq
	if !verdict.Changed {
		return verdict
	}
	var tableModes int
	if err := tx.QueryRow(`select count(*) from change_contract_families f join change_contract_tables t on t.table_name = f.table_name where f.family = ? and t.mode = 'table' and t.last_seq > ?`, family, cursor.Seq).Scan(&tableModes); err != nil {
		t.Fatal(err)
	}
	if cursor.Seq < prunedThrough || tableModes > 0 {
		verdict.Full = true
		return verdict
	}
	rows, err := tx.Query(`select l.seq, t.table_name, l.op, cast(l.row_key as text) from change_contract_log l
		join change_contract_tables t on t.table_id = l.table_id
		join change_contract_families f on f.table_name = t.table_name and f.family = ?
		where l.seq > ? and l.seq <= ? order by l.seq`, family, cursor.Seq, next.Seq)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var e changeContractEntry
		if err := rows.Scan(&e.Seq, &e.Table, &e.Op, &e.Key); err != nil {
			t.Fatal(err)
		}
		verdict.Rows = append(verdict.Rows, e)
	}
	return verdict
}

func TestChangeContractJournalsUpsertsUpdatesAndCascadingDeletes(t *testing.T) {
	ctx := context.Background()
	s := changeContractFixture(t, 0)
	db := s.DB()
	start := changeContractSequence(t, db)

	revision := ThreadRevision{ContentHash: "evidence-hash", EvidenceJSON: `{"v":1}`, CreatedAt: "2026-01-02T00:00:00Z"}
	first, err := s.upsertThreadRevisionEvidenceBlob(ctx, revision)
	if err != nil {
		t.Fatal(err)
	}
	revision.EvidenceJSON = `{"v":2}`
	second, err := s.upsertThreadRevisionEvidenceBlob(ctx, revision)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("hash conflict must update in place: %d != %d", first, second)
	}
	if err := s.UpsertPullRequestReviewThreads(ctx, 1, "2026-01-02T00:00:00Z", []PullRequestReviewThread{{ReviewThreadID: "RT_1", CommentsJSON: "[]"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`delete from threads where id = 1`); err != nil {
		t.Fatal(err)
	}
	got := changeContractEntriesAfter(t, db, start)
	var summary []string
	for _, e := range got {
		summary = append(summary, fmt.Sprintf("%s:%d:%s", e.Table, e.Op, e.Key))
	}
	joined := strings.Join(summary, " ")
	for _, want := range []string{
		fmt.Sprintf("blobs:1:%d", first),
		fmt.Sprintf("blobs:2:%d", first),
		`pull_request_review_threads:1:[1,"RT_1"]`,
		`pull_request_review_threads:3:[1,"RT_1"]`,
		"thread_revisions:3:1",
		"threads:3:1",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("missing %s in %s", want, joined)
		}
	}
	for i := 1; i < len(got); i++ {
		if got[i].Seq != got[i-1].Seq+1 {
			t.Fatalf("row-mode sequences must be dense here: %v", got)
		}
	}
}

func TestChangeContractPreservesWriterStatementResults(t *testing.T) {
	s := changeContractFixture(t, 0)
	result, err := s.DB().Exec(`insert into blobs(sha256, media_type, size_bytes, storage_kind, inline_text, created_at) values('direct', 'text/plain', 1, 'inline', 'x', '2026-01-01')`)
	if err != nil {
		t.Fatal(err)
	}
	id, _ := result.LastInsertId()
	affected, _ := result.RowsAffected()
	var blobID int64
	if err := s.DB().QueryRow(`select id from blobs where sha256 = 'direct'`).Scan(&blobID); err != nil {
		t.Fatal(err)
	}
	if id != blobID || affected != 1 {
		t.Fatalf("trigger journaling changed statement results: last=%d blob=%d affected=%d", id, blobID, affected)
	}
	result, err = s.DB().Exec(`update threads set title = title || '!'`)
	if err != nil {
		t.Fatal(err)
	}
	if affected, _ = result.RowsAffected(); affected != 2 {
		t.Fatalf("update rows affected = %d", affected)
	}
}

func TestChangeContractRollbackLeavesNoTrace(t *testing.T) {
	s := changeContractFixture(t, 0)
	db := s.DB()
	before := changeContractSequence(t, db)
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`update threads set title = 'rolled back' where id = 1`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if after := changeContractSequence(t, db); after != before || len(changeContractEntriesAfter(t, db, before)) != 0 {
		t.Fatalf("rolled-back write was journaled: %d -> %d", before, after)
	}
}

func TestChangeContractKeyChangeAndTableMode(t *testing.T) {
	s := changeContractFixture(t, 0)
	db := s.DB()
	if _, err := db.Exec(`insert into pull_request_review_threads(thread_id, review_thread_id, comments_json, raw_json, fetched_at) values(2, 'old', '[]', '{}', '2026-01-01')`); err != nil {
		t.Fatal(err)
	}
	start := changeContractSequence(t, db)
	if _, err := db.Exec(`update pull_request_review_threads set review_thread_id = 'new' where thread_id = 2`); err != nil {
		t.Fatal(err)
	}
	got := changeContractEntriesAfter(t, db, start)
	if len(got) != 2 || got[0].Op != ChangeContractOpDelete || got[0].Key != `[2,"old"]` || got[1].Op != ChangeContractOpInsert || got[1].Key != `[2,"new"]` || got[1].Seq != got[0].Seq+1 {
		t.Fatalf("key change must journal delete(old)+insert(new): %+v", got)
	}
	before := changeContractSequence(t, db)
	if _, err := db.Exec(`insert into comments(id, thread_id, github_id, comment_type, body, raw_json) values(1, 2, 'c1', 'issue_comment', 'x', '{}')`); err != nil {
		t.Fatal(err)
	}
	after := changeContractSequence(t, db)
	var last int64
	if err := db.QueryRow(`select last_seq from change_contract_tables where table_name = 'comments'`).Scan(&last); err != nil {
		t.Fatal(err)
	}
	if after != before+1 || last != after || len(changeContractEntriesAfter(t, db, before)) != 0 {
		t.Fatalf("table mode must advance sequence and last_seq without row entries: %d %d %d", before, after, last)
	}
}

func TestChangeContractPruneBoundsLogAndForcesFullRebuild(t *testing.T) {
	s := changeContractFixture(t, MinChangeContractRetention)
	db := s.DB()
	verdict := readerCheck(t, db, "history.blobs", nil)
	if !verdict.Full {
		t.Fatal("first read must be full")
	}
	cursor := verdict.Next
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5000; i++ {
		if _, err := tx.Exec(`insert into blobs(sha256, media_type, size_bytes, storage_kind, inline_text, created_at) values(?, 'text/plain', 1, 'inline', 'x', '2026-01-01')`, fmt.Sprintf("bulk-%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	status, err := s.ChangeContractStatus(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status.LogEntries > MinChangeContractRetention+changeContractPruneInterval || status.PrunedThrough == 0 {
		t.Fatalf("log must stay bounded: %+v", status)
	}
	var oldest int64
	if err := db.QueryRow(`select min(seq) from change_contract_log`).Scan(&oldest); err != nil {
		t.Fatal(err)
	}
	if oldest <= status.PrunedThrough {
		t.Fatalf("entry %d at or below pruned_through %d", oldest, status.PrunedThrough)
	}
	if v := readerCheck(t, db, "history.blobs", &cursor); !v.Full {
		t.Fatalf("cursor behind pruned_through must rebuild fully: %+v", v)
	}
}

func TestChangeContractReaderSkipsUnchangedFamiliesAndReadsRows(t *testing.T) {
	s := changeContractFixture(t, 0)
	db := s.DB()
	cursors := map[string]readerCursor{}
	for _, family := range []string{"reader.code", "reader.clusters", "history.thread_revisions", "history.blobs"} {
		v := readerCheck(t, db, family, nil)
		if !v.Full {
			t.Fatalf("%s first read must be full", family)
		}
		cursors[family] = v.Next
	}
	if _, err := db.Exec(`update thread_revisions set title_hash = 't2' where id = 2`); err != nil {
		t.Fatal(err)
	}
	for _, family := range []string{"reader.code", "history.blobs"} {
		c := cursors[family]
		if v := readerCheck(t, db, family, &c); v.Full || v.Changed {
			t.Fatalf("%s must be unchanged: %+v", family, v)
		}
	}
	c := cursors["history.thread_revisions"]
	v := readerCheck(t, db, "history.thread_revisions", &c)
	if v.Full || !v.Changed || len(v.Rows) != 1 || v.Rows[0].Key != "2" || v.Rows[0].Op != ChangeContractOpUpdate {
		t.Fatalf("thread revision change must be incremental: %+v", v)
	}
	c = cursors["reader.clusters"]
	if v := readerCheck(t, db, "reader.clusters", &c); v.Full || !v.Changed {
		t.Fatalf("clusters read thread_revisions and must change: %+v", v)
	}
}

func TestChangeContractTableRebuildIsDetectedAndRearmedWithNewGeneration(t *testing.T) {
	ctx := context.Background()
	s := changeContractFixture(t, 0)
	db := s.DB()
	cursor := readerCheck(t, db, "history.blobs", nil).Next
	// A legacy table rebuild drops the table's triggers; its copied rows are
	// invisible to the journal.
	if _, err := db.Exec(`
create table blobs_rebuild as select * from blobs;
drop table blobs;
create table blobs (
  id integer primary key,
  sha256 text not null unique,
  media_type text not null,
  compression text not null default 'none',
  size_bytes integer not null,
  storage_kind text not null,
  storage_path text,
  inline_text text,
  created_at text not null
);
insert into blobs select * from blobs_rebuild;
drop table blobs_rebuild;
insert into blobs(sha256, media_type, size_bytes, storage_kind, created_at) values('untracked', 'x', 1, 'inline', '2026-01-01');
`); err != nil {
		t.Fatal(err)
	}
	status, err := s.ChangeContractStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if status.Armed {
		t.Fatal("dropped triggers must report unarmed")
	}
	if v := readerCheck(t, db, "history.blobs", &cursor); !v.Full {
		t.Fatal("reader must rebuild after a table rebuild")
	}
	path := s.Path()
	s.Close()
	reopened, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	status, err = reopened.ChangeContractStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !status.Armed || status.Generation != 2 || !strings.Contains(status.GenerationWhy, "blobs") {
		t.Fatalf("writable open must re-arm with a new generation: %+v", status)
	}
	if v := readerCheck(t, reopened.DB(), "history.blobs", &cursor); !v.Full {
		t.Fatal("generation change must force a rebuild")
	}
	next := readerCheck(t, reopened.DB(), "history.blobs", nil).Next
	if _, err := reopened.DB().Exec(`update blobs set inline_text = 'tracked' where sha256 = 'untracked'`); err != nil {
		t.Fatal(err)
	}
	if v := readerCheck(t, reopened.DB(), "history.blobs", &next); v.Full || len(v.Rows) != 1 {
		t.Fatalf("re-armed table must journal again: %+v", v)
	}
}

func TestChangeContractEnableIsIdempotentAndBumpDisableWork(t *testing.T) {
	ctx := context.Background()
	s := changeContractFixture(t, 0)
	before, err := s.ChangeContractStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	repair, err := s.EnableChangeContract(ctx, ChangeContractEnableOptions{})
	if err != nil {
		t.Fatal(err)
	}
	after, _ := s.ChangeContractStatus(ctx)
	if repair.Created || len(repair.Rearmed) != 0 || after.Generation != before.Generation || after.Epoch != before.Epoch {
		t.Fatalf("enable on an armed contract must be a no-op: %+v %+v", repair, after)
	}
	generation, err := s.BumpChangeContractGeneration(ctx, "owner repair", time.Time{})
	if err != nil || generation != before.Generation+1 {
		t.Fatalf("bump = %d, %v", generation, err)
	}
	if _, err := s.BumpChangeContractGeneration(ctx, " ", time.Time{}); err == nil {
		t.Fatal("bump requires a reason")
	}
	removed, err := s.DisableChangeContract(ctx)
	if err != nil || !removed {
		t.Fatalf("disable = %v, %v", removed, err)
	}
	var objects int
	if err := s.DB().QueryRow(`select count(*) from sqlite_schema where name like 'change_contract%'`).Scan(&objects); err != nil {
		t.Fatal(err)
	}
	if objects != 0 {
		t.Fatalf("disable left %d objects", objects)
	}
	if _, err := s.DB().Exec(`update threads set title = 'after disable'`); err != nil {
		t.Fatal(err)
	}
	reenabled, err := s.EnableChangeContract(ctx, ChangeContractEnableOptions{})
	if err != nil || !reenabled.Created {
		t.Fatalf("re-enable = %+v, %v", reenabled, err)
	}
	fresh, _ := s.ChangeContractStatus(ctx)
	if fresh.Epoch == before.Epoch || fresh.Generation != 1 {
		t.Fatalf("re-enable must start a new epoch: %+v", fresh)
	}
}

func TestChangeContractPartialContractIsReplacedWithNewEpoch(t *testing.T) {
	ctx := context.Background()
	s := changeContractFixture(t, 0)
	before, _ := s.ChangeContractStatus(ctx)
	// An interrupted non-transactional strip could leave only some objects.
	if _, err := s.DB().Exec(`drop trigger change_contract_log_prune; drop table change_contract_log`); err != nil {
		t.Fatal(err)
	}
	path := s.Path()
	s.Close()
	reopened, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	after, err := reopened.ChangeContractStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !after.Armed || after.Epoch == before.Epoch || after.Generation != 1 {
		t.Fatalf("partial contract must be replaced by a new epoch: %+v", after)
	}
}

func TestChangeContractOrphanedObjectsAreRepairedKeepingRetention(t *testing.T) {
	ctx := context.Background()
	for _, drop := range []string{"drop table change_contract_state", "drop table change_contract_families"} {
		s := changeContractFixture(t, 2048)
		before, _ := s.ChangeContractStatus(ctx)
		if _, err := s.DB().Exec(drop); err != nil {
			t.Fatal(err)
		}
		path := s.Path()
		s.Close()
		reopened, err := Open(ctx, path)
		if err != nil {
			t.Fatal(err)
		}
		after, err := reopened.ChangeContractStatus(ctx)
		reopened.Close()
		if err != nil {
			t.Fatal(err)
		}
		if !after.Enabled || !after.Armed || after.Epoch == before.Epoch || after.Retention != 2048 {
			t.Fatalf("%s: orphaned contract must be replaced keeping retention: %+v", drop, after)
		}
	}
}

func TestOpenWritableWithoutMigrationRefusesOtherSchemaVersions(t *testing.T) {
	ctx := context.Background()
	s := changeContractFixture(t, 0)
	path := s.Path()
	if _, err := s.DB().Exec(`update observation_schema_convergence set checked_observation_sequence = -1`); err != nil {
		t.Fatal(err)
	}
	s.Close()
	st, err := OpenWritableWithoutMigration(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	var checked int64
	if err := st.DB().QueryRow(`select checked_observation_sequence from observation_schema_convergence`).Scan(&checked); err != nil {
		t.Fatal(err)
	}
	if checked != -1 {
		t.Fatal("maintenance open must not run convergence work")
	}
	if _, err := st.DB().Exec(fmt.Sprintf("pragma user_version = %d", schemaVersion-1)); err != nil {
		t.Fatal(err)
	}
	st.Close()
	if _, err := OpenWritableWithoutMigration(ctx, path); err == nil {
		t.Fatal("older schema version accepted")
	}
	if _, err := OpenWritableWithoutMigration(ctx, path+".missing"); err == nil {
		t.Fatal("missing archive accepted")
	}
}

func TestChangeContractLowerRetentionPrunesImmediately(t *testing.T) {
	ctx := context.Background()
	s := changeContractFixture(t, 4096)
	tx, err := s.DB().Begin()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3000; i++ {
		if _, err := tx.Exec(`insert into blobs(sha256, media_type, size_bytes, storage_kind, created_at) values(?, 'x', 1, 'inline', '2026-01-01')`, fmt.Sprintf("r-%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.EnableChangeContract(ctx, ChangeContractEnableOptions{Retention: MinChangeContractRetention}); err != nil {
		t.Fatal(err)
	}
	status, err := s.ChangeContractStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if status.LogEntries > MinChangeContractRetention || status.PrunedThrough != status.Sequence-MinChangeContractRetention || status.Retention != MinChangeContractRetention {
		t.Fatalf("lowered retention must prune at once: %+v", status)
	}
}

func TestChangeContractStatusReportsFamilies(t *testing.T) {
	s := changeContractFixture(t, 0)
	status, err := s.ChangeContractStatus(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !status.Enabled || !status.Armed || status.ContractVersion != ChangeContractVersion || len(status.Epoch) != 32 {
		t.Fatalf("status = %+v", status)
	}
	families := map[string]ChangeContractFamily{}
	for _, f := range status.Families {
		families[f.Family] = f
	}
	for family, tables := range changeContractFamilies {
		got, ok := families[family]
		if !ok || len(got.Tables) != len(tables) || !got.Armed {
			t.Fatalf("family %s = %+v", family, got)
		}
	}
	for _, table := range status.Tables {
		if table.Present && !table.Armed {
			t.Fatalf("present table %s unarmed", table.Table)
		}
	}
	data, _ := json.Marshal(status)
	if !strings.Contains(string(data), `"history.blobs"`) {
		t.Fatalf("status JSON lacks families: %s", data)
	}
}

func TestChangeContractTableSpecsAreStable(t *testing.T) {
	ids := map[int64]bool{}
	names := map[string]bool{}
	for i, spec := range changeContractTableSpecs {
		if spec.id != int64(i+1) || ids[spec.id] || names[spec.name] {
			t.Fatalf("table identifiers must be permanent, dense and unique: %+v", spec)
		}
		ids[spec.id], names[spec.name] = true, true
	}
	for family, tables := range changeContractFamilies {
		for _, table := range tables {
			if !names[table] {
				t.Fatalf("family %s names untracked table %s", family, table)
			}
		}
	}
}

func TestPortablePruneStripsChangeContract(t *testing.T) {
	ctx := context.Background()
	s := changeContractFixture(t, 0)
	if _, err := s.PrunePortablePayloads(ctx, PortablePruneOptions{}); err != nil {
		t.Fatal(err)
	}
	var objects int
	if err := s.DB().QueryRow(`select count(*) from sqlite_schema where name like 'change_contract%'`).Scan(&objects); err != nil {
		t.Fatal(err)
	}
	if objects != 0 {
		t.Fatalf("portable prune kept %d change contract objects", objects)
	}
}

// BenchmarkChangeContractWriterOverhead measures the per-row trigger cost
// inside one writer transaction, excluding commit durability, so the
// difference between enabled=false and enabled=true is the journal itself.
func BenchmarkChangeContractWriterOverhead(b *testing.B) {
	workloads := map[string]string{
		"blob_upsert_conflict": `insert into blobs(sha256, media_type, compression, size_bytes, storage_kind, inline_text, created_at)
			values(?, 'application/json', 'none', 1, 'inline', ?, '2026-01-01')
			on conflict(sha256) do update set inline_text = excluded.inline_text`,
		"comment_insert_table_mode": `insert into comments(thread_id, github_id, comment_type, body, raw_json) values(1, ?, 'issue_comment', ?, '{}')`,
	}
	for _, name := range []string{"blob_upsert_conflict", "comment_insert_table_mode"} {
		for _, enabled := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/enabled=%v", name, enabled), func(b *testing.B) {
				ctx := context.Background()
				s, err := Open(ctx, filepath.Join(b.TempDir(), "archive.db"))
				if err != nil {
					b.Fatal(err)
				}
				defer s.Close()
				if _, err := s.DB().Exec(`INSERT INTO repositories(id,owner,name,full_name,github_repo_id,raw_json,updated_at) VALUES(1,'f','r','f/r','1','{}','2026-01-01');
INSERT INTO threads(id,repo_id,github_id,number,kind,state,title,body,html_url,labels_json,assignees_json,raw_json,content_hash,updated_at) VALUES(1,1,'g',1,'issue','open','t','b','u','[]','[]','{}','h','2026-01-01')`); err != nil {
					b.Fatal(err)
				}
				if enabled {
					if _, err := s.EnableChangeContract(ctx, ChangeContractEnableOptions{}); err != nil {
						b.Fatal(err)
					}
				}
				tx, err := s.DB().Begin()
				if err != nil {
					b.Fatal(err)
				}
				defer tx.Rollback()
				stmt, err := tx.Prepare(workloads[name])
				if err != nil {
					b.Fatal(err)
				}
				defer stmt.Close()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					key := fmt.Sprintf("k-%d", i)
					if name == "blob_upsert_conflict" {
						key = fmt.Sprintf("k-%d", i%4096)
					}
					if _, err := stmt.Exec(key, fmt.Sprintf(`{"i":%d}`, i)); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
