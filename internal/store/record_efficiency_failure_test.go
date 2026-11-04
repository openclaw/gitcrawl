package store

import (
	"context"
	"path/filepath"
	"testing"
)

func TestRecordLayoutMigrationAndFailureBoundaries(t *testing.T) {
	ctx := context.Background()
	st, _ := recordFixture(t)
	if !st.RecordChecksEnabled() {
		t.Fatal("not enabled")
	}
	if err := st.prepareRecordEfficiencyMigration(ctx); err != nil {
		t.Fatal(err)
	}
	if err := st.ensureRecordEfficiencySchema(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := st.DB().Exec(`drop view threads_current; create view threads_current as select id from threads`); err != nil {
		t.Fatal(err)
	}
	if err := st.ensureRecordEfficiencyViews(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := st.recordEfficiencyViewSQL(ctx, recordEfficiencyView{name: "missing", table: "missing"}); err != nil || ok {
		t.Fatalf("missing table accepted: %v %v", ok, err)
	}
	tx, err := st.DB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = StripRecordEfficiency(ctx, tx); err != nil {
		t.Fatal(err)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if !st.hasTable(ctx, "record_evidence_blobs") {
		t.Fatal("rollback destroyed source")
	}
	empty, err := Open(ctx, filepath.Join(t.TempDir(), "portable.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer empty.Close()
	if _, err = empty.DB().Exec(`create table portable_metadata (id integer)`); err != nil {
		t.Fatal(err)
	}
	if _, err = empty.EnableRecordEfficiency(ctx, "now"); err == nil {
		t.Fatal("portable enabled")
	}
	st.Close()
	if _, err = st.tableColumns(ctx, "threads"); err == nil {
		t.Fatal("closed DB read accepted")
	}
	if _, err = st.EnableRecordEfficiency(ctx, "now"); err == nil {
		t.Fatal("closed DB enable accepted")
	}
	if err = st.ensureRecordCheckHistory(ctx); err == nil {
		t.Fatal("closed history accepted")
	}
	if err = st.dropRecordCheckHistory(ctx); err == nil {
		t.Fatal("closed trigger drop accepted")
	}
	if err = st.retainExactBlob(ctx, "x", "y", "hash", []byte("raw")); err == nil {
		t.Fatal("closed evidence write accepted")
	}
	if err = st.DisableRecordEfficiency(ctx); err == nil {
		t.Fatal("closed disable accepted")
	}
	if _, err = st.BackfillRecordEfficiency(ctx); err == nil {
		t.Fatal("closed backfill accepted")
	}
}
