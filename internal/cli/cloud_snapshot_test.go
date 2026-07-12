package cli

import (
	"context"
	"database/sql"
	"testing"
)

func TestLatestRFC3339QueryValueUsesParsedTimestampOrder(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`
		create table timestamps(value text not null);
		insert into timestamps(value) values
			('2026-07-12T01:00:00+02:00'),
			('2026-07-12T00:30:00Z'),
			('');
	`); err != nil {
		t.Fatalf("seed timestamps: %v", err)
	}

	got, err := latestRFC3339QueryValue(
		context.Background(),
		db,
		`select value from timestamps`,
	)
	if err != nil {
		t.Fatalf("latest timestamp: %v", err)
	}
	if got != "2026-07-12T00:30:00Z" {
		t.Fatalf("latest timestamp = %q, want parsed maximum", got)
	}
}

func TestGitcrawlCloudSourceSyncAtNormalizesRFC3339Maximum(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`
		create table sync_runs(status text, started_at text, finished_at text);
		create table threads(updated_at_gh text, updated_at text);
		create table repositories(updated_at text);
		insert into sync_runs(status, started_at, finished_at)
		values('success', '2026-07-12T01:00:00+02:00', '');
		insert into threads(updated_at_gh, updated_at)
		values('2026-07-12T00:30:00Z', '2026-07-12T00:00:00Z');
		insert into repositories(updated_at)
		values('2026-07-12T00:15:00Z');
	`); err != nil {
		t.Fatalf("seed source clocks: %v", err)
	}

	got, err := gitcrawlCloudSourceSyncAt(context.Background(), db)
	if err != nil {
		t.Fatalf("source sync at: %v", err)
	}
	if got != "2026-07-12T00:30:00Z" {
		t.Fatalf("source sync at = %q, want parsed maximum", got)
	}
}

func TestLatestRFC3339QueryValueRejectsInvalidTimestamp(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`create table timestamps(value text); insert into timestamps values('not-a-time')`); err != nil {
		t.Fatalf("seed timestamp: %v", err)
	}

	if _, err := latestRFC3339QueryValue(
		context.Background(),
		db,
		`select value from timestamps`,
	); err == nil {
		t.Fatal("invalid RFC3339 timestamp was accepted")
	}
}
