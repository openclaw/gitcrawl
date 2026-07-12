package cli

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	crawlstore "github.com/openclaw/gitcrawl/internal/store"
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

func TestGitcrawlCloudSourceSyncAtUsesPortableMetadataWithoutSyncRuns(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "portable.db")
	st, err := crawlstore.Open(ctx, path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	repoID, err := st.UpsertRepository(ctx, crawlstore.Repository{
		Owner:     "openclaw",
		Name:      "gitcrawl",
		FullName:  "openclaw/gitcrawl",
		UpdatedAt: "2026-07-11T23:00:00Z",
	})
	if err != nil {
		t.Fatalf("seed repository: %v", err)
	}
	if _, err := st.UpsertThread(ctx, crawlstore.Thread{
		RepoID:          repoID,
		GitHubID:        "portable-1",
		Number:          1,
		Kind:            "issue",
		State:           "open",
		Title:           "Portable source clock",
		HTMLURL:         "https://github.com/openclaw/gitcrawl/issues/1",
		LabelsJSON:      "[]",
		AssigneesJSON:   "[]",
		ContentHash:     "portable-1",
		UpdatedAtGitHub: "2026-07-11T23:30:00Z",
		UpdatedAt:       "2026-07-11T23:30:00Z",
	}); err != nil {
		t.Fatalf("seed thread: %v", err)
	}
	if _, err := st.PrunePortablePayloads(ctx, crawlstore.PortablePruneOptions{BodyChars: 64}); err != nil {
		t.Fatalf("prune portable store: %v", err)
	}
	hasSyncRuns, err := sqliteTableExists(ctx, st.DB(), "sync_runs")
	if err != nil {
		t.Fatalf("inspect sync_runs: %v", err)
	}
	if hasSyncRuns {
		t.Fatal("portable store retained sync_runs")
	}
	var exportedAt string
	if err := st.DB().QueryRowContext(
		ctx,
		`select value from portable_metadata where key = 'exported_at'`,
	).Scan(&exportedAt); err != nil {
		t.Fatalf("read portable exported_at: %v", err)
	}

	got, err := gitcrawlCloudSourceSyncAt(ctx, st.DB())
	if err != nil {
		t.Fatalf("source sync at: %v", err)
	}
	if got != exportedAt {
		t.Fatalf("source sync at = %q, want portable exported_at %q", got, exportedAt)
	}
}

func TestObservationOrderRevisionCoverageCountsFreshSelectedThreads(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "coverage.db")
	st, err := crawlstore.Open(ctx, path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	repoID, err := st.UpsertRepository(ctx, crawlstore.Repository{
		Owner:     "openclaw",
		Name:      "gitcrawl",
		FullName:  "openclaw/gitcrawl",
		UpdatedAt: "2026-07-12T10:00:00Z",
	})
	if err != nil {
		t.Fatalf("seed repository: %v", err)
	}
	threadIDs := make([]int64, 0, 3)
	threads := make([]crawlstore.Thread, 0, 3)
	for number := 1; number <= 3; number++ {
		thread := crawlstore.Thread{
			RepoID:          repoID,
			GitHubID:        fmt.Sprintf("thread-%d", number),
			Number:          number,
			Kind:            "issue",
			State:           "open",
			Title:           "Revision coverage",
			HTMLURL:         fmt.Sprintf("https://github.com/openclaw/gitcrawl/issues/%d", number),
			LabelsJSON:      "[]",
			AssigneesJSON:   "[]",
			ContentHash:     fmt.Sprintf("thread-%d", number),
			UpdatedAtGitHub: "2026-07-12T10:00:00Z",
			UpdatedAt:       "2026-07-12T10:00:00Z",
		}
		threadID, err := st.UpsertThread(ctx, thread)
		if err != nil {
			t.Fatalf("seed thread %d: %v", number, err)
		}
		thread.ID = threadID
		threadIDs = append(threadIDs, threadID)
		threads = append(threads, thread)
	}
	if _, err := st.UpsertThreadRevisionAndFingerprint(
		ctx,
		crawlstore.ThreadEvidence{Thread: threads[1]},
		"2026-07-12T10:01:00Z",
	); err != nil {
		t.Fatalf("seed fresh selected revision: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `
		insert into thread_revisions(
			thread_id, source_updated_at, content_hash, title_hash, body_hash,
			labels_hash, observation_sequence, created_at
		) values
			(?, '2026-07-12T09:00:00Z', 'later-stale', 'later-stale', 'later-stale', 'later-stale', 99, '2026-07-12T10:02:00Z'),
			(?, '2026-07-12T09:00:00Z', 'stale', 'stale', 'stale', 'stale', 1, '2026-07-12T10:03:00Z')
	`, threadIDs[1], threadIDs[2]); err != nil {
		t.Fatalf("seed revisions: %v", err)
	}
	coverage, err := st.ArchiveCoverage(ctx, crawlstore.ArchiveCoverageOptions{})
	if err != nil {
		t.Fatalf("archive coverage: %v", err)
	}
	revisions := coverage.Totals.Enrichment.Revisions
	if revisions.Eligible != 3 || revisions.Covered != 2 || revisions.Fresh != 1 {
		t.Fatalf("revision hydration = %#v, want eligible=3 covered=2 fresh=1", revisions)
	}
	datasets, err := loadGitcrawlCloudDatasets(ctx, st.DB(), true, coverage.Totals.Enrichment)
	if err != nil {
		t.Fatalf("load datasets: %v", err)
	}
	var revisionDataset gitcrawlCloudDataset
	for _, dataset := range datasets {
		if dataset.Name == "thread_revisions" {
			revisionDataset = dataset
			break
		}
	}
	if revisionDataset.RowCount != 3 ||
		revisionDataset.EligibleCount != 3 ||
		revisionDataset.CoveredCount != 1 ||
		revisionDataset.Complete {
		t.Fatalf("revision dataset coverage = %#v", revisionDataset)
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
