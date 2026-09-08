package cli

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	crawlstore "github.com/openclaw/gitcrawl/internal/store"
)

func TestCloudSQLiteSnapshotScrubsRevisionAndFailurePayloads(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "source.db")
	st, err := crawlstore.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	if _, err := st.DB().ExecContext(ctx, `
		insert into repositories(id, owner, name, full_name, raw_json, updated_at)
		values(1, 'example', 'repo', 'example/repo', '{}', '2026-07-12T00:00:00Z');
		insert into threads(
			id, repo_id, github_id, number, kind, state, title, body, html_url,
			labels_json, assignees_json, raw_json, content_hash, updated_at
		) values(
			1, 1, 'pr-1', 1, 'pull_request', 'open', 'Canonical PR', 'Full PR body
with a second line', 'https://github.com/example/repo/pull/1',
			'[]', '[]', '{}', 'hash', '2026-07-12T00:00:00Z'
		);
		insert into comments(id, thread_id, github_id, comment_type, body, raw_json)
		values(1, 1, 'comment-1', 'review', 'Current canonical comment', '{}');
		insert into comment_revisions(
			id, comment_id, body, raw_json, deleted_at, deletion_reason, recorded_at
		) values
			(1, 1, 'Original canonical comment', '{"diagnostic":"synthetic-comment-private-1"}',
				null, null, '2026-07-11T00:00:00Z'),
			(2, 1, 'Current canonical comment', '{"diagnostic":"synthetic-comment-private-2"}',
				'2026-07-12T00:00:00Z', 'source deletion', '2026-07-12T00:00:00Z');
		insert into pull_request_review_threads(
			thread_id, review_thread_id, path, line, first_comment_body,
			comments_json, raw_json, fetched_at
		) values(
			1, 'review-1', 'main.go', 12, 'Current canonical review',
			'[{"body":"Current canonical review","diffHunk":"synthetic-review-comments-private"}]',
			'{}', '2026-07-12T00:00:00Z'
		);
		insert into pull_request_review_thread_revisions(
			id, thread_id, review_thread_id, path, line, is_resolved, first_comment_body,
			comments_json, raw_json, fetched_at, deleted_at, deletion_reason, recorded_at
		) values
			(1, 1, 'review-1', 'main.go', 10, 0, 'Original canonical review',
				'[{"body":"Original canonical review","diffHunk":"synthetic-review-comments-private-1"}]',
				'{"diagnostic":"synthetic-review-private-1"}', '2026-07-11T00:00:00Z',
				null, null, '2026-07-11T00:00:00Z'),
			(2, 1, 'review-1', 'main.go', 12, 1, 'Current canonical review',
				'[{"body":"Current canonical review","diffHunk":"synthetic-review-comments-private-2"}]',
				'{"diagnostic":"synthetic-review-private-2"}', '2026-07-12T00:00:00Z',
				'2026-07-12T00:00:00Z', 'source deletion', '2026-07-12T00:00:00Z');
		insert into pull_request_files(thread_id, position, path, patch, raw_json, fetched_at)
		values(1, 0, 'main.go', '@@ -1 +1 @@
-old
+new
', '{}', '2026-07-12T00:00:00Z');
		insert into sync_attempt_failures(
			id, repo_id, thread_id, number, operation, error_class, error_message,
			first_seen_at, last_seen_at, retry_count, resolved_at
		) values
			(1, 1, 1, 1, 'pr_details', 'transport', 'synthetic-failure-private-1',
				'2026-07-11T00:00:00Z', '2026-07-12T00:00:00Z', 3, null),
			(2, 1, 1, 1, 'comments', 'transport', 'synthetic-failure-private-2',
				'2026-07-11T00:00:00Z', '2026-07-12T00:00:00Z', 1, '2026-07-12T00:00:00Z');
	`); err != nil {
		t.Fatalf("seed cloud payloads: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close seeded store: %v", err)
	}
	sourceHash, err := cloudFileSHA256(dbPath)
	if err != nil {
		t.Fatalf("hash source: %v", err)
	}
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	defer db.Close()
	readValues := func(db *sql.DB, table, columns string) string {
		t.Helper()
		var values string
		query := `select json_group_array(json_array(` + columns + `)) from (select * from ` + table + ` order by rowid)`
		if err := db.QueryRowContext(ctx, query).Scan(&values); err != nil {
			t.Fatalf("read %s (%s): %v", table, columns, err)
		}
		return values
	}
	checks := []struct {
		table, canonical, excluded, wantExcluded string
		canonicalBefore, excludedBefore          string
	}{
		{table: "threads", canonical: "id, repo_id, github_id, number, kind, state, title, body",
			excluded: "raw_json", wantExcluded: `[[""]]`},
		{table: "comments", canonical: "id, thread_id, github_id, comment_type, body",
			excluded: "raw_json", wantExcluded: `[[""]]`},
		{table: "comment_revisions", canonical: "id, comment_id, author_login, author_type, body, is_bot, created_at_gh, updated_at_gh, deleted_at, deletion_reason, recorded_at",
			excluded: "raw_json", wantExcluded: `[[""],[""]]`},
		{table: "pull_request_review_threads", canonical: "thread_id, review_thread_id, path, line, first_comment_body, fetched_at",
			excluded: "raw_json, comments_json", wantExcluded: `[["",""]]`},
		{table: "pull_request_review_thread_revisions", canonical: "id, thread_id, review_thread_id, path, line, start_line, is_resolved, is_outdated, viewer_can_resolve, viewer_can_unresolve, viewer_can_reply, first_author_login, first_author_type, first_comment_body, first_comment_url, first_comment_created_at, first_comment_updated_at, fetched_at, deleted_at, deletion_reason, recorded_at",
			excluded: "raw_json, comments_json", wantExcluded: `[["",""],["",""]]`},
		{table: "pull_request_files", canonical: "thread_id, position, path, patch",
			excluded: "raw_json", wantExcluded: `[[""]]`},
		{table: "sync_attempt_failures", canonical: "id, repo_id, thread_id, number, operation, error_class, first_seen_at, last_seen_at, retry_count, resolved_at",
			excluded: "error_message", wantExcluded: `[[""],[""]]`},
	}
	for i := range checks {
		checks[i].canonicalBefore = readValues(db, checks[i].table, checks[i].canonical)
		checks[i].excludedBefore = readValues(db, checks[i].table, checks[i].excluded)
	}

	snapshotPath, cleanup, err := cloudSQLiteSnapshotPath(ctx, db, dbPath)
	if err != nil {
		t.Fatalf("cloud snapshot: %v", err)
	}
	defer cleanup()
	if snapshotPath == dbPath {
		t.Fatal("cloud snapshot returned source path")
	}
	snapshotDB, err := sql.Open("sqlite", snapshotPath)
	if err != nil {
		t.Fatalf("open snapshot: %v", err)
	}
	defer snapshotDB.Close()
	for pass := 0; pass < 2; pass++ {
		if pass > 0 {
			if err := sanitizeCloudSQLiteSnapshot(ctx, snapshotDB); err != nil {
				t.Fatalf("sanitize again: %v", err)
			}
		}
		for _, check := range checks {
			if got := readValues(snapshotDB, check.table, check.canonical); got != check.canonicalBefore {
				t.Errorf("pass %d: %s canonical rows = %s, want %s", pass, check.table, got, check.canonicalBefore)
			}
			if got := readValues(snapshotDB, check.table, check.excluded); got != check.wantExcluded {
				t.Errorf("pass %d: %s excluded payloads = %s, want %s", pass, check.table, got, check.wantExcluded)
			}
			if got := readValues(db, check.table, check.canonical); got != check.canonicalBefore {
				t.Errorf("pass %d: %s source canonical rows changed", pass, check.table)
			}
			if got := readValues(db, check.table, check.excluded); got != check.excludedBefore {
				t.Errorf("pass %d: %s source payloads changed", pass, check.table)
			}
		}
	}
	if err := snapshotDB.Close(); err != nil {
		t.Fatalf("close snapshot: %v", err)
	}
	snapshotBytes, err := os.ReadFile(snapshotPath)
	if err != nil {
		t.Fatalf("read snapshot bytes: %v", err)
	}
	for _, marker := range []string{
		"synthetic-comment-private-", "synthetic-review-private-",
		"synthetic-review-comments-private", "synthetic-failure-private-",
	} {
		if bytes.Contains(snapshotBytes, []byte(marker)) {
			t.Errorf("snapshot bytes retained %q", marker)
		}
	}
	cleanup()
	cleanup()
	if _, err := os.Stat(filepath.Dir(snapshotPath)); !os.IsNotExist(err) {
		t.Fatalf("snapshot cleanup left directory: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close source: %v", err)
	}
	if got, err := cloudFileSHA256(dbPath); err != nil || got != sourceHash {
		t.Fatalf("source file changed: hash = %q, want %q, error = %v", got, sourceHash, err)
	}
}

func TestCloudSQLiteSnapshotLegacyPrivacyColumns(t *testing.T) {
	for _, tc := range []struct {
		name, schema, query, want string
	}{
		{
			name:   "missing tables",
			schema: `create table threads(body text); insert into threads values('canonical');`,
			query:  `select body from threads`,
			want:   "canonical",
		},
		{
			name: "missing columns",
			schema: `
				create table comment_revisions(body text);
				insert into comment_revisions values('comment history');
				create table pull_request_review_thread_revisions(first_comment_body text);
				insert into pull_request_review_thread_revisions values('review history');
				create table sync_attempt_failures(retry_count integer);
				insert into sync_attempt_failures values(3);`,
			query: `select json_array(body, first_comment_body, retry_count)
				from comment_revisions, pull_request_review_thread_revisions, sync_attempt_failures`,
			want: `["comment history","review history",3]`,
		},
		{
			name: "raw without comments",
			schema: `
				create table comment_revisions(raw_json text);
				insert into comment_revisions values(null), (''), ('private');
				create table pull_request_review_thread_revisions(raw_json text);
				insert into pull_request_review_thread_revisions values('private');
				create table sync_attempt_failures(error_message text);
				insert into sync_attempt_failures values('private');`,
			query: `select json_array(
				(select json_group_array(raw_json) from comment_revisions),
				(select raw_json from pull_request_review_thread_revisions),
				(select error_message from sync_attempt_failures))`,
			want: `[[null,"",""],"",""]`,
		},
		{
			name: "comments without raw",
			schema: `
				create table pull_request_review_thread_revisions(first_comment_body text, comments_json text);
				insert into pull_request_review_thread_revisions values('review history', '[{"diffHunk":"private"}]');`,
			query: `select json_array(first_comment_body, comments_json) from pull_request_review_thread_revisions`,
			want:  `["review history",""]`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "legacy.db"))
			if err != nil {
				t.Fatalf("open legacy source: %v", err)
			}
			defer db.Close()
			if _, err := db.ExecContext(ctx, tc.schema); err != nil {
				t.Fatalf("seed legacy source: %v", err)
			}
			snapshotPath, cleanup, err := cloudSQLiteSnapshotPath(ctx, db, "")
			if err != nil {
				t.Fatalf("legacy cloud snapshot: %v", err)
			}
			defer cleanup()
			snapshotDB, err := sql.Open("sqlite", snapshotPath)
			if err != nil {
				t.Fatalf("open legacy snapshot: %v", err)
			}
			defer snapshotDB.Close()
			var got string
			if err := snapshotDB.QueryRowContext(ctx, tc.query).Scan(&got); err != nil {
				t.Fatalf("read legacy snapshot: %v", err)
			}
			if got != tc.want {
				t.Fatalf("legacy snapshot = %q, want %q", got, tc.want)
			}
		})
	}
}

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
	if err := st.Close(); err != nil {
		t.Fatalf("close portable store: %v", err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("reopen portable sqlite: %v", err)
	}
	defer db.Close()
	snapshot, err := buildGitcrawlCloudSnapshot(ctx, db, path, true, false)
	if err != nil {
		t.Fatalf("build portable cloud snapshot: %v", err)
	}
	if snapshot.SourceSyncAt != exportedAt {
		t.Fatalf("snapshot source sync at = %q, want %q", snapshot.SourceSyncAt, exportedAt)
	}
	if counts := gitcrawlCloudDatasetCounts(snapshot); counts["repositories"] != 1 || counts["threads"] != 1 {
		t.Fatalf("portable snapshot counts = %#v", counts)
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
	threadIDs := make([]int64, 0, 4)
	threads := make([]crawlstore.Thread, 0, 4)
	for number := 1; number <= 4; number++ {
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
	if revisions.Eligible != 4 || revisions.Covered != 2 || revisions.Fresh != 1 {
		t.Fatalf("revision hydration = %#v, want eligible=4 covered=2 fresh=1", revisions)
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
		revisionDataset.EligibleCount != 4 ||
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
