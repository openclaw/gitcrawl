package store

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestSyncAttemptFailureRetryAndResolve(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repoID, err := st.UpsertRepository(ctx, Repository{Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl", RawJSON: "{}", UpdatedAt: "2026-06-06T00:00:00Z"})
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	threadID, err := st.UpsertThread(ctx, Thread{
		RepoID: repoID, GitHubID: "84", Number: 84, Kind: "pull_request", State: "open",
		Title: "Track failed sync attempts", HTMLURL: "https://github.com/openclaw/gitcrawl/pull/84",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "h84", UpdatedAt: "2026-06-06T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("thread: %v", err)
	}
	first := SyncAttemptFailure{
		RepoID: repoID, ThreadID: threadID, Number: 84, Operation: "pull_request_details", ErrorClass: "rate_limit",
		ErrorMessage: "secondary rate limit", FirstSeenAt: "2026-06-06T00:00:00Z", LastSeenAt: "2026-06-06T00:00:00Z",
	}
	id, err := st.RecordSyncAttemptFailure(ctx, first)
	if err != nil {
		t.Fatalf("record first failure: %v", err)
	}
	if _, err := st.RecordSyncAttemptFailure(ctx, SyncAttemptFailure{
		RepoID: repoID, ThreadID: threadID, Number: 84, Operation: "pull_request_details", ErrorClass: "rate_limit",
		ErrorMessage: "secondary rate limit again", LastSeenAt: "2026-06-06T00:05:00Z",
	}); err != nil {
		t.Fatalf("record retry failure: %v", err)
	}

	failures, err := st.ListSyncAttemptFailures(ctx, SyncAttemptFailureListOptions{RepoID: repoID})
	if err != nil {
		t.Fatalf("list failures: %v", err)
	}
	if len(failures) != 1 {
		t.Fatalf("failure count = %d, want 1", len(failures))
	}
	if failures[0].ID != id || failures[0].RetryCount != 1 || failures[0].ResolvedAt != "" || failures[0].ErrorMessage != "secondary rate limit again" {
		t.Fatalf("failure = %+v", failures[0])
	}

	resolved, err := st.ResolveSyncAttemptFailures(ctx, repoID, 84, "2026-06-06T00:10:00Z")
	if err != nil {
		t.Fatalf("resolve failures: %v", err)
	}
	if resolved != 1 {
		t.Fatalf("resolved = %d, want 1", resolved)
	}
	unresolved, err := st.ListSyncAttemptFailures(ctx, SyncAttemptFailureListOptions{RepoID: repoID})
	if err != nil {
		t.Fatalf("list unresolved failures: %v", err)
	}
	if unresolved == nil || len(unresolved) != 0 {
		t.Fatalf("unresolved failures = %+v", unresolved)
	}
	history, err := st.ListSyncAttemptFailures(ctx, SyncAttemptFailureListOptions{RepoID: repoID, IncludeResolved: true})
	if err != nil {
		t.Fatalf("list history failures: %v", err)
	}
	if len(history) != 1 || history[0].ResolvedAt != "2026-06-06T00:10:00Z" {
		t.Fatalf("history = %+v", history)
	}

	if _, err := st.RecordSyncAttemptFailure(ctx, SyncAttemptFailure{
		RepoID: repoID, ThreadID: threadID, Number: 84, Operation: "pull_request_details", ErrorClass: "rate_limit",
		ErrorMessage: "secondary rate limit after resolve", LastSeenAt: "2026-06-06T00:15:00Z",
	}); err != nil {
		t.Fatalf("record unresolved retry: %v", err)
	}
	failures, err = st.ListSyncAttemptFailures(ctx, SyncAttemptFailureListOptions{RepoID: repoID})
	if err != nil {
		t.Fatalf("list reopened failures: %v", err)
	}
	if len(failures) != 1 || failures[0].RetryCount != 2 || failures[0].ResolvedAt != "" {
		t.Fatalf("reopened failure = %+v", failures)
	}
}

func TestListSyncAttemptFailuresTreatsPreLedgerReadOnlyStoreAsEmpty(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "gitcrawl.db")
	st, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	repoID, err := st.UpsertRepository(ctx, Repository{Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl", RawJSON: "{}", UpdatedAt: "2026-06-06T00:00:00Z"})
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open raw db: %v", err)
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf(`
drop table sync_attempt_failures;
pragma user_version = %d;
`, syncAttemptFailuresSchemaVersion-1)); err != nil {
		t.Fatalf("downgrade ledger schema: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close raw db: %v", err)
	}

	readOnly, err := OpenReadOnly(ctx, dbPath)
	if err != nil {
		t.Fatalf("open read-only store: %v", err)
	}
	defer readOnly.Close()
	failures, err := readOnly.ListSyncAttemptFailures(ctx, SyncAttemptFailureListOptions{RepoID: repoID})
	if err != nil {
		t.Fatalf("list failures: %v", err)
	}
	if len(failures) != 0 {
		t.Fatalf("failures = %+v, want empty", failures)
	}
}

func TestPortablePruneDropsSyncFailureLedgerByDefault(t *testing.T) {
	ctx := context.Background()
	st, repoID, historicalMessage, currentMessage := seedPortableSyncFailureHistory(t, ctx)
	dbPath := st.Path()

	stats, err := st.PrunePortablePayloads(ctx, PortablePruneOptions{BodyChars: 64, Vacuum: false})
	if err != nil {
		t.Fatalf("portable prune: %v", err)
	}
	if st.tableExists(ctx, "sync_attempt_failures") {
		t.Fatal("portable prune retained sync_attempt_failures by default")
	}
	if !slices.Contains(stats.DroppedTables, "sync_attempt_failures") || stats.SyncFailuresIncluded || stats.SyncFailureErrorsRedacted != 0 || !stats.SyncFailureVacuumForced || !stats.Vacuumed {
		t.Fatalf("portable prune stats = %+v", stats)
	}
	var excluded string
	if err := st.DB().QueryRowContext(ctx, `select value from portable_metadata where key = 'excluded'`).Scan(&excluded); err != nil {
		t.Fatalf("read excluded metadata: %v", err)
	}
	if !strings.Contains(excluded, "sync_attempt_failures") {
		t.Fatalf("excluded metadata = %q", excluded)
	}
	failures, err := st.ListSyncAttemptFailures(ctx, SyncAttemptFailureListOptions{RepoID: repoID})
	if err != nil {
		t.Fatalf("list omitted portable sync failures: %v", err)
	}
	if len(failures) != 0 {
		t.Fatalf("omitted portable sync failures = %+v, want empty", failures)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close portable store: %v", err)
	}
	assertPortableFileDoesNotContain(t, dbPath, historicalMessage)
	assertPortableFileDoesNotContain(t, dbPath, currentMessage)
}

func TestPortablePruneCanIncludeRedactedSyncFailureLedger(t *testing.T) {
	ctx := context.Background()
	st, repoID, historicalMessage, currentMessage := seedPortableSyncFailureHistory(t, ctx)
	dbPath := st.Path()

	stats, err := st.PrunePortablePayloads(ctx, PortablePruneOptions{
		BodyChars:           64,
		Vacuum:              false,
		IncludeSyncFailures: true,
	})
	if err != nil {
		t.Fatalf("portable prune with failures: %v", err)
	}
	if !st.tableExists(ctx, "sync_attempt_failures") {
		t.Fatal("portable prune dropped opted-in sync_attempt_failures")
	}
	if !stats.SyncFailuresIncluded || stats.SyncFailureErrorsRedacted != 1 || !stats.SyncFailureVacuumForced || !stats.Vacuumed || slices.Contains(stats.DroppedTables, "sync_attempt_failures") {
		t.Fatalf("portable prune stats = %+v", stats)
	}
	failures, err := st.ListSyncAttemptFailures(ctx, SyncAttemptFailureListOptions{RepoID: repoID})
	if err != nil {
		t.Fatalf("list portable sync failures: %v", err)
	}
	if len(failures) != 1 || failures[0].ErrorMessage != portableSyncFailureErrorRedaction || strings.Contains(failures[0].ErrorMessage, currentMessage) {
		t.Fatalf("portable failures = %+v", failures)
	}
	var includes, excluded, capabilities string
	if err := st.DB().QueryRowContext(ctx, `select value from portable_metadata where key = 'includes'`).Scan(&includes); err != nil {
		t.Fatalf("read includes metadata: %v", err)
	}
	if err := st.DB().QueryRowContext(ctx, `select value from portable_metadata where key = 'excluded'`).Scan(&excluded); err != nil {
		t.Fatalf("read excluded metadata: %v", err)
	}
	if err := st.DB().QueryRowContext(ctx, `select value from portable_metadata where key = 'capabilities'`).Scan(&capabilities); err != nil {
		t.Fatalf("read capabilities metadata: %v", err)
	}
	if !strings.Contains(includes, "sync_attempt_failures") || strings.Contains(excluded, "sync_attempt_failures") || !strings.Contains(capabilities, "sync_failure_ledger_redacted") {
		t.Fatalf("portable metadata includes=%q excluded=%q capabilities=%q", includes, excluded, capabilities)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close portable store: %v", err)
	}
	assertPortableFileDoesNotContain(t, dbPath, historicalMessage)
	assertPortableFileDoesNotContain(t, dbPath, currentMessage)
}

func TestPortablePruneScrubsDeletedSyncFailureBytes(t *testing.T) {
	ctx := context.Background()
	fragment := "deleted-private-failure-sentinel-90"
	message := fragment + strings.Repeat(" private failure detail", 40)
	st, _ := seedPortableSyncFailure(t, ctx, message)
	dbPath := st.Path()

	if _, err := st.DB().ExecContext(ctx, `pragma wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatalf("checkpoint seeded store: %v", err)
	}
	conn, err := st.DB().Conn(ctx)
	if err != nil {
		t.Fatalf("open interruption fixture connection: %v", err)
	}
	if _, err := conn.ExecContext(ctx, `pragma secure_delete = off`); err != nil {
		t.Fatalf("disable secure deletion for interruption fixture: %v", err)
	}
	if _, err := conn.ExecContext(ctx, `delete from sync_attempt_failures`); err != nil {
		t.Fatalf("delete sync failure fixture: %v", err)
	}
	if _, err := conn.ExecContext(ctx, `pragma wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatalf("checkpoint interrupted store: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("close interruption fixture connection: %v", err)
	}
	data, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("read interrupted database bytes: %v", err)
	}
	if !bytes.Contains(data, []byte(fragment)) {
		t.Fatal("fixture does not retain deleted failure bytes")
	}

	stats, err := st.PrunePortablePayloads(ctx, PortablePruneOptions{BodyChars: 64, Vacuum: false})
	if err != nil {
		t.Fatalf("finish portable prune: %v", err)
	}
	if !stats.SyncFailureVacuumForced || !stats.Vacuumed {
		t.Fatalf("portable prune stats = %+v", stats)
	}
	if st.tableExists(ctx, "sync_attempt_failures") {
		t.Fatal("portable prune retained empty sync failure ledger")
	}
	var pending int
	if err := st.DB().QueryRowContext(ctx, `select count(*) from portable_metadata where key = ?`, portableSyncFailureScrubPendingKey).Scan(&pending); err != nil {
		t.Fatalf("read scrub marker: %v", err)
	}
	if pending != 0 {
		t.Fatalf("pending scrub marker count = %d, want 0", pending)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close portable store: %v", err)
	}
	assertPortableFileDoesNotContain(t, dbPath, fragment)
}

func TestPortablePruneFinishesPendingSyncFailureScrub(t *testing.T) {
	ctx := context.Background()
	st, _ := seedPortableSyncFailure(t, ctx, "private failure detail")
	if err := st.ensurePortableMetadata(ctx); err != nil {
		t.Fatalf("ensure portable metadata: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `insert into portable_metadata(key, value) values(?, '1')`, portableSyncFailureScrubPendingKey); err != nil {
		t.Fatalf("mark pending scrub: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `drop table sync_attempt_failures`); err != nil {
		t.Fatalf("drop scrubbed ledger: %v", err)
	}

	stats, err := st.PrunePortablePayloads(ctx, PortablePruneOptions{BodyChars: 64, Vacuum: false})
	if err != nil {
		t.Fatalf("finish pending portable prune: %v", err)
	}
	if !stats.SyncFailureVacuumForced || !stats.Vacuumed {
		t.Fatalf("portable prune stats = %+v", stats)
	}
	var pending int
	if err := st.DB().QueryRowContext(ctx, `select count(*) from portable_metadata where key = ?`, portableSyncFailureScrubPendingKey).Scan(&pending); err != nil {
		t.Fatalf("read scrub marker: %v", err)
	}
	if pending != 0 {
		t.Fatalf("pending scrub marker count = %d, want 0", pending)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close portable store: %v", err)
	}
}

func TestPortablePruneKeepsSyncFailureScrubPendingWhenCheckpointIsBusy(t *testing.T) {
	ctx := context.Background()
	st, repoID := seedPortableSyncFailure(t, ctx, "failure before reader")
	dbPath := st.Path()
	if _, err := st.DB().ExecContext(ctx, `pragma wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatalf("checkpoint seeded store: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `pragma busy_timeout = 1`); err != nil {
		t.Fatalf("set checkpoint busy timeout: %v", err)
	}

	reader, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open checkpoint-blocking reader: %v", err)
	}
	defer reader.Close()
	tx, err := reader.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("begin checkpoint-blocking read: %v", err)
	}
	defer tx.Rollback()
	var count int
	if err := tx.QueryRowContext(ctx, `select count(*) from sync_attempt_failures`).Scan(&count); err != nil {
		t.Fatalf("establish checkpoint-blocking snapshot: %v", err)
	}
	if count != 1 {
		t.Fatalf("failure count = %d, want 1", count)
	}
	if _, err := st.RecordSyncAttemptFailure(ctx, SyncAttemptFailure{
		RepoID: repoID, Number: 90, Operation: "pull_request_details", ErrorClass: "network",
		ErrorMessage: "failure after reader", LastSeenAt: "2026-07-16T00:05:00Z",
	}); err != nil {
		t.Fatalf("record failure after reader snapshot: %v", err)
	}

	_, err = st.PrunePortablePayloads(ctx, PortablePruneOptions{BodyChars: 64, Vacuum: false})
	if err == nil || !strings.Contains(err.Error(), "checkpoint wal: busy") {
		t.Fatalf("portable prune error = %v, want busy checkpoint", err)
	}
	var pending string
	if err := st.DB().QueryRowContext(ctx, `select value from portable_metadata where key = ?`, portableSyncFailureScrubPendingKey).Scan(&pending); err != nil {
		t.Fatalf("read retained scrub marker: %v", err)
	}
	if pending != "1" {
		t.Fatalf("pending scrub marker = %q, want 1", pending)
	}
}

func seedPortableSyncFailureHistory(t *testing.T, ctx context.Context) (*Store, int64, string, string) {
	t.Helper()
	historicalMessage := strings.Repeat("historical private endpoint detail ", 1024)
	currentMessage := "current private endpoint detail"
	st, repoID := seedPortableSyncFailure(t, ctx, historicalMessage)
	if _, err := st.RecordSyncAttemptFailure(ctx, SyncAttemptFailure{
		RepoID: repoID, Number: 90, Operation: "pull_request_details", ErrorClass: "network",
		ErrorMessage: currentMessage, LastSeenAt: "2026-07-16T00:05:00Z",
	}); err != nil {
		_ = st.Close()
		t.Fatalf("record sync failure retry: %v", err)
	}
	return st, repoID, historicalMessage, currentMessage
}

func seedPortableSyncFailure(t *testing.T, ctx context.Context, message string) (*Store, int64) {
	t.Helper()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	repoID, err := st.UpsertRepository(ctx, Repository{
		Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl", RawJSON: "{}", UpdatedAt: "2026-07-16T00:00:00Z",
	})
	if err != nil {
		_ = st.Close()
		t.Fatalf("upsert repository: %v", err)
	}
	if _, err := st.RecordSyncAttemptFailure(ctx, SyncAttemptFailure{
		RepoID: repoID, Number: 90, Operation: "pull_request_details", ErrorClass: "network",
		ErrorMessage: message, FirstSeenAt: "2026-07-16T00:00:00Z", LastSeenAt: "2026-07-16T00:00:00Z",
	}); err != nil {
		_ = st.Close()
		t.Fatalf("record sync failure: %v", err)
	}
	return st, repoID
}

func assertPortableFileDoesNotContain(t *testing.T, path, value string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read portable database bytes: %v", err)
	}
	if bytes.Contains(data, []byte(value)) {
		t.Fatalf("portable database retains scrubbed value %q", value)
	}
}
