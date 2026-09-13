package syncer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	gh "github.com/openclaw/gitcrawl/internal/github"
	"github.com/openclaw/gitcrawl/internal/store"
)

type partialGitHub struct {
	fakeGitHub
	failNumber int
	operation  string
	calls      []int
	cancel     context.CancelFunc
}

func (f *partialGitHub) fail(number int, operation string) error {
	if number != f.failNumber || operation != f.operation {
		return nil
	}
	if f.cancel != nil {
		f.cancel()
		return context.Canceled
	}
	return fmt.Errorf("%s unavailable", operation)
}

func (f *partialGitHub) GetIssue(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) (map[string]any, error) {
	f.calls = append(f.calls, number)
	if err := f.fail(number, "issue"); err != nil {
		return nil, err
	}
	row, err := f.fakeGitHub.GetIssue(ctx, owner, repo, number, reporter)
	row["id"], row["number"] = number, number
	return row, err
}

func (f *partialGitHub) ListIssueComments(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) ([]map[string]any, error) {
	if err := f.fail(number, "issue_comments"); err != nil {
		return nil, err
	}
	return f.fakeGitHub.ListIssueComments(ctx, owner, repo, number, reporter)
}

func (f *partialGitHub) ListPullReviews(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) ([]map[string]any, error) {
	if err := f.fail(number, "pull_reviews"); err != nil {
		return nil, err
	}
	return f.fakeGitHub.ListPullReviews(ctx, owner, repo, number, reporter)
}

func (f *partialGitHub) ListPullReviewComments(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) ([]map[string]any, error) {
	if err := f.fail(number, "pull_review_comments"); err != nil {
		return nil, err
	}
	return f.fakeGitHub.ListPullReviewComments(ctx, owner, repo, number, reporter)
}

func (f *partialGitHub) ListPullReviewThreads(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) ([]map[string]any, error) {
	if err := f.fail(number, "pull_review_threads"); err != nil {
		return nil, err
	}
	return f.fakeGitHub.ListPullReviewThreads(ctx, owner, repo, number, reporter)
}

func (f *partialGitHub) GetPull(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) (map[string]any, error) {
	if err := f.fail(number, "pull_request_metadata"); err != nil {
		return nil, err
	}
	return f.fakeGitHub.GetPull(ctx, owner, repo, number, reporter)
}

func (f *partialGitHub) ListPullFiles(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) ([]map[string]any, error) {
	if err := f.fail(number, "pull_request_details"); err != nil {
		return nil, err
	}
	return f.fakeGitHub.ListPullFiles(ctx, owner, repo, number, reporter)
}

func TestSyncPartialItemFailuresRetainHealthySiblingsAndRetry(t *testing.T) {
	for _, operation := range []string{
		"issue", "issue_comments", "pull_reviews", "pull_review_comments",
		"pull_review_threads", "pull_request_metadata", "pull_request_details",
	} {
		for _, numbers := range [][]int{{8, 7, 9}, {7, 8, 9}, {7, 9, 8}} {
			t.Run(fmt.Sprintf("%s/%v", operation, numbers), func(t *testing.T) {
				ctx := context.Background()
				st, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
				if err != nil {
					t.Fatal(err)
				}
				defer st.Close()
				client := &partialGitHub{operation: operation, failNumber: 8}
				s := New(client, st)
				opts := Options{Owner: "fixture", Repo: "repo", Numbers: numbers, IncludeComments: true,
					IncludePRDetails: operation != "pull_request_metadata", IncludePRMetadata: true}
				stats, err := s.Sync(ctx, opts)
				if err == nil || !strings.Contains(err.Error(), operation+" unavailable") {
					t.Fatalf("failure=%v", err)
				}
				if stats.ThreadsSynced != 2 || stats.IssuesSynced != 2 || stats.CommentsSynced != 1 ||
					stats.EvidenceObserved != 2 || stats.ClosedSweepThrough != "" {
					t.Fatalf("committed counts=%+v", stats)
				}
				repo, err := st.RepositoryByFullName(ctx, "fixture/repo")
				if err != nil {
					t.Fatal(err)
				}
				failures, err := st.ListSyncAttemptFailures(ctx, store.SyncAttemptFailureListOptions{RepoID: repo.ID})
				if err != nil || len(failures) != 1 || failures[0].Operation != operation || failures[0].Number != 8 {
					t.Fatalf("failure ledger=%+v err=%v", failures, err)
				}
				if (failures[0].ThreadID == 0) != (operation == "issue") {
					t.Fatalf("only an observed parent can own a thread ID: %+v", failures[0])
				}
				assertTableRowCount(t, st, "documents", 2)
				assertTableRowCount(t, st, "thread_revisions", 2)
				assertNoSuccessfulSync(t, st, repo.ID)
				client.operation = ""
				opts.Numbers = []int{8}
				if _, err := s.Sync(ctx, opts); err != nil {
					t.Fatal(err)
				}
				failures, err = st.ListSyncAttemptFailures(ctx, store.SyncAttemptFailureListOptions{RepoID: repo.ID})
				if err != nil || len(failures) != 0 {
					t.Fatalf("successful retry unresolved=%+v err=%v", failures, err)
				}
			})
		}
	}
}

func assertNoSuccessfulSync(t *testing.T, st *store.Store, repoID int64) {
	t.Helper()
	ctx := context.Background()
	last, err := st.LastSuccessfulSyncAt(ctx, repoID)
	if err != nil || !last.IsZero() {
		t.Fatalf("partial run certified sync freshness=%v err=%v", last, err)
	}
	last, err = st.LastSuccessfulListSyncAt(ctx, repoID, "open")
	if err != nil || !last.IsZero() {
		t.Fatalf("partial run certified list freshness=%v err=%v", last, err)
	}
	runs, err := st.SuccessfulListSyncRuns(ctx, repoID, "open")
	if err != nil || len(runs) != 0 {
		t.Fatalf("partial run certified coverage=%+v err=%v", runs, err)
	}
}

func TestSyncPartialPersistenceRollsBackOnlyFailedItem(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if _, err := st.DB().ExecContext(ctx, `
		create trigger reject_middle_fingerprint before insert on thread_fingerprints
		when (select t.number from threads t join thread_revisions r on r.thread_id = t.id where r.id = new.thread_revision_id) = 8
		begin select raise(abort, 'middle fingerprint rejected'); end`); err != nil {
		t.Fatal(err)
	}
	stats, err := New(&partialGitHub{}, st).Sync(ctx, Options{
		Owner: "fixture", Repo: "repo", Numbers: []int{7, 8, 9}, IncludeComments: true, IncludePRDetails: true,
	})
	if err == nil || !strings.Contains(err.Error(), "middle fingerprint rejected") {
		t.Fatalf("failure=%v", err)
	}
	if stats.ThreadsSynced != 2 || stats.EvidenceObserved != 2 || stats.RevisionsCreated != 2 ||
		stats.FingerprintsUpserted != 2 || stats.PRDetailsSynced != 0 {
		t.Fatalf("rolled-back item counted=%+v", stats)
	}
	for _, table := range []string{"threads", "documents", "thread_revisions", "thread_fingerprints"} {
		assertTableRowCount(t, st, table, 2)
	}
	for _, table := range []string{"pull_request_details", "pull_request_review_thread_syncs"} {
		assertTableRowCount(t, st, table, 0)
	}
	var reservations int
	if err := st.DB().QueryRowContext(ctx, `select count(*) from thread_child_observation_reservations where thread_id not in (select id from threads)`).Scan(&reservations); err != nil || reservations != 0 {
		t.Fatalf("orphan child reservations=%d err=%v", reservations, err)
	}
	repo, err := st.RepositoryByFullName(ctx, "fixture/repo")
	if err != nil {
		t.Fatal(err)
	}
	assertNoSuccessfulSync(t, st, repo.ID)
}

func TestSyncFailureResolutionRequiresObservedFamilies(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	client := &partialGitHub{}
	s := New(client, st)
	opts := Options{Owner: "fixture", Repo: "repo", Numbers: []int{8}}
	if _, err := s.Sync(ctx, opts); err != nil {
		t.Fatal(err)
	}
	repo, err := st.RepositoryByFullName(ctx, "fixture/repo")
	if err != nil {
		t.Fatal(err)
	}
	operations := []string{"issue", "issue_comments", "pull_reviews", "pull_review_comments", "pull_review_threads", "pull_request_metadata", "pull_request_details"}
	for _, operation := range operations {
		if _, err := st.RecordSyncAttemptFailure(ctx, store.SyncAttemptFailure{
			RepoID: repo.ID, Number: 8, Operation: operation, ErrorMessage: "unavailable", LastSeenAt: time.Now().UTC().Format(time.RFC3339Nano),
		}); err != nil {
			t.Fatal(err)
		}
	}
	opts.IncludePRDetails = true
	if _, err := s.Sync(ctx, opts); err != nil {
		t.Fatal(err)
	}
	failures, err := st.ListSyncAttemptFailures(ctx, store.SyncAttemptFailureListOptions{RepoID: repo.ID})
	if err != nil {
		t.Fatal(err)
	}
	var remaining []string
	for _, failure := range failures {
		remaining = append(remaining, failure.Operation)
	}
	slices.Sort(remaining)
	if !slices.Equal(remaining, []string{"issue_comments", "pull_review_comments", "pull_reviews"}) {
		t.Fatalf("unobserved comments resolved=%v", remaining)
	}
	opts.IncludePRDetails, opts.IncludeComments = false, true
	if _, err := s.Sync(ctx, opts); err != nil {
		t.Fatal(err)
	}
	failures, err = st.ListSyncAttemptFailures(ctx, store.SyncAttemptFailureListOptions{RepoID: repo.ID})
	if err != nil || len(failures) != 0 {
		t.Fatalf("observed comments unresolved=%+v err=%v", failures, err)
	}
}

func TestSyncCancellationRecordsFailureAndStopsAcquisition(t *testing.T) {
	background := context.Background()
	ctx, cancel := context.WithCancel(background)
	defer cancel()
	st, err := store.Open(background, filepath.Join(t.TempDir(), "archive.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	client := &partialGitHub{failNumber: 8, operation: "issue", cancel: cancel}
	_, err = New(client, st).Sync(ctx, Options{Owner: "fixture", Repo: "repo", Numbers: []int{7, 8, 9}})
	if !errors.Is(err, context.Canceled) || !slices.Equal(client.calls, []int{7, 8}) {
		t.Fatalf("cancellation=%v calls=%v", err, client.calls)
	}
	repo, err := st.RepositoryByFullName(background, "fixture/repo")
	if err != nil {
		t.Fatal(err)
	}
	failures, err := st.ListSyncAttemptFailures(background, store.SyncAttemptFailureListOptions{RepoID: repo.ID})
	if err != nil || len(failures) != 1 || failures[0].ErrorClass != "context_canceled" {
		t.Fatalf("cancel failure=%+v err=%v", failures, err)
	}
	assertNoSuccessfulSync(t, st, repo.ID)
}

type cancelAfterCommitWriter struct{ cancel context.CancelFunc }

func (w cancelAfterCommitWriter) Write(data []byte) (int, error) {
	if strings.Contains(string(data), "state=progress done=1 ") {
		w.cancel()
	}
	return len(data), nil
}

func TestSyncCancellationRetainsOnlyCommittedItems(t *testing.T) {
	for _, afterCommit := range []bool{false, true} {
		t.Run(fmt.Sprint(afterCommit), func(t *testing.T) {
			background := context.Background()
			ctx, cancel := context.WithCancel(background)
			defer cancel()
			st, err := store.Open(background, filepath.Join(t.TempDir(), "archive.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			s := New(&partialGitHub{}, st)
			opts := Options{Owner: "fixture", Repo: "repo", Numbers: []int{7, 9}, IncludeComments: true}
			want := 0
			if afterCommit {
				want = 1
				opts.Logger = slog.New(slog.NewTextHandler(cancelAfterCommitWriter{cancel}, nil))
			} else {
				s.beforePersist = func() {
					calls := 0
					s.now = func() time.Time {
						calls++
						// Repository and parent timestamps precede enrichment.
						// Cancel after the parent and comments were written.
						if calls == 3 {
							cancel()
						}
						return time.Now().UTC()
					}
				}
			}
			stats, err := s.Sync(ctx, opts)
			if !errors.Is(err, context.Canceled) || stats.ThreadsSynced != want || stats.EvidenceObserved != want {
				t.Fatalf("cancellation counts=%+v err=%v", stats, err)
			}
			for _, table := range []string{"threads", "comments", "documents", "thread_revisions", "thread_fingerprints"} {
				assertTableRowCount(t, st, table, want)
			}
		})
	}
}

func TestSyncBusyCommitRetryDoesNotDoubleCount(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "archive.db")
	st, err := store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	// A rollback-journal reader blocks COMMIT after every item write succeeded.
	// This exercises the driver's rollback and WithTx's complete callback retry.
	if _, err := st.DB().ExecContext(ctx, `pragma journal_mode = delete; pragma busy_timeout = 1`); err != nil {
		t.Fatal(err)
	}
	reader, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	s := New(&partialGitHub{}, st)
	var readTx *sql.Tx
	calls := 0
	s.beforePersist = func() {
		readTx, err = reader.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		var count int
		if err := readTx.QueryRowContext(ctx, `select count(*) from repositories`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		s.now = func() time.Time {
			calls++
			// Four reads cover repository, parent, enrichment, and resolution.
			// The fifth starts the retried item after the first COMMIT was busy.
			if calls == 5 {
				if err := readTx.Rollback(); err != nil {
					t.Fatal(err)
				}
			}
			return time.Now().UTC()
		}
	}
	defer func() {
		if readTx != nil {
			_ = readTx.Rollback()
		}
	}()
	stats, err := s.Sync(ctx, Options{Owner: "fixture", Repo: "repo", Numbers: []int{7}, IncludeComments: true})
	if err != nil {
		t.Fatal(err)
	}
	if calls < 9 || stats.ThreadsSynced != 1 || stats.CommentsSynced != 1 ||
		stats.EvidenceObserved != 1 || stats.RevisionsCreated != 1 || stats.FingerprintsUpserted != 1 {
		t.Fatalf("retry counts=%+v clock reads=%d", stats, calls)
	}
	for _, table := range []string{"threads", "comments", "documents", "thread_revisions", "thread_fingerprints"} {
		assertTableRowCount(t, st, table, 1)
	}
}

type partialSharedHeadGitHub struct{ *sameSyncSharedHeadGitHub }

func (f partialSharedHeadGitHub) GetIssue(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) (map[string]any, error) {
	if number == 10 {
		return nil, errors.New("issue unavailable")
	}
	return f.sameSyncSharedHeadGitHub.GetIssue(ctx, owner, repo, number, reporter)
}

func TestSyncPartialBatchPreservesSharedHeadConsolidation(t *testing.T) {
	for _, fixture := range []struct {
		name    string
		client  sameSyncSharedHeadGitHub
		wantIDs []string
	}{
		{"subset first", sameSyncSharedHeadGitHub{subsetFirst: true}, []string{"900", "901"}},
		{"superset first", sameSyncSharedHeadGitHub{}, []string{"900", "901"}},
		{"later deletion", sameSyncSharedHeadGitHub{verifiedDeletion: true}, []string{"900"}},
	} {
		for _, numbers := range [][]int{{10, 8, 9}, {8, 10, 9}, {8, 9, 10}} {
			t.Run(fmt.Sprintf("%s/%v", fixture.name, numbers), func(t *testing.T) {
				ctx := context.Background()
				st, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
				if err != nil {
					t.Fatal(err)
				}
				defer st.Close()
				client := fixture.client
				stats, err := New(partialSharedHeadGitHub{&client}, st).Sync(ctx, Options{
					Owner: "fixture", Repo: "repo", Numbers: numbers, IncludePRDetails: true,
				})
				if err == nil || stats.PRDetailsSynced != 2 || stats.WorkflowRunsSynced != len(fixture.wantIDs) {
					t.Fatalf("partial shared-head stats=%+v err=%v", stats, err)
				}
				repo, err := st.RepositoryByFullName(ctx, "fixture/repo")
				if err != nil {
					t.Fatal(err)
				}
				runs, err := st.ListWorkflowRuns(ctx, repo.ID, store.WorkflowRunListOptions{HeadSHA: "same-sync-head", Limit: -1})
				if err != nil {
					t.Fatal(err)
				}
				var ids []string
				for _, run := range runs {
					ids = append(ids, run.RunID)
				}
				slices.Sort(ids)
				if !slices.Equal(ids, fixture.wantIDs) {
					t.Fatalf("shared-head rows=%v want=%v", ids, fixture.wantIDs)
				}
				assertWorkflowRunReservation(t, ctx, st, repo.ID, "same-sync-head", 1)
				assertNoSuccessfulSync(t, st, repo.ID)
			})
		}
	}
}

func TestSyncSharedHeadSurvivesSiblingTransactionFailure(t *testing.T) {
	for _, failedNumber := range []int{8, 9} {
		t.Run(fmt.Sprint(failedNumber), func(t *testing.T) {
			ctx := context.Background()
			st, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			if _, err := st.DB().ExecContext(ctx, fmt.Sprintf(`
				create trigger reject_sibling_document before insert on documents
				when (select number from threads where id = new.thread_id) = %d
				begin select raise(abort, 'sibling document rejected'); end`, failedNumber)); err != nil {
				t.Fatal(err)
			}
			stats, err := New(&sameSyncSharedHeadGitHub{}, st).Sync(ctx, Options{
				Owner: "fixture", Repo: "repo", Numbers: []int{8, 9}, IncludePRDetails: true,
			})
			if err == nil || stats.PRDetailsSynced != 1 || stats.WorkflowRunsSynced != 2 {
				t.Fatalf("shared-head transaction counts=%+v err=%v", stats, err)
			}
			repo, err := st.RepositoryByFullName(ctx, "fixture/repo")
			if err != nil {
				t.Fatal(err)
			}
			threads, err := st.ListThreads(ctx, repo.ID, true)
			if err != nil || len(threads) != 1 || threads[0].Number == failedNumber {
				t.Fatalf("failed sibling retained=%+v err=%v", threads, err)
			}
			runs, err := st.ListWorkflowRuns(ctx, repo.ID, store.WorkflowRunListOptions{HeadSHA: "same-sync-head", Limit: -1})
			if err != nil || len(runs) != 2 {
				t.Fatalf("consolidated workflows lost=%+v err=%v", runs, err)
			}
			assertWorkflowRunReservation(t, ctx, st, repo.ID, "same-sync-head", 1)
			assertNoSuccessfulSync(t, st, repo.ID)
		})
	}
}
