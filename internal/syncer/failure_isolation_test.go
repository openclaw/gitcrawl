package syncer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	gh "github.com/openclaw/gitcrawl/internal/github"
	"github.com/openclaw/gitcrawl/internal/store"
)

type isolatedWorkflowGitHub struct {
	sameSyncSharedHeadGitHub
	failure string
}

func (f *isolatedWorkflowGitHub) GetIssue(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) (map[string]any, error) {
	if number == 7 {
		return f.fakeGitHub.GetIssue(ctx, owner, repo, number, reporter)
	}
	return f.sameSyncSharedHeadGitHub.GetIssue(ctx, owner, repo, number, reporter)
}

func (f *isolatedWorkflowGitHub) GetPull(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) (map[string]any, error) {
	row, err := f.sameSyncSharedHeadGitHub.GetPull(ctx, owner, repo, number, reporter)
	if number == 10 {
		row["head"].(map[string]any)["sha"] = "other-head"
	}
	return row, err
}

func (f *isolatedWorkflowGitHub) ListWorkflowRuns(ctx context.Context, owner, repo string, options gh.ListWorkflowRunsOptions, reporter gh.Reporter) ([]map[string]any, error) {
	if options.HeadSHA == "other-head" {
		return []map[string]any{{"id": 1000, "head_sha": "other-head", "created_at": "2026-07-12T00:00:00Z", "updated_at": "2026-07-12T00:01:00Z"}}, nil
	}
	rows, err := f.sameSyncSharedHeadGitHub.ListWorkflowRuns(ctx, owner, repo, options, reporter)
	if f.failure == "conflict" && f.runCalls == 2 {
		for _, row := range rows {
			if jsonID(row["id"]) == "901" {
				row["name"] = "conflicting same-time observation"
			}
		}
	}
	return rows, err
}

func (f *isolatedWorkflowGitHub) GetWorkflowRun(ctx context.Context, owner, repo, runID string, reporter gh.Reporter) (map[string]any, error) {
	if f.failure == "lookup" {
		f.lookupCalls++
		return nil, errors.New("exact workflow lookup unavailable")
	}
	return f.sameSyncSharedHeadGitHub.GetWorkflowRun(ctx, owner, repo, runID, reporter)
}

func TestSyncConsolidationFailureRetainsUnrelatedAcquisitions(t *testing.T) {
	for _, failure := range []string{"conflict", "lookup"} {
		for _, numbers := range [][]int{{7, 8, 9, 10}, {10, 9, 8, 7}} {
			t.Run(fmt.Sprintf("%s/%v", failure, numbers), func(t *testing.T) {
				ctx := context.Background()
				st, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
				if err != nil {
					t.Fatal(err)
				}
				defer st.Close()
				client := &isolatedWorkflowGitHub{failure: failure}
				opts := Options{Owner: "fixture", Repo: "repo", Numbers: numbers, IncludePRDetails: true}
				stats, err := New(client, st).Sync(ctx, opts)
				if err == nil || stats.ThreadsSynced != 2 || stats.PRDetailsSynced != 1 || stats.WorkflowRunsSynced != 1 {
					t.Fatalf("unrelated acquisitions lost: %+v err=%v", stats, err)
				}
				repo, err := st.RepositoryByFullName(ctx, "fixture/repo")
				if err != nil {
					t.Fatal(err)
				}
				failures, err := st.ListSyncAttemptFailures(ctx, store.SyncAttemptFailureListOptions{RepoID: repo.ID})
				if err != nil || len(failures) != 2 {
					t.Fatalf("affected group ledger=%+v err=%v", failures, err)
				}
				for _, failed := range failures {
					if !slices.Contains([]int{8, 9}, failed.Number) || failed.Operation != "pull_request_details" || failed.ThreadID == 0 {
						t.Fatalf("wrong group failure=%+v", failed)
					}
				}
				runs, err := st.ListWorkflowRuns(ctx, repo.ID, store.WorkflowRunListOptions{Limit: -1})
				if err != nil || len(runs) != 1 || runs[0].HeadSHA != "other-head" {
					t.Fatalf("failed shared head persisted=%+v err=%v", runs, err)
				}
				assertTableRowCount(t, st, "documents", 2)
				assertNoSuccessfulSync(t, st, repo.ID)
				opts.Numbers = []int{8, 9}
				if _, err := New(&isolatedWorkflowGitHub{}, st).Sync(ctx, opts); err != nil {
					t.Fatal(err)
				}
				failures, err = st.ListSyncAttemptFailures(ctx, store.SyncAttemptFailureListOptions{RepoID: repo.ID})
				if err != nil || len(failures) != 0 {
					t.Fatalf("completed group retry unresolved=%+v err=%v", failures, err)
				}
			})
		}
	}
}

type quotaWorkflowLookupGitHub struct {
	fakeGitHub
	calls int
}

func (f *quotaWorkflowLookupGitHub) GetWorkflowRun(context.Context, string, string, string, gh.Reporter) (map[string]any, error) {
	f.calls++
	return nil, &gh.RateLimitReserveError{Reserve: 10, RateLimit: gh.RateLimitSnapshot{Resource: "core", Remaining: 10}}
}

func TestConsolidationQuotaStopDoesNotAttemptOrBlameLaterGroups(t *testing.T) {
	var payloads []threadSyncPayload
	for group, head := range []string{"first", "later", "offline"} {
		row := func(id int) map[string]any {
			return map[string]any{"id": id, "head_sha": head, "created_at": "2026-07-12T00:00:00Z", "updated_at": "2026-07-12T00:01:00Z"}
		}
		for observation := 0; observation < 2; observation++ {
			rows := []map[string]any{row(10*group + 1)}
			if observation == 0 && head != "offline" {
				rows = append(rows, row(10*group+2))
			}
			payloads = append(payloads, threadSyncPayload{hasPullDetails: true, pullDetails: pullRequestDetailRows{
				pull: map[string]any{"head": map[string]any{"sha": head}}, runsRaw: rows,
				workflowSnapshotFresh: true, workflowSourceUpdatedAt: "2026-07-12T00:01:00Z",
				workflowObservationOrder: len(payloads) + 1,
			}})
		}
	}
	client := &quotaWorkflowLookupGitHub{}
	failures := New(client, nil).consolidateWorkflowSnapshots(context.Background(), Options{}, payloads, true)
	if client.calls != 1 || len(failures) != 4 {
		t.Fatalf("continued verification after quota: calls=%d failures=%v", client.calls, failures)
	}
	for _, index := range []int{0, 1} {
		var reserveErr *gh.RateLimitReserveError
		if !errors.As(failures[index], &reserveErr) {
			t.Fatalf("actual group failure lost at %d: %v", index, failures[index])
		}
	}
	for _, index := range []int{2, 3} {
		if cause, excluded := failures[index]; !excluded || cause != nil {
			t.Fatalf("unattempted verification must be excluded without a failure: index=%d excluded=%v cause=%v", index, excluded, cause)
		}
	}
	for _, index := range []int{4, 5} {
		if _, excluded := failures[index]; excluded {
			t.Fatalf("already complete offline group excluded: %d", index)
		}
	}
}

func TestSyncNativeQuotaStopPreservesCompletedPayloadsWithoutFurtherRequests(t *testing.T) {
	for _, phase := range []string{"parents", "comments"} {
		t.Run(phase, func(t *testing.T) {
			ctx := context.Background()
			st, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			var requests []string
			rateCalls, stopAt := 0, 3
			if phase == "comments" {
				stopAt = 6
			}
			resetAt := time.Now().Add(time.Hour).Unix()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests = append(requests, r.URL.Path)
				if r.URL.Path == "/rate_limit" {
					rateCalls++
					remaining := 100
					if rateCalls >= stopAt {
						remaining = 10
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"resources": map[string]any{
						"core": map[string]any{"limit": 5000, "remaining": remaining, "reset": resetAt},
					}})
					return
				}
				switch {
				case r.URL.Path == "/repos/fixture/repo":
					_ = json.NewEncoder(w).Encode(map[string]any{"id": 123})
				case r.URL.Path == "/repos/fixture/repo/issues/7/comments":
					_ = json.NewEncoder(w).Encode([]map[string]any{{"id": 11, "body": "retained", "updated_at": "2026-07-12T00:00:00Z"}})
				case strings.HasPrefix(r.URL.Path, "/repos/fixture/repo/issues/"):
					number, err := strconv.Atoi(strings.TrimPrefix(r.URL.Path, "/repos/fixture/repo/issues/"))
					if err != nil {
						t.Errorf("unexpected request after quota stop: %s", r.URL.Path)
						http.Error(w, "unexpected request", http.StatusBadRequest)
						return
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"id": number, "number": number, "state": "open",
						"title": "fixture", "body": "retained", "created_at": "2026-07-12T00:00:00Z", "updated_at": "2026-07-12T00:00:00Z"})
				default:
					t.Errorf("unexpected request: %s", r.URL.Path)
					http.Error(w, "unexpected request", http.StatusBadRequest)
				}
			}))
			defer server.Close()
			client := gh.New(gh.Options{BaseURL: server.URL, RateLimitReserve: 10})
			stats, err := New(client, st).Sync(ctx, Options{Owner: "fixture", Repo: "repo", Numbers: []int{7, 8, 9}, IncludeComments: phase == "comments"})
			var reserveErr *gh.RateLimitReserveError
			if !errors.As(err, &reserveErr) || reserveErr.Reserve != 10 || reserveErr.RateLimit.Remaining != 10 {
				t.Fatalf("original quota error lost: %v", err)
			}
			if stats.ThreadsSynced != 1 || rateCalls != stopAt || len(requests) != 2*stopAt-1 || requests[len(requests)-1] != "/rate_limit" {
				t.Fatalf("continued after quota stop: stats=%+v rate=%d requests=%v", stats, rateCalls, requests)
			}
			repo, err := st.RepositoryByFullName(ctx, "fixture/repo")
			if err != nil {
				t.Fatal(err)
			}
			failures, err := st.ListSyncAttemptFailures(ctx, store.SyncAttemptFailureListOptions{RepoID: repo.ID})
			operation := "issue"
			if phase == "comments" {
				operation = "issue_comments"
			}
			if err != nil || len(failures) != 1 || failures[0].Number != 8 || failures[0].Operation != operation {
				t.Fatalf("unattempted item recorded=%+v err=%v", failures, err)
			}
			assertNoSuccessfulSync(t, st, repo.ID)
		})
	}
}

func TestSyncPersistenceFailureLedgerSurvivesRollbackAndRequiresObservedFamilies(t *testing.T) {
	for _, fixture := range []struct {
		number   int
		existing bool
	}{{7, false}, {7, true}, {8, false}, {8, true}} {
		t.Run(fmt.Sprint(fixture), func(t *testing.T) {
			ctx := context.Background()
			st, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			s := New(&partialGitHub{}, st)
			opts := Options{Owner: "fixture", Repo: "repo", Numbers: []int{fixture.number}}
			if fixture.existing {
				if _, err := s.Sync(ctx, opts); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := st.DB().ExecContext(ctx, `create trigger reject_fingerprint before insert on thread_fingerprints
				begin select raise(abort, 'fingerprint rejected'); end`); err != nil {
				t.Fatal(err)
			}
			opts.IncludeComments, opts.IncludePRDetails = true, true
			stats, err := s.Sync(ctx, opts)
			if err == nil || !strings.Contains(err.Error(), "fingerprint rejected") || stats.ThreadsSynced != 0 {
				t.Fatalf("persistence failure=%+v err=%v", stats, err)
			}
			repo, err := st.RepositoryByFullName(ctx, "fixture/repo")
			if err != nil {
				t.Fatal(err)
			}
			failures, err := st.ListSyncAttemptFailures(ctx, store.SyncAttemptFailureListOptions{RepoID: repo.ID})
			operations := []string{"issue", "issue_comments"}
			if fixture.number == 8 {
				operations = append(operations, "pull_request_metadata", "pull_request_details",
					"pull_review_threads", "pull_reviews", "pull_review_comments")
			}
			slices.Sort(operations)
			assertFailureOperations := func(failures []store.SyncAttemptFailure, want []string) {
				t.Helper()
				var got []string
				for _, failure := range failures {
					got = append(got, failure.Operation)
				}
				slices.Sort(got)
				if !slices.Equal(got, want) {
					t.Fatalf("failure operations=%v want=%v", got, want)
				}
			}
			if err != nil {
				t.Fatalf("rollback failure ledger=%+v err=%v", failures, err)
			}
			assertFailureOperations(failures, operations)
			for _, failure := range failures {
				if (failure.ThreadID != 0) != fixture.existing || !strings.Contains(failure.ErrorMessage, "fingerprint rejected") {
					t.Fatalf("rollback failure parent/cause=%+v", failure)
				}
			}
			coverage, err := st.ArchiveCoverage(ctx, store.ArchiveCoverageOptions{})
			if err != nil || coverage.Totals.KnownFailedHydrations == nil || *coverage.Totals.KnownFailedHydrations != len(operations) {
				t.Fatalf("inventory hides persistence failure: %+v err=%v", coverage.Totals, err)
			}
			assertTableRowCount(t, st, "thread_revisions", 0)
			if _, err := s.Sync(ctx, opts); err == nil {
				t.Fatal("failed retry unexpectedly succeeded")
			}
			failures, err = st.ListSyncAttemptFailures(ctx, store.SyncAttemptFailureListOptions{RepoID: repo.ID})
			if err != nil {
				t.Fatalf("rolled-back retry cleared persistence failure=%+v err=%v", failures, err)
			}
			assertFailureOperations(failures, operations)
			for _, failure := range failures {
				if failure.RetryCount != 1 || failure.ResolvedAt != "" {
					t.Fatalf("rolled-back retry resolved or failed to count=%+v", failure)
				}
			}
			shallow := opts
			shallow.IncludeComments, shallow.IncludePRDetails, shallow.IncludePRMetadata = false, false, true
			if _, err := s.Sync(ctx, shallow); err != nil {
				t.Fatal(err)
			}
			failures, err = st.ListSyncAttemptFailures(ctx, store.SyncAttemptFailureListOptions{RepoID: repo.ID})
			if err != nil {
				t.Fatal(err)
			}
			remaining := []string{"issue_comments"}
			if fixture.number == 8 {
				remaining = append(remaining, "pull_request_details", "pull_review_comments", "pull_review_threads", "pull_reviews")
			}
			assertFailureOperations(failures, remaining)
			if _, err := st.DB().ExecContext(ctx, `drop trigger reject_fingerprint`); err != nil {
				t.Fatal(err)
			}
			shallow.IncludeComments, shallow.IncludePRMetadata = true, false
			if _, err := s.Sync(ctx, shallow); err != nil {
				t.Fatal(err)
			}
			failures, err = st.ListSyncAttemptFailures(ctx, store.SyncAttemptFailureListOptions{RepoID: repo.ID})
			if err != nil {
				t.Fatal(err)
			}
			remaining = nil
			if fixture.number == 8 {
				remaining = []string{"pull_request_details", "pull_review_threads"}
			}
			assertFailureOperations(failures, remaining)
			if _, err := s.Sync(ctx, opts); err != nil {
				t.Fatal(err)
			}
			failures, err = st.ListSyncAttemptFailures(ctx, store.SyncAttemptFailureListOptions{RepoID: repo.ID, IncludeResolved: true})
			if err != nil {
				t.Fatalf("complete retry failed to resolve=%+v err=%v", failures, err)
			}
			assertFailureOperations(failures, operations)
			for _, failure := range failures {
				if failure.ResolvedAt == "" {
					t.Fatalf("complete retry left unresolved family=%+v", failure)
				}
			}
		})
	}
}

func TestSyncPersistenceBookkeepingFailurePreservesOriginalErrorAndPriorCommit(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if _, err := st.DB().ExecContext(ctx, `
		create trigger reject_second before insert on documents
		when (select number from threads where id = new.thread_id) = 8
		begin select raise(abort, 'original persistence failure'); end;
		create trigger reject_failure_record before insert on sync_attempt_failures
		when new.operation = 'issue_comments'
		begin select raise(abort, 'failure ledger unavailable'); end;`); err != nil {
		t.Fatal(err)
	}
	stats, err := New(&partialGitHub{}, st).Sync(ctx, Options{
		Owner: "fixture", Repo: "repo", Numbers: []int{7, 8, 9}, IncludeComments: true,
	})
	if err == nil || !strings.Contains(err.Error(), "original persistence failure") ||
		!strings.Contains(err.Error(), "failure ledger unavailable") || stats.ThreadsSynced != 1 {
		t.Fatalf("original failure or prior commit lost: %+v err=%v", stats, err)
	}
	assertTableRowCount(t, st, "threads", 1)
	assertTableRowCount(t, st, "documents", 1)
	assertTableRowCount(t, st, "sync_attempt_failures", 0)
}

func TestSyncNoPersistedFamilyDoesNotResolveFailures(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	client := &partialGitHub{}
	s := New(client, st)
	opts := Options{Owner: "fixture", Repo: "repo", Numbers: []int{7}, IncludeComments: true}
	if _, err := s.Sync(ctx, opts); err != nil {
		t.Fatal(err)
	}
	repo, err := st.RepositoryByFullName(ctx, "fixture/repo")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.RecordSyncAttemptFailure(ctx, store.SyncAttemptFailure{
		RepoID: repo.ID, Number: 7, Operation: "issue_comments", ErrorMessage: "unavailable",
		LastSeenAt: time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatal(err)
	}
	s.beforePersist = func() {
		row, err := client.GetIssue(ctx, opts.Owner, opts.Repo, 7, nil)
		if err != nil {
			t.Fatal(err)
		}
		thread := mapIssueToThread(repo.ID, row, time.Now().UTC().Format(time.RFC3339Nano))
		if err := st.WithTx(ctx, func(tx *store.Store) error {
			sequence, err := tx.NextThreadObservationSequence(ctx, thread.UpdatedAt)
			if err != nil {
				return err
			}
			// A competing observation owns the parent and comments, but has not
			// advanced complete evidence. This payload can persist only enrichment.
			upsert, err := tx.UpsertThreadObservation(ctx, thread, store.UpsertThreadOptions{
				IncompleteEvidence: true, ObservationSequence: sequence,
			})
			if err != nil {
				return err
			}
			_, err = tx.ReserveThreadChildObservation(ctx, upsert.ID, store.ThreadChildComments, thread.UpdatedAtGitHub, sequence)
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	stats, err := s.Sync(ctx, opts)
	if err != nil || stats.ThreadsSynced != 1 || stats.CommentsSynced != 0 || stats.EvidenceObserved != 1 {
		t.Fatalf("expected enrichment without parent/child replacement: %+v err=%v", stats, err)
	}
	failures, err := st.ListSyncAttemptFailures(ctx, store.SyncAttemptFailureListOptions{RepoID: repo.ID})
	if err != nil || len(failures) != 1 || failures[0].Operation != "issue_comments" {
		t.Fatalf("empty family set resolved failures: %+v err=%v", failures, err)
	}
}
