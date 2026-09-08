package syncer

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	gh "github.com/openclaw/gitcrawl/internal/github"
	"github.com/openclaw/gitcrawl/internal/store"
)

type metadataGitHub struct {
	interleavedPRDetailsGitHub
	pullErr error
}

func (f *metadataGitHub) GetPull(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) (map[string]any, error) {
	if f.pullErr != nil {
		return nil, f.pullErr
	}
	row, err := f.interleavedPRDetailsGitHub.GetPull(ctx, owner, repo, number, reporter)
	row["merged_by"] = map[string]any{"login": "alice"}
	return row, err
}

func (f *metadataGitHub) ListIssueComments(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) ([]map[string]any, error) {
	return pullCommentRows(1), nil
}

func TestSyncPRMetadataSelectsOnlyRequestedFamilies(t *testing.T) {
	for _, tc := range []struct {
		name     string
		comments bool
		full     bool
	}{
		{name: "metadata"},
		{name: "metadata with comments", comments: true},
		{name: "full details wins", comments: true, full: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			client := &metadataGitHub{}
			stats, err := New(client, st).Sync(ctx, Options{
				Owner: "openclaw", Repo: "gitcrawl", Numbers: []int{8},
				IncludePRMetadata: true, IncludeComments: tc.comments, IncludePRDetails: tc.full,
			})
			if err != nil {
				t.Fatal(err)
			}
			if client.pullCalls != 1 || stats.PRDetailsSynced != 1 || stats.MetadataOnly {
				t.Fatalf("metadata fetches=%d stats=%+v", client.pullCalls, stats)
			}
			wantChildren := 0
			if tc.full {
				wantChildren = 1
			}
			for _, count := range []int{client.fileCalls, client.commitCalls, client.checkCalls, client.runCalls, client.reviewCalls} {
				if count != wantChildren {
					t.Fatalf("child fetch count=%d, want %d", count, wantChildren)
				}
			}
			if (stats.CommentsSynced > 0) != tc.comments || stats.RevisionsCreated != wantChildren || stats.FingerprintsUpserted != wantChildren {
				t.Fatalf("comments/revision stats=%+v", stats)
			}
			repo, err := st.RepositoryByFullName(ctx, "openclaw/gitcrawl")
			if err != nil {
				t.Fatal(err)
			}
			cache, err := st.PullRequestCache(ctx, repo.ID, 8)
			if err != nil || !strings.Contains(cache.Detail.RawJSON, `"merged_by":{"login":"alice"}`) {
				t.Fatalf("stored metadata=%+v err=%v", cache.Detail, err)
			}
			reservations := 1 + 4*wantChildren
			if tc.comments {
				reservations++
			}
			assertTableRowCount(t, st, "thread_child_observation_reservations", reservations)
			assertTableRowCount(t, st, "pull_request_review_thread_syncs", wantChildren)
			assertTableRowCount(t, st, "github_workflow_runs", wantChildren)
		})
	}
}

func TestSyncPRMetadataPreservesFullHydration(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	client := &metadataGitHub{}
	service := New(client, st)
	options := Options{Owner: "openclaw", Repo: "gitcrawl", Numbers: []int{8}, IncludeComments: true, IncludePRDetails: true}
	if _, err := service.Sync(ctx, options); err != nil {
		t.Fatal(err)
	}
	repo, err := st.RepositoryByFullName(ctx, "openclaw/gitcrawl")
	if err != nil {
		t.Fatal(err)
	}
	before, err := st.PullRequestCache(ctx, repo.ID, 8)
	if err != nil {
		t.Fatal(err)
	}
	reviews, err := st.PullRequestReviewThreads(ctx, before.Detail.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	runs, err := st.ListWorkflowRuns(ctx, repo.ID, store.WorkflowRunListOptions{Limit: -1})
	if err != nil {
		t.Fatal(err)
	}
	options.IncludeComments, options.IncludePRDetails, options.IncludePRMetadata = false, false, true
	stats, err := service.Sync(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	after, err := st.PullRequestCache(ctx, repo.ID, 8)
	if err != nil {
		t.Fatal(err)
	}
	afterReviews, err := st.PullRequestReviewThreads(ctx, before.Detail.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	afterRuns, err := st.ListWorkflowRuns(ctx, repo.ID, store.WorkflowRunListOptions{Limit: -1})
	if err != nil {
		t.Fatal(err)
	}
	if after.Detail.RawJSON == before.Detail.RawJSON || stats.PRDetailsSynced != 1 {
		t.Fatal("metadata was not refreshed")
	}
	if !reflect.DeepEqual(before.Files, after.Files) || !reflect.DeepEqual(before.Commits, after.Commits) ||
		!reflect.DeepEqual(before.Checks, after.Checks) || !reflect.DeepEqual(reviews, afterReviews) || !reflect.DeepEqual(runs, afterRuns) {
		t.Fatal("metadata refresh changed child hydration")
	}
	for _, family := range []store.ThreadChildObservationFamily{
		store.ThreadChildComments, store.ThreadChildPullRequestFiles, store.ThreadChildPullRequestCommits,
		store.ThreadChildPullRequestChecks, store.ThreadChildReviewThreads,
	} {
		assertChildReservation(t, ctx, st, before.Detail.ThreadID, family, 1)
	}
	if client.pullCalls != 2 || client.fileCalls != 1 || client.commitCalls != 1 || client.checkCalls != 1 || client.runCalls != 1 || client.reviewCalls != 1 {
		t.Fatal("metadata refresh fetched unrelated families")
	}
	if stats.EvidenceObserved != 0 || stats.RevisionsCreated != 0 || stats.FingerprintsUpserted != 0 {
		t.Fatalf("metadata refresh claimed full evidence: %+v", stats)
	}
	assertTableRowCount(t, st, "thread_revisions", 1)
	assertTableRowCount(t, st, "thread_fingerprints", 1)
}

func TestSyncPRMetadataResolvesOnlyMetadataFailures(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	client := &metadataGitHub{pullErr: errors.New("metadata unavailable")}
	service := New(client, st)
	options := Options{Owner: "openclaw", Repo: "gitcrawl", Numbers: []int{8}, IncludePRMetadata: true}
	if _, err := service.Sync(ctx, options); err == nil {
		t.Fatal("metadata failure was accepted")
	}
	repo, err := st.RepositoryByFullName(ctx, "openclaw/gitcrawl")
	if err != nil {
		t.Fatal(err)
	}
	failures, err := st.ListSyncAttemptFailures(ctx, store.SyncAttemptFailureListOptions{RepoID: repo.ID})
	if err != nil || len(failures) != 1 || failures[0].Operation != "pull_request_metadata" {
		t.Fatalf("metadata failure=%+v err=%v", failures, err)
	}
	for _, operation := range []string{"pull_request_details", "pull_review_threads"} {
		if _, err := st.RecordSyncAttemptFailure(ctx, store.SyncAttemptFailure{
			RepoID: repo.ID, Number: 8, Operation: operation, LastSeenAt: "2026-04-26T00:00:00Z",
		}); err != nil {
			t.Fatal(err)
		}
	}
	client.pullErr = nil
	if _, err := service.Sync(ctx, options); err != nil {
		t.Fatal(err)
	}
	failures, err = st.ListSyncAttemptFailures(ctx, store.SyncAttemptFailureListOptions{RepoID: repo.ID, IncludeResolved: true})
	if err != nil || len(failures) != 3 {
		t.Fatalf("failure history=%+v err=%v", failures, err)
	}
	for _, failure := range failures {
		if (failure.ResolvedAt != "") != (failure.Operation == "pull_request_metadata") {
			t.Fatalf("wrong failure resolved: %+v", failure)
		}
	}
}
