package syncer

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	gh "github.com/openclaw/gitcrawl/internal/github"
	"github.com/openclaw/gitcrawl/internal/store"
)

type metadataClosedSweepGitHub struct {
	metadataGitHub
	closed       bool
	updated      string
	sweepErr     error
	requests     []gh.ListIssuesOptions
	commentCalls int
}

func (f *metadataClosedSweepGitHub) ListRepositoryIssues(ctx context.Context, owner, repo string, opts gh.ListIssuesOptions, reporter gh.Reporter) ([]map[string]any, error) {
	f.requests = append(f.requests, opts)
	if !f.closed {
		rows, err := f.fakeGitHub.ListRepositoryIssues(ctx, owner, repo, opts, reporter)
		for _, row := range rows {
			row["updated_at"] = f.updated
		}
		return rows, err
	}
	if opts.State != "closed" {
		return nil, nil
	}
	row, err := f.fakeGitHub.GetIssue(ctx, owner, repo, 8, reporter)
	if err != nil {
		return nil, err
	}
	row["state"], row["closed_at"], row["updated_at"] = "closed", f.updated, f.updated
	return []map[string]any{row}, f.sweepErr
}

func (f *metadataClosedSweepGitHub) GetPull(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) (map[string]any, error) {
	row, err := f.metadataGitHub.GetPull(ctx, owner, repo, number, reporter)
	if err != nil {
		return nil, err
	}
	row["updated_at"], row["state"] = f.updated, "open"
	if f.closed {
		row["state"], row["closed_at"] = "closed", f.updated
	}
	return row, nil
}

func (f *metadataClosedSweepGitHub) ListIssueComments(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) ([]map[string]any, error) {
	f.commentCalls++
	if number == 8 {
		return f.metadataGitHub.ListIssueComments(ctx, owner, repo, number, reporter)
	}
	return f.fakeGitHub.ListIssueComments(ctx, owner, repo, number, reporter)
}

func TestDefaultClosedSweepWithPRMetadataPreservesChildrenAndFailureCheckpoint(t *testing.T) {
	ctx := t.Context()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	now := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	client := &metadataClosedSweepGitHub{updated: now.Format(time.RFC3339Nano)}
	service := New(client, st)
	service.now = func() time.Time { return now }
	opts := Options{Owner: "fixture", Repo: "repo", IncludeComments: true, IncludePRDetails: true}
	if _, err := service.Sync(ctx, opts); err != nil {
		t.Fatal(err)
	}
	repo, err := st.RepositoryByFullName(ctx, "fixture/repo")
	if err != nil {
		t.Fatal(err)
	}
	threads, err := st.ListThreads(ctx, repo.ID, true)
	if err != nil || len(threads) != 2 {
		t.Fatalf("seed threads=%+v err=%v", threads, err)
	}
	var omitted store.Thread
	for _, thread := range threads {
		if thread.Number == 7 {
			omitted = thread
		}
	}
	if omitted.ID == 0 {
		t.Fatal("missing omitted-thread fixture")
	}
	before, err := st.PullRequestCache(ctx, repo.ID, 8)
	if err != nil {
		t.Fatal(err)
	}
	childState := func() string {
		t.Helper()
		cache, err := st.PullRequestCache(ctx, repo.ID, 8)
		if err != nil {
			t.Fatal(err)
		}
		comments, err := st.ListComments(ctx, before.Detail.ThreadID)
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
		for _, family := range []store.ThreadChildObservationFamily{
			store.ThreadChildComments, store.ThreadChildPullRequestFiles,
			store.ThreadChildPullRequestCommits, store.ThreadChildPullRequestChecks,
			store.ThreadChildReviewThreads,
		} {
			assertChildReservation(t, ctx, st, before.Detail.ThreadID, family, 1)
		}
		return mustJSON(map[string]any{
			"files": cache.Files, "commits": cache.Commits, "checks": cache.Checks,
			"comments": comments, "reviews": reviews, "runs": runs,
		})
	}
	beforeChildren := childState()
	beforeCoverage, err := st.ArchiveCoverage(ctx, store.ArchiveCoverageOptions{RepoIDs: []int64{repo.ID}})
	if err != nil {
		t.Fatal(err)
	}
	if len(before.Files) == 0 || len(before.Commits) == 0 || len(before.Checks) == 0 ||
		beforeCoverage.Totals.PRReviewThreads == 0 || beforeCoverage.Totals.WorkflowRuns == 0 {
		t.Fatal("full hydration fixture must contain unrelated child collections")
	}
	seededAt := now
	now = now.Add(30 * 24 * time.Hour)
	client.closed, client.updated = true, now.Format(time.RFC3339Nano)
	client.requests = nil
	opts.IncludeComments, opts.IncludePRDetails, opts.IncludePRMetadata = false, false, true
	stats, err := service.Sync(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	if stats.ThreadsClosed != 1 || stats.PRDetailsSynced != 1 || stats.MetadataOnly ||
		stats.ClosedSweepThrough != now.Format(time.RFC3339Nano) {
		t.Fatalf("closure/metadata stats=%+v", stats)
	}
	if stats.CommentsSynced != 0 || stats.PRFilesSynced != 0 || stats.PRCommitsSynced != 0 ||
		stats.PRChecksSynced != 0 || stats.ReviewThreadsSynced != 0 || stats.WorkflowRunsSynced != 0 ||
		stats.EvidenceObserved != 0 || stats.RevisionsCreated != 0 || stats.FingerprintsUpserted != 0 {
		t.Fatalf("metadata-only closure claimed unrelated hydration: %+v", stats)
	}
	after, err := st.PullRequestCache(ctx, repo.ID, 8)
	if err != nil || after.Detail.RawJSON == before.Detail.RawJSON {
		t.Fatalf("metadata was not refreshed: err=%v", err)
	}
	if len(client.requests) != 2 || client.requests[0].State != "open" ||
		client.requests[0].Since != "" || client.requests[1].State != "closed" ||
		client.requests[1].Since != seededAt.Add(-time.Minute).Format(time.RFC3339Nano) {
		t.Fatalf("default listing/closed sweep=%+v", client.requests)
	}
	assertRetained := func() {
		t.Helper()
		current, err := st.ListThreads(ctx, repo.ID, true)
		if err != nil || len(current) != 2 {
			t.Fatalf("retained threads=%+v err=%v", current, err)
		}
		for _, thread := range current {
			if thread.Number == 7 && !reflect.DeepEqual(thread, omitted) {
				t.Fatalf("omitted thread changed: %+v", thread)
			}
			if thread.Number == 8 && thread.State != "closed" {
				t.Fatalf("observed closure missing: %+v", thread)
			}
		}
		if childState() != beforeChildren {
			t.Fatal("metadata-only closure changed unrelated children or their coverage reservations")
		}
		if client.commentCalls != 2 || client.fileCalls != 1 || client.commitCalls != 1 ||
			client.checkCalls != 1 || client.runCalls != 1 || client.reviewCalls != 1 {
			t.Fatal("metadata-only closure fetched unrelated child collections")
		}
	}
	assertRetained()
	afterCoverage, err := st.ArchiveCoverage(ctx, store.ArchiveCoverageOptions{RepoIDs: []int64{repo.ID}})
	if err != nil {
		t.Fatal(err)
	}
	a, b := beforeCoverage.Totals, afterCoverage.Totals
	if a.Comments != b.Comments || a.PRFiles != b.PRFiles || a.PRCommits != b.PRCommits ||
		a.PRChecks != b.PRChecks || a.PRReviewThreads != b.PRReviewThreads || a.WorkflowRuns != b.WorkflowRuns ||
		a.Enrichment.PRFiles.Covered != b.Enrichment.PRFiles.Covered ||
		a.Enrichment.PRFiles.Eligible != b.Enrichment.PRFiles.Eligible ||
		a.Enrichment.PRFiles.Missing != b.Enrichment.PRFiles.Missing ||
		a.Enrichment.PRFiles.Fresh != b.Enrichment.PRFiles.Fresh ||
		a.Enrichment.PRFiles.Complete != b.Enrichment.PRFiles.Complete {
		t.Fatalf("metadata-only closure changed unrelated archive coverage: before=%+v after=%+v", a, b)
	}
	completedAt := now
	successfulRuns, err := st.ListRuns(ctx, repo.ID, "sync", 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, operation := range []string{"sweep", "metadata"} {
		now = now.Add(time.Hour)
		client.updated = now.Format(time.RFC3339Nano)
		failure := errors.New("synthetic " + operation + " failure")
		if operation == "sweep" {
			client.sweepErr = failure
		} else {
			client.pullErr = failure
		}
		if _, err := service.Sync(ctx, opts); !errors.Is(err, failure) {
			t.Fatalf("%s failure=%v", operation, err)
		}
		watermark, err := st.ClosedSweepWatermark(ctx, repo.ID)
		if err != nil || !watermark.Equal(completedAt) {
			t.Fatalf("%s advanced successful checkpoint=%v err=%v", operation, watermark, err)
		}
		runs, err := st.ListRuns(ctx, repo.ID, "sync", 10)
		if err != nil || !reflect.DeepEqual(runs, successfulRuns) {
			t.Fatalf("%s changed successful run history: %+v err=%v", operation, runs, err)
		}
		if got := client.requests[len(client.requests)-1].Since; got != completedAt.Add(-time.Minute).Format(time.RFC3339Nano) {
			t.Fatalf("%s retry coverage=%q", operation, got)
		}
		client.sweepErr, client.pullErr = nil, nil
		assertRetained()
	}
	now = now.Add(time.Hour)
	client.updated = now.Format(time.RFC3339Nano)
	if _, err := service.Sync(ctx, opts); err != nil {
		t.Fatal(err)
	}
	watermark, err := st.ClosedSweepWatermark(ctx, repo.ID)
	if err != nil || !watermark.Equal(now) {
		t.Fatalf("successful retry checkpoint=%v err=%v", watermark, err)
	}
	assertRetained()
}
