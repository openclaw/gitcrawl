package syncer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	gh "github.com/openclaw/gitcrawl/internal/github"
	"github.com/openclaw/gitcrawl/internal/store"
)

type fakeGitHub struct{}

type observationProbeGitHub struct {
	fakeGitHub
	store         *store.Store
	sequence      int64
	childSequence int64
}

func (f *observationProbeGitHub) GetRepo(ctx context.Context, owner, repo string, reporter gh.Reporter) (map[string]any, error) {
	if err := f.store.DB().QueryRowContext(ctx, `
		select value
		from thread_observation_sequence
		where id = 1
	`).Scan(&f.sequence); err != nil {
		return nil, err
	}
	return f.fakeGitHub.GetRepo(ctx, owner, repo, reporter)
}

func (f *observationProbeGitHub) ListIssueComments(
	ctx context.Context,
	owner, repo string,
	number int,
	reporter gh.Reporter,
) ([]map[string]any, error) {
	if err := f.store.DB().QueryRowContext(ctx, `
		select value
		from thread_observation_sequence
		where id = 1
	`).Scan(&f.childSequence); err != nil {
		return nil, err
	}
	return f.fakeGitHub.ListIssueComments(ctx, owner, repo, number, reporter)
}

type mutableCommentGitHub struct {
	fakeGitHub
	empty            bool
	commentUpdatedAt string
}

type delayedObservationGitHub struct {
	fakeGitHub
	mu               sync.Mutex
	listCalls        int
	commentCalls     int
	firstListStarted chan struct{}
	releaseFirstList chan struct{}
}

type interleavedEvidenceGitHub struct {
	fakeGitHub
	mu                    sync.Mutex
	commentCalls          int
	firstEvidenceFetched  chan struct{}
	secondEvidenceFetched chan struct{}
	releaseFirstEvidence  chan struct{}
	releaseSecondEvidence chan struct{}
}

type interleavedPRCommentsGitHub struct {
	fakeGitHub
	mu            sync.Mutex
	commentCalls  int
	firstFetched  chan struct{}
	secondFetched chan struct{}
	releaseFirst  chan struct{}
	releaseSecond chan struct{}
}

type interleavedPRDetailsGitHub struct {
	fakeGitHub
	mu            sync.Mutex
	pullCalls     int
	fileCalls     int
	commitCalls   int
	checkCalls    int
	runCalls      int
	reviewCalls   int
	firstFetched  chan struct{}
	secondFetched chan struct{}
	releaseFirst  chan struct{}
	releaseSecond chan struct{}
}

type versionedPRDetailsGitHub struct {
	fakeGitHub
	version int
}

type sameHeadWorkflowGitHub struct {
	fakeGitHub
	number      int
	version     int
	prUpdatedAt string
	deletedRun  string
	fetched     chan struct{}
	release     chan struct{}
}

type sameSyncSharedHeadGitHub struct {
	fakeGitHub
	mu               sync.Mutex
	runCalls         int
	lookupCalls      int
	subsetFirst      bool
	verifiedDeletion bool
}

type workflowLookupResultGitHub struct {
	sameHeadWorkflowGitHub
	exact map[string]any
	err   error
}

type delayedVersionedPRGitHub struct {
	versionedPRDetailsGitHub
	fetched chan struct{}
	release chan struct{}
}

func (f delayedVersionedPRGitHub) GetIssue(
	ctx context.Context,
	owner, repo string,
	number int,
	reporter gh.Reporter,
) (map[string]any, error) {
	return map[string]any{
		"id":                 2,
		"number":             number,
		"state":              "open",
		"title":              fmt.Sprintf("parent-v%d", f.version),
		"body":               fmt.Sprintf("parent body v%d", f.version),
		"html_url":           fmt.Sprintf("https://github.com/openclaw/gitcrawl/pull/%d", number),
		"created_at":         "2026-07-12T00:00:00Z",
		"updated_at":         fmt.Sprintf("2026-07-12T00:00:0%dZ", f.version),
		"labels":             []map[string]any{},
		"assignees":          []map[string]any{},
		"user":               map[string]any{"login": "alice", "type": "User"},
		"author_association": "MEMBER",
		"pull_request":       map[string]any{"url": fmt.Sprintf("https://api.github.com/repos/openclaw/gitcrawl/pulls/%d", number)},
	}, nil
}

func (f delayedVersionedPRGitHub) ListIssueComments(
	ctx context.Context,
	owner, repo string,
	number int,
	reporter gh.Reporter,
) ([]map[string]any, error) {
	if f.fetched != nil {
		close(f.fetched)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-f.release:
		}
	}
	return pullCommentRows(f.version), nil
}

func (f *interleavedEvidenceGitHub) ListIssueComments(
	ctx context.Context,
	owner, repo string,
	number int,
	reporter gh.Reporter,
) ([]map[string]any, error) {
	f.mu.Lock()
	f.commentCalls++
	call := f.commentCalls
	f.mu.Unlock()
	body := "sequence two comment"
	if call == 2 {
		body = "sequence three comment"
	}
	rows := []map[string]any{{
		"id":         11,
		"body":       body,
		"created_at": "2026-04-26T00:00:00Z",
		"updated_at": "2026-04-26T00:00:00Z",
		"user":       map[string]any{"login": "vincentkoc", "type": "User"},
	}}
	var fetched, release chan struct{}
	switch call {
	case 1:
		fetched, release = f.firstEvidenceFetched, f.releaseFirstEvidence
	case 2:
		fetched, release = f.secondEvidenceFetched, f.releaseSecondEvidence
	}
	if fetched != nil {
		close(fetched)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-release:
		}
	}
	return rows, nil
}

func (f *interleavedPRCommentsGitHub) ListIssueComments(
	ctx context.Context,
	owner, repo string,
	number int,
	reporter gh.Reporter,
) ([]map[string]any, error) {
	f.mu.Lock()
	f.commentCalls++
	call := f.commentCalls
	f.mu.Unlock()
	rows := pullCommentRows(call)
	fetched, release := interleavedSignals(
		call,
		f.firstFetched,
		f.secondFetched,
		f.releaseFirst,
		f.releaseSecond,
	)
	if fetched != nil {
		close(fetched)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-release:
		}
	}
	return rows, nil
}

func (f *interleavedPRDetailsGitHub) GetPull(
	ctx context.Context,
	owner, repo string,
	number int,
	reporter gh.Reporter,
) (map[string]any, error) {
	f.mu.Lock()
	f.pullCalls++
	call := f.pullCalls
	f.mu.Unlock()
	return pullDetailRow(number, call), nil
}

func (f *interleavedPRDetailsGitHub) ListPullFiles(
	ctx context.Context,
	owner, repo string,
	number int,
	reporter gh.Reporter,
) ([]map[string]any, error) {
	f.mu.Lock()
	f.fileCalls++
	call := f.fileCalls
	f.mu.Unlock()
	return pullFileRows(call), nil
}

func (f *interleavedPRDetailsGitHub) ListPullCommits(
	ctx context.Context,
	owner, repo string,
	number int,
	reporter gh.Reporter,
) ([]map[string]any, error) {
	f.mu.Lock()
	f.commitCalls++
	call := f.commitCalls
	f.mu.Unlock()
	return pullCommitRows(call), nil
}

func (f *interleavedPRDetailsGitHub) ListCommitCheckRuns(
	ctx context.Context,
	owner, repo, ref string,
	reporter gh.Reporter,
) ([]map[string]any, error) {
	f.mu.Lock()
	f.checkCalls++
	call := f.checkCalls
	f.mu.Unlock()
	return pullCheckRows(call), nil
}

func (f *interleavedPRDetailsGitHub) ListWorkflowRuns(
	ctx context.Context,
	owner, repo string,
	options gh.ListWorkflowRunsOptions,
	reporter gh.Reporter,
) ([]map[string]any, error) {
	f.mu.Lock()
	f.runCalls++
	call := f.runCalls
	f.mu.Unlock()
	rows := pullWorkflowRows(call, options.HeadSHA)
	fetched, release := interleavedSignals(
		call,
		f.firstFetched,
		f.secondFetched,
		f.releaseFirst,
		f.releaseSecond,
	)
	if fetched != nil {
		close(fetched)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-release:
		}
	}
	return rows, nil
}

func (f *interleavedPRDetailsGitHub) ListPullReviewThreads(
	ctx context.Context,
	owner, repo string,
	number int,
	reporter gh.Reporter,
) ([]map[string]any, error) {
	f.mu.Lock()
	f.reviewCalls++
	call := f.reviewCalls
	f.mu.Unlock()
	return pullReviewThreadRows(call), nil
}

func (f versionedPRDetailsGitHub) GetPull(
	ctx context.Context,
	owner, repo string,
	number int,
	reporter gh.Reporter,
) (map[string]any, error) {
	return pullDetailRow(number, f.version), nil
}

func (f versionedPRDetailsGitHub) ListPullFiles(
	ctx context.Context,
	owner, repo string,
	number int,
	reporter gh.Reporter,
) ([]map[string]any, error) {
	return pullFileRows(f.version), nil
}

func (f versionedPRDetailsGitHub) ListPullCommits(
	ctx context.Context,
	owner, repo string,
	number int,
	reporter gh.Reporter,
) ([]map[string]any, error) {
	return pullCommitRows(f.version), nil
}

func (f versionedPRDetailsGitHub) ListCommitCheckRuns(
	ctx context.Context,
	owner, repo, ref string,
	reporter gh.Reporter,
) ([]map[string]any, error) {
	return pullCheckRows(f.version), nil
}

func (f versionedPRDetailsGitHub) ListWorkflowRuns(
	ctx context.Context,
	owner, repo string,
	options gh.ListWorkflowRunsOptions,
	reporter gh.Reporter,
) ([]map[string]any, error) {
	return pullWorkflowRows(f.version, options.HeadSHA), nil
}

func (f versionedPRDetailsGitHub) ListPullReviewThreads(
	ctx context.Context,
	owner, repo string,
	number int,
	reporter gh.Reporter,
) ([]map[string]any, error) {
	return pullReviewThreadRows(f.version), nil
}

func (f sameHeadWorkflowGitHub) GetIssue(
	ctx context.Context,
	owner, repo string,
	number int,
	reporter gh.Reporter,
) (map[string]any, error) {
	updatedAt := f.prUpdatedAt
	if updatedAt == "" {
		updatedAt = "2026-07-12T00:00:00Z"
	}
	return map[string]any{
		"id":                 1000 + f.number,
		"number":             f.number,
		"state":              "open",
		"title":              fmt.Sprintf("shared head PR %d", f.number),
		"body":               "",
		"html_url":           fmt.Sprintf("https://github.com/openclaw/gitcrawl/pull/%d", f.number),
		"created_at":         "2026-07-12T00:00:00Z",
		"updated_at":         updatedAt,
		"labels":             []map[string]any{},
		"assignees":          []map[string]any{},
		"user":               map[string]any{"login": "alice", "type": "User"},
		"author_association": "MEMBER",
		"pull_request":       map[string]any{"url": fmt.Sprintf("https://api.github.com/repos/openclaw/gitcrawl/pulls/%d", f.number)},
	}, nil
}

func (f sameHeadWorkflowGitHub) GetPull(
	ctx context.Context,
	owner, repo string,
	number int,
	reporter gh.Reporter,
) (map[string]any, error) {
	return map[string]any{
		"number": number,
		"head": map[string]any{
			"sha":  "shared-head",
			"ref":  "shared-branch",
			"repo": map[string]any{"full_name": "openclaw/gitcrawl"},
		},
		"base":            map[string]any{"sha": "base-sha"},
		"mergeable_state": "clean",
		"draft":           false,
	}, nil
}

func (f sameHeadWorkflowGitHub) ListWorkflowRuns(
	ctx context.Context,
	owner, repo string,
	options gh.ListWorkflowRunsOptions,
	reporter gh.Reporter,
) ([]map[string]any, error) {
	rows := []map[string]any{{
		"id":          900,
		"run_number":  1,
		"head_branch": "shared-branch",
		"head_sha":    options.HeadSHA,
		"status":      "queued",
		"name":        "old CI",
		"event":       "pull_request",
		"created_at":  "2026-07-12T00:00:00Z",
		"updated_at":  "2026-07-12T00:00:00Z",
	}}
	if f.version >= 2 {
		rows[0]["status"] = "completed"
		rows[0]["conclusion"] = "success"
		rows[0]["name"] = "new CI"
		rows[0]["updated_at"] = "2026-07-12T00:01:00Z"
	}
	if f.version == 4 {
		rows[0]["status"] = "queued"
		rows[0]["conclusion"] = ""
		rows[0]["name"] = "stale CI"
		rows[0]["updated_at"] = "2026-07-12T00:00:00Z"
	}
	if f.version == 2 {
		rows = append(rows, map[string]any{
			"id":          901,
			"run_number":  2,
			"head_branch": "shared-branch",
			"head_sha":    options.HeadSHA,
			"status":      "completed",
			"conclusion":  "success",
			"name":        "new lint",
			"event":       "pull_request",
			"created_at":  "2026-07-12T00:01:00Z",
			"updated_at":  "2026-07-12T00:02:00Z",
		})
	}
	if f.version == 4 {
		rows = append(rows,
			map[string]any{
				"id":          901,
				"run_number":  2,
				"head_branch": "shared-branch",
				"head_sha":    options.HeadSHA,
				"status":      "completed",
				"conclusion":  "success",
				"name":        "new lint",
				"event":       "pull_request",
				"created_at":  "2026-07-12T00:01:00Z",
				"updated_at":  "2026-07-12T00:02:00Z",
			},
			map[string]any{
				"id":          902,
				"run_number":  3,
				"head_branch": "shared-branch",
				"head_sha":    options.HeadSHA,
				"status":      "completed",
				"conclusion":  "success",
				"name":        "future sibling",
				"event":       "pull_request",
				"created_at":  "2026-07-12T00:03:00Z",
				"updated_at":  "2026-07-12T00:03:00Z",
			},
		)
	}
	if f.fetched != nil {
		close(f.fetched)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-f.release:
		}
	}
	return rows, nil
}

func (f sameHeadWorkflowGitHub) GetWorkflowRun(
	_ context.Context,
	_, _ string,
	runID string,
	_ gh.Reporter,
) (map[string]any, error) {
	if runID == f.deletedRun {
		return nil, &gh.RequestError{
			Method: "GET",
			URL:    "/repos/openclaw/gitcrawl/actions/runs/" + runID,
			Status: 404,
		}
	}
	return map[string]any{
		"id":         runID,
		"created_at": "2026-07-12T00:00:00Z",
		"updated_at": "2026-07-12T00:03:00Z",
	}, nil
}

func (f workflowLookupResultGitHub) GetWorkflowRun(
	_ context.Context,
	_, _ string,
	_ string,
	_ gh.Reporter,
) (map[string]any, error) {
	return f.exact, f.err
}

func (f *sameSyncSharedHeadGitHub) GetIssue(
	_ context.Context,
	_, _ string,
	number int,
	_ gh.Reporter,
) (map[string]any, error) {
	return map[string]any{
		"id":                 1000 + number,
		"number":             number,
		"state":              "open",
		"title":              fmt.Sprintf("shared head PR %d", number),
		"body":               "",
		"html_url":           fmt.Sprintf("https://github.com/openclaw/gitcrawl/pull/%d", number),
		"created_at":         "2026-07-12T00:00:00Z",
		"updated_at":         "2026-07-12T00:00:00Z",
		"labels":             []map[string]any{},
		"assignees":          []map[string]any{},
		"user":               map[string]any{"login": "alice", "type": "User"},
		"author_association": "MEMBER",
		"pull_request": map[string]any{
			"url": fmt.Sprintf("https://api.github.com/repos/openclaw/gitcrawl/pulls/%d", number),
		},
	}, nil
}

func (f *sameSyncSharedHeadGitHub) GetPull(
	_ context.Context,
	_, _ string,
	number int,
	_ gh.Reporter,
) (map[string]any, error) {
	return map[string]any{
		"number": number,
		"head": map[string]any{
			"sha":  "same-sync-head",
			"ref":  "same-sync-branch",
			"repo": map[string]any{"full_name": "openclaw/gitcrawl"},
		},
		"base":            map[string]any{"sha": "base-sha"},
		"mergeable_state": "clean",
		"draft":           false,
	}, nil
}

func (f *sameSyncSharedHeadGitHub) ListWorkflowRuns(
	_ context.Context,
	_, _ string,
	options gh.ListWorkflowRunsOptions,
	_ gh.Reporter,
) ([]map[string]any, error) {
	f.mu.Lock()
	f.runCalls++
	call := f.runCalls
	f.mu.Unlock()
	if f.verifiedDeletion {
		rows := []map[string]any{{
			"id":          900,
			"run_number":  1,
			"head_branch": "same-sync-branch",
			"head_sha":    options.HeadSHA,
			"status":      "completed",
			"conclusion":  "success",
			"name":        "latest",
			"event":       "pull_request",
			"created_at":  "2026-07-12T00:00:00Z",
			"updated_at":  "2026-07-12T00:02:00Z",
		}}
		includeDeleted := call == 1
		if f.subsetFirst {
			includeDeleted = call == 2
		}
		if includeDeleted {
			rows = append(rows, map[string]any{
				"id":          901,
				"run_number":  2,
				"head_branch": "same-sync-branch",
				"head_sha":    options.HeadSHA,
				"status":      "completed",
				"conclusion":  "success",
				"name":        "earlier",
				"event":       "pull_request",
				"created_at":  "2026-07-12T00:00:00Z",
				"updated_at":  "2026-07-12T00:01:00Z",
			})
		}
		return rows, nil
	}
	rows := []map[string]any{{
		"id":          901,
		"run_number":  2,
		"head_branch": "same-sync-branch",
		"head_sha":    options.HeadSHA,
		"status":      "completed",
		"conclusion":  "success",
		"name":        "latest",
		"event":       "pull_request",
		"created_at":  "2026-07-12T00:01:00Z",
		"updated_at":  "2026-07-12T00:02:00Z",
	}}
	includeEarlier := call == 1
	if f.subsetFirst {
		includeEarlier = call == 2
	}
	if includeEarlier {
		rows = append([]map[string]any{{
			"id":          900,
			"run_number":  1,
			"head_branch": "same-sync-branch",
			"head_sha":    options.HeadSHA,
			"status":      "completed",
			"conclusion":  "success",
			"name":        "earlier",
			"event":       "pull_request",
			"created_at":  "2026-07-12T00:00:00Z",
			"updated_at":  "2026-07-12T00:01:00Z",
		}}, rows...)
	}
	return rows, nil
}

func (f *sameSyncSharedHeadGitHub) GetWorkflowRun(
	_ context.Context,
	_, _ string,
	runID string,
	_ gh.Reporter,
) (map[string]any, error) {
	f.mu.Lock()
	f.lookupCalls++
	f.mu.Unlock()
	if f.verifiedDeletion && runID == "901" {
		return nil, &gh.RequestError{
			Method: "GET",
			URL:    "/repos/openclaw/gitcrawl/actions/runs/" + runID,
			Status: 404,
		}
	}
	return map[string]any{
		"id":         runID,
		"created_at": "2026-07-12T00:00:00Z",
		"updated_at": "2026-07-12T00:02:00Z",
	}, nil
}

func interleavedSignals(
	call int,
	firstFetched, secondFetched, releaseFirst, releaseSecond chan struct{},
) (chan struct{}, chan struct{}) {
	switch call {
	case 1:
		return firstFetched, releaseFirst
	case 2:
		return secondFetched, releaseSecond
	default:
		return nil, nil
	}
}

func pullCommentRows(version int) []map[string]any {
	return []map[string]any{{
		"id":         80 + version,
		"body":       fmt.Sprintf("comment-v%d", version),
		"created_at": "2026-07-12T00:00:00Z",
		"updated_at": fmt.Sprintf("2026-07-12T00:00:0%dZ", version),
		"user":       map[string]any{"login": "alice", "type": "User"},
	}}
}

func pullDetailRow(number, version int) map[string]any {
	return map[string]any{
		"number": number,
		"head": map[string]any{
			"sha":  fmt.Sprintf("head-v%d", version),
			"ref":  fmt.Sprintf("feature-v%d", version),
			"repo": map[string]any{"full_name": "openclaw/gitcrawl"},
		},
		"base":            map[string]any{"sha": fmt.Sprintf("base-v%d", version)},
		"mergeable_state": fmt.Sprintf("state-v%d", version),
		"additions":       version,
		"deletions":       version,
		"changed_files":   1,
		"draft":           false,
	}
}

func pullFileRows(version int) []map[string]any {
	return []map[string]any{{
		"filename":  fmt.Sprintf("file-v%d.go", version),
		"status":    "modified",
		"additions": version,
		"changes":   version,
		"patch":     fmt.Sprintf("@@ file-v%d", version),
	}}
}

func pullCommitRows(version int) []map[string]any {
	return []map[string]any{{
		"sha":      fmt.Sprintf("commit-v%d", version),
		"html_url": fmt.Sprintf("https://github.com/openclaw/gitcrawl/commit/v%d", version),
		"author":   map[string]any{"login": "alice"},
		"commit": map[string]any{
			"message": fmt.Sprintf("fix: version %d", version),
			"author":  map[string]any{"name": "Alice", "date": "2026-07-12T00:00:00Z"},
		},
	}}
}

func pullCheckRows(version int) []map[string]any {
	return []map[string]any{{
		"name":        fmt.Sprintf("check-v%d", version),
		"status":      "completed",
		"conclusion":  "success",
		"details_url": fmt.Sprintf("https://github.com/openclaw/gitcrawl/actions/check-v%d", version),
	}}
}

func pullWorkflowRows(version int, headSHA string) []map[string]any {
	return []map[string]any{{
		"id":          900 + version,
		"run_number":  version,
		"head_branch": fmt.Sprintf("feature-v%d", version),
		"head_sha":    headSHA,
		"status":      "completed",
		"conclusion":  "success",
		"name":        fmt.Sprintf("workflow-v%d", version),
		"event":       "pull_request",
		"created_at":  "2026-07-12T00:00:00Z",
		"updated_at":  fmt.Sprintf("2026-07-12T00:00:0%dZ", version),
	}}
}

func pullReviewThreadRows(version int) []map[string]any {
	return []map[string]any{{
		"id":         fmt.Sprintf("review-thread-v%d", version),
		"path":       fmt.Sprintf("file-v%d.go", version),
		"line":       version,
		"isResolved": false,
		"isOutdated": false,
		"comments": map[string]any{"nodes": []any{map[string]any{
			"id":        fmt.Sprintf("review-comment-v%d", version),
			"body":      fmt.Sprintf("review-v%d", version),
			"author":    map[string]any{"login": "alice", "__typename": "User"},
			"createdAt": "2026-07-12T00:00:00Z",
			"updatedAt": fmt.Sprintf("2026-07-12T00:00:0%dZ", version),
		}}},
	}}
}

func (f *delayedObservationGitHub) ListRepositoryIssues(
	ctx context.Context,
	owner, repo string,
	options gh.ListIssuesOptions,
	reporter gh.Reporter,
) ([]map[string]any, error) {
	f.mu.Lock()
	f.listCalls++
	call := f.listCalls
	f.mu.Unlock()
	if call == 1 {
		close(f.firstListStarted)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-f.releaseFirstList:
		}
	}
	state := "state-b"
	if call == 1 {
		state = "state-a"
	}
	return []map[string]any{{
		"id":                 1,
		"number":             7,
		"state":              "open",
		"title":              state,
		"body":               state + " body",
		"html_url":           "https://github.com/openclaw/gitcrawl/issues/7",
		"created_at":         "2026-07-12T00:00:00Z",
		"updated_at":         fmt.Sprintf("2026-07-12T00:00:0%dZ", call),
		"labels":             []map[string]any{},
		"assignees":          []map[string]any{},
		"user":               map[string]any{"login": "vincentkoc", "type": "User"},
		"author_association": "MEMBER",
	}}, nil
}

func (f *delayedObservationGitHub) ListIssueComments(
	ctx context.Context,
	owner, repo string,
	number int,
	reporter gh.Reporter,
) ([]map[string]any, error) {
	f.mu.Lock()
	f.commentCalls++
	call := f.commentCalls
	f.mu.Unlock()
	body := "state-b comment"
	if call == 2 {
		body = "state-a comment"
	}
	return []map[string]any{{
		"id":         11,
		"body":       body,
		"created_at": "2026-07-12T00:00:00Z",
		"updated_at": "2026-07-12T00:00:00Z",
		"user":       map[string]any{"login": "vincentkoc", "type": "User"},
	}}, nil
}

func (f *mutableCommentGitHub) ListIssueComments(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) ([]map[string]any, error) {
	if f.empty {
		return []map[string]any{}, nil
	}
	rows, err := f.fakeGitHub.ListIssueComments(ctx, owner, repo, number, reporter)
	if err != nil || f.commentUpdatedAt == "" {
		return rows, err
	}
	for _, row := range rows {
		row["updated_at"] = f.commentUpdatedAt
	}
	return rows, nil
}

func (fakeGitHub) GetRepo(ctx context.Context, owner, repo string, reporter gh.Reporter) (map[string]any, error) {
	return map[string]any{"id": 123}, nil
}

func (fakeGitHub) GetIssue(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) (map[string]any, error) {
	if number == 8 {
		return map[string]any{
			"id":                 2,
			"number":             8,
			"state":              "open",
			"title":              "fix sync",
			"body":               "",
			"html_url":           "https://github.com/openclaw/gitcrawl/pull/8",
			"created_at":         "2026-04-26T00:00:00Z",
			"updated_at":         "2026-04-26T00:00:00Z",
			"labels":             []map[string]any{},
			"assignees":          []map[string]any{},
			"user":               map[string]any{"login": "vincentkoc", "type": "User"},
			"author_association": "MEMBER",
			"pull_request":       map[string]any{"url": "https://api.github.com/repos/openclaw/gitcrawl/pulls/8"},
		}, nil
	}
	return map[string]any{
		"id":                 1,
		"number":             7,
		"state":              "open",
		"title":              "download stalls",
		"body":               "large file download stalls",
		"html_url":           "https://github.com/openclaw/gitcrawl/issues/7",
		"created_at":         "2026-04-26T00:00:00Z",
		"updated_at":         "2026-04-26T00:00:00Z",
		"labels":             []map[string]any{{"name": "bug"}},
		"assignees":          []map[string]any{},
		"user":               map[string]any{"login": "vincentkoc", "type": "User"},
		"author_association": "CONTRIBUTOR",
	}, nil
}

func (fakeGitHub) GetPull(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) (map[string]any, error) {
	return map[string]any{
		"number":          number,
		"head":            map[string]any{"sha": "head-sha", "ref": "feature", "repo": map[string]any{"full_name": "openclaw/gitcrawl"}},
		"base":            map[string]any{"sha": "base-sha"},
		"mergeable_state": "clean",
		"additions":       12,
		"deletions":       3,
		"changed_files":   2,
		"draft":           true,
	}, nil
}

func (fakeGitHub) ListRepositoryIssues(ctx context.Context, owner, repo string, options gh.ListIssuesOptions, reporter gh.Reporter) ([]map[string]any, error) {
	if options.State == "closed" {
		return nil, nil
	}
	return []map[string]any{
		{
			"id":                 1,
			"number":             7,
			"state":              "open",
			"title":              "download stalls",
			"body":               "large file download stalls",
			"html_url":           "https://github.com/openclaw/gitcrawl/issues/7",
			"created_at":         "2026-04-26T00:00:00Z",
			"updated_at":         "2026-04-26T00:00:00Z",
			"labels":             []map[string]any{{"name": "bug"}},
			"assignees":          []map[string]any{},
			"user":               map[string]any{"login": "vincentkoc", "type": "User"},
			"author_association": "CONTRIBUTOR",
		},
		{
			"id":                 2,
			"number":             8,
			"state":              "open",
			"title":              "fix sync",
			"body":               "",
			"html_url":           "https://github.com/openclaw/gitcrawl/pull/8",
			"created_at":         "2026-04-26T00:00:00Z",
			"updated_at":         "2026-04-26T00:00:00Z",
			"labels":             []map[string]any{},
			"assignees":          []map[string]any{},
			"user":               map[string]any{"login": "vincentkoc", "type": "User"},
			"author_association": "MEMBER",
			"pull_request":       map[string]any{"url": "https://api.github.com/repos/openclaw/gitcrawl/pulls/8"},
		},
	}, nil
}

func (fakeGitHub) ListIssueComments(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) ([]map[string]any, error) {
	if number != 7 {
		return nil, nil
	}
	return []map[string]any{{
		"id":         11,
		"body":       "same bug here",
		"created_at": "2026-04-26T00:00:00Z",
		"updated_at": "2026-04-26T00:00:00Z",
		"user":       map[string]any{"login": "vincentkoc", "type": "User"},
	}}, nil
}

func (fakeGitHub) ListPullReviews(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) ([]map[string]any, error) {
	return nil, nil
}

func (fakeGitHub) ListPullReviewComments(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) ([]map[string]any, error) {
	return nil, nil
}

func (fakeGitHub) ListPullReviewThreads(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) ([]map[string]any, error) {
	return nil, nil
}

func (fakeGitHub) ListPullFiles(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) ([]map[string]any, error) {
	return nil, nil
}

func (fakeGitHub) ListPullCommits(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) ([]map[string]any, error) {
	return nil, nil
}

func (fakeGitHub) ListCommitCheckRuns(ctx context.Context, owner, repo, ref string, reporter gh.Reporter) ([]map[string]any, error) {
	return nil, nil
}

func (fakeGitHub) ListWorkflowRuns(ctx context.Context, owner, repo string, options gh.ListWorkflowRunsOptions, reporter gh.Reporter) ([]map[string]any, error) {
	return nil, nil
}

type mutableUpdatedGitHub struct {
	fakeGitHub
	updatedAt        string
	commentUpdatedAt string
}

func (f *mutableUpdatedGitHub) ListRepositoryIssues(ctx context.Context, owner, repo string, options gh.ListIssuesOptions, reporter gh.Reporter) ([]map[string]any, error) {
	rows, err := f.fakeGitHub.ListRepositoryIssues(ctx, owner, repo, options, reporter)
	if err != nil || f.updatedAt == "" {
		return rows, err
	}
	for _, row := range rows {
		row["updated_at"] = f.updatedAt
	}
	return rows, nil
}

func (f *mutableUpdatedGitHub) ListIssueComments(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) ([]map[string]any, error) {
	rows, err := f.fakeGitHub.ListIssueComments(ctx, owner, repo, number, reporter)
	if err != nil || f.commentUpdatedAt == "" {
		return rows, err
	}
	for _, row := range rows {
		row["updated_at"] = f.commentUpdatedAt
	}
	return rows, nil
}

type metadataDraftGitHub struct {
	fakeGitHub
	draftPresent bool
	draft        bool
}

func (f *metadataDraftGitHub) ListRepositoryIssues(ctx context.Context, owner, repo string, options gh.ListIssuesOptions, reporter gh.Reporter) ([]map[string]any, error) {
	rows, err := f.fakeGitHub.ListRepositoryIssues(ctx, owner, repo, options, reporter)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		if issueKind(row) != "pull_request" {
			continue
		}
		if f.draftPresent {
			row["draft"] = f.draft
		} else {
			delete(row, "draft")
		}
	}
	return rows, nil
}

type sinceCaptureGitHub struct {
	fakeGitHub
	since string
}

func (f *sinceCaptureGitHub) ListRepositoryIssues(ctx context.Context, owner, repo string, options gh.ListIssuesOptions, reporter gh.Reporter) ([]map[string]any, error) {
	f.since = options.Since
	return nil, nil
}

type stateCaptureGitHub struct {
	fakeGitHub
	state string
}

func (f *stateCaptureGitHub) ListRepositoryIssues(ctx context.Context, owner, repo string, options gh.ListIssuesOptions, reporter gh.Reporter) ([]map[string]any, error) {
	f.state = options.State
	return nil, nil
}

type closedSweepGitHub struct {
	fakeGitHub
}

func (f closedSweepGitHub) ListRepositoryIssues(ctx context.Context, owner, repo string, options gh.ListIssuesOptions, reporter gh.Reporter) ([]map[string]any, error) {
	if options.State == "closed" {
		return []map[string]any{{
			"id":           2,
			"number":       8,
			"state":        "closed",
			"title":        "fix sync",
			"body":         "",
			"html_url":     "https://github.com/openclaw/gitcrawl/pull/8",
			"created_at":   "2026-04-26T00:00:00Z",
			"updated_at":   "2026-04-27T00:00:00Z",
			"closed_at":    "2026-04-27T00:00:00Z",
			"labels":       []map[string]any{},
			"assignees":    []map[string]any{},
			"user":         map[string]any{"login": "vincentkoc", "type": "User"},
			"pull_request": map[string]any{"url": "https://api.github.com/repos/openclaw/gitcrawl/pulls/8"},
		}}, nil
	}
	return f.fakeGitHub.ListRepositoryIssues(ctx, owner, repo, options, reporter)
}

type targetedGitHub struct {
	fakeGitHub
	listCalled bool
	numbers    []int
}

func (f *targetedGitHub) GetIssue(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) (map[string]any, error) {
	f.numbers = append(f.numbers, number)
	return f.fakeGitHub.GetIssue(ctx, owner, repo, number, reporter)
}

func (f *targetedGitHub) ListRepositoryIssues(ctx context.Context, owner, repo string, options gh.ListIssuesOptions, reporter gh.Reporter) ([]map[string]any, error) {
	f.listCalled = true
	return nil, nil
}

type pullCommentGitHub struct {
	fakeGitHub
}

func (pullCommentGitHub) ListPullReviews(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) ([]map[string]any, error) {
	if number != 8 {
		return nil, nil
	}
	return []map[string]any{{
		"id":         81,
		"body":       "",
		"state":      "APPROVED",
		"commit_id":  "head-sha",
		"created_at": "2026-04-26T00:00:00Z",
		"updated_at": "2026-04-26T00:01:00Z",
		"user":       map[string]any{"login": "reviewbot[bot]", "type": "User"},
	}}, nil
}

func (pullCommentGitHub) ListPullReviewComments(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) ([]map[string]any, error) {
	if number != 8 {
		return nil, nil
	}
	return []map[string]any{{
		"id":         82,
		"body":       "line comment",
		"created_at": "2026-04-26T00:02:00Z",
		"updated_at": "2026-04-26T00:03:00Z",
		"user":       map[string]any{"login": "alice", "type": "Bot"},
	}}, nil
}

func (pullCommentGitHub) ListPullReviewThreads(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) ([]map[string]any, error) {
	if number != 8 {
		return nil, nil
	}
	return []map[string]any{{
		"id":                 "PRRT_8",
		"path":               "internal/cache.go",
		"line":               42,
		"isResolved":         false,
		"isOutdated":         false,
		"viewerCanResolve":   true,
		"viewerCanUnresolve": false,
		"viewerCanReply":     true,
		"comments": map[string]any{"nodes": []any{map[string]any{
			"id":         "PRRC_82",
			"databaseId": 82,
			"body":       "line comment",
			"author":     map[string]any{"login": "alice", "__typename": "Bot"},
			"path":       "internal/cache.go",
			"diffHunk":   "@@ cache",
			"createdAt":  "2026-04-26T00:02:00Z",
			"updatedAt":  "2026-04-26T00:03:00Z",
			"url":        "https://github.com/openclaw/gitcrawl/pull/8#discussion_r82",
		}}},
	}}, nil
}

type pullDetailsGitHub struct {
	fakeGitHub
}

type completeWorkflowRunsGitHub struct {
	pullDetailsGitHub
	limit int
}

type emptyHeadPullGitHub struct {
	fakeGitHub
	checksCalled bool
	runsCalled   bool
}

type failingReviewThreadsGitHub struct {
	pullDetailsGitHub
}

type failingSecondCommentGitHub struct {
	fakeGitHub
}

type txProbePullDetailsGitHub struct {
	fakeGitHub
	st                        *store.Store
	sawPersistedThread        bool
	sawMissingPersistedThread bool
	sawPersistedThreadReadErr error
}

func (failingSecondCommentGitHub) ListIssueComments(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) ([]map[string]any, error) {
	if number == 8 {
		return nil, errors.New("comments unavailable")
	}
	return fakeGitHub{}.ListIssueComments(ctx, owner, repo, number, reporter)
}

func (g *txProbePullDetailsGitHub) ListPullFiles(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) ([]map[string]any, error) {
	storedRepo, err := g.st.RepositoryByFullName(ctx, owner+"/"+repo)
	if err != nil {
		g.sawMissingPersistedThread = true
		return nil, nil
	}
	threads, err := g.st.ListThreads(ctx, storedRepo.ID, true)
	if err != nil {
		g.sawPersistedThreadReadErr = err
		return nil, nil
	}
	for _, thread := range threads {
		if thread.Number == number && thread.Kind == "pull_request" {
			g.sawPersistedThread = true
		}
	}
	return nil, nil
}

func (emptyHeadPullGitHub) GetPull(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) (map[string]any, error) {
	return map[string]any{
		"number":          number,
		"head":            map[string]any{"ref": "feature", "repo": map[string]any{"full_name": "openclaw/gitcrawl"}},
		"base":            map[string]any{"sha": "base-sha"},
		"mergeable_state": "unknown",
	}, nil
}

func (g *emptyHeadPullGitHub) ListCommitCheckRuns(ctx context.Context, owner, repo, ref string, reporter gh.Reporter) ([]map[string]any, error) {
	g.checksCalled = true
	return nil, nil
}

func (g *emptyHeadPullGitHub) ListWorkflowRuns(ctx context.Context, owner, repo string, options gh.ListWorkflowRunsOptions, reporter gh.Reporter) ([]map[string]any, error) {
	g.runsCalled = true
	return nil, nil
}

func (pullDetailsGitHub) ListPullReviewThreads(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) ([]map[string]any, error) {
	return pullCommentGitHub{}.ListPullReviewThreads(ctx, owner, repo, number, reporter)
}

func (failingReviewThreadsGitHub) ListPullReviewThreads(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) ([]map[string]any, error) {
	return nil, errors.New("graphql unavailable")
}

func (pullDetailsGitHub) ListPullFiles(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) ([]map[string]any, error) {
	return []map[string]any{{
		"filename":  "internal/cache.go",
		"status":    "modified",
		"additions": 10,
		"deletions": 2,
		"changes":   12,
		"patch":     "@@ cache",
	}}, nil
}

func (pullDetailsGitHub) ListPullCommits(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) ([]map[string]any, error) {
	return []map[string]any{{
		"sha":      "commit-sha",
		"html_url": "https://github.com/openclaw/gitcrawl/commit/commit-sha",
		"author":   map[string]any{"login": "alice"},
		"commit": map[string]any{
			"message": "feat: cache",
			"author":  map[string]any{"name": "Alice", "date": "2026-04-26T00:00:00Z"},
		},
	}}, nil
}

func (pullDetailsGitHub) ListCommitCheckRuns(ctx context.Context, owner, repo, ref string, reporter gh.Reporter) ([]map[string]any, error) {
	return []map[string]any{{
		"name":        "test",
		"status":      "completed",
		"conclusion":  "success",
		"details_url": "https://github.com/openclaw/gitcrawl/actions/runs/99",
		"check_suite": map[string]any{"app": map[string]any{"name": "GitHub Actions"}},
	}}, nil
}

func (pullDetailsGitHub) ListWorkflowRuns(ctx context.Context, owner, repo string, options gh.ListWorkflowRunsOptions, reporter gh.Reporter) ([]map[string]any, error) {
	return []map[string]any{{
		"id":          99,
		"run_number":  7,
		"head_branch": "feature",
		"head_sha":    options.HeadSHA,
		"status":      "completed",
		"conclusion":  "success",
		"name":        "CI",
		"event":       "pull_request",
		"html_url":    "https://github.com/openclaw/gitcrawl/actions/runs/99",
		"created_at":  "2026-04-26T00:00:00Z",
		"updated_at":  "2026-04-26T00:01:00Z",
	}}, nil
}

func (g *completeWorkflowRunsGitHub) ListWorkflowRuns(ctx context.Context, owner, repo string, options gh.ListWorkflowRunsOptions, reporter gh.Reporter) ([]map[string]any, error) {
	g.limit = options.Limit
	rows := make([]map[string]any, 25)
	for index := range rows {
		rows[index] = map[string]any{
			"id":         index + 1,
			"run_number": index + 1,
			"head_sha":   options.HeadSHA,
			"status":     "completed",
			"conclusion": "success",
			"name":       "CI",
			"created_at": "2026-07-12T00:00:00Z",
			"updated_at": "2026-07-12T00:01:00Z",
		}
	}
	return rows, nil
}

func TestSyncPersistsIssuesAndPullRequests(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	s := New(fakeGitHub{}, st)
	s.now = func() time.Time { return time.Date(2026, 4, 26, 0, 0, 0, 0, time.UTC) }
	var progressLogs bytes.Buffer
	stats, err := s.Sync(ctx, Options{
		Owner:            "openclaw",
		Repo:             "gitcrawl",
		IncludeComments:  true,
		IncludePRDetails: true,
		Logger:           testProgressLogger(&progressLogs),
	})
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if stats.ThreadsSynced != 2 || stats.IssuesSynced != 1 || stats.PullRequestsSynced != 1 {
		t.Fatalf("unexpected stats: %#v", stats)
	}
	if stats.EvidenceObserved != 2 || stats.RevisionsCreated != 2 || stats.FingerprintsUpserted != 2 {
		t.Fatalf("enrichment stats: %#v", stats)
	}
	if stats.CommentsSynced != 1 {
		t.Fatalf("comments synced: got %d want 1", stats.CommentsSynced)
	}
	if stats.MetadataOnly {
		t.Fatal("metadata only: got true want false")
	}

	repo, err := st.RepositoryByFullName(ctx, "openclaw/gitcrawl")
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	threads, err := st.ListThreads(ctx, repo.ID, false)
	if err != nil {
		t.Fatalf("threads: %v", err)
	}
	if len(threads) != 2 {
		t.Fatalf("threads: got %d want 2", len(threads))
	}
	if threads[1].Kind != "pull_request" {
		t.Fatalf("second thread kind: %s", threads[1].Kind)
	}
	if !threads[1].IsDraft {
		t.Fatal("pull request draft state was not persisted from pull detail")
	}
	if threads[0].AuthorAssociation != "CONTRIBUTOR" || threads[1].AuthorAssociation != "MEMBER" {
		t.Fatalf("author associations: %+v", threads)
	}
	var revisions, fingerprints int
	if err := st.DB().QueryRowContext(ctx, `select count(*) from thread_revisions`).Scan(&revisions); err != nil {
		t.Fatalf("revision count: %v", err)
	}
	if err := st.DB().QueryRowContext(ctx, `select count(*) from thread_fingerprints where algorithm_version = ?`, store.ThreadFingerprintAlgorithmVersion).Scan(&fingerprints); err != nil {
		t.Fatalf("fingerprint count: %v", err)
	}
	if revisions != 2 || fingerprints != 2 {
		t.Fatalf("revision/fingerprint counts = %d/%d", revisions, fingerprints)
	}
	secondStats, err := s.Sync(ctx, Options{Owner: "openclaw", Repo: "gitcrawl", IncludeComments: true, IncludePRDetails: true})
	if err != nil {
		t.Fatalf("repeat sync: %v", err)
	}
	if secondStats.RevisionsCreated != 0 || secondStats.FingerprintsUpserted != 0 {
		t.Fatalf("repeat enrichment stats: %#v", secondStats)
	}
	var documentCount int
	if err := st.DB().QueryRowContext(ctx, `select count(*) from documents_fts where documents_fts match 'failure OR bug'`).Scan(&documentCount); err != nil {
		t.Fatalf("query document index: %v", err)
	}
	if documentCount != 1 {
		t.Fatalf("document count: got %d want 1", documentCount)
	}
	for _, want := range []string{
		`msg="sync progress"`,
		`state=finished`,
		`unit=threads`,
		`percent=100.0`,
		`completion=100.0%`,
		`repository=openclaw/gitcrawl`,
	} {
		if !strings.Contains(progressLogs.String(), want) {
			t.Fatalf("missing %q in progress logs:\n%s", want, progressLogs.String())
		}
	}
}

func TestSyncAllocatesObservationSequenceAfterParentFetch(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	client := &observationProbeGitHub{store: st}
	s := New(client, st)
	s.now = func() time.Time { return time.Date(2026, 7, 12, 0, 0, 0, 0, time.UTC) }
	if _, err := s.Sync(ctx, Options{
		Owner:           "openclaw",
		Repo:            "gitcrawl",
		IncludeComments: true,
	}); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if client.sequence != 0 {
		t.Fatalf("repository fetch observed sequence %d, want allocation deferred until parent observation", client.sequence)
	}
	if client.childSequence != 1 {
		t.Fatalf("child fetch observed sequence %d, want parent generation 1", client.childSequence)
	}
	var allocatedSequence int64
	if err := st.DB().QueryRowContext(ctx, `
		select value
		from thread_observation_sequence
		where id = 1
	`).Scan(&allocatedSequence); err != nil {
		t.Fatalf("read allocated observation sequence: %v", err)
	}
	if allocatedSequence != 1 {
		t.Fatalf("allocated observation sequence = %d, want 1", allocatedSequence)
	}
	var revisionSequences int
	if err := st.DB().QueryRowContext(ctx, `
		select count(distinct observation_sequence)
		from thread_revisions
	`).Scan(&revisionSequences); err != nil {
		t.Fatalf("revision observation sequences: %v", err)
	}
	if revisionSequences != 1 {
		t.Fatalf("revision observation sequence count = %d, want 1", revisionSequences)
	}
}

func TestSyncDelayedNewerParentUsesSameGenerationForAllChildren(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	slowFetched := make(chan struct{})
	releaseSlow := make(chan struct{})
	slow := New(delayedVersionedPRGitHub{
		versionedPRDetailsGitHub: versionedPRDetailsGitHub{version: 2},
		fetched:                  slowFetched,
		release:                  releaseSlow,
	}, st)
	fast := New(delayedVersionedPRGitHub{
		versionedPRDetailsGitHub: versionedPRDetailsGitHub{version: 1},
	}, st)
	now := func() time.Time { return time.Date(2026, 7, 12, 0, 1, 0, 0, time.UTC) }
	slow.now = now
	fast.now = now
	options := Options{
		Owner:            "openclaw",
		Repo:             "gitcrawl",
		Numbers:          []int{8},
		IncludeComments:  true,
		IncludePRDetails: true,
	}
	type syncResult struct {
		stats Stats
		err   error
	}
	slowResult := make(chan syncResult, 1)
	go func() {
		stats, err := slow.Sync(ctx, options)
		slowResult <- syncResult{stats: stats, err: err}
	}()
	awaitSyncSignal(t, slowFetched, "delayed newer child fetch")

	fastStats, err := fast.Sync(ctx, options)
	if err != nil {
		t.Fatalf("fast older-source sync: %v", err)
	}
	if fastStats.PRDetailsSynced != 1 {
		t.Fatalf("fast older-source stats = %#v", fastStats)
	}
	close(releaseSlow)
	slowCompleted := <-slowResult
	if slowCompleted.err != nil {
		t.Fatalf("delayed newer-source sync: %v", slowCompleted.err)
	}
	if slowCompleted.stats.PRDetailsSynced != 1 {
		t.Fatalf("delayed newer-source stats = %#v", slowCompleted.stats)
	}

	thread, detail := assertVersionedPRHydration(t, ctx, st, 2, true)
	if thread.Title != "parent-v2" {
		t.Fatalf("parent title = %q, want parent-v2", thread.Title)
	}
	for _, family := range []store.ThreadChildObservationFamily{
		store.ThreadChildComments,
		store.ThreadChildPullRequestDetails,
		store.ThreadChildPullRequestFiles,
		store.ThreadChildPullRequestCommits,
		store.ThreadChildPullRequestChecks,
		store.ThreadChildReviewThreads,
	} {
		assertChildReservation(t, ctx, st, thread.ID, family, 1)
	}
	assertWorkflowRunReservation(t, ctx, st, detail.RepoID, detail.HeadSHA, 1)
	var parentSequence int64
	if err := st.DB().QueryRowContext(ctx, `
		select observation_sequence
		from threads
		where id = ?
	`, thread.ID).Scan(&parentSequence); err != nil {
		t.Fatalf("read parent observation sequence: %v", err)
	}
	if parentSequence != 1 {
		t.Fatalf("parent observation sequence = %d, want newer source generation 1", parentSequence)
	}
}

func TestSyncPersistsNewerCompleteEvidenceBelowParentHighWaterMark(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if sequence, err := st.NextThreadObservationSequence(ctx, "2026-07-12T00:00:00Z"); err != nil || sequence != 1 {
		t.Fatalf("sequence floor = %d, %v", sequence, err)
	}
	client := &interleavedEvidenceGitHub{
		firstEvidenceFetched:  make(chan struct{}),
		secondEvidenceFetched: make(chan struct{}),
		releaseFirstEvidence:  make(chan struct{}),
		releaseSecondEvidence: make(chan struct{}),
	}
	s := New(client, st)
	s.now = func() time.Time { return time.Date(2026, 7, 12, 0, 1, 0, 0, time.UTC) }
	fullOptions := Options{
		Owner:           "openclaw",
		Repo:            "gitcrawl",
		Numbers:         []int{7},
		IncludeComments: true,
	}
	type syncResult struct {
		stats Stats
		err   error
	}
	firstResult := make(chan syncResult, 1)
	go func() {
		stats, err := s.Sync(ctx, fullOptions)
		firstResult <- syncResult{stats: stats, err: err}
	}()
	select {
	case <-client.firstEvidenceFetched:
	case <-time.After(5 * time.Second):
		t.Fatal("sequence two sync did not fetch complete evidence")
	}
	secondResult := make(chan syncResult, 1)
	go func() {
		stats, err := s.Sync(ctx, fullOptions)
		secondResult <- syncResult{stats: stats, err: err}
	}()
	select {
	case <-client.secondEvidenceFetched:
	case <-time.After(5 * time.Second):
		t.Fatal("sequence three sync did not fetch complete evidence")
	}

	metadataStats, err := s.Sync(ctx, Options{
		Owner:   "openclaw",
		Repo:    "gitcrawl",
		Numbers: []int{7},
	})
	if err != nil {
		t.Fatalf("sequence four metadata sync: %v", err)
	}
	if metadataStats.ThreadsSynced != 1 || metadataStats.EvidenceObserved != 0 {
		t.Fatalf("sequence four metadata stats = %#v", metadataStats)
	}

	close(client.releaseSecondEvidence)
	second := <-secondResult
	if second.err != nil {
		t.Fatalf("sequence three sync: %v", second.err)
	}
	if second.stats.ThreadsSynced != 1 || second.stats.ThreadsSkippedStale != 0 ||
		second.stats.CommentsSynced != 1 || second.stats.EvidenceObserved != 1 ||
		second.stats.RevisionsCreated != 1 || second.stats.FingerprintsUpserted != 1 {
		t.Fatalf("sequence three stats = %#v", second.stats)
	}
	close(client.releaseFirstEvidence)
	first := <-firstResult
	if first.err != nil {
		t.Fatalf("sequence two sync: %v", first.err)
	}
	if first.stats.ThreadsSynced != 0 || first.stats.ThreadsSkippedStale != 1 ||
		first.stats.CommentsSynced != 0 || first.stats.EvidenceObserved != 0 ||
		first.stats.RevisionsCreated != 0 || first.stats.FingerprintsUpserted != 0 {
		t.Fatalf("sequence two stats = %#v", first.stats)
	}

	var threadID, parentSequence, evidenceSequence, latestRevisionID, latestRevisionSequence, revisions int64
	if err := st.DB().QueryRowContext(ctx, `
		select id, observation_sequence, evidence_observation_sequence
		from threads
		where number = 7
	`).Scan(&threadID, &parentSequence, &evidenceSequence); err != nil {
		t.Fatalf("parent sequence: %v", err)
	}
	if err := st.DB().QueryRowContext(ctx, `
		select id, observation_sequence
		from thread_revisions
		where thread_id = ?
		order by observation_sequence desc
		limit 1
	`, threadID).Scan(&latestRevisionID, &latestRevisionSequence); err != nil {
		t.Fatalf("latest revision sequence: %v", err)
	}
	if err := st.DB().QueryRowContext(ctx, `
		select count(*)
		from thread_revisions
		where thread_id = ?
	`, threadID).Scan(&revisions); err != nil {
		t.Fatalf("revision count: %v", err)
	}
	comments, err := st.ListComments(ctx, threadID)
	if err != nil {
		t.Fatalf("comments: %v", err)
	}
	if parentSequence != 4 || evidenceSequence != 3 || latestRevisionSequence != 3 || revisions != 1 ||
		len(comments) != 1 || comments[0].Body != "sequence three comment" {
		t.Fatalf(
			"persisted state = parent %d, evidence %d, revision %d, revisions %d, comments %+v",
			parentSequence,
			evidenceSequence,
			latestRevisionSequence,
			revisions,
			comments,
		)
	}
	repo, err := st.RepositoryByFullName(ctx, "openclaw/gitcrawl")
	if err != nil {
		t.Fatalf("repository: %v", err)
	}
	summaryTasks, err := st.ListSummaryTasks(ctx, store.SummaryTaskOptions{
		RepoID:        repo.ID,
		Provider:      "test",
		Model:         "test",
		SummaryKind:   store.SummaryKindLLMKey,
		PromptVersion: store.SummaryPromptVersionV1,
		Number:        7,
		Force:         true,
	})
	if err != nil {
		t.Fatalf("summary tasks: %v", err)
	}
	if len(summaryTasks) != 1 || summaryTasks[0].RevisionID != latestRevisionID {
		t.Fatalf("summary tasks after metadata-only observation = %+v", summaryTasks)
	}
	coverage, err := st.ArchiveCoverage(ctx, store.ArchiveCoverageOptions{
		RepoIDs: []int64{repo.ID},
	})
	if err != nil {
		t.Fatalf("archive coverage: %v", err)
	}
	if len(coverage.Rows) != 1 ||
		coverage.Rows[0].Enrichment.Revisions.Fresh != 1 ||
		coverage.Rows[0].Enrichment.Fingerprints.Fresh != 1 {
		t.Fatalf("coverage after metadata-only observation = %+v", coverage)
	}
}

func TestSyncPartialPRCommentsUseParentObservationGeneration(t *testing.T) {
	for _, test := range []struct {
		name                string
		secondPersistsFirst bool
	}{
		{name: "first child snapshot persists first"},
		{name: "second child snapshot persists first", secondPersistsFirst: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
			if err != nil {
				t.Fatalf("open store: %v", err)
			}
			defer st.Close()

			client := &interleavedPRCommentsGitHub{
				firstFetched:  make(chan struct{}),
				secondFetched: make(chan struct{}),
				releaseFirst:  make(chan struct{}),
				releaseSecond: make(chan struct{}),
			}
			s := New(client, st)
			s.now = func() time.Time { return time.Date(2026, 7, 12, 1, 0, 0, 0, time.UTC) }
			runInterleavedSyncs(
				t,
				ctx,
				s,
				Options{
					Owner:           "openclaw",
					Repo:            "gitcrawl",
					Numbers:         []int{8},
					IncludeComments: true,
				},
				client.firstFetched,
				client.secondFetched,
				client.releaseFirst,
				client.releaseSecond,
				test.secondPersistsFirst,
			)

			repo, err := st.RepositoryByFullName(ctx, "openclaw/gitcrawl")
			if err != nil {
				t.Fatalf("repository: %v", err)
			}
			threads, err := st.ListThreads(ctx, repo.ID, true)
			if err != nil || len(threads) != 1 {
				t.Fatalf("threads = %+v, %v", threads, err)
			}
			comments, err := st.ListComments(ctx, threads[0].ID)
			if err != nil {
				t.Fatalf("comments: %v", err)
			}
			wantBody := "comment-v2"
			if len(comments) != 1 || comments[0].Body != wantBody {
				t.Fatalf("comments = %+v, want %s from parent generation 2", comments, wantBody)
			}
			assertChildReservation(
				t,
				ctx,
				st,
				threads[0].ID,
				store.ThreadChildComments,
				2,
			)
		})
	}
}

func TestSyncPartialPRDetailsUseParentObservationGeneration(t *testing.T) {
	for _, test := range []struct {
		name                string
		secondPersistsFirst bool
	}{
		{name: "first child snapshot persists first"},
		{name: "second child snapshot persists first", secondPersistsFirst: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
			if err != nil {
				t.Fatalf("open store: %v", err)
			}
			defer st.Close()

			client := &interleavedPRDetailsGitHub{
				firstFetched:  make(chan struct{}),
				secondFetched: make(chan struct{}),
				releaseFirst:  make(chan struct{}),
				releaseSecond: make(chan struct{}),
			}
			s := New(client, st)
			s.now = func() time.Time { return time.Date(2026, 7, 12, 1, 0, 0, 0, time.UTC) }
			runInterleavedSyncs(
				t,
				ctx,
				s,
				Options{
					Owner:            "openclaw",
					Repo:             "gitcrawl",
					Numbers:          []int{8},
					IncludePRDetails: true,
				},
				client.firstFetched,
				client.secondFetched,
				client.releaseFirst,
				client.releaseSecond,
				test.secondPersistsFirst,
			)

			thread, detail := assertVersionedPRHydration(t, ctx, st, 2, true)
			for _, family := range []store.ThreadChildObservationFamily{
				store.ThreadChildPullRequestDetails,
				store.ThreadChildPullRequestFiles,
				store.ThreadChildPullRequestCommits,
				store.ThreadChildPullRequestChecks,
				store.ThreadChildReviewThreads,
			} {
				assertChildReservation(t, ctx, st, thread.ID, family, 2)
			}
			assertWorkflowRunReservation(t, ctx, st, detail.RepoID, detail.HeadSHA, 2)
			wantHead := "head-v2"
			if detail.HeadSHA != wantHead {
				t.Fatalf("detail = %+v, want %s from parent generation 2", detail, wantHead)
			}
		})
	}
}

func TestSyncPartialPRDetailsAdvanceIndependentFamilies(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	oldSyncer := New(versionedPRDetailsGitHub{version: 1}, st)
	oldSyncer.now = func() time.Time { return time.Date(2026, 7, 12, 1, 0, 0, 0, time.UTC) }
	if _, err := oldSyncer.Sync(ctx, Options{
		Owner:            "openclaw",
		Repo:             "gitcrawl",
		Numbers:          []int{8},
		IncludePRDetails: true,
	}); err != nil {
		t.Fatalf("seed details: %v", err)
	}
	thread, detail := assertVersionedPRHydration(t, ctx, st, 1, true)
	if applied, err := st.ReserveThreadChildObservation(
		ctx,
		thread.ID,
		store.ThreadChildPullRequestFiles,
		"2026-04-26T00:00:00Z",
		10,
	); err != nil || !applied {
		t.Fatalf("reserve newer files = %t, %v", applied, err)
	}
	if err := st.UpsertPullRequestCacheFamilies(
		ctx,
		detail,
		[]store.PullRequestFile{{
			ThreadID:  thread.ID,
			Path:      "reserved-file.go",
			RawJSON:   "{}",
			FetchedAt: "2026-07-12T01:10:00Z",
		}},
		nil,
		nil,
		nil,
		store.PullRequestHydrationFamilies{Files: true},
	); err != nil {
		t.Fatalf("seed newer files: %v", err)
	}

	newSyncer := New(versionedPRDetailsGitHub{version: 2}, st)
	newSyncer.now = func() time.Time { return time.Date(2026, 7, 12, 1, 1, 0, 0, time.UTC) }
	if _, err := newSyncer.Sync(ctx, Options{
		Owner:            "openclaw",
		Repo:             "gitcrawl",
		Numbers:          []int{8},
		IncludePRDetails: true,
	}); err != nil {
		t.Fatalf("refresh details: %v", err)
	}
	thread, detail = assertVersionedPRHydration(t, ctx, st, 2, false)
	files, err := st.PullRequestFiles(ctx, thread.ID)
	if err != nil {
		t.Fatalf("files: %v", err)
	}
	if len(files) != 1 || files[0].Path != "reserved-file.go" {
		t.Fatalf("files = %+v, want independently newer file family", files)
	}
	if detail.HeadSHA != "head-v2" {
		t.Fatalf("detail = %+v, want independently advanced detail family", detail)
	}
	assertChildReservation(t, ctx, st, thread.ID, store.ThreadChildPullRequestFiles, 10)
	for _, family := range []store.ThreadChildObservationFamily{
		store.ThreadChildPullRequestDetails,
		store.ThreadChildPullRequestCommits,
		store.ThreadChildPullRequestChecks,
		store.ThreadChildReviewThreads,
	} {
		assertChildReservation(t, ctx, st, thread.ID, family, 2)
	}
	assertWorkflowRunReservation(t, ctx, st, detail.RepoID, "head-v1", 1)
	assertWorkflowRunReservation(t, ctx, st, detail.RepoID, detail.HeadSHA, 2)
}

func TestSyncWorkflowRunsSharedHeadRejectsOlderPRSnapshot(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	oldFetched := make(chan struct{})
	releaseOld := make(chan struct{})
	oldSyncer := New(sameHeadWorkflowGitHub{
		number:      8,
		version:     1,
		prUpdatedAt: "2026-07-12T00:10:00Z",
		fetched:     oldFetched,
		release:     releaseOld,
	}, st)
	oldSyncer.now = func() time.Time { return time.Date(2026, 7, 12, 1, 0, 0, 0, time.UTC) }
	type result struct {
		stats Stats
		err   error
	}
	oldResult := make(chan result, 1)
	go func() {
		stats, err := oldSyncer.Sync(ctx, Options{
			Owner:            "openclaw",
			Repo:             "gitcrawl",
			Numbers:          []int{8},
			IncludePRDetails: true,
		})
		oldResult <- result{stats: stats, err: err}
	}()
	awaitSyncSignal(t, oldFetched, "older PR workflow snapshot")

	newSyncer := New(sameHeadWorkflowGitHub{
		number:      9,
		version:     2,
		prUpdatedAt: "2026-07-12T00:00:00Z",
	}, st)
	newSyncer.now = func() time.Time { return time.Date(2026, 7, 12, 1, 1, 0, 0, time.UTC) }
	newStats, err := newSyncer.Sync(ctx, Options{
		Owner:            "openclaw",
		Repo:             "gitcrawl",
		Numbers:          []int{9},
		IncludePRDetails: true,
	})
	if err != nil {
		t.Fatalf("newer PR sync: %v", err)
	}
	if newStats.PRDetailsSynced != 1 || newStats.WorkflowRunsSynced != 2 {
		t.Fatalf("newer PR stats = %#v", newStats)
	}

	close(releaseOld)
	old := <-oldResult
	if old.err != nil {
		t.Fatalf("older PR sync: %v", old.err)
	}
	if old.stats.PRDetailsSynced != 1 || old.stats.WorkflowRunsSynced != 0 {
		t.Fatalf("older PR stats = %#v", old.stats)
	}

	repo, err := st.RepositoryByFullName(ctx, "openclaw/gitcrawl")
	if err != nil {
		t.Fatalf("repository: %v", err)
	}
	runs, err := st.ListWorkflowRuns(ctx, repo.ID, store.WorkflowRunListOptions{
		HeadSHA: "shared-head",
		Limit:   -1,
	})
	if err != nil {
		t.Fatalf("workflow runs: %v", err)
	}
	if len(runs) != 2 {
		t.Fatalf("workflow runs = %+v, want newer two-run snapshot", runs)
	}
	runsByID := make(map[string]store.WorkflowRun, len(runs))
	for _, run := range runs {
		runsByID[run.RunID] = run
	}
	if runsByID["900"].WorkflowName != "new CI" ||
		runsByID["900"].Status != "completed" ||
		runsByID["901"].WorkflowName != "new lint" {
		t.Fatalf("workflow runs = %+v, older PR overwrote newer snapshot", runs)
	}
	assertWorkflowRunReservation(t, ctx, st, repo.ID, "shared-head", 2)
	var legacyReservations int64
	if err := st.DB().QueryRowContext(ctx, `
		select count(*)
		from thread_child_observation_reservations
		where family = 'workflow_runs'
	`).Scan(&legacyReservations); err != nil {
		t.Fatalf("legacy workflow reservations: %v", err)
	}
	if legacyReservations != 0 {
		t.Fatalf("legacy workflow reservations = %d, want none", legacyReservations)
	}
}

func TestSyncWorkflowRunsAcceptsVerifiedDeletionWithoutRegressingSnapshotClock(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	initial := New(sameHeadWorkflowGitHub{number: 9, version: 2}, st)
	initial.now = func() time.Time { return time.Date(2026, 7, 12, 1, 0, 0, 0, time.UTC) }
	if _, err := initial.Sync(ctx, Options{
		Owner: "openclaw", Repo: "gitcrawl", State: "open",
		Numbers: []int{9}, IncludePRDetails: true,
	}); err != nil {
		t.Fatalf("initial workflow sync: %v", err)
	}

	deleted := New(sameHeadWorkflowGitHub{
		number: 9, version: 3, deletedRun: "901",
	}, st)
	deleted.now = func() time.Time { return time.Date(2026, 7, 12, 1, 1, 0, 0, time.UTC) }
	if _, err := deleted.Sync(ctx, Options{
		Owner: "openclaw", Repo: "gitcrawl", State: "open",
		Numbers: []int{9}, IncludePRDetails: true,
	}); err != nil {
		t.Fatalf("verified deletion sync: %v", err)
	}
	repo, err := st.RepositoryByFullName(ctx, "openclaw/gitcrawl")
	if err != nil {
		t.Fatalf("repository: %v", err)
	}
	runs, err := st.ListWorkflowRuns(ctx, repo.ID, store.WorkflowRunListOptions{
		HeadSHA: "shared-head",
		Limit:   -1,
	})
	if err != nil {
		t.Fatalf("workflow runs: %v", err)
	}
	if len(runs) != 1 || runs[0].RunID != "900" {
		t.Fatalf("workflow runs after verified deletion = %+v", runs)
	}
	source, sequence, found, err := st.WorkflowRunObservationReservation(
		ctx,
		repo.ID,
		"shared-head",
	)
	if err != nil || !found {
		t.Fatalf("workflow reservation = %q/%d found=%t err=%v", source, sequence, found, err)
	}
	if source != "2026-07-12T00:02:00Z" || sequence != 2 {
		t.Fatalf("workflow reservation after deletion = %s/%d", source, sequence)
	}

	replayed := New(sameHeadWorkflowGitHub{
		number: 9, version: 2, deletedRun: "901",
	}, st)
	replayStats, err := replayed.Sync(ctx, Options{
		Owner: "openclaw", Repo: "gitcrawl", State: "open",
		Numbers: []int{9}, IncludePRDetails: true,
	})
	if err != nil {
		t.Fatalf("stale deleted-run replay: %v", err)
	}
	if replayStats.WorkflowRunsSynced != 0 {
		t.Fatalf("stale deleted-run replay stats = %#v", replayStats)
	}
	runs, err = st.ListWorkflowRuns(ctx, repo.ID, store.WorkflowRunListOptions{
		HeadSHA: "shared-head",
		Limit:   -1,
	})
	if err != nil {
		t.Fatalf("workflow runs after stale replay: %v", err)
	}
	if len(runs) != 1 || runs[0].RunID != "900" {
		t.Fatalf("deleted workflow run was resurrected: %+v", runs)
	}
}

func TestSyncWorkflowRunsCommitTimeCASRejectsStaleResurrection(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	initial := New(sameHeadWorkflowGitHub{number: 9, version: 2}, st)
	if _, err := initial.Sync(ctx, Options{
		Owner: "openclaw", Repo: "gitcrawl", State: "open",
		Numbers: []int{9}, IncludePRDetails: true,
	}); err != nil {
		t.Fatalf("initial workflow sync: %v", err)
	}

	deletionFetched := make(chan struct{})
	releaseDeletion := make(chan struct{})
	deletion := New(sameHeadWorkflowGitHub{
		number: 9, version: 3, deletedRun: "901",
		fetched: deletionFetched, release: releaseDeletion,
	}, st)
	type syncResult struct {
		stats Stats
		err   error
	}
	deletionResult := make(chan syncResult, 1)
	go func() {
		stats, err := deletion.Sync(ctx, Options{
			Owner: "openclaw", Repo: "gitcrawl", State: "open",
			Numbers: []int{9}, IncludePRDetails: true,
		})
		deletionResult <- syncResult{stats: stats, err: err}
	}()
	awaitSyncSignal(t, deletionFetched, "deletion workflow fetch")

	staleValidated := make(chan struct{})
	releaseStale := make(chan struct{})
	stale := New(sameHeadWorkflowGitHub{number: 9, version: 2}, st)
	stale.beforePersist = func() {
		close(staleValidated)
		<-releaseStale
	}
	staleResult := make(chan syncResult, 1)
	go func() {
		stats, err := stale.Sync(ctx, Options{
			Owner: "openclaw", Repo: "gitcrawl", State: "open",
			Numbers: []int{9}, IncludePRDetails: true,
		})
		staleResult <- syncResult{stats: stats, err: err}
	}()
	awaitSyncSignal(t, staleValidated, "stale workflow validation")

	close(releaseDeletion)
	deleted := <-deletionResult
	if deleted.err != nil {
		t.Fatalf("verified deletion sync: %v", deleted.err)
	}
	if deleted.stats.WorkflowRunsSynced != 1 {
		t.Fatalf("verified deletion stats = %#v", deleted.stats)
	}
	close(releaseStale)
	replayed := <-staleResult
	if replayed.err != nil {
		t.Fatalf("stale workflow sync: %v", replayed.err)
	}
	if replayed.stats.WorkflowRunsSynced != 0 {
		t.Fatalf("stale workflow stats = %#v", replayed.stats)
	}

	repo, err := st.RepositoryByFullName(ctx, "openclaw/gitcrawl")
	if err != nil {
		t.Fatalf("repository: %v", err)
	}
	runs, err := st.ListWorkflowRuns(ctx, repo.ID, store.WorkflowRunListOptions{
		HeadSHA: "shared-head",
		Limit:   -1,
	})
	if err != nil {
		t.Fatalf("workflow runs: %v", err)
	}
	if len(runs) != 1 || runs[0].RunID != "900" {
		t.Fatalf("stale higher-sequence snapshot resurrected deletion: %+v", runs)
	}
	_, sequence, found, err := st.WorkflowRunObservationReservation(
		ctx,
		repo.ID,
		"shared-head",
	)
	if err != nil || !found || sequence != 2 {
		t.Fatalf("workflow reservation sequence = %d found=%t err=%v, want 2", sequence, found, err)
	}
}

func TestSyncWorkflowRunsVerifiesReappearingRun(t *testing.T) {
	tests := []struct {
		name       string
		exact      map[string]any
		err        error
		wantSynced int
		wantErr    string
	}{
		{
			name: "current run",
			exact: map[string]any{
				"id":         901,
				"created_at": "2026-07-12T00:01:00Z",
				"updated_at": "2026-07-12T00:02:00Z",
			},
			wantSynced: 2,
		},
		{
			name: "still deleted",
			err: &gh.RequestError{
				Method: "GET",
				URL:    "/repos/openclaw/gitcrawl/actions/runs/901",
				Status: 404,
			},
		},
		{
			name: "newer exact run",
			exact: map[string]any{
				"id":         901,
				"created_at": "2026-07-12T00:01:00Z",
				"updated_at": "2026-07-12T00:03:00Z",
			},
		},
		{
			name: "lookup failure",
			err: &gh.RequestError{
				Method: "GET",
				URL:    "/repos/openclaw/gitcrawl/actions/runs/901",
				Status: 500,
			},
			wantErr: "verify reappearing workflow run 901",
		},
		{
			name: "mismatched run",
			exact: map[string]any{
				"id":         902,
				"created_at": "2026-07-12T00:01:00Z",
				"updated_at": "2026-07-12T00:02:00Z",
			},
			wantErr: "exact lookup returned 902",
		},
		{
			name: "malformed source",
			exact: map[string]any{
				"id":         901,
				"created_at": "2026-07-12T00:01:00Z",
				"updated_at": "not-a-timestamp",
			},
			wantErr: "verify reappearing workflow run 901 source",
		},
		{
			name:    "missing source",
			exact:   map[string]any{"id": 901},
			wantErr: "missing created_at and updated_at",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
			if err != nil {
				t.Fatalf("open store: %v", err)
			}
			defer st.Close()

			initial := New(sameHeadWorkflowGitHub{number: 9, version: 2}, st)
			if _, err := initial.Sync(ctx, Options{
				Owner: "openclaw", Repo: "gitcrawl", State: "open",
				Numbers: []int{9}, IncludePRDetails: true,
			}); err != nil {
				t.Fatalf("initial workflow sync: %v", err)
			}
			deleted := New(sameHeadWorkflowGitHub{
				number: 9, version: 3, deletedRun: "901",
			}, st)
			if _, err := deleted.Sync(ctx, Options{
				Owner: "openclaw", Repo: "gitcrawl", State: "open",
				Numbers: []int{9}, IncludePRDetails: true,
			}); err != nil {
				t.Fatalf("verified deletion sync: %v", err)
			}

			replayed := New(workflowLookupResultGitHub{
				sameHeadWorkflowGitHub: sameHeadWorkflowGitHub{number: 9, version: 2},
				exact:                  tt.exact,
				err:                    tt.err,
			}, st)
			stats, err := replayed.Sync(ctx, Options{
				Owner: "openclaw", Repo: "gitcrawl", State: "open",
				Numbers: []int{9}, IncludePRDetails: true,
			})
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("reappearing run error = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("reappearing run sync: %v", err)
			}
			if stats.WorkflowRunsSynced != tt.wantSynced {
				t.Fatalf("reappearing run stats = %#v, want synced=%d", stats, tt.wantSynced)
			}
		})
	}
}

func TestSyncWorkflowRunsEqualReservationDoesNotReplaceSameSyncSnapshot(t *testing.T) {
	for _, test := range []struct {
		name        string
		subsetFirst bool
		wantLookups int
	}{
		{name: "subset then superset", subsetFirst: true},
		{name: "superset then subset", wantLookups: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
			if err != nil {
				t.Fatalf("open store: %v", err)
			}
			defer st.Close()

			client := &sameSyncSharedHeadGitHub{subsetFirst: test.subsetFirst}
			s := New(client, st)
			stats, err := s.Sync(ctx, Options{
				Owner: "openclaw", Repo: "gitcrawl", State: "open",
				Numbers: []int{8, 9}, IncludePRDetails: true,
			})
			if err != nil {
				t.Fatalf("same-sync shared-head sync: %v", err)
			}
			if stats.PRDetailsSynced != 2 || stats.WorkflowRunsSynced != 2 {
				t.Fatalf("same-sync shared-head stats = %#v", stats)
			}
			if client.lookupCalls != test.wantLookups {
				t.Fatalf(
					"exact workflow lookups = %d, want %d",
					client.lookupCalls,
					test.wantLookups,
				)
			}
			repo, err := st.RepositoryByFullName(ctx, "openclaw/gitcrawl")
			if err != nil {
				t.Fatalf("repository: %v", err)
			}
			runs, err := st.ListWorkflowRuns(ctx, repo.ID, store.WorkflowRunListOptions{
				HeadSHA: "same-sync-head",
				Limit:   -1,
			})
			if err != nil {
				t.Fatalf("workflow runs: %v", err)
			}
			runIDs := make(map[string]bool, len(runs))
			for _, run := range runs {
				runIDs[run.RunID] = true
			}
			if len(runs) != 2 || !runIDs["900"] || !runIDs["901"] {
				t.Fatalf("equal reservation lost a live run: %+v", runs)
			}
		})
	}
}

func TestSyncWorkflowRunsLaterSiblingTombstonesNewlyObservedRun(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	client := &sameSyncSharedHeadGitHub{verifiedDeletion: true}
	stats, err := New(client, st).Sync(ctx, Options{
		Owner: "openclaw", Repo: "gitcrawl", State: "open",
		Numbers: []int{8, 9}, IncludePRDetails: true,
	})
	if err != nil {
		t.Fatalf("same-generation new-run deletion sync: %v", err)
	}
	if stats.PRDetailsSynced != 2 || stats.WorkflowRunsSynced != 1 {
		t.Fatalf("same-generation new-run deletion stats = %#v", stats)
	}
	if client.lookupCalls != 1 {
		t.Fatalf("exact workflow lookups = %d, want 1", client.lookupCalls)
	}

	repo, err := st.RepositoryByFullName(ctx, "openclaw/gitcrawl")
	if err != nil {
		t.Fatalf("repository: %v", err)
	}
	runs, err := st.ListWorkflowRuns(ctx, repo.ID, store.WorkflowRunListOptions{
		HeadSHA: "same-sync-head",
		Limit:   -1,
	})
	if err != nil {
		t.Fatalf("workflow runs: %v", err)
	}
	if len(runs) != 1 || runs[0].RunID != "900" {
		t.Fatalf("later sibling deletion resurrected new run: %+v", runs)
	}
	assertWorkflowRunReservation(t, ctx, st, repo.ID, "same-sync-head", 1)
}

func TestConsolidateWorkflowSnapshotsTombstonesDeletionAcrossStaleReappearanceOrders(t *testing.T) {
	runRow := func(id int, updatedAt string) map[string]any {
		return map[string]any{
			"id":          id,
			"run_number":  id - 899,
			"head_branch": "same-sync-branch",
			"head_sha":    "same-sync-head",
			"status":      "completed",
			"conclusion":  "success",
			"name":        fmt.Sprintf("workflow-%d", id),
			"event":       "pull_request",
			"created_at":  "2026-07-12T00:00:00Z",
			"updated_at":  updatedAt,
		}
	}
	makePayloads := func(reverse bool) []threadSyncPayload {
		pull := map[string]any{"head": map[string]any{"sha": "same-sync-head"}}
		observed := threadSyncPayload{
			hasPullDetails: true,
			pullDetails: pullRequestDetailRows{
				workflowSourceUpdatedAt:  "2026-07-12T00:02:00Z",
				workflowSnapshotFresh:    true,
				workflowObservationOrder: 1,
				pull:                     pull,
				runsRaw: []map[string]any{
					runRow(900, "2026-07-12T00:02:00Z"),
					runRow(901, "2026-07-12T00:01:00Z"),
				},
			},
		}
		deleted := threadSyncPayload{
			hasPullDetails: true,
			pullDetails: pullRequestDetailRows{
				workflowSourceUpdatedAt:  "2026-07-12T00:02:00Z",
				workflowSnapshotFresh:    true,
				workflowObservationOrder: 2,
				pull:                     pull,
				runsRaw: []map[string]any{
					runRow(900, "2026-07-12T00:02:00Z"),
				},
			},
		}
		staleReappearance := threadSyncPayload{
			hasPullDetails: true,
			pullDetails: pullRequestDetailRows{
				workflowSourceUpdatedAt:  "2026-07-12T00:02:00Z",
				workflowSnapshotFresh:    true,
				workflowObservationOrder: 3,
				pull:                     pull,
				runsRaw: []map[string]any{
					runRow(900, "2026-07-12T00:02:00Z"),
					runRow(901, "2026-07-12T00:01:00Z"),
				},
			},
		}
		if reverse {
			return []threadSyncPayload{staleReappearance, deleted, observed}
		}
		return []threadSyncPayload{observed, deleted, staleReappearance}
	}

	for _, test := range []struct {
		name    string
		reverse bool
	}{
		{name: "observation order"},
		{name: "reverse persistence order", reverse: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			payloads := makePayloads(test.reverse)
			client := workflowLookupResultGitHub{
				err: &gh.RequestError{
					Method: "GET",
					URL:    "/repos/openclaw/gitcrawl/actions/runs/901",
					Status: 404,
				},
			}
			s := New(client, nil)
			if err := s.consolidateWorkflowSnapshots(ctx, Options{
				Owner: "openclaw",
				Repo:  "gitcrawl",
			}, payloads); err != nil {
				t.Fatalf("consolidate workflow snapshots: %v", err)
			}
			for index := range payloads {
				rows := payloads[index].pullDetails
				if len(rows.runsRaw) != 1 || jsonID(rows.runsRaw[0]["id"]) != "900" {
					t.Fatalf("payload %d workflow rows = %#v", index, rows.runsRaw)
				}
				if len(rows.workflowDeletedRunIDs) != 1 ||
					rows.workflowDeletedRunIDs[0] != "901" {
					t.Fatalf(
						"payload %d workflow tombstones = %v",
						index,
						rows.workflowDeletedRunIDs,
					)
				}
			}

			st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
			if err != nil {
				t.Fatalf("open store: %v", err)
			}
			defer st.Close()
			repoID, err := st.UpsertRepository(ctx, store.Repository{
				Owner:     "openclaw",
				Name:      "gitcrawl",
				FullName:  "openclaw/gitcrawl",
				RawJSON:   "{}",
				UpdatedAt: "2026-07-12T00:03:00Z",
			})
			if err != nil {
				t.Fatalf("seed repository: %v", err)
			}
			for index := range payloads {
				rows := payloads[index].pullDetails
				if _, err := st.ApplyWorkflowRunSnapshot(
					ctx,
					repoID,
					"same-sync-head",
					rows.workflowSourceUpdatedAt,
					1,
					rows.workflowBaseline,
					mapWorkflowRuns(repoID, rows.runsRaw, "2026-07-12T00:03:00Z"),
				); err != nil {
					t.Fatalf("persist payload %d: %v", index, err)
				}
			}
			runs, err := st.ListWorkflowRuns(ctx, repoID, store.WorkflowRunListOptions{
				HeadSHA: "same-sync-head",
				Limit:   -1,
			})
			if err != nil {
				t.Fatalf("list persisted workflow runs: %v", err)
			}
			if len(runs) != 1 || runs[0].RunID != "900" {
				t.Fatalf("persistence order resurrected deletion: %+v", runs)
			}
		})
	}
}

func TestSyncWorkflowRunsSameGenerationVerifiedDeletionWinsSiblingSnapshot(t *testing.T) {
	for _, test := range []struct {
		name        string
		subsetFirst bool
	}{
		{name: "verified deletion then live sibling", subsetFirst: true},
		{name: "live sibling then verified deletion"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
			if err != nil {
				t.Fatalf("open store: %v", err)
			}
			defer st.Close()

			initial := New(&sameSyncSharedHeadGitHub{verifiedDeletion: true}, st)
			if _, err := initial.Sync(ctx, Options{
				Owner: "openclaw", Repo: "gitcrawl", State: "open",
				Numbers: []int{8}, IncludePRDetails: true,
			}); err != nil {
				t.Fatalf("initial workflow sync: %v", err)
			}

			client := &sameSyncSharedHeadGitHub{
				subsetFirst:      test.subsetFirst,
				verifiedDeletion: true,
			}
			stats, err := New(client, st).Sync(ctx, Options{
				Owner: "openclaw", Repo: "gitcrawl", State: "open",
				Numbers: []int{8, 9}, IncludePRDetails: true,
			})
			if err != nil {
				t.Fatalf("same-generation deletion sync: %v", err)
			}
			if stats.PRDetailsSynced != 2 || stats.WorkflowRunsSynced != 1 {
				t.Fatalf("same-generation deletion stats = %#v", stats)
			}
			if client.lookupCalls != 1 {
				t.Fatalf("exact workflow lookups = %d, want 1", client.lookupCalls)
			}

			repo, err := st.RepositoryByFullName(ctx, "openclaw/gitcrawl")
			if err != nil {
				t.Fatalf("repository: %v", err)
			}
			runs, err := st.ListWorkflowRuns(ctx, repo.ID, store.WorkflowRunListOptions{
				HeadSHA: "same-sync-head",
				Limit:   -1,
			})
			if err != nil {
				t.Fatalf("workflow runs: %v", err)
			}
			if len(runs) != 1 || runs[0].RunID != "900" {
				t.Fatalf("verified deletion was resurrected: %+v", runs)
			}
			assertWorkflowRunReservation(t, ctx, st, repo.ID, "same-sync-head", 2)
		})
	}
}

func TestSyncWorkflowRunsRejectsRegressedMemberDespiteNewerSibling(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	initial := New(sameHeadWorkflowGitHub{number: 9, version: 2}, st)
	if _, err := initial.Sync(ctx, Options{
		Owner: "openclaw", Repo: "gitcrawl", State: "open",
		Numbers: []int{9}, IncludePRDetails: true,
	}); err != nil {
		t.Fatalf("initial workflow sync: %v", err)
	}

	regressed := New(sameHeadWorkflowGitHub{number: 9, version: 4}, st)
	stats, err := regressed.Sync(ctx, Options{
		Owner: "openclaw", Repo: "gitcrawl", State: "open",
		Numbers: []int{9}, IncludePRDetails: true,
	})
	if err != nil {
		t.Fatalf("regressed workflow sync: %v", err)
	}
	if stats.WorkflowRunsSynced != 0 {
		t.Fatalf("regressed workflow stats = %#v", stats)
	}
	repo, err := st.RepositoryByFullName(ctx, "openclaw/gitcrawl")
	if err != nil {
		t.Fatalf("repository: %v", err)
	}
	runs, err := st.ListWorkflowRuns(ctx, repo.ID, store.WorkflowRunListOptions{
		HeadSHA: "shared-head",
		Limit:   -1,
	})
	if err != nil {
		t.Fatalf("workflow runs: %v", err)
	}
	if len(runs) != 2 {
		t.Fatalf("workflow runs = %+v, want original two-run snapshot", runs)
	}
	for _, run := range runs {
		if run.RunID == "900" && run.WorkflowName != "new CI" {
			t.Fatalf("workflow run 900 regressed: %+v", run)
		}
		if run.RunID == "902" {
			t.Fatalf("newer sibling admitted stale snapshot: %+v", runs)
		}
	}
}

func TestWorkflowSnapshotRejectsInvalidSourceTimestamp(t *testing.T) {
	for _, test := range []struct {
		name    string
		row     map[string]any
		wantErr string
	}{
		{
			name: "malformed",
			row: map[string]any{
				"id":         900,
				"updated_at": "not-a-timestamp",
				"created_at": "2026-07-12T00:00:00Z",
			},
			wantErr: "invalid timestamp",
		},
		{
			name:    "missing both clocks",
			row:     map[string]any{"id": 900},
			wantErr: "missing created_at and updated_at",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, _, err := workflowSnapshotOrder([]map[string]any{test.row})
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("workflow timestamp error = %v, want %q", err, test.wantErr)
			}
		})
	}
}

func runInterleavedSyncs(
	t *testing.T,
	ctx context.Context,
	s *Syncer,
	options Options,
	firstFetched, secondFetched, releaseFirst, releaseSecond chan struct{},
	secondPersistsFirst bool,
) {
	t.Helper()
	type result struct {
		stats Stats
		err   error
	}
	firstResult := make(chan result, 1)
	go func() {
		stats, err := s.Sync(ctx, options)
		firstResult <- result{stats: stats, err: err}
	}()
	awaitSyncSignal(t, firstFetched, "lower-sequence hydration fetch")

	secondResult := make(chan result, 1)
	go func() {
		stats, err := s.Sync(ctx, options)
		secondResult <- result{stats: stats, err: err}
	}()
	awaitSyncSignal(t, secondFetched, "higher-sequence hydration fetch")

	if secondPersistsFirst {
		close(releaseSecond)
		if second := <-secondResult; second.err != nil {
			t.Fatalf("higher-sequence sync: %v", second.err)
		}
		close(releaseFirst)
		if first := <-firstResult; first.err != nil {
			t.Fatalf("lower-sequence sync: %v", first.err)
		}
		return
	}
	close(releaseFirst)
	if first := <-firstResult; first.err != nil {
		t.Fatalf("lower-sequence sync: %v", first.err)
	}
	close(releaseSecond)
	if second := <-secondResult; second.err != nil {
		t.Fatalf("higher-sequence sync: %v", second.err)
	}
}

func awaitSyncSignal(t *testing.T, signal <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", label)
	}
}

func assertChildReservation(
	t *testing.T,
	ctx context.Context,
	st *store.Store,
	threadID int64,
	family store.ThreadChildObservationFamily,
	want int64,
) {
	t.Helper()
	var got int64
	if err := st.DB().QueryRowContext(ctx, `
		select observation_sequence
		from thread_child_observation_reservations
		where thread_id = ? and family = ?
	`, threadID, family).Scan(&got); err != nil {
		t.Fatalf("read %s reservation: %v", family, err)
	}
	if got != want {
		t.Fatalf("%s reservation = %d, want %d", family, got, want)
	}
}

func assertWorkflowRunReservation(
	t *testing.T,
	ctx context.Context,
	st *store.Store,
	repoID int64,
	headSHA string,
	want int64,
) {
	t.Helper()
	var got int64
	if err := st.DB().QueryRowContext(ctx, `
		select observation_sequence
		from workflow_run_observation_reservations
		where repo_id = ? and head_sha = ?
	`, repoID, headSHA).Scan(&got); err != nil {
		t.Fatalf("read workflow reservation for %s: %v", headSHA, err)
	}
	if got != want {
		t.Fatalf("workflow reservation for %s = %d, want %d", headSHA, got, want)
	}
}

func assertVersionedPRHydration(
	t *testing.T,
	ctx context.Context,
	st *store.Store,
	version int,
	checkFiles bool,
) (store.Thread, store.PullRequestDetail) {
	t.Helper()
	repo, err := st.RepositoryByFullName(ctx, "openclaw/gitcrawl")
	if err != nil {
		t.Fatalf("repository: %v", err)
	}
	threads, err := st.ListThreads(ctx, repo.ID, true)
	if err != nil || len(threads) != 1 {
		t.Fatalf("threads = %+v, %v", threads, err)
	}
	thread := threads[0]
	detail, ok, err := st.PullRequestDetailByThread(ctx, thread.ID)
	if err != nil || !ok {
		t.Fatalf("detail = %+v, %t, %v", detail, ok, err)
	}
	if detail.HeadSHA != fmt.Sprintf("head-v%d", version) {
		t.Fatalf("detail = %+v, want version %d", detail, version)
	}
	if checkFiles {
		files, err := st.PullRequestFiles(ctx, thread.ID)
		if err != nil {
			t.Fatalf("files: %v", err)
		}
		if len(files) != 1 || files[0].Path != fmt.Sprintf("file-v%d.go", version) {
			t.Fatalf("files = %+v, want version %d", files, version)
		}
	}
	commits, err := st.PullRequestCommits(ctx, thread.ID)
	if err != nil {
		t.Fatalf("commits: %v", err)
	}
	if len(commits) != 1 || commits[0].SHA != fmt.Sprintf("commit-v%d", version) {
		t.Fatalf("commits = %+v, want version %d", commits, version)
	}
	checks, err := st.PullRequestChecks(ctx, thread.ID)
	if err != nil {
		t.Fatalf("checks: %v", err)
	}
	if len(checks) != 1 || checks[0].Name != fmt.Sprintf("check-v%d", version) {
		t.Fatalf("checks = %+v, want version %d", checks, version)
	}
	reviewThreads, err := st.PullRequestReviewThreads(ctx, thread.ID)
	if err != nil {
		t.Fatalf("review threads: %v", err)
	}
	if len(reviewThreads) != 1 ||
		reviewThreads[0].ReviewThreadID != fmt.Sprintf("review-thread-v%d", version) {
		t.Fatalf("review threads = %+v, want version %d", reviewThreads, version)
	}
	runs, err := st.ListWorkflowRuns(ctx, repo.ID, store.WorkflowRunListOptions{
		HeadSHA: detail.HeadSHA,
		Limit:   -1,
	})
	if err != nil {
		t.Fatalf("workflow runs: %v", err)
	}
	if len(runs) != 1 || runs[0].WorkflowName != fmt.Sprintf("workflow-v%d", version) {
		t.Fatalf("workflow runs = %+v, want version %d", runs, version)
	}
	return thread, detail
}

func TestSyncDelayedObservationCannotOverwriteNewerHydration(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	client := &delayedObservationGitHub{
		firstListStarted: make(chan struct{}),
		releaseFirstList: make(chan struct{}),
	}
	s := New(client, st)
	s.now = func() time.Time { return time.Date(2026, 7, 12, 0, 1, 0, 0, time.UTC) }
	options := Options{
		Owner:           "openclaw",
		Repo:            "gitcrawl",
		IncludeComments: true,
	}
	type syncResult struct {
		stats Stats
		err   error
	}
	firstResult := make(chan syncResult, 1)
	go func() {
		stats, err := s.Sync(ctx, options)
		firstResult <- syncResult{stats: stats, err: err}
	}()

	select {
	case <-client.firstListStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("first sync did not reach delayed issue fetch")
	}
	secondStats, secondErr := s.Sync(ctx, options)
	close(client.releaseFirstList)
	first := <-firstResult
	if secondErr != nil {
		t.Fatalf("newer sync: %v", secondErr)
	}
	if first.err != nil {
		t.Fatalf("delayed sync: %v", first.err)
	}
	if secondStats.ThreadsSynced != 1 || secondStats.ThreadsSkippedStale != 0 {
		t.Fatalf("newer sync stats = %#v", secondStats)
	}
	if first.stats.ThreadsSynced != 0 || first.stats.ThreadsSkippedStale != 1 ||
		first.stats.CommentsSynced != 0 || first.stats.EvidenceObserved != 0 {
		t.Fatalf("delayed sync stats = %#v", first.stats)
	}

	repo, err := st.RepositoryByFullName(ctx, "openclaw/gitcrawl")
	if err != nil {
		t.Fatalf("repository: %v", err)
	}
	threads, err := st.ListThreads(ctx, repo.ID, true)
	if err != nil {
		t.Fatalf("threads: %v", err)
	}
	if len(threads) != 1 || threads[0].Title != "state-b" || threads[0].Body != "state-b body" {
		t.Fatalf("canonical threads = %+v", threads)
	}
	comments, err := st.ListComments(ctx, threads[0].ID)
	if err != nil {
		t.Fatalf("comments: %v", err)
	}
	if len(comments) != 1 || comments[0].Body != "state-b comment" {
		t.Fatalf("canonical comments = %+v", comments)
	}
	var documentText string
	if err := st.DB().QueryRowContext(ctx, `
		select raw_text
		from documents
		where thread_id = ?
	`, threads[0].ID).Scan(&documentText); err != nil {
		t.Fatalf("document: %v", err)
	}
	if !strings.Contains(documentText, "state-b comment") || strings.Contains(documentText, "state-a comment") {
		t.Fatalf("canonical document = %q", documentText)
	}
	var revisions int
	if err := st.DB().QueryRowContext(ctx, `
		select count(*)
		from thread_revisions
		where thread_id = ?
	`, threads[0].ID).Scan(&revisions); err != nil {
		t.Fatalf("revisions: %v", err)
	}
	if revisions != 1 {
		t.Fatalf("revision count = %d, want 1", revisions)
	}
}

func TestSyncRollsBackThreadRevisionWhenFingerprintFails(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	if _, err := st.DB().ExecContext(ctx, `
		create trigger reject_synced_fingerprint
		before insert on thread_fingerprints
		begin
			select raise(abort, 'fingerprint rejected');
		end
	`); err != nil {
		t.Fatalf("trigger: %v", err)
	}

	s := New(fakeGitHub{}, st)
	s.now = func() time.Time { return time.Date(2026, 4, 26, 0, 0, 0, 0, time.UTC) }
	if _, err := s.Sync(ctx, Options{Owner: "openclaw", Repo: "gitcrawl", IncludeComments: true, IncludePRDetails: true}); err == nil || !strings.Contains(err.Error(), "fingerprint rejected") {
		t.Fatalf("sync error = %v", err)
	}
	for _, table := range []string{"repositories", "threads", "thread_revisions", "thread_fingerprints"} {
		var count int
		if err := st.DB().QueryRowContext(ctx, `select count(*) from `+table).Scan(&count); err != nil {
			t.Fatalf("%s count: %v", table, err)
		}
		if count != 0 {
			t.Fatalf("%s count = %d, want rollback", table, count)
		}
	}
}

func TestSyncWithCommentsRollsBackOnCommentFetchError(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	s := New(failingSecondCommentGitHub{}, st)
	s.now = func() time.Time { return time.Date(2026, 4, 26, 0, 0, 0, 0, time.UTC) }
	_, err = s.Sync(ctx, Options{Owner: "openclaw", Repo: "gitcrawl", IncludeComments: true})
	if err == nil || !strings.Contains(err.Error(), "comments unavailable") {
		t.Fatalf("sync error = %v", err)
	}
	if _, err := st.RepositoryByFullName(ctx, "openclaw/gitcrawl"); err == nil {
		t.Fatal("repository persisted after failed comment hydration")
	}
	assertTableRowCount(t, st, "repositories", 0)
	assertTableRowCount(t, st, "threads", 0)
	assertTableRowCount(t, st, "comments", 0)
	assertTableRowCount(t, st, "documents", 0)
	assertTableRowCount(t, st, "sync_runs", 0)
}

func TestMetadataOnlySyncPreservesCommentBackedDocumentText(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	client := &mutableUpdatedGitHub{commentUpdatedAt: "2026-04-28T00:00:00Z"}
	s := New(client, st)
	s.now = func() time.Time { return time.Date(2026, 4, 26, 0, 0, 0, 0, time.UTC) }
	if _, err := s.Sync(ctx, Options{Owner: "openclaw", Repo: "gitcrawl", IncludeComments: true, IncludePRDetails: true}); err != nil {
		t.Fatalf("sync with comments: %v", err)
	}
	repo, err := st.RepositoryByFullName(ctx, "openclaw/gitcrawl")
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	assertDocumentFTSCount(t, st, "same", 1)
	stats, err := s.Sync(ctx, Options{Owner: "openclaw", Repo: "gitcrawl"})
	if err != nil {
		t.Fatalf("same-source metadata sync: %v", err)
	}
	if !stats.MetadataOnly || stats.ThreadsSynced != 2 || stats.ThreadsSkippedStale != 0 {
		t.Fatalf("same-source metadata stats = %#v", stats)
	}
	coverage, err := st.ArchiveCoverage(ctx, store.ArchiveCoverageOptions{})
	if err != nil {
		t.Fatalf("same-source archive coverage: %v", err)
	}
	if len(coverage.Rows) != 1 ||
		coverage.Rows[0].Enrichment.Revisions.Fresh != 2 ||
		coverage.Rows[0].Enrichment.Fingerprints.Fresh != 2 {
		t.Fatalf("same-source metadata coverage = %+v", coverage.Rows)
	}

	client.updatedAt = "2026-04-27T00:00:00Z"
	stats, err = s.Sync(ctx, Options{Owner: "openclaw", Repo: "gitcrawl"})
	if err != nil {
		t.Fatalf("metadata sync: %v", err)
	}
	if !stats.MetadataOnly || stats.CommentsSynced != 0 || stats.RevisionsCreated != 0 || stats.FingerprintsUpserted != 0 {
		t.Fatalf("metadata stats = %#v", stats)
	}
	threads, err := st.ListThreads(ctx, repo.ID, true)
	if err != nil {
		t.Fatalf("threads after metadata sync: %v", err)
	}
	if len(threads) != 2 || !threads[1].IsDraft {
		t.Fatalf("metadata sync erased known draft state: %+v", threads)
	}
	assertDocumentFTSCount(t, st, "same", 1)
	coverage, err = st.ArchiveCoverage(ctx, store.ArchiveCoverageOptions{})
	if err != nil {
		t.Fatalf("archive coverage: %v", err)
	}
	if len(coverage.Rows) != 1 ||
		coverage.Rows[0].Enrichment.Revisions.Fresh != 1 ||
		coverage.Rows[0].Enrichment.Revisions.Stale != 1 ||
		coverage.Rows[0].Enrichment.Fingerprints.Fresh != 1 ||
		coverage.Rows[0].Enrichment.Fingerprints.Stale != 1 {
		t.Fatalf("metadata-only enrichment coverage = %+v", coverage)
	}
}

func TestMetadataOnlySyncHonorsAuthoritativeIssueDraftState(t *testing.T) {
	tests := []struct {
		name         string
		initialDraft bool
		nextPresent  bool
		nextDraft    bool
		wantDraft    bool
	}{
		{name: "draft to ready", initialDraft: true, nextPresent: true, nextDraft: false, wantDraft: false},
		{name: "ready to draft", initialDraft: false, nextPresent: true, nextDraft: true, wantDraft: true},
		{name: "absent preserves draft", initialDraft: true, nextPresent: false, wantDraft: true},
		{name: "absent preserves ready", initialDraft: false, nextPresent: false, wantDraft: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
			if err != nil {
				t.Fatalf("open store: %v", err)
			}
			defer st.Close()

			client := &metadataDraftGitHub{draftPresent: true, draft: test.initialDraft}
			s := New(client, st)
			s.now = func() time.Time { return time.Date(2026, 4, 26, 0, 0, 0, 0, time.UTC) }
			if _, err := s.Sync(ctx, Options{Owner: "openclaw", Repo: "gitcrawl"}); err != nil {
				t.Fatalf("initial metadata sync: %v", err)
			}
			assertStoredPullDraft(t, ctx, st, test.initialDraft)

			client.draftPresent = test.nextPresent
			client.draft = test.nextDraft
			stats, err := s.Sync(ctx, Options{Owner: "openclaw", Repo: "gitcrawl"})
			if err != nil {
				t.Fatalf("updated metadata sync: %v", err)
			}
			if !stats.MetadataOnly {
				t.Fatalf("metadata only = false: %#v", stats)
			}
			assertStoredPullDraft(t, ctx, st, test.wantDraft)
		})
	}
}

func assertStoredPullDraft(t *testing.T, ctx context.Context, st *store.Store, want bool) {
	t.Helper()
	repo, err := st.RepositoryByFullName(ctx, "openclaw/gitcrawl")
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	threads, err := st.ListThreads(ctx, repo.ID, true)
	if err != nil {
		t.Fatalf("list threads: %v", err)
	}
	for _, thread := range threads {
		if thread.Kind != "pull_request" {
			continue
		}
		if thread.IsDraft != want {
			t.Fatalf("pull request draft = %t, want %t", thread.IsDraft, want)
		}
		return
	}
	t.Fatal("pull request was not stored")
}

func TestCommentHydrationReplacesDeletedCommentsWithEmptySnapshot(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	client := &mutableCommentGitHub{commentUpdatedAt: "2026-04-26T00:05:00Z"}
	s := New(client, st)
	s.now = func() time.Time { return time.Date(2026, 4, 26, 0, 0, 0, 0, time.UTC) }
	options := Options{Owner: "openclaw", Repo: "gitcrawl", Numbers: []int{7}, IncludeComments: true}
	if _, err := s.Sync(ctx, options); err != nil {
		t.Fatalf("initial comment sync: %v", err)
	}
	assertDocumentFTSCount(t, st, "same", 1)

	client.empty = true
	stats, err := s.Sync(ctx, options)
	if err != nil {
		t.Fatalf("empty comment sync: %v", err)
	}
	if stats.CommentsSynced != 0 || stats.RevisionsCreated != 1 {
		t.Fatalf("empty comment sync stats = %#v", stats)
	}
	assertTableRowCount(t, st, "comments", 0)
	assertDocumentFTSCount(t, st, "same", 0)
	coverage, err := st.ArchiveCoverage(ctx, store.ArchiveCoverageOptions{})
	if err != nil {
		t.Fatalf("archive coverage after empty snapshot: %v", err)
	}
	if len(coverage.Rows) != 1 ||
		coverage.Rows[0].Enrichment.Revisions.Fresh != 1 ||
		coverage.Rows[0].Enrichment.Revisions.Stale != 0 {
		t.Fatalf("revision coverage after empty snapshot = %+v", coverage.Rows)
	}

	if _, err := s.Sync(ctx, Options{Owner: "openclaw", Repo: "gitcrawl", Numbers: []int{7}}); err != nil {
		t.Fatalf("metadata sync after empty snapshot: %v", err)
	}
	assertTableRowCount(t, st, "comments", 0)
	assertDocumentFTSCount(t, st, "same", 0)
}

func TestSyncHydratesPullReviewComments(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	s := New(pullCommentGitHub{}, st)
	s.now = func() time.Time { return time.Date(2026, 4, 26, 0, 0, 0, 0, time.UTC) }
	stats, err := s.Sync(ctx, Options{Owner: "openclaw", Repo: "gitcrawl", Numbers: []int{8}, IncludeComments: true})
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if stats.CommentsSynced != 2 {
		t.Fatalf("comments synced = %d, want 2", stats.CommentsSynced)
	}
	repo, err := st.RepositoryByFullName(ctx, "openclaw/gitcrawl")
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	threads, err := st.ListThreads(ctx, repo.ID, true)
	if err != nil {
		t.Fatalf("threads: %v", err)
	}
	if len(threads) != 1 || threads[0].Kind != "pull_request" {
		t.Fatalf("threads = %+v", threads)
	}
	comments, err := st.ListComments(ctx, threads[0].ID)
	if err != nil {
		t.Fatalf("comments: %v", err)
	}
	var foundBodylessReview bool
	for _, comment := range comments {
		if comment.CommentType == "pull_review" && comment.GitHubID == "81" && comment.Body == "" && comment.ReviewState == "APPROVED" {
			foundBodylessReview = true
		}
	}
	if !foundBodylessReview {
		t.Fatalf("bodyless pull review was not persisted: %+v", comments)
	}
}

func TestSyncUsesCompleteWorkflowRunsForRevisionEvidence(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	client := &completeWorkflowRunsGitHub{}
	s := New(client, st)
	stats, err := s.Sync(ctx, Options{
		Owner: "openclaw", Repo: "gitcrawl", Numbers: []int{8}, IncludeComments: true, IncludePRDetails: true,
	})
	if err != nil {
		t.Fatalf("sync pull request details: %v", err)
	}
	if client.limit != 0 || stats.WorkflowRunsSynced != 25 {
		t.Fatalf("workflow run fetch limit=%d synced=%d", client.limit, stats.WorkflowRunsSynced)
	}
	repo, err := st.RepositoryByFullName(ctx, "openclaw/gitcrawl")
	if err != nil {
		t.Fatalf("repository: %v", err)
	}
	runs, err := st.ListWorkflowRuns(ctx, repo.ID, store.WorkflowRunListOptions{HeadSHA: "head-sha", Limit: -1})
	if err != nil {
		t.Fatalf("list workflow runs: %v", err)
	}
	if len(runs) != 25 {
		t.Fatalf("workflow runs = %d, want 25", len(runs))
	}
	var revisions int
	if err := st.DB().QueryRowContext(ctx, `
		select count(*)
		from thread_revisions tr
		join threads t on t.id = tr.thread_id
		where t.repo_id = ? and t.number = 8
	`, repo.ID).Scan(&revisions); err != nil {
		t.Fatalf("revision count: %v", err)
	}
	if revisions != 1 {
		t.Fatalf("revision count = %d, want 1", revisions)
	}
}

func TestSyncHydratesPullRequestDetails(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	s := New(pullDetailsGitHub{}, st)
	s.now = func() time.Time { return time.Date(2026, 4, 26, 0, 0, 0, 0, time.UTC) }
	stats, err := s.Sync(ctx, Options{Owner: "openclaw", Repo: "gitcrawl", Numbers: []int{8}, IncludePRDetails: true})
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if stats.ReviewThreadsSynced != 1 || stats.PRDetailsSynced != 1 || stats.PRFilesSynced != 1 || stats.PRCommitsSynced != 1 || stats.PRChecksSynced != 1 || stats.WorkflowRunsSynced != 1 {
		t.Fatalf("stats = %#v", stats)
	}
	repo, err := st.RepositoryByFullName(ctx, "openclaw/gitcrawl")
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	cache, err := st.PullRequestCache(ctx, repo.ID, 8)
	if err != nil {
		t.Fatalf("pr cache: %v", err)
	}
	if cache.Detail.HeadSHA != "head-sha" || len(cache.Files) != 1 || len(cache.Commits) != 1 || len(cache.Checks) != 1 {
		t.Fatalf("cache = %+v", cache)
	}
	runs, err := st.ListWorkflowRuns(ctx, repo.ID, store.WorkflowRunListOptions{HeadSHA: "head-sha", Limit: 10})
	if err != nil {
		t.Fatalf("workflow runs: %v", err)
	}
	if len(runs) != 1 || runs[0].RunID != "99" {
		t.Fatalf("runs = %+v", runs)
	}
	threads, err := st.ListThreads(ctx, repo.ID, true)
	if err != nil {
		t.Fatalf("threads: %v", err)
	}
	if len(threads) != 1 {
		t.Fatalf("threads = %+v", threads)
	}
	reviewThreads, err := st.PullRequestReviewThreads(ctx, threads[0].ID)
	if err != nil {
		t.Fatalf("review threads: %v", err)
	}
	if len(reviewThreads) != 1 || reviewThreads[0].ReviewThreadID != "PRRT_8" {
		t.Fatalf("review threads = %+v", reviewThreads)
	}
	fetchedAt, err := st.PullRequestReviewThreadsFetchedAt(ctx, threads[0].ID)
	if err != nil {
		t.Fatalf("review thread marker: %v", err)
	}
	if fetchedAt == "" {
		t.Fatal("missing review thread sync marker")
	}
	assertDocumentFTSCount(t, st, "internal cache", 1)
	assertDocumentFTSCount(t, st, "feat cache", 1)
}

func TestPullRequestDetailsUseFetchTimestampWhenPersistedLater(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repoID, err := st.UpsertRepository(ctx, store.Repository{
		Owner:     "openclaw",
		Name:      "gitcrawl",
		FullName:  "openclaw/gitcrawl",
		RawJSON:   "{}",
		UpdatedAt: "2026-04-26T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("seed repo: %v", err)
	}
	issueRow, err := fakeGitHub{}.GetIssue(ctx, "openclaw", "gitcrawl", 8, nil)
	thread := mapIssueToThread(repoID, mustIssue(t, issueRow, err), "2026-04-26T00:00:00Z")
	threadID, err := st.UpsertThread(ctx, thread)
	if err != nil {
		t.Fatalf("seed thread: %v", err)
	}
	thread.ID = threadID

	const fetchTime = "2026-04-26T00:01:00Z"
	const persistTime = "2026-04-26T00:05:00Z"
	s := New(pullDetailsGitHub{}, st)
	s.now = func() time.Time { return mustTime(t, fetchTime) }
	rows, err := s.fetchPullRequestDetails(ctx, Options{Owner: "openclaw", Repo: "gitcrawl"}, 8)
	if err != nil {
		t.Fatalf("fetch details: %v", err)
	}
	s.now = func() time.Time { return mustTime(t, persistTime) }
	if _, err := s.persistPullRequestDetails(ctx, st, thread, rows, store.PullRequestHydrationFamilies{
		Details:      true,
		Files:        true,
		Commits:      true,
		Checks:       true,
		WorkflowRuns: true,
	}, 1); err != nil {
		t.Fatalf("persist details: %v", err)
	}

	cache, err := st.PullRequestCache(ctx, repoID, 8)
	if err != nil {
		t.Fatalf("pr cache: %v", err)
	}
	if cache.Detail.FetchedAt != fetchTime || cache.Detail.UpdatedAt != fetchTime {
		t.Fatalf("detail timestamps = fetched %q updated %q, want %q", cache.Detail.FetchedAt, cache.Detail.UpdatedAt, fetchTime)
	}
	if len(cache.Files) != 1 || cache.Files[0].FetchedAt != fetchTime {
		t.Fatalf("file timestamps = %+v, want %q", cache.Files, fetchTime)
	}
	if len(cache.Commits) != 1 || cache.Commits[0].FetchedAt != fetchTime {
		t.Fatalf("commit timestamps = %+v, want %q", cache.Commits, fetchTime)
	}
	if len(cache.Checks) != 1 || cache.Checks[0].FetchedAt != fetchTime {
		t.Fatalf("check timestamps = %+v, want %q", cache.Checks, fetchTime)
	}
	runs, err := st.ListWorkflowRuns(ctx, repoID, store.WorkflowRunListOptions{HeadSHA: "head-sha", Limit: 10})
	if err != nil {
		t.Fatalf("workflow runs: %v", err)
	}
	if len(runs) != 1 || runs[0].FetchedAt != fetchTime {
		t.Fatalf("run timestamps = %+v, want %q", runs, fetchTime)
	}
	assertDocumentFTSCount(t, st, "internal cache", 1)
	assertDocumentFTSCount(t, st, "feat cache", 1)
}

func TestSyncPullRequestDetailsFailsOnReviewThreadFetchError(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	s := New(failingReviewThreadsGitHub{}, st)
	s.now = func() time.Time { return time.Date(2026, 4, 26, 0, 0, 0, 0, time.UTC) }
	_, err = s.Sync(ctx, Options{Owner: "openclaw", Repo: "gitcrawl", Numbers: []int{8}, IncludePRDetails: true})
	if err == nil || !strings.Contains(err.Error(), "list pull request review threads for #8") {
		t.Fatalf("sync error = %v", err)
	}
	if _, err := st.RepositoryByFullName(ctx, "openclaw/gitcrawl"); err == nil {
		t.Fatal("repository persisted after failed PR detail hydration")
	}
	assertTableRowCount(t, st, "repositories", 0)
	assertTableRowCount(t, st, "threads", 0)
	assertTableRowCount(t, st, "pull_request_review_thread_syncs", 0)
	assertTableRowCount(t, st, "sync_runs", 0)
}

func TestSyncPullRequestDetailsSkipsCheckAndWorkflowFetchWithoutHeadSHA(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	client := &emptyHeadPullGitHub{}
	s := New(client, st)
	s.now = func() time.Time { return time.Date(2026, 4, 26, 0, 0, 0, 0, time.UTC) }
	stats, err := s.Sync(ctx, Options{Owner: "openclaw", Repo: "gitcrawl", Numbers: []int{8}, IncludePRDetails: true})
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if stats.PRDetailsSynced != 1 || stats.PRChecksSynced != 0 || stats.WorkflowRunsSynced != 0 {
		t.Fatalf("stats = %#v", stats)
	}
	if client.checksCalled {
		t.Fatal("check runs should not be fetched without head SHA")
	}
	if client.runsCalled {
		t.Fatal("workflow runs should not be fetched without head SHA")
	}
}

func TestSyncPullRequestDetailsDoesNotFetchInsideTransaction(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	client := &txProbePullDetailsGitHub{st: st}
	s := New(client, st)
	s.now = func() time.Time { return time.Date(2026, 4, 26, 0, 0, 0, 0, time.UTC) }
	if _, err := s.Sync(ctx, Options{Owner: "openclaw", Repo: "gitcrawl", Numbers: []int{8}, IncludePRDetails: true}); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if client.sawPersistedThreadReadErr != nil {
		t.Fatalf("probe persisted thread: %v", client.sawPersistedThreadReadErr)
	}
	if !client.sawMissingPersistedThread {
		t.Fatal("PR detail fetch saw repository writes before hydration finished")
	}
	if client.sawPersistedThread {
		t.Fatal("PR detail fetch saw persisted PR thread before hydration finished")
	}
	repo, err := st.RepositoryByFullName(ctx, "openclaw/gitcrawl")
	if err != nil {
		t.Fatalf("repo after sync: %v", err)
	}
	threads, err := st.ListThreads(ctx, repo.ID, true)
	if err != nil {
		t.Fatalf("threads after sync: %v", err)
	}
	if len(threads) != 1 || threads[0].Number != 8 {
		t.Fatalf("threads after sync = %+v", threads)
	}
}

func TestSyncCanTargetIssueNumbers(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	client := &targetedGitHub{}
	s := New(client, st)
	s.now = func() time.Time { return time.Date(2026, 4, 26, 0, 0, 0, 0, time.UTC) }
	stats, err := s.Sync(ctx, Options{Owner: "openclaw", Repo: "gitcrawl", Numbers: []int{7, 7, 8}, IncludeComments: true})
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if client.listCalled {
		t.Fatal("targeted sync should not call repository issue listing")
	}
	if got, want := client.numbers, []int{7, 8}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("targeted numbers: got %#v want %#v", got, want)
	}
	if stats.ThreadsSynced != 2 || stats.IssuesSynced != 1 || stats.PullRequestsSynced != 1 {
		t.Fatalf("unexpected stats: %#v", stats)
	}
	if stats.CommentsSynced != 1 {
		t.Fatalf("comments synced: got %d want 1", stats.CommentsSynced)
	}

	repo, err := st.RepositoryByFullName(ctx, "openclaw/gitcrawl")
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	threads, err := st.ListThreads(ctx, repo.ID, false)
	if err != nil {
		t.Fatalf("threads: %v", err)
	}
	if len(threads) != 2 {
		t.Fatalf("threads: got %d want 2", len(threads))
	}
}

func TestSyncNormalizesRelativeSince(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	client := &sinceCaptureGitHub{}
	s := New(client, st)
	s.now = func() time.Time { return time.Date(2026, 4, 27, 8, 30, 0, 0, time.UTC) }
	stats, err := s.Sync(ctx, Options{Owner: "openclaw", Repo: "gitcrawl", Since: "15m"})
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	want := "2026-04-27T08:15:00Z"
	if client.since != want || stats.RequestedSince != want {
		t.Fatalf("since: client=%q stats=%q want %q", client.since, stats.RequestedSince, want)
	}
}

func TestSyncRejectsInvalidSince(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	s := New(fakeGitHub{}, st)
	if _, err := s.Sync(ctx, Options{Owner: "openclaw", Repo: "gitcrawl", Since: "yesterday"}); err == nil {
		t.Fatal("expected invalid since to fail")
	}
}

func TestSyncPassesRequestedState(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	client := &stateCaptureGitHub{}
	s := New(client, st)
	if _, err := s.Sync(ctx, Options{Owner: "openclaw", Repo: "gitcrawl", State: "all"}); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if client.state != "all" {
		t.Fatalf("state = %q, want all", client.state)
	}
}

func TestSyncDefaultsToOpenState(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	client := &stateCaptureGitHub{}
	s := New(client, st)
	if _, err := s.Sync(ctx, Options{Owner: "openclaw", Repo: "gitcrawl"}); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if client.state != "open" {
		t.Fatalf("default state = %q, want open", client.state)
	}
}

func TestSyncRejectsInvalidState(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	s := New(fakeGitHub{}, st)
	if _, err := s.Sync(ctx, Options{Owner: "openclaw", Repo: "gitcrawl", State: "merged"}); err == nil {
		t.Fatal("expected invalid state to fail")
	}
}

func TestSyncOpenSinceAppliesClosedOverlapSweep(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repoID, err := st.UpsertRepository(ctx, store.Repository{
		Owner:     "openclaw",
		Name:      "gitcrawl",
		FullName:  "openclaw/gitcrawl",
		RawJSON:   "{}",
		UpdatedAt: "2026-04-26T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("seed repo: %v", err)
	}
	if _, err := st.UpsertThread(ctx, store.Thread{
		RepoID:          repoID,
		GitHubID:        "2",
		Number:          8,
		Kind:            "pull_request",
		State:           "open",
		Title:           "fix sync",
		HTMLURL:         "https://github.com/openclaw/gitcrawl/pull/8",
		LabelsJSON:      "[]",
		AssigneesJSON:   "[]",
		RawJSON:         "{}",
		ContentHash:     "old",
		CreatedAtGitHub: "2026-04-26T00:00:00Z",
		UpdatedAtGitHub: "2026-04-26T00:00:00Z",
		FirstPulledAt:   "2026-04-26T00:00:00Z",
		LastPulledAt:    "2026-04-26T00:00:00Z",
		UpdatedAt:       "2026-04-26T00:00:00Z",
	}); err != nil {
		t.Fatalf("seed thread: %v", err)
	}

	s := New(closedSweepGitHub{}, st)
	s.now = func() time.Time { return time.Date(2026, 4, 27, 1, 0, 0, 0, time.UTC) }
	stats, err := s.Sync(ctx, Options{
		Owner:            "openclaw",
		Repo:             "gitcrawl",
		Since:            "1h",
		IncludeComments:  true,
		IncludePRDetails: true,
	})
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if stats.ThreadsClosed != 1 {
		t.Fatalf("threads closed = %d, want 1", stats.ThreadsClosed)
	}
	if stats.ThreadsSynced != 2 || stats.IssuesSynced != 1 || stats.PullRequestsSynced != 1 {
		t.Fatalf("overlap threads were not included in sync denominators: %#v", stats)
	}
	if stats.PRDetailsSynced != 1 || stats.RevisionsCreated != 2 || stats.FingerprintsUpserted != 2 {
		t.Fatalf("overlap threads were not fully hydrated: %#v", stats)
	}
	threads, err := st.ListThreads(ctx, repoID, true)
	if err != nil {
		t.Fatalf("threads: %v", err)
	}
	var closedPull *store.Thread
	for index := range threads {
		if threads[index].Number == 8 {
			closedPull = &threads[index]
		}
	}
	if len(threads) != 2 || closedPull == nil || closedPull.State != "closed" || closedPull.ClosedAtGitHub == "" {
		t.Fatalf("thread not closed from overlap sweep: %#v", threads)
	}
}

func TestExpectedIssueTotal(t *testing.T) {
	cases := []struct {
		name  string
		repo  map[string]any
		state string
		since string
		limit int
		want  int
	}{
		{name: "open no filters", repo: map[string]any{"open_issues_count": float64(666)}, state: "open", want: 666},
		{name: "open with limit below count", repo: map[string]any{"open_issues_count": float64(666)}, state: "open", limit: 100, want: 100},
		{name: "open with limit above count", repo: map[string]any{"open_issues_count": float64(50)}, state: "open", limit: 200, want: 50},
		{name: "open with since", repo: map[string]any{"open_issues_count": float64(666)}, state: "open", since: "2026-04-26T00:00:00Z", want: 0},
		{name: "closed state", repo: map[string]any{"open_issues_count": float64(666)}, state: "closed", want: 0},
		{name: "all state", repo: map[string]any{"open_issues_count": float64(666)}, state: "all", want: 0},
		{name: "missing count", repo: map[string]any{}, state: "open", want: 0},
		{name: "zero count", repo: map[string]any{"open_issues_count": float64(0)}, state: "open", want: 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := expectedIssueTotal(tc.repo, tc.state, tc.since, tc.limit); got != tc.want {
				t.Fatalf("expectedIssueTotal = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestMappingHelperBranches(t *testing.T) {
	if got := jsonID("abc"); got != "abc" {
		t.Fatalf("string json id = %q", got)
	}
	if got := jsonID(float64(12)); got != "12" {
		t.Fatalf("float json id = %q", got)
	}
	if got := jsonID(int64(13)); got != "13" {
		t.Fatalf("int64 json id = %q", got)
	}
	if got := jsonID(json.Number("14")); got != "14" {
		t.Fatalf("json number id = %q", got)
	}
	if got := jsonID(struct{}{}); got != "" {
		t.Fatalf("unknown json id = %q", got)
	}
	if got := intValue(float64(22)); got != 22 {
		t.Fatalf("float int value = %d", got)
	}
	if got := intValue(int64(23)); got != 23 {
		t.Fatalf("int64 int value = %d", got)
	}
	if got := intValue(json.Number("24")); got != 24 {
		t.Fatalf("json number int value = %d", got)
	}
	if got := intValue("bad"); got != 0 {
		t.Fatalf("bad int value = %d", got)
	}
	if got := stringValue(time.Unix(0, 0).UTC()); got == "" {
		t.Fatal("Stringer value should render")
	}
	if loginFromUser("not-user") != "" || typeFromUser("not-user") != "" {
		t.Fatal("non-map user should return empty fields")
	}
	comment := mapComment(77, "review", map[string]any{
		"id":         json.Number("88"),
		"body":       time.Unix(0, 0).UTC(),
		"created_at": "2026-04-30T00:00:00Z",
		"updated_at": "2026-04-30T00:01:00Z",
		"user":       map[string]any{"login": "dependabot[bot]", "type": "User"},
	})
	if comment.GitHubID != "88" || !comment.IsBot || comment.Body == "" {
		t.Fatalf("comment = %+v", comment)
	}
}

func TestMappingFallbackBranches(t *testing.T) {
	now := time.Date(2026, 5, 5, 12, 0, 0, 123, time.UTC)
	normalized, err := normalizeSince("2026-05-05T12:00:00+02:00", now)
	if err != nil {
		t.Fatalf("normalize iso since: %v", err)
	}
	if normalized != "2026-05-05T10:00:00Z" {
		t.Fatalf("normalized iso since = %q", normalized)
	}
	if got, err := normalizeSince("2w", now); err != nil || got != "2026-04-21T12:00:00.000000123Z" {
		t.Fatalf("normalize weeks = %q, %v", got, err)
	}
	if got := mustJSON(map[string]any{"bad": make(chan int)}); got != "{}" {
		t.Fatalf("mustJSON marshal fallback = %q", got)
	}

	thread := mapIssueToThread(99, map[string]any{
		"id":         int64(123),
		"number":     456,
		"state":      "closed",
		"title":      "fallbacks",
		"body":       "body",
		"html_url":   "https://github.com/openclaw/gitcrawl/issues/456",
		"labels":     nil,
		"assignees":  nil,
		"created_at": "2026-05-05T10:00:00Z",
		"updated_at": "2026-05-05T11:00:00Z",
		"closed_at":  "2026-05-05T12:00:00Z",
	}, "2026-05-05T12:00:00Z")
	if thread.LabelsJSON != "[]" || thread.AssigneesJSON != "[]" {
		t.Fatalf("nullable label defaults: labels=%s assignees=%s", thread.LabelsJSON, thread.AssigneesJSON)
	}
	if thread.GitHubID != "123" || thread.Number != 456 || thread.AuthorLogin != "" || thread.ClosedAtGitHub == "" {
		t.Fatalf("thread = %+v", thread)
	}
}

func testProgressLogger(out *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewTextHandler(out, &slog.HandlerOptions{
		ReplaceAttr: func(_ []string, attr slog.Attr) slog.Attr {
			if attr.Key == slog.TimeKey {
				return slog.Attr{}
			}
			return attr
		},
	}))
}

func assertDocumentFTSCount(t *testing.T, st *store.Store, query string, want int) {
	t.Helper()
	var got int
	if err := st.DB().QueryRowContext(context.Background(), `select count(*) from documents_fts where documents_fts match ?`, query).Scan(&got); err != nil {
		t.Fatalf("query document index: %v", err)
	}
	if got != want {
		t.Fatalf("document FTS count for %q: got %d want %d", query, got, want)
	}
}

func assertTableRowCount(t *testing.T, st *store.Store, table string, want int) {
	t.Helper()
	var got int
	if err := st.DB().QueryRowContext(context.Background(), `select count(*) from `+table).Scan(&got); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	if got != want {
		t.Fatalf("%s rows: got %d want %d", table, got, want)
	}
}

func mustIssue(t *testing.T, row map[string]any, err error) map[string]any {
	t.Helper()
	if err != nil {
		t.Fatalf("issue row: %v", err)
	}
	return row
}

func mustTime(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		t.Fatalf("parse time %q: %v", value, err)
	}
	return parsed
}
