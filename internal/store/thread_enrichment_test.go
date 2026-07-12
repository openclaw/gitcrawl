package store

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func TestUpsertThreadRevisionAndFingerprintTracksCanonicalEvidence(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repoID, err := st.UpsertRepository(ctx, Repository{
		Owner:     "openclaw",
		Name:      "gitcrawl",
		FullName:  "openclaw/gitcrawl",
		RawJSON:   "{}",
		UpdatedAt: "2026-07-12T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("repository: %v", err)
	}
	thread := Thread{
		RepoID:            repoID,
		GitHubID:          "42",
		Number:            42,
		Kind:              "pull_request",
		State:             "open",
		Title:             "fix portable evidence #17",
		Body:              "hydrate review evidence",
		AuthorLogin:       "alice",
		AuthorType:        "User",
		AuthorAssociation: "MEMBER",
		HTMLURL:           "https://github.com/openclaw/gitcrawl/pull/42",
		LabelsJSON:        `[{"name":"bug"},{"name":"storage"}]`,
		AssigneesJSON:     `[{"login":"bob"}]`,
		RawJSON:           "{}",
		ContentHash:       "github-thread-hash",
		IsDraft:           true,
		UpdatedAtGitHub:   "2026-07-12T00:00:00Z",
		UpdatedAt:         "2026-07-12T00:00:00Z",
	}
	thread.ID, err = st.UpsertThread(ctx, thread)
	if err != nil {
		t.Fatalf("thread: %v", err)
	}
	evidence := ThreadEvidence{
		Thread: thread,
		Detail: &PullRequestDetail{
			ThreadID: thread.ID, RepoID: repoID, Number: 42, BaseSHA: "base", HeadSHA: "head",
			MergeableState: "clean", Additions: 12, Deletions: 3, ChangedFiles: 2,
			FetchedAt: "2026-07-12T00:01:00Z", UpdatedAt: "2026-07-12T00:01:00Z",
		},
		Files: []PullRequestFile{
			{ThreadID: thread.ID, Path: "internal/store/portable.go", Status: "modified", Patch: "@@ portable"},
			{ThreadID: thread.ID, Path: "internal/store/schema.go", Status: "modified", Patch: "@@ schema"},
		},
		Commits: []PullRequestCommit{
			{ThreadID: thread.ID, SHA: "def", Message: "test: cover evidence"},
			{ThreadID: thread.ID, SHA: "abc", Message: "feat: hydrate evidence\n\nbody"},
		},
		ReviewThreads: []PullRequestReviewThread{
			{ThreadID: thread.ID, ReviewThreadID: "RT2", Path: "internal/store/schema.go", FirstCommentBody: "schema note", CommentsJSON: `[{"body":"schema note"}]`},
			{ThreadID: thread.ID, ReviewThreadID: "RT1", Path: "internal/store/portable.go", FirstCommentBody: "portable note", CommentsJSON: `[{"body":"portable note"}]`},
		},
		Checks: []PullRequestCheck{
			{ThreadID: thread.ID, Name: "test", Status: "completed", Conclusion: "success", WorkflowName: "CI"},
		},
		WorkflowRuns: []WorkflowRun{
			{RepoID: repoID, RunID: "99", RunNumber: 7, HeadSHA: "head", Status: "completed", Conclusion: "success", WorkflowName: "CI"},
		},
	}

	first, err := st.UpsertThreadRevisionAndFingerprint(ctx, evidence, "2026-07-12T00:02:00Z")
	if err != nil {
		t.Fatalf("first enrichment: %v", err)
	}
	if !first.RevisionCreated || !first.FingerprintUpserted {
		t.Fatalf("first result = %+v", first)
	}
	var evidenceHash, evidenceJSON string
	if err := st.DB().QueryRowContext(ctx, `
		select b.sha256, b.inline_text
		from thread_revisions tr
		join blobs b on b.id = tr.raw_json_blob_id
		where tr.id = ?
	`, first.RevisionID).Scan(&evidenceHash, &evidenceJSON); err != nil {
		t.Fatalf("revision evidence: %v", err)
	}
	if evidenceHash != StableHash(evidenceJSON) ||
		!strings.Contains(evidenceJSON, `"is_draft":true`) ||
		!strings.Contains(evidenceJSON, `"mergeable_state":"clean"`) ||
		!strings.Contains(evidenceJSON, `"checks":[`) ||
		!strings.Contains(evidenceJSON, `"workflow_runs":[`) ||
		!strings.Contains(evidenceJSON, `"review_threads":[`) {
		t.Fatalf("revision evidence hash=%q json=%s", evidenceHash, evidenceJSON)
	}

	evidence.Files[0], evidence.Files[1] = evidence.Files[1], evidence.Files[0]
	evidence.Commits[0], evidence.Commits[1] = evidence.Commits[1], evidence.Commits[0]
	evidence.ReviewThreads[0], evidence.ReviewThreads[1] = evidence.ReviewThreads[1], evidence.ReviewThreads[0]
	second, err := st.UpsertThreadRevisionAndFingerprint(ctx, evidence, "2026-07-12T00:03:00Z")
	if err != nil {
		t.Fatalf("idempotent enrichment: %v", err)
	}
	if second.RevisionID != first.RevisionID || second.RevisionCreated || second.FingerprintUpserted {
		t.Fatalf("idempotent result = %+v, first = %+v", second, first)
	}

	evidence.ReviewThreads[0].IsResolved = true
	third, err := st.UpsertThreadRevisionAndFingerprint(ctx, evidence, "2026-07-12T00:04:00Z")
	if err != nil {
		t.Fatalf("changed enrichment: %v", err)
	}
	if !third.RevisionCreated || !third.FingerprintUpserted || third.RevisionID == first.RevisionID {
		t.Fatalf("changed result = %+v", third)
	}

	evidence.ReviewThreads[0].IsResolved = false
	reverted, err := st.UpsertThreadRevisionAndFingerprint(ctx, evidence, "2026-07-12T00:05:00Z")
	if err != nil {
		t.Fatalf("reverted enrichment: %v", err)
	}
	if !reverted.RevisionCreated || !reverted.FingerprintUpserted ||
		reverted.RevisionID == first.RevisionID || reverted.RevisionID == third.RevisionID {
		t.Fatalf("reverted result = %+v, first = %+v, changed = %+v", reverted, first, third)
	}
	repeated, err := st.UpsertThreadRevisionAndFingerprint(ctx, evidence, "2026-07-12T00:06:00Z")
	if err != nil {
		t.Fatalf("repeated reverted enrichment: %v", err)
	}
	if repeated.RevisionID != reverted.RevisionID || repeated.RevisionCreated || repeated.FingerprintUpserted {
		t.Fatalf("repeated reverted result = %+v, reverted = %+v", repeated, reverted)
	}

	var revisions, fingerprints int
	if err := st.DB().QueryRowContext(ctx, `select count(*) from thread_revisions where thread_id = ?`, thread.ID).Scan(&revisions); err != nil {
		t.Fatalf("revision count: %v", err)
	}
	if err := st.DB().QueryRowContext(ctx, `
		select count(*)
		from thread_fingerprints tf
		join thread_revisions tr on tr.id = tf.thread_revision_id
		where tr.thread_id = ? and tf.algorithm_version = ?
	`, thread.ID, ThreadFingerprintAlgorithmVersion).Scan(&fingerprints); err != nil {
		t.Fatalf("fingerprint count: %v", err)
	}
	if revisions != 3 || fingerprints != 3 {
		t.Fatalf("revision/fingerprint counts = %d/%d", revisions, fingerprints)
	}
	var slug, algorithm string
	if err := st.DB().QueryRowContext(ctx, `
		select fingerprint_slug, algorithm_version
		from thread_fingerprints
		where thread_revision_id = ?
	`, reverted.RevisionID).Scan(&slug, &algorithm); err != nil {
		t.Fatalf("fingerprint: %v", err)
	}
	if algorithm != ThreadFingerprintAlgorithmVersion || len(strings.Split(slug, "-")) != 4 {
		t.Fatalf("fingerprint slug/version = %q/%q", slug, algorithm)
	}
}

func TestUpsertThreadRevisionAndFingerprintRollsBackTogether(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repoID, err := st.UpsertRepository(ctx, Repository{Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl", RawJSON: "{}", UpdatedAt: "2026-07-12T00:00:00Z"})
	if err != nil {
		t.Fatalf("repository: %v", err)
	}
	thread := Thread{
		RepoID: repoID, GitHubID: "7", Number: 7, Kind: "issue", State: "open",
		Title: "transactional evidence", Body: "body",
		HTMLURL:    "https://github.com/openclaw/gitcrawl/issues/7",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "thread",
		UpdatedAtGitHub: "2026-07-12T00:00:00Z", UpdatedAt: "2026-07-12T00:00:00Z",
	}
	thread.ID, err = st.UpsertThread(ctx, thread)
	if err != nil {
		t.Fatalf("thread: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `
		create trigger reject_thread_fingerprint
		before insert on thread_fingerprints
		begin
			select raise(abort, 'fingerprint rejected');
		end
	`); err != nil {
		t.Fatalf("trigger: %v", err)
	}

	if _, err := st.UpsertThreadRevisionAndFingerprint(ctx, ThreadEvidence{Thread: thread}, "2026-07-12T00:01:00Z"); err == nil || !strings.Contains(err.Error(), "fingerprint rejected") {
		t.Fatalf("enrichment error = %v", err)
	}
	var revisions int
	if err := st.DB().QueryRowContext(ctx, `select count(*) from thread_revisions where thread_id = ?`, thread.ID).Scan(&revisions); err != nil {
		t.Fatalf("revision count: %v", err)
	}
	if revisions != 0 {
		t.Fatalf("revision count = %d, want rollback", revisions)
	}
}

func TestUpsertThreadRevisionRefreshesSourceTimestampWithoutNewRevision(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repoID, err := st.UpsertRepository(ctx, Repository{
		Owner:     "openclaw",
		Name:      "gitcrawl",
		FullName:  "openclaw/gitcrawl",
		RawJSON:   "{}",
		UpdatedAt: "2026-07-12T12:00:00Z",
	})
	if err != nil {
		t.Fatalf("repository: %v", err)
	}
	thread := Thread{
		RepoID:          repoID,
		GitHubID:        "8",
		Number:          8,
		Kind:            "issue",
		State:           "open",
		Title:           "stable evidence",
		HTMLURL:         "https://github.com/openclaw/gitcrawl/issues/8",
		LabelsJSON:      "[]",
		AssigneesJSON:   "[]",
		RawJSON:         "{}",
		ContentHash:     "thread",
		UpdatedAtGitHub: "2026-07-12T12:00:00Z",
		UpdatedAt:       "2026-07-12T12:00:00Z",
	}
	thread.ID, err = st.UpsertThread(ctx, thread)
	if err != nil {
		t.Fatalf("thread: %v", err)
	}
	first, err := st.UpsertThreadRevisionAndFingerprint(ctx, ThreadEvidence{Thread: thread}, "2026-07-12T12:00:01Z")
	if err != nil {
		t.Fatalf("first enrichment: %v", err)
	}

	thread.UpdatedAtGitHub = "2026-07-12T12:00:00.500Z"
	second, err := st.UpsertThreadRevisionAndFingerprint(ctx, ThreadEvidence{Thread: thread}, "2026-07-12T12:00:02Z")
	if err != nil {
		t.Fatalf("refreshed enrichment: %v", err)
	}
	if second.RevisionID != first.RevisionID || second.RevisionCreated || second.FingerprintUpserted {
		t.Fatalf("refreshed result = %+v, first = %+v", second, first)
	}

	thread.UpdatedAtGitHub = "2026-07-12T12:00:00Z"
	third, err := st.UpsertThreadRevisionAndFingerprint(ctx, ThreadEvidence{Thread: thread}, "2026-07-12T12:00:03Z")
	if err != nil {
		t.Fatalf("older enrichment: %v", err)
	}
	if third.RevisionID != first.RevisionID || third.RevisionCreated || third.FingerprintUpserted {
		t.Fatalf("older result = %+v, first = %+v", third, first)
	}

	var revisions, fingerprints int
	var sourceUpdatedAt string
	if err := st.DB().QueryRowContext(ctx, `
		select count(*), max(source_updated_at)
		from thread_revisions
		where thread_id = ?
	`, thread.ID).Scan(&revisions, &sourceUpdatedAt); err != nil {
		t.Fatalf("revision state: %v", err)
	}
	if err := st.DB().QueryRowContext(ctx, `
		select count(*)
		from thread_fingerprints tf
		join thread_revisions tr on tr.id = tf.thread_revision_id
		where tr.thread_id = ?
	`, thread.ID).Scan(&fingerprints); err != nil {
		t.Fatalf("fingerprint count: %v", err)
	}
	if revisions != 1 || fingerprints != 1 || sourceUpdatedAt != "2026-07-12T12:00:00.500Z" {
		t.Fatalf("revision/fingerprint/source state = %d/%d/%q", revisions, fingerprints, sourceUpdatedAt)
	}
}

func TestLatestTimestampComparesRFC3339Instants(t *testing.T) {
	tests := []struct {
		name   string
		values []string
		want   string
	}{
		{
			name:   "fractional second after whole second",
			values: []string{"2026-07-12T12:00:00Z", "2026-07-12T12:00:00.500Z"},
			want:   "2026-07-12T12:00:00.500Z",
		},
		{
			name:   "input order does not change instant ordering",
			values: []string{"2026-07-12T12:00:00.500Z", "2026-07-12T12:00:00Z"},
			want:   "2026-07-12T12:00:00.500Z",
		},
		{
			name:   "valid timestamp wins over malformed input",
			values: []string{"zz-invalid", "2026-07-12T12:00:00Z"},
			want:   "2026-07-12T12:00:00Z",
		},
		{
			name:   "malformed inputs retain deterministic fallback",
			values: []string{"aa-invalid", "zz-invalid"},
			want:   "zz-invalid",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := latestTimestamp(test.values...); got != test.want {
				t.Fatalf("latestTimestamp(%q) = %q, want %q", test.values, got, test.want)
			}
		})
	}
}

func TestThreadRevisionTracksPullRequestDecisionState(t *testing.T) {
	evidence := ThreadEvidence{
		Thread: Thread{
			ID: 1, RepoID: 1, Number: 42, Kind: "pull_request", State: "open",
			Title: "track decision state", LabelsJSON: "[]", AssigneesJSON: "[]",
			UpdatedAtGitHub: "2026-07-12T00:00:00Z",
		},
		Detail: &PullRequestDetail{ThreadID: 1, RepoID: 1, Number: 42, HeadSHA: "head"},
		Comments: []Comment{{
			GitHubID: "review-1", CommentType: "pull_review", ReviewState: "APPROVED",
		}},
		Checks: []PullRequestCheck{{
			Name: "test", Status: "in_progress", FetchedAt: "2026-07-12T00:01:00Z",
		}},
		WorkflowRuns: []WorkflowRun{{
			RunID: "99", RunNumber: 7, HeadSHA: "head", Status: "in_progress", WorkflowName: "CI",
			UpdatedAtGH: "2026-07-12T00:01:00Z",
		}},
	}
	hash := func(value ThreadEvidence) string {
		revision, _ := buildThreadEnrichment(value, "2026-07-12T00:02:00Z")
		return revision.ContentHash
	}
	baseHash := hash(evidence)

	evidence.Thread.IsDraft = true
	draftHash := hash(evidence)
	if draftHash == baseHash {
		t.Fatal("draft transition did not change canonical evidence")
	}
	evidence.Comments[0].ReviewState = "CHANGES_REQUESTED"
	reviewHash := hash(evidence)
	if reviewHash == draftHash {
		t.Fatal("review decision transition did not change canonical evidence")
	}
	evidence.Checks[0].Status = "completed"
	evidence.Checks[0].Conclusion = "failure"
	checkHash := hash(evidence)
	if checkHash == reviewHash {
		t.Fatal("check transition did not change canonical evidence")
	}
	evidence.WorkflowRuns[0].Status = "completed"
	evidence.WorkflowRuns[0].Conclusion = "failure"
	if hash(evidence) == checkHash {
		t.Fatal("workflow run transition did not change canonical evidence")
	}
}
