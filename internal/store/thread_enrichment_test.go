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
		UpdatedAtGitHub:   "2026-07-12T00:00:00Z",
		UpdatedAt:         "2026-07-12T00:00:00Z",
	}
	thread.ID, err = st.UpsertThread(ctx, thread)
	if err != nil {
		t.Fatalf("thread: %v", err)
	}
	evidence := ThreadEvidence{
		Thread: thread,
		Detail: &PullRequestDetail{ThreadID: thread.ID, RepoID: repoID, Number: 42, BaseSHA: "base", HeadSHA: "head", FetchedAt: "2026-07-12T00:01:00Z", UpdatedAt: "2026-07-12T00:01:00Z"},
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
	}

	first, err := st.UpsertThreadRevisionAndFingerprint(ctx, evidence, "2026-07-12T00:02:00Z")
	if err != nil {
		t.Fatalf("first enrichment: %v", err)
	}
	if !first.RevisionCreated || !first.FingerprintUpserted {
		t.Fatalf("first result = %+v", first)
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
