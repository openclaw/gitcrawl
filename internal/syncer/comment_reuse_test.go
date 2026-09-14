package syncer

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	gh "github.com/openclaw/gitcrawl/internal/github"
	"github.com/openclaw/gitcrawl/internal/store"
)

type commentReuseGitHub struct {
	fakeGitHub
	row          map[string]any
	comments     []map[string]any
	commentCalls int
	reviewCalls  int
	reviews      []map[string]any
	commentErr   error
}

func (f *commentReuseGitHub) GetIssue(context.Context, string, string, int, gh.Reporter) (map[string]any, error) {
	return f.row, nil
}

func (f *commentReuseGitHub) ListIssueComments(context.Context, string, string, int, gh.Reporter) ([]map[string]any, error) {
	f.commentCalls++
	return f.comments, f.commentErr
}

func (f *commentReuseGitHub) ListPullReviews(context.Context, string, string, int, gh.Reporter) ([]map[string]any, error) {
	f.reviewCalls++
	return f.reviews, nil
}

func TestSyncCommentReuseInvalidation(t *testing.T) {
	for _, scenario := range []string{"timestamp", "count", "missing-count", "malformed-time", "force", "missing-membership", "missing-member", "metadata-only", "failed-force", "pruned"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			st, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			row, _ := (fakeGitHub{}).GetIssue(ctx, "openclaw", "gitcrawl", 7, nil)
			comments, _ := (fakeGitHub{}).ListIssueComments(ctx, "openclaw", "gitcrawl", 7, nil)
			row["comments"] = 1
			client := &commentReuseGitHub{row: row, comments: comments}
			s := New(client, st)
			opts := Options{Owner: "openclaw", Repo: "gitcrawl", Numbers: []int{7}, IncludeComments: scenario != "metadata-only"}
			if _, err := s.Sync(ctx, opts); err != nil {
				t.Fatal(err)
			}
			opts.IncludeComments = true
			switch scenario {
			case "timestamp":
				row["updated_at"] = "2026-04-27T00:00:00Z"
			case "count":
				row["comments"] = 2
			case "missing-count":
				delete(row, "comments")
			case "malformed-time":
				row["updated_at"] = "invalid"
			case "force":
				opts.Force = true
			case "pruned":
				if _, err := st.PrunePortablePayloads(ctx, store.PortablePruneOptions{BodyChars: 4}); err != nil {
					t.Fatal(err)
				}
				if err := st.Close(); err != nil {
					t.Fatal(err)
				}
				st, err = store.Open(ctx, st.Path())
				if err != nil {
					t.Fatal(err)
				}
				defer st.Close()
				s = New(client, st)
			case "missing-membership":
				if _, err := st.DB().ExecContext(ctx, "delete from thread_child_observation_memberships"); err != nil {
					t.Fatal(err)
				}
			case "missing-member":
				if _, err := st.DB().ExecContext(ctx, "update comments set deleted_at = '2026-04-27T00:00:00Z', deletion_reason = 'fixture'"); err != nil {
					t.Fatal(err)
				}
			case "failed-force":
				opts.Force = true
				client.commentErr = errors.New("fixture download failure")
				if _, err := s.Sync(ctx, opts); err == nil {
					t.Fatal("failed download succeeded")
				}
				opts.Force = false
				client.commentErr = nil
			}
			before := client.commentCalls
			if _, err := s.Sync(ctx, opts); err != nil {
				t.Fatal(err)
			}
			if client.commentCalls != before+1 {
				t.Fatalf("ListIssueComments calls = %d, want %d after invalidation", client.commentCalls, before+1)
			}
			if scenario == "pruned" {
				var body string
				if err := st.DB().QueryRowContext(ctx, "select body from comments where github_id='11'").Scan(&body); err != nil {
					t.Fatal(err)
				}
				if body != "same bug here" {
					t.Fatalf("pruned comment not restored: %q", body)
				}
			}
		})
	}
}

func TestSyncCommentReuseEmptyAndDeletedMembership(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	row, _ := (fakeGitHub{}).GetIssue(ctx, "openclaw", "gitcrawl", 7, nil)
	comments, _ := (fakeGitHub{}).ListIssueComments(ctx, "openclaw", "gitcrawl", 7, nil)
	row["comments"] = 1
	client := &commentReuseGitHub{row: row, comments: comments}
	s := New(client, st)
	opts := Options{Owner: "openclaw", Repo: "gitcrawl", Numbers: []int{7}, IncludeComments: true}
	if _, err := s.Sync(ctx, opts); err != nil {
		t.Fatal(err)
	}
	row["comments"] = 0
	client.comments = nil
	if _, err := s.Sync(ctx, opts); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(ctx, opts); err != nil {
		t.Fatal(err)
	}
	if client.commentCalls != 2 {
		t.Fatalf("empty completed snapshot not reused: %d", client.commentCalls)
	}
	var encoded string
	if err := st.DB().QueryRowContext(ctx, `select m.member_ids_json from thread_child_observation_memberships m join thread_child_observation_reservations r using(thread_id,family,observation_sequence) where r.family='comments'`).Scan(&encoded); err != nil {
		t.Fatal(err)
	}
	if encoded != "[]" {
		t.Fatalf("reuse resurrected historical comments: %s", encoded)
	}
}

func TestSyncCommentReuseKeepsPRReviewEvidenceLive(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	row, _ := (fakeGitHub{}).GetIssue(ctx, "openclaw", "gitcrawl", 8, nil)
	comments, _ := (fakeGitHub{}).ListIssueComments(ctx, "openclaw", "gitcrawl", 7, nil)
	row["comments"] = 1
	client := &commentReuseGitHub{row: row, comments: comments}
	s := New(client, st)
	opts := Options{Owner: "openclaw", Repo: "gitcrawl", Numbers: []int{8}, IncludeComments: true, IncludePRDetails: true}
	if _, err := s.Sync(ctx, opts); err != nil {
		t.Fatal(err)
	}
	client.reviews = []map[string]any{{"id": 123, "state": "CHANGES_REQUESTED", "body": "please fix"}}
	stats, err := s.Sync(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	for name, result := range map[string]struct{ got, want int }{
		"issue comment requests": {client.commentCalls, 1},
		"review requests":        {client.reviewCalls, 2},
		"comments synced":        {stats.CommentsSynced, 1},
		"revisions created":      {stats.RevisionsCreated, 1},
		"PR details synced":      {stats.PRDetailsSynced, 1},
	} {
		if result.got != result.want {
			t.Errorf("%s = %d, want %d", name, result.got, result.want)
		}
	}
}

func TestSyncCommentReuseRejectsConcurrentReplacement(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	row, _ := (fakeGitHub{}).GetIssue(ctx, "openclaw", "gitcrawl", 7, nil)
	comments, _ := (fakeGitHub{}).ListIssueComments(ctx, "openclaw", "gitcrawl", 7, nil)
	row["comments"] = 1
	client := &commentReuseGitHub{row: row, comments: comments}
	s := New(client, st)
	opts := Options{Owner: "openclaw", Repo: "gitcrawl", Numbers: []int{7}, IncludeComments: true}
	if _, err := s.Sync(ctx, opts); err != nil {
		t.Fatal(err)
	}
	s.beforePersist = func() {
		client.comments[0]["body"] = "new concurrent body"
		forced := opts
		forced.Force = true
		if _, err := New(client, st).Sync(ctx, forced); err != nil {
			t.Fatal(err)
		}
	}
	stats, err := s.Sync(ctx, opts)
	if err == nil || !strings.Contains(err.Error(), "saved issue comments changed") {
		t.Fatalf("Sync error = %v, want saved-comment replacement error", err)
	}
	if stats.EvidenceObserved != 0 || stats.ThreadsSynced != 0 {
		t.Fatalf("certified stale cached evidence: %+v", stats)
	}
	var body string
	if err := st.DB().QueryRowContext(ctx, "select body from comments where github_id='11'").Scan(&body); err != nil {
		t.Fatal(err)
	}
	if body != "new concurrent body" {
		t.Fatalf("concurrent comment overwritten: %s", body)
	}
}

func TestSyncReusesUnchangedIssueComments(t *testing.T) {
	for _, test := range []struct {
		name        string
		number      int
		updatedAt   string
		wantReviews int
	}{
		{"issue_discussion_remains_reusable", 7, "2026-04-26T00:00:00Z", 0},
		{"PR_discussion_remains_reusable", 8, "2026-04-26T00:00:00Z", 2},
		{"equivalent_timezone_reuses_comments", 7, "2026-04-26T00:00:00+00:00", 0},
		{"equivalent_fraction_reuses_comments", 7, "2026-04-26T00:00:00.000Z", 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			st, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			row, _ := (fakeGitHub{}).GetIssue(ctx, "openclaw", "gitcrawl", test.number, nil)
			comments, _ := (fakeGitHub{}).ListIssueComments(ctx, "openclaw", "gitcrawl", 7, nil)
			row["comments"] = len(comments)
			client := &commentReuseGitHub{row: row, comments: comments}
			s := New(client, st)
			var received SyncProgress
			opts := Options{Owner: "openclaw", Repo: "gitcrawl", Numbers: []int{test.number}, IncludeComments: true,
				Progress: func(p SyncProgress) error { received = p; return nil }}
			first, err := s.Sync(ctx, opts)
			if err != nil {
				t.Fatal(err)
			}
			if first.CommentsSynced != 1 {
				t.Fatalf("initial CommentsSynced = %d, want 1", first.CommentsSynced)
			}
			row["updated_at"] = test.updatedAt
			second, err := s.Sync(ctx, opts)
			if err != nil {
				t.Fatal(err)
			}
			if client.commentCalls != 1 || second.CommentsSynced != 0 || received.CommentsReceived != 0 {
				t.Fatalf("unchanged thread re-downloaded comments: calls=%d synced=%d received=%d", client.commentCalls, second.CommentsSynced, received.CommentsReceived)
			}
			if client.reviewCalls != test.wantReviews {
				t.Errorf("review requests after second sync = %d, want %d", client.reviewCalls, test.wantReviews)
			}
			var threadID, commentID int64
			var body, deletedAt string
			if err := st.DB().QueryRowContext(ctx, "select thread_id, id, body, coalesce(deleted_at, '') from comments where github_id='11'").Scan(&threadID, &commentID, &body, &deletedAt); err != nil {
				t.Fatal(err)
			}
			if body != "same bug here" || deletedAt != "" {
				t.Errorf("saved comment = (%q, deleted_at=%q), want original live comment", body, deletedAt)
			}
			_, sequence, found, err := st.ThreadChildObservation(ctx, threadID, store.ThreadChildComments)
			if err != nil || !found {
				t.Fatalf("completed comment observation found=%t, err=%v; want present", found, err)
			}
			members, found, err := st.ThreadChildObservationMemberIDs(ctx, threadID, store.ThreadChildComments, sequence)
			if err != nil || !found || !slices.Equal(members, []int64{commentID}) {
				t.Errorf("latest comment membership = %v, found=%t, err=%v; want [%d]", members, found, err, commentID)
			}
			if _, err := s.Sync(ctx, opts); err != nil {
				t.Fatal(err)
			}
			if client.commentCalls != 1 {
				t.Errorf("ListIssueComments calls after third sync = %d, want 1", client.commentCalls)
			}
		})
	}
}
