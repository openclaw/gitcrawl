package syncer

import (
	"context"
	"errors"
	"path/filepath"
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
				t.Fatal("invalidated comments were reused")
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
	if client.commentCalls != 1 || client.reviewCalls != 2 || stats.CommentsSynced != 1 || stats.RevisionsCreated != 1 || stats.PRDetailsSynced != 1 {
		t.Fatalf("fresh PR evidence lost with reused discussion: calls=%d reviews=%d stats=%+v", client.commentCalls, client.reviewCalls, stats)
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
		t.Fatalf("err=%v", err)
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
	for _, number := range []int{7, 8} {
		t.Run(map[int]string{7: "issue", 8: "pull_request"}[number], func(t *testing.T) {
			ctx := context.Background()
			st, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			row, _ := (fakeGitHub{}).GetIssue(ctx, "openclaw", "gitcrawl", number, nil)
			comments, _ := (fakeGitHub{}).ListIssueComments(ctx, "openclaw", "gitcrawl", 7, nil)
			row["comments"] = len(comments)
			client := &commentReuseGitHub{row: row, comments: comments}
			s := New(client, st)
			var received SyncProgress
			opts := Options{Owner: "openclaw", Repo: "gitcrawl", Numbers: []int{number}, IncludeComments: true,
				Progress: func(p SyncProgress) error { received = p; return nil }}
			first, err := s.Sync(ctx, opts)
			if err != nil {
				t.Fatal(err)
			}
			if first.CommentsSynced != 1 {
				t.Fatalf("first: %+v", first)
			}
			second, err := s.Sync(ctx, opts)
			if err != nil {
				t.Fatal(err)
			}
			if client.commentCalls != 1 || second.CommentsSynced != 0 || received.CommentsReceived != 0 {
				t.Fatalf("unchanged thread re-downloaded comments: calls=%d synced=%d received=%d", client.commentCalls, second.CommentsSynced, received.CommentsReceived)
			}
			if number == 8 && client.reviewCalls != 2 {
				t.Fatal("PR reviews must remain live")
			}
			var count int
			if err := st.DB().QueryRowContext(ctx, "select count(*) from comments where body = 'same bug here'").Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != 1 {
				t.Fatalf("reuse lost archived comments: %d", count)
			}
		})
	}
}
