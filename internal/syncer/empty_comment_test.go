package syncer

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	gh "github.com/openclaw/gitcrawl/internal/github"
	"github.com/openclaw/gitcrawl/internal/store"
)

type emptyBodyGitHub struct {
	fakeGitHub
	body string
}

func (f *emptyBodyGitHub) ListIssueComments(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) ([]map[string]any, error) {
	return []map[string]any{{"id": 101, "body": f.body, "created_at": "2026-04-26T00:00:00Z", "updated_at": "2026-04-27T00:00:00Z"}}, nil
}
func (f *emptyBodyGitHub) ListPullReviewComments(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) ([]map[string]any, error) {
	return []map[string]any{{"id": 102, "body": f.body, "created_at": "2026-04-26T00:00:00Z", "updated_at": "2026-04-27T00:00:00Z"}}, nil
}
func TestSyncRetainsEmptyLiveCommentsAndEdits(t *testing.T) {
	for _, number := range []int{7, 8} {
		t.Run(fmt.Sprint(number), func(t *testing.T) {
			ctx := context.Background()
			st, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.sqlite"))
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			client := &emptyBodyGitHub{body: "before edit"}
			s := New(client, st)
			options := Options{Owner: "openclaw", Repo: "gitcrawl", Numbers: []int{number}, IncludeComments: true, Force: true}
			if _, err = s.Sync(ctx, options); err != nil {
				t.Fatal(err)
			}
			client.body = ""
			stats, err := s.Sync(ctx, options)
			if err != nil {
				t.Fatal(err)
			}
			expected := 1
			if number == 8 {
				expected = 2
			}
			var count int
			if err = st.DB().QueryRowContext(ctx, "select count(*) from comments where github_id in ('101','102') and body = '' and coalesce(deleted_at,'') = ''").Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != expected {
				t.Fatalf("empty live comments lost: %d", count)
			}
			if number == 7 && stats.RevisionsCreated != 1 {
				t.Fatalf("edit-to-empty lost revision: %+v", stats)
			}
			stats, err = s.Sync(ctx, options)
			if err != nil {
				t.Fatal(err)
			}
			if stats.RevisionsCreated != 0 {
				t.Fatalf("replay created a revision: %+v", stats)
			}
			if err = st.DB().QueryRowContext(ctx, "select count(*) from comments where github_id in ('101','102')").Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != expected {
				t.Fatalf("replay duplicated identities: %d", count)
			}
		})
	}
}

type malformedEmptyGitHub struct{ fakeGitHub }

func (malformedEmptyGitHub) ListIssueComments(ctx context.Context, owner, repo string, number int, reporter gh.Reporter) ([]map[string]any, error) {
	return []map[string]any{{"body": ""}}, nil
}
func TestSyncRejectsAnonymousEmptyComment(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	_, err = New(malformedEmptyGitHub{}, st).Sync(ctx, Options{Owner: "openclaw", Repo: "gitcrawl", Numbers: []int{7}, IncludeComments: true, Force: true})
	if err == nil || !strings.Contains(err.Error(), "missing GitHub identity") {
		t.Fatalf("anonymous empty comment accepted: %v", err)
	}
	var count int
	if err = st.DB().QueryRowContext(ctx, "select count(*) from comments").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("anonymous row persisted")
	}
}
