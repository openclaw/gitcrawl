package capture

import (
	"context"
	"path/filepath"
	"slices"
	"testing"

	"github.com/openclaw/gitcrawl/internal/store"
)

func TestBuildThreadOrdersCommentsByInstant(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	repoID, err := st.UpsertRepository(ctx, store.Repository{
		Owner: "example", Name: "archive", FullName: "example/archive", UpdatedAt: "2026-09-15T00:00:01Z",
	})
	if err != nil {
		t.Fatal(err)
	}
	thread := store.Thread{
		RepoID: repoID, GitHubID: "1", Number: 1, Kind: "pull_request", State: "open",
		Title: "Synthetic conversation", LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}",
		UpdatedAtGitHub: "2026-09-15T00:00:01Z", UpdatedAt: "2026-09-15T00:00:01Z",
	}
	thread.ID, err = st.UpsertThread(ctx, thread)
	if err != nil {
		t.Fatal(err)
	}
	for _, comment := range []store.Comment{
		{GitHubID: "missing", CommentType: "issue_comment"},
		{GitHubID: "whole", CommentType: "issue_comment", CreatedAtGitHub: "2026-09-15T00:00:00Z"},
		{GitHubID: "tenth", CommentType: "issue_comment", CreatedAtGitHub: "2026-09-15T00:00:00.1Z"},
		{GitHubID: "later", CommentType: "issue_comment", CreatedAtGitHub: "2026-09-15T00:00:00.11Z"},
		{GitHubID: "nano", CommentType: "issue_comment", CreatedAtGitHub: "2026-09-15T00:00:00.000000001Z"},
		{GitHubID: "review", CommentType: "pull_review", CreatedAtGitHub: "2026-09-15T01:00:00.1+01:00"},
		{GitHubID: "a-tenth", CommentType: "issue_comment", CreatedAtGitHub: "2026-09-15T00:00:00.100Z"},
	} {
		comment.ThreadID, comment.RawJSON = thread.ID, "{}"
		if _, err := st.UpsertComment(ctx, comment); err != nil {
			t.Fatal(err)
		}
	}
	reserveComments(t, ctx, st, thread.ID, thread.UpdatedAtGitHub)
	got, err := buildThread(ctx, st, thread)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, comment := range got.Comments {
		ids = append(ids, comment.ID)
	}
	want := []string{"missing", "whole", "nano", "a-tenth", "tenth", "review", "later"}
	if !slices.Equal(ids, want) {
		t.Fatalf("comment order = %v, want %v", ids, want)
	}
}
