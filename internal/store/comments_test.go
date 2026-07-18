package store

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func TestUpsertComment(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repoID, err := st.UpsertRepository(ctx, Repository{Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl", RawJSON: "{}", UpdatedAt: "2026-04-26T00:00:00Z"})
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	threadID, err := st.UpsertThread(ctx, Thread{
		RepoID: repoID, GitHubID: "1", Number: 1, Kind: "issue", State: "open",
		Title: "download stalls", HTMLURL: "https://github.com/openclaw/gitcrawl/issues/1",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "hash", UpdatedAt: "2026-04-26T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("thread: %v", err)
	}
	id, err := st.UpsertComment(ctx, Comment{
		ThreadID: threadID, GitHubID: "c1", CommentType: "issue_comment",
		AuthorLogin: "vincentkoc", Body: "same bug here", RawJSON: "{}", CreatedAtGitHub: "2026-04-26T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("comment: %v", err)
	}
	if id == 0 {
		t.Fatal("expected comment id")
	}
	if _, err := st.UpsertComment(ctx, Comment{
		ThreadID: threadID, GitHubID: "c0", CommentType: "issue_comment",
		AuthorLogin: "octobot", AuthorType: "Bot", Body: "earlier bot note", IsBot: true, RawJSON: "{}",
		CreatedAtGitHub: "2026-04-25T00:00:00Z", UpdatedAtGitHub: "2026-04-25T00:01:00Z",
	}); err != nil {
		t.Fatalf("second comment: %v", err)
	}
	comments, err := st.ListComments(ctx, threadID)
	if err != nil {
		t.Fatalf("list comments: %v", err)
	}
	if len(comments) != 2 || comments[0].GitHubID != "c0" || !comments[0].IsBot || comments[1].GitHubID != "c1" {
		t.Fatalf("comments = %+v", comments)
	}

	edited := comments[1]
	edited.Body = "same edited bug here"
	edited.UpdatedAtGitHub = "2026-04-26T00:02:00Z"
	if _, err := st.UpsertComment(ctx, edited); err != nil {
		t.Fatalf("edit comment: %v", err)
	}
	deletedAt := "2026-04-26T00:03:00Z"
	if _, err := st.UpsertComment(ctx, Comment{
		ThreadID: threadID, GitHubID: "c1", CommentType: "issue_comment",
		DeletedAt: deletedAt, DeletionReason: "explicit-source-delete",
	}); err != nil {
		t.Fatalf("import sparse comment tombstone: %v", err)
	}
	comments, err = st.ListComments(ctx, threadID)
	if err != nil {
		t.Fatalf("list comments after tombstone: %v", err)
	}
	if len(comments) != 1 || comments[0].GitHubID != "c0" {
		t.Fatalf("tombstoned comment remained visible: %+v", comments)
	}
	var revisionCount int
	if err := st.DB().QueryRowContext(ctx, `
		select count(*) from comment_revisions where comment_id = ?
	`, id).Scan(&revisionCount); err != nil {
		t.Fatalf("comment revisions: %v", err)
	}
	if revisionCount != 3 {
		t.Fatalf("comment revisions = %d, want create/edit/delete", revisionCount)
	}
	var retainedBody string
	if err := st.DB().QueryRowContext(ctx, `select body from comments where id = ?`, id).Scan(&retainedBody); err != nil {
		t.Fatalf("retained tombstone body: %v", err)
	}
	if retainedBody != edited.Body {
		t.Fatalf("sparse tombstone replaced body with %q", retainedBody)
	}
	edited.DeletedAt = ""
	edited.DeletionReason = ""
	edited.UpdatedAtGitHub = "2026-04-26T00:04:00Z"
	if _, err := st.UpsertComment(ctx, edited); err != nil {
		t.Fatalf("restore comment: %v", err)
	}
	comments, err = st.ListComments(ctx, threadID)
	if err != nil {
		t.Fatalf("list restored comments: %v", err)
	}
	if len(comments) != 2 || comments[1].GitHubID != "c1" || comments[1].Body != edited.Body {
		t.Fatalf("restored comments = %+v", comments)
	}
	if err := st.DB().QueryRowContext(ctx, `
		select count(*) from comment_revisions where comment_id = ?
	`, id).Scan(&revisionCount); err != nil {
		t.Fatalf("restored comment revisions: %v", err)
	}
	if revisionCount != 4 {
		t.Fatalf("comment revisions after restore = %d, want 4", revisionCount)
	}
}

func TestTombstoneFieldsRejectSurroundingWhitespace(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repoID, err := st.UpsertRepository(ctx, Repository{Owner: "openclaw", Name: "gitcrawl", FullName: "openclaw/gitcrawl", RawJSON: "{}", UpdatedAt: "2026-05-01T00:00:00Z"})
	if err != nil {
		t.Fatalf("repo: %v", err)
	}
	threadID, err := st.UpsertThread(ctx, Thread{
		RepoID: repoID, GitHubID: "2", Number: 2, Kind: "issue", State: "open",
		Title: "whitespace", HTMLURL: "https://github.com/openclaw/gitcrawl/issues/2",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "hash", UpdatedAt: "2026-05-01T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("thread: %v", err)
	}

	for _, test := range []struct {
		name      string
		deletedAt string
		reason    string
	}{
		{name: "whitespace-only pair", deletedAt: " ", reason: " "},
		{name: "timestamp padding", deletedAt: " 2026-05-01T00:01:00Z", reason: "explicit-source-delete"},
		{name: "reason padding", deletedAt: "2026-05-01T00:01:00Z", reason: "explicit-source-delete "},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := st.UpsertComment(ctx, Comment{
				ThreadID: threadID, GitHubID: "space", CommentType: "issue_comment",
				Body: "body", RawJSON: `{}`, DeletedAt: test.deletedAt, DeletionReason: test.reason,
			})
			if err == nil || !strings.Contains(err.Error(), "surrounding whitespace") {
				t.Fatalf("upsert comment error = %v, want surrounding whitespace rejection", err)
			}
		})
	}
	if _, err := st.UpsertComment(ctx, Comment{
		ThreadID: threadID, GitHubID: "direct", CommentType: "issue_comment",
		Body: "direct tombstone", RawJSON: `{}`,
	}); err != nil {
		t.Fatalf("seed direct tombstone comment: %v", err)
	}
	applied, err := st.TombstoneComment(ctx, threadID, "issue_comment", "direct", "2026-05-01T00:02:00Z", "explicit-source-delete")
	if err != nil || !applied {
		t.Fatalf("direct comment tombstone = %t, %v", applied, err)
	}
	applied, err = st.TombstoneComment(ctx, threadID, "issue_comment", "missing", "2026-05-01T00:02:00Z", "explicit-source-delete")
	if err != nil || applied {
		t.Fatalf("missing comment tombstone = %t, %v", applied, err)
	}
	if _, err := st.TombstoneComment(ctx, threadID, "issue_comment", "direct", "", ""); err == nil || !strings.Contains(err.Error(), "deleted_at is required") {
		t.Fatalf("empty comment tombstone error = %v", err)
	}
}
