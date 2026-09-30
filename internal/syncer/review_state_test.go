package syncer

import (
	"context"
	"encoding/json"
	"errors"
	gh "github.com/openclaw/gitcrawl/internal/github"
	"github.com/openclaw/gitcrawl/internal/store"
	"path/filepath"
	"testing"
	"time"
)

type reviewOnlyFixture struct {
	*gh.Client
	items []gh.ReviewStateItem
	err   error
}

func (f reviewOnlyFixture) FetchGraphQLReviewState(context.Context, string, string, []int, gh.Reporter) ([]gh.ReviewStateItem, error) {
	return f.items, f.err
}
func TestReviewOnlyNeverOverwritesCanonicalContentHistoryOrVectors(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	legacy := &metadataGitHub{}
	opts := Options{Owner: "openclaw", Repo: "gitcrawl", Numbers: []int{8}, State: "all", IncludeComments: true, IncludePRMetadata: true}
	if _, err = New(legacy, s).Sync(ctx, opts); err != nil {
		t.Fatal(err)
	}
	repo, err := s.RepositoryByFullName(ctx, "openclaw/gitcrawl")
	if err != nil {
		t.Fatal(err)
	}
	heads, err := s.ListThreads(ctx, repo.ID, true)
	if err != nil {
		t.Fatal(err)
	}
	head := heads[0]
	if _, err = s.DB().Exec("update threads set raw_json=json_set(raw_json,'$.node_id','PR8') where id=?", head.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = s.DB().Exec("update repositories set raw_json=json_set(raw_json,'$.node_id','R1') where id=?", repo.ID); err != nil {
		t.Fatal(err)
	}
	s.DB().QueryRow("select raw_json from threads where id=?", head.ID).Scan(&head.RawJSON)
	s.DB().QueryRow("select raw_json from repositories where id=?", repo.ID).Scan(&repo.RawJSON)
	if err = s.UpsertThreadVector(ctx, store.ThreadVector{ThreadID: head.ID, Basis: "title_original", Model: "fixture", Dimensions: 2, ContentHash: head.ContentHash, Vector: []float64{1, 2}, CreatedAt: "2026-01-01T00:00:00Z", UpdatedAt: "2026-01-01T00:00:00Z"}); err != nil {
		t.Fatal(err)
	}
	snapshot := func() string {
		tables := []string{"threads", "comments", "comment_revisions", "thread_revisions", "thread_fingerprints", "thread_vectors"}
		out := map[string][][]any{}
		for _, table := range tables {
			rows, e := s.DB().Query("select * from " + table)
			if e != nil {
				t.Fatal(e)
			}
			cols, _ := rows.Columns()
			for rows.Next() {
				v := make([]any, len(cols))
				p := make([]any, len(v))
				for i := range v {
					p[i] = &v[i]
				}
				if e = rows.Scan(p...); e != nil {
					t.Fatal(e)
				}
				out[table] = append(out[table], v)
			}
			rows.Close()
		}
		b, _ := json.Marshal(out)
		return string(b)
	}
	before := snapshot()
	var raw map[string]any
	json.Unmarshal([]byte(head.RawJSON), &raw)
	nodeID := stringValue(raw["node_id"])
	if nodeID == "" {
		nodeID = "PR8"
	}
	// Fixture repo supplies its retained database identity to the targeted path.
	var repoRaw map[string]any
	json.Unmarshal([]byte(repo.RawJSON), &repoRaw)
	item := gh.ReviewStateItem{Number: 8, NodeID: nodeID, RepositoryID: repo.GitHubRepoID, RepositoryNodeID: stringValue(repoRaw["node_id"]), UpdatedAt: time.Now().UTC().Format(time.RFC3339Nano), Threads: []map[string]any{{"id": "RT1", "isResolved": true, "isOutdated": true, "path": "file.go", "line": 3, "comments": map[string]any{"nodes": []any{map[string]any{"id": "RC1", "body": "inline body", "url": "https://github.com/openclaw/gitcrawl/pull/8#r1", "createdAt": "2026-01-01T00:00:00Z", "updatedAt": "2026-01-02T00:00:00Z", "author": map[string]any{"login": "fixture", "__typename": "User"}, "replyTo": map[string]any{"id": "RC0"}}}}}}}
	f := reviewOnlyFixture{items: []gh.ReviewStateItem{item}}
	opts.GraphQLHistory = true
	opts.ReviewStateOnly = true
	opts.ReceiptOperation = "review_state"
	stats, err := New(f, s).Sync(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	if !stats.ReviewStateOnly || stats.CommentsSynced != 0 || stats.ReviewThreadsSynced != 1 {
		t.Fatalf("unexpected stats %+v", stats)
	}
	if before != snapshot() {
		t.Fatal("canonical content/history/vector changed")
	}
	var membership, body, login, comments, url, created, updated, authorType string
	var resolved, outdated int
	s.DB().QueryRow("select review_thread_ids_json from pull_request_review_thread_syncs where thread_id=?", head.ID).Scan(&membership)
	s.DB().QueryRow("select first_comment_body,first_author_login,comments_json,is_resolved,is_outdated,first_comment_url,first_comment_created_at,first_comment_updated_at,first_author_type from pull_request_review_threads where thread_id=?", head.ID).Scan(&body, &login, &comments, &resolved, &outdated, &url, &created, &updated, &authorType)
	if membership != `["RT1"]` || body != "inline body" || login != "fixture" || resolved != 1 || outdated != 1 {
		t.Fatal("review contract lost", membership, body, login)
	}
	if url != "https://github.com/openclaw/gitcrawl/pull/8#r1" || created != "2026-01-01T00:00:00Z" || updated != "2026-01-02T00:00:00Z" || authorType != "User" {
		t.Fatal("first-comment metadata lost")
	}
	saved := membership + comments
	for _, failure := range []error{errors.New("partial response"), context.Canceled} {
		f.err = failure
		if _, err = New(f, s).Sync(ctx, opts); err == nil {
			t.Fatal("failure accepted")
		}
		s.DB().QueryRow("select review_thread_ids_json from pull_request_review_thread_syncs where thread_id=?", head.ID).Scan(&membership)
		s.DB().QueryRow("select comments_json from pull_request_review_threads where thread_id=?", head.ID).Scan(&comments)
		if saved != membership+comments || before != snapshot() {
			t.Fatal("failed observation overwrote retained data")
		}
	}
	f.err = nil
	f.items[0].RepositoryID = "wrong"
	if _, err = New(f, s).Sync(ctx, opts); err == nil {
		t.Fatal("wrong repository accepted")
	}
	f.items[0] = item
	f.items[0].NodeID = "wrong"
	if stringValue(raw["node_id"]) != "" {
		if _, err = New(f, s).Sync(ctx, opts); err == nil {
			t.Fatal("wrong node accepted")
		}
	}
	// A later identity failure rolls back earlier state/history in the batch.
	f.items = []gh.ReviewStateItem{item, item}
	f.items[1].Number = 9
	f.items[0].Threads[0]["isResolved"] = false
	opts.Numbers = []int{8, 9}
	var revisionsBefore, revisionsAfter int
	s.DB().QueryRow("select count(*) from pull_request_review_thread_revisions").Scan(&revisionsBefore)
	if _, err = New(f, s).Sync(ctx, opts); err == nil {
		t.Fatal("missing parent accepted")
	}
	s.DB().QueryRow("select count(*) from pull_request_review_thread_revisions").Scan(&revisionsAfter)
	s.DB().QueryRow("select is_resolved from pull_request_review_threads where thread_id=?", head.ID).Scan(&resolved)
	if resolved != 1 || revisionsAfter != revisionsBefore || before != snapshot() {
		t.Fatal("partial batch publication or canonical mutation")
	}
}
