package store

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
)

func TestPublicationRepairPreservesSourceEvidence(t *testing.T) {
	ctx := context.Background()
	s, e := Open(ctx, filepath.Join(t.TempDir(), "source.db"))
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	_, e = s.db.Exec(`INSERT INTO repositories(id,owner,name,full_name,github_repo_id,raw_json,updated_at) VALUES(1,'fixture','repo','fixture/repo',1,'{}','2026-01-01');
 INSERT INTO threads(id,repo_id,github_id,number,kind,state,title,body,html_url,labels_json,assignees_json,raw_json,content_hash,updated_at) VALUES(1,1,'pr',1,'pull_request','open','','','','[]','[]','{}','h','2026-01-01')`)
	if e != nil {
		t.Fatal(e)
	}
	raw := `{"state":"APPROVED","created_at":"2026-01-01T00:00:00Z","submitted_at":"2026-01-03T12:34:56Z","user":{"login":"a","type":"User"}}`
	id, e := s.UpsertComment(ctx, Comment{ThreadID: 1, GitHubID: "review", CommentType: "pull_review", RawJSON: raw, CreatedAtGitHub: "2026-01-01T00:00:00Z"})
	if e != nil {
		t.Fatal(e)
	}
	_, e = s.db.Exec("UPDATE comments SET submitted_at_gh=NULL,publication_at_gh=NULL; UPDATE comment_revisions SET submitted_at_gh=NULL,publication_at_gh=NULL")
	if e != nil {
		t.Fatal(e)
	}
	var originalRecorded string
	s.db.QueryRow("SELECT recorded_at FROM comment_revisions").Scan(&originalRecorded)
	preview, e := s.RepairPublication(ctx, false)
	if e != nil || preview.WouldChange != 2 || preview.Changed != 0 {
		t.Fatalf("dry-run %+v %v", preview, e)
	}
	r, e := s.RepairPublication(ctx, true)
	if e != nil || r.Changed != 2 {
		t.Fatalf("%+v %v", r, e)
	}
	r, e = s.RepairPublication(ctx, true)
	if e != nil || r.Changed != 0 {
		t.Fatalf("replay %+v %v", r, e)
	}
	var published, submitted, created, stored string
	s.db.QueryRow("SELECT publication_at_gh,submitted_at_gh,created_at_gh,raw_json FROM comments WHERE id=?", id).Scan(&published, &submitted, &created, &stored)
	if published != "2026-01-03T12:34:56Z" || submitted != published || created != "2026-01-01T00:00:00Z" || stored != raw {
		t.Fatalf("source timestamps/evidence changed incorrectly")
	}
	var n int
	var recorded string
	s.db.QueryRow("SELECT count(*),min(recorded_at) FROM comment_revisions").Scan(&n, &recorded)
	if n != 1 || recorded != originalRecorded {
		t.Fatal("repair manufactured a source revision")
	}
	for _, c := range []struct{ kind, raw, want string }{
		{"pull_review", `{"state":"PENDING","submitted_at":"2026-01-01T00:00:00Z"}`, ""},
		{"pull_review", `{"submitted_at":"not a date"}`, ""},
		{"pull_review", `{"_graphql":{"submittedAt":"2026-01-01T00:00:00+02:00"}}`, "2025-12-31T22:00:00Z"},
		{"pull_review_comment", `{"created_at":"2026-01-01T00:00:00Z","_graphql":{"publishedAt":"2026-01-02T00:00:00Z"}}`, "2026-01-02T00:00:00Z"},
	} {
		if got := CommentPublication(c.kind, c.raw).Published; got != c.want {
			t.Fatalf("publication=%q want %q", got, c.want)
		}
	}
	nodes := []map[string]any{{"id": "comment-node", "author": map[string]any{"id": "user-one", "login": "same", "__typename": "User"}}, {"id": "other-node", "author": map[string]any{"id": "user-two", "login": "same", "__typename": "User"}}}
	if e = s.SaveActorEvidence(ctx, nodes, "2026-01-01T00:00:00Z"); e != nil {
		t.Fatal(e)
	}
	s.db.QueryRow("SELECT count(distinct actor_node_id) FROM actor_identity_evidence").Scan(&n)
	if n != 2 {
		t.Fatal("login reuse merged identities")
	}
	_, _ = json.Marshal(nodes)
}

func TestAnalyticsQueuesNewUnresolvedSourceIdentities(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "source.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, raw := range []string{`{"node_id":"content","user":{"node_id":"actor"}}`, `{"node_id":"unresolved","user":null}`} {
		if err := s.queueAnalyticsActor(ctx, raw); err != nil {
			t.Fatal(err)
		}
	}
	var count int
	if err := s.DB().QueryRowContext(ctx, `SELECT count(*) FROM analytics_pending_nodes WHERE (node_id='actor' AND kind='profile') OR (node_id='unresolved' AND kind='identity')`).Scan(&count); err != nil || count != 2 {
		t.Fatalf("count=%d err=%v", count, err)
	}
}
