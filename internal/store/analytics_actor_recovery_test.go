package store

import (
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"
)

func TestAnalyticsActorRecoverySeedsAndRefreshesNativeIdentitiesAtomically(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "archive.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	// Historical rows predate the enqueue-on-capture path.
	_, err = s.DB().ExecContext(ctx, `INSERT INTO repositories(id,owner,name,full_name,github_repo_id,raw_json,updated_at) VALUES(1,'fixture','repo','fixture/repo','1','{}','2026-01-01');
 INSERT INTO threads(id,repo_id,github_id,number,kind,state,title,html_url,labels_json,assignees_json,raw_json,content_hash,updated_at) VALUES
 (1,1,'1',1,'issue','open','fixture','','[]','[]','{"node_id":"content-unknown","user":null}','h','2026-01-01'),
 (2,1,'2',2,'issue','open','fixture','','[]','[]','{"node_id":"content-known","user":{"node_id":"actor-known"}}','h','2026-01-01')`)
	if err != nil {
		t.Fatal(err)
	}
	for _, profiles := range []bool{false, true} {
		if err = s.SeedAnalyticsNodes(ctx, profiles); err != nil {
			t.Fatal(err)
		}
	}
	assertNodes := func(profiles bool, want []string) {
		t.Helper()
		var got []string
		var err error
		if profiles {
			got, err = s.AnalyticsProfileNodes(ctx, 100)
		} else {
			got, err = s.AnalyticsIdentityNodes(ctx, 100)
		}
		if err != nil {
			t.Fatal(err)
		}
		slices.Sort(got)
		slices.Sort(want)
		if !slices.Equal(got, want) {
			t.Fatalf("profiles=%v nodes=%v want%v", profiles, got, want)
		}
	}
	assertNodes(false, []string{"content-unknown"})
	assertNodes(true, []string{"actor-known"})
	now := time.Now().UTC()
	at := now.Format(time.RFC3339Nano)
	evidence := map[string]any{"id": "content-unknown", "author": map[string]any{"id": "actor-recovered", "login": "shared-login", "__typename": "User"}}
	if err = s.SaveActorEvidence(ctx, []map[string]any{evidence}, at); err != nil {
		t.Fatal(err)
	}
	assertNodes(false, nil)
	assertNodes(true, []string{"actor-known", "actor-recovered"})
	profile := func(id, name string) map[string]any {
		return map[string]any{"id": id, "login": "shared-login", "__typename": "User", "name": name, "bio": "fixture profile", "url": "https://github.com/shared-login", "createdAt": "2026-01-01T02:00:00+02:00"}
	}
	first, second := profile("actor-known", "Known"), profile("actor-recovered", "Recovered")
	if err = s.SaveActorProfiles(ctx, []map[string]any{first, second}, at); err != nil {
		t.Fatal(err)
	}
	assertNodes(true, nil)
	var count int
	if err = s.DB().QueryRowContext(ctx, "SELECT count(*) FROM actor_profiles WHERE login='shared-login'").Scan(&count); err != nil || count != 2 {
		t.Fatalf("login reuse merged native actors: %d %v", count, err)
	}
	read := func(id string) (string, string, string) {
		t.Helper()
		var raw, created, observed string
		if err := s.DB().QueryRowContext(ctx, "SELECT raw_json,created_at,observed_at FROM actor_profiles WHERE node_id=?", id).Scan(&raw, &created, &observed); err != nil {
			t.Fatal(err)
		}
		return raw, created, observed
	}
	raw, created, observed := read("actor-known")
	var retained map[string]any
	if err = json.Unmarshal([]byte(raw), &retained); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(retained, first) || created != "2026-01-01T00:00:00Z" || observed != at {
		t.Fatal("native profile evidence or provider date changed")
	}
	changed := profile("actor-known", "Changed")
	invalid := profile("actor-recovered", "Must not apply")
	invalid["unsupported"] = make(chan int)
	if err = s.SaveActorProfiles(ctx, []map[string]any{changed, invalid}, at); err == nil {
		t.Fatal("invalid batch accepted")
	}
	after, _, _ := read("actor-known")
	if after != raw {
		t.Fatal("failed later profile leaked an earlier update")
	}
	// Failed identity capture must roll back its newly enqueued profile as well.
	invalidEvidence := map[string]any{"id": "other-content", "author": map[string]any{"id": "actor-leaked"}, "unsupported": make(chan int)}
	if err = s.SaveActorEvidence(ctx, []map[string]any{invalidEvidence}, at); err == nil {
		t.Fatal("invalid actor evidence accepted")
	}
	assertNodes(true, nil)
	if err = s.DB().QueryRowContext(ctx, "SELECT count(*) FROM actor_identity_evidence").Scan(&count); err != nil || count != 1 {
		t.Fatalf("failed identity batch changed evidence: %d %v", count, err)
	}
	// Fresh profiles are not polled repeatedly, but become eligible after 24 hours.
	if err = s.SaveActorProfiles(ctx, []map[string]any{first}, now.Add(-25*time.Hour).Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	assertNodes(true, []string{"actor-known"})
	if err = s.SaveActorProfiles(ctx, []map[string]any{changed}, at); err != nil {
		t.Fatal(err)
	}
	assertNodes(true, nil)
	refreshed, _, _ := read("actor-known")
	if err = json.Unmarshal([]byte(refreshed), &retained); err != nil || retained["name"] != "Changed" || retained["id"] != "actor-known" {
		t.Fatalf("refresh lost native identity: %+v %v", retained, err)
	}
	if err = s.SeedAnalyticsNodes(ctx, true); err != nil {
		t.Fatal(err)
	}
	if err = s.SeedAnalyticsNodes(ctx, false); err != nil {
		t.Fatal(err)
	}
	assertNodes(true, nil)
	assertNodes(false, nil)
}
