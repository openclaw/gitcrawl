package store

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestPortableRecentSelection(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, t.TempDir()+"/recent.db")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	old := "2026-01-01T00:00:00Z"
	cutoff := "2026-11-01T00:00:00.000000001Z"
	since, _ := time.Parse(time.RFC3339Nano, cutoff)
	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := s.DB().ExecContext(ctx, query, args...); err != nil {
			t.Fatal(err)
		}
	}
	exec(`insert into repositories(id,owner,name,full_name,raw_json,updated_at) values(1,'example','repo','example/repo','{}',?)`, old)
	cases := []struct {
		name, state       string
		updated, optional any
		commentField      string
		commentTime       any
		keep              bool
	}{
		{"old closed optional NULL", "closed", old, nil, "", nil, false},
		{"old open", "open", old, nil, "", nil, true},
		{"unknown state", "unknown", old, nil, "", nil, true},
		{"missing thread update", "closed", nil, nil, "", nil, true},
		{"malformed thread update", "closed", "bad", nil, "", nil, true},
		{"one nanosecond before", "closed", "2026-11-01T00:00:00Z", nil, "", nil, false},
		{"exact cutoff", "closed", cutoff, nil, "", nil, true},
		{"offset before", "closed", "2026-11-01T01:00:00+01:00", nil, "", nil, false},
		{"offset equal", "closed", "2026-11-01T01:00:00.000000001+01:00", nil, "", nil, true},
		{"future", "closed", "2027-01-01T00:00:00Z", nil, "", nil, true},
		{"malformed optional", "closed", old, "bad", "", nil, true},
		{"recent closure", "closed", old, cutoff, "", nil, true},
		{"recent comment creation", "closed", old, nil, "created_at_gh", cutoff, true},
		{"recent comment edit", "closed", old, nil, "updated_at_gh", cutoff, true},
		{"recent submission", "closed", old, nil, "submitted_at_gh", cutoff, true},
		{"recent publication", "closed", old, nil, "publication_at_gh", cutoff, true},
		{"recent deletion", "closed", old, nil, "deleted_at", cutoff, true},
		{"comment creation fallback", "closed", old, nil, "updated_at_gh", nil, false},
		{"malformed comment", "closed", old, nil, "updated_at_gh", "bad", true},
		{"unknown comment", "closed", old, nil, "created_at_gh", nil, true},
	}
	for i, tc := range cases {
		id := i + 1
		exec(`insert into threads(id,repo_id,github_id,number,kind,state,title,html_url,labels_json,assignees_json,raw_json,content_hash,created_at_gh,updated_at_gh,closed_at_gh,updated_at)
			values(?,1,?,?,'issue',?,'fixture','https://example.test/','[]','[]','{}','hash',?,?,?,?)`, id, fmt.Sprint(id), id, tc.state, old, tc.updated, tc.optional, "2027-01-01T00:00:00Z")
		if tc.commentField != "" {
			exec(`insert into comments(id,thread_id,github_id,comment_type,body,raw_json,created_at_gh) values(?,?,?,'issue_comment','old body','{}',?)`, id, id, fmt.Sprint(id), old)
			if tc.commentField == "deleted_at" {
				exec(`update comments set deleted_at=?,deletion_reason='not returned' where id=?`, tc.commentTime, id)
			} else {
				exec(`update comments set `+tc.commentField+`=? where id=?`, tc.commentTime, id)
			}
		}
	}
	if err := s.RestrictPortableSince(ctx, since); err != nil {
		t.Fatal(err)
	}
	for i, tc := range cases {
		var exists bool
		if err := s.DB().QueryRow(`select exists(select 1 from threads where id=?)`, i+1).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if exists != tc.keep {
			t.Errorf("%s retained=%v want %v", tc.name, exists, tc.keep)
		}
	}
	if count, err := portableForeignKeyViolationCount(ctx, s.DB()); err != nil || count != 0 {
		t.Fatalf("FK: %d %v", count, err)
	}
}

func TestPortableRecentCommentBatchBoundary(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, t.TempDir()+"/batch.db")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	_, err = s.DB().Exec(`insert into repositories(id,owner,name,full_name,raw_json,updated_at) values(1,'example','repo','example/repo','{}','old');
		insert into threads(id,repo_id,github_id,number,kind,state,title,html_url,labels_json,assignees_json,raw_json,content_hash,updated_at_gh,updated_at)
		values(1,1,'1',1,'issue','closed','','','[]','[]','{}','hash','2026-01-01T00:00:00Z','old');
		with recursive ids(n) as (select 1 union all select n+1 from ids where n<501)
		insert into comments(id,thread_id,github_id,comment_type,body,raw_json,created_at_gh)
		select n,1,cast(n as text),'issue_comment','','{}',case when n=501 then '2026-11-01T00:00:00Z' else '2026-01-01T00:00:00Z' end from ids;`)
	if err != nil {
		t.Fatal(err)
	}
	since, _ := time.Parse(time.RFC3339, "2026-11-01T00:00:00Z")
	if err := s.RestrictPortableSince(ctx, since); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := s.DB().QueryRow(`select count(*) from comments`).Scan(&count); err != nil || count != 501 {
		t.Fatalf("retained child closure: %d %v", count, err)
	}
}
