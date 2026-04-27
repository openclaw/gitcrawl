package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
)

func TestOpenMigratesSchema(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	var version int
	if err := st.DB().QueryRowContext(ctx, `pragma user_version`).Scan(&version); err != nil {
		t.Fatalf("read user_version: %v", err)
	}
	if version != schemaVersion {
		t.Fatalf("schema version: got %d want %d", version, schemaVersion)
	}
}

func TestStatusOnEmptyStore(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	status, err := st.Status(ctx)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.RepositoryCount != 0 || status.ThreadCount != 0 || status.ClusterCount != 0 {
		t.Fatalf("expected empty status, got %#v", status)
	}
}

func TestOpenMigratesPortableStoreColumns(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "portable.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open seed db: %v", err)
	}
	_, err = db.ExecContext(ctx, `
		create table repositories (
			id integer primary key,
			owner text not null,
			name text not null,
			full_name text not null,
			github_repo_id text,
			updated_at text not null
		);
		create table threads (
			id integer primary key,
			repo_id integer not null,
			github_id text not null,
			number integer not null,
			kind text not null,
			state text not null,
			title text not null,
			body_excerpt text,
			body_length integer not null default 0,
			author_login text,
			author_type text,
			html_url text not null,
			labels_json text not null,
			assignees_json text not null,
			content_hash text not null,
			is_draft integer not null default 0,
			created_at_gh text,
			updated_at_gh text,
			closed_at_gh text,
			merged_at_gh text,
			first_pulled_at text,
			last_pulled_at text,
			updated_at text not null,
			closed_at_local text,
			close_reason_local text
		);
		insert into repositories(id, owner, name, full_name, updated_at)
		values(1, 'openclaw', 'openclaw', 'openclaw/openclaw', '2026-04-26T00:00:00Z');
		insert into threads(id, repo_id, github_id, number, kind, state, title, body_excerpt, html_url, labels_json, assignees_json, content_hash, updated_at)
		values(1, 1, '1', 42, 'issue', 'open', 'portable issue', 'portable body', 'https://github.com/openclaw/openclaw/issues/42', '[]', '[]', 'hash', '2026-04-26T00:00:00Z');
	`)
	if err != nil {
		t.Fatalf("seed portable db: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close seed db: %v", err)
	}

	st, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	repo, err := st.RepositoryByFullName(ctx, "openclaw/openclaw")
	if err != nil {
		t.Fatalf("repository: %v", err)
	}
	threads, err := st.ListThreadsFiltered(ctx, ThreadListOptions{RepoID: repo.ID, Numbers: []int{42}})
	if err != nil {
		t.Fatalf("threads: %v", err)
	}
	if len(threads) != 1 || threads[0].Body != "portable body" {
		t.Fatalf("unexpected portable thread: %#v", threads)
	}
}

func TestDocumentsFTSWorks(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	_, err = st.DB().ExecContext(ctx, `
		insert into repositories(owner, name, full_name, raw_json, updated_at)
		values('openclaw', 'gitcrawl', 'openclaw/gitcrawl', '{}', '2026-04-26T00:00:00Z');
		insert into threads(repo_id, github_id, number, kind, state, title, body, html_url, labels_json, assignees_json, raw_json, content_hash, updated_at)
		values(1, '1', 1, 'issue', 'open', 'download stalls', 'body', 'https://github.com/openclaw/gitcrawl/issues/1', '[]', '[]', '{}', 'hash', '2026-04-26T00:00:00Z');
		insert into documents(thread_id, title, body, raw_text, dedupe_text, updated_at)
		values(1, 'download stalls', 'body', 'download stalls body', 'download stalls', '2026-04-26T00:00:00Z');
	`)
	if err != nil {
		t.Fatalf("seed documents: %v", err)
	}

	var count int
	if err := st.DB().QueryRowContext(ctx, `select count(*) from documents_fts where documents_fts match 'download'`).Scan(&count); err != nil {
		t.Fatalf("query fts: %v", err)
	}
	if count != 1 {
		t.Fatalf("fts count: got %d want 1", count)
	}
}
