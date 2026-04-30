package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
)

func TestSearchDocuments(t *testing.T) {
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
	if _, err := st.UpsertDocument(ctx, Document{ThreadID: threadID, Title: "download stalls", RawText: "large file download stalls", DedupeText: "large file download stalls", UpdatedAt: "2026-04-26T00:00:00Z"}); err != nil {
		t.Fatalf("document: %v", err)
	}

	hits, err := st.SearchDocuments(ctx, repoID, "file", 10)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(hits) != 1 || hits[0].Number != 1 {
		t.Fatalf("unexpected hits: %#v", hits)
	}
}

func TestSearchDocumentsEscapesFTSQuery(t *testing.T) {
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
		RepoID: repoID, GitHubID: "2", Number: 2, Kind: "issue", State: "open",
		Title: "scope upgrade request", Body: "scope upgrade breaks search", HTMLURL: "https://github.com/openclaw/gitcrawl/issues/2",
		LabelsJSON: "[]", AssigneesJSON: "[]", RawJSON: "{}", ContentHash: "hash-2", UpdatedAt: "2026-04-26T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("thread: %v", err)
	}
	if _, err := st.UpsertDocument(ctx, Document{ThreadID: threadID, Title: "scope upgrade request", RawText: "scope upgrade breaks search", DedupeText: "scope upgrade breaks search", UpdatedAt: "2026-04-26T00:00:00Z"}); err != nil {
		t.Fatalf("document: %v", err)
	}

	hits, err := st.SearchDocuments(ctx, repoID, "scope-upgrade", 10)
	if err != nil {
		t.Fatalf("search should not reject hyphenated query: %v", err)
	}
	if len(hits) != 1 || hits[0].Number != 2 {
		t.Fatalf("unexpected hits: %#v", hits)
	}
}

func TestSearchThreadsSupportsPortableSchema(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "portable.sync.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open seed db: %v", err)
	}
	_, err = db.ExecContext(ctx, `
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
		insert into threads(id, repo_id, github_id, number, kind, state, title, body_excerpt, html_url, labels_json, assignees_json, content_hash, updated_at)
		values(1, 1, '1', 42, 'issue', 'open', 'portable store freshness', 'stale sidecar cleanup', 'https://github.com/openclaw/openclaw/issues/42', '[]', '[]', 'hash', '2026-04-30T00:00:00Z');
	`)
	if err != nil {
		t.Fatalf("seed portable db: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close seed db: %v", err)
	}
	st, err := OpenReadOnly(ctx, dbPath)
	if err != nil {
		t.Fatalf("open portable db: %v", err)
	}
	defer st.Close()

	threads, err := st.SearchThreads(ctx, ThreadSearchOptions{RepoID: 1, Query: "sidecar", Kind: "issue", State: "all", Limit: 10})
	if err != nil {
		t.Fatalf("search portable threads: %v", err)
	}
	if len(threads) != 1 || threads[0].Number != 42 || threads[0].Body != "stale sidecar cleanup" || threads[0].RawJSON != "" {
		t.Fatalf("unexpected portable search results: %#v", threads)
	}
}
