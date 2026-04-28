package store

import (
	"bytes"
	"context"
	"database/sql"
	"os"
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

func TestOpenReadOnlyDoesNotMutateStore(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "gitcrawl.db")
	st, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if _, err := st.UpsertRepository(ctx, Repository{
		Owner:     "openclaw",
		Name:      "openclaw",
		FullName:  "openclaw/openclaw",
		RawJSON:   "{}",
		UpdatedAt: "2026-04-27T00:00:00Z",
	}); err != nil {
		t.Fatalf("seed repository: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	before, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("read db before: %v", err)
	}

	readOnly, err := OpenReadOnly(ctx, dbPath)
	if err != nil {
		t.Fatalf("open readonly: %v", err)
	}
	status, err := readOnly.Status(ctx)
	if err != nil {
		t.Fatalf("readonly status: %v", err)
	}
	if status.RepositoryCount != 1 {
		t.Fatalf("repository count: got %d want 1", status.RepositoryCount)
	}
	if err := readOnly.Close(); err != nil {
		t.Fatalf("close readonly: %v", err)
	}
	after, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("read db after: %v", err)
	}
	if !bytes.Equal(after, before) {
		t.Fatal("readonly open mutated database bytes")
	}
}

func TestOpenReadOnlySupportsCanonicalPortableStore(t *testing.T) {
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
		create table repo_sync_state (
			repo_id integer primary key,
			last_full_open_scan_started_at text,
			last_overlapping_open_scan_completed_at text,
			last_non_overlapping_scan_completed_at text,
			last_open_close_reconciled_at text,
			updated_at text not null
		);
		create table cluster_groups (
			id integer primary key,
			repo_id integer not null,
			stable_key text not null,
			stable_slug text not null,
			status text not null,
			cluster_type text not null,
			representative_thread_id integer,
			title text,
			created_at text not null,
			updated_at text not null,
			closed_at text
		);
		insert into repositories(id, owner, name, full_name, updated_at)
		values(1, 'openclaw', 'openclaw', 'openclaw/openclaw', '2026-04-28T00:00:00Z');
		insert into threads(id, repo_id, github_id, number, kind, state, title, body_excerpt, html_url, labels_json, assignees_json, content_hash, updated_at)
		values(1, 1, '1', 42, 'issue', 'open', 'portable issue', 'portable body', 'https://github.com/openclaw/openclaw/issues/42', '[]', '[]', 'hash', '2026-04-28T00:00:00Z');
		insert into repo_sync_state(repo_id, last_open_close_reconciled_at, updated_at)
		values(1, '2026-04-28T01:02:03Z', '2026-04-28T01:02:03Z');
		insert into cluster_groups(id, repo_id, stable_key, stable_slug, status, cluster_type, representative_thread_id, title, created_at, updated_at)
		values(1, 1, 'stable', 'stable', 'active', 'similarity', 1, 'portable cluster', '2026-04-28T00:00:00Z', '2026-04-28T00:00:00Z');
	`)
	if err != nil {
		t.Fatalf("seed portable db: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close seed db: %v", err)
	}
	before, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("read db before: %v", err)
	}

	st, err := OpenReadOnly(ctx, dbPath)
	if err != nil {
		t.Fatalf("open readonly portable: %v", err)
	}
	defer st.Close()

	status, err := st.Status(ctx)
	if err != nil {
		t.Fatalf("portable status: %v", err)
	}
	if status.RepositoryCount != 1 || status.ThreadCount != 1 || status.OpenThreadCount != 1 || status.ClusterCount != 1 {
		t.Fatalf("unexpected portable status: %#v", status)
	}
	if status.LastSyncAt.IsZero() {
		t.Fatalf("portable last sync was not read from repo_sync_state: %#v", status)
	}
	repo, err := st.RepositoryByFullName(ctx, "openclaw/openclaw")
	if err != nil {
		t.Fatalf("portable repository: %v", err)
	}
	if repo.RawJSON != "" {
		t.Fatalf("portable raw json: got %q want empty", repo.RawJSON)
	}
	threads, err := st.ListThreadsFiltered(ctx, ThreadListOptions{RepoID: repo.ID, Numbers: []int{42}})
	if err != nil {
		t.Fatalf("portable threads: %v", err)
	}
	if len(threads) != 1 || threads[0].Body != "portable body" || threads[0].RawJSON != "" {
		t.Fatalf("unexpected portable thread: %#v", threads)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close portable readonly: %v", err)
	}
	after, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("read db after: %v", err)
	}
	if !bytes.Equal(after, before) {
		t.Fatal("readonly portable open mutated database bytes")
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

func TestSearchFallsBackToThreadPayloadsWhenDocumentsArePruned(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	_, err = st.DB().ExecContext(ctx, `
		insert into repositories(id, owner, name, full_name, raw_json, updated_at)
		values(1, 'openclaw', 'openclaw', 'openclaw/openclaw', '{}', '2026-04-26T00:00:00Z');
		insert into threads(repo_id, github_id, number, kind, state, title, body, html_url, labels_json, assignees_json, raw_json, content_hash, updated_at)
		values(1, '1', 73038, 'pull_request', 'open', 'feat(providers): add DeepInfra provider plugin', 'DeepInfra provider plugin', 'https://github.com/openclaw/openclaw/pull/73038', '[]', '[]', '{}', 'hash', '2026-04-27T00:00:00Z');
	`)
	if err != nil {
		t.Fatalf("seed threads: %v", err)
	}

	hits, err := st.SearchDocuments(ctx, 1, "DeepInfra", 10)
	if err != nil {
		t.Fatalf("search documents: %v", err)
	}
	if len(hits) != 1 || hits[0].Number != 73038 {
		t.Fatalf("hits = %#v, want PR 73038", hits)
	}
}

func TestPrunePortablePayloads(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "gitcrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	_, err = st.DB().ExecContext(ctx, `
		insert into repositories(id, owner, name, full_name, raw_json, updated_at)
		values(1, 'openclaw', 'gitcrawl', 'openclaw/gitcrawl', '{"id":1}', '2026-04-26T00:00:00Z');
		insert into threads(id, repo_id, github_id, number, kind, state, title, body, html_url, labels_json, assignees_json, raw_json, content_hash, updated_at)
		values(1, 1, '1', 1, 'issue', 'open', 'download stalls', 'abcdefghijklmnopqrstuvwxyz', 'https://github.com/openclaw/gitcrawl/issues/1', '[]', '[]', '{"body":"abcdefghijklmnopqrstuvwxyz"}', 'hash', '2026-04-26T00:00:00Z');
		insert into documents(thread_id, title, body, raw_text, dedupe_text, updated_at)
		values(1, 'download stalls', 'abcdefghijklmnopqrstuvwxyz', 'download stalls abcdefghijklmnopqrstuvwxyz', 'download stalls', '2026-04-26T00:00:00Z');
		insert into thread_revisions(thread_id, source_updated_at, content_hash, title_hash, body_hash, labels_hash, created_at)
		values(1, '2026-04-26T00:00:00Z', 'hash', 'title-hash', 'body-hash', 'labels-hash', '2026-04-26T00:00:00Z');
		insert into thread_fingerprints(thread_revision_id, algorithm_version, fingerprint_hash, fingerprint_slug, title_tokens_json, body_token_hash, linked_refs_json, file_set_hash, module_buckets_json, simhash64, feature_json, created_at)
		values(1, 'v1', 'fp-hash', 'fp-slug', '["download","stalls"]', 'body-token-hash', '["#1"]', 'files', '["runtime"]', '123', '{"tokens":["download"]}', '2026-04-26T00:00:00Z');
	`)
	if err != nil {
		t.Fatalf("seed prune data: %v", err)
	}

	stats, err := st.PrunePortablePayloads(ctx, PortablePruneOptions{BodyChars: 8})
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if stats.DocumentsDeleted != 1 || stats.FingerprintsPruned != 1 {
		t.Fatalf("unexpected stats: %#v", stats)
	}

	var bodyExcerpt, titleTokens, linkedRefs, buckets, features string
	if st.hasColumn(ctx, "repositories", "raw_json") {
		t.Fatal("repositories.raw_json was not dropped")
	}
	if st.hasColumn(ctx, "threads", "raw_json") {
		t.Fatal("threads.raw_json was not dropped")
	}
	if st.hasColumn(ctx, "threads", "body") {
		t.Fatal("threads.body was not dropped")
	}
	if err := st.DB().QueryRowContext(ctx, `select body_excerpt from threads where id = 1`).Scan(&bodyExcerpt); err != nil {
		t.Fatalf("thread body excerpt: %v", err)
	}
	if err := st.DB().QueryRowContext(ctx, `select title_tokens_json, linked_refs_json, module_buckets_json, feature_json from thread_fingerprints where id = 1`).Scan(&titleTokens, &linkedRefs, &buckets, &features); err != nil {
		t.Fatalf("fingerprint payload: %v", err)
	}
	if st.tableExists(ctx, "documents") {
		t.Fatal("documents table was not dropped")
	}
	if bodyExcerpt != "abcdefgh" || titleTokens != "[]" || linkedRefs != "[]" || buckets != "[]" || features != "{}" {
		t.Fatalf("payloads not pruned: bodyExcerpt=%q titleTokens=%q linkedRefs=%q buckets=%q features=%q", bodyExcerpt, titleTokens, linkedRefs, buckets, features)
	}
}
