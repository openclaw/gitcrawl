package store

import (
	"context"
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
