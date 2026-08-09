package store

import (
	"context"
	"testing"
)

func TestRestrictPortableRepositoryRemovesDependentCurrentSchemaRows(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, t.TempDir()+"/scope.db")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	statements := []string{
		`insert into repositories(id, owner, name, full_name, raw_json, updated_at) values(1, 'openclaw', 'keep', 'openclaw/keep', '{}', '2026-08-08T00:00:00Z')`,
		`insert into repositories(id, owner, name, full_name, raw_json, updated_at) values(2, 'openclaw', 'drop', 'openclaw/drop', '{}', '2026-08-08T00:00:00Z')`,
		`insert into threads(id, repo_id, github_id, number, kind, state, title, html_url, labels_json, assignees_json, raw_json, content_hash, updated_at) values(1, 1, 'T1', 1, 'pull_request', 'open', 'keep', 'https://github.com/openclaw/keep/pull/1', '[]', '[]', '{}', 'keep', '2026-08-08T00:00:00Z')`,
		`insert into threads(id, repo_id, github_id, number, kind, state, title, html_url, labels_json, assignees_json, raw_json, content_hash, updated_at) values(2, 2, 'T2', 2, 'pull_request', 'open', 'drop', 'https://github.com/openclaw/drop/pull/2', '[]', '[]', '{}', 'drop', '2026-08-08T00:00:00Z')`,
		`insert into comments(id, thread_id, github_id, comment_type, body, raw_json) values(1, 1, 'C1', 'issue_comment', 'keep', '{}')`,
		`insert into comments(id, thread_id, github_id, comment_type, body, raw_json) values(2, 2, 'C2', 'issue_comment', 'drop', '{}')`,
		`insert into comment_revisions(id, comment_id, body, raw_json, recorded_at) values(1, 1, 'keep history', '{}', '2026-08-08T00:00:00Z')`,
		`insert into comment_revisions(id, comment_id, body, raw_json, recorded_at) values(2, 2, 'drop history', '{}', '2026-08-08T00:00:00Z')`,
		`insert into thread_revisions(id, thread_id, content_hash, title_hash, body_hash, labels_hash, created_at) values(1, 1, 'keep', 'keep', 'keep', 'keep', '2026-08-08T00:00:00Z')`,
		`insert into thread_revisions(id, thread_id, content_hash, title_hash, body_hash, labels_hash, created_at) values(2, 2, 'drop', 'drop', 'drop', 'drop', '2026-08-08T00:00:00Z')`,
		`insert into thread_fingerprints(id, thread_revision_id, algorithm_version, fingerprint_hash, fingerprint_slug, title_tokens_json, body_token_hash, linked_refs_json, file_set_hash, module_buckets_json, simhash64, feature_json, created_at) values(1, 1, 'v1', 'keep', 'keep', '[]', 'keep', '[]', 'keep', '[]', '1', '{}', '2026-08-08T00:00:00Z')`,
		`insert into thread_fingerprints(id, thread_revision_id, algorithm_version, fingerprint_hash, fingerprint_slug, title_tokens_json, body_token_hash, linked_refs_json, file_set_hash, module_buckets_json, simhash64, feature_json, created_at) values(2, 2, 'v1', 'drop', 'drop', '[]', 'drop', '[]', 'drop', '[]', '2', '{}', '2026-08-08T00:00:00Z')`,
		`insert into pull_request_details(thread_id, repo_id, number, raw_json, fetched_at, updated_at) values(1, 1, 1, '{}', '2026-08-08T00:00:00Z', '2026-08-08T00:00:00Z')`,
		`insert into pull_request_details(thread_id, repo_id, number, raw_json, fetched_at, updated_at) values(2, 2, 2, '{}', '2026-08-08T00:00:00Z', '2026-08-08T00:00:00Z')`,
	}
	for _, statement := range statements {
		if _, err := st.DB().ExecContext(ctx, statement); err != nil {
			t.Fatalf("seed portable scope with %q: %v", statement, err)
		}
	}
	result, err := st.RestrictPortableRepository(ctx, "openclaw/keep")
	if err != nil {
		t.Fatalf("restrict repository: %v", err)
	}
	if result.Repository.ID != 1 || result.Repository.FullName != "openclaw/keep" || result.RepositoriesBefore != 2 || result.RepositoriesRemoved != 1 {
		t.Fatalf("scope result = %+v", result)
	}
	for _, table := range []string{"repositories", "threads", "comments", "comment_revisions", "thread_revisions", "thread_fingerprints", "pull_request_details"} {
		var count int
		if err := st.DB().QueryRowContext(ctx, `select count(*) from `+sqliteIdentifier(table)).Scan(&count); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		if count != 1 {
			t.Fatalf("%s rows = %d, want 1", table, count)
		}
	}
	var title, commentBody, history string
	if err := st.DB().QueryRowContext(ctx, `select title from threads`).Scan(&title); err != nil {
		t.Fatal(err)
	}
	if err := st.DB().QueryRowContext(ctx, `select body from comments`).Scan(&commentBody); err != nil {
		t.Fatal(err)
	}
	if err := st.DB().QueryRowContext(ctx, `select body from comment_revisions`).Scan(&history); err != nil {
		t.Fatal(err)
	}
	if title != "keep" || commentBody != "keep" || history != "keep history" {
		t.Fatalf("retained data = %q / %q / %q", title, commentBody, history)
	}
	violations, err := portableForeignKeyViolationCount(ctx, st.DB())
	if err != nil || violations != 0 {
		t.Fatalf("foreign key violations = %d, err=%v", violations, err)
	}
}

func TestRestrictPortableRepositoryMissingTargetDoesNotMutate(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, t.TempDir()+"/missing.db")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	if _, err := st.DB().ExecContext(ctx, `insert into repositories(id, owner, name, full_name, raw_json, updated_at) values(1, 'openclaw', 'keep', 'openclaw/keep', '{}', '2026-08-08T00:00:00Z')`); err != nil {
		t.Fatal(err)
	}
	if _, err := st.RestrictPortableRepository(ctx, "openclaw/missing"); err == nil {
		t.Fatal("missing repository restriction succeeded")
	}
	var count int
	if err := st.DB().QueryRowContext(ctx, `select count(*) from repositories`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("missing target mutated repository count to %d", count)
	}
}
