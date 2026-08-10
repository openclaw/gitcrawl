package store

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestPortableThreadsRebuildPreservesSchemaRelationshipsAndUniqueIndexes(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "threads-rebuild.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	statements := []string{
		`alter table threads add column future_payload text not null default 'future-default'`,
		`insert into repositories(id, owner, name, full_name, raw_json, updated_at) values(1, 'openclaw', 'gitcrawl', 'openclaw/gitcrawl', '{"large":"repo"}', '2026-08-09T00:00:00Z')`,
		`insert into threads(
			id, repo_id, github_id, number, kind, state, title, body,
			author_login, author_type, author_association, html_url,
			labels_json, assignees_json, raw_json, content_hash, is_draft,
			created_at_gh, updated_at_gh, closed_at_gh, merged_at_gh,
			closed_at_local, close_reason_local, first_pulled_at, last_pulled_at,
			observation_sequence, evidence_observation_sequence,
			evidence_source_updated_at, updated_at, future_payload
		) values(
			1, 1, 'THREAD-1', 42, 'pull_request', 'closed', 'rebuild thread',
			'abcdefghijklmnopqrstuvwxyz', 'alice', 'User', 'MEMBER',
			'https://github.com/openclaw/gitcrawl/pull/42', '[{"name":"bug"}]',
			'[{"login":"bob"}]', '{"large":"thread"}', 'content-hash', 1,
			'2026-08-01T00:00:00Z', '2026-08-02T00:00:00Z',
			'2026-08-03T00:00:00Z', '2026-08-04T00:00:00Z',
			'2026-08-05T00:00:00Z', 'local close', '2026-08-06T00:00:00Z',
			'2026-08-07T00:00:00Z', 44, 43, '2026-08-02T00:00:00Z',
			'2026-08-09T00:00:00Z', 'future-retained'
		)`,
		`insert into comments(id, thread_id, github_id, comment_type, body, raw_json) values(1, 1, 'COMMENT-1', 'issue_comment', 'comment-payload-retained', '{}')`,
		`insert into thread_revisions(id, thread_id, content_hash, title_hash, body_hash, labels_hash, observation_sequence, created_at) values(1, 1, 'revision-content', 'title-hash', 'body-hash', 'labels-hash', 44, '2026-08-09T00:00:00Z')`,
		`insert into thread_fingerprints(id, thread_revision_id, algorithm_version, fingerprint_hash, fingerprint_slug, title_tokens_json, body_token_hash, linked_refs_json, file_set_hash, module_buckets_json, simhash64, feature_json, created_at) values(1, 1, 'v1', 'fingerprint-retained', 'slug-retained', '[]', 'body-token', '[]', 'files', '[]', '123', '{}', '2026-08-09T00:00:00Z')`,
		`insert into pull_request_details(thread_id, repo_id, number, base_sha, head_sha, head_ref, additions, deletions, changed_files, raw_json, fetched_at, updated_at) values(1, 1, 42, 'base', 'head', 'feature', 10, 2, 1, '{}', '2026-08-09T00:00:00Z', '2026-08-09T00:00:00Z')`,
		`insert into pull_request_files(thread_id, position, path, status, additions, deletions, changes, previous_path, patch, raw_json, fetched_at) values(1, 0, 'new.go', 'renamed', 10, 2, 12, 'old.go', '@@ patch', '{}', '2026-08-09T00:00:00Z')`,
		`insert into pull_request_checks(id, thread_id, name, status, conclusion, details_url, raw_json, fetched_at) values(1, 1, 'CI', 'completed', 'success', 'https://example.test/check', '{}', '2026-08-09T00:00:00Z')`,
		`insert into pull_request_review_threads(thread_id, review_thread_id, path, line, is_resolved, first_comment_body, comments_json, raw_json, fetched_at) values(1, 'REVIEW-1', 'new.go', 7, 1, 'review-body-retained', '[]', '{}', '2026-08-09T00:00:00Z')`,
		`insert into pull_request_review_thread_revisions(id, thread_id, review_thread_id, path, line, is_resolved, first_comment_body, comments_json, raw_json, fetched_at, recorded_at) values(1, 1, 'REVIEW-1', 'new.go', 7, 1, 'review-revision-retained', '[]', '{}', '2026-08-09T00:00:00Z', '2026-08-09T00:00:00Z')`,
		`insert into pull_request_review_thread_syncs(thread_id, fetched_at) values(1, '2026-08-09T00:00:00Z')`,
		`insert into thread_child_observation_memberships(thread_id, family, observation_sequence, member_ids_json) values(1, 'comments', 44, '["COMMENT-1"]')`,
		`create index custom_threads_title on threads(title)`,
		`create unique index unique_threads_github_id on threads(github_id)`,
		`create table threads_audit(thread_id integer not null)`,
		`create trigger custom_threads_audit after insert on threads begin insert into threads_audit(thread_id) values(new.id); end`,
		`update observation_schema_convergence set checked_observation_sequence = 0 where id = 1`,
	}
	for _, statement := range statements {
		if _, err := st.DB().ExecContext(ctx, statement); err != nil {
			t.Fatalf("seed rebuild with %q: %v", statement, err)
		}
	}
	stats, err := st.PrunePortablePayloads(ctx, PortablePruneOptions{
		BodyChars:                     12,
		DeferSecureRewrite:            true,
		RetainSanitizedPayloadColumns: true,
	})
	if err != nil {
		t.Fatalf("rebuild portable threads: %v", err)
	}
	for _, index := range []string{"custom_threads_title", "idx_threads_repo_number", "idx_threads_repo_state_closed", "idx_threads_repo_updated"} {
		if !slices.Contains(stats.DroppedIndexes, index) {
			t.Fatalf("dropped thread indexes = %v, missing %s", stats.DroppedIndexes, index)
		}
	}
	if slices.Contains(stats.DroppedIndexes, "unique_threads_github_id") {
		t.Fatalf("explicit unique index reported dropped: %v", stats.DroppedIndexes)
	}
	var title, body, excerpt, rawJSON, authorLogin, authorType, association, futurePayload string
	var bodyLength, observationSequence, evidenceSequence int
	if err := st.DB().QueryRowContext(ctx, `
		select title, body, body_excerpt, body_length, raw_json,
		       author_login, author_type, author_association, future_payload,
		       observation_sequence, evidence_observation_sequence
		from threads where id = 1
	`).Scan(&title, &body, &excerpt, &bodyLength, &rawJSON, &authorLogin, &authorType, &association, &futurePayload, &observationSequence, &evidenceSequence); err != nil {
		t.Fatalf("read rebuilt thread: %v", err)
	}
	if title != "rebuild thread" || body != "abcdefghijkl" || excerpt != body || bodyLength != 26 || rawJSON != "" || authorLogin != "alice" || authorType != "User" || association != "MEMBER" || futurePayload != "future-retained" || observationSequence != 44 || evidenceSequence != 43 {
		t.Fatalf("rebuilt thread title=%q body=%q excerpt=%q length=%d raw=%q author=%q/%q/%q future=%q observation=%d/%d", title, body, excerpt, bodyLength, rawJSON, authorLogin, authorType, association, futurePayload, observationSequence, evidenceSequence)
	}
	for _, table := range []string{
		"comments", "thread_revisions", "pull_request_details",
		"pull_request_files", "pull_request_checks", "pull_request_review_threads",
		"pull_request_review_thread_revisions", "pull_request_review_thread_syncs",
		"thread_child_observation_memberships",
	} {
		var count int
		if err := st.DB().QueryRowContext(ctx, `select count(*) from `+sqliteIdentifier(table)+` where thread_id = 1`).Scan(&count); err != nil {
			t.Fatalf("count retained %s: %v", table, err)
		}
		if count != 1 {
			t.Fatalf("retained %s rows = %d, want 1", table, count)
		}
	}
	var fingerprintCount int
	if err := st.DB().QueryRowContext(ctx, `select count(*) from thread_fingerprints where thread_revision_id = 1`).Scan(&fingerprintCount); err != nil {
		t.Fatalf("count retained thread_fingerprints: %v", err)
	}
	if fingerprintCount != 1 {
		t.Fatalf("retained thread_fingerprints rows = %d, want 1", fingerprintCount)
	}
	var patch sql.NullString
	var filePath, fileStatus, previousPath string
	if err := st.DB().QueryRowContext(ctx, `select path, status, previous_path, patch from pull_request_files where thread_id = 1`).Scan(&filePath, &fileStatus, &previousPath, &patch); err != nil {
		t.Fatal(err)
	}
	if filePath != "new.go" || fileStatus != "renamed" || previousPath != "old.go" || patch.Valid {
		t.Fatalf("retained PR file=%q/%q/%q patch=%#v", filePath, fileStatus, previousPath, patch)
	}
	var commentBody, revisionHash, fingerprintHash, baseSHA, headSHA, checkConclusion, reviewPath, reviewBody, membershipJSON string
	var reviewResolved int
	if err := st.DB().QueryRowContext(ctx, `select body from comments where id = 1`).Scan(&commentBody); err != nil {
		t.Fatal(err)
	}
	if err := st.DB().QueryRowContext(ctx, `select content_hash from thread_revisions where id = 1`).Scan(&revisionHash); err != nil {
		t.Fatal(err)
	}
	if err := st.DB().QueryRowContext(ctx, `select fingerprint_hash from thread_fingerprints where id = 1`).Scan(&fingerprintHash); err != nil {
		t.Fatal(err)
	}
	if err := st.DB().QueryRowContext(ctx, `select base_sha, head_sha from pull_request_details where thread_id = 1`).Scan(&baseSHA, &headSHA); err != nil {
		t.Fatal(err)
	}
	if err := st.DB().QueryRowContext(ctx, `select conclusion from pull_request_checks where id = 1`).Scan(&checkConclusion); err != nil {
		t.Fatal(err)
	}
	if err := st.DB().QueryRowContext(ctx, `select path, is_resolved, first_comment_body from pull_request_review_threads where thread_id = 1`).Scan(&reviewPath, &reviewResolved, &reviewBody); err != nil {
		t.Fatal(err)
	}
	if err := st.DB().QueryRowContext(ctx, `select member_ids_json from thread_child_observation_memberships where thread_id = 1 and family = 'comments'`).Scan(&membershipJSON); err != nil {
		t.Fatal(err)
	}
	if commentBody != "comment-payl" || revisionHash != "revision-content" || fingerprintHash != "fingerprint-retained" || baseSHA != "base" || headSHA != "head" || checkConclusion != "success" || reviewPath != "new.go" || reviewResolved != 1 || reviewBody != "review-body-" || membershipJSON != `["COMMENT-1"]` {
		t.Fatalf("retained child values comment=%q revision=%q fingerprint=%q pr=%q/%q check=%q review=%q/%d/%q membership=%q", commentBody, revisionHash, fingerprintHash, baseSHA, headSHA, checkConclusion, reviewPath, reviewResolved, reviewBody, membershipJSON)
	}
	if !indexExistsForPortableTest(t, st.DB(), "unique_threads_github_id") {
		t.Fatal("explicit unique threads index was not recreated")
	}
	if _, err := st.DB().ExecContext(ctx, `insert into threads(repo_id, github_id, number, kind, state, title, html_url, labels_json, assignees_json, raw_json, content_hash, updated_at) values(1, 'THREAD-2', 42, 'pull_request', 'open', 'duplicate tuple', 'https://example.test/2', '[]', '[]', '', 'hash-2', '2026-08-09T00:00:00Z')`); err == nil {
		t.Fatal("table-declared unique(repo_id,kind,number) was not enforced")
	}
	if _, err := st.DB().ExecContext(ctx, `insert into threads(repo_id, github_id, number, kind, state, title, html_url, labels_json, assignees_json, raw_json, content_hash, updated_at) values(1, 'THREAD-1', 43, 'pull_request', 'open', 'duplicate github id', 'https://example.test/3', '[]', '[]', '', 'hash-3', '2026-08-09T00:00:00Z')`); err == nil {
		t.Fatal("explicit unique threads index was not enforced")
	}
	var auditCount int
	if err := st.DB().QueryRowContext(ctx, `select count(*) from threads_audit`).Scan(&auditCount); err != nil {
		t.Fatal(err)
	}
	if auditCount != 0 {
		t.Fatalf("bulk rebuild fired custom trigger %d times", auditCount)
	}
	if _, err := st.DB().ExecContext(ctx, `insert into threads(repo_id, github_id, number, kind, state, title, html_url, labels_json, assignees_json, raw_json, content_hash, updated_at) values(1, 'THREAD-NEW', 44, 'pull_request', 'open', 'new thread', 'https://example.test/44', '[]', '[]', '', 'hash-44', '2026-08-09T00:00:00Z')`); err != nil {
		t.Fatalf("insert through recreated custom trigger: %v", err)
	}
	if err := st.DB().QueryRowContext(ctx, `select count(*) from threads_audit`).Scan(&auditCount); err != nil {
		t.Fatal(err)
	}
	if auditCount != 1 {
		t.Fatalf("recreated custom trigger audit rows = %d, want 1", auditCount)
	}
	for _, table := range []string{"comments", "thread_revisions", "pull_request_details", "pull_request_files", "pull_request_checks", "pull_request_review_threads", "pull_request_review_thread_syncs", "thread_child_observation_memberships"} {
		if !foreignKeyReferencesPortableTable(t, st.DB(), table, "threads") {
			t.Fatalf("child schema %s no longer references threads", table)
		}
	}
	violations, err := portableForeignKeyViolationCount(ctx, st.DB())
	if err != nil || violations != 0 {
		t.Fatalf("rebuilt threads FK violations=%d err=%v", violations, err)
	}
	var triggerCount int
	if err := st.DB().QueryRowContext(ctx, `select count(*) from sqlite_schema where type = 'trigger' and tbl_name = 'threads'`).Scan(&triggerCount); err != nil {
		t.Fatal(err)
	}
	if triggerCount != 4 {
		t.Fatalf("rebuilt threads triggers = %d, want 4", triggerCount)
	}
	var convergence int
	if err := st.DB().QueryRowContext(ctx, `select checked_observation_sequence from observation_schema_convergence where id = 1`).Scan(&convergence); err != nil {
		t.Fatal(err)
	}
	if convergence != -1 {
		t.Fatalf("observation convergence after rebuild = %d, want -1", convergence)
	}
}

func TestRewritePortableThreadsCreateSQLReplacesOnlyTableIdentifier(t *testing.T) {
	original := "CREATE TABLE IF NOT EXISTS \"threads\" (id integer primary key, title text not null, unique(title))"
	rewritten, err := rewritePortableThreadsCreateSQL(original, "threads_portable_0123")
	if err != nil {
		t.Fatalf("rewrite threads DDL: %v", err)
	}
	want := "CREATE TABLE IF NOT EXISTS \"threads_portable_0123\" (id integer primary key, title text not null, unique(title))"
	if rewritten != want {
		t.Fatalf("rewritten DDL = %q, want %q", rewritten, want)
	}
	if _, err := rewritePortableThreadsCreateSQL("CREATE TABLE threads_archive(id integer)", "threads_portable_0123"); err == nil {
		t.Fatal("unrecognized threads DDL was accepted")
	}
}

func TestPortableThreadsRebuildPreservesAutoincrementAndDependentView(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "autoincrement.db"))
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	_, err = db.ExecContext(ctx, `
		pragma foreign_keys = on;
		create table repositories(id integer primary key);
		create table threads(
			id integer primary key autoincrement,
			repo_id integer not null references repositories(id),
			github_id text not null,
			number integer not null,
			kind text not null,
			title text not null,
			body text,
			raw_json text not null,
			body_excerpt text,
			body_length integer not null default 0,
			unique(repo_id, kind, number)
		);
		create table child_rows(thread_id integer not null references threads(id));
		create view thread_titles as select id, title from threads;
		create index ordinary_threads_title on threads(title);
		create unique index unique_threads_github on threads(github_id);
		insert into repositories(id) values(1);
		insert into threads(id, repo_id, github_id, number, kind, title, body, raw_json, body_excerpt, body_length)
		values(1, 1, 'one', 1, 'issue', 'retained title', 'full body', '{"raw":true}', 'compact body', 9);
		insert into threads(id, repo_id, github_id, number, kind, title, body, raw_json, body_excerpt, body_length)
		values(100, 1, 'deleted-high', 100, 'issue', 'deleted title', 'deleted', '{}', 'deleted', 7);
		delete from threads where id = 100;
		insert into child_rows(thread_id) values(1);
	`)
	if err != nil {
		t.Fatalf("seed autoincrement rebuild: %v", err)
	}
	st := &Store{db: db, path: "autoincrement.db"}
	dropped, err := st.rebuildPortableCompatibilityThreads(ctx)
	if err != nil {
		t.Fatalf("rebuild autoincrement threads: %v", err)
	}
	if !slices.Contains(dropped, "ordinary_threads_title") || slices.Contains(dropped, "unique_threads_github") {
		t.Fatalf("rebuild dropped indexes = %v", dropped)
	}
	var sequence int64
	if err := db.QueryRowContext(ctx, `select seq from sqlite_sequence where name = 'threads'`).Scan(&sequence); err != nil {
		t.Fatalf("read threads sequence: %v", err)
	}
	if sequence != 100 {
		t.Fatalf("threads sequence = %d, want 100", sequence)
	}
	result, err := db.ExecContext(ctx, `insert into threads(repo_id, github_id, number, kind, title, body, raw_json, body_excerpt, body_length) values(1, 'next', 2, 'issue', 'next title', 'next', '', 'next', 4)`)
	if err != nil {
		t.Fatalf("insert after rebuild: %v", err)
	}
	nextID, err := result.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	if nextID != 101 {
		t.Fatalf("next threads id = %d, want 101", nextID)
	}
	var viewTitle, body, rawJSON string
	if err := db.QueryRowContext(ctx, `select title from thread_titles where id = 1`).Scan(&viewTitle); err != nil {
		t.Fatalf("query retained view: %v", err)
	}
	if err := db.QueryRowContext(ctx, `select body, raw_json from threads where id = 1`).Scan(&body, &rawJSON); err != nil {
		t.Fatal(err)
	}
	if viewTitle != "retained title" || body != "compact body" || rawJSON != "" {
		t.Fatalf("rebuilt view/body/raw = %q/%q/%q", viewTitle, body, rawJSON)
	}
	if !indexExistsForPortableTest(t, db, "unique_threads_github") || indexExistsForPortableTest(t, db, "ordinary_threads_title") {
		t.Fatal("rebuild did not preserve unique/omit ordinary indexes")
	}
	violations, err := portableForeignKeyViolationCount(ctx, db)
	if err != nil || violations != 0 {
		t.Fatalf("autoincrement rebuild FK violations=%d err=%v", violations, err)
	}
}

func TestPortableThreadsRebuildFailsBeforeUnknownUpdateTrigger(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "update-trigger.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	if _, err := st.DB().ExecContext(ctx, `
		alter table threads add column body_excerpt text;
		alter table threads add column body_length integer not null default 0;
		create table custom_update_audit(thread_id integer not null);
		create trigger custom_threads_body_update after update of body on threads
		begin
		  insert into custom_update_audit(thread_id) values(new.id);
		end;
	`); err != nil {
		t.Fatalf("create custom update trigger: %v", err)
	}
	if _, err := st.rebuildPortableCompatibilityThreads(ctx); err == nil || !strings.Contains(err.Error(), "cannot preserve update-trigger semantics") {
		t.Fatalf("custom update trigger error = %v", err)
	}
	if !tableExistsForPortableTest(t, st.DB(), "threads") {
		t.Fatal("failed trigger audit mutated threads table")
	}
}

func TestPortableThreadsRebuildRejectsReplacedConvergenceUpdateTrigger(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "replaced-trigger.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	if _, err := st.DB().ExecContext(ctx, `
		alter table threads add column body_excerpt text;
		alter table threads add column body_length integer not null default 0;
		drop trigger observation_convergence_threads_update;
		create table replaced_update_audit(thread_id integer not null);
		create trigger observation_convergence_threads_update after update of body on threads
		begin
		  insert into replaced_update_audit(thread_id) values(new.id);
		end;
	`); err != nil {
		t.Fatalf("replace convergence update trigger: %v", err)
	}
	if _, err := st.rebuildPortableCompatibilityThreads(ctx); err == nil || !strings.Contains(err.Error(), "cannot preserve update-trigger semantics") {
		t.Fatalf("replaced convergence trigger error = %v", err)
	}
}

func TestPortableThreadsRebuildAbortsTransformedConstraintConflict(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "conflict.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if _, err := db.ExecContext(ctx, `
		pragma foreign_keys = on;
		create table repositories(id integer primary key);
		create table threads(
			id integer primary key,
			repo_id integer not null references repositories(id),
			body text,
			raw_json text not null unique on conflict replace,
			body_excerpt text,
			body_length integer not null default 0
		);
		insert into repositories(id) values(1);
		insert into threads values(1, 1, 'body one', 'raw one', 'compact one', 8);
		insert into threads values(2, 1, 'body two', 'raw two', 'compact two', 8);
	`); err != nil {
		t.Fatal(err)
	}
	st := &Store{db: db, path: "conflict.db"}
	if _, err := st.rebuildPortableCompatibilityThreads(ctx); err == nil || !strings.Contains(err.Error(), "copy compact portable threads") {
		t.Fatalf("transformed uniqueness conflict error = %v", err)
	}
	var count int
	if err := db.QueryRowContext(ctx, `select count(*) from threads`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("constraint conflict retained %d threads, want 2", count)
	}
}

func TestPortableThreadsRebuildRollsBackTransformedForeignKeyViolation(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "foreign-key.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if _, err := db.ExecContext(ctx, `
		pragma foreign_keys = on;
		create table repositories(id integer primary key);
		create table threads(
			id integer primary key,
			repo_id integer not null references repositories(id),
			body text unique,
			raw_json text not null,
			body_excerpt text,
			body_length integer not null default 0
		);
		create table body_children(body text references threads(body));
		insert into repositories(id) values(1);
		insert into threads values(1, 1, 'full body', '{}', 'compact body', 9);
		insert into body_children(body) values('full body');
	`); err != nil {
		t.Fatal(err)
	}
	st := &Store{db: db, path: "foreign-key.db"}
	if _, err := st.rebuildPortableCompatibilityThreads(ctx); err == nil || !strings.Contains(err.Error(), "referencing mutable threads column body") {
		t.Fatalf("transformed foreign-key error = %v", err)
	}
	var body, childBody string
	if err := db.QueryRowContext(ctx, `select body from threads where id = 1`).Scan(&body); err != nil {
		t.Fatalf("read rolled-back thread: %v", err)
	}
	if err := db.QueryRowContext(ctx, `select body from body_children`).Scan(&childBody); err != nil {
		t.Fatalf("read retained child: %v", err)
	}
	if body != "full body" || childBody != "full body" {
		t.Fatalf("failed rebuild persisted body=%q child=%q", body, childBody)
	}
	violations, err := portableForeignKeyViolationCount(ctx, db)
	if err != nil || violations != 0 {
		t.Fatalf("rolled-back rebuild FK violations=%d err=%v", violations, err)
	}
}

func TestPortableThreadsRebuildRejectsForeignKeyFromTransformedColumn(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "outgoing-foreign-key.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if _, err := db.ExecContext(ctx, `
		pragma foreign_keys = on;
		create table repositories(id integer primary key);
		create table retained_bodies(body text primary key);
		create table threads(
			id integer primary key,
			repo_id integer not null references repositories(id),
			body text references retained_bodies(body),
			raw_json text not null,
			body_excerpt text,
			body_length integer not null default 0
		);
		insert into repositories(id) values(1);
		insert into retained_bodies(body) values('full body');
		insert into threads values(1, 1, 'full body', '{}', 'compact body', 9);
	`); err != nil {
		t.Fatal(err)
	}
	st := &Store{db: db, path: "outgoing-foreign-key.db"}
	if _, err := st.rebuildPortableCompatibilityThreads(ctx); err == nil || !strings.Contains(err.Error(), "foreign key from transformed threads column body") {
		t.Fatalf("outgoing transformed foreign-key error = %v", err)
	}
	var body string
	if err := db.QueryRowContext(ctx, `select body from threads where id = 1`).Scan(&body); err != nil {
		t.Fatal(err)
	}
	if body != "full body" {
		t.Fatalf("rejected rebuild changed body to %q", body)
	}
}

func TestPortableThreadsRebuildRejectsForeignKeyViolationBeforeSchemaSwap(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "preflight-violation.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	if _, err := st.DB().ExecContext(ctx, `
		alter table threads add column body_excerpt text;
		alter table threads add column body_length integer not null default 0;
		insert into repositories(id, owner, name, full_name, raw_json, updated_at)
		values(1, 'openclaw', 'gitcrawl', 'openclaw/gitcrawl', '{}', '2026-08-09T00:00:00Z');
		insert into threads(id, repo_id, github_id, number, kind, state, title, body, html_url, labels_json, assignees_json, raw_json, content_hash, updated_at)
		values(1, 1, 'THREAD-1', 1, 'issue', 'open', 'title', 'body', 'https://example.test/1', '[]', '[]', '{}', 'hash', '2026-08-09T00:00:00Z');
		pragma foreign_keys = off;
		insert into pull_request_checks(id, thread_id, name, status, raw_json, fetched_at)
		values(99, 999, 'orphan', 'completed', '{}', '2026-08-09T00:00:00Z');
		pragma foreign_keys = on;
	`); err != nil {
		t.Fatalf("seed FK violation: %v", err)
	}
	var stages []PortablePruneStage
	stats := &PortablePruneStats{}
	_, err = st.rebuildPortableCompatibilityThreadsWithOptions(ctx, PortablePruneOptions{
		Progress: func(stage PortablePruneStage) { stages = append(stages, stage) },
	}, stats)
	if err == nil || !strings.Contains(err.Error(), "found 1 foreign-key violations before rebuild") {
		t.Fatalf("pre-rebuild foreign-key error = %v", err)
	}
	if !stats.ForeignKeyValidated || stats.ForeignKeyViolations != 1 {
		t.Fatalf("foreign-key stats = %+v", stats)
	}
	if slices.Contains(stages, PortablePruneStageThreadsRebuildSchemaSwap) {
		t.Fatalf("invalid database reached schema swap: %v", stages)
	}
	if !tableExistsForPortableTest(t, st.DB(), "threads") {
		t.Fatal("preflight violation removed original threads table")
	}
}

func TestPortableThreadsRebuildIdentityMismatchRollsBack(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "identity-mismatch.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	if _, err := st.DB().ExecContext(ctx, `
		alter table threads add column body_excerpt text;
		alter table threads add column body_length integer not null default 0;
		insert into repositories(id, owner, name, full_name, raw_json, updated_at)
		values(1, 'openclaw', 'gitcrawl', 'openclaw/gitcrawl', '{}', '2026-08-09T00:00:00Z');
		insert into threads(id, repo_id, github_id, number, kind, state, title, body, html_url, labels_json, assignees_json, raw_json, content_hash, updated_at)
		values(1, 1, 'THREAD-1', 1, 'issue', 'open', 'title', 'full body', 'https://example.test/1', '[]', '[]', '{}', 'hash', '2026-08-09T00:00:00Z');
		update threads set body_excerpt = 'compact', body_length = 9 where id = 1;
	`); err != nil {
		t.Fatalf("seed identity mismatch: %v", err)
	}
	stats := &PortablePruneStats{}
	_, err = st.rebuildPortableCompatibilityThreadsWithOptions(ctx, PortablePruneOptions{
		threadsRebuildHook: func(stage portableThreadsRebuildHookStage, tx *sql.Tx, sibling string) error {
			if stage != portableThreadsRebuildHookAfterCopy {
				return nil
			}
			_, err := tx.ExecContext(ctx, `update `+sqliteIdentifier(sibling)+` set repo_id = 2 where id = 1`)
			return err
		},
	}, stats)
	if err == nil || !strings.Contains(err.Error(), "identity mismatch") {
		t.Fatalf("identity mismatch error = %v", err)
	}
	var repoID int64
	var body string
	if err := st.DB().QueryRowContext(ctx, `select repo_id, body from threads where id = 1`).Scan(&repoID, &body); err != nil {
		t.Fatalf("read original threads after rollback: %v", err)
	}
	if repoID != 1 || body != "full body" {
		t.Fatalf("identity mismatch persisted repo/body = %d/%q", repoID, body)
	}
}

func TestPortablePruneExportModeRunsOneForeignKeyProof(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "one-fk-proof.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	checks := 0
	stats, err := st.PrunePortablePayloads(ctx, PortablePruneOptions{
		BodyChars:                     8,
		DeferSecureRewrite:            true,
		RetainSanitizedPayloadColumns: true,
		foreignKeyCheck: func(ctx context.Context, q dbQueries) (int, error) {
			checks++
			var discardedTable, ordinaryIndex int
			if err := q.QueryRowContext(ctx, `select exists(select 1 from sqlite_schema where type = 'table' and name = 'documents')`).Scan(&discardedTable); err != nil {
				return 0, err
			}
			if err := q.QueryRowContext(ctx, `select exists(select 1 from sqlite_schema where type = 'index' and name = 'idx_threads_repo_number')`).Scan(&ordinaryIndex); err != nil {
				return 0, err
			}
			if discardedTable != 0 || ordinaryIndex != 1 {
				return 0, fmt.Errorf("foreign-key proof placement documents=%d ordinary_index=%d", discardedTable, ordinaryIndex)
			}
			return portableForeignKeyViolationCount(ctx, q)
		},
	})
	if err != nil {
		t.Fatalf("prune export mode: %v", err)
	}
	if checks != 1 || !stats.ForeignKeyValidated || stats.ForeignKeyViolations != 0 {
		t.Fatalf("foreign-key proofs=%d stats=%+v", checks, stats)
	}
}

func TestPortableTriggerUpdateDetectionUsesSQLTokens(t *testing.T) {
	updateSQL := `create trigger "BEGIN" after update /* BEGIN ON threads */ of body
		on threads when new.body != 'BEGIN'
		begin
		  select 1;
		end`
	updates, err := portableTriggerUpdatesThreads(updateSQL)
	if err != nil || !updates {
		t.Fatalf("quoted/commented update trigger detected=%v err=%v", updates, err)
	}
	insertSQL := `create trigger custom_insert after insert on threads
		begin
		  update custom_table set value = 'ON threads BEGIN';
		end`
	updates, err = portableTriggerUpdatesThreads(insertSQL)
	if err != nil || updates {
		t.Fatalf("insert trigger detected as update=%v err=%v", updates, err)
	}
	if _, err := portableTriggerUpdatesThreads(`create trigger broken after update begin select 1; end`); err == nil {
		t.Fatal("trigger without recognized ON threads target was accepted")
	}
}

func indexExistsForPortableTest(t *testing.T, db *sql.DB, name string) bool {
	t.Helper()
	var exists int
	if err := db.QueryRow(`select exists(select 1 from sqlite_schema where type = 'index' and name = ?)`, name).Scan(&exists); err != nil {
		t.Fatalf("inspect index %s: %v", name, err)
	}
	return exists == 1
}

func tableExistsForPortableTest(t *testing.T, db *sql.DB, name string) bool {
	t.Helper()
	var exists int
	if err := db.QueryRow(`select exists(select 1 from sqlite_schema where type = 'table' and name = ?)`, name).Scan(&exists); err != nil {
		t.Fatalf("inspect table %s: %v", name, err)
	}
	return exists == 1
}

func foreignKeyReferencesPortableTable(t *testing.T, db *sql.DB, table, target string) bool {
	t.Helper()
	rows, err := db.Query(`pragma foreign_key_list(` + sqliteIdentifier(table) + `)`)
	if err != nil {
		t.Fatalf("inspect foreign keys for %s: %v", table, err)
	}
	defer rows.Close()
	found := false
	for rows.Next() {
		var id, sequence int
		var referencedTable, from, onUpdate, onDelete, match string
		var to sql.NullString
		if err := rows.Scan(&id, &sequence, &referencedTable, &from, &to, &onUpdate, &onDelete, &match); err != nil {
			t.Fatalf("scan foreign keys for %s: %v", table, err)
		}
		if referencedTable == target {
			found = true
		}
		if strings.HasPrefix(referencedTable, "threads_portable_") {
			t.Fatalf("child %s references temporary sibling %s", table, referencedTable)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read foreign keys for %s: %v", table, err)
	}
	return found
}
