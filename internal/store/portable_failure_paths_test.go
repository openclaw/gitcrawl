package store

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestPortableSQLTokenizerRejectsMalformedSchema(t *testing.T) {
	for _, test := range []struct {
		name      string
		value     string
		wantError string
	}{
		{name: "comment", value: "select /* missing", wantError: "unterminated SQL comment"},
		{name: "single quote", value: "select 'missing", wantError: "unterminated SQL quote"},
		{name: "double quote", value: `select "missing`, wantError: "unterminated SQL quote"},
		{name: "backtick", value: "select `missing", wantError: "unterminated SQL quote"},
		{name: "bracket", value: "select [missing", wantError: "unterminated SQL bracket identifier"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := tokenizePortableSQL(test.value); err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("tokenize error = %v, want %q", err, test.wantError)
			}
		})
	}
}

func TestPortableSQLTokenizerPreservesQuotedIdentifiersAndLiterals(t *testing.T) {
	tokens, err := tokenizePortableSQL("-- ignored\n SELECT 'it''s', \"table\"\"name\", `tick``name`, [bracket-name], schema.table $value /* ignored */;")
	if err != nil {
		t.Fatal(err)
	}
	var values []string
	var literals int
	for _, token := range tokens {
		values = append(values, token.value)
		if token.literal {
			literals++
		}
	}
	joined := strings.Join(values, "|")
	for _, want := range []string{"it's", `table"name`, "tick`name", "bracket-name", "schema", "table", "$value"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("tokenized values %q do not contain %q", joined, want)
		}
	}
	if literals != 1 {
		t.Fatalf("literal token count = %d, want 1", literals)
	}
}

func TestPortableSQLTableIdentifierShapes(t *testing.T) {
	tests := []struct {
		name     string
		tokens   []portableSQLToken
		wantName string
		wantUsed int
	}{
		{name: "empty"},
		{name: "subquery", tokens: []portableSQLToken{{value: "("}}},
		{name: "simple", tokens: []portableSQLToken{{value: "documents"}}, wantName: "documents", wantUsed: 1},
		{name: "qualified", tokens: []portableSQLToken{{value: "main"}, {value: "."}, {value: "documents"}}, wantName: "documents", wantUsed: 3},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			name, used := portableSQLTableIdentifier(test.tokens)
			if name != test.wantName || used != test.wantUsed {
				t.Fatalf("identifier = %q/%d, want %q/%d", name, used, test.wantName, test.wantUsed)
			}
		})
	}
}

func TestPortableTriggerDependencyRejectsMalformedDefinitions(t *testing.T) {
	dropped := map[string]bool{"documents": true}
	for _, test := range []struct {
		name      string
		sql       string
		wantError string
	}{
		{name: "tokenization", sql: "create trigger broken /*", wantError: "unterminated SQL comment"},
		{name: "missing body", sql: "create trigger broken after update on repositories", wantError: "unrecognized trigger body"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := portableTriggerDroppedDependency(test.sql, dropped); err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("trigger dependency error = %v, want %q", err, test.wantError)
			}
		})
	}

	updates, err := portableTriggerUpdatesThreads("create trigger broken /*")
	if err == nil || updates {
		t.Fatalf("malformed update trigger result = %v, %v", updates, err)
	}
}

func TestPortableDependencyScannerHandlesDMLAndNestedSources(t *testing.T) {
	dropped := map[string]bool{"documents": true}
	statements := []string{
		`delete from documents where id = 1`,
		`update or abort main.documents set title = 'x'`,
		`insert or replace into documents(id) values(1)`,
		`select * from (select * from documents) nested`,
		`select * from repositories where exists(select 1 from documents)`,
		`select * from repositories union select * from documents`,
	}
	for _, statement := range statements {
		tokens, err := tokenizePortableSQL(statement)
		if err != nil {
			t.Fatal(err)
		}
		if dependency := portableDroppedTableDependency(tokens, dropped); !strings.EqualFold(dependency, "documents") {
			t.Fatalf("dependency for %q = %q, want documents", statement, dependency)
		}
	}

	tokens, err := tokenizePortableSQL(`select * from (select 1`)
	if err != nil {
		t.Fatal(err)
	}
	if end := portableSQLMatchingParenthesis(tokens, 3); end != len(tokens)-1 {
		t.Fatalf("unmatched parenthesis ended at %d, want %d", end, len(tokens)-1)
	}
}

func TestPortableForeignKeyTransformSafetyContracts(t *testing.T) {
	verbatim := map[string]bool{"id": true, "repo_id": true, "parent_id": true}
	valid := []portableForeignKeyDefinition{
		{fromColumn: "repo_id", referencedTable: "repositories", toColumn: "id"},
		{fromColumn: "parent_id", referencedTable: "threads", toColumn: ""},
		{fromColumn: "repo_id", referencedTable: "THREADS", toColumn: "repo_id"},
	}
	if err := validatePortableThreadsForeignKeyTransformSafety(valid, verbatim); err != nil {
		t.Fatalf("safe foreign keys rejected: %v", err)
	}

	tests := []struct {
		name       string
		definition portableForeignKeyDefinition
		wantError  string
	}{
		{
			name:       "transformed source",
			definition: portableForeignKeyDefinition{fromColumn: "body", referencedTable: "repositories", toColumn: "id"},
			wantError:  "foreign key from transformed threads column body",
		},
		{
			name:       "transformed self target",
			definition: portableForeignKeyDefinition{fromColumn: "parent_id", referencedTable: "threads", toColumn: "body"},
			wantError:  "self-referencing foreign key targeting transformed threads column body",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validatePortableThreadsForeignKeyTransformSafety([]portableForeignKeyDefinition{test.definition}, verbatim)
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("foreign key safety error = %v, want %q", err, test.wantError)
			}
		})
	}
}

func TestPortablePragmaSettersFailClosed(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "pragma.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if err := setPortableForeignKeys(ctx, conn, 2); err == nil || !strings.Contains(err.Error(), "invalid portable foreign_keys") {
		t.Fatalf("invalid foreign_keys error = %v", err)
	}
	if err := setPortableJournalMode(ctx, conn, "unknown"); err == nil || !strings.Contains(err.Error(), "invalid portable journal_mode") {
		t.Fatalf("invalid journal mode error = %v", err)
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := setPortableForeignKeys(canceled, conn, 1); err == nil {
		t.Fatal("canceled foreign_keys change succeeded")
	}
	if err := setPortableJournalMode(canceled, conn, "memory"); err == nil {
		t.Fatal("canceled journal mode change succeeded")
	}

	if _, err := conn.ExecContext(ctx, `pragma foreign_keys = on; begin`); err != nil {
		t.Fatal(err)
	}
	defer conn.ExecContext(context.Background(), `rollback`)
	if err := setPortableForeignKeys(ctx, conn, 0); err == nil || !strings.Contains(err.Error(), "pragma remained") {
		t.Fatalf("ignored in-transaction foreign_keys change error = %v", err)
	}
}

func TestPortableStoreHelpersPropagateCancellation(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "canceled.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	canceled, cancel := context.WithCancel(ctx)
	cancel()

	checks := []struct {
		name string
		run  func() error
	}{
		{name: "repository scope", run: func() error { _, err := st.RestrictPortableRepository(canceled, "openclaw/gitcrawl"); return err }},
		{name: "table names", run: func() error { _, err := portableTableNames(canceled, st.DB()); return err }},
		{name: "column set", run: func() error { _, err := portableTableColumnSet(canceled, st.DB(), "threads"); return err }},
		{name: "foreign key count", run: func() error { _, err := portableForeignKeyViolationCount(canceled, st.DB()); return err }},
		{name: "schema table name", run: func() error { _, _, err := portableSchemaTableName(canceled, st.DB(), "threads"); return err }},
		{name: "retained dependencies", run: func() error {
			return validatePortableRetainedSchemaDependencies(canceled, st.DB(), []string{"documents"})
		}},
		{name: "bulk drop", run: func() error { return st.dropCanonicalPortableTablesBulk(canceled, false, &PortablePruneStats{}, nil) }},
		{name: "threads rebuild", run: func() error { _, err := st.rebuildPortableCompatibilityThreads(canceled); return err }},
		{name: "threads sequence", run: func() error { _, err := portableThreadsSequence(canceled, st.DB()); return err }},
		{name: "threads identity", run: func() error { _, err := portableThreadsIdentityForTable(canceled, st.DB(), "threads"); return err }},
		{name: "foreign keys", run: func() error { _, err := portableForeignKeysForTable(canceled, st.DB(), "threads"); return err }},
		{name: "child foreign keys", run: func() error { _, err := portableChildForeignKeysReferencingThreads(canceled, st.DB()); return err }},
		{name: "rebuild columns", run: func() error { _, _, _, err := portableThreadsRebuildColumns(canceled, st.DB()); return err }},
		{name: "rebuild indexes", run: func() error { _, _, err := portableThreadsRebuildIndexes(canceled, st.DB()); return err }},
		{name: "rebuild triggers", run: func() error { _, err := portableThreadsRebuildTriggers(canceled, st.DB()); return err }},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			if err := check.run(); err == nil {
				t.Fatal("canceled store operation succeeded")
			}
		})
	}
}

func TestRestrictPortableRepositoryFutureSchemaColumns(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "future-schema.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if _, err := st.DB().ExecContext(ctx, `
		insert into repositories(id, owner, name, full_name, raw_json, updated_at)
		values(1, 'openclaw', 'keep', 'openclaw/keep', '{}', '2026-08-09T00:00:00Z'),
		      (2, 'openclaw', 'drop', 'openclaw/drop', '{}', '2026-08-09T00:00:00Z');
		insert into threads(id, repo_id, github_id, number, kind, state, title, html_url, labels_json, assignees_json, raw_json, content_hash, updated_at)
		values(1, 1, 'keep', 1, 'issue', 'open', 'keep', 'https://example.test/1', '[]', '[]', '{}', 'keep', '2026-08-09T00:00:00Z'),
		      (2, 2, 'drop', 2, 'issue', 'open', 'drop', 'https://example.test/2', '[]', '[]', '{}', 'drop', '2026-08-09T00:00:00Z');
		create table future_repo_data(id integer primary key, repo_id integer, payload text);
		create table future_thread_data(id integer primary key, thread_id integer, payload text);
		insert into future_repo_data values(1, 1, 'keep'), (2, 2, 'drop');
		insert into future_thread_data values(1, 1, 'keep'), (2, 2, 'drop');
	`); err != nil {
		t.Fatal(err)
	}
	result, err := st.RestrictPortableRepositoryWithOptions(ctx, "openclaw/keep", PortableRepositoryScopeOptions{DeferForeignKeyValidation: true})
	if err != nil {
		t.Fatal(err)
	}
	if result.RepositoriesRemoved != 1 {
		t.Fatalf("repositories removed = %d, want 1", result.RepositoriesRemoved)
	}
	for _, table := range []string{"future_repo_data", "future_thread_data"} {
		var count int
		var payload string
		if err := st.DB().QueryRowContext(ctx, `select count(*), payload from `+sqliteIdentifier(table)).Scan(&count, &payload); err != nil {
			t.Fatal(err)
		}
		if count != 1 || payload != "keep" {
			t.Fatalf("%s retained count/payload = %d/%q", table, count, payload)
		}
	}
}

func TestPortableRuntimeIdentifierRejectsNUL(t *testing.T) {
	if _, err := portableRuntimeIdentifier("bad\x00name"); err == nil {
		t.Fatal("runtime identifier with NUL succeeded")
	}
	if got, err := portableRuntimeIdentifier(`report"name`); err != nil || got != `"report""name"` {
		t.Fatalf("quoted runtime identifier = %q, %v", got, err)
	}
}

func TestValidatePortableRetainedSchemaDependenciesNoDrops(t *testing.T) {
	if err := validatePortableRetainedSchemaDependencies(context.Background(), nil, nil); err != nil {
		t.Fatalf("empty dropped set queried database: %v", err)
	}
}

func TestPortableChildForeignKeyTargetDefaultsToID(t *testing.T) {
	if err := validatePortableChildForeignKeyIdentityTargets([]portableForeignKeyDefinition{{ownerTable: "child", fromColumn: "thread_id"}}); err != nil {
		t.Fatalf("implicit primary-key target rejected: %v", err)
	}
	err := validatePortableChildForeignKeyIdentityTargets([]portableForeignKeyDefinition{{ownerTable: "child", fromColumn: "body", toColumn: "body"}})
	if err == nil || !strings.Contains(err.Error(), "referencing mutable threads column body") {
		t.Fatalf("mutable child target error = %v", err)
	}
}

func TestPortableCanceledErrorRemainsDiscoverable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "closed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = portableTableNames(ctx, db)
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("portable cancellation error = %v", err)
	}
}

func TestPortablePayloadCompactionEdgeCases(t *testing.T) {
	if got := compactPortableReviewComments("not-json", 8); got != "[]" {
		t.Fatalf("malformed review comments compacted to %q", got)
	}
	got := compactPortableReviewComments(`[{"body":"abcdefghijk","nested":{"bodyText":"123456789","body":7},"other":"preserved"}]`, 5)
	if got != `[{"body":"abcde","nested":{"body":7,"bodyText":"12345"},"other":"preserved"}]` {
		t.Fatalf("nested review comments compacted to %q", got)
	}
	if got := truncatePortableText("abcdef", 0); got != "" {
		t.Fatalf("zero-limit text = %q", got)
	}
	if got := truncatePortableText("short", 8); got != "short" {
		t.Fatalf("short text = %q", got)
	}
	if got := truncatePortableText("åßçdé", 3); got != "åßç" {
		t.Fatalf("rune-safe text = %q", got)
	}

	nameTests := []struct {
		name  string
		raw   string
		field string
		want  string
	}{
		{name: "malformed", raw: "{", field: "name", want: "{"},
		{name: "strings", raw: `[" bug ","feature"]`, field: "name", want: `["bug","feature"]`},
		{name: "objects", raw: `[{"name":" bug "},{"name":"feature"}]`, field: "name", want: `["bug","feature"]`},
		{name: "wrong type", raw: `[7]`, field: "name", want: `[7]`},
		{name: "missing field", raw: `[{"color":"red"}]`, field: "name", want: `[{"color":"red"}]`},
	}
	for _, test := range nameTests {
		t.Run(test.name, func(t *testing.T) {
			if got := compactPortableNameList(test.raw, test.field); got != test.want {
				t.Fatalf("compact name list = %q, want %q", got, test.want)
			}
		})
	}
}

func TestPortablePayloadHelpersHandleMissingAndExistingColumns(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "payload-helpers.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, `create table threads(id integer primary key); create table custom_payload(id integer primary key)`); err != nil {
		t.Fatal(err)
	}
	st := &Store{db: db, path: "payload-helpers.db"}
	labels, assignees, err := st.compactPortableThreadMetadata(ctx)
	if err != nil || labels != 0 || assignees != 0 {
		t.Fatalf("missing metadata columns result = %d/%d, %v", labels, assignees, err)
	}
	if err := st.ensurePortableExcerptColumns(ctx, "custom_payload"); err != nil {
		t.Fatal(err)
	}
	if !st.hasColumn(ctx, "custom_payload", "body_excerpt") || !st.hasColumn(ctx, "custom_payload", "body_length") {
		t.Fatal("excerpt helper did not add compatibility columns")
	}
	if err := st.ensurePortableExcerptColumns(ctx, "custom_payload"); err != nil {
		t.Fatalf("idempotent excerpt helper: %v", err)
	}
}

func TestPortablePayloadHelpersPropagateCancellation(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "payload-canceled.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	checks := []struct {
		name string
		run  func() error
	}{
		{name: "vacuum", run: func() error { return st.vacuumPortableDatabase(canceled) }},
		{name: "metadata", run: func() error { return st.ensurePortableMetadata(canceled) }},
		{name: "canonical schema", run: func() error {
			return st.canonicalizePortableSchema(canceled, PortablePruneOptions{}, &PortablePruneStats{})
		}},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			if err := check.run(); err == nil {
				t.Fatal("canceled payload operation succeeded")
			}
		})
	}
}

func TestRestrictPortableRepositoryRollsBackFutureSchemaDeleteFailure(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "scope-rollback.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if _, err := st.DB().ExecContext(ctx, `
		insert into repositories(id, owner, name, full_name, raw_json, updated_at)
		values(1, 'openclaw', 'keep', 'openclaw/keep', '{}', '2026-08-09T00:00:00Z'),
		      (2, 'openclaw', 'drop', 'openclaw/drop', '{}', '2026-08-09T00:00:00Z');
		insert into threads(id, repo_id, github_id, number, kind, state, title, html_url, labels_json, assignees_json, raw_json, content_hash, updated_at)
		values(1, 1, 'keep', 1, 'issue', 'open', 'keep', 'https://example.test/1', '[]', '[]', '{}', 'keep', '2026-08-09T00:00:00Z'),
		      (2, 2, 'drop', 2, 'issue', 'open', 'drop', 'https://example.test/2', '[]', '[]', '{}', 'drop', '2026-08-09T00:00:00Z');
		create table future_guarded(id integer primary key, repo_id integer, payload text);
		insert into future_guarded values(1, 1, 'keep'), (2, 2, 'drop');
		create trigger reject_future_delete before delete on future_guarded
		when old.repo_id = 2 begin select raise(abort, 'guarded future row'); end;
	`); err != nil {
		t.Fatal(err)
	}
	_, err = st.RestrictPortableRepository(ctx, "openclaw/keep")
	if err == nil || !strings.Contains(err.Error(), "delete out-of-scope portable rows from future_guarded") {
		t.Fatalf("future schema delete error = %v", err)
	}
	var repositories, futureRows int
	if err := st.DB().QueryRowContext(ctx, `select count(*) from repositories`).Scan(&repositories); err != nil {
		t.Fatal(err)
	}
	if err := st.DB().QueryRowContext(ctx, `select count(*) from future_guarded`).Scan(&futureRows); err != nil {
		t.Fatal(err)
	}
	if repositories != 2 || futureRows != 2 {
		t.Fatalf("failed scope persisted repositories/future rows = %d/%d", repositories, futureRows)
	}
}

func TestRestrictPortableRepositoryReportsRetainedForeignKeyViolation(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "scope-fk.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if _, err := st.DB().ExecContext(ctx, `
		insert into repositories(id, owner, name, full_name, raw_json, updated_at)
		values(1, 'openclaw', 'keep', 'openclaw/keep', '{}', '2026-08-09T00:00:00Z');
		pragma foreign_keys = off;
		insert into pull_request_checks(id, thread_id, name, status, raw_json, fetched_at)
		values(99, 999, 'orphan', 'completed', '{}', '2026-08-09T00:00:00Z');
		pragma foreign_keys = on;
	`); err != nil {
		t.Fatal(err)
	}
	_, err = st.RestrictPortableRepository(ctx, "openclaw/keep")
	if err == nil || !strings.Contains(err.Error(), "left 1 foreign-key violations") {
		t.Fatalf("retained foreign-key violation error = %v", err)
	}
}

func TestRestrictPortableRepositoryRejectsIncompleteFutureSchemas(t *testing.T) {
	tests := []struct {
		name      string
		schema    string
		wantError string
	}{
		{
			name:      "missing threads",
			schema:    ``,
			wantError: "capture out-of-scope portable threads",
		},
		{
			name:      "missing revisions",
			schema:    `create table threads(id integer primary key, repo_id integer not null);`,
			wantError: "capture out-of-scope portable revisions",
		},
		{
			name: "missing clusters",
			schema: `
				create table threads(id integer primary key, repo_id integer not null);
				create table thread_revisions(id integer primary key, thread_id integer not null);
			`,
			wantError: "capture out-of-scope portable clusters",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "incomplete.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if _, err := db.ExecContext(ctx, `
				create table repositories(id integer primary key, owner text, name text, full_name text);
				insert into repositories values(1, 'openclaw', 'keep', 'openclaw/keep');
			`+test.schema); err != nil {
				t.Fatal(err)
			}
			st := &Store{db: db, path: "incomplete.db"}
			_, err = st.RestrictPortableRepository(ctx, "openclaw/keep")
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("incomplete schema error = %v, want %q", err, test.wantError)
			}
			var repositories int
			if err := db.QueryRowContext(ctx, `select count(*) from repositories`).Scan(&repositories); err != nil {
				t.Fatal(err)
			}
			if repositories != 1 {
				t.Fatalf("incomplete schema changed repository count to %d", repositories)
			}
		})
	}
}

func TestRestrictPortableRepositoryRejectsNullIdentityWithoutMutation(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "null-identity.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, `
		create table repositories(id integer primary key, owner text, name text, full_name text);
		insert into repositories values(1, null, 'keep', 'openclaw/keep');
	`); err != nil {
		t.Fatal(err)
	}
	st := &Store{db: db, path: "null-identity.db"}
	_, err = st.RestrictPortableRepository(ctx, "openclaw/keep")
	if err == nil || !strings.Contains(err.Error(), "read portable target repository") {
		t.Fatalf("null repository identity error = %v", err)
	}
	var count int
	if err := db.QueryRowContext(ctx, `select count(*) from repositories`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("null identity repository count = %d, %v", count, err)
	}
}

func TestPortablePayloadMutationFailuresPreserveSensitiveRows(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name      string
		schema    string
		run       func(*Store) error
		read      string
		wantValue any
		wantError string
	}{
		{
			name: "thread excerpt",
			schema: `
				create table threads(id integer primary key, body text, raw_json text, body_excerpt text, body_length integer not null default 0);
				insert into threads values(1, 'private body', '{}', null, 0);
				create trigger reject_excerpt before update of body_excerpt on threads begin select raise(abort, 'reject excerpt'); end;
			`,
			run: func(st *Store) error {
				_, err := st.PrunePortablePayloads(ctx, PortablePruneOptions{BodyChars: 4, DeferSecureRewrite: true})
				return err
			},
			read:      `select body from threads where id = 1`,
			wantValue: "private body",
			wantError: "prune thread body excerpts",
		},
		{
			name: "comment body",
			schema: `
				create table comments(id integer primary key, body text, body_excerpt text, body_length integer not null default 0);
				insert into comments values(1, 'private comment', null, 0);
				create trigger reject_comment before update of body on comments begin select raise(abort, 'reject comment'); end;
			`,
			run: func(st *Store) error {
				_, err := st.PrunePortablePayloads(ctx, PortablePruneOptions{BodyChars: 4, DeferSecureRewrite: true})
				return err
			},
			read:      `select body from comments where id = 1`,
			wantValue: "private comment",
			wantError: "prune comment bodies",
		},
		{
			name: "comment revision body",
			schema: `
				create table comment_revisions(id integer primary key, body text);
				insert into comment_revisions values(1, 'private revision');
				create trigger reject_revision before update of body on comment_revisions begin select raise(abort, 'reject revision'); end;
			`,
			run: func(st *Store) error {
				_, err := st.PrunePortablePayloads(ctx, PortablePruneOptions{BodyChars: 4, DeferSecureRewrite: true})
				return err
			},
			read:      `select body from comment_revisions where id = 1`,
			wantValue: "private revision",
			wantError: "prune comment revision bodies",
		},
		{
			name: "thread metadata",
			schema: `
				create table threads(id integer primary key, labels_json text, assignees_json text);
				insert into threads values(1, '[{"name":"bug"}]', '[{"login":"octocat"}]');
				create trigger reject_metadata before update of labels_json on threads begin select raise(abort, 'reject metadata'); end;
			`,
			run: func(st *Store) error {
				_, err := st.PrunePortablePayloads(ctx, PortablePruneOptions{BodyChars: 4, DeferSecureRewrite: true})
				return err
			},
			read:      `select labels_json from threads where id = 1`,
			wantValue: `[{"name":"bug"}]`,
			wantError: "compact portable thread metadata",
		},
		{
			name: "review bodies",
			schema: `
				create table pull_request_review_threads(first_comment_body text, comments_json text);
				insert into pull_request_review_threads values('private review', '[{"body":"private review"}]');
				create trigger reject_review before update on pull_request_review_threads begin select raise(abort, 'reject review'); end;
			`,
			run: func(st *Store) error {
				_, err := st.PrunePortablePayloads(ctx, PortablePruneOptions{BodyChars: 4, DeferSecureRewrite: true})
				return err
			},
			read:      `select first_comment_body from pull_request_review_threads`,
			wantValue: "private review",
			wantError: "compact portable review bodies",
		},
		{
			name: "raw json",
			schema: `
				create table comments(raw_json text);
				insert into comments values('{"private":true}');
				create trigger reject_raw before update of raw_json on comments begin select raise(abort, 'reject raw'); end;
			`,
			run: func(st *Store) error {
				_, err := st.PrunePortablePayloads(ctx, PortablePruneOptions{BodyChars: 4, DeferSecureRewrite: true})
				return err
			},
			read:      `select raw_json from comments`,
			wantValue: `{"private":true}`,
			wantError: "clear portable raw json comments.raw_json",
		},
		{
			name: "pull request patch",
			schema: `
				create table pull_request_files(patch text);
				insert into pull_request_files values('private patch');
				create trigger reject_patch before update of patch on pull_request_files begin select raise(abort, 'reject patch'); end;
			`,
			run: func(st *Store) error {
				_, err := st.PrunePortablePayloads(ctx, PortablePruneOptions{BodyChars: 4, DeferSecureRewrite: true})
				return err
			},
			read:      `select patch from pull_request_files`,
			wantValue: "private patch",
			wantError: "clear portable pull request file patches",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "payload.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if _, err := db.ExecContext(ctx, test.schema); err != nil {
				t.Fatal(err)
			}
			st := &Store{db: db, path: "payload.db"}
			err = test.run(st)
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("payload mutation error = %v, want %q", err, test.wantError)
			}
			var got any
			if err := db.QueryRowContext(ctx, test.read).Scan(&got); err != nil {
				t.Fatal(err)
			}
			if got != test.wantValue {
				t.Fatalf("failed payload mutation changed value to %#v, want %#v", got, test.wantValue)
			}
		})
	}
}

func TestPortableSchemaInspectionRejectsUnsafeDefinitions(t *testing.T) {
	ctx := context.Background()

	t.Run("sequence table without threads entry", func(t *testing.T) {
		db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "sequence.db"))
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		if _, err := db.ExecContext(ctx, `create table other(id integer primary key autoincrement)`); err != nil {
			t.Fatal(err)
		}
		sequence, err := portableThreadsSequence(ctx, db)
		if err != nil || sequence.Valid {
			t.Fatalf("missing threads sequence = %#v, %v", sequence, err)
		}
	})

	t.Run("missing required rebuild column", func(t *testing.T) {
		db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "columns.db"))
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		if _, err := db.ExecContext(ctx, `create table threads(id integer primary key, body text, body_excerpt text)`); err != nil {
			t.Fatal(err)
		}
		_, _, _, err = portableThreadsRebuildColumns(ctx, db)
		if err == nil || !strings.Contains(err.Error(), "requires column raw_json") {
			t.Fatalf("missing raw_json rebuild error = %v", err)
		}
	})

	t.Run("generated columns stay out of compact copy", func(t *testing.T) {
		db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "generated.db"))
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		if _, err := db.ExecContext(ctx, `create table threads(id integer primary key, body text, body_excerpt text, raw_json text, generated text generated always as (body || raw_json) virtual)`); err != nil {
			t.Fatal(err)
		}
		columns, expressions, _, err := portableThreadsRebuildColumns(ctx, db)
		if err != nil {
			t.Fatal(err)
		}
		if slices.Contains(columns, `"generated"`) || len(columns) != len(expressions) {
			t.Fatalf("generated rebuild columns/expressions = %v / %v", columns, expressions)
		}
	})

	t.Run("unrecognized unique index SQL", func(t *testing.T) {
		db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "index.db"))
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		if _, err := db.ExecContext(ctx, `
			create table threads(id integer primary key, value text);
			create unique index custom_unique on threads(value);
			pragma writable_schema = on;
			update sqlite_schema set sql = 'CREATE INDEX custom_unique on threads(value)' where name = 'custom_unique';
			pragma writable_schema = off;
		`); err != nil {
			t.Fatal(err)
		}
		_, _, err = portableThreadsRebuildIndexes(ctx, db)
		if err == nil || !strings.Contains(err.Error(), "has unrecognized SQL") {
			t.Fatalf("unsafe unique index error = %v", err)
		}
	})

	t.Run("unrecognized trigger SQL", func(t *testing.T) {
		db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "trigger.db"))
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		if _, err := db.ExecContext(ctx, `
			create table threads(id integer primary key);
			create trigger custom_trigger after insert on threads begin select 1; end;
			pragma writable_schema = on;
			update sqlite_schema set sql = 'SELECT 1' where name = 'custom_trigger';
			pragma writable_schema = off;
		`); err != nil {
			t.Fatal(err)
		}
		_, err = portableThreadsRebuildTriggers(ctx, db)
		if err == nil || !strings.Contains(err.Error(), "has unrecognized SQL") {
			t.Fatalf("unsafe trigger error = %v", err)
		}
	})
}

func TestSanitizePortableRepositoryCompatibilityColumnFailsClosed(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "repository-raw.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, `
		create table repositories(id integer primary key, raw_json text);
		insert into repositories values(1, '{"private":true}');
		create trigger reject_repository_raw before update of raw_json on repositories begin select raise(abort, 'reject raw'); end;
	`); err != nil {
		t.Fatal(err)
	}
	st := &Store{db: db, path: "repository-raw.db"}
	if err := st.sanitizePortableRepositoryCompatibilityColumn(ctx); err == nil || !strings.Contains(err.Error(), "sanitize portable repository compatibility column") {
		t.Fatalf("repository compatibility sanitization error = %v", err)
	}
	var rawJSON string
	if err := db.QueryRowContext(ctx, `select raw_json from repositories`).Scan(&rawJSON); err != nil {
		t.Fatal(err)
	}
	if rawJSON != `{"private":true}` {
		t.Fatalf("failed compatibility sanitization changed raw JSON to %q", rawJSON)
	}
}

func TestValidatePortableRetainedSchemaRejectsMalformedTriggerSQL(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "retained-trigger.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, `
		create table retained(id integer primary key);
		create table disposable(id integer primary key);
		create trigger retained_trigger after insert on retained begin select 1; end;
		drop table disposable;
		pragma writable_schema = on;
		update sqlite_schema set sql = 'create trigger retained_trigger /*' where name = 'retained_trigger';
		pragma writable_schema = off;
	`); err != nil {
		t.Fatal(err)
	}
	err = validatePortableRetainedSchemaDependencies(ctx, db, []string{"disposable"})
	if err == nil || !strings.Contains(err.Error(), "inspect retained portable trigger retained_trigger") {
		t.Fatalf("malformed retained trigger error = %v", err)
	}
}

func TestCanonicalizePortableSchemaUpgradesLegacyPayloadColumns(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "legacy-columns.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, `
		create table repositories(id integer primary key, raw_json text);
		create table threads(id integer primary key, repo_id integer, body text, raw_json text);
		insert into repositories values(1, '{"private":true}');
		insert into threads values(1, 1, 'abcdef', '{"private":true}');
	`); err != nil {
		t.Fatal(err)
	}
	st := &Store{db: db, path: "legacy-columns.db"}
	stats := &PortablePruneStats{}
	if err := st.canonicalizePortableSchema(ctx, PortablePruneOptions{BodyChars: 3}, stats); err != nil {
		t.Fatalf("canonicalize legacy columns: %v", err)
	}
	for _, column := range []struct{ table, name string }{
		{table: "repositories", name: "raw_json"},
		{table: "threads", name: "raw_json"},
		{table: "threads", name: "body"},
	} {
		if st.hasColumn(ctx, column.table, column.name) {
			t.Fatalf("legacy payload column remains: %s.%s", column.table, column.name)
		}
	}
	var excerpt string
	var bodyLength int
	if err := db.QueryRowContext(ctx, `select body_excerpt, body_length from threads`).Scan(&excerpt, &bodyLength); err != nil {
		t.Fatal(err)
	}
	if excerpt != "abc" || bodyLength != 0 {
		t.Fatalf("canonical legacy excerpt/length = %q/%d", excerpt, bodyLength)
	}
	if !slices.Contains(stats.DroppedColumns, "threads.body") || !slices.Contains(stats.DroppedColumns, "repositories.raw_json") {
		t.Fatalf("legacy dropped columns = %v", stats.DroppedColumns)
	}
}

func TestEnsurePortableExcerptColumnsRejectsViews(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "excerpt-view.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, `
		create table payload(body text);
		create view missing_excerpt as select body from payload;
		create view missing_length as select body as body_excerpt from payload;
	`); err != nil {
		t.Fatal(err)
	}
	st := &Store{db: db, path: "excerpt-view.db"}
	if err := st.ensurePortableExcerptColumns(ctx, "missing_excerpt"); err == nil || !strings.Contains(err.Error(), "add portable missing_excerpt.body_excerpt") {
		t.Fatalf("view excerpt alteration error = %v", err)
	}
	if err := st.ensurePortableExcerptColumns(ctx, "missing_length"); err == nil || !strings.Contains(err.Error(), "add portable missing_length.body_length") {
		t.Fatalf("view length alteration error = %v", err)
	}
}

func TestPortableThreadsIdentityRejectsNonIntegerIdentity(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "identity-types.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, `create table threads(id text, repo_id integer); insert into threads values('not-an-integer', 1)`); err != nil {
		t.Fatal(err)
	}
	if _, err := portableThreadsIdentityForTable(ctx, db, "threads"); err == nil || !strings.Contains(err.Error(), "scan portable threads identity") {
		t.Fatalf("non-integer identity error = %v", err)
	}
}
