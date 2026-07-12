package store

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
)

const canonicalThreadsCreateSQL = `create table threads (
  id integer primary key,
  repo_id integer not null references repositories(id) on delete cascade,
  github_id text not null,
  number integer not null,
  kind text not null check (kind in ('issue', 'pull_request')),
  state text not null,
  title text not null,
  body text,
  author_login text,
  author_type text,
  author_association text,
  html_url text not null,
  labels_json text not null,
  assignees_json text not null,
  raw_json text not null,
  content_hash text not null,
  is_draft integer not null default 0,
  created_at_gh text,
  updated_at_gh text,
  closed_at_gh text,
  merged_at_gh text,
  closed_at_local text,
  close_reason_local text,
  first_pulled_at text,
  last_pulled_at text,
  observation_sequence integer not null default 0,
  evidence_observation_sequence integer not null default 0,
  updated_at text not null,
  unique(repo_id, kind, number)
)`

const canonicalThreadRevisionsCreateSQL = `create table thread_revisions (
  id integer primary key,
  thread_id integer not null references threads(id) on delete cascade,
  source_updated_at text,
  content_hash text not null,
  title_hash text not null,
  body_hash text not null,
  labels_hash text not null,
  raw_json_blob_id integer references blobs(id) on delete set null,
  observation_sequence integer not null default 0,
  created_at text not null
)`

const canonicalThreadObservationSequenceCreateSQL = `create table thread_observation_sequence (
  id integer primary key check (id = 1),
  value integer not null,
  last_started_at text not null
)`

const canonicalThreadChildReservationsCreateSQL = `create table thread_child_observation_reservations (
  thread_id integer not null references threads(id) on delete cascade,
  family text not null check (family in (
    'comments',
    'pull_request_details',
    'pull_request_files',
    'pull_request_commits',
    'pull_request_checks',
    'pull_request_review_threads'
  )),
  observation_sequence integer not null check (observation_sequence > 0),
  primary key(thread_id, family)
)`

const canonicalWorkflowReservationsCreateSQL = `create table workflow_run_observation_reservations (
  repo_id integer not null references repositories(id) on delete cascade,
  head_sha text not null check (trim(head_sha) <> ''),
  observation_sequence integer not null check (observation_sequence > 0),
  primary key(repo_id, head_sha)
)`

const canonicalObservationConvergenceCreateSQL = `create table observation_schema_convergence (
  id integer primary key check (id = 1),
  checked_observation_sequence integer not null
)`

const (
	createThreadsRepoNumberIndexSQL   = `create index if not exists idx_threads_repo_number on threads(repo_id, number)`
	createThreadsRepoStateIndexSQL    = `create index if not exists idx_threads_repo_state_closed on threads(repo_id, state, closed_at_local)`
	createThreadsRepoUpdatedSQL       = `create index if not exists idx_threads_repo_updated on threads(repo_id, updated_at)`
	createRevisionCreatedIndexSQL     = `create index if not exists idx_thread_revisions_thread_created on thread_revisions(thread_id, created_at)`
	createRevisionObservationIndexSQL = `create index if not exists idx_thread_revisions_thread_observation
		on thread_revisions(thread_id, observation_sequence desc)
	`
)

type observationConvergenceTrigger struct {
	name  string
	table string
	event string
}

var observationConvergenceTriggers = []observationConvergenceTrigger{
	{name: "observation_convergence_threads_insert", table: "threads", event: "insert"},
	{name: "observation_convergence_threads_update", table: "threads", event: "update"},
	{name: "observation_convergence_threads_delete", table: "threads", event: "delete"},
	{name: "observation_convergence_revisions_insert", table: "thread_revisions", event: "insert"},
	{name: "observation_convergence_revisions_update", table: "thread_revisions", event: "update"},
	{name: "observation_convergence_revisions_delete", table: "thread_revisions", event: "delete"},
	{name: "observation_convergence_pr_details_insert", table: "pull_request_details", event: "insert"},
	{name: "observation_convergence_pr_details_update", table: "pull_request_details", event: "update"},
	{name: "observation_convergence_pr_details_delete", table: "pull_request_details", event: "delete"},
	{name: "observation_convergence_children_insert", table: "thread_child_observation_reservations", event: "insert"},
	{name: "observation_convergence_children_update", table: "thread_child_observation_reservations", event: "update"},
	{name: "observation_convergence_children_delete", table: "thread_child_observation_reservations", event: "delete"},
	{name: "observation_convergence_workflows_insert", table: "workflow_run_observation_reservations", event: "insert"},
	{name: "observation_convergence_workflows_update", table: "workflow_run_observation_reservations", event: "update"},
	{name: "observation_convergence_workflows_delete", table: "workflow_run_observation_reservations", event: "delete"},
	{name: "observation_convergence_allocator_insert", table: "thread_observation_sequence", event: "insert"},
	{name: "observation_convergence_allocator_update", table: "thread_observation_sequence", event: "update"},
	{name: "observation_convergence_allocator_delete", table: "thread_observation_sequence", event: "delete"},
}

var (
	threadChildFamilyCheckPattern = regexp.MustCompile(
		`(?is)\bfamily\s+text\s+not\s+null\s+check\s*\(\s*family\s+in\s*\(([^)]*)\)\s*\)`,
	)
	sqlStringLiteralPattern = regexp.MustCompile(`'([^']*)'`)
)

type schemaColumn struct {
	name       string
	columnType string
	notNull    int
	defaultSQL string
	primaryKey int
}

type schemaForeignKey struct {
	table    string
	from     string
	to       string
	onUpdate string
	onDelete string
	match    string
}

func (s *Store) threadsHaveCanonicalShape(ctx context.Context) bool {
	return s.tableHasCanonicalSQL(ctx, "threads", canonicalThreadsCreateSQL) &&
		s.indexHasCanonicalSQL(ctx, "idx_threads_repo_number", createThreadsRepoNumberIndexSQL) &&
		s.indexHasCanonicalSQL(ctx, "idx_threads_repo_state_closed", createThreadsRepoStateIndexSQL) &&
		s.indexHasCanonicalSQL(ctx, "idx_threads_repo_updated", createThreadsRepoUpdatedSQL)
}

func (s *Store) threadRevisionsHaveCanonicalShape(ctx context.Context) bool {
	return s.tableHasCanonicalSQL(ctx, "thread_revisions", canonicalThreadRevisionsCreateSQL) &&
		s.indexHasCanonicalSQL(ctx, "idx_thread_revisions_thread_created", createRevisionCreatedIndexSQL) &&
		s.indexHasCanonicalSQL(ctx, "idx_thread_revisions_thread_observation", createRevisionObservationIndexSQL)
}

func (s *Store) threadObservationSequenceHasCurrentShape(ctx context.Context) bool {
	return s.tableHasCanonicalSQL(
		ctx,
		"thread_observation_sequence",
		canonicalThreadObservationSequenceCreateSQL,
	)
}

func (s *Store) threadChildObservationReservationsHaveCurrentShape(ctx context.Context) bool {
	if !s.tableHasCanonicalSQL(
		ctx,
		"thread_child_observation_reservations",
		canonicalThreadChildReservationsCreateSQL,
	) {
		return false
	}
	if !s.tableColumnsMatch(ctx, "thread_child_observation_reservations", []schemaColumn{
		{name: "thread_id", columnType: "INTEGER", notNull: 1, primaryKey: 1},
		{name: "family", columnType: "TEXT", notNull: 1, primaryKey: 2},
		{name: "observation_sequence", columnType: "INTEGER", notNull: 1},
	}) {
		return false
	}
	if !s.tableForeignKeysMatch(ctx, "thread_child_observation_reservations", []schemaForeignKey{
		{
			table:    "threads",
			from:     "thread_id",
			to:       "id",
			onUpdate: "NO ACTION",
			onDelete: "CASCADE",
			match:    "NONE",
		},
	}) {
		return false
	}
	if !s.tableHasUniqueColumns(
		ctx,
		"thread_child_observation_reservations",
		[]string{"thread_id", "family"},
	) {
		return false
	}
	createSQL, ok := s.tableCreateSQL(ctx, "thread_child_observation_reservations")
	if !ok || !strings.Contains(compactSQL(createSQL), "check(observation_sequence>0)") {
		return false
	}
	match := threadChildFamilyCheckPattern.FindStringSubmatch(createSQL)
	if len(match) != 2 {
		return false
	}
	literals := sqlStringLiteralPattern.FindAllStringSubmatch(match[1], -1)
	if len(literals) != len(threadChildObservationFamilies) {
		return false
	}
	expected := make(map[string]struct{}, len(threadChildObservationFamilies))
	for _, family := range threadChildObservationFamilies {
		expected[string(family)] = struct{}{}
	}
	for _, literal := range literals {
		if _, ok := expected[literal[1]]; !ok {
			return false
		}
		delete(expected, literal[1])
	}
	return len(expected) == 0
}

func (s *Store) workflowRunObservationReservationsHaveCurrentShape(ctx context.Context) bool {
	if !s.tableHasCanonicalSQL(
		ctx,
		"workflow_run_observation_reservations",
		canonicalWorkflowReservationsCreateSQL,
	) {
		return false
	}
	if !s.tableColumnsMatch(ctx, "workflow_run_observation_reservations", []schemaColumn{
		{name: "repo_id", columnType: "INTEGER", notNull: 1, primaryKey: 1},
		{name: "head_sha", columnType: "TEXT", notNull: 1, primaryKey: 2},
		{name: "observation_sequence", columnType: "INTEGER", notNull: 1},
	}) {
		return false
	}
	if !s.tableForeignKeysMatch(ctx, "workflow_run_observation_reservations", []schemaForeignKey{
		{
			table:    "repositories",
			from:     "repo_id",
			to:       "id",
			onUpdate: "NO ACTION",
			onDelete: "CASCADE",
			match:    "NONE",
		},
	}) {
		return false
	}
	if !s.tableHasUniqueColumns(
		ctx,
		"workflow_run_observation_reservations",
		[]string{"repo_id", "head_sha"},
	) {
		return false
	}
	createSQL, ok := s.tableCreateSQL(ctx, "workflow_run_observation_reservations")
	if !ok {
		return false
	}
	compact := compactSQL(createSQL)
	return strings.Contains(compact, "check(trim(head_sha)<>'')") &&
		strings.Contains(compact, "check(observation_sequence>0)")
}

func (s *Store) observationSchemaConvergenceHasCurrentShape(ctx context.Context) bool {
	if !s.tableHasCanonicalSQL(
		ctx,
		"observation_schema_convergence",
		canonicalObservationConvergenceCreateSQL,
	) {
		return false
	}
	for _, trigger := range observationConvergenceTriggers {
		var actual string
		if err := s.q().QueryRowContext(ctx, `
			select sql
			from sqlite_schema
			where type = 'trigger' and name = ?
		`, trigger.name).Scan(&actual); err != nil {
			return false
		}
		if actual != sqliteStoredSQL(observationConvergenceTriggerSQL(trigger)) {
			return false
		}
	}
	return true
}

func (s *Store) tableHasCanonicalSQL(ctx context.Context, table, canonical string) bool {
	actual, ok := s.tableCreateSQL(ctx, table)
	return ok && actual == sqliteStoredSQL(canonical)
}

func (s *Store) indexHasCanonicalSQL(ctx context.Context, index, canonical string) bool {
	var actual string
	if err := s.q().QueryRowContext(ctx, `
		select sql
		from sqlite_schema
		where type = 'index' and name = ?
	`, index).Scan(&actual); err != nil {
		return false
	}
	return actual == sqliteStoredSQL(canonical)
}

func (s *Store) tableCreateSQL(ctx context.Context, table string) (string, bool) {
	var createSQL string
	if err := s.q().QueryRowContext(ctx, `
		select sql
		from sqlite_schema
		where type = 'table' and name = ?
	`, table).Scan(&createSQL); err != nil {
		return "", false
	}
	return createSQL, true
}

func sqliteStoredSQL(createSQL string) string {
	switch {
	case strings.HasPrefix(createSQL, "create table "):
		return "CREATE TABLE " + strings.TrimPrefix(createSQL, "create table ")
	case strings.HasPrefix(createSQL, "create index if not exists "):
		return "CREATE INDEX " + strings.TrimPrefix(createSQL, "create index if not exists ")
	case strings.HasPrefix(createSQL, "create index "):
		return "CREATE INDEX " + strings.TrimPrefix(createSQL, "create index ")
	case strings.HasPrefix(createSQL, "create trigger if not exists "):
		return "CREATE TRIGGER " + strings.TrimPrefix(createSQL, "create trigger if not exists ")
	case strings.HasPrefix(createSQL, "create trigger "):
		return "CREATE TRIGGER " + strings.TrimPrefix(createSQL, "create trigger ")
	default:
		return createSQL
	}
}

func compactSQL(value string) string {
	return strings.ToLower(strings.Join(strings.Fields(value), ""))
}

func (s *Store) tableColumnsMatch(ctx context.Context, table string, expected []schemaColumn) bool {
	rows, err := s.q().QueryContext(ctx, `pragma table_info(`+sqliteIdentifier(table)+`)`)
	if err != nil {
		return false
	}
	defer rows.Close()
	var actual []schemaColumn
	for rows.Next() {
		var (
			cid          int
			column       schemaColumn
			defaultValue sql.NullString
		)
		if err := rows.Scan(
			&cid,
			&column.name,
			&column.columnType,
			&column.notNull,
			&defaultValue,
			&column.primaryKey,
		); err != nil {
			return false
		}
		if defaultValue.Valid {
			column.defaultSQL = defaultValue.String
		}
		actual = append(actual, column)
	}
	if err := rows.Err(); err != nil || len(actual) != len(expected) {
		return false
	}
	for i := range expected {
		if actual[i] != expected[i] {
			return false
		}
	}
	return true
}

func (s *Store) tableForeignKeysMatch(
	ctx context.Context,
	table string,
	expected []schemaForeignKey,
) bool {
	rows, err := s.q().QueryContext(ctx, `pragma foreign_key_list(`+sqliteIdentifier(table)+`)`)
	if err != nil {
		return false
	}
	defer rows.Close()
	var actual []schemaForeignKey
	for rows.Next() {
		var (
			id, sequence int
			foreignKey   schemaForeignKey
		)
		if err := rows.Scan(
			&id,
			&sequence,
			&foreignKey.table,
			&foreignKey.from,
			&foreignKey.to,
			&foreignKey.onUpdate,
			&foreignKey.onDelete,
			&foreignKey.match,
		); err != nil {
			return false
		}
		actual = append(actual, foreignKey)
	}
	if err := rows.Err(); err != nil || len(actual) != len(expected) {
		return false
	}
	for i := range expected {
		if actual[i] != expected[i] {
			return false
		}
	}
	return true
}

func (s *Store) tableHasUniqueColumns(ctx context.Context, table string, expected []string) bool {
	rows, err := s.q().QueryContext(ctx, `pragma index_list(`+sqliteIdentifier(table)+`)`)
	if err != nil {
		return false
	}
	var indexes []string
	for rows.Next() {
		var sequence, unique, partial int
		var name, origin string
		if err := rows.Scan(&sequence, &name, &unique, &origin, &partial); err != nil {
			_ = rows.Close()
			return false
		}
		if unique != 0 && partial == 0 {
			indexes = append(indexes, name)
		}
	}
	if err := rows.Close(); err != nil {
		return false
	}
	for _, index := range indexes {
		indexRows, err := s.q().QueryContext(ctx, `pragma index_info(`+sqliteIdentifier(index)+`)`)
		if err != nil {
			continue
		}
		var columns []string
		for indexRows.Next() {
			var sequence, columnID int
			var name sql.NullString
			if err := indexRows.Scan(&sequence, &columnID, &name); err != nil {
				columns = nil
				break
			}
			columns = append(columns, name.String)
		}
		_ = indexRows.Close()
		if stringSlicesEqual(columns, expected) {
			return true
		}
	}
	return false
}

func stringSlicesEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func (s *Store) ensureCanonicalObservationTables(ctx context.Context) error {
	if s.threadsHaveCanonicalShape(ctx) && s.threadRevisionsHaveCanonicalShape(ctx) {
		return nil
	}
	return s.withForeignKeysDisabled(ctx, "canonical observation tables", func(tx *sql.Tx) error {
		statements := []string{
			`drop table if exists threads_migration_backup`,
			`drop table if exists thread_revisions_migration_backup`,
			`create table threads_migration_backup as select * from threads`,
			`create table thread_revisions_migration_backup as select * from thread_revisions`,
			`drop table thread_revisions`,
			`drop table threads`,
			canonicalThreadsCreateSQL,
			`insert into threads(
				id, repo_id, github_id, number, kind, state, title, body,
				author_login, author_type, author_association, html_url,
				labels_json, assignees_json, raw_json, content_hash, is_draft,
				created_at_gh, updated_at_gh, closed_at_gh, merged_at_gh,
				closed_at_local, close_reason_local, first_pulled_at,
				last_pulled_at, observation_sequence,
				evidence_observation_sequence, updated_at
			)
			select
				id, repo_id, github_id, number, kind, state, title, body,
				author_login, author_type, author_association, html_url,
				labels_json, assignees_json, coalesce(raw_json, '{}'),
				content_hash, is_draft,
				created_at_gh, updated_at_gh, closed_at_gh, merged_at_gh,
				closed_at_local, close_reason_local, first_pulled_at,
				last_pulled_at, observation_sequence,
				evidence_observation_sequence, updated_at
			from threads_migration_backup`,
			canonicalThreadRevisionsCreateSQL,
			`insert into thread_revisions(
				id, thread_id, source_updated_at, content_hash, title_hash,
				body_hash, labels_hash, raw_json_blob_id,
				observation_sequence, created_at
			)
			select
				id, thread_id, source_updated_at, content_hash, title_hash,
				body_hash, labels_hash, raw_json_blob_id,
				observation_sequence, created_at
			from thread_revisions_migration_backup`,
			`drop table thread_revisions_migration_backup`,
			`drop table threads_migration_backup`,
			createThreadsRepoNumberIndexSQL,
			createThreadsRepoStateIndexSQL,
			createThreadsRepoUpdatedSQL,
			createRevisionCreatedIndexSQL,
			createRevisionObservationIndexSQL,
		}
		for _, statement := range statements {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) ensureWorkflowRunObservationReservationsSchema(ctx context.Context) error {
	if s.workflowRunObservationReservationsHaveCurrentShape(ctx) {
		return nil
	}
	hasColumns := s.hasColumns(
		ctx,
		"workflow_run_observation_reservations",
		"repo_id",
		"head_sha",
		"observation_sequence",
	)
	return s.withForeignKeysDisabled(ctx, "workflow observation reservation schema", func(tx *sql.Tx) error {
		for _, statement := range []string{
			`drop table if exists workflow_run_observation_reservations_migration_backup`,
			`create table workflow_run_observation_reservations_migration_backup(
				repo_id integer,
				head_sha text,
				observation_sequence integer
			)`,
		} {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return err
			}
		}
		if hasColumns {
			if _, err := tx.ExecContext(ctx, `
				insert into workflow_run_observation_reservations_migration_backup(
					repo_id, head_sha, observation_sequence
				)
				select repo_id, trim(head_sha), max(observation_sequence)
				from workflow_run_observation_reservations
				where repo_id in (select id from repositories)
					and trim(coalesce(head_sha, '')) <> ''
					and observation_sequence > 0
				group by repo_id, trim(head_sha)
			`); err != nil {
				return err
			}
		}
		for _, statement := range []string{
			`drop table if exists workflow_run_observation_reservations`,
			canonicalWorkflowReservationsCreateSQL,
			`insert into workflow_run_observation_reservations(
				repo_id, head_sha, observation_sequence
			)
			select repo_id, head_sha, observation_sequence
			from workflow_run_observation_reservations_migration_backup`,
			`drop table workflow_run_observation_reservations_migration_backup`,
		} {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) ensureThreadChildObservationReservationsSchema(ctx context.Context) error {
	if s.threadChildObservationReservationsHaveCurrentShape(ctx) {
		return nil
	}
	hasColumns := s.hasColumns(
		ctx,
		"thread_child_observation_reservations",
		"thread_id",
		"family",
		"observation_sequence",
	)
	return s.withForeignKeysDisabled(ctx, "child observation reservation schema", func(tx *sql.Tx) error {
		for _, statement := range []string{
			`drop table if exists thread_child_observation_reservations_migration_backup`,
			`create table thread_child_observation_reservations_migration_backup(
				thread_id integer,
				family text,
				observation_sequence integer
			)`,
		} {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return err
			}
		}
		if hasColumns {
			if _, err := tx.ExecContext(ctx, `
				insert into thread_child_observation_reservations_migration_backup(
					thread_id, family, observation_sequence
				)
				select thread_id, family, max(observation_sequence)
				from thread_child_observation_reservations
				where thread_id in (select id from threads)
					and family in (
						'comments',
						'pull_request_details',
						'pull_request_files',
						'pull_request_commits',
						'pull_request_checks',
						'pull_request_review_threads'
					)
					and observation_sequence > 0
				group by thread_id, family
			`); err != nil {
				return err
			}
		}
		for _, statement := range []string{
			`drop table if exists thread_child_observation_reservations`,
			canonicalThreadChildReservationsCreateSQL,
			`insert into thread_child_observation_reservations(
				thread_id, family, observation_sequence
			)
			select thread_id, family, observation_sequence
			from thread_child_observation_reservations_migration_backup`,
			`drop table thread_child_observation_reservations_migration_backup`,
		} {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) ensureThreadObservationSequenceSchema(ctx context.Context) error {
	if s.threadObservationSequenceHasCurrentShape(ctx) {
		_, err := s.db.ExecContext(ctx, `
			insert into thread_observation_sequence(id, value, last_started_at)
			values(1, 0, '')
			on conflict(id) do nothing
		`)
		return err
	}
	hasColumns := s.hasColumns(ctx, "thread_observation_sequence", "id", "value", "last_started_at")
	return s.withForeignKeysDisabled(ctx, "thread observation allocator schema", func(tx *sql.Tx) error {
		for _, statement := range []string{
			`drop table if exists thread_observation_sequence_migration_backup`,
			`create table thread_observation_sequence_migration_backup(
				value integer,
				last_started_at text
			)`,
		} {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return err
			}
		}
		if hasColumns {
			if _, err := tx.ExecContext(ctx, `
				insert into thread_observation_sequence_migration_backup(value, last_started_at)
				select max(coalesce(value, 0)), max(coalesce(last_started_at, ''))
				from thread_observation_sequence
			`); err != nil {
				return err
			}
		}
		for _, statement := range []string{
			`drop table if exists thread_observation_sequence`,
			canonicalThreadObservationSequenceCreateSQL,
			`insert into thread_observation_sequence(id, value, last_started_at)
			select 1, coalesce(max(value), 0), coalesce(max(last_started_at), '')
			from thread_observation_sequence_migration_backup`,
			`drop table thread_observation_sequence_migration_backup`,
		} {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) ensureObservationSchemaConvergence(ctx context.Context) error {
	if s.observationSchemaConvergenceHasCurrentShape(ctx) {
		return nil
	}
	hasColumns := s.hasColumns(
		ctx,
		"observation_schema_convergence",
		"id",
		"checked_observation_sequence",
	)
	return s.withForeignKeysDisabled(ctx, "observation schema convergence", func(tx *sql.Tx) error {
		for _, trigger := range observationConvergenceTriggers {
			if _, err := tx.ExecContext(ctx, `drop trigger if exists `+sqliteIdentifier(trigger.name)); err != nil {
				return err
			}
		}
		for _, statement := range []string{
			`drop table if exists observation_schema_convergence_migration_backup`,
			`create table observation_schema_convergence_migration_backup(
				checked_observation_sequence integer
			)`,
		} {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return err
			}
		}
		if hasColumns {
			if _, err := tx.ExecContext(ctx, `
				insert into observation_schema_convergence_migration_backup(
					checked_observation_sequence
				)
				select max(checked_observation_sequence)
				from observation_schema_convergence
				where id = 1
			`); err != nil {
				return err
			}
		}
		for _, statement := range []string{
			`drop table if exists observation_schema_convergence`,
			canonicalObservationConvergenceCreateSQL,
			`insert into observation_schema_convergence(
				id, checked_observation_sequence
			)
			select 1, coalesce(max(checked_observation_sequence), -1)
			from observation_schema_convergence_migration_backup`,
			`drop table observation_schema_convergence_migration_backup`,
		} {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return err
			}
		}
		for _, trigger := range observationConvergenceTriggers {
			if _, err := tx.ExecContext(ctx, observationConvergenceTriggerSQL(trigger)); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) observationSchemaConvergenceIsCurrent(ctx context.Context) (bool, error) {
	var checked, allocated int64
	if err := s.q().QueryRowContext(ctx, `
		select
			observation_schema_convergence.checked_observation_sequence,
			thread_observation_sequence.value
		from observation_schema_convergence
		join thread_observation_sequence
			on thread_observation_sequence.id = observation_schema_convergence.id
		where observation_schema_convergence.id = 1
	`).Scan(&checked, &allocated); err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, fmt.Errorf("read observation schema convergence: %w", err)
	}
	return checked == allocated, nil
}

func (s *Store) markObservationSchemaConverged(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `
		insert into observation_schema_convergence(
			id, checked_observation_sequence
		)
		select 1, value
		from thread_observation_sequence
		where id = 1
		on conflict(id) do update set
			checked_observation_sequence = excluded.checked_observation_sequence
	`); err != nil {
		return fmt.Errorf("mark observation schema converged: %w", err)
	}
	return nil
}

func observationConvergenceTriggerSQL(trigger observationConvergenceTrigger) string {
	return fmt.Sprintf(`create trigger if not exists %s
after %s on %s
begin
  update observation_schema_convergence
  set checked_observation_sequence = -1
  where id = 1
    and checked_observation_sequence = coalesce((
      select value
      from thread_observation_sequence
      where id = 1
    ), -1);
end`, trigger.name, trigger.event, trigger.table)
}

func (s *Store) withForeignKeysDisabled(
	ctx context.Context,
	name string,
	fn func(*sql.Tx) error,
) error {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("open %s migration connection: %w", name, err)
	}
	defer conn.Close()
	var foreignKeys int
	if err := conn.QueryRowContext(ctx, `pragma foreign_keys`).Scan(&foreignKeys); err != nil {
		return fmt.Errorf("read foreign key mode for %s: %w", name, err)
	}
	if foreignKeys != 0 {
		if _, err := conn.ExecContext(ctx, `pragma foreign_keys = off`); err != nil {
			return fmt.Errorf("disable foreign keys for %s: %w", name, err)
		}
		defer conn.ExecContext(context.Background(), `pragma foreign_keys = on`)
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin %s migration: %w", name, err)
	}
	defer tx.Rollback()
	if err := fn(tx); err != nil {
		return fmt.Errorf("migrate %s: %w", name, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit %s migration: %w", name, err)
	}
	if foreignKeys != 0 {
		if _, err := conn.ExecContext(ctx, `pragma foreign_keys = on`); err != nil {
			return fmt.Errorf("restore foreign keys after %s: %w", name, err)
		}
	}
	rows, err := conn.QueryContext(ctx, `pragma foreign_key_check`)
	if err != nil {
		return fmt.Errorf("check %s foreign keys: %w", name, err)
	}
	defer rows.Close()
	if rows.Next() {
		return fmt.Errorf("%s migration introduced foreign key violations", name)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read %s foreign key check: %w", name, err)
	}
	return nil
}

func (s *Store) hasColumns(ctx context.Context, table string, columns ...string) bool {
	for _, column := range columns {
		if !s.hasColumn(ctx, table, column) {
			return false
		}
	}
	return true
}
