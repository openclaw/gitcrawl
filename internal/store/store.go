package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	crawlstore "github.com/openclaw/crawlkit/store"
	"github.com/openclaw/gitcrawl/internal/store/storedb"
)

const (
	schemaVersion = 10
	timeLayout    = time.RFC3339Nano
)

var sqliteBusyRetryDelays = []time.Duration{
	50 * time.Millisecond,
	100 * time.Millisecond,
	200 * time.Millisecond,
	400 * time.Millisecond,
	800 * time.Millisecond,
}

type sqliteCoder interface {
	Code() int
}

type Store struct {
	db      *sql.DB
	queries dbQueries
	sqlc    *storedb.Queries
	path    string
}

type dbQueries interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type Status struct {
	DBPath          string    `json:"db_path"`
	RepositoryCount int       `json:"repository_count"`
	ThreadCount     int       `json:"thread_count"`
	OpenThreadCount int       `json:"open_thread_count"`
	ClusterCount    int       `json:"cluster_count"`
	LastSyncAt      time.Time `json:"last_sync_at,omitempty"`
}

func Open(ctx context.Context, path string) (*Store, error) {
	base, err := crawlstore.Open(ctx, crawlstore.Options{Path: path})
	if err != nil {
		return nil, err
	}
	db := base.DB()
	st := &Store{db: db, sqlc: storedb.New(db), path: path}
	if err := st.migrate(ctx); err != nil {
		_ = base.Close()
		return nil, err
	}
	return st, nil
}

func OpenReadOnly(ctx context.Context, path string) (*Store, error) {
	base, err := crawlstore.OpenReadOnly(ctx, path)
	if err != nil {
		return nil, err
	}
	db := base.DB()
	st := &Store{db: db, sqlc: storedb.New(db), path: path}
	current, err := st.schemaVersion(ctx)
	if err != nil {
		_ = base.Close()
		return nil, err
	}
	if current > schemaVersion {
		_ = base.Close()
		return nil, fmt.Errorf("database schema version %d is newer than supported version %d", current, schemaVersion)
	}
	return st, nil
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) DB() *sql.DB {
	return s.db
}

func (s *Store) Path() string {
	return s.path
}

func (s *Store) q() dbQueries {
	if s.queries != nil {
		return s.queries
	}
	return s.db
}

func (s *Store) qsql() *storedb.Queries {
	if s.sqlc != nil {
		return s.sqlc
	}
	return storedb.New(s.q())
}

func (s *Store) WithTx(ctx context.Context, fn func(*Store) error) error {
	return withSQLiteBusyRetry(ctx, sqliteBusyRetryDelays, func() error {
		return s.withTxOnce(ctx, fn)
	})
}

func (s *Store) withTxOnce(ctx context.Context, fn func(*Store) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	txStore := &Store{db: s.db, queries: tx, sqlc: s.qsql().WithTx(tx), path: s.path}
	if err := fn(txStore); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}

func withSQLiteBusyRetry(ctx context.Context, delays []time.Duration, fn func() error) error {
	attempts := len(delays) + 1
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}
		lastErr = err
		if !IsTransientSQLiteBusy(err) || attempt == len(delays) {
			break
		}
		timer := time.NewTimer(delays[attempt])
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	if IsTransientSQLiteBusy(lastErr) {
		return fmt.Errorf("sqlite busy after %d attempts: %w", attempts, lastErr)
	}
	return lastErr
}

func IsTransientSQLiteBusy(err error) bool {
	if err == nil {
		return false
	}
	for current := err; current != nil; current = errors.Unwrap(current) {
		if coder, ok := current.(sqliteCoder); ok {
			code := coder.Code() & 0xff
			return code == 5 || code == 6
		}
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "sqlite_busy") ||
		strings.Contains(message, "database is locked") ||
		strings.Contains(message, "database table is locked")
}

func (s *Store) Status(ctx context.Context) (Status, error) {
	status := Status{DBPath: s.path}
	if !s.hasTable(ctx, "repositories") {
		return status, nil
	}
	repositoryCount, err := s.qsql().CountRepositories(ctx)
	if err != nil {
		return Status{}, fmt.Errorf("count repositories: %w", err)
	}
	status.RepositoryCount = int(repositoryCount)
	threadCount, err := s.qsql().CountThreads(ctx)
	if err != nil {
		return Status{}, fmt.Errorf("count threads: %w", err)
	}
	status.ThreadCount = int(threadCount)
	openThreadCount, err := s.qsql().CountOpenThreads(ctx)
	if err != nil {
		return Status{}, fmt.Errorf("count open threads: %w", err)
	}
	status.OpenThreadCount = int(openThreadCount)
	clusterCount, err := s.qsql().CountClusters(ctx)
	if err != nil {
		return Status{}, fmt.Errorf("count clusters: %w", err)
	}
	status.ClusterCount = int(clusterCount)
	var lastSync string
	if s.hasTable(ctx, "sync_runs") {
		lastSync, err = s.qsql().MaxSuccessfulSyncFinishedAt(ctx)
		if err != nil {
			return Status{}, fmt.Errorf("read last sync: %w", err)
		}
	}
	if lastSync == "" && s.hasTable(ctx, "portable_metadata") {
		lastSync, err = s.qsql().PortableExportedAt(ctx)
		if err != nil && err != sql.ErrNoRows {
			return Status{}, fmt.Errorf("read portable exported timestamp: %w", err)
		}
	}
	if lastSync == "" && s.hasTable(ctx, "repo_sync_state") {
		lastSync, err = s.qsql().RepoSyncStateLastSync(ctx)
		if err != nil {
			return Status{}, fmt.Errorf("read portable sync state: %w", err)
		}
	}
	if lastSync != "" {
		parsed, err := time.Parse(timeLayout, lastSync)
		if err == nil {
			status.LastSyncAt = parsed
		}
	}
	return status, nil
}

func (s *Store) migrate(ctx context.Context) error {
	current, err := s.schemaVersion(ctx)
	if err != nil {
		return err
	}
	if current > schemaVersion {
		return fmt.Errorf("database schema version %d is newer than supported version %d", current, schemaVersion)
	}
	hadEvidenceObservationSequence := s.hasColumn(ctx, "threads", "evidence_observation_sequence")
	hadChildObservationReservations := s.hasTable(ctx, "thread_child_observation_reservations")
	hadWorkflowRunObservationReservations := s.hasTable(ctx, "workflow_run_observation_reservations")
	if _, err := s.db.ExecContext(ctx, schemaSQL); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	if err := s.ensureLegacyPortableColumns(ctx); err != nil {
		return err
	}
	if current < 8 {
		if err := s.ensureThreadObservationSequenceFloor(ctx); err != nil {
			return err
		}
	}
	if current < 9 || !hadEvidenceObservationSequence {
		if err := s.ensureThreadEvidenceObservationSequence(ctx); err != nil {
			return err
		}
	}
	if current < 10 || !hadChildObservationReservations {
		if err := s.ensureThreadChildObservationReservations(ctx); err != nil {
			return err
		}
	}
	if current < 10 || !hadWorkflowRunObservationReservations {
		if err := s.ensureWorkflowRunObservationReservations(ctx); err != nil {
			return err
		}
	}
	if err := s.ensureThreadVectorsCompositeKey(ctx); err != nil {
		return err
	}
	if err := s.ensurePullRequestFilesPositionKey(ctx); err != nil {
		return err
	}
	if err := s.ensureThreadRevisionTransitionHistory(ctx); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf(`pragma user_version = %d`, schemaVersion)); err != nil {
		return fmt.Errorf("set schema version: %w", err)
	}
	return nil
}

func (s *Store) ensureLegacyPortableColumns(ctx context.Context) error {
	if err := s.ensureColumn(ctx, "repositories", "raw_json", "text"); err != nil {
		return err
	}
	if err := s.ensureColumn(ctx, "thread_revisions", "raw_json_blob_id", "integer references blobs(id) on delete set null"); err != nil {
		return err
	}
	if err := s.ensureColumn(ctx, "thread_revisions", "observation_sequence", "integer not null default 0"); err != nil {
		return err
	}
	if err := s.ensureColumn(ctx, "threads", "observation_sequence", "integer not null default 0"); err != nil {
		return err
	}
	if err := s.ensureColumn(ctx, "threads", "evidence_observation_sequence", "integer not null default 0"); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `
		create index if not exists idx_thread_revisions_thread_observation
		on thread_revisions(thread_id, observation_sequence desc)
	`); err != nil {
		return fmt.Errorf("ensure thread revision observation index: %w", err)
	}
	hadThreadBody := s.hasColumn(ctx, "threads", "body")
	if err := s.ensureColumn(ctx, "threads", "body", "text"); err != nil {
		return err
	}
	if err := s.ensureColumn(ctx, "threads", "raw_json", "text"); err != nil {
		return err
	}
	if err := s.ensureColumn(ctx, "threads", "author_association", "text"); err != nil {
		return err
	}
	if !hadThreadBody && s.hasColumn(ctx, "threads", "body_excerpt") {
		if _, err := s.db.ExecContext(ctx, `update threads set body = body_excerpt where body is null and body_excerpt is not null`); err != nil {
			return fmt.Errorf("backfill thread body from portable excerpt: %w", err)
		}
	}
	return nil
}

func (s *Store) ensureThreadEvidenceObservationSequence(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `
		update threads
		set evidence_observation_sequence = max(
			evidence_observation_sequence,
			coalesce((
				select max(observation_sequence)
				from thread_revisions
				where thread_id = threads.id
					and observation_sequence > 0
			), 0)
		)
	`); err != nil {
		return fmt.Errorf("backfill thread evidence observation sequence: %w", err)
	}
	return nil
}

func (s *Store) ensureThreadChildObservationReservations(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `
		insert into thread_child_observation_reservations(
			thread_id, family, observation_sequence
		)
		select id, 'comments', evidence_observation_sequence
		from threads
		where evidence_observation_sequence > 0
		on conflict(thread_id, family) do update set
			observation_sequence = max(
				thread_child_observation_reservations.observation_sequence,
				excluded.observation_sequence
			)
	`); err != nil {
		return fmt.Errorf("backfill comment observation reservations: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `
		insert into thread_child_observation_reservations(
			thread_id, family, observation_sequence
		)
		select threads.id, families.family, threads.evidence_observation_sequence
		from threads
		cross join (
			select 'pull_request_details' as family
			union all select 'pull_request_files'
			union all select 'pull_request_commits'
			union all select 'pull_request_checks'
			union all select 'pull_request_review_threads'
		) as families
		where threads.kind = 'pull_request'
			and threads.evidence_observation_sequence > 0
		on conflict(thread_id, family) do update set
			observation_sequence = max(
				thread_child_observation_reservations.observation_sequence,
				excluded.observation_sequence
			)
	`); err != nil {
		return fmt.Errorf("backfill pull request observation reservations: %w", err)
	}
	return nil
}

func (s *Store) ensureWorkflowRunObservationReservations(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `
		with candidates(repo_id, head_sha, observation_sequence) as (
			select
				pull_request_details.repo_id,
				trim(pull_request_details.head_sha),
				threads.evidence_observation_sequence
			from pull_request_details
			join threads on threads.id = pull_request_details.thread_id
			where trim(coalesce(pull_request_details.head_sha, '')) <> ''
				and threads.evidence_observation_sequence > 0
			union all
			select
				pull_request_details.repo_id,
				trim(pull_request_details.head_sha),
				thread_child_observation_reservations.observation_sequence
			from pull_request_details
			join thread_child_observation_reservations
				on thread_child_observation_reservations.thread_id = pull_request_details.thread_id
				and thread_child_observation_reservations.family = 'workflow_runs'
			where trim(coalesce(pull_request_details.head_sha, '')) <> ''
				and thread_child_observation_reservations.observation_sequence > 0
		)
		insert into workflow_run_observation_reservations(
			repo_id, head_sha, observation_sequence
		)
		select repo_id, head_sha, max(observation_sequence)
		from candidates
		group by repo_id, head_sha
		on conflict(repo_id, head_sha) do update set
			observation_sequence = max(
				workflow_run_observation_reservations.observation_sequence,
				excluded.observation_sequence
			)
	`); err != nil {
		return fmt.Errorf("backfill workflow run observation reservations: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `
		delete from thread_child_observation_reservations
		where family = 'workflow_runs'
	`); err != nil {
		return fmt.Errorf("remove legacy thread workflow run reservations: %w", err)
	}
	return nil
}

func (s *Store) ensureThreadObservationSequenceFloor(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `
		update thread_observation_sequence
		set value = max(
			value,
			coalesce((
				select max(case
					when observation_sequence < 0 then -observation_sequence
					else observation_sequence
				end)
				from threads
			), 0),
			coalesce((select max(observation_sequence) from thread_revisions), 0)
		)
		where id = 1
	`); err != nil {
		return fmt.Errorf("reconcile thread observation sequence: %w", err)
	}
	return nil
}

func (s *Store) ensureThreadVectorsCompositeKey(ctx context.Context) error {
	if !s.hasTable(ctx, "thread_vectors") || s.threadVectorsHaveCompositeKey(ctx) {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin thread vector key migration: %w", err)
	}
	defer tx.Rollback()
	for _, stmt := range []string{
		`drop index if exists idx_thread_vectors_basis_model`,
		`alter table thread_vectors rename to thread_vectors_old`,
		`create table thread_vectors (
			thread_id integer not null references threads(id) on delete cascade,
			basis text not null,
			model text not null,
			dimensions integer not null,
			content_hash text not null,
			vector_json text not null,
			vector_backend text not null,
			created_at text not null,
			updated_at text not null,
			primary key(thread_id, basis, model)
		)`,
		`insert into thread_vectors(thread_id, basis, model, dimensions, content_hash, vector_json, vector_backend, created_at, updated_at)
			select thread_id, basis, model, dimensions, content_hash, vector_json, vector_backend, created_at, updated_at
			from thread_vectors_old`,
		`drop table thread_vectors_old`,
		`create index if not exists idx_thread_vectors_basis_model on thread_vectors(basis, model)`,
	} {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("migrate thread vector key: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit thread vector key migration: %w", err)
	}
	return nil
}

func (s *Store) ensurePullRequestFilesPositionKey(ctx context.Context) error {
	if !s.hasTable(ctx, "pull_request_files") || s.pullRequestFilesHavePositionKey(ctx) {
		return nil
	}
	// Existing stores keyed PR files by path. The new key uses the fetched
	// snapshot position so duplicate GitHub filenames can coexist. Legacy rows
	// were unique by path, so ordering them by path gives each row a stable
	// migration position; later syncs replace the full per-PR file snapshot.
	// See https://github.com/openclaw/gitcrawl/issues/77 for the duplicate-path
	// bug and why position is snapshot-local rather than a durable file identity.
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin pull request files key migration: %w", err)
	}
	defer tx.Rollback()
	for _, stmt := range []string{
		`drop index if exists idx_pull_request_files_path`,
		`drop index if exists idx_pull_request_files_thread_path`,
		`alter table pull_request_files rename to pull_request_files_old`,
		`create table pull_request_files (
			thread_id integer not null references threads(id) on delete cascade,
			position integer not null default 0,
			path text not null,
			status text,
			additions integer not null default 0,
			deletions integer not null default 0,
			changes integer not null default 0,
			previous_path text,
			patch text,
			raw_json text not null,
			fetched_at text not null,
			primary key(thread_id, position)
		)`,
		`insert into pull_request_files(thread_id, position, path, status, additions, deletions, changes, previous_path, patch, raw_json, fetched_at)
			select old.thread_id,
				(select count(*) from pull_request_files_old prior where prior.thread_id = old.thread_id and prior.path < old.path),
				old.path,
				old.status,
				old.additions,
				old.deletions,
				old.changes,
				old.previous_path,
				old.patch,
				old.raw_json,
				old.fetched_at
			from pull_request_files_old old`,
		`drop table pull_request_files_old`,
		`create index if not exists idx_pull_request_files_path on pull_request_files(path)`,
		`create index if not exists idx_pull_request_files_thread_path on pull_request_files(thread_id, path)`,
	} {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("migrate pull request files key: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit pull request files key migration: %w", err)
	}
	return nil
}

func (s *Store) pullRequestFilesHavePositionKey(ctx context.Context) bool {
	rows, err := s.db.QueryContext(ctx, `pragma table_info(pull_request_files)`)
	if err != nil {
		return false
	}
	defer rows.Close()
	pk := map[string]int{}
	for rows.Next() {
		var cid int
		var name, columnType string
		var notNull, primaryKey int
		var defaultValue any
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			return false
		}
		if primaryKey > 0 {
			pk[name] = primaryKey
		}
	}
	return pk["thread_id"] == 1 && pk["position"] == 2
}

func (s *Store) ensureThreadRevisionTransitionHistory(ctx context.Context) error {
	if !s.hasTable(ctx, "thread_revisions") || !s.threadRevisionsHaveUniqueContentHash(ctx) {
		return nil
	}
	observationSequenceExpression := "0"
	if s.hasColumn(ctx, "thread_revisions", "observation_sequence") {
		observationSequenceExpression = "observation_sequence"
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("open thread revision migration connection: %w", err)
	}
	defer conn.Close()

	var foreignKeys int
	if err := conn.QueryRowContext(ctx, `pragma foreign_keys`).Scan(&foreignKeys); err != nil {
		return fmt.Errorf("read foreign key mode: %w", err)
	}
	if foreignKeys != 0 {
		if _, err := conn.ExecContext(ctx, `pragma foreign_keys = off`); err != nil {
			return fmt.Errorf("disable foreign keys for thread revision migration: %w", err)
		}
		defer conn.ExecContext(context.Background(), `pragma foreign_keys = on`)
	}

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin thread revision transition migration: %w", err)
	}
	defer tx.Rollback()
	copyRevisions := fmt.Sprintf(`insert into thread_revisions_new(
			id, thread_id, source_updated_at, content_hash, title_hash, body_hash,
			labels_hash, raw_json_blob_id, observation_sequence, created_at
		)
		select id, thread_id, source_updated_at, content_hash, title_hash, body_hash,
			labels_hash, raw_json_blob_id, %s, created_at
		from thread_revisions`, observationSequenceExpression)
	for _, stmt := range []string{
		`drop table if exists thread_revisions_new`,
		`create table thread_revisions_new (
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
			)`,
		copyRevisions,
		`drop table thread_revisions`,
		`alter table thread_revisions_new rename to thread_revisions`,
		`create index if not exists idx_thread_revisions_thread_created on thread_revisions(thread_id, created_at)`,
		`create index if not exists idx_thread_revisions_thread_observation on thread_revisions(thread_id, observation_sequence desc)`,
	} {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("migrate thread revision transitions: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit thread revision transition migration: %w", err)
	}
	if foreignKeys != 0 {
		if _, err := conn.ExecContext(ctx, `pragma foreign_keys = on`); err != nil {
			return fmt.Errorf("restore foreign keys after thread revision migration: %w", err)
		}
	}
	rows, err := conn.QueryContext(ctx, `pragma foreign_key_check`)
	if err != nil {
		return fmt.Errorf("check thread revision migration foreign keys: %w", err)
	}
	defer rows.Close()
	if rows.Next() {
		return fmt.Errorf("thread revision migration introduced foreign key violations")
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read thread revision migration foreign key check: %w", err)
	}
	return nil
}

func (s *Store) threadRevisionsHaveUniqueContentHash(ctx context.Context) bool {
	rows, err := s.q().QueryContext(ctx, `pragma index_list("thread_revisions")`)
	if err != nil {
		return false
	}
	var uniqueIndexes []string
	for rows.Next() {
		var sequence, unique, partial int
		var name, origin string
		if err := rows.Scan(&sequence, &name, &unique, &origin, &partial); err != nil {
			_ = rows.Close()
			return false
		}
		if unique != 0 {
			uniqueIndexes = append(uniqueIndexes, name)
		}
	}
	if err := rows.Close(); err != nil {
		return false
	}
	for _, index := range uniqueIndexes {
		indexRows, err := s.q().QueryContext(ctx, `pragma index_info(`+sqliteIdentifier(index)+`)`)
		if err != nil {
			continue
		}
		var columns []string
		for indexRows.Next() {
			var sequence, columnID int
			var name sql.NullString
			if err := indexRows.Scan(&sequence, &columnID, &name); err != nil {
				_ = indexRows.Close()
				columns = nil
				break
			}
			columns = append(columns, name.String)
		}
		_ = indexRows.Close()
		if len(columns) == 2 && columns[0] == "thread_id" && columns[1] == "content_hash" {
			return true
		}
	}
	return false
}

func (s *Store) threadVectorsHaveCompositeKey(ctx context.Context) bool {
	rows, err := s.db.QueryContext(ctx, `pragma table_info(thread_vectors)`)
	if err != nil {
		return false
	}
	defer rows.Close()
	pk := map[string]int{}
	for rows.Next() {
		var cid int
		var name, columnType string
		var notNull, primaryKey int
		var defaultValue any
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			return false
		}
		if primaryKey > 0 {
			pk[name] = primaryKey
		}
	}
	return pk["thread_id"] == 1 && pk["basis"] == 2 && pk["model"] == 3
}

func (s *Store) hasTable(ctx context.Context, table string) bool {
	var name string
	err := s.q().QueryRowContext(ctx, `select name from sqlite_schema where type in ('table', 'virtual table') and name = ?`, table).Scan(&name)
	return err == nil
}

func (s *Store) ensureColumn(ctx context.Context, table, column, definition string) error {
	if s.hasColumn(ctx, table, column) {
		return nil
	}
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf(`alter table %s add column %s %s`, table, column, definition)); err != nil {
		return fmt.Errorf("add %s.%s: %w", table, column, err)
	}
	return nil
}

func (s *Store) hasColumn(ctx context.Context, table, column string) bool {
	rows, err := s.q().QueryContext(ctx, fmt.Sprintf(`pragma table_info(%s)`, table))
	if err != nil {
		return false
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, typ string
		var notNull int
		var defaultValue sql.NullString
		var primaryKey int
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &primaryKey); err != nil {
			return false
		}
		if name == column {
			return true
		}
	}
	return false
}

func (s *Store) schemaVersion(ctx context.Context) (int, error) {
	var version int
	if err := s.db.QueryRowContext(ctx, `pragma user_version`).Scan(&version); err != nil {
		return 0, fmt.Errorf("read schema version: %w", err)
	}
	return version, nil
}
