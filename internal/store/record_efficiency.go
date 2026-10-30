package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

// Record efficiency separates content from check bookkeeping.
//
// Content rows (threads, pull_request_details, pull_request_review_threads,
// repositories, analytics_coverage and analytics_review_state_coverage) are
// written only when a content field changes. Every check is still recorded,
// in a check table keyed like its content row:
//
//   - thread_checks: last pull, observation order and evidence bookkeeping;
//   - pull_request_detail_fetches: last PR detail fetch;
//   - pull_request_review_thread_syncs (existing): last review-thread fetch;
//   - repository_checks: last repository check and provider activity time;
//   - analytics_coverage_checks: coverage observation time and watermark.
//
// Merge state, which flips without any author action, is an append-only event
// series (pull_request_merge_state_events) and coverage counts are a
// run-length metric series (analytics_coverage_count_samples). Neither creates
// a content version.
//
// The *_current views overlay the latest check values onto the content rows,
// reproducing the pre-split column values for readers that have not moved to
// the check tables. A view prefers the check value only when it is at least as
// new as the content row, so a rollback to an older binary that rewrites the
// content columns remains visible.

const recordEfficiencySchemaSQL = `
create table if not exists record_efficiency_state (
  id integer primary key check (id = 1),
  version integer not null,
  enabled_at text not null
);

create table if not exists thread_checks (
  thread_id integer primary key,
  first_pulled_at text,
  last_pulled_at text,
  observation_sequence integer not null default 0
    check (typeof(observation_sequence) = 'integer' and observation_sequence >= 0),
  evidence_complete integer not null default 0 check (evidence_complete in (0, 1)),
  evidence_observation_sequence integer not null default 0
    check (typeof(evidence_observation_sequence) = 'integer' and evidence_observation_sequence >= 0),
  evidence_source_updated_at text not null default '',
  evidence_source_order_key text not null default '',
  content_evidence_source_order_key text not null default '',
  checked_at text not null default ''
);

create table if not exists pull_request_detail_fetches (
  thread_id integer primary key,
  fetched_at text not null
);

create table if not exists pull_request_merge_state_events (
  id integer primary key autoincrement,
  thread_id integer not null,
  source text not null check (source in ('graphql', 'rest')),
  observed_at text not null,
  mergeable text,
  merge_state_status text,
  mergeable_state text
);

create index if not exists idx_pull_request_merge_state_events_thread
  on pull_request_merge_state_events(thread_id, source, id);

create index if not exists idx_pull_request_merge_state_events_latest
  on pull_request_merge_state_events(thread_id, id);

create table if not exists repository_checks (
  repo_id integer primary key,
  checked_at text not null,
  provider_updated_at text not null default ''
);

create table if not exists analytics_coverage_checks (
  repository text not null,
  coverage text not null
    check (coverage in ('analytics_coverage', 'analytics_review_state_coverage')),
  observed_at text not null,
  through text,
  primary key(repository, coverage)
) without rowid;

create table if not exists analytics_coverage_count_samples (
  repository text not null,
  observed_at text not null,
  issues integer,
  pull_requests integer,
  primary key(repository, observed_at)
) without rowid;
`

// recordEfficiencyTables lists the check and event tables. Portable stores
// fold their current values into the content rows and drop them.
var recordEfficiencyTables = []string{
	"record_check_observations",
	"record_evidence_blobs",
	"thread_checks",
	"pull_request_detail_fetches",
	"pull_request_merge_state_events",
	"repository_checks",
	"analytics_coverage_checks",
	"analytics_coverage_count_samples",
}

// recordEfficiencyViews lists the compatibility views in dependency-free order.
var recordEfficiencyViews = []string{
	"threads_current",
	"pull_request_details_current",
	"pull_request_review_threads_current",
	"repositories_current",
	"analytics_coverage_current",
	"analytics_review_state_coverage_current",
}

// RecordEfficiencyVersion is the layout version recorded by EnableRecordEfficiency.
const RecordEfficiencyVersion = 1

// recordEfficiencyActive reports whether an archive opted in to the split
// layout. The layout is opt-in per archive, like the change contract: until
// EnableRecordEfficiency runs, every write keeps the legacy single-row
// semantics and no check table, view or marker exists.
func (s *Store) recordEfficiencyActive(ctx context.Context) bool {
	if s.hasTable(ctx, "portable_metadata") || !s.hasTable(ctx, "record_efficiency_state") {
		return false
	}
	var version int64
	if err := s.q().QueryRowContext(ctx, `select version from record_efficiency_state where id = 1`).Scan(&version); err != nil {
		return false
	}
	return version >= 1
}

// ensureRecordEfficiencySchema refreshes the check tables and compatibility
// views of an opted-in native archive after migrations. Portable stores and
// mirrors keep the legacy single-row layout.
func (s *Store) ensureRecordEfficiencySchema(ctx context.Context) error {
	if !s.recordEfficiencyActive(ctx) {
		s.recordChecks = false
		return nil
	}
	if _, err := s.q().ExecContext(ctx, recordEfficiencySchemaSQL); err != nil {
		return fmt.Errorf("create record efficiency tables: %w", err)
	}
	if err := s.ensureRecordCheckHistory(ctx); err != nil {
		return err
	}
	if err := s.ensureRecordEfficiencyViews(ctx); err != nil {
		return err
	}
	s.recordChecks = true
	return nil
}

// detectRecordChecks enables the split write path on stores opened without
// migration when the archive opted in and every check table exists.
func (s *Store) detectRecordChecks(ctx context.Context) {
	s.recordChecks = s.recordEfficiencyActive(ctx)
	for _, table := range recordEfficiencyTables {
		s.recordChecks = s.recordChecks && s.hasTable(ctx, table)
	}
}

// EnableRecordEfficiency opts a native archive in to the split layout: it
// creates the check tables, event tables and compatibility views, records the
// marker and seeds missing check rows from the content rows. It is idempotent.
// Content rows are never modified.
func (s *Store) EnableRecordEfficiency(ctx context.Context, now string) (RecordEfficiencyBackfill, error) {
	if s.hasTable(ctx, "portable_metadata") {
		return RecordEfficiencyBackfill{}, fmt.Errorf("record efficiency requires a native archive")
	}
	if !s.hasTable(ctx, "threads") {
		return RecordEfficiencyBackfill{}, fmt.Errorf("record efficiency requires an archive with threads")
	}
	if err := s.WithTx(ctx, func(tx *Store) error {
		if _, err := tx.q().ExecContext(ctx, recordEfficiencySchemaSQL); err != nil {
			return fmt.Errorf("create record efficiency tables: %w", err)
		}
		if _, err := tx.q().ExecContext(ctx, `insert into record_efficiency_state(id, version, enabled_at) values(1, ?, ?)
			on conflict(id) do update set version = excluded.version`, RecordEfficiencyVersion, now); err != nil {
			return fmt.Errorf("record record efficiency marker: %w", err)
		}
		if err := tx.ensureRecordCheckHistory(ctx); err != nil {
			return err
		}
		return tx.ensureRecordEfficiencyViews(ctx)
	}); err != nil {
		return RecordEfficiencyBackfill{}, err
	}
	s.recordChecks = true
	return s.BackfillRecordEfficiency(ctx)
}

// DisableRecordEfficiency returns an archive to the legacy single-row layout:
// the latest check values are folded into the content rows, the views and
// marker are removed, and the check and event tables are retained as history.
func (s *Store) DisableRecordEfficiency(ctx context.Context) error {
	err := s.WithTx(ctx, func(tx *Store) error {
		if err := tx.dropRecordCheckHistory(ctx); err != nil {
			return err
		}
		if err := tx.foldRecordEfficiencyIntoContent(ctx); err != nil {
			return err
		}
		if err := dropRecordEfficiencyViews(ctx, tx.q()); err != nil {
			return err
		}
		_, err := tx.q().ExecContext(ctx, `drop table if exists record_efficiency_state`)
		return err
	})
	if err == nil {
		s.recordChecks = false
	}
	return err
}

// prepareRecordEfficiencyMigration runs before schema migrations that may
// rebuild content tables. Views would block a rebuild, and legacy migrations
// read bookkeeping from the content rows, so the current check values are
// folded into the content rows and the views dropped; Open recreates them.
func (s *Store) prepareRecordEfficiencyMigration(ctx context.Context) error {
	if !s.hasTable(ctx, "record_efficiency_state") {
		return dropRecordEfficiencyViews(ctx, s.q())
	}
	if err := s.dropRecordCheckHistory(ctx); err != nil {
		return err
	}
	if err := s.foldRecordEfficiencyIntoContent(ctx); err != nil {
		return err
	}
	return dropRecordEfficiencyViews(ctx, s.q())
}

// RecordChecksEnabled reports whether this store writes content rows only on
// content change and records checks separately.
func (s *Store) RecordChecksEnabled() bool { return s.recordChecks }

func (s *Store) hasView(ctx context.Context, name string) bool {
	var found string
	err := s.q().QueryRowContext(ctx, `select name from sqlite_schema where type = 'view' and name = ?`, name).Scan(&found)
	return err == nil
}

// CurrentSource returns the compatibility view for a content table when it
// exists (see currentSource).
func (s *Store) CurrentSource(ctx context.Context, table string) string {
	return s.currentSource(ctx, table)
}

// currentSource returns the compatibility view for a content table when it
// exists, so internal readers observe the latest check values.
func (s *Store) currentSource(ctx context.Context, table string) string {
	view := table + "_current"
	if s.hasView(ctx, view) {
		return view
	}
	return table
}

func (s *Store) tableColumns(ctx context.Context, table string) ([]string, error) {
	rows, err := s.q().QueryContext(ctx, `select name from pragma_table_info(?) order by cid`, table)
	if err != nil {
		return nil, fmt.Errorf("inspect %s columns: %w", table, err)
	}
	defer rows.Close()
	var columns []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		columns = append(columns, name)
	}
	return columns, rows.Err()
}

type recordEfficiencyView struct {
	name     string
	table    string
	alias    string
	joins    string
	overlay  map[string]string
	extra    []string
	required []string
}

func recordEfficiencyViewSpecs() []recordEfficiencyView {
	useThreadCheck := `(c.thread_id is not null and c.observation_sequence >= abs(t.observation_sequence))`
	useEvidenceCheck := `(c.thread_id is not null and (c.evidence_source_order_key > coalesce(nullif(c.content_evidence_source_order_key,''),'') or (c.evidence_source_order_key = coalesce(nullif(c.content_evidence_source_order_key,''),'') and c.evidence_observation_sequence >= t.evidence_observation_sequence)))`
	newer := func(check, content string) string {
		return `(` + check + ` is not null and (` + content + ` is null or julianday(` + check + `) >= julianday(` + content + `)))`
	}
	latestMerge := `(select e.%s from pull_request_merge_state_events e where e.thread_id = d.thread_id order by e.id desc limit 1)`
	latestMergeAt := fmt.Sprintf(latestMerge, "observed_at")
	useMerge := `(` + latestMergeAt + ` is not null)`
	useFetch := newer("f.fetched_at", "d.fetched_at")
	inMembership := `(s.review_thread_ids_json is not null and json_valid(s.review_thread_ids_json) and exists(select 1 from json_each(s.review_thread_ids_json) j where j.value = rt.review_thread_id))`
	useSync := `(` + inMembership + ` and ` + newer("s.fetched_at", "rt.fetched_at") + `)`
	useRepoCheck := newer("rc.checked_at", "r.updated_at")
	coverageCheck := newer("k.observed_at", "a.observed_at")
	latestCount := `(select n.%s from analytics_coverage_count_samples n where n.repository = a.repository order by julianday(n.observed_at) desc, n.observed_at desc limit 1)`
	latestCountAt := fmt.Sprintf(latestCount, "observed_at")
	useCount := newer(latestCountAt, "a.observed_at")
	reviewCheck := newer("k.observed_at", "a.observed_at")
	return []recordEfficiencyView{
		{
			name:  "threads_current",
			table: "threads",
			alias: "t",
			joins: `left join thread_checks c on c.thread_id = t.id`,
			overlay: map[string]string{
				"first_pulled_at": `coalesce(t.first_pulled_at, c.first_pulled_at)`,
				"last_pulled_at":  `case when ` + useThreadCheck + ` then c.last_pulled_at else t.last_pulled_at end`,
				"observation_sequence": `case when ` + useThreadCheck + ` then
		case when c.evidence_complete = 1 then c.observation_sequence else -c.observation_sequence end
		else t.observation_sequence end`,
				"evidence_observation_sequence": `case when ` + useEvidenceCheck + ` then c.evidence_observation_sequence else t.evidence_observation_sequence end`,
				"evidence_source_updated_at":    `case when ` + useEvidenceCheck + ` then c.evidence_source_updated_at else t.evidence_source_updated_at end`,
				"updated_at":                    `case when ` + useThreadCheck + ` and ` + newer("nullif(c.checked_at, '')", "t.updated_at") + ` then c.checked_at else t.updated_at end`,
			},
			extra: []string{
				`case when ` + useThreadCheck + ` then c.evidence_complete else (t.observation_sequence > 0) end as evidence_complete`,
				`t.updated_at as content_updated_at`,
			},
			required: []string{"id", "observation_sequence", "evidence_observation_sequence", "evidence_source_updated_at", "first_pulled_at", "last_pulled_at", "updated_at"},
		},
		{
			name:  "pull_request_details_current",
			table: "pull_request_details",
			alias: "d",
			joins: `left join pull_request_detail_fetches f on f.thread_id = d.thread_id`,
			overlay: map[string]string{
				"fetched_at":      `case when ` + useFetch + ` then f.fetched_at else d.fetched_at end`,
				"mergeable_state": `case when ` + useMerge + ` then ` + fmt.Sprintf(latestMerge, "mergeable_state") + ` else d.mergeable_state end`,
			},
			extra: []string{
				`case when ` + useMerge + ` then ` + fmt.Sprintf(latestMerge, "mergeable") + ` else null end as mergeable`,
				`case when ` + useMerge + ` then ` + fmt.Sprintf(latestMerge, "merge_state_status") + ` else null end as merge_state_status`,
				`d.fetched_at as content_fetched_at`,
			},
			required: []string{"thread_id", "fetched_at", "mergeable_state", "updated_at"},
		},
		{
			name:  "pull_request_review_threads_current",
			table: "pull_request_review_threads",
			alias: "rt",
			joins: `left join pull_request_review_thread_syncs s on s.thread_id = rt.thread_id`,
			overlay: map[string]string{
				"fetched_at": `case when ` + useSync + ` then s.fetched_at else rt.fetched_at end`,
			},
			extra:    []string{`rt.fetched_at as content_fetched_at`},
			required: []string{"thread_id", "review_thread_id", "fetched_at"},
		},
		{
			name:  "repositories_current",
			table: "repositories",
			alias: "r",
			joins: `left join repository_checks rc on rc.repo_id = r.id`,
			overlay: map[string]string{
				"updated_at": `case when ` + useRepoCheck + ` then rc.checked_at else r.updated_at end`,
			},
			extra: []string{
				`coalesce(rc.provider_updated_at, '') as provider_updated_at`,
				`r.updated_at as content_updated_at`,
			},
			required: []string{"id", "updated_at"},
		},
		{
			name:  "analytics_coverage_current",
			table: "analytics_coverage",
			alias: "a",
			joins: `left join analytics_coverage_checks k on k.repository = a.repository and k.coverage = 'analytics_coverage'`,
			overlay: map[string]string{
				"through":       `case when ` + coverageCheck + ` and k.through is not null then k.through else a.through end`,
				"observed_at":   `case when ` + coverageCheck + ` then k.observed_at else a.observed_at end`,
				"issues":        `case when ` + useCount + ` then ` + fmt.Sprintf(latestCount, "issues") + ` else a.issues end`,
				"pull_requests": `case when ` + useCount + ` then ` + fmt.Sprintf(latestCount, "pull_requests") + ` else a.pull_requests end`,
			},
			extra:    []string{`a.observed_at as content_observed_at`},
			required: []string{"repository", "through", "issues", "pull_requests", "observed_at"},
		},
		{
			name:  "analytics_review_state_coverage_current",
			table: "analytics_review_state_coverage",
			alias: "a",
			joins: `left join analytics_coverage_checks k on k.repository = a.repository and k.coverage = 'analytics_review_state_coverage'`,
			overlay: map[string]string{
				"observed_at": `case when ` + reviewCheck + ` then k.observed_at else a.observed_at end`,
			},
			extra:    []string{`a.observed_at as content_observed_at`},
			required: []string{"repository", "observed_at"},
		},
	}
}

func (s *Store) recordEfficiencyViewSQL(ctx context.Context, spec recordEfficiencyView) (string, bool, error) {
	if !s.hasTable(ctx, spec.table) {
		return "", false, nil
	}
	columns, err := s.tableColumns(ctx, spec.table)
	if err != nil {
		return "", false, err
	}
	present := map[string]bool{}
	for _, column := range columns {
		present[column] = true
	}
	for _, column := range spec.required {
		if !present[column] {
			return "", false, nil
		}
	}
	parts := make([]string, 0, len(columns)+len(spec.extra))
	for _, column := range columns {
		if expr, ok := spec.overlay[column]; ok {
			parts = append(parts, expr+` as `+sqliteQuotedIdentifier(column))
			continue
		}
		parts = append(parts, spec.alias+`.`+sqliteQuotedIdentifier(column))
	}
	parts = append(parts, spec.extra...)
	return `create view ` + spec.name + ` as
select ` + strings.Join(parts, ",\n\t") + `
from ` + spec.table + ` ` + spec.alias + `
` + spec.joins, true, nil
}

func (s *Store) ensureRecordEfficiencyViews(ctx context.Context) error {
	for _, spec := range recordEfficiencyViewSpecs() {
		want, ok, err := s.recordEfficiencyViewSQL(ctx, spec)
		if err != nil {
			return err
		}
		var have sql.NullString
		err = s.q().QueryRowContext(ctx, `select sql from sqlite_schema where type = 'view' and name = ?`, spec.name).Scan(&have)
		if err != nil && err != sql.ErrNoRows {
			return fmt.Errorf("inspect view %s: %w", spec.name, err)
		}
		if ok && have.Valid && have.String == sqliteStoredCreateSQL(want) {
			continue
		}
		if have.Valid {
			if _, err := s.q().ExecContext(ctx, `drop view if exists `+spec.name); err != nil {
				return fmt.Errorf("drop stale view %s: %w", spec.name, err)
			}
		}
		if !ok {
			continue
		}
		if _, err := s.q().ExecContext(ctx, want); err != nil {
			return fmt.Errorf("create view %s: %w", spec.name, err)
		}
	}
	return nil
}

// dropRecordEfficiencyViews removes the compatibility views, which depend on
// check tables that portable stores do not retain.
func dropRecordEfficiencyViews(ctx context.Context, q dbQueries) error {
	for _, name := range recordEfficiencyViews {
		if _, err := q.ExecContext(ctx, `drop view if exists `+name); err != nil {
			return fmt.Errorf("drop view %s: %w", name, err)
		}
	}
	return nil
}

// foldRecordEfficiencyChecks copies the current check values into the content
// rows of a derived copy (portable store or snapshot), then drops the views and
// check tables. The result has the legacy single-row layout with current values.
func (s *Store) foldRecordEfficiencyChecks(ctx context.Context) error {
	if err := s.dropRecordCheckHistory(ctx); err != nil {
		return err
	}
	if err := s.foldRecordEfficiencyIntoContent(ctx); err != nil {
		return err
	}
	if err := dropRecordEfficiencyViews(ctx, s.q()); err != nil {
		return err
	}
	for _, table := range append([]string{"record_efficiency_state"}, recordEfficiencyTables...) {
		if _, err := s.q().ExecContext(ctx, `drop table if exists `+table); err != nil {
			return fmt.Errorf("drop %s: %w", table, err)
		}
	}
	s.recordChecks = false
	return nil
}

// foldRecordEfficiencyIntoContent copies the current values exposed by the
// compatibility views into the content rows' legacy columns. Rows whose values
// already match are not written.
func (s *Store) foldRecordEfficiencyIntoContent(ctx context.Context) error {
	type fold struct {
		view, table, key string
		columns          []string
	}
	folds := []fold{
		{"threads_current", "threads", "id", []string{"first_pulled_at", "last_pulled_at", "observation_sequence", "evidence_observation_sequence", "evidence_source_updated_at", "updated_at"}},
		{"pull_request_details_current", "pull_request_details", "thread_id", []string{"fetched_at", "mergeable_state"}},
		{"repositories_current", "repositories", "id", []string{"updated_at"}},
		{"analytics_coverage_current", "analytics_coverage", "repository", []string{"through", "issues", "pull_requests", "observed_at"}},
		{"analytics_review_state_coverage_current", "analytics_review_state_coverage", "repository", []string{"observed_at"}},
	}
	for _, f := range folds {
		if !s.hasView(ctx, f.view) {
			continue
		}
		assignments := make([]string, 0, len(f.columns))
		differs := make([]string, 0, len(f.columns))
		for _, column := range f.columns {
			assignments = append(assignments, column+` = v.`+column)
			differs = append(differs, f.table+`.`+column+` is not v.`+column)
		}
		if _, err := s.q().ExecContext(ctx, `update `+f.table+` set `+strings.Join(assignments, ", ")+`
			from `+f.view+` v where v.`+f.key+` = `+f.table+`.`+f.key+` and (`+strings.Join(differs, " or ")+`)`); err != nil {
			return fmt.Errorf("fold %s into %s: %w", f.view, f.table, err)
		}
	}
	if s.hasView(ctx, "pull_request_review_threads_current") {
		if _, err := s.q().ExecContext(ctx, `update pull_request_review_threads set fetched_at = v.fetched_at
			from pull_request_review_threads_current v
			where v.thread_id = pull_request_review_threads.thread_id
				and v.review_thread_id = pull_request_review_threads.review_thread_id
				and pull_request_review_threads.fetched_at is not v.fetched_at`); err != nil {
			return fmt.Errorf("fold review thread fetches: %w", err)
		}
	}
	return nil
}

// --- normalization ---------------------------------------------------------

// threadContentRaw returns the thread envelope used for content comparison.
// Merge state is an event series, never thread content.
func threadContentRaw(raw string) string {
	return normalizeRawJSON(raw, nil, map[string][]string{"_graphql": {"mergeable", "mergeStateStatus"}})
}

// pullDetailContentRaw returns the PR detail envelope used for content
// comparison: merge state and the provider activity clock are excluded.
func pullDetailContentRaw(raw string) string {
	return normalizeRawJSON(raw,
		[]string{"mergeable", "mergeable_state", "updated_at"},
		map[string][]string{"_graphql": {"mergeable", "mergeStateStatus", "updatedAt"}})
}

// repositoryContentRaw excludes provider activity clocks and counters from a
// repository envelope. Exact counter observations remain in retained evidence.
func repositoryContentRaw(raw string) string {
	return normalizeRawJSON(raw,
		[]string{"updated_at", "pushed_at", "stargazers_count", "watchers_count", "watchers", "forks_count", "forks", "open_issues_count", "open_issues", "size", "network_count", "subscribers_count"},
		map[string][]string{"_graphql": {"updatedAt", "pushedAt", "stargazerCount", "forkCount"}})
}

// repositoryProviderUpdatedAt returns the provider activity clock recorded in
// repository_checks.
func repositoryProviderUpdatedAt(raw string) string {
	var payload map[string]any
	if json.Unmarshal([]byte(raw), &payload) != nil {
		return ""
	}
	if g, ok := payload["_graphql"].(map[string]any); ok {
		if value, ok := g["updatedAt"].(string); ok {
			return value
		}
	}
	if value, ok := payload["updated_at"].(string); ok {
		return value
	}
	return ""
}

func normalizeRawJSON(raw string, topLevel []string, nested map[string][]string) string {
	if strings.TrimSpace(raw) == "" {
		return raw
	}
	var payload map[string]any
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&payload); err != nil || payload == nil {
		return raw
	}
	for _, key := range topLevel {
		delete(payload, key)
	}
	for parent, keys := range nested {
		child, ok := payload[parent].(map[string]any)
		if !ok {
			continue
		}
		copied := make(map[string]any, len(child))
		for key, value := range child {
			copied[key] = value
		}
		for _, key := range keys {
			delete(copied, key)
		}
		payload[parent] = copied
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return raw
	}
	return string(encoded)
}

// pullDetailThreadOwnedFields are projection fields owned by the thread row.
var pullDetailThreadOwnedFields = []string{"body", "title", "user", "author_association", "html_url", "created_at", "updated_at"}

// GraphQLPullRequestEnvelopeRef marks a PR detail whose full GraphQL node is
// stored once in the same thread's threads.raw_json under "_graphql".
const GraphQLPullRequestEnvelopeRef = "threads.raw_json#/_graphql"

// slimPullDetailRaw removes the GraphQL PR node duplicated from the thread
// envelope. REST payloads and payloads without a GraphQL node are unchanged.
func slimPullDetailRaw(raw string) string {
	var payload map[string]any
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&payload); err != nil || payload == nil {
		return raw
	}
	node, ok := payload["_graphql"].(map[string]any)
	if !ok || payload["_gitcrawl_source"] != "graphql" {
		return raw
	}
	own := map[string]any{}
	// Closed exclusions: unknown provider fields always remain meaningful.
	for key, value := range node {
		own[key] = value
	}
	for _, key := range []string{"body", "title", "author", "authorAssociation", "url", "createdAt", "updatedAt", "comments", "reviews", "reviewThreads", "labels", "assignees", "mergeable", "mergeStateStatus"} {
		delete(own, key)
	}
	delete(payload, "_graphql")
	for _, key := range pullDetailThreadOwnedFields {
		delete(payload, key)
	}
	payload["_graphql_pr"] = own
	payload["_graphql_ref"] = GraphQLPullRequestEnvelopeRef
	encoded, err := json.Marshal(payload)
	if err != nil {
		return raw
	}
	return string(encoded)
}

// --- merge state events ----------------------------------------------------

type mergeStateObservation struct {
	source           string
	mergeable        sql.NullString
	mergeStateStatus sql.NullString
	mergeableState   sql.NullString
}

func jsonScalarString(value any) sql.NullString {
	switch v := value.(type) {
	case nil:
		return sql.NullString{}
	case string:
		return sql.NullString{String: v, Valid: true}
	case bool:
		if v {
			return sql.NullString{String: "true", Valid: true}
		}
		return sql.NullString{String: "false", Valid: true}
	case json.Number:
		return sql.NullString{String: v.String(), Valid: true}
	default:
		encoded, err := json.Marshal(v)
		if err != nil {
			return sql.NullString{}
		}
		return sql.NullString{String: string(encoded), Valid: true}
	}
}

// mergeStateFromRaw extracts the merge-state triple from a thread or PR
// detail envelope. GraphQL nodes derive mergeable_state from mergeStateStatus
// exactly as the PR projection does, so a thread and its PR detail record the
// same triple.
func mergeStateFromRaw(raw string) (mergeStateObservation, bool) {
	var payload map[string]any
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&payload); err != nil || payload == nil {
		return mergeStateObservation{}, false
	}
	if node, ok := payload["_graphql"].(map[string]any); ok {
		_, hasMergeable := node["mergeable"]
		status, hasStatus := node["mergeStateStatus"]
		if !hasMergeable && !hasStatus {
			return mergeStateObservation{}, false
		}
		observation := mergeStateObservation{
			source:           "graphql",
			mergeable:        jsonScalarString(node["mergeable"]),
			mergeStateStatus: jsonScalarString(status),
		}
		if observation.mergeStateStatus.Valid {
			observation.mergeableState = sql.NullString{String: strings.ToLower(observation.mergeStateStatus.String), Valid: true}
		}
		return observation, true
	}
	if payload["_gitcrawl_source"] == "graphql" {
		return mergeStateObservation{}, false
	}
	_, hasMergeable := payload["mergeable"]
	_, hasState := payload["mergeable_state"]
	if !hasMergeable && !hasState {
		return mergeStateObservation{}, false
	}
	return mergeStateObservation{
		source:         "rest",
		mergeable:      jsonScalarString(payload["mergeable"]),
		mergeableState: jsonScalarString(payload["mergeable_state"]),
	}, true
}

// recordMergeState appends a merge-state event when the triple differs from the
// latest event across sources. It returns whether an event was inserted.
func (s *Store) recordMergeState(ctx context.Context, threadID int64, raw, observedAt string) (bool, error) {
	if !s.recordChecks || threadID <= 0 {
		return false, nil
	}
	observation, ok := mergeStateFromRaw(raw)
	if !ok {
		return false, nil
	}
	result, err := s.q().ExecContext(ctx, `
		insert into pull_request_merge_state_events(thread_id, source, observed_at, mergeable, merge_state_status, mergeable_state)
		select ?1, ?2, ?3, ?4, ?5, ?6
		where not exists (
			select 1 from pull_request_merge_state_events e
			where e.id = (
				select max(latest.id) from pull_request_merge_state_events latest
				where latest.thread_id = ?1
			)
				and e.source is ?2
				and e.mergeable is ?4
				and e.merge_state_status is ?5
				and e.mergeable_state is ?6
		)
	`, threadID, observation.source, observedAt, observation.mergeable, observation.mergeStateStatus, observation.mergeableState)
	if err != nil {
		return false, fmt.Errorf("record merge state: %w", err)
	}
	return rowsAffected(result) > 0, nil
}

// --- backfill --------------------------------------------------------------

// RecordEfficiencyBackfill reports rows created by BackfillRecordEfficiency.
type RecordEfficiencyBackfill struct {
	ThreadChecks         int64 `json:"thread_checks"`
	DetailFetches        int64 `json:"pull_request_detail_fetches"`
	MergeStateEvents     int64 `json:"pull_request_merge_state_events"`
	RepositoryChecks     int64 `json:"repository_checks"`
	CoverageChecks       int64 `json:"analytics_coverage_checks"`
	CoverageCountRows    int64 `json:"analytics_coverage_count_samples"`
	ReviewCoverageChecks int64 `json:"analytics_review_state_coverage_checks"`
}

type backfillStep struct {
	target *int64
	sql    string
}

// BackfillRecordEfficiency seeds the check and event tables from the current
// content rows. It never modifies content rows and never overwrites an
// existing check row, so it is idempotent and safe beside a writer. Merge-state
// history before the split exists only in downstream observations; the seed
// records the current value once per pull request.
func (s *Store) BackfillRecordEfficiency(ctx context.Context) (RecordEfficiencyBackfill, error) {
	var out RecordEfficiencyBackfill
	if !s.recordChecks {
		return out, fmt.Errorf("record checks are not enabled on this store")
	}
	err := s.WithTx(ctx, func(tx *Store) error {
		out = RecordEfficiencyBackfill{}
		steps := []backfillStep{
			{&out.ThreadChecks, `insert or ignore into thread_checks(
				thread_id, first_pulled_at, last_pulled_at, observation_sequence, evidence_complete,
				evidence_observation_sequence, evidence_source_updated_at, checked_at)
				select id, first_pulled_at, last_pulled_at, abs(observation_sequence), observation_sequence > 0,
					evidence_observation_sequence, evidence_source_updated_at, updated_at
				from threads`},
			{&out.DetailFetches, `insert or ignore into pull_request_detail_fetches(thread_id, fetched_at)
				select thread_id, fetched_at from pull_request_details`},
			{&out.MergeStateEvents, `insert into pull_request_merge_state_events(thread_id, source, observed_at, mergeable, merge_state_status, mergeable_state)
				select d.thread_id,
					case when json_type(d.raw_json, '$._graphql') = 'object' then 'graphql' else 'rest' end,
					d.fetched_at,
					case when json_type(d.raw_json, '$._graphql') = 'object'
						then json_extract(d.raw_json, '$._graphql.mergeable')
						else case json_type(d.raw_json, '$.mergeable') when 'true' then 'true' when 'false' then 'false' else json_extract(d.raw_json, '$.mergeable') end
					end,
					case when json_type(d.raw_json, '$._graphql') = 'object' then coalesce(upper(d.mergeable_state),json_extract(d.raw_json,'$._graphql.mergeStateStatus')) end,
					case when json_type(d.raw_json, '$._graphql') = 'object'
						then coalesce(d.mergeable_state,lower(json_extract(d.raw_json, '$._graphql.mergeStateStatus')))
						else coalesce(d.mergeable_state,json_extract(d.raw_json, '$.mergeable_state')) end
				from pull_request_details d
				where json_valid(d.raw_json)
					and (json_type(d.raw_json, '$._graphql.mergeStateStatus') is not null
						or json_type(d.raw_json, '$._graphql.mergeable') is not null
						or (json_type(d.raw_json, '$._graphql') is null
							and (json_type(d.raw_json, '$.mergeable_state') is not null or json_type(d.raw_json, '$.mergeable') is not null)))
					and not exists (select 1 from pull_request_merge_state_events e where e.id=(select max(latest.id) from pull_request_merge_state_events latest where latest.thread_id=d.thread_id) and (julianday(e.observed_at)>=julianday(d.fetched_at) or e.mergeable_state is d.mergeable_state))`},
			{&out.RepositoryChecks, `insert or ignore into repository_checks(repo_id, checked_at, provider_updated_at)
				select id, updated_at, coalesce(case when json_valid(raw_json) then coalesce(json_extract(raw_json, '$._graphql.updatedAt'), json_extract(raw_json, '$.updated_at')) end, '')
				from repositories`},
		}
		if tx.hasTable(ctx, "analytics_coverage") {
			steps = append(steps, backfillStep{&out.CoverageChecks, `insert or ignore into analytics_coverage_checks(repository, coverage, observed_at, through)
					select repository, 'analytics_coverage', observed_at, through from analytics_coverage`},
				backfillStep{&out.CoverageCountRows, `insert or ignore into analytics_coverage_count_samples(repository, observed_at, issues, pull_requests)
					select repository, observed_at, issues, pull_requests from analytics_coverage a
					where not exists (select 1 from analytics_coverage_count_samples n where n.repository = a.repository and julianday(n.observed_at)>=julianday(a.observed_at))`})
		}
		if tx.hasTable(ctx, "analytics_review_state_coverage") {
			steps = append(steps, backfillStep{&out.ReviewCoverageChecks, `insert or ignore into analytics_coverage_checks(repository, coverage, observed_at, through)
				select repository, 'analytics_review_state_coverage', observed_at, null from analytics_review_state_coverage`})
		}
		for _, step := range steps {
			result, err := tx.q().ExecContext(ctx, step.sql)
			if err != nil {
				return fmt.Errorf("backfill record checks: %w", err)
			}
			*step.target = rowsAffected(result)
		}
		_, err := tx.q().ExecContext(ctx, `update thread_checks set evidence_source_order_key=coalesce(gitcrawl_timestamp_key(nullif(evidence_source_updated_at,'')),''),content_evidence_source_order_key=coalesce((select gitcrawl_timestamp_key(nullif(t.evidence_source_updated_at,'')) from threads t where t.id=thread_checks.thread_id),'') where evidence_source_order_key is not coalesce(gitcrawl_timestamp_key(nullif(evidence_source_updated_at,'')),'') or content_evidence_source_order_key is not coalesce((select gitcrawl_timestamp_key(nullif(t.evidence_source_updated_at,'')) from threads t where t.id=thread_checks.thread_id),'')`)
		return err
	})
	return out, err
}

// RecordEfficiencyStatus is the read-only view of the split layout.
type RecordEfficiencyStatus struct {
	Enabled bool             `json:"enabled"`
	Version int64            `json:"version,omitempty"`
	Tables  map[string]int64 `json:"tables,omitempty"`
	Views   []string         `json:"views,omitempty"`
	// ThreadsWithoutChecks counts threads whose bookkeeping still comes from
	// the content row; backfill seeds them.
	ThreadsWithoutChecks int64 `json:"threads_without_checks"`
}

// RecordEfficiencyStatus reports the check tables, their row counts and the
// compatibility views.
func (s *Store) RecordEfficiencyStatus(ctx context.Context) (RecordEfficiencyStatus, error) {
	status := RecordEfficiencyStatus{Tables: map[string]int64{}}
	for _, table := range recordEfficiencyTables {
		if !s.hasTable(ctx, table) {
			continue
		}
		var n int64
		if err := s.q().QueryRowContext(ctx, `select count(*) from `+table).Scan(&n); err != nil {
			return status, fmt.Errorf("count %s: %w", table, err)
		}
		status.Tables[table] = n
	}
	for _, view := range recordEfficiencyViews {
		if s.hasView(ctx, view) {
			status.Views = append(status.Views, view)
		}
	}
	status.Enabled = s.recordEfficiencyActive(ctx) && len(status.Tables) == len(recordEfficiencyTables)
	if status.Enabled {
		_ = s.q().QueryRowContext(ctx, `select version from record_efficiency_state where id = 1`).Scan(&status.Version)
	}
	if status.Enabled {
		if err := s.q().QueryRowContext(ctx, `select count(*) from threads t where not exists (select 1 from thread_checks c where c.thread_id = t.id)`).Scan(&status.ThreadsWithoutChecks); err != nil {
			return status, fmt.Errorf("count threads without checks: %w", err)
		}
	}
	return status, nil
}

// StripRecordEfficiency folds and removes private split state from a derived
// publication transaction. It never targets the source archive.
func StripRecordEfficiency(ctx context.Context, tx *sql.Tx) error {
	st := &Store{queries: tx}
	return st.foldRecordEfficiencyChecks(ctx)
}
