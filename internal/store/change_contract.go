package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

// The change contract lets a read-only consumer decide cheaply and exactly
// whether the rows of selected archive tables changed since a recorded
// cursor. SQLite triggers record every insert, update and delete, including
// UPSERT updates and foreign-key cascades, in the same transaction as the
// change. Because triggers live in the database file, every writer records
// changes, including pinned older gitcrawl binaries that do not know about
// the contract. Rewrites the triggers cannot observe drop the triggers or
// change the schema cookie; readers must treat either as a full rebuild, and
// the next writable open re-arms the tables and advances the generation.
//
// Contract version 2 keeps the version 1 objects and log format and adds one
// guarantee: an UPDATE that leaves every column of a tracked row unchanged is
// not journaled. Its update triggers carry a WHEN guard over all columns, so a
// version 2 trigger set must be re-armed whenever a tracked table gains a
// column. Version 1 remains the default for new contracts; an existing
// contract moves between versions only through an explicit enable.
const (
	ChangeContractVersion          = 1
	ChangeContractVersionGuarded   = 2
	ChangeContractLatestVersion    = ChangeContractVersionGuarded
	DefaultChangeContractRetention = int64(1_000_000)
	MinChangeContractRetention     = int64(changeContractPruneInterval)
	changeContractPruneInterval    = 1024
	changeContractTriggerPrefix    = "change_contract_"
)

// ChangeContractMode selects whether a table records row keys.
const (
	ChangeContractModeRows  = "rows"
	ChangeContractModeTable = "table"
)

// Log operation codes. Readers re-read the current row by key; a missing row
// means the key was deleted. An update that changes the key records a delete
// of the old key followed by an insert of the new key.
const (
	ChangeContractOpInsert = 1
	ChangeContractOpUpdate = 2
	ChangeContractOpDelete = 3
)

type changeContractTableSpec struct {
	id   int64
	name string
	mode string
}

// Table identifiers are permanent. Append new tables with new identifiers;
// never renumber or reuse an identifier.
var changeContractTableSpecs = []changeContractTableSpec{
	{1, "repositories", ChangeContractModeRows},
	{2, "threads", ChangeContractModeRows},
	{3, "thread_revisions", ChangeContractModeRows},
	{4, "blobs", ChangeContractModeRows},
	{5, "pull_request_review_threads", ChangeContractModeRows},
	{6, "thread_fingerprints", ChangeContractModeRows},
	{7, "thread_key_summaries", ChangeContractModeRows},
	{8, "document_summaries", ChangeContractModeRows},
	{9, "thread_child_observation_reservations", ChangeContractModeRows},
	{10, "thread_exclusions", ChangeContractModeRows},
	{11, "pull_request_details", ChangeContractModeRows},
	{12, "pull_request_files", ChangeContractModeTable},
	{13, "pull_request_commits", ChangeContractModeTable},
	{14, "pull_request_checks", ChangeContractModeTable},
	{15, "comments", ChangeContractModeTable},
	{16, "github_workflow_runs", ChangeContractModeTable},
	{17, "sync_attempt_failures", ChangeContractModeRows},
	{18, "sync_runs", ChangeContractModeRows},
	{19, "summary_runs", ChangeContractModeRows},
	{20, "repo_sync_state", ChangeContractModeRows},
	{21, "portable_metadata", ChangeContractModeRows},
	{22, "analytics_coverage", ChangeContractModeRows},
	{23, "analytics_review_state_coverage", ChangeContractModeRows},
	{24, "cluster_runs", ChangeContractModeRows},
	{25, "clusters", ChangeContractModeRows},
	{26, "cluster_members", ChangeContractModeRows},
	{27, "cluster_groups", ChangeContractModeRows},
	{28, "cluster_memberships", ChangeContractModeRows},
	{29, "cluster_overrides", ChangeContractModeRows},
	{30, "cluster_closures", ChangeContractModeRows},
	{31, "cluster_candidate_generations", ChangeContractModeRows},
	{32, "cluster_candidates", ChangeContractModeRows},
	{33, "cluster_candidate_members", ChangeContractModeRows},
	{34, "cluster_candidate_overlaps", ChangeContractModeRows},
	{35, "cluster_candidate_selection", ChangeContractModeRows},
	{36, "code_snapshots", ChangeContractModeRows},
	{37, "code_documents", ChangeContractModeTable},
	{38, "thread_checks", ChangeContractModeRows},
	{39, "pull_request_detail_fetches", ChangeContractModeRows},
	{40, "pull_request_merge_state_events", ChangeContractModeRows},
	{41, "repository_checks", ChangeContractModeRows},
	{42, "analytics_coverage_checks", ChangeContractModeRows},
	{43, "analytics_coverage_count_samples", ChangeContractModeRows},
	{44, "pull_request_review_thread_syncs", ChangeContractModeRows},
	{45, "record_evidence_blobs", ChangeContractModeRows},
	{46, "record_check_observations", ChangeContractModeRows},
}

// Families name the tables each Clawmmunity reader family reads. A consumer
// must still verify that every table it reads is armed; families are a
// source-declared convenience, not authority over the consumer's read set.
var changeContractFamilies = map[string][]string{
	"reader.coverage": {
		"repositories", "threads", "thread_revisions", "thread_fingerprints",
		"thread_key_summaries", "thread_child_observation_reservations",
		"cluster_runs", "cluster_groups", "cluster_memberships",
		"pull_request_details", "pull_request_files", "comments",
		"pull_request_commits", "pull_request_checks",
		"pull_request_review_threads", "github_workflow_runs",
		"sync_attempt_failures", "sync_runs", "summary_runs", "repo_sync_state",
		"portable_metadata", "thread_exclusions", "analytics_coverage",
		"analytics_review_state_coverage",
	},
	"reader.clusters": {
		"repositories", "threads", "thread_revisions", "thread_key_summaries",
		"document_summaries", "cluster_runs", "clusters", "cluster_members",
		"cluster_groups", "cluster_memberships", "cluster_overrides",
		"cluster_closures", "cluster_candidate_generations", "cluster_candidates",
		"cluster_candidate_members", "cluster_candidate_overlaps",
		"cluster_candidate_selection", "thread_exclusions",
	},
	"reader.code": {
		"repositories", "code_snapshots", "code_documents", "thread_exclusions",
	},
	"reader.checks": {
		"thread_checks", "pull_request_detail_fetches", "pull_request_review_thread_syncs",
		"repository_checks", "analytics_coverage_checks", "record_evidence_blobs", "record_check_observations",
	},
	"history.pull_request_merge_state_events":  {"pull_request_merge_state_events"},
	"history.analytics_coverage_count_samples": {"analytics_coverage_count_samples"},
	"history.blobs":                       {"blobs"},
	"history.pull_request_review_threads": {"pull_request_review_threads"},
	"history.thread_revisions":            {"thread_revisions"},
}

const changeContractSchemaSQL = `
create table if not exists change_contract_state(
	id integer primary key check(id = 1),
	contract_version integer not null,
	epoch text not null,
	generation integer not null check(generation >= 1),
	generation_reason text not null,
	generation_at text not null,
	enabled_at text not null
);
create table if not exists change_contract_sequence(
	id integer primary key check(id = 1),
	value integer not null,
	retention integer not null check(retention >= 1024),
	next_prune integer not null,
	pruned_through integer not null
);
create table if not exists change_contract_tables(
	table_id integer primary key,
	table_name text not null unique,
	mode text not null check(mode in ('rows', 'table')),
	key_columns text not null,
	last_seq integer not null default 0,
	insert_trigger text not null,
	update_trigger text not null,
	delete_trigger text not null
);
create table if not exists change_contract_families(
	family text not null,
	table_name text not null,
	primary key(family, table_name)
) without rowid;
create table if not exists change_contract_log(
	seq integer primary key,
	table_id integer not null,
	op integer not null,
	row_key
);
`

const changeContractPruneTriggerSQL = `create trigger change_contract_log_prune
after insert on change_contract_log
when NEW.seq >= (select next_prune from change_contract_sequence where id = 1)
begin
	delete from change_contract_log
	where seq <= NEW.seq - (select retention from change_contract_sequence where id = 1);
	update change_contract_sequence
	set next_prune = NEW.seq + 1024,
		pruned_through = max(pruned_through, NEW.seq - retention)
	where id = 1;
end`

var changeContractObjectTables = []string{
	"change_contract_log",
	"change_contract_families",
	"change_contract_tables",
	"change_contract_sequence",
	"change_contract_state",
}

// ChangeContractStatus is the read-only view of the contract.
type ChangeContractStatus struct {
	Enabled         bool                        `json:"enabled"`
	ContractVersion int64                       `json:"contract_version,omitempty"`
	Epoch           string                      `json:"epoch,omitempty"`
	Generation      int64                       `json:"generation,omitempty"`
	GenerationAt    string                      `json:"generation_at,omitempty"`
	GenerationWhy   string                      `json:"generation_reason,omitempty"`
	EnabledAt       string                      `json:"enabled_at,omitempty"`
	SchemaVersion   int64                       `json:"schema_version"`
	Sequence        int64                       `json:"sequence"`
	Retention       int64                       `json:"retention,omitempty"`
	PrunedThrough   int64                       `json:"pruned_through"`
	LogEntries      int64                       `json:"log_entries"`
	Armed           bool                        `json:"armed"`
	Tables          []ChangeContractTableStatus `json:"tables,omitempty"`
	Families        []ChangeContractFamily      `json:"families,omitempty"`
}

type ChangeContractTableStatus struct {
	TableID    int64    `json:"table_id"`
	Table      string   `json:"table"`
	Mode       string   `json:"mode"`
	KeyColumns []string `json:"key_columns"`
	LastSeq    int64    `json:"last_seq"`
	Present    bool     `json:"present"`
	Armed      bool     `json:"armed"`
}

type ChangeContractFamily struct {
	Family  string   `json:"family"`
	Tables  []string `json:"tables"`
	LastSeq int64    `json:"last_seq"`
	Armed   bool     `json:"armed"`
}

// ChangeContractEnableOptions configures EnableChangeContract.
type ChangeContractEnableOptions struct {
	Retention int64
	Now       time.Time
	// Version selects the contract version. Zero keeps an existing contract's
	// version and creates new contracts at ChangeContractVersion.
	Version int64
}

// ChangeContractRepair reports what a repair changed.
type ChangeContractRepair struct {
	Enabled    bool     `json:"enabled"`
	Created    bool     `json:"created"`
	Version    int64    `json:"contract_version,omitempty"`
	Upgraded   bool     `json:"version_changed,omitempty"`
	Rearmed    []string `json:"rearmed,omitempty"`
	Generation int64    `json:"generation,omitempty"`
}

type changeContractExec interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// changeContractPresent reports whether any contract object survives, so a
// partial contract is repaired rather than mistaken for an absent one.
func changeContractPresent(ctx context.Context, q changeContractExec) (bool, error) {
	var n int
	if err := q.QueryRowContext(ctx, `select count(*) from sqlite_schema where type in ('table', 'trigger') and name like 'change\_contract\_%' escape '\'`).Scan(&n); err != nil {
		return false, fmt.Errorf("inspect change contract: %w", err)
	}
	return n > 0, nil
}

// EnableChangeContract creates the contract when absent, or re-arms an
// existing contract. Creating a contract starts a new epoch at generation 1.
// Re-arming any table advances the generation.
func (s *Store) EnableChangeContract(ctx context.Context, options ChangeContractEnableOptions) (ChangeContractRepair, error) {
	retention := options.Retention
	if retention == 0 {
		retention = DefaultChangeContractRetention
	}
	if retention < MinChangeContractRetention {
		return ChangeContractRepair{}, fmt.Errorf("change contract retention must be at least %d", MinChangeContractRetention)
	}
	now := options.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if options.Version != 0 && options.Version != ChangeContractVersion && options.Version != ChangeContractVersionGuarded {
		return ChangeContractRepair{}, fmt.Errorf("unsupported change contract version %d", options.Version)
	}
	var result ChangeContractRepair
	err := s.WithTx(ctx, func(tx *Store) error {
		q := tx.q()
		present, err := changeContractPresent(ctx, q)
		if err != nil {
			return err
		}
		if present {
			complete, err := changeContractComplete(ctx, q)
			if err != nil {
				return err
			}
			if !complete {
				// A partial contract cannot prove continuity. Replace it
				// with a new epoch so every reader rebuilds, keeping a
				// surviving retention unless the caller chose another.
				if options.Retention == 0 {
					if kept, ok := changeContractStoredRetention(ctx, q); ok {
						retention = kept
					}
				}
				if options.Version == 0 {
					if kept, ok := changeContractStoredVersionIfPresent(ctx, q); ok {
						options.Version = kept
					}
				}
				if _, err := StripChangeContract(ctx, q); err != nil {
					return err
				}
				present = false
			}
		}
		if !present {
			version := options.Version
			if version == 0 {
				version = ChangeContractVersion
			}
			if err := createChangeContractVersion(ctx, q, retention, now, version); err != nil {
				return err
			}
			result.Created = true
		} else if options.Version != 0 {
			current := changeContractStoredVersion(ctx, q)
			if current != options.Version {
				if _, err := q.ExecContext(ctx, `update change_contract_state set contract_version = ? where id = 1`, options.Version); err != nil {
					return fmt.Errorf("set change contract version: %w", err)
				}
				result.Upgraded = true
			}
		}
		if present && options.Retention != 0 {
			// Apply a changed bound to the existing log immediately.
			if _, err := q.ExecContext(ctx, `
				update change_contract_sequence
				set retention = ?1,
					next_prune = value + 1024,
					pruned_through = max(pruned_through, value - ?1)
				where id = 1`, retention); err != nil {
				return fmt.Errorf("set change contract retention: %w", err)
			}
			if _, err := q.ExecContext(ctx, `delete from change_contract_log where seq <= (select pruned_through from change_contract_sequence where id = 1)`); err != nil {
				return fmt.Errorf("apply change contract retention: %w", err)
			}
		}
		if err := syncChangeContractFamilies(ctx, q); err != nil {
			return err
		}
		rearmed, err := armChangeContract(ctx, q)
		if err != nil {
			return err
		}
		result.Rearmed = rearmed
		result.Version = changeContractStoredVersion(ctx, q)
		if !result.Created && (len(rearmed) > 0 || result.Upgraded) {
			reason := "rearmed: " + strings.Join(rearmed, ",")
			if result.Upgraded {
				reason = fmt.Sprintf("contract version %d; ", result.Version) + reason
			}
			if err := bumpChangeContractGeneration(ctx, q, reason, now); err != nil {
				return err
			}
		}
		result.Enabled = true
		return q.QueryRowContext(ctx, `select generation from change_contract_state where id = 1`).Scan(&result.Generation)
	})
	if err != nil {
		return ChangeContractRepair{}, err
	}
	return result, nil
}

// repairChangeContract re-arms an existing contract after writable opens.
// It never creates a contract. A failure leaves tables unarmed, which readers
// detect and treat as a full rebuild, so it does not fail the open.
func (s *Store) repairChangeContract(ctx context.Context) (ChangeContractRepair, error) {
	present, err := changeContractPresent(ctx, s.q())
	if err != nil || !present {
		return ChangeContractRepair{}, err
	}
	complete, err := changeContractComplete(ctx, s.q())
	if err != nil {
		return ChangeContractRepair{}, err
	}
	armed := false
	if complete {
		if armed, err = changeContractArmed(ctx, s.q()); err != nil {
			return ChangeContractRepair{}, err
		}
	}
	if armed && changeContractStoredVersion(ctx, s.q()) == ChangeContractVersionGuarded {
		if armed, err = changeContractGuardsCurrent(ctx, s.q()); err != nil {
			return ChangeContractRepair{}, err
		}
	}
	if armed {
		return ChangeContractRepair{Enabled: true}, nil
	}
	return s.EnableChangeContract(ctx, ChangeContractEnableOptions{})
}

// BumpChangeContractGeneration forces every reader to rebuild. Use it after
// an owner-run rewrite the triggers could not observe.
func (s *Store) BumpChangeContractGeneration(ctx context.Context, reason string, now time.Time) (int64, error) {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return 0, fmt.Errorf("change contract generation bump requires a reason")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	var generation int64
	err := s.WithTx(ctx, func(tx *Store) error {
		present, err := changeContractPresent(ctx, tx.q())
		if err != nil {
			return err
		}
		if !present {
			return fmt.Errorf("change contract is not enabled")
		}
		if err := bumpChangeContractGeneration(ctx, tx.q(), reason, now); err != nil {
			return err
		}
		return tx.q().QueryRowContext(ctx, `select generation from change_contract_state where id = 1`).Scan(&generation)
	})
	return generation, err
}

// DisableChangeContract removes the contract triggers and tables. Readers
// observe the contract as absent and must rebuild fully.
func (s *Store) DisableChangeContract(ctx context.Context) (bool, error) {
	var removed bool
	err := s.WithTx(ctx, func(tx *Store) error {
		var err error
		removed, err = StripChangeContract(ctx, tx.q())
		return err
	})
	return removed, err
}

// StripChangeContract removes contract objects from a database handle. Derived
// copies such as portable and cloud snapshots call it before rewriting rows.
func StripChangeContract(ctx context.Context, q changeContractExec) (bool, error) {
	rows, err := q.QueryContext(ctx, `select name from sqlite_schema where type = 'trigger' and name like 'change\_contract\_%' escape '\'`)
	if err != nil {
		return false, fmt.Errorf("list change contract triggers: %w", err)
	}
	var triggers []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return false, err
		}
		triggers = append(triggers, name)
	}
	if err := rows.Close(); err != nil {
		return false, err
	}
	removed := len(triggers) > 0
	for _, name := range triggers {
		if _, err := q.ExecContext(ctx, `drop trigger if exists `+sqliteQuotedIdentifier(name)); err != nil {
			return false, fmt.Errorf("drop change contract trigger %s: %w", name, err)
		}
	}
	for _, table := range changeContractObjectTables {
		var n int
		if err := q.QueryRowContext(ctx, `select count(*) from sqlite_schema where type = 'table' and name = ?`, table).Scan(&n); err != nil {
			return false, err
		}
		if n == 0 {
			continue
		}
		removed = true
		if _, err := q.ExecContext(ctx, `drop table `+table); err != nil {
			return false, fmt.Errorf("drop change contract table %s: %w", table, err)
		}
	}
	return removed, nil
}

func changeContractStoredRetention(ctx context.Context, q changeContractExec) (int64, bool) {
	present, err := changeContractTablePresent(ctx, q, "change_contract_sequence")
	if err != nil || !present {
		return 0, false
	}
	var retention int64
	if err := q.QueryRowContext(ctx, `select retention from change_contract_sequence where id = 1`).Scan(&retention); err != nil || retention < MinChangeContractRetention {
		return 0, false
	}
	return retention, true
}

// changeContractComplete reports whether every contract table and singleton
// row exists.
func changeContractComplete(ctx context.Context, q changeContractExec) (bool, error) {
	for _, table := range changeContractObjectTables {
		present, err := changeContractTablePresent(ctx, q, table)
		if err != nil || !present {
			return false, err
		}
	}
	var n int
	if err := q.QueryRowContext(ctx, `select (select count(*) from change_contract_state where id = 1) + (select count(*) from change_contract_sequence where id = 1)`).Scan(&n); err != nil {
		return false, err
	}
	return n == 2, nil
}

func createChangeContract(ctx context.Context, q changeContractExec, retention int64, now time.Time) error {
	return createChangeContractVersion(ctx, q, retention, now, ChangeContractVersion)
}

// changeContractStoredVersion returns the recorded contract version, or
// version 1 when it cannot be read.
func changeContractStoredVersion(ctx context.Context, q changeContractExec) int64 {
	var version int64
	if err := q.QueryRowContext(ctx, `select contract_version from change_contract_state where id = 1`).Scan(&version); err != nil {
		return ChangeContractVersion
	}
	return version
}

func changeContractStoredVersionIfPresent(ctx context.Context, q changeContractExec) (int64, bool) {
	present, err := changeContractTablePresent(ctx, q, "change_contract_state")
	if err != nil || !present {
		return 0, false
	}
	var version int64
	if err := q.QueryRowContext(ctx, `select contract_version from change_contract_state where id = 1`).Scan(&version); err != nil {
		return 0, false
	}
	if version != ChangeContractVersion && version != ChangeContractVersionGuarded {
		return 0, false
	}
	return version, true
}

// syncChangeContractFamilies records the declared families. Families are a
// source-declared convenience, so an existing contract adopts new families.
func syncChangeContractFamilies(ctx context.Context, q changeContractExec) error {
	families := make([]string, 0, len(changeContractFamilies))
	for family := range changeContractFamilies {
		families = append(families, family)
	}
	sort.Strings(families)
	for _, family := range families {
		for _, table := range changeContractFamilies[family] {
			if _, err := q.ExecContext(ctx, `insert or ignore into change_contract_families(family, table_name) values(?, ?)`, family, table); err != nil {
				return fmt.Errorf("record change contract family %s: %w", family, err)
			}
		}
	}
	return nil
}

func createChangeContractVersion(ctx context.Context, q changeContractExec, retention int64, now time.Time, version int64) error {
	if _, err := q.ExecContext(ctx, changeContractSchemaSQL); err != nil {
		return fmt.Errorf("create change contract schema: %w", err)
	}
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return fmt.Errorf("create change contract epoch: %w", err)
	}
	stamp := now.UTC().Format(timeLayout)
	if _, err := q.ExecContext(ctx, `insert into change_contract_state(id, contract_version, epoch, generation, generation_reason, generation_at, enabled_at) values(1, ?, ?, 1, 'enabled', ?, ?)`,
		version, hex.EncodeToString(raw[:]), stamp, stamp); err != nil {
		return fmt.Errorf("create change contract state: %w", err)
	}
	if _, err := q.ExecContext(ctx, `insert into change_contract_sequence(id, value, retention, next_prune, pruned_through) values(1, 0, ?, ?, 0)`, retention, changeContractPruneInterval); err != nil {
		return fmt.Errorf("create change contract sequence: %w", err)
	}
	if _, err := q.ExecContext(ctx, changeContractPruneTriggerSQL); err != nil {
		return fmt.Errorf("create change contract prune trigger: %w", err)
	}
	families := make([]string, 0, len(changeContractFamilies))
	for family := range changeContractFamilies {
		families = append(families, family)
	}
	sort.Strings(families)
	for _, family := range families {
		for _, table := range changeContractFamilies[family] {
			if _, err := q.ExecContext(ctx, `insert into change_contract_families(family, table_name) values(?, ?)`, family, table); err != nil {
				return fmt.Errorf("record change contract family %s: %w", family, err)
			}
		}
	}
	return nil
}

func bumpChangeContractGeneration(ctx context.Context, q changeContractExec, reason string, now time.Time) error {
	if _, err := q.ExecContext(ctx, `update change_contract_state set generation = generation + 1, generation_reason = ?, generation_at = ? where id = 1`, reason, now.UTC().Format(timeLayout)); err != nil {
		return fmt.Errorf("advance change contract generation: %w", err)
	}
	return nil
}

type changeContractTrigger struct {
	name string
	sql  string
}

// armChangeContract installs triggers for every present table whose recorded
// triggers are missing, altered or bound to a different key. It returns the
// re-armed table names.
func armChangeContract(ctx context.Context, q changeContractExec) ([]string, error) {
	var count int
	if err := q.QueryRowContext(ctx, `select count(*) from sqlite_schema where type = 'trigger' and name = 'change_contract_log_prune'`).Scan(&count); err != nil {
		return nil, err
	}
	version := changeContractStoredVersion(ctx, q)
	var rearmed []string
	if count == 0 {
		if _, err := q.ExecContext(ctx, changeContractPruneTriggerSQL); err != nil {
			return nil, fmt.Errorf("restore change contract prune trigger: %w", err)
		}
		rearmed = append(rearmed, "change_contract_log")
	}
	for _, spec := range changeContractTableSpecs {
		present, err := changeContractTablePresent(ctx, q, spec.name)
		if err != nil {
			return nil, err
		}
		if !present {
			continue
		}
		keys, err := changeContractKeyColumns(ctx, q, spec.name)
		if err != nil {
			return nil, err
		}
		keyJSON, _ := json.Marshal(keys)
		triggers, err := changeContractExpectedTriggers(ctx, q, spec, keys, version)
		if err != nil {
			return nil, err
		}
		var recordedKeys, recordedInsert, recordedUpdate, recordedDelete sql.NullString
		err = q.QueryRowContext(ctx, `select key_columns, insert_trigger, update_trigger, delete_trigger from change_contract_tables where table_id = ? and table_name = ? and mode = ?`, spec.id, spec.name, spec.mode).Scan(&recordedKeys, &recordedInsert, &recordedUpdate, &recordedDelete)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
		armed := err == nil && recordedKeys.String == string(keyJSON) &&
			recordedInsert.String == sqliteStoredCreateSQL(triggers[0].sql) &&
			recordedUpdate.String == sqliteStoredCreateSQL(triggers[1].sql) &&
			recordedDelete.String == sqliteStoredCreateSQL(triggers[2].sql)
		if armed {
			armed, err = changeContractTableArmed(ctx, q, spec.name)
			if err != nil {
				return nil, err
			}
		}
		if armed {
			continue
		}
		for _, trigger := range triggers {
			if _, err := q.ExecContext(ctx, `drop trigger if exists `+sqliteQuotedIdentifier(trigger.name)); err != nil {
				return nil, fmt.Errorf("drop stale change contract trigger %s: %w", trigger.name, err)
			}
			if _, err := q.ExecContext(ctx, trigger.sql); err != nil {
				return nil, fmt.Errorf("create change contract trigger %s: %w", trigger.name, err)
			}
		}
		stored := make([]string, len(triggers))
		for i, trigger := range triggers {
			if err := q.QueryRowContext(ctx, `select sql from sqlite_schema where type = 'trigger' and name = ?`, trigger.name).Scan(&stored[i]); err != nil {
				return nil, fmt.Errorf("read change contract trigger %s: %w", trigger.name, err)
			}
		}
		if _, err := q.ExecContext(ctx, `
			insert into change_contract_tables(table_id, table_name, mode, key_columns, last_seq, insert_trigger, update_trigger, delete_trigger)
			values(?, ?, ?, ?, (select value from change_contract_sequence where id = 1), ?, ?, ?)
			on conflict(table_id) do update set
				table_name = excluded.table_name,
				mode = excluded.mode,
				key_columns = excluded.key_columns,
				last_seq = excluded.last_seq,
				insert_trigger = excluded.insert_trigger,
				update_trigger = excluded.update_trigger,
				delete_trigger = excluded.delete_trigger
		`, spec.id, spec.name, spec.mode, string(keyJSON), stored[0], stored[1], stored[2]); err != nil {
			return nil, fmt.Errorf("record change contract table %s: %w", spec.name, err)
		}
		rearmed = append(rearmed, spec.name)
	}
	return rearmed, nil
}

func changeContractTablePresent(ctx context.Context, q changeContractExec, table string) (bool, error) {
	var n int
	if err := q.QueryRowContext(ctx, `select count(*) from sqlite_schema where type = 'table' and name = ?`, table).Scan(&n); err != nil {
		return false, fmt.Errorf("inspect table %s: %w", table, err)
	}
	return n > 0, nil
}

// changeContractArmedSQL is the exact reader check: zero rows means every
// present tracked table carries its three recorded triggers verbatim.
const changeContractUnarmedSQL = `
select t.table_name
from change_contract_tables t
where exists(select 1 from sqlite_schema m where m.type = 'table' and m.name = t.table_name)
	and (select count(*) from sqlite_schema s
		where s.type = 'trigger' and s.tbl_name = t.table_name
			and s.sql in (t.insert_trigger, t.update_trigger, t.delete_trigger)) <> 3`

func changeContractTableArmed(ctx context.Context, q changeContractExec, table string) (bool, error) {
	var n int
	if err := q.QueryRowContext(ctx, `
		select count(*) from change_contract_tables t
		join sqlite_schema s on s.type = 'trigger' and s.tbl_name = t.table_name
			and s.sql in (t.insert_trigger, t.update_trigger, t.delete_trigger)
		where t.table_name = ?`, table).Scan(&n); err != nil {
		return false, err
	}
	return n == 3, nil
}

func changeContractArmed(ctx context.Context, q changeContractExec) (bool, error) {
	var prune int
	if err := q.QueryRowContext(ctx, `select count(*) from sqlite_schema where type = 'trigger' and name = 'change_contract_log_prune'`).Scan(&prune); err != nil {
		return false, err
	}
	if prune == 0 {
		return false, nil
	}
	var unarmed sql.NullString
	err := q.QueryRowContext(ctx, changeContractUnarmedSQL+` limit 1`).Scan(&unarmed)
	if errors.Is(err, sql.ErrNoRows) {
		// Present tracked tables created after the last arm have no row yet.
		for _, spec := range changeContractTableSpecs {
			present, err := changeContractTablePresent(ctx, q, spec.name)
			if err != nil {
				return false, err
			}
			if !present {
				continue
			}
			var n int
			if err := q.QueryRowContext(ctx, `select count(*) from change_contract_tables where table_name = ?`, spec.name).Scan(&n); err != nil {
				return false, err
			}
			if n == 0 {
				return false, nil
			}
		}
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return false, nil
}

func changeContractKeyColumns(ctx context.Context, q changeContractExec, table string) ([]string, error) {
	rows, err := q.QueryContext(ctx, `select name, pk from pragma_table_info(?) where pk > 0 order by pk`, table)
	if err != nil {
		return nil, fmt.Errorf("inspect %s key: %w", table, err)
	}
	defer rows.Close()
	keys := []string{}
	for rows.Next() {
		var name string
		var pk int
		if err := rows.Scan(&name, &pk); err != nil {
			return nil, err
		}
		keys = append(keys, name)
	}
	return keys, rows.Err()
}

func sqliteQuotedIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

// changeContractKeySQL renders the logged key for a row reference. A single
// key column is logged as its value; a composite key as a JSON array; a table
// without a declared key as its rowid. BLOB key cells, which the archive never
// stores, are logged as their SQL literal so a write is never rejected.
func changeContractKeySQL(ref string, keys []string) string {
	cell := func(column string) string {
		c := ref + "." + sqliteQuotedIdentifier(column)
		return "case typeof(" + c + ") when 'blob' then quote(" + c + ") else " + c + " end"
	}
	switch len(keys) {
	case 0:
		return ref + ".rowid"
	case 1:
		return cell(keys[0])
	default:
		cells := make([]string, len(keys))
		for i, key := range keys {
			cells[i] = cell(key)
		}
		return "json_array(" + strings.Join(cells, ", ") + ")"
	}
}

func changeContractKeyChangedSQL(keys []string) string {
	if len(keys) == 0 {
		return "(OLD.rowid is not NEW.rowid)"
	}
	parts := make([]string, len(keys))
	for i, key := range keys {
		c := sqliteQuotedIdentifier(key)
		parts[i] = "OLD." + c + " is not NEW." + c
	}
	return "(" + strings.Join(parts, " or ") + ")"
}

// changeContractExpectedTriggers renders the triggers a table should carry at
// the given contract version.
func changeContractExpectedTriggers(ctx context.Context, q changeContractExec, spec changeContractTableSpec, keys []string, version int64) ([]changeContractTrigger, error) {
	if version != ChangeContractVersionGuarded {
		return changeContractTriggers(spec, keys), nil
	}
	columns, err := changeContractColumns(ctx, q, spec.name)
	if err != nil {
		return nil, err
	}
	return changeContractGuardedTriggers(spec, keys, columns), nil
}

// changeContractGuardsCurrent reports whether every armed version 2 update
// trigger still guards the table's current column set.
func changeContractGuardsCurrent(ctx context.Context, q changeContractExec) (bool, error) {
	for _, spec := range changeContractTableSpecs {
		present, err := changeContractTablePresent(ctx, q, spec.name)
		if err != nil {
			return false, err
		}
		if !present {
			continue
		}
		keys, err := changeContractKeyColumns(ctx, q, spec.name)
		if err != nil {
			return false, err
		}
		triggers, err := changeContractExpectedTriggers(ctx, q, spec, keys, ChangeContractVersionGuarded)
		if err != nil {
			return false, err
		}
		var recorded sql.NullString
		err = q.QueryRowContext(ctx, `select update_trigger from change_contract_tables where table_id = ?`, spec.id).Scan(&recorded)
		if errors.Is(err, sql.ErrNoRows) || err == nil && recorded.String != sqliteStoredCreateSQL(triggers[1].sql) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
	}
	return true, nil
}

// sqliteStoredCreateSQL renders a CREATE statement as SQLite stores it in
// sqlite_schema: the leading CREATE keyword and object type are upper-cased.
func sqliteStoredCreateSQL(statement string) string {
	for _, kind := range []string{"trigger", "view", "table", "index"} {
		prefix := "create " + kind + " "
		if strings.HasPrefix(strings.ToLower(statement), prefix) {
			return "CREATE " + strings.ToUpper(kind) + " " + statement[len(prefix):]
		}
	}
	return statement
}

func changeContractColumns(ctx context.Context, q changeContractExec, table string) ([]string, error) {
	rows, err := q.QueryContext(ctx, `select name from pragma_table_info(?) order by cid`, table)
	if err != nil {
		return nil, fmt.Errorf("inspect %s columns: %w", table, err)
	}
	defer rows.Close()
	columns := []string{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		columns = append(columns, name)
	}
	return columns, rows.Err()
}

// changeContractGuardedTriggers renders version 2 triggers: identical to
// version 1 except that the update trigger fires only when a column changed.
func changeContractGuardedTriggers(spec changeContractTableSpec, keys, columns []string) []changeContractTrigger {
	triggers := changeContractTriggers(spec, keys)
	parts := make([]string, 0, len(columns)+1)
	for _, column := range columns {
		c := sqliteQuotedIdentifier(column)
		parts = append(parts, "OLD."+c+" is not NEW."+c)
	}
	if len(keys) == 0 {
		parts = append(parts, "OLD.rowid is not NEW.rowid")
	}
	guard := "when " + strings.Join(parts, " or ") + "\n"
	update := triggers[1].sql
	update = strings.Replace(update, "\nbegin\n", "\n"+guard+"begin\n", 1)
	triggers[1].sql = update
	return triggers
}

func changeContractTriggers(spec changeContractTableSpec, keys []string) []changeContractTrigger {
	table := sqliteQuotedIdentifier(spec.name)
	id := fmt.Sprint(spec.id)
	advance := func(extra string) string {
		return "\tupdate change_contract_sequence set value = value + 1" + extra + " where id = 1;\n"
	}
	stamp := "\tupdate change_contract_tables set last_seq = (select value from change_contract_sequence where id = 1) where table_id = " + id + ";\n"
	logRow := func(seq, op, key, where string) string {
		return "\tinsert into change_contract_log(seq, table_id, op, row_key) select " + seq + ", " + id + ", " + op + ", " + key + " from change_contract_sequence where id = 1" + where + ";\n"
	}
	name := func(event string) string { return changeContractTriggerPrefix + spec.name + "_" + event }
	header := func(event, verb string) string {
		return "create trigger " + sqliteQuotedIdentifier(name(event)) + "\nafter " + verb + " on " + table + "\nbegin\n"
	}
	var insertBody, updateBody, deleteBody string
	if spec.mode == ChangeContractModeTable {
		insertBody = advance("") + stamp
		updateBody = insertBody
		deleteBody = insertBody
	} else {
		changed := changeContractKeyChangedSQL(keys)
		insertBody = advance("") + logRow("value", fmt.Sprint(ChangeContractOpInsert), changeContractKeySQL("NEW", keys), "") + stamp
		updateBody = advance(" + "+changed) +
			logRow("value - "+changed, fmt.Sprintf("case when %s then %d else %d end", changed, ChangeContractOpDelete, ChangeContractOpUpdate), changeContractKeySQL("OLD", keys), "") +
			logRow("value", fmt.Sprint(ChangeContractOpInsert), changeContractKeySQL("NEW", keys), " and "+changed) +
			stamp
		deleteBody = advance("") + logRow("value", fmt.Sprint(ChangeContractOpDelete), changeContractKeySQL("OLD", keys), "") + stamp
	}
	return []changeContractTrigger{
		{name: name("insert"), sql: header("insert", "insert") + insertBody + "end"},
		{name: name("update"), sql: header("update", "update") + updateBody + "end"},
		{name: name("delete"), sql: header("delete", "delete") + deleteBody + "end"},
	}
}

// ChangeContractStatus reads the contract in one read transaction.
func (s *Store) ChangeContractStatus(ctx context.Context) (ChangeContractStatus, error) {
	var status ChangeContractStatus
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return status, err
	}
	defer tx.Rollback()
	if err := tx.QueryRowContext(ctx, `pragma schema_version`).Scan(&status.SchemaVersion); err != nil {
		return status, err
	}
	present, err := changeContractPresent(ctx, tx)
	if err != nil || !present {
		return status, err
	}
	status.Enabled = true
	if err := tx.QueryRowContext(ctx, `select contract_version, epoch, generation, generation_reason, generation_at, enabled_at from change_contract_state where id = 1`).Scan(
		&status.ContractVersion, &status.Epoch, &status.Generation, &status.GenerationWhy, &status.GenerationAt, &status.EnabledAt); err != nil {
		return status, fmt.Errorf("read change contract state: %w", err)
	}
	if err := tx.QueryRowContext(ctx, `select value, retention, pruned_through from change_contract_sequence where id = 1`).Scan(&status.Sequence, &status.Retention, &status.PrunedThrough); err != nil {
		return status, fmt.Errorf("read change contract sequence: %w", err)
	}
	if err := tx.QueryRowContext(ctx, `select count(*) from change_contract_log`).Scan(&status.LogEntries); err != nil {
		return status, err
	}
	if status.Armed, err = changeContractArmed(ctx, tx); err != nil {
		return status, err
	}
	byName := map[string]ChangeContractTableStatus{}
	for _, spec := range changeContractTableSpecs {
		row := ChangeContractTableStatus{TableID: spec.id, Table: spec.name, Mode: spec.mode, KeyColumns: []string{}}
		if row.Present, err = changeContractTablePresent(ctx, tx, spec.name); err != nil {
			return status, err
		}
		var keyJSON string
		err := tx.QueryRowContext(ctx, `select key_columns, last_seq from change_contract_tables where table_id = ?`, spec.id).Scan(&keyJSON, &row.LastSeq)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return status, err
		}
		if err == nil {
			_ = json.Unmarshal([]byte(keyJSON), &row.KeyColumns)
			if row.Armed, err = changeContractTableArmed(ctx, tx, spec.name); err != nil {
				return status, err
			}
		}
		status.Tables = append(status.Tables, row)
		byName[spec.name] = row
	}
	rows, err := tx.QueryContext(ctx, `select family, table_name from change_contract_families order by family, table_name`)
	if err != nil {
		return status, err
	}
	defer rows.Close()
	var current *ChangeContractFamily
	for rows.Next() {
		var family, table string
		if err := rows.Scan(&family, &table); err != nil {
			return status, err
		}
		if current == nil || current.Family != family {
			status.Families = append(status.Families, ChangeContractFamily{Family: family, Armed: true})
			current = &status.Families[len(status.Families)-1]
		}
		current.Tables = append(current.Tables, table)
		row, known := byName[table]
		if !known || row.Present && !row.Armed {
			current.Armed = false
		}
		if row.LastSeq > current.LastSeq {
			current.LastSeq = row.LastSeq
		}
	}
	return status, rows.Err()
}
