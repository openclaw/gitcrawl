---
title: Reader change contract
nav_order: 17
permalink: /change-contract/
---

# Reader change contract
{: .no_toc }

Let a read-only consumer decide cheaply and exactly whether selected archive
tables changed since a recorded cursor, and which rows changed.
{: .fs-6 .fw-300 }

1. TOC
{:toc}

## Purpose

SQLite's `PRAGMA data_version` only reports that *some* commit happened. A
continuously written archive therefore looks changed on almost every poll, and a
consumer that rebuilds derived state from a few tables rebuilds it on every
cycle. The change contract records, inside the archive and in the writer's own
transaction, a monotonic sequence for every insert, update and delete of tracked
tables, and the primary key of each changed row for row-mode tables.

The contract is opt-in per archive. It is never published in portable stores
or cloud snapshots: `portable prune`, `portable export` and the cloud snapshot
strip it before they rewrite rows.

## Commands

```bash
gitcrawl change-contract status [--json]
gitcrawl change-contract enable [--retention N] [--contract-version 1|2] [--json]
gitcrawl change-contract bump --reason TEXT [--json]
gitcrawl change-contract disable [--json]
```

- `enable` creates the contract (new epoch, generation 1) or re-arms tables whose
  triggers are missing or altered, advancing the generation. It is idempotent.
  `--retention` sets the retained log span, default 1,000,000 sequence values,
  minimum 1,024. Changing it on an enabled contract prunes the existing log to
  the new bound in the same transaction. Any surviving partial contract (for
  example after an interrupted manual removal) is replaced by a new epoch,
  keeping its retention.
- `--contract-version 2` moves an existing contract to [version 2](#version-2)
  or creates a new one at version 2; `--contract-version 1` moves it back.
  Either change re-arms every table and advances the generation. Without the
  flag, `enable` keeps the recorded version and creates version 1.
- `bump` advances the generation after an owner-run rewrite the triggers could
  not observe, forcing every reader to rebuild once.
- `disable` removes every contract trigger and table. Readers then see no
  contract and rebuild as before.
- `status` is read-only and reports the cursor, per-table state and families.

`enable`, `bump` and `disable` open an existing archive without running schema
migrations or convergence work, and refuse an archive whose schema version
differs from the binary's. They take only SQLite's ordinary write lock, so they
can run beside a live collector.

Every writable open by this or a later Gitcrawl binary re-arms an enabled
contract whose triggers a migration removed, and advances the generation. Open
does not fail if the repair fails; readers detect unarmed tables independently.

## How changes are recorded

`enable` installs three `AFTER` triggers, `change_contract_<table>_insert`,
`_update` and `_delete`, on each tracked table. Triggers are part of the
database, so every writer records changes, including older pinned Gitcrawl
binaries and other SQLite clients that know nothing about the contract. They
fire for ordinary statements, `INSERT ... ON CONFLICT DO UPDATE` (for example
`upsertThreadRevisionEvidenceBlob` updating an existing `blobs` row on a hash
conflict), foreign-key cascades and bulk deletes, and roll back with the
writer's transaction. Statement results are unchanged: `changes()` and
`last_insert_rowid()` exclude trigger writes. Gitcrawl never uses
`INSERT OR REPLACE`, whose implicit deletes do not fire delete triggers unless
`recursive_triggers` is on.

Each journaled mutation:

1. advances `change_contract_sequence.value` by one (two for a key change);
2. in `rows` mode, appends `change_contract_log(seq, table_id, op, row_key)`;
3. sets `change_contract_tables.last_seq` for the table to the new value.

`op` is `1` insert, `2` update in place, `3` delete. An update that changes a
row's key journals a delete of the old key at `seq - 1` and an insert of the new
key at `seq`. `row_key` is the declared primary key: its value for a single
column, a JSON array in key order for a composite key (for example
`[thread_id,"review_thread_id"]`), and the `rowid` for a table without one.
`change_contract_tables.key_columns` names the columns. Keys are declared
primary keys, so `VACUUM` does not invalidate them.

`table` mode tables advance the sequence and `last_seq` without row entries.
They answer "changed since C" but not "which rows".

## Objects

| Object | Contents |
| --- | --- |
| `change_contract_state` | `contract_version` (1 or 2), random `epoch`, `generation`, its reason and time |
| `change_contract_sequence` | current `value`, `retention`, `next_prune`, `pruned_through` |
| `change_contract_tables` | `table_id`, `table_name`, `mode`, `key_columns`, `last_seq`, and the exact SQL of its three triggers |
| `change_contract_families` | source-declared `(family, table_name)` pairs |
| `change_contract_log` | `(seq, table_id, op, row_key)`; `seq` is the primary key |
| `change_contract_log_prune` | trigger that bounds the log |

Table identifiers are permanent. New tables receive new identifiers.

## Families

| Family | Tables |
| --- | --- |
| `reader.coverage` | `repositories`, `threads`, `thread_revisions`, `thread_fingerprints`, `thread_key_summaries`, `thread_child_observation_reservations`, `cluster_runs`, `cluster_groups`, `cluster_memberships`, `pull_request_details`, `pull_request_files`¹, `comments`¹, `pull_request_commits`¹, `pull_request_checks`¹, `pull_request_review_threads`, `github_workflow_runs`¹, `sync_attempt_failures`, `sync_runs`, `summary_runs`, `repo_sync_state`, `portable_metadata`, `thread_exclusions`, `analytics_coverage`, `analytics_review_state_coverage` |
| `reader.clusters` | `repositories`, `threads`, `thread_revisions`, `thread_key_summaries`, `document_summaries`, `cluster_runs`, `clusters`, `cluster_members`, `cluster_groups`, `cluster_memberships`, `cluster_overrides`, `cluster_closures`, `cluster_candidate_generations`, `cluster_candidates`, `cluster_candidate_members`, `cluster_candidate_overlaps`, `cluster_candidate_selection`, `thread_exclusions` |
| `reader.code` | `repositories`, `code_snapshots`, `code_documents`¹, `thread_exclusions` |
| `history.blobs` | `blobs` |
| `history.pull_request_review_threads` | `pull_request_review_threads` |
| `history.thread_revisions` | `thread_revisions` |
| `reader.checks` | `thread_checks`, `pull_request_detail_fetches`, `pull_request_review_thread_syncs`, `repository_checks`, `analytics_coverage_checks`, `record_evidence_blobs`, `record_check_observations` |
| `history.pull_request_merge_state_events` | `pull_request_merge_state_events` |
| `history.analytics_coverage_count_samples` | `analytics_coverage_count_samples` |

¹ `table` mode. All other tracked tables are `rows` mode.

The check, event and series tables of the
[record-efficient layout](/record-efficiency/) and
`pull_request_review_thread_syncs` are tracked with identifiers 38 to 46.
Enable and every writable-open repair add newly declared families to an
existing contract. Arming newly tracked tables on an existing contract advances
its generation once.

Families are a convenience derived from the Clawmmunity reader. A consumer must
still verify that every table it reads, including tables named by its own
removal policy, is tracked and armed, and treat any untracked table as "always
changed".

## Version 2

Version 2 keeps every version 1 object, log format, key and reader rule, and
adds one guarantee: an `UPDATE` that leaves every column of a tracked row
unchanged is not journaled. Its update triggers carry a guard,

```sql
create trigger "change_contract_threads_update"
after update on "threads"
when OLD."id" is not NEW."id" or OLD."repo_id" is not NEW."repo_id" or ...
begin
	...
end
```

over all columns of the table (and `rowid` for a table without a declared key).
Inserts, deletes and key changes are journaled exactly as in version 1. An
unchanged rewrite by any writer, including an older binary's unconditional
upsert, therefore leaves the sequence, `last_seq` and the log untouched.

Because the guard names columns, version 2 triggers must cover the current
column set: `enable` compares every table's recorded triggers with the triggers
rendered from its current columns, and the writable-open repair re-arms (and
advances the generation) when a tracked table gained a column. A reader that
accepts version 2 needs no other change; a version 1 reader must treat
version 2 as unknown and rebuild fully, as the reader algorithm requires.

## Bounded growth

The log keeps at most `retention` sequence values, plus one prune interval of
1,024. When an appended entry reaches `next_prune`, the prune trigger deletes
entries at or below `seq - retention` in the same transaction and raises
`pruned_through`. Entries with `seq <= pruned_through` may be gone; a cursor
below `pruned_through` must rebuild fully. All other contract tables have a
fixed size. A log entry occupies about 16 bytes for an integer key and about
48 bytes for a composite review-thread key, so the default retention costs
about 16-48 MB.

## Generations and rebuilds

The cursor is `(epoch, generation, schema_version, seq)`. A full rebuild is
required when any of these differs from the last read, or when the contract is
absent, unarmed or of an unknown version:

- `epoch` changes when the contract is created again after `disable`.
- `generation` changes when a table is re-armed or after `bump`.
- `schema_version` changes on any DDL. A table rebuild drops its triggers and
  copies rows without journaling; the schema change makes the cursor invalid
  even if identical triggers are recreated.
- An unarmed table means writes since its triggers disappeared are not
  journaled, so readers rebuild on every cycle until a writable open or
  `enable` re-arms it.
- `seq` lower than the cursor means the file was restored. Restores and file
  replacements also change the file identity (device, inode, birth time), which
  consumers should bind as well; the contract cannot detect a restore of an
  older same-epoch copy that later advanced past the cursor.

Git object repacks of portable stores, including byte-aware optional
compaction, never touch an archive's SQLite rows and need no generation change.
Portable prune and cloud snapshots strip the contract from their copies.
`VACUUM` preserves rows and declared keys.

## Reader algorithm

Run in one read transaction, so the cursor and the rows read are one snapshot:

```sql
-- 1. Contract identity and cursor.
PRAGMA schema_version;                                  -- sv
SELECT s.contract_version, s.epoch, s.generation, q.value, q.pruned_through
FROM change_contract_state s, change_contract_sequence q; -- v, e, g, seq, pruned

-- 2. Every present table of family F must be tracked and armed (expect 0).
SELECT count(*) FROM change_contract_families f
JOIN sqlite_schema m ON m.type = 'table' AND m.name = f.table_name
LEFT JOIN change_contract_tables t ON t.table_name = f.table_name
WHERE f.family = :F
  AND (t.table_id IS NULL OR (SELECT count(*) FROM sqlite_schema s
       WHERE s.type = 'trigger' AND s.tbl_name = t.table_name
         AND s.sql IN (t.insert_trigger, t.update_trigger, t.delete_trigger)) <> 3);
SELECT count(*) FROM sqlite_schema
WHERE type = 'trigger' AND name = 'change_contract_log_prune';  -- expect 1

-- 3. Changed since cursor C?
SELECT max(t.last_seq) FROM change_contract_families f
JOIN change_contract_tables t ON t.table_name = f.table_name
WHERE f.family = :F;                                           -- last

-- 4. Incremental rows (rows-mode tables only, requires C.seq >= pruned).
SELECT l.seq, t.table_name, t.key_columns, l.op, l.row_key
FROM change_contract_log l
JOIN change_contract_tables t ON t.table_id = l.table_id
JOIN change_contract_families f ON f.table_name = t.table_name AND f.family = :F
WHERE l.seq > :C_seq AND l.seq <= :seq ORDER BY l.seq;
```

```text
if contract absent, v not in (1, 2), step 2 fails, C missing,
   (e, g, sv) != (C.epoch, C.generation, C.schema_version), or seq < C.seq:
    full rebuild; store cursor (e, g, sv, seq)
elif last <= C.seq:
    family F unchanged; advance the cursor to (e, g, sv, seq)
elif C.seq < pruned or a table-mode table of F has last_seq > C.seq:
    full rebuild; store new cursor
else:
    for each distinct (table, row_key) in step 4:
        re-read the row by key; absent means deleted
    store new cursor
```

The reference reader in `internal/store/change_contract_test.go` implements exactly this
algorithm.

## Overhead

Measured on Apple M-series with the bundled pure-Go SQLite, inside one writer
transaction so commit flushes are excluded
(`go test ./internal/store -bench ChangeContractWriterOverhead`):

| Statement | Without | With | Added |
| --- | ---: | ---: | ---: |
| `blobs` hash-conflict upsert (rows mode) | 1.9 µs | 8.7 µs | 6.8 µs per row |
| `comments` insert (table mode) | 5.3 µs | 7.7 µs | 2.4 µs per row |

Each committing transaction that touches tracked tables also dirties the
sequence page, the table-state page and the log tail page (at most three extra
WAL pages), plus a prune every 1,024 log entries.

Exact native evidence and append-only check observations use IDs 45 and 46 in
`reader.checks`. See [reconstruction and cutover](/record-efficiency/#exact-observations-and-reconstruction).
