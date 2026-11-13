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
gitcrawl change-contract enable [--retention N] [--json]
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
| `change_contract_state` | `contract_version` (1), random `epoch`, `generation`, its reason and time |
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

¹ `table` mode. All other tracked tables are `rows` mode.

Families are a convenience derived from the Clawmmunity reader. A consumer must
still verify that every table it reads, including tables named by its own
removal policy, is tracked and armed, and treat any untracked table as "always
changed".

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
if contract absent, v != 1, step 2 fails, C missing,
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


## Bounded native report snapshots

`store.CaptureNativeReport` is a local, read-only snapshot API. It requires an
explicit lowercase `owner/repo`, the expected `NativeReportFileIdentity`, positive
row/byte limits and a context deadline. It opens the existing schema-16 native
archive without migration and reads the enabled, armed change contract and all
selected data in one transaction. It does not enable tracking or change native
retention. The caller must keep the pathname bound to one physical file during
the call. Concurrent SQLite writers are supported; rename/replacement is not.
Pre/post device/inode checks detect persistent replacement, not a transient
A → B → A swap. Identity is currently supported on Darwin and Linux.

The fixed `gitcrawl.native-report/v1` local read set contains:

- The selected repository and current threads/comments after filtering parent
  exclusions by repository and thread number.
- Comment-family observation reservations and completed membership rows for
  those threads. Other enrichment-family observations are outside this profile.
- Both complete exclusion ledgers for that repository, including exclusions
  whose native thread has already been purged.

A retained thread/comment whose native `raw_json.node_id` is in the global
excluded-node ledger refuses the entire capture. This uses the purge owner's
native node interpretation; it does not silently remove individual rows or
rewrite completed membership lists. Parent-excluded content is already filtered.
The hold also covers an excluded node reappearing under a surviving parent.

Every table, including an empty table, has an explicit column/key layout,
ordered rows and a SHA-256 digest. Missing or extra columns are refused. Native
SQL NULL, signed int64 values and exact text bytes are retained separately;
text can contain NUL or invalid UTF-8. Comment submission/publication timestamps
remain their stored values, independently of creation time. A non-null comment
`raw_json_blob_id` refuses the capture: this slice does not dereference inline
or filesystem blobs or claim their raw evidence is complete. Historical
revisions, PR enrichment and acquisition completeness are outside the profile.

The anchor records the physical file, repository name/native ID, profile/scope
hash, contract epoch/generation, schema cookie, sequence and retained-log floor.
It is observation evidence, **not a qualified delivery baseline or authority**.
The scope hash binds the fixed profile identifier and exact repository name.
Table hashes bind table name, ordered column names and row scalars; strings use
an unsigned 64-bit big-endian byte length followed by exact bytes, and scalars
use a kind byte followed by text or an eight-byte integer. The snapshot hash
binds the scope hash and ordered table hashes. File identity and contract cursor
are separate from content equality. No JSON encoding or wire compatibility is
implied by these local digests or types.

Each call scans the complete selected content, including tables without row
journaling. In particular, comments have no native content digest: table
invalidation cannot suppress unchanged rows without reading/hashing them.
This supports incremental provider acquisition and eventual receipt/delta
delivery; it does not promise O(changes) native scanning. Table-mode mutations
legitimately leave gaps in the row log. Untracked membership or excluded-node
changes can change the snapshot digest while the contract sequence stays equal.
An equal sequence must never be used alone to skip delivery.

Preflight SQL measures scoped rows and scalar bytes before each table is loaded;
loading also enforces the limits. The byte limit counts column-name bytes and
scalar payload encoding (one kind byte, plus eight bytes for integers or text
lengths, plus text bytes). It is not a process RSS limit. SQL scan time is bounded
by the deadline. Any error returns no successful or partial capture. No source
checkpoint, outbox, baseline qualification, wire projection or removal comparator
is implemented here. Future removal comparison must use a complete inventory
from the same qualified binding; local absence is not a provider deletion.
Current exclusion-policy qualification and pending-payload policy enforcement
remain delivery prerequisites, independently of the native snapshot's freshness.


### Private report projection

`remotedelivery.ProjectPrivateReport` converts a successful native capture into
`gitcrawl-private-report-v1`. It is a pure local projection, with no HTTP, CLI,
outbox or baseline-registration behavior. The caller still owns source custody,
raw-payload provenance and any later destination approval.

All seven tables retain their fixed columns, keys and ordered rows, including
empty tables. Each scalar is `{kind, value}`: SQL NULL has kind `null` and a null
value; text has kind `text`; signed integers have kind `integer` and an exact
decimal string. Empty text remains distinct from NULL. Text, including private
raw JSON and NUL, is unchanged. Invalid UTF-8 is refused before JSON encoding
can replace it. Native submission/publication timestamps are neither parsed nor
replaced with creation or ingestion times.

The projection retains repository identity, logical cursor and native inventory
digests; integer metadata also uses decimal strings. It omits the physical
`Anchor.File`. Hashes and byte counts still describe the native capture, not the
encoded projection. The store remains the exclusion-policy owner: no second
filter or deletion inference is applied. Unsupported layouts/scalars and blob
references return a zero result with a content-free error. This profile neither
certifies historical bodies or acquisition completeness nor authorizes delivery.


### In-memory private seed preparation

`remotedelivery.PreparePrivateSeed(ctx, report, request)` consumes an unmodified
`ProjectPrivateReport` result and returns a Crawlkit `GitHubSeedPlan` plus exact
manifest, ordered chunk and completion bytes. The request supplies archive,
binding ID, a preallocated random 32-character lowercase hexadecimal seed ID,
and a positive `MaxBytes` total wire budget. The same report and request produce
the same bytes. A different captured inventory needs its own downstream seed
lifecycle; this packer does not authorize reusing an already delivered seed ID.

The adapter uses the existing seven-table profile and its key-column order.
Local `{kind:"null", value:null}` cells become wire `{kind:"null"}`; text and
signed integer strings retain their values, including NUL, private raw JSON and
native publication timestamps. The native SQLite repository ID remains separate
from provider `github_repo_id` text. No rows are collapsed, sanitized or removed.
Every table has a descriptor, even when it has no rows or chunks.

Chunks are filled in table/row order, with at most 500 rows and 4 MiB of exact
JSON per body, including the envelope, cursor and separators. One-row Crawlkit
encoding validates scalar bounds before marshaling and measures JSON escaping.
The adapter accounts for the actual chunk header and encodes the completed chunk
once; it does not repeatedly encode each growing row prefix. A single row that
cannot fit is refused. Table hashes bind ordered binary chunk digests; manifest
and completion hashes refer to the final exact bytes, never a later re-encoding.

The native token retains epoch, generation, SQLite schema cookie, journal head,
retained-log floor, scope digest and native inventory digest. The schema cookie
is distinct from native schema 16. An inventory change produces a different
token even when the head is unchanged. Wire hashes never replace native hashes.
The capture owner remains responsible for the native hashes and exclusion/blob
holds; preparation does not duplicate that policy or re-read native state.

`MaxBytes` counts all returned manifest/chunk/completion bodies. It is not an
RSS budget and does not include the caller's existing capture/projection. The
8192-chunk codec limit also applies. Cancellation is checked during packing;
callers own the deadline and process budget. Malformed input, an oversized row,
exhausted total budget or cancellation returns a zero result and a content-free
error; cancellation/deadline causes remain recognizable with `errors.Is`.

These are persistable bytes, not durable staging, HTTP delivery or a qualified
baseline. No file, source cursor, checkpoint or receiver state changes. Later
delivery must independently qualify destination/binding authority and current
native-owner exclusion policy against pending exact payloads, including replay.
A saved capture or included ledger is not current policy authority; stale pending
bytes must be held rather than silently rewritten or allowed to clear historical
exclusions. Packing makes no provider freshness, history-completeness or public
publication claim.
