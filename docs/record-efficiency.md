---
title: Record-efficient archive layout
nav_order: 18
permalink: /record-efficiency/
---

# Record-efficient archive layout
{: .no_toc }

Write content rows only when content changes, and record every check in a
separate check log.
{: .fs-6 .fw-300 }

1. TOC
{:toc}

## Purpose

A continuously running collector rechecks the same issues, pull requests and
repository every few minutes. In the legacy layout each check rewrites the
content row with new check bookkeeping (`last_pulled_at`, `observation_sequence`,
local `updated_at`, `fetched_at`) and with values that change without any
author action (merge state, provider activity clocks, coverage counts). Every
rewrite is a new version for downstream history readers such as Clawmmunity,
and the repository row alone was rewritten about every 1.9 seconds.

The record-efficient layout separates them:

- content rows are written only when a content field changes;
- every check is recorded in a check table keyed like the content row;
- merge state is an append-only event series and coverage counts a run-length
  metric series, neither of which creates a content version;
- the full GraphQL pull request node is stored once, in the thread envelope.

## Commands

```bash
gitcrawl record-checks status [--json]
gitcrawl record-checks enable [--json]
gitcrawl record-checks backfill [--json]
gitcrawl record-checks disable [--json]
```

The layout is opt-in per native archive, like the [change contract](/change-contract/).
Until `enable` runs, every write keeps the legacy semantics and the archive
contains no check table, view or marker.

- `enable` creates the tables and compatibility views, records the marker
  `record_efficiency_state(version = 1)` and seeds missing check rows. It is
  idempotent. Every later writable open by a Gitcrawl binary that knows the
  layout uses the split write path.
- `backfill` repeats the seed: missing `thread_checks`,
  `pull_request_detail_fetches`, `repository_checks` and
  `analytics_coverage_checks` rows are copied from the content rows, and each
  pull request without a merge-state event receives one event with its current
  value. It never modifies content rows.
- `disable` folds the latest check values back into the content rows' legacy
  columns, drops the views and the marker, and keeps the check and event tables
  as history. Older binaries then continue with correct current values.
- `status` is read-only.

Portable stores, cloud SQLite publications and mirrors never use the layout: `portable prune` folds the
current check values into the copied content rows and drops the check tables,
views and marker, so published stores keep the legacy single-row format. Cloud
snapshots read `threads_current`.

## Tables

| Table | Key | Written | Contents |
| --- | --- | --- | --- |
| `thread_checks` | `thread_id` | every accepted thread observation | `first_pulled_at`, `last_pulled_at`, absolute `observation_sequence`, `evidence_complete` (0/1), `evidence_observation_sequence`, `evidence_source_updated_at`, `checked_at` |
| `pull_request_detail_fetches` | `thread_id` | every PR detail fetch | `fetched_at` |
| `pull_request_review_thread_syncs` (existing) | `thread_id` | every review-thread fetch | `fetched_at`, `review_thread_ids_json` |
| `pull_request_merge_state_events` | `id` | append-only, when the triple changes | `thread_id`, `source` (`graphql` or `rest`), `observed_at`, `mergeable`, `merge_state_status`, `mergeable_state` |
| `repository_checks` | `repo_id` | once per sync cycle | `checked_at`, `provider_updated_at` |
| `analytics_coverage_checks` | `(repository, coverage)` | every coverage save | `observed_at`, and `through` for `analytics_coverage` |
| `analytics_coverage_count_samples` | `(repository, observed_at)` | append-only, when a count changes | `issues`, `pull_requests` |

A merge-state event is inserted only when `(mergeable, merge_state_status,
mergeable_state)` differs from the latest event of the same source for that
pull request. GraphQL events derive `mergeable_state` from `mergeStateStatus`
exactly like the pull request projection, so a thread and its PR detail record
one event. REST payloads record `mergeable` as `true`/`false` and the REST
`mergeable_state`.

`evidence_complete` replaces the sign of `threads.observation_sequence`: the
legacy column stored a negative sequence while child evidence for the source
revision was incomplete. `thread_checks.observation_sequence` is always
non-negative.

## Content rows

| Table | Written when | Excluded from comparison |
| --- | --- | --- |
| `threads` | any stored column other than check bookkeeping changes | `_graphql.mergeable`, `_graphql.mergeStateStatus` inside `raw_json`; `first_pulled_at`, `last_pulled_at`, `observation_sequence`, `evidence_*`, `updated_at` |
| `pull_request_details` | repo, number, base/head SHA and ref, head repository, additions, deletions, changed files or the PR-own envelope change | `mergeable_state`, `fetched_at`, `updated_at`; `mergeable`, `mergeable_state`, `updated_at` and GraphQL `mergeable`, `mergeStateStatus`, `updatedAt` inside `raw_json` |
| `pull_request_review_threads` | any column other than `fetched_at` changes | `fetched_at` |
| `repositories` | owner, name, GitHub ID or the envelope changes | `updated_at`; REST `updated_at`, `pushed_at` and counters; GraphQL `updatedAt`, `pushedAt`, `stargazerCount`, `forkCount` |
| `analytics_coverage` | the row is created or `complete` changes | `through`, `observed_at`, `issues`, `pull_requests` |
| `analytics_review_state_coverage` | any progress column changes | `observed_at` |

When a content row is written, every column, including the legacy bookkeeping
columns, receives the current value. Between content changes those columns keep
the values of the last content change. Existing rows are never rewritten to
the new format; a legacy envelope is compared after the same normalization, so
enabling the layout does not create a burst of new versions.

A GraphQL-sourced PR detail keeps its REST-shaped projection without the
thread-owned fields (`body`, `title`, `user`, `author_association`, `html_url`,
`created_at`, `updated_at`) and replaces the duplicated `_graphql` node with
`_graphql_pr`, the PR-only GraphQL fields (head and base refs and OIDs,
repositories, line counts, commit count, merge, draft and state fields), and
`"_graphql_ref": "threads.raw_json#/_graphql"`. The full node, including
comments, reviews, review threads and labels, is stored once in
`threads.raw_json`. Line counts and `base_sha` stay content: they change only
with the head or base revision.

Per-item sync transactions resolve the repository without writing; the sync
cycle records one repository check.

## Compatibility views

Each view returns every column of its content table, with the legacy columns
replaced by their current values, plus extra columns.

| View | Current columns | Extra columns |
| --- | --- | --- |
| `threads_current` | `first_pulled_at`, `last_pulled_at`, signed `observation_sequence`, `evidence_observation_sequence`, `evidence_source_updated_at`, `updated_at` | `evidence_complete`, `content_updated_at` |
| `pull_request_details_current` | `fetched_at`, `mergeable_state` | `mergeable`, `merge_state_status`, `content_fetched_at` |
| `pull_request_review_threads_current` | `fetched_at` (when the thread is in the latest membership) | `content_fetched_at` |
| `repositories_current` | `updated_at` | `provider_updated_at`, `content_updated_at` |
| `analytics_coverage_current` | `through`, `observed_at`, `issues`, `pull_requests` | `content_observed_at` |
| `analytics_review_state_coverage_current` | `observed_at` | `content_observed_at` |

A view prefers the check value only when it is at least as new as the content
row (a larger observation sequence for parent thread checks, source time before
sequence for evidence pointers, a later or equal time elsewhere). If an older binary rewrites the legacy columns after a rollback,
the views follow that newer write.

Gitcrawl's own readers (archive coverage, enrichment, summary and embedding
task selection, cluster key summaries, analytics integrity, closed-sweep
watermarks and cloud snapshots) read through the views when present.

## Change contract

The check and event tables and `pull_request_review_thread_syncs` are tracked
by the [change contract](/change-contract/) (identifiers 38 to 46). The family
`reader.checks` groups the check tables;
`history.pull_request_merge_state_events` and
`history.analytics_coverage_count_samples` the series. Contract version 2 also
stops journaling updates that change no column.

## Migrations and older binaries

Schema migrations that may rebuild tables first fold the current check values
into the content rows and drop the views; Open recreates the views. A converged
writable open at the current schema version changes nothing.

Binaries without this layout ignore the check tables. If one writes an enabled
archive, it rewrites content rows with legacy bookkeeping (the views follow it)
and does not record checks. Every writer of an enabled archive should therefore
run a binary that includes this layout. A binary that must rebuild a content
table at an older schema version will fail on the views; run `disable` first.

## Reader migration

A reader that wants current check values reads the `*_current` views or joins
the check tables. A history reader treats content-row changes as content
versions and the check tables as a clock overlay; it reads merge state and
coverage counts from their series. The fields that moved:

| Legacy column | Now |
| --- | --- |
| `threads.last_pulled_at`, `observation_sequence`, `evidence_observation_sequence`, `evidence_source_updated_at`, `updated_at` | `thread_checks` (`threads_current`) |
| sign of `threads.observation_sequence` | `thread_checks.evidence_complete` |
| `pull_request_details.fetched_at` | `pull_request_detail_fetches.fetched_at` |
| `pull_request_details.mergeable_state`, `raw_json._graphql.mergeable`, `mergeStateStatus` | `pull_request_merge_state_events` |
| `pull_request_details.raw_json._graphql` | `threads.raw_json._graphql` (and `_graphql_pr`) |
| `pull_request_review_threads.fetched_at` | `pull_request_review_thread_syncs.fetched_at` |
| `repositories.updated_at`, GitHub `updatedAt` | `repository_checks` |
| `analytics_coverage.through`, `observed_at` | `analytics_coverage_checks` |
| `analytics_coverage.issues`, `pull_requests` | `analytics_coverage_count_samples` |
| `analytics_review_state_coverage.observed_at` | `analytics_coverage_checks` |

## Exact observations and reconstruction

Latest-check tables are overlays, not the preservation archive. Every narrow
check-row insert/update is also appended to `record_check_observations`, including
intermediate evidence reservations. Native thread, PR detail, repository and
review-thread inputs retain their exact `raw_json` bytes separately, including
whitespace, unknown fields and metric-only differences. Stale thread inputs are
retained without overriding accepted content or observation ordering.

| Table | Key | Columns |
| --- | --- | --- |
| `record_evidence_blobs` | `sha256` (hex SHA-256 of reconstructed bytes) | `raw_bytes` BLOB, nullable `base_sha256`, `prefix_bytes`, `suffix_bytes`, `delta_gzip` BLOB, `depth` |
| `record_check_observations` | monotonic `id` | `source_table`, `source_key`, `observed_at`, nullable `evidence_sha256`, `check_json` |

An anchor has `raw_bytes` and depth zero. A delta takes the first `prefix_bytes`
and last `suffix_bytes` bytes from its recursively reconstructed base and inserts
the gzip-decoded `delta_gzip` between them. Verify the resulting SHA-256. Chains
are capped at 32; an anchor is also used when a delta would not save space.
`Store.EvidenceRaw` implements this reconstruction. Identical responses share one
blob, while every observation still has its own ID. A-B-A preserves all three
observations and reuses A's blob. Unknown fields remain content unless explicitly
listed as clocks or metrics. `rebaseable` remains meaningful PR content.

`source_key` is `repo_id/kind/number` for thread inputs, `thread_id` for detail
inputs and thread/fetch overlays, `thread_id/review_thread_id` for review inputs,
`repo_id` for repositories, and a JSON array `[repository,coverage]` for coverage
checks. Input checks contain original clocks and observation sequence/completeness;
overlay checks contain every column of their source row. Join thread input keys
through the unchanged `(repo_id,kind,number)` identity. These records retain native
collector inputs, not HTTP headers or wire bytes that the collector never stored.

Contract v2 adds table IDs 45 (`record_evidence_blobs`) and 46
(`record_check_observations`) to `reader.checks`. Consumers must fetch referenced
base blobs before reconstructing deltas and retain source permission boundaries.
These are not unrestricted public datasets. The change log is only invalidation;
its bounded retention does not delete the append-only evidence/check archive.

No old native ID or history row is deleted, compacted or rekeyed. Backfill seeds
only known current values and cannot recover past checks absent from legacy data.
Existing legacy raw rows remain untouched until a normal content change. Existing
downstream history must remain retained when it cannot safely be mapped.

## Coordinated cutover and rollback

Do not enable against a running mixed-version writer fleet. First pause all
writers and derived importers, establish the approved consistent storage clone
covering SQLite/WAL and derived stores, and record the matching old binaries and
configuration. Prepare compatible readers for all new tables and views, then:

```sh
NEW_BINARY --config ARCHIVE_CONFIG record-checks enable --json
NEW_BINARY --config ARCHIVE_CONFIG record-checks backfill --json
NEW_BINARY --config ARCHIVE_CONFIG change-contract enable --contract-version 2 --json
NEW_BINARY --config ARCHIVE_CONFIG record-checks status --json
NEW_BINARY --config ARCHIVE_CONFIG change-contract status --json
```

Rebaseline consumers on the new generation, check current/history/evidence reads
and permissions, then resume only after acceptance. Cadence is unchanged.
Before resuming writes, restore the coordinated clone plus matching binaries and
configuration if acceptance fails. `record-checks disable` is a non-destructive
layout rollback on the new schema: it folds latest clocks back, removes history
triggers/views/marker, and retains evidence, observations and events. It is not a
replacement for restoring an old-schema collector from the clone. After resumed
writes, preserve or replay new data before any snapshot rollback.

## Publication and owner removal boundaries

Cloud SQLite sanitization folds current overlays, then strips all record-efficiency
views, triggers, checks and exact evidence before publishing the copy. Native
archives are unchanged. Portable pruning uses the same split-state removal path.
Owner-directed purge refuses archives retaining `record_check_observations`, even
when the layout is disabled: shared/delta evidence needs ownership-aware repair,
and successful removal must never leave reconstructable private payloads behind.
This is a fail-closed limitation, not a new retention deletion policy.

Evidence comparison keys (`evidence_source_order_key` and
`content_evidence_source_order_key`) are UTC nanosecond-normalized strings saved
by the writer in `thread_checks`; compatibility views require only built-in
SQLite functions, not a process-local extension. Backfill reconciles newer
legacy merge states and coverage samples when re-enabling after legacy writes.
Repository fetches are retained before child hydration can fail; per-item
resolution remains write-free.

Merge-state event time is the transition time, not its latest confirmation.
Current PR views therefore retain the latest known categorical event across
content-only head changes. Re-enable compares the folded scalar merge state with
the latest event before seeding; frozen raw payloads are not new observations at
a folded fetch timestamp. Exact confirmation inputs remain in the check archive.
