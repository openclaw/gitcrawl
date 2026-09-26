---
title: Analytics source preparation
nav_order: 8
permalink: /analytics-source/
---

# Analytics source preparation

`gitcrawl analytics owner/repo` supports collector-owned publication repair,
identity enrichment and ongoing GraphQL collection for a full-history archive.
It uses the configured native source database and the existing token helper.
It does not run embeddings or classification models.

```sh
gitcrawl --config SOURCE_CONFIG analytics owner/repo --json
gitcrawl --config SOURCE_CONFIG analytics owner/repo --status --json
gitcrawl --config SOURCE_CONFIG analytics owner/repo --apply --json
gitcrawl --config SOURCE_CONFIG --github-token-command TOKEN_HELPER \
  analytics owner/repo --enrich --watch --json
```

Without apply/enrich/watch/once, the command audits retained publication evidence
read-only. `--apply` performs an idempotent, bounded repair after taking an
operator-managed consistent backup. It stores `submitted_at_gh` and
`publication_at_gh` for current comments and source revisions. Review submission
is distinct from draft creation; pending reviews have no published event. Raw
payloads, existing IDs, genuine creation times and original observation times
remain unchanged, and normalization creates no fictional provider edit.

Source schema 14 added these fields and the actor/coverage evidence tables.
Historical normalization completion is recorded in
`analytics_collection_state.publication_repair_generation`; downstream consumers
must reconcile the affected datasets when this generation changes rather than
relying solely on append-only revision IDs.

GraphQL history now retains actor node IDs. `--enrich` recovers missing identities
through original content nodes, not login guessing, and stores public actor
profiles. Unavailable nodes are explicit unknowns; permission or transport
failures are not converted to successful missing-data evidence. New source actors
enter the profile queue, and profiles become eligible for refresh after 24 hours.
Operational queue tables are separate from publishable evidence.

`--watch` polls updated issues and PRs every two minutes, overlapping the prior
verified watermark by five minutes. It paginates without GitHub search's hit cap
and hydrates relevant updated threads. Issue and PR lanes have independent durable
checkpoints and process at most two discovery pages per cycle. Eight two-thread
requests progress independently per page. A rejected batch splits into individual
requests; an item may be passed only after its failure is durably queued. Unrelated
items and the other discovery lane continue. Core retries process at most eight
due items before discovery, with a two-minute request deadline and bounded
exponential backoff. After persisting core coverage, targeted review recovery
uses the time until the next nominal two-minute core poll; it does not add a
two-minute idle wait after recovery. Each wave rechecks actual GraphQL quota and
admits up to sixteen items through the existing eight two-item workers. Admission
uses the authoritative GraphQL `rateLimit` response rather than REST resource
counters, which can differ. Each history session also enforces its configured
floor against observed GraphQL balances before pagination. A
client retains the lowest observed GraphQL balance until the reset boundary
passes. An upward sample or a shifted future reset cannot increase admission;
REST snapshot refreshes do not overwrite this evidence. Logs distinguish the
raw provider balance/reset from the conservative effective admission values. A
32-point-per-item admission margin and per-request native quota checks preserve
3,000 points for recovery, leaving 1,500 points above ordinary capture's floor.
Missing or expired quota stops provider recovery; local discovery can continue.
The request window yields before core polling and cancellation retains retry
receipts and the last committed scan. Quota and numeric per-query cost logs make
the actual spending observable without credentials or content bodies.
No partial connection is accepted as complete evidence. Non-nested
continuation pages use 100 nodes to avoid repeated small round trips. Identity/profile enrichment uses eight disjoint 100-node requests with normal
quota guards and can run concurrently under
the same source owner. `--once` performs one resumable update cycle.

Discovery skips unresolved core retry items. A review-only recovery queue does
not defer a newly discovered core edit: that edit is collected as core work. If
it fails, a core retry obligation prevents repeated discovery attempts until the
bounded scheduler selects it. This prevents enrichment backoff from hiding fresh
core data while its verified coverage watermark advances.
When both queues contain an item, its core retry owns the backoff; the review
retry pass cannot dispatch the same item. Recovery resolves only after an
accepted membership observation exists, including a proven empty set.

Hydration workers reuse the watch owner's open store instead of repeating
full-archive migration checks for each batch. Independent guarded clients overlap
network work; native transactions still coordinate source writes. Enrichment
continues checking its queues every two minutes after the initial drain. New
observations enqueue unresolved content identities or known actor profiles
directly, avoiding repeated whole-archive scans during watch operation.

A completed historical discovery receipt supplies the initial discovery baseline.
The pre-upgrade checkpoint is retained; migration copies its in-flight cursor into
the corresponding independent lane. `through:owner/repo:issues` and
`through:owner/repo:pullRequests` record discovery progress; the aggregate watermark
is their minimum. These are not proof that queued failures have been repaired.
`analytics_coverage.complete` continues to describe verified core issue/PR/comment
traversal. Core failures clear it until reconciled; the previously verified
watermark remains available while another page is in progress. Historical
review-state enrichment does not invalidate core contributor/response coverage.

## Review-state and failure contracts (schema 15)

GraphQL collection requires explicit `isResolved` and `isOutdated` values and
retains review-thread IDs, source state, source comments and immediate reply IDs.
It writes the existing tables:

- `pull_request_review_threads`: current observation, keyed by
  `(thread_id, review_thread_id)`, including `is_resolved`, `is_outdated`,
  `comments_json`, retained `raw_json`, and actual `fetched_at`.
- `pull_request_review_thread_revisions`: append-only changes, with integer `id`
  and actual `recorded_at`; unchanged observations do not manufacture revisions.
- `pull_request_review_thread_syncs`: last complete observation per `thread_id`.
  New nullable `review_thread_ids_json` contains its exact native ID membership.
  NULL means not acquired under this contract; `[]` means a complete empty set.
- `analytics_review_state_coverage`: keyed by `repository`, with `cursor`,
  `ceiling`, `scanned`, `queued`, `pending_items`, `scan_complete`, `complete`,
  and `observed_at`. This separate completion contract requires a finished
  targeted scan and no outstanding review-state recovery items.

Absence from a complete membership set never invents a provider deletion or
removes retained history. Existing comment IDs/replies and
`thread_child_observation_memberships` keep their existing contract. Consumers
must distinguish historical rows from the latest provider membership.

The watch inspects bounded chunks of 5,000 primary-key rows within each recovery
window up to a captured
ceiling, queuing only PRs with retained review threads or unknown connection data.
Complete retained zero-count GraphQL connections materialize `[]` using their
actual retained observation time. Conditional writes preserve a newer live
observation; incomplete or undated evidence instead enters provider recovery.
`review_state_recovery:owner/repo` records cursor, ceiling, scanned/queued counts
and scan completion. Queue completion is separately required for review-state
coverage, never for the existing core coverage flag. This
targeted recovery does not restart historical discovery or enable remote syncing;
missing historical resolution states cannot be reconstructed from old payloads.

Operational tables are **not public conversation datasets**:

- `analytics_fetch_attempts(id, repository, number, operation, started_at,
  finished_at, status, error_class, error_text, evidence_json)` retains successful
  and rejected GraphQL work. `number=0` identifies a discovery-lane request.
  Rejection evidence is bounded structural metadata, IDs, counts, pagination and
  body lengths/hashes; it contains no credentials, response headers or prose bodies.
- `analytics_retries(repository, number, operation, first_seen_at, last_seen_at,
  next_attempt_at, attempts, last_attempt_id, resolved_at)` is keyed by
  `(repository, number, operation)`. Resolved entries and attempt history remain.
  Operations include `graphql_history` (core traversal), `review_state`
  (targeted enrichment), `discover_issues`, and
  `discover_pullRequests`. Recovery queue rows start with zero attempts.

`analytics --status --json` reads bounded recent receipts, outstanding retry count,
discovery coverage and review-state scan progress without credentials or collection.
Timestamped logs distinguish `github_update_complete`, `github_update_failed`,
`github_coverage_pending` (core), and `review_state_progress` (enrichment).
`review_state_quota` records fresh limit/remaining/reset/reserve and admitted wave
size; `review_state_cost` records actual provider points per recovery query.
The older successful-only `sync_runs` table is not a
complete failure ledger. Existing embeddings are reused; this command does not
generate embeddings or reinterpret empty review bodies as missing replies.

The permanent `runner.lock` coordinates ownership with the full-history backfill
supervisor. Do not unlink it or start a second supervisor against the same archive.
A finished backfill should be disabled as an automatic startup job when ongoing
collection takes ownership. The token-command result is held only in memory and
refreshed before its expected expiry, preserving one credential across quota
reservation and dispatch. Existing provider-rate protections remain active.

See [sync](/sync/), [configuration](/configuration/) and the [command reference](/commands/).
