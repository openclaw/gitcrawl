---
title: Bounded Octopool hydration
permalink: /octopool-hydration/
---

# Bounded Octopool hydration

`gitcrawl-hydrate` is an operator repair tool for an existing schema-16 native
archive. It leaves the ordinary `gitcrawl sync` command unchanged. Build it with:

```sh
go build -o gitcrawl-hydrate ./cmd/gitcrawl-hydrate
```

Use an approved staged archive and the existing Octopool caller credential:

```sh
./gitcrawl-hydrate --db /path/to/staged/native.sqlite \
  --repo owner/repository --numbers-file /path/to/numbers.txt --batch-size 20 \
  --inventory-checkpoint /path/to/inventory.sqlite
```

The number file contains positive issue/PR numbers separated by whitespace or
commas. Duplicate numbers are rejected. `OCTOPOOL_TOKEN`, `OCTOPOOL_POOL` (default
`maintainers`), and `OCTOPOOL_URL` (default `https://octopool.dev`) select the
existing relay. No direct GitHub fallback exists.

REST and conversation GraphQL reads retry pre-response connection resets,
incomplete response bodies (unexpected EOF or connection reset), and HTTP 424
`fallback_local` rejections whose typed reason is `relay_storage_unavailable`,
waiting 250 ms then 500 ms. Each read has at most three relay requests, shared with existing
REST content retries, and never extends its context deadline. Conversation
GraphQL handles the exact `github_response_too_large` reason as described below;
other relay rejection reasons remain terminal. Persistent failures preserve their bounded
diagnostic and leave the checkpoint available for a later controlled resume.

Large invocations stream a fresh REST issue inventory through a private SQLite
checkpoint. A repair that fits in one acquisition window (at most 25 numbers)
fetches only its selected issue records unless a checkpoint is supplied. REST
owns thread metadata, full bodies, raw objects, and backing issue IDs, including
for pull requests. Historical numbers absent from the completed inventory are
reported as unresolved after available records are hydrated. Deleted or
transferred records need a separate reconciliation decision; this tool never
removes them.

Conversation batches default to 20 and cannot exceed 20, keeping expanded
GraphQL selections below the relay admission limit. Batches retrieve complete
issue comments, submitted reviews, and inline comments through the
repository-scoped GraphQL relay. Inline comments are collected from
both reviews and review threads, including empty bodies. Larger conversations
fall back to the existing fully paginated REST readers through the same pool.
If Octopool rejects the response with HTTP 424 `fallback_local` and the exact
`github_response_too_large` reason, or the response exceeds the local 3 MiB
GraphQL envelope limit, the batch first requests only GraphQL identity and
update timestamps. After the same identity and freshness checks, it uses
those full REST readers for every child collection. Full thread fields remain
owned by the REST inventory; no body or child collection is clipped.
An exact oversized-response rejection from a REST page restarts that collection
from its first page at sizes 100, 10, then 1, following each response's own Link
chain. Partial page results are discarded before restarting, so resizing a later
page cannot skip or duplicate rows. A single oversized record remains an error;
other relay failures do not trigger page-size retries.
The tool rejects incomplete GraphQL responses, unqualified relay identities,
stale responses, conflicting duplicates, and moved or mismatched nodes. A compact
query or REST failure leaves the batch unwritten; other response-size limits
remain unchanged.

Native sync persists each complete conversation with its existing exclusion,
observation ordering, membership, raw JSON, and revision rules. Only the current
window of batches is cached. Previous successful batches remain available if a later batch
fails. Standard output contains one `batch_complete` event with the exact settled
numbers and native sync statistics after each successful batch. Retain these
receipts; counts and offsets are not safe completion checkpoints.
Owner exclusions are checked before acquisition and again by native sync.

A failed or interrupted batch has no completion receipt and can be retried.
Concurrent policy exclusions withhold the batch receipt rather than marking
excluded records complete.

This tool does not claim full repository coverage or advance a full-sync
checkpoint. It does not collect PR files, checks, commits, or review-thread state,
and does not claim complete PR enrichment. It does not activate delivery,
publish an archive, arm a source, install a scheduler, or migrate the database.
Keep the original seed and follow the archive owner's staging and delivery
procedure separately.

## Full-source reconciliation

Add `--include-discovered` to explicitly union the supplied historical numbers
with every number found in the fresh REST inventory. Without this flag, the tool
keeps the supplied selection. The inventory result reports its start and end
UTC timestamps, requested, available, selected, newly discovered, excluded, and
missing counts. Use the inventory start as the overlap anchor for subsequent
incremental acquisition; historical repository checkpoint dates do not qualify
this reconciliation.

In discovery mode, missing historical numbers are emitted individually as
`unresolved` results, remain untouched in the native archive, and do not prevent
present conversations from being processed. The command exits unsuccessfully
after processing available records if any historical number remains unresolved.
Small selected-number repairs fail before hydration if an input is missing.

The full inventory follows one creation-ascending chain of provider-issued
cursors through Octopool. It never constructs offset pages: GitHub rejects
large-dataset offsets. Each full REST page and its next cursor (or end-of-stream)
commit atomically to `--inventory-checkpoint` before hydration starts. Acquisition
overlaps hydration through a bounded page queue. A failed request preserves all
previous pages and native progress; it does not claim a complete inventory.

The checkpoint binds endpoint, pool, caller credential fingerprint, repository
name and immutable ID, query profile, original input selection, discovery mode,
and native file identity. It retains the original scan start on resume. Tokens
are not stored. Keep the checkpoint private and use one helper process per
checkpoint/native archive. A changed source, caller, selection or physical
archive is rejected rather than silently reusing another run's completion state.

Use `--fetch-workers 4` to acquire up to four batches concurrently after a small
pilot has qualified quota use; the limit is eight and the default is one. Only
one window of batches is held in memory. Native writes remain serial and ordered,
and only successfully persisted batches produce sync results. An acquisition
failure prevents that window from writing; previous successful windows remain.

Each batch compares GraphQL parent update timestamps with its REST inventory.
A mismatch refreshes the REST parent through the same pool and checks its immutable
identity. The batch is queried again only if the REST timestamp changed; a PR and
its backing issue can have different timestamps. Three changing REST attempts fail
visibly. This prevents an hours-old inventory row from accompanying newly fetched
child content; it does not claim an atomic snapshot across GitHub requests.

The launch controller must check and monitor disk headroom for the native
archive, PostgreSQL import, and backup to coexist. Keep the operator's chosen
reserve (for example, 25 GiB) available throughout the run and stop the owned
helper process if it is crossed. A retained-key count alone does not establish
complete discovery or classify unavailable records as deleted.

Resume an interrupted run with the same original number file, discovery flag,
checkpoint path and native archive. Do not subtract completed keys from that
input: the checkpoint records exact successful native batches and skips them on
replay. A crash between native commit and checkpoint completion can repeat one
bounded batch safely. Newer versions encountered later in the inventory may
produce another completion event for the same number. Controllers must count
unique numbers separately from batch events.

Standard output begins with `inventory_started` (`complete: false`,
`streaming: true`). Native `batch_complete` events can arrive before the final
`inventory` event. Only true cursor exhaustion produces that final event with
`complete: true`, the original start, final counts and unresolved historical
keys. Stderr reports saved or replayed page numbers. Preserve partial receipts
even when the run ends before the final inventory event.

Optionally pass `--selected-numbers-output /path/to/selected.txt`. At successful
cursor exhaustion, the tool writes the sorted selected roster to a new private
file and includes its SHA-256 in the final inventory event. The file must not
already exist; omit this optional output on resume if it was already produced.
Preserve the checkpoint, receipts and final roster until native
and downstream reconciliation is validated. Later discoveries belong to
incremental acquisition from the original inventory start with the chosen overlap.

The operator-only `gitcrawl-hydrate` exits with status 75 when every failed
acquisition in its window exhausted all three Octopool attempts on connection
resets or incomplete response bodies. Its supervisor may resume the same durable
checkpoint after that status.
Authentication, quota, storage, data, cancellation, deadline, and mixed failures
retain status 1; ordinary `gitcrawl` exit behavior is unchanged. A canceled sibling
is not another failure, but external cancellation always prevents status 75.
