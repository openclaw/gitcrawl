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
  --repo owner/repository --numbers-file /path/to/numbers.txt --batch-size 25
```

The number file contains positive issue/PR numbers separated by whitespace or
commas. Duplicate numbers are rejected. `OCTOPOOL_TOKEN`, `OCTOPOOL_POOL` (default
`maintainers`), and `OCTOPOOL_URL` (default `https://octopool.dev`) select the
existing relay. No direct GitHub fallback exists.

Large invocations read one fresh, fully paginated REST issue inventory. A repair
that fits in one acquisition window (at most 25 numbers) fetches only its selected
issue records. That inventory
owns thread metadata, full bodies, raw REST objects, and backing issue
IDs, including for pull requests. A requested number absent from the inventory
fails visibly; historical deleted or transferred records need a separate
reconciliation decision. The tool never removes them.

Batches retrieve complete issue comments, submitted reviews, and inline comments
through the repository-scoped GraphQL relay. Inline comments are collected from
both reviews and review threads, including empty bodies. Larger conversations
fall back to the existing fully paginated REST readers through the same pool.
The tool rejects incomplete GraphQL responses, unqualified relay identities,
stale responses, conflicting duplicates, and moved or mismatched nodes. A large
response fails with no batch write; reduce the batch size before retrying.

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
Ordinary selected-number mode fails before hydration if an input is missing.

The full REST inventory uses the same worker bound for creation-ascending,
numbered pages through Octopool. It assembles pages in order and stops only at a
verified short or empty tail. Provider cursor links are validated but not
constructed; numbered-page equivalence is qualified separately. Duplicate
identities, failed pages, or contradictory tails fail without publishing a
partial inventory. The inventory itself remains in memory and restarts after an
interruption; completed hydration still uses the frozen roster and receipts.

Use `--fetch-workers 4` to acquire up to four batches concurrently after a small
pilot has qualified quota use; the limit is eight and the default is one. Only
one window of batches is held in memory. Native writes remain serial and ordered,
and only successfully persisted batches produce sync results. An acquisition
failure prevents that window from writing; previous successful windows remain.

Each batch compares GraphQL parent update timestamps with its REST inventory.
Changed parents are refreshed through the same pool, their immutable identities
are checked, and the batch is queried again. Three changing attempts fail visibly.
This prevents an hours-old inventory row from accompanying newly fetched child
content; it does not claim an atomic snapshot across GitHub requests.

The launch controller must check and monitor disk headroom for the native
archive, PostgreSQL import, and backup to coexist. Keep the operator's chosen
reserve (for example, 25 GiB) available throughout the run and stop the owned
helper process if it is crossed. A retained-key count alone does not establish
complete discovery or classify unavailable records as deleted.

For a resumable discovery run, pass `--selected-numbers-output /path/to/selected.txt`.
The file must not already exist. Before hydration begins, the tool writes the
sorted selected roster with private permissions and includes its SHA-256 in the
inventory event. Preserve that file, the inventory start, the batch receipts,
and their binding to the same native archive. For a retry, subtract completed
receipt numbers from this frozen roster into a new input and omit
`--include-discovered`. Otherwise a new discovery union would add completed keys
back. Later discoveries belong to incremental acquisition from the original
inventory start with the chosen overlap.
