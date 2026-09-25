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

Source schema 14 adds these fields and the actor/coverage evidence tables.
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
verified watermark by five minutes. It paginates without GitHub search's hit cap,
hydrates only relevant updated threads, and commits page checkpoints only after
native conversation collection succeeds. Up to sixteen two-thread requests progress independently per
page. Persistently failing transient requests split into smaller batches,
preserving the transport and complete-evidence requirement. Non-nested
continuation pages use 100 nodes to avoid repeated small round trips. Identity/profile enrichment uses eight disjoint 100-node requests with normal
quota guards and can run concurrently under
the same source owner. `--once` performs one resumable update cycle.

Hydration workers reuse the watch owner's open store instead of repeating
full-archive migration checks for each batch. Independent guarded clients overlap
network work; native transactions still coordinate source writes. Enrichment
continues checking its queues every two minutes after the initial drain. New
observations enqueue unresolved content identities or known actor profiles
directly, avoiding repeated whole-archive scans during watch operation.

A completed historical discovery receipt supplies the initial coverage baseline;
a later completed update cycle advances it. Fresh collector checks do not imply
complete historical/provider coverage. `analytics_coverage` exposes the verified
watermark independently of source row observation times.

The permanent `runner.lock` coordinates ownership with the full-history backfill
supervisor. Do not unlink it or start a second supervisor against the same archive.
A finished backfill should be disabled as an automatic startup job when ongoing
collection takes ownership. The token-command result is held only in memory and
refreshed before its expected expiry, preserving one credential across quota
reservation and dispatch. Existing provider-rate protections remain active.

See [sync](/sync/), [configuration](/configuration/) and the [command reference](/commands/).
