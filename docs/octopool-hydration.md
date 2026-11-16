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

Each invocation reads one fresh, fully paginated REST issue inventory. That
inventory owns thread metadata, full bodies, raw REST objects, and backing issue
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
batch is cached. Previous successful batches remain available if a later batch
fails. Standard output contains one native sync result per completed batch.
Owner exclusions are checked before acquisition and again by native sync.

This tool does not claim full repository coverage or advance a full-sync
checkpoint. It does not collect PR files, checks, commits, or review-thread state,
and does not claim complete PR enrichment. It does not activate delivery,
publish an archive, arm a source, install a scheduler, or migrate the database.
Keep the original seed and follow the archive owner's staging and delivery
procedure separately.
