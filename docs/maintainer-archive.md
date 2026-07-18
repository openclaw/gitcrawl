---
title: Maintainer archive workflow
nav_order: 13
permalink: /maintainer-archive/
---

# Maintainer archive workflow
{: .no_toc }

A first-run checklist for maintainers who want a local SQLite mirror for issue and pull request triage, with Octopool handling pooled live GitHub reads.
{: .fs-6 .fw-300 }

1. TOC
{:toc}

## Start with local health

Initialize or verify the runtime before spending GitHub quota:

```bash
gitcrawl init
gitcrawl status --json
gitcrawl doctor --json
```

`status --json` is the quick inventory check: it reports the configured archive database, its repository/thread/open-thread/cluster inventory under `databases[].counts`, and the last successful sync time. Use it to confirm that your local archive is present and to decide whether a refresh is needed.

`doctor --json` is the setup check: it reports config path, database health, credential discovery, model settings, portable-store status, and the same core counts. Use it when a run fails, when a machine has multiple config paths, or when an agent needs a machine-readable readiness gate.

## Bring one repository local

Run an initial metadata sync, then decide whether comments and PR details are worth hydrating immediately:

```bash
gitcrawl sync openclaw/gitcrawl --state open --json
gitcrawl sync openclaw/gitcrawl --numbers 94,95,96 --include-comments --with pr-details --json
```

The first command mirrors open issues and pull requests into SQLite. The targeted `sync --numbers` command refreshes exact issues or pull requests after local discovery, so you can hydrate only the rows you intend to inspect.

Use `--include-comments` when issue discussion matters. Use `--with pr-details` for pull requests that need files, commits, checks, workflow runs, and review-thread state. Those detail rows are useful for review work, but they cost more GitHub API calls than metadata-only sync.

Child hydration is lossless by default. Gitcrawl merges comments, reviews, review threads, and pull-request commit references by their stable GitHub identity; an item missing from one response is not treated as deleted. Deletion therefore requires an explicit sourced record rather than inference from GitHub list responses. Explicit deletion records remain as tombstones with their reason, edited comments and review threads retain revisions, and a later live observation restores their current row without discarding that revision history. Commit references are immutable identities, so they retain tombstone state but do not have an edit-revision table.

## Search with bounded staleness

Use local search for discovery and add a staleness bound when an agent or script should refresh before answering:

```bash
gitcrawl search issues "archive coverage" \
  -R openclaw/gitcrawl \
  --state open \
  --sync-if-stale 5m \
  --json number,title,url,updatedAt,labels
```

`--sync-if-stale` runs one metadata sync only when the local mirror is older than the duration you provide. The search result still comes from SQLite, so repeated searches stay local once the freshness window is satisfied.

## Inspect recent work

Use run history to understand what populated the archive:

```bash
gitcrawl runs openclaw/gitcrawl --kind sync --limit 5 --json
gitcrawl runs openclaw/gitcrawl --kind embedding --limit 5 --json
gitcrawl runs openclaw/gitcrawl --kind cluster --limit 5 --json
```

`gitcrawl runs` shows recent pipeline records by kind. Pair it with `status --json` when deciding whether an archive is fresh enough for triage, or with `doctor --json` when a sync succeeded but downstream search, embedding, or clustering output looks stale.

## Decide when to hydrate

For a small candidate set, hydrate exact rows:

```bash
NUMS=$(gitcrawl search prs "needs proof" -R openclaw/gitcrawl \
  --state open \
  --sync-if-stale 5m \
  --json number \
  | jq -r '[.[].number] | join(",")')

if [ -n "$NUMS" ]; then
  gitcrawl sync openclaw/gitcrawl --numbers "$NUMS" --include-comments --with pr-details --json
fi
```

For broad backlog sweeps, keep the first pass metadata-only. Hydrate comments and PR details after you have narrowed the candidate list. This keeps local triage responsive and avoids consuming review-thread, check-run, and workflow-run quota for rows you will not inspect.

## Know the Octopool boundary

Gitcrawl owns the local SQLite mirror, search, clustering, TUI, and read-only JSON control surfaces. Octopool owns pooled live `gh` reads:

```bash
octopool login
octopool gh api repos/openclaw/gitcrawl/issues/82
```

Use `gitcrawl search` and `gitcrawl cluster-detail` for local discovery. Use Octopool, or the real `gh` CLI when you need to write, for final live verification, comments, labels, PR creation, and other GitHub mutations.

## First-run checklist

1. Run `gitcrawl init`, then `gitcrawl doctor --json`.
2. Run `gitcrawl status --json` and confirm the database path is the one you expect.
3. Sync the target repository metadata with `gitcrawl sync owner/repo --state open --json`.
4. Search locally with `--sync-if-stale` when freshness matters.
5. Use `sync --numbers` to hydrate only the candidate issues or pull requests you are actively triaging.
6. Check `gitcrawl runs owner/repo --kind sync --json` if freshness or last-run status is unclear.
7. Use Octopool or real `gh` for final live GitHub reads and all write actions.
