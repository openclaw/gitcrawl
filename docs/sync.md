---
title: Sync
nav_order: 6
permalink: /sync/
---

# Sync
{: .no_toc }

Bring GitHub issues and pull requests into local SQLite. Idempotent, incremental, and tunable per workflow.
{: .fs-6 .fw-300 }

1. TOC
{:toc}

## The default

```bash
gitcrawl sync owner/repo
```

This fetches **open** issues and pull requests for the repository. To keep local state from rotting, an incremental sync also sweeps recently closed items so that issues and PRs closed between runs are reflected locally.

A sync writes:

- `repositories` — repo metadata
- `threads` — issues and PRs (titles, bodies, authors, author association, labels, state, timestamps)
- `thread_revisions` — immutable revisions when fully hydrated canonical thread or review evidence changes
- `thread_fingerprints` — one deterministic `thread-fingerprint-v2` row for each persisted revision
- `documents` — canonical thread documents (when bodies change)
- `run_records` — sync run statistics

Revision and fingerprint production fails closed on incomplete evidence. Issues require
`--include-comments`; pull requests require both `--include-comments` and
`--include-pr-details` (or `--with pr-details`). Without that hydration, sync still
updates the thread and document rows but intentionally does not create a revision that
could appear complete while omitting discussion, review, file, commit, or check evidence.
Pull request revisions also track draft state, review decisions, workflow runs, and check
transitions, so those changes invalidate downstream summaries and review evidence even
when the title and body are unchanged.

## State filters

```bash
gitcrawl sync owner/repo --state open    # default
gitcrawl sync owner/repo --state closed  # only closed
gitcrawl sync owner/repo --state all     # full backfill
```

`--state all` is the right choice for a one-shot historical backfill on a new repository. After that, the default `--state open` (with its closed sweep) is enough for ongoing freshness.

## Time-windowed sync

```bash
gitcrawl sync owner/repo --since 2026-04-01T00:00:00Z
```

`--since` accepts an RFC 3339 timestamp and limits the GitHub query to threads updated after that point. Combine with `--state` to scope tightly:

```bash
gitcrawl sync owner/repo --state all --since 2026-04-01T00:00:00Z
```

## Exact rows

```bash
gitcrawl sync owner/repo --numbers 123,456 --include-comments
gitcrawl sync owner/repo --numbers https://github.com/owner/repo/issues/123 --with pr-details
```

`--numbers` is the safest way to refresh specific issues or PRs — it bypasses list ordering and the updated-time window, fetching exactly the rows you ask for. Pair it with `--include-comments` and/or `--include-pr-details` to hydrate the conversation and PR-only data at the same time.

`--numbers` accepts comma-separated thread references, not just integers:
`123`, `#123`, `issues/123`, `pull/123`, `owner/repo#123`, and full GitHub
issue or pull request URLs.

## Hydration depth

| Flag | What it adds |
| --- | --- |
| `--include-comments` | Issue comments, PR review comments, reviews |
| `--include-pr-details` | PR files, commits, status checks, workflow runs |
| `--with pr-details` | Same as `--include-pr-details` (gh-style flag) |

PR details land in `pr_files`, `pr_commits`, `pr_checks`, and `pr_runs` tables for local review, search, clustering, and TUI workflows.

Use `gitcrawl coverage [owner/repo] --json` to inspect archive completeness after a sync. It reports issue, PR, comment, and review counts alongside hydrated PR detail rows, missing PR details, and detail-table row counts per repository. The additive `enrichment` object exposes supported, eligible, covered, fresh, missing, stale, completeness, ratios, and latest timestamps for revisions, fingerprints, key summaries, clusters, and PR details. Use `--repos owner/a,owner/b` to compare selected repositories and `--min-missing-pr-details N` to focus backfill work on repositories with gaps. Known failed or skipped hydration attempts are reported as unavailable until the separate failure ledger is present.

`--include-code` is accepted for compatibility but is currently a no-op.

## Limit and pagination

```bash
gitcrawl sync owner/repo --limit 200
```

`--limit` caps the number of rows fetched in this invocation. The underlying GitHub paginator surfaces total page counts in run records and honors GitHub's `Retry-After` and rate-limit response headers, so partial syncs interrupted by rate limiting resume cleanly.

## JSON output

```bash
gitcrawl sync owner/repo --json
```

```json
{
  "repository": "owner/repo",
  "state": "open",
  "since": "",
  "selected": 124,
  "inserted": 12,
  "updated": 9,
  "deleted": 0,
  "comments_inserted": 0,
  "comments_updated": 0,
  "reviews_inserted": 0,
  "pr_files_inserted": 0,
  "pr_commits_inserted": 0,
  "run_id": 42,
  "started_at": "2026-05-05T07:30:11Z",
  "finished_at": "2026-05-05T07:30:43Z"
}
```

## Common workflows

### First-time setup for a repo

```bash
gitcrawl sync owner/repo --state all --include-comments
gitcrawl embed owner/repo
gitcrawl cluster owner/repo
```

Or in one step:

```bash
gitcrawl refresh owner/repo --include-comments
```

### Periodic incremental refresh

```bash
gitcrawl sync owner/repo
```

The closed sweep keeps the open list honest without paying for a full backfill.

### Pull a specific issue + comments + PR detail

```bash
gitcrawl sync owner/repo --numbers 123 --include-comments --with pr-details
```

### Refresh a batch you got from search

```bash
NUMS=$(gitcrawl search issues "manifest cache" -R owner/repo --json number --limit 20 \
        | jq -r '[.[].number] | join(",")')
gitcrawl sync owner/repo --numbers "$NUMS" --with pr-details
```

## Required credentials

`sync` requires a GitHub token. gitcrawl resolves it from `GITHUB_TOKEN`, the `[env]` table in `config.toml`, or from `gh auth token` if the real `gh` CLI is installed and authenticated. `gitcrawl doctor` reports the source.

## See also

- [Refresh and embed](/refresh-and-embed/) — the wrapper that runs sync, embed, and cluster end to end
- [gh shim migration](/gh-shim/) — Octopool owns pooled `gh` reads now
- [Portable stores](/portable-stores/) — sharing the synced cache across machines
