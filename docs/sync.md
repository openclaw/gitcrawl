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
- `thread_revisions` — immutable revisions with content-addressed canonical evidence payloads when fully hydrated thread or review evidence changes
- `thread_fingerprints` — one deterministic `thread-fingerprint-v2` row for each persisted revision
- `documents` — canonical thread documents (when bodies change)
- `sync_runs` — sync run statistics and retry checkpoints

Revision and fingerprint production fails closed on incomplete evidence. Issues require
`--include-comments`; pull requests require both `--include-comments` and
`--include-pr-details` (or `--with pr-details`). Without that hydration, sync still
updates the thread and document rows but intentionally does not create a revision that
could appear complete while omitting discussion, review, file, commit, or check evidence.
Pull request revisions also track draft state, review decisions, workflow runs, and check
transitions, so those changes invalidate downstream summaries and review evidence even
when the title and body are unchanged. Summary prompts read the exact canonical payload
bound to the selected revision and skip revisions whose payload is missing or too large.

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
| `--with pr-metadata` | PR object, including merge attribution, head/base references, and diff counts |
| `--include-pr-details` | PR object, files, commits, status checks, workflow runs, review threads |
| `--with pr-details` | Same as `--include-pr-details` (gh-style flag) |
| `--progress-file <absolute-path>` | Atomically publish sanitized machine-readable activity |

`pr-metadata` writes only `pull_request_details`, using the normal per-thread
observation ordering. It does not fetch, clear, or mark files, commits, checks,
workflow runs, or review-thread resolution as fresh. Comments remain independent:
add `--include-comments` when needed. Selecting both hydration modes uses full
`pr-details` hydration. Metadata-only hydration does not create full PR revisions
or fingerprints. It resolves PR metadata-fetch failures, not failures of omitted
child collections.

Full PR details also populate `pull_request_files`, `pull_request_commits`,
`pull_request_checks`, and `github_workflow_runs` for local review and search.

`fill-pr-details` selects PRs without a metadata row; it does not upgrade existing
metadata-only rows. To upgrade them, use
`gitcrawl sync owner/repo --numbers 123,456 --with pr-details`
(and `--include-comments` for full revision evidence).

Review-thread and nested-comment pagination fails if GitHub claims another page
but returns an empty or previously followed `endCursor`. Sync reports a
`missing endCursor` or `repeated endCursor` error instead of repeatedly fetching
the same page. Retry after the GitHub API or proxy returns advancing cursors;
the incomplete review-thread response is not saved as complete evidence.

Use `gitcrawl coverage [owner/repo] --json` to inspect archive completeness after a sync. It reports issue, PR, comment, and review counts alongside hydrated PR detail rows, missing PR details, known failed hydrations, and detail-table row counts per repository. The additive `enrichment` object exposes supported, eligible, covered, fresh, missing, stale, completeness, ratios, and latest timestamps for revisions, fingerprints, key summaries, clusters, and PR details. Use `--repos owner/a,owner/b` to compare selected repositories and `--min-missing-pr-details N` to focus backfill work on repositories with gaps.

`gitcrawl sync-failures owner/repo --json` lists unresolved issue, comment, and PR hydration failures with their operation, error class and message, timestamps, and retry count. Add `--include-resolved` to inspect failures cleared by a later successful hydration of that same family. This operational ledger stays local when `portable prune` runs unless the publisher explicitly passes `--include-sync-failures`, which retains the ledger only after replacing every error message with a redaction marker.

## Partial failures

Each completed issue or PR commits atomically with its requested children,
document, revision, fingerprint, and failure resolutions. A failed item does not
roll back completed siblings. Shared-head workflow observations are consolidated
before those writes, so sibling ordering cannot replace a newer snapshot.
A consolidation failure excludes only its shared-head group, records those
items as failed PR-detail hydrations, and preserves unrelated completed items.

An item fetch failure records its actual operation: `issue`, `issue_comments`,
`pull_reviews`, `pull_review_comments`, `pull_review_threads`,
`pull_request_metadata`, or `pull_request_details`. A failed issue lookup does
not create a thread stub. Failed child fetches can retain the observed parent
metadata, but do not replace incomplete child collections or certify complete
evidence. Cancellation stops further work; transactions already committed remain.
A quota-reserve failure also stops new acquisition, including quota probes.
Completed payloads can still commit. Skipped requests are not recorded as
failures; shared-head groups that still need verification remain uncommitted.

An item transaction failure records each requested operation after rollback,
with only an existing parent reference when available. It does not recreate the
rolled-back item. A retry resolves only the families it actually persists:
metadata-only retries leave comments and full-detail failures unresolved.

An incomplete batch exits nonzero and never records a successful sync or advances
the closed-sweep watermark. Before partial writes, archives without a recorded
watermark retain their previous retry lower bound as a `checkpoint` in
`sync_runs`. A new archive uses the default 24-hour lower bound. This checkpoint
is not successful list coverage or freshness. Retry the failed numbers with the
same hydration flags after resolving the reported cause.
Older binaries can read these archives but do not honor the retry checkpoint
when writing. Do not downgrade the writer to resume a partially completed sync.

`--include-code` is accepted for compatibility but is currently a no-op.

## Machine-readable progress

Automation may pass an absolute `--progress-file` path. Gitcrawl atomically
replaces that private file as synchronization advances, then leaves the
terminal snapshot in place:

```json
{
  "schema": "gitcrawl.sync-progress.v1",
  "repository": "owner/repo",
  "state": "running",
  "stage": "syncing_threads",
  "issues_received": 56,
  "pull_requests_received": 238,
  "comments_received": 350,
  "observed_at": "2026-07-31T04:17:36.000000000Z"
}
```

`state` is `running`, `succeeded`, or `failed`; `stage` is `connecting`,
`syncing_threads`, or `finalizing`. Counts describe upstream activity observed
by the current process, not rows committed to SQLite. The snapshot never
contains credentials, issue content, request URLs, or raw diagnostics.

## Limit and pagination

```bash
gitcrawl sync owner/repo --limit 200
```

`--limit` caps the number of rows fetched in this invocation. The underlying GitHub paginator surfaces total page counts in run records and honors GitHub's `Retry-After` and rate-limit response headers, so partial syncs interrupted by rate limiting resume cleanly.

REST pagination stops with an error if a `next` link returns to an already
fetched page. This prevents repeated requests and avoids treating a cyclic
response as a complete page collection. Retry when GitHub or the proxy returns
advancing links.

## JSON output

The result reports processed thread and hydration counts, not separate insert
and update counts. For a compact view:

```bash
gitcrawl sync owner/repo --json \
  | jq '{repository, threads_synced, issues_synced, pull_requests_synced, comments_synced, metadata_only, started_at, finished_at}'
```

```json
{
  "repository": "owner/repo",
  "threads_synced": 124,
  "issues_synced": 100,
  "pull_requests_synced": 24,
  "comments_synced": 0,
  "metadata_only": true,
  "started_at": "2026-05-05T07:30:11Z",
  "finished_at": "2026-05-05T07:30:43Z"
}
```

The full result also includes PR-detail and enrichment counters, closure and
stale-observation counts, the requested scope when present, and the database
write destination. Use `gitcrawl runs owner/repo --kind sync --json` for
recorded run IDs.

After partial persistence, `sync --json` still emits committed counts, but exits
nonzero and leaves progress marked `failed`. `fill-pr-details --json` likewise
reports committed `filled` and remaining selected items, including a partially
completed batch. A quota stop exits nonzero with
`stopped_reason: "rate-limit-reserve"`; other sync failures use `"sync-failed"`.
The partial batch and final result retain the stopping request's quota snapshot
without a subsequent credential or quota lookup.
Automation must check the exit status, not treat a JSON result as success.

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
