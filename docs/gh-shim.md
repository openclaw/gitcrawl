---
title: gh shim
nav_order: 12
permalink: /gh-shim/
---

# gh shim
{: .no_toc }

A `gh`-compatible binary that answers from local SQLite first and falls through to the real `gh` for everything else. The fastest way to cut GitHub API load across an agent fleet.
{: .fs-6 .fw-300 }

1. TOC
{:toc}

## What it is

The same `gitcrawl` binary serves a `gh`-compatible mode. Invoked as `gitcrawl gh ...`, or as `gh` / `gitcrawl-gh` via symlink, it intercepts read-only commands and serves them from the local mirror. Anything it cannot serve locally falls through to the real `gh` binary you already have installed, with a short persistent cache layered on top.

The shim never adds GitHub write behavior. Mutating commands (`gh issue close`, `gh pr merge`, `gh api -X POST ...`, `gh label create`, etc.) pass straight through to the real `gh`, increment a write counter, and clear the relevant cache entries on success.

## Install

```bash
# Side-by-side: agents opt in by calling `gitcrawl-gh`.
mkdir -p "$HOME/bin"
ln -sf "$(command -v gitcrawl)" "$HOME/bin/gitcrawl-gh"

# Or replace the global `gh` so every caller picks up the cache automatically.
REAL_GH="$(command -v gh)"              # capture this before shadowing gh
ln -sf "$(command -v gitcrawl)" "$HOME/bin/gh"
export GITCRAWL_GH_PATH="$REAL_GH"      # tell the shim where the real gh is
```

Make sure `~/bin` is on `PATH` before the original `gh` location if you want the shim to be picked up as `gh`. If `GITCRAWL_GH_PATH` is unset, the shim probes common Homebrew paths and then `PATH`. Set it explicitly when you replace the global `gh` so the shim does not recurse into itself.

## Supported local reads

### `gh search issues|prs`

```bash
gh search issues "download stalls" -R owner/repo --state open \
  --match comments --json number,title,url
gh search prs    "manifest cache" -R owner/repo --state open \
  --json number,title,url --limit 20
```

Answered from the local FTS index. Honors `--state`, `--json`, `--limit`. `--match` is accepted for parity (the local index already covers documents). Falls through if an unsupported filter combination is requested.

### `gh issue view` / `gh pr view`

```bash
gh issue view 123 -R owner/repo --json number,title,state,url,body,labels,author
gh pr view  123 -R owner/repo --json number,title,state,url,isDraft,author,headRef,baseRef
gh issue view https://github.com/owner/repo/issues/123 --json number,title,url
gh pr view https://github.com/owner/repo/pull/123 --json number,title,url
```

Full GitHub issue/PR URLs provide both the repository and thread number when
`-R`/`--repo` is omitted.

Supported JSON fields include `number`, `title`, `state`, `url`, `body`, `author`, `createdAt`, `updatedAt`, `closedAt`, `labels`, plus PR-specific `isDraft`, `headRef`, `baseRef`. PR detail fields (`files`, `commits`, `checks`, `statusCheckRollup`) are answered from cached PR detail and trigger [auto-hydration](#auto-hydration) on miss.

### `gh issue list` / `gh pr list`

```bash
gh issue list -R owner/repo --state open --search "hot loop" \
  --author octocat --label bug --label triage --json number,title,url
gh pr list    -R owner/repo --state open --search "manifest cache" \
  --assignee me --json number,title,url
```

Supports `--state`, `--search` (keyword search), `--author`, `--assignee`, repeated `--label`, `--limit`, and `--json`. Falls through for unsupported filters.

### `gh pr checks`

```bash
gh pr checks 123 -R owner/repo --json name,state,conclusion,detailsUrl
gh pr checks https://github.com/owner/repo/pull/123 --json name,state,conclusion
```

Returns the cached check/status summary for the PR. If the cached PR detail is older than 90 seconds or its head SHA is stale, [auto-hydration](#auto-hydration) refreshes it before answering. Supported fields: `name`, `state`, `status`, `conclusion`, `detailsUrl`, `workflow`, `startedAt`, `completedAt`.

Like `gh pr view`, a full pull request URL can supply both repository and
number.

### `gh run list` / `gh run view`

```bash
gh run list -R owner/repo --branch main --limit 20 \
  --json databaseId,workflowName,status,conclusion
gh run view 123456789 -R owner/repo --json status,conclusion,headSha
```

Workflow runs come from cached PR detail. Filters: `--branch`, `--commit` (head SHA). Supported fields: `databaseId`, `id`, `number`, `workflowName`, `name`, `displayTitle`, `status`, `conclusion`, `url`, `event`, `headBranch`, `headSha`, `createdAt`, `updatedAt`.

## Read-only fallthroughs (cached)

These commands always run real `gh` but the response body is cached for the next caller in the same workspace:

- `gh pr diff <number-or-url>` — keyed by the cached PR head SHA when available, so the cache is stable across many sequential agent reads; full PR URLs can omit `-R`
- `gh issue list/status/view`, `gh pr list/status/view/checks`, and unsupported read-only local shim shapes
- `gh release list/view`, `gh workflow list/view`, `gh secret list`, and `gh variable get/list`
- `gh project list/view/field-list/item-list`, `gh ruleset check/list/view`, `gh gist list/view`, and `gh org list`
- `gh repo view` / `gh repo list`
- `gh search code/commits/issues/prs/repos`
- `gh label list`
- `gh api <GET path>` — only `GET` requests for REST; never cached for `POST`/`PATCH`/`DELETE`/`PUT`.
- `gh api graphql` — cached only when the `query` field is a read-only query. Mutations, file-backed query fields, and `--input` calls pass through uncached.

Common Actions REST reads such as run status, job lists, and logs get Actions-aware TTLs.

Default cache TTLs are command-aware: active `gh run list` and run-status reads use `30s`; completed run views, completed Actions job lists, and run/job logs are kept for `12h`; completed run lists are kept for `30m`; workflow reads use `15m`; search reads use `15m`; release metadata uses `1h`; GitHub user profile reads use `7d`; read-only GraphQL queries use `6h`; GitHub Pages metadata uses `15m` to `30m`; tagged/SHA `contents` API reads use `7d`; `gh pr diff` uses `5m` without a stable SHA and `7d` with one. Most other read-only fallthroughs use `5m` to `10m`. Override with `GITCRAWL_GH_CACHE_TTL=5m` or similar.

Repeat read failures are cached by default too. That avoids a fleet of agents all rediscovering the same missing release, workflow, secret, or unsupported field. Error entries are capped to shorter lifetimes, and rate-limit errors are capped at `2m` so a reset is not masked all day. If GitHub returns a rate-limit error while refreshing an expired successful entry, the shim serves that stale success with a warning instead of failing the read. When another process is already refreshing an expired successful entry, peers can serve that stale entry within a short command-aware grace window instead of joining the backend stampede. Set `GITCRAWL_GH_STALE_GRACE=0` to disable stale-while-revalidate, or `GITCRAWL_GH_CACHE_ERRORS=0` to cache successful reads only.

## Auto-hydration

When a local issue or PR read misses the cache, the shim can auto-hydrate exactly one thread before falling back:

1. Shim detects a missing issue/PR row or stale PR detail (older than 90s, or head SHA mismatch)
2. If `GITCRAWL_GH_AUTO_HYDRATE != 0` (the default), runs `gitcrawl sync --numbers <n>` and adds `--with pr-details` for PR detail reads
3. Retries the local query against the freshly populated cache
4. Falls through to the real `gh` if hydration failed

This keeps `gh issue view`, `gh pr view`, `gh pr checks`, and `gh run` reads cheap and fresh without manual sync orchestration. Disable with `GITCRAWL_GH_AUTO_HYDRATE=0` if you want the shim to be strictly cache-or-fallthrough.

When the configured database comes from a portable store, auto-hydration writes to the local runtime mirror, not the Git checkout. Broad empty open-issue discovery is also guarded: if `gh issue list` or empty-query `gh search issues --state open` would return no rows but the repo only has targeted sync history, the shim falls through to the real `gh` instead of treating that incomplete local snapshot as authoritative.

## Cache inspection: `xcache`

```bash
gitcrawl gh xcache stats        # summary
gitcrawl gh xcache keys         # per-entry detail
gitcrawl gh xcache gc           # remove expired entries + stale lock files
gitcrawl gh xcache flush        # clear everything
gitcrawl gh xcache reset        # reset counters without deleting entries
gitcrawl gh xcache snapshot     # write a counter snapshot for later comparison
```

All accept `--json` for scripting. `stats` accepts `--since 1h` for recent-window counters. `snapshot` accepts `--reset` to checkpoint counters before a noisy release/debugging session.

`stats` JSON:

```json
{
  "cache_dir": "/Users/me/.config/gitcrawl/cache/gh-shim",
  "entries": 142,
  "expired": 6,
  "locks": 0,
  "bytes": 1841234,
  "cache_hits": 629,
  "total_reads": 641,
  "hit_rate_percent": 98.1,
  "counters": {
    "local_hits": 540,
    "fallback_hits": 88,
    "stale_hits": 1,
    "backend_misses": 12,
    "pass_through_writes": 4,
    "backend_misses_by_command": {
      "run view": 7,
      "api": 5
    },
    "backend_misses_by_route": {
      "api repos/:owner/:repo/actions/runs/:id/logs": 3
    },
    "backend_misses_by_key": {
      "api repos/openclaw/gitcrawl/actions/runs/123/logs -i": 2
    }
  },
  "commands": {
    "pr diff": { "entries": 30, "bytes": 184320 },
    "release view": { "entries": 14, "bytes": 18230 }
  }
}
```

`local_hits` are answered from SQLite; `fallback_hits` are answered from the fallthrough cache; `stale_hits` are expired successful cache entries served after a backend rate-limit response or while another process refreshes the key; `backend_misses` actually hit GitHub. The per-command, per-route, and per-key miss maps show which shapes still escape the cache, which is usually the fastest way to find the next optimization.

## Cache key composition

Cache keys are deterministic SHA-256 hashes of:

- A version tag (`v4`)
- The resolved gitcrawl config path
- The current working directory when the command depends on implicit repo resolution
- The `GH_HOST` env var
- The `GH_REPO` env var when the command relies on it for implicit repo resolution
- An explicit-scope marker for commands that include their own API path or repository
- For `gh pr diff`: the stable identity `pr-diff:owner/repo:number:head-sha` (when available)
- A canonicalized command argument vector, null-separated. Common equivalent forms such as `-R` vs. `--repo`, flag ordering, and `--json a,b` vs. `--json b,a` share the same cache key.

This isolates implicit repo reads in sibling checkouts while still coalescing explicit reads such as `gh api users/octocat`, `gh api repos/openclaw/openclaw/...`, and `gh repo view openclaw/gitcrawl` across those checkouts. Explicit reads ignore unrelated `GH_REPO` values so agents with different ambient repo settings still share cache entries when the command itself names the target. Concurrent cache misses use a lock file so one process populates the entry while peers wait for the result, instead of all of them firing at GitHub.

## What does not flow through the shim

- **Mutating commands** — `gh issue close`, `gh pr merge`, `gh pr comment`, `gh api -X POST`, etc. These pass straight through, increment `pass_through_writes`, and invalidate matching cache tags on success. Unknown mutation scope falls back to clearing all entries.
- **Auth flows** — `gh auth login`, `gh auth refresh`, etc. Always real `gh`.
- **Anything the shim does not recognize** — falls through unmodified.

## Agent integration

Pattern: replace `gh` with `gitcrawl-gh` (or symlink to `gh`) for every agent in the fleet, then keep your existing prompts and tools. Most read-only triage flows ("look up this issue", "check the PR status", "list open issues for this label") become local-only without any prompt changes.

For best results, schedule a periodic `gitcrawl refresh owner/repo` (every few minutes per repo, depending on activity) so the local mirror stays warm. The shim's `--sync-if-stale` (via `gitcrawl search`) and auto-hydration handle the rest.

See [Automation](/automation/) for full agent recipes and JSON contracts.
