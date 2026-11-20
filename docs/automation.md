---
title: Automation
nav_order: 14
permalink: /automation/
---

# Automation
{: .no_toc }

Stable JSON contracts, agent recipes, and patterns for keeping the local mirror warm without manual ceremony.
{: .fs-6 .fw-300 }

1. TOC
{:toc}

## JSON output is first class

Every command supports `--json` (or the global `--format json`). The resulting payload is pretty-printed with stable field names so you can pipe it directly into `jq` or feed it to an agent as structured context.

```bash
gitcrawl sync owner/repo --json | jq '{threads_synced, comments_synced, finished_at}'
gitcrawl clusters owner/repo --json --sort size --min-size 5 \
  | jq '.clusters[] | {id, members: .member_count, latest: .latest_thread_number}'
```

For the full per-command JSON shapes, see the individual feature pages and the [Commands reference](/commands/).

## Exit codes

- `0` — success
- non-zero — usage error, command not implemented, runtime error

Stderr always carries a human-readable error message; stdout is reserved for the requested output (text or JSON) so you can pipe stdout to `jq` without losing diagnostics.

## Keeping the mirror fresh

Three patterns, in increasing order of automation:

### On-demand staleness check

Use `--sync-if-stale` on `gitcrawl search`:

```bash
gitcrawl search issues "manifest cache" \
  -R owner/repo \
  --sync-if-stale 5m \
  --json number,title,url
```

Best for ad-hoc agent tools that should bound staleness but minimize sync calls.

### Periodic background refresh

Run `gitcrawl refresh owner/repo` on a cron, systemd timer, or `launchd` agent every few minutes per repo.

```cron
# Every 5 minutes, refresh the active repos.
*/5 * * * * $HOME/bin/gitcrawl refresh openclaw/gitcrawl --json > /tmp/gitcrawl.openclaw.json 2>&1
```

For multiple repos, loop in a small shell script — gitcrawl is happy to run sequentially against a shared SQLite file.

### Portable subscribers

After one-time initialization, schedule `portable refresh`, not repeated
`init` or `doctor`. It retains configuration and refuses unsafe checkout state:

```bash
gitcrawl --config /path/to/config.toml portable refresh \
  --expected-remote https://github.com/example/archive-store.git \
  --git /absolute/path/to/git --timeout 2m \
  --min-free-bytes 2147483648 --max-growth-bytes 2147483648 \
  --json > refresh.json 2> refresh.log
```

Treat nonzero exits as refusals requiring inspection. Do not automatically
delete locks, temporary packs, backups or SQLite sidecars and retry. A `partial`
result identifies an interrupted advancement, not a rollback. A
`mirror_result` of `preserved-local` means local runtime writes remain intact.
See [Portable stores](/portable-stores/#routine-subscriber-refresh) for the
admission rules, sampled growth budget and exact JSON contract.

## Agent recipes

### "Look up an issue without burning quota"

```bash
octopool gh api repos/owner/repo/issues/123
```

Octopool owns pooled GitHub reads. Use `gitcrawl search` first when local discovery is enough.

### "Find candidates, hydrate them, summarize"

```bash
NUMS=$(gitcrawl search issues "checksum mismatch" -R owner/repo \
        --json number --limit 30 \
        | jq -r '[.[].number] | join(",")')

gitcrawl sync owner/repo --numbers "$NUMS" --include-comments --with pr-details

gitcrawl cluster-detail owner/repo --id "$(gitcrawl clusters owner/repo --json \
        | jq '.clusters[0].id')"
```

Search is local; the targeted sync brings exactly the rows you need; cluster-detail returns the structured triage view.

### "Find duplicates around a new bug report"

```bash
NUM=789
gitcrawl sync owner/repo --numbers "$NUM" --include-comments
gitcrawl embed owner/repo --number "$NUM"
gitcrawl neighbors owner/repo --number "$NUM" --limit 10 --json
```

### "Triage a cluster end to end"

```bash
ID=42

# Read.
gitcrawl cluster-detail owner/repo --id "$ID" --body-chars 600 --json

# Decide canonical, then close locally.
gitcrawl set-cluster-canonical owner/repo --id "$ID" --number 123
gitcrawl close-cluster owner/repo --id "$ID" --reason "consolidated under #123"

# Comment upstream via real gh.
gh issue comment 456 -R owner/repo --body "Duplicate of #123"
```

### Shared GitHub cache

```bash
octopool login
octopool gh api repos/openclaw/openclaw/pulls/123
```

Gitcrawl no longer maintains a runtime `gh` command cache. Use Octopool for org-authenticated pooled GitHub reads, and keep using gitcrawl for local mirror/search/cluster automation.

## PostgreSQL-only acquisition

`acquire-api` collects full GitHub issue, pull request and published child rows
through Octopool into an existing PostgreSQL acquisition source. It does not
open a SQLite database, invoke `gh` or run model work.
The source must already have a completed baseline, acquisition preparation,
stable native ID mappings and child-observation allocation prepared by the
CrawlRemote owner. Original ID and observation high-waters must be reconciled
before those preparation steps.

```bash
gitcrawl acquire-api \
  --endpoint https://archive.example \
  --source github-example \
  --repo example/project \
  --publisher-token-env ARCHIVE_PUBLISHER_TOKEN \
  --reader-token-env ARCHIVE_READER_TOKEN \
  --since 2026-01-01T00:00:00Z \
  --timeout 10m --max-threads 100 --json
```

Supply separate, source-scoped publisher and reader credentials through the
named environment variables, and the existing `OCTOPOOL_TOKEN` caller session.
`OCTOPOOL_URL` and `OCTOPOOL_POOL` select the existing relay. The archive endpoint
requires HTTPS except for loopback tests. An explicitly selected `--config`
can supply the existing Cloudflare Access binding; archive database settings
are not consumed. No personal GitHub credential fallback is used.

The first incremental run requires `--since`. Later runs overlap the last
completed horizon by two minutes. PostgreSQL retains the original run scope,
observation timestamp, parent queue and child cursor after an interrupted or
bounded invocation. Repeating the command resumes that run; complete child
pages and their checkpoints commit together. A lost page acknowledgement gets
one retry with identical request bytes. Other rejections remain visible.
`complete: true` means acquisition finalized its requested horizon. Readers
explicitly configured for acquisition mode then select that completed run
automatically; partial runs remain invisible. One-time acquisition preparation
is required, but no per-run legacy snapshot preparation is needed. The horizon
is the collector's requested bound, not an independent provider coverage proof.

For a fixed historical selection, add `--numbers-file numbers.txt` containing
unique positive issue/PR numbers separated by whitespace or commas. Keep the
file and order unchanged across invocations. Each bounded historical run
retains its offset in PostgreSQL and preserves the previously completed
incremental horizon. Historical collection therefore requires an established
horizon. Incremental runs can occur between completed historical batches;
an active run must finish before another mode starts.

The default parent page contains up to 100 rows, cached for the current process;
only queued parents after a restart need an individual metadata refetch.
`--parent-page-size` and `--child-page-size` select 1–100 rows before a new run.
The default child page size is 20. Provider responses are bounded at 8 MiB;
acquisition writes are bounded at 500 changes and 4 MiB, including full raw
provider fields. Oversized rows/pages fail without advancing that checkpoint;
content is never clipped. The initial runner does not split an oversized child
page or shrink a recorded page size automatically. A missing selected parent
also fails visibly rather than silently dropping it from the selection.

## Multi-repo automation

A single `gitcrawl.db` can hold many repositories. Loop in shell:

```bash
for repo in openclaw/gitcrawl steipete/repo-a octocat/repo-b; do
  gitcrawl refresh "$repo" --json | jq '{repo: "'"$repo"'", sync: .sync, embed: .embed}'
done
```

Or maintain a small script that reads a list of repos from a file and runs them on a schedule.

## Output formats

| Format | When to use |
| --- | --- |
| `text` (default) | Humans at a terminal |
| `json` (or `--json`) | Pipelines, scripts, agents |
| `log` | Internal logging output; structured key/value pairs |

Force a format globally with `--format json` or per-command with `--json`. The `log` format is mostly used internally and is subject to change.

## CI integration

Run gitcrawl in CI to validate a portable store's freshness, sanity-check cluster shapes, or produce a triage report:

```yaml
- name: Refresh and snapshot clusters
  run: |
    gitcrawl init --portable-store $PORTABLE_STORE_URL
    gitcrawl refresh openclaw/gitcrawl --json > sync.json
    gitcrawl clusters openclaw/gitcrawl --json --sort size --min-size 5 > clusters.json
  env:
    GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
    OPENAI_API_KEY: ${{ secrets.OPENAI_API_KEY }}

- uses: actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a # v7.0.1
  with: { name: triage, path: "*.json" }
```

The artifact gives reviewers a structured view of what changed and how the cluster graph looks today.

## Best practices

- **Set both tokens in a single place.** Either env or `[env]` in `config.toml`. Mixing sources tends to confuse `doctor` reports.
- **Bound the staleness window.** `--sync-if-stale` on every agent-driven search is cheaper than a hot cron loop.
- **Re-cluster after a backfill.** A large `--state all` sync should be followed by `gitcrawl refresh --no-sync` (or just `gitcrawl embed && gitcrawl cluster`) so the durable graph reflects the new content.

### Selected parent-only repair

Use `acquire-api --parent-selection-file selection.json` with the same endpoint,
source and credential flags to refresh only audited parents through Octopool
GraphQL. Do not combine it with `--numbers-file` or `--since`. The file contains:

```json
{"schema":"gitcrawl-parent-selection/v1","repository":"example/project","parents":[{"number":7,"kind":"pull_request","backingIssueId":"123456789","nodeId":"PR_example"}]}
```

Use `issue` or `pull_request` for `kind`. `backingIssueId` is the original REST
issue ID, including for pull requests; the GraphQL pull-request database ID is a
different identity. `nodeId` is optional and, when supplied, must match. Select
identities from the reconciled original inventory, not from guessed numbers.

This mode requires an existing completed incremental horizon and the prepared
native ID and observation allocators. It cannot initialize coverage. It keeps
that horizon, fetches no children, and publishes parent rows marked with
incomplete child observation. Prior child rows and evidence remain retained.
Existing labels, assignees and local fields survive; newly unobserved collections
are JSON `null`, not empty lists. Raw metadata identifies the partial GraphQL
observation explicitly.

The ordered selection and original capture time are frozen in the PostgreSQL
run. Each accepted page persists its next selection offset. Reuse the unchanged
file to resume. `--max-threads` bounds each run; `--parent-page-size` controls
GraphQL batches up to 50. Response-size failures stop without advancing that
batch; retry with a smaller page size, including 1. An individual row exceeding
the 4 MiB acquisition limit is rejected without clipping. `complete: true`
means the selected run completed, not that comments or other history are complete.

### Selected lifecycle events through PostgreSQL

Use `acquire-api --lifecycle-numbers-file numbers.txt --since RFC3339` to collect
closure, reopen and merge timeline events for 1–500 existing PostgreSQL parents.
The number file uses the same comma/whitespace syntax as `--numbers-file`.
It cannot be combined with parent repair or conversation selection. Set
`--max-threads` at least as high as the selection size (maximum 500).

This mode uses the same explicit endpoint, source, reader/publisher environment
names and Octopool credentials as conversation acquisition. It reads full parent
rows at one completed snapshot and preserves their exact native ID, provider
node ID and sequence/ordinal pins. Missing parents stop collection; hydrate them
through the parent owner first. No SQLite archive or local checkpoint is opened.

The source must already have a completed acquisition horizon. Lifecycle work
keeps that horizon unchanged and retains events in `[since, horizon)`; it does
not establish repository-wide closure/merge completeness or collect commits,
security advisories, comments or reviews. Raw event objects retain missing
actors rather than substituting the parent author.

Each invocation stops after `--lifecycle-max-pages` provider pages (default 100)
or `--timeout`, preserving acknowledged progress in PostgreSQL. Resume with the
same number selection; an active run retains its original lower bound and
parent versions. `complete: false` means selected timelines still need another
invocation. The server's completed-run metadata resolves a lost final response:
repeating the same latest selection, window and parent versions returns its
completion without calling the provider again.


### Commit history through PostgreSQL

Use `acquire-api --commits --since RFC3339` with the same explicit API tokens and
Octopool configuration. The source must have an established acquisition horizon;
commit collection preserves that horizon. This mode cannot be combined with a
thread selection. It pins the repository identity, default branch OID and lower
bound in the existing acquisition run, then resumes its saved cursor.

Each atomic page carries one full commit and every associated-PR page, including
nullable merger identities, merge times, merge commit OIDs and repository names.
Full messages retain coauthor trailers. Attribution is preserved for the report
owner; the collector never substitutes author credit for unavailable merger
proof. It does not claim repository or report coverage.

`--commit-max-pages` defaults to 100. A commit with more than 100 attribution
pages or a request exceeding the existing 4 MiB page limit fails before that
commit's checkpoint advances. Provider response limits still apply; payloads
are never clipped. Incomplete runs stay invisible to the completed-only
`gitcrawl.report-window.commits` query. This mode opens no SQLite archive and
requires the server's commit migration and explicit role grants before use.
