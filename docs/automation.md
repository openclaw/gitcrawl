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

### Public-repository App authentication

An unattended supervisor can instead use an existing GitHub App installation.
Keep the App private key in a root-owned regular file (0400 or 0600), accessible
only to the rooted mint step. The nonroot collector never needs that key.

The machine-oriented mint command is:

```text
gitcrawl mint-acquisition-token --app-id ID --installation-id ID \
  --repo example/project --repository-id ID --private-key-file /secure/app-key.pem
```

**Its stdout is a credential.** Consume stdout through a private pipe in the
supervisor. Do not run it in a terminal or capture stdout in a journal, report,
shell trace or ordinary log. The command makes one mint attempt and writes only
JSON with `token`, `expires_at`, `app_id`, `installation_id`, `repository_id`,
`repository` and `permissions`. Errors omit response bodies and credentials.
An uncertain mint is not automatically retried.

Minting verifies the installation's App ID, repository owner and suspension
state, then verifies the repository's immutable ID and public visibility. It
requests exactly one repository ID and `metadata`, `contents`, `issues` and
`pull_requests` **read** permissions, even when the installation has write
access. A broader returned token is rejected. `private-*` names are excluded.
The operator remains responsible for an approved repository allowlist and the
matching prepared PostgreSQL source; public visibility alone is not admission.

For a supervisor rotating several archives in one organization, use
`--cohort-stdin` in place of `--repo` and `--repository-id`. Send a trusted
JSON allowlist of up to 500 `{ "id": 123, "name": "example/project" }` entries
through stdin. The command checks all requested names, immutable IDs and public
visibility against one paginated public catalog before minting. It then asks
for a separate, single-repository token for each entry. Stdout is a private
JSON array in input order. A failed lookup or mint returns an error without
a credential array. Earlier requests in a failed batch may have issued tokens.
The command does not repeat a mint. Keep each credential with its own prepared
PostgreSQL source and use the existing `acquire-api` per-repository quota checks.

Pass the trusted JSON envelope only to the collector's named environment
variable and add `--github-app-token-env ENV` to the existing `acquire-api`
command. Remove `OCTOPOOL_TOKEN`, `OCTOPOOL_URL` and `OCTOPOOL_POOL` from this
invocation. Use a protected runtime credential handoff such as systemd
`LoadCredential` for the short-lived envelope, not for the private key. A
nonroot runner can read that credential into only its Gitcrawl child's
environment. Never reuse the previous envelope when minting fails.

The token must outlast the entire `--timeout` by at least 60 seconds. For a
15-minute cycle, mint once immediately before each cycle. There is no token
refresh daemon or key path in the collector. It verifies the token's accessible
repository set and current public identity before collection; rejected or
expired credentials never fall back to Octopool or a personal login.

Direct reads use only `https://api.github.com`, reject redirects and preserve
half of each observed installation REST and GraphQL limit. Unknown or expired
quota fails closed. Each GraphQL query admits a conservative 100-point cost;
REST reads admit one point. Other installation consumers remain concurrent,
so this is an observed reserve, not an exclusive quota reservation. All cycle
modes use the same token and existing checkpoint/publication contracts.

### Run and completion boundaries

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

To enumerate repair candidates with recent activity, use
`discover-parents --repo owner/repo --updated-since RFC3339 --page-size 20 --json`.
This follows repository issues pages sorted by update time and includes old
issues and pull requests updated after the lower bound. It exports only native
backing IDs, numbers and kinds, plus the lower bound, observation time and cursor
receipt; provider bodies stay inside the reader. Resume with the same lower
bound, page size and returned cursor, retaining each successful output. Deduplicate
the combined selections by number and identity before admitting a fixed batch.

This is a current mutable inventory, not a historical snapshot or coverage proof.
A thread updated again after the intended report cutoff still belongs in the
repair cohort. Do not replace this inventory with an opening-date search or drop
such threads based on their latest update. The terminal cursor proves only that
this traversal ended; provider changes during traversal can still require a later
overlapping pass. A page or timeout bound leaves `complete: false` and its cursor
for continuation. No acquisition checkpoint, existing selection or report pin is
changed by discovery.
Validation, credential or repository-binding failures (including a bootstrap
timeout) emit no discovery receipt. Preserve the previously accepted cursor;
absence of output does not mean enumeration completed.

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

### Scheduled reporting turns

`acquire-api --cycle`
uses the same explicit endpoint and credential options. Each bounded invocation
dispatches one turn: incremental conversations, default-branch commits, or
lifecycle events. Add `--numbers-file history.txt` to include a historical
conversation batch after lifecycle work. PostgreSQL's active run
always resumes before another turn starts. Only a completed native receipt advances
the rotation; a new incremental horizon cannot overtake unfinished supplemental work.
The commit and lifecycle lower bound comes from that completed incremental run,
not the scheduler's clock. Supplemental turns preserve its horizon. A new bootstrap
`--since` must use at most microsecond precision. Existing parent runs with finer
bounds resume unchanged; supplemental scans round their lower bound down to the
preceding microsecond so no event is excluded by precision conversion. A legacy
empty window receives one microsecond of overlap rather than blocking later turns.

A scheduler can use `--cycle-turns 2..8` to run several turns for one source with
one provider session and one invocation deadline. Existing thread and page
limits apply per turn. After each terminal receipt the next turn reads the
PostgreSQL checkpoint again. The first unfinished or failed turn ends the
invocation. Multi-turn JSON returns a `turns` array of observed results. The
single-turn output remains the individual result object.

Keep the historical file unchanged once the archive records its digest or offset.
The cycle requires that file for retained historical progress and skips exhausted
history. For lifecycle events the cycle reads eligible
parent numbers from the completed PostgreSQL inventory in ascending batches of 100.
It retains the original pin and window across batches and restarts. Older issues
and PRs with recent changes are eligible. The provider timeline supplies the event
type and time. An empty candidate page advances to the next scheduled turn.

When a separate read service serves `gitcrawl.lifecycle.candidates`, set
`--lifecycle-reader-endpoint` to that origin. Only the candidate query uses the
alternate origin, with the same source and publication pin. It uses the acquisition
reader credential by default. For a separate reader audience, pass
`--lifecycle-reader-token-env NAME` with the name of its reader credential variable.
Parent validation and acquisition stay on `--endpoint`. An explicitly restricted
cycle can still use `--lifecycle-numbers-file` for a fixed selection of up to 500
parents. Resume any active fixed run with its original file before switching.

Use one scheduler and one source lock. `--timeout`, `--max-threads` and each mode's
page limit bound an invocation. `--max-threads` applies to conversation and
historical turns. Lifecycle uses `--lifecycle-max-pages` to bound work per invocation. Commit turns use
`--commit-max-pages`. `--commit-timeout` (default 10 minutes) bounds each run or
page commit request: the final page of a run waits while the server checks the
whole candidate snapshot before publication. Other acquisition API calls keep a
30 second bound, and `--timeout` still bounds the invocation. These limits do
not impose a deadline on the durable run:
large incremental inventories can require multiple invocations. `complete: false`
and `deferredModes` expose that condition. Do not start a second mode or discard its
checkpoint to obtain apparent fairness. Once the run completes, the other admitted
turns run before the next incremental scan. Provider failures preserve accepted pages
for the next invocation. A completed horizon alone is never all-family coverage proof.

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


Commit and lifecycle scans preserve the exact preceding parent checkpoint in
their immutable run scope. Switching back to conversation or parent-only work
restores its historical selection and offset. A completed legacy scan without
that saved checkpoint requires reconciliation; the collector never assumes
empty progress. An identical `--commits` retry may omit `--since` only to recover
the recorded completed run. Starting a new commit scan still requires `--since`.
Associated PR receipts include their repository and number so the PostgreSQL
owner can reject source-excluded associations before any progress is committed.
