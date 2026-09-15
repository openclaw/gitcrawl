---
title: Repository metrics
nav_order: 16
permalink: /metrics/
---

# Repository metrics

`gitcrawl metrics` collects repository headline counters and stable release events
into a separate, private SQLite database. It does not sync the thread archive,
refresh a portable store, generate embeddings, call a model, or start a scheduler.

## Configuration and commands

Create a metrics JSON config. `database` must be an absolute filesystem path to a
new file or an existing Gitcrawl metrics database, outside your archive and portable
store. The example path is illustrative; choose a private directory on your host.

```json
{
  "database": "/private/metrics/gitcrawl/metrics.sqlite",
  "targets": [
    {"entity": "OpenClaw", "target": "openclaw/openclaw"},
    {"entity": "Hermes", "target": "NousResearch/hermes-agent"}
  ]
}
```

```sh
gitcrawl metrics collect --config /private/metrics/github.json --json
gitcrawl metrics import --config /private/metrics/github.json --json < history.ndjson
gitcrawl metrics status --config /private/metrics/github.json --json
gitcrawl help metrics
```

The config is independent of `config.toml` and `GITCRAWL_DB`. For these commands,
`--config` selects the metrics JSON file; it can appear before `metrics` or after
the subcommand. Global output flags and command-local `--json` work normally.

Public counters and releases can be read without authentication. Authenticated
collection uses `GITHUB_TOKEN`, then the normal native `gh auth token` resolver.
An optional `tokenEnv` selects another environment variable. A global
`--github-token-command /absolute/executable` selects the normal managed credential
provider exclusively on supported platforms. Tokens never appear in results.
The metrics config contains no token values; cookie authentication is not used.

## What is collected

Each invocation observes both configured targets at a single UTC timestamp. Run
`collect` hourly with an external scheduler when hourly history is required.

| Metric | Meaning |
| --- | --- |
| `stars` | Repository `stargazers_count` |
| `forks` | Repository `forks_count`; changes between snapshots are net changes |
| `watchers` | Actual subscribers, `subscribers_count`; **not** `watchers_count`, which aliases stars |
| `open_prs` | Open pull-request search count, only when results are complete |
| `open_issues` | Combined repository issue/PR count minus the valid open-PR count |
| `clones` | Optional daily clone counts from the authenticated traffic endpoint |

Missing, invalid, or incomplete required counts remain SQL `NULL`; they never
become zero. If the PR count is unavailable or exceeds the combined count,
`open_issues` remains unknown. Real zeroes and decreases are retained. Individual
fork creation events are not crawled.

Clone traffic requires suitable repository access. HTTP 403/404 means optional
unavailability and does not fail otherwise healthy collection. Only completed UTC
days are recorded: `ts` is that day's final millisecond and `observed_at` is the
actual read time. Unchanged daily values are not re-appended; corrected values
receive a new sequence. Sum only the latest observation for each day, never all
revisions. GitHub's traffic window limits how far a missed day can be backfilled.

Stable releases exclude drafts and prereleases. All release pages are read;
events use stable GitHub release IDs so repeated collection is idempotent.
`published_at` is preferred, with `created_at` as the fallback for older records.

## Storage, imports, and failures

New database files are private (`0600`). Existing files must identify themselves
with `metric_meta.owner = gitcrawl` and `metric_meta.version = 1`. Databases with
foreign tables, another owner/version, database symlinks, and pre-existing empty
files are rejected before a writable open. `status` checks identity read-only and
does not create a missing database. Never point this config at the thread archive.

The delivery tables are:

- `metric_observations(sequence, id, entity, target, metric, kind, ts, value,
  observed_at, provenance)` — counters and daily observations; `value` is nullable.
- `metric_events(sequence, id, entity, target, kind, ts, label, url, observed_at,
  provenance)` — release history.
- `metric_runs(sequence, ts, status, rows_written)` — completed collection attempts.

Observation and event sequences advance independently. Read each table using its
own delivery cursor. Daily revisions supersede by latest sequence. Counter values
are snapshots, not increments; derive net change from consecutive observations.

Import accepts one JSON object per line on stdin:

```json
{"type":"metric","id":"history:github:watchers:1","entity":"OpenClaw","target":"openclaw/openclaw","metric":"watchers","kind":"counter","ts":"2026-09-14T00:00:00Z","value":null,"observed_at":"2026-09-15T00:00:00Z","provenance":"historical-import"}
{"type":"event","id":"history:github:release:1","entity":"Hermes","target":"NousResearch/hermes-agent","kind":"release","ts":"2026-09-14T00:00:00Z","label":"v1","url":"https://github.com/NousResearch/hermes-agent/releases/tag/v1","observed_at":"2026-09-15T00:00:00Z","provenance":"historical-import"}
```

IDs are required and idempotent within each destination table. Imported explicit
IDs preserve distinct observations even when values match. `entity` and `target`
must match a configured pair. Import validates every row and commits the whole
input atomically; a malformed or out-of-scope late row rolls everything back.
Daily imports must identify a day completed before `observed_at`'s UTC day.

On a partial source failure, successful reads and explicit unknown counter values
are committed together with a `partial` run; stdout contains the result and the
command exits nonzero. A canceled collection gets a bounded opportunity to retain
already completed reads, without starting another network request. Diagnostics
never include GitHub response bodies or credential output.

This command does not install schedules, migrate another application's history,
change existing refresh jobs, or publish the private database. Those are explicit
operator/integration responsibilities.
