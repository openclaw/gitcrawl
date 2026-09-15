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
    {"entity": "Example", "target": "example/project"}
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

## Isolated source-built installation

A source-built metrics runtime can coexist with an official Gitcrawl installation
that refreshes a portable mirror. Give the metrics binary a private, versioned
directory and invoke that exact executable. The installed metrics CLI path is:

```text
$HOME/.local/share/gitcrawl/metrics-runtimes/<source-commit>/gitcrawl
```

Copy an already validated artifact into a new directory; do not overwrite an
existing version. Record its full source commit and expected SHA-256 from the
validation handoff, verify the hash before and after copying, and use directory
mode `0700` and executable mode `0500`. This installation does not replace a
`current` symlink, the command on `PATH`, an archive config, or a refresh job.

Local source builds do not require official release signing credentials under the
[installation policy](/installation/#install-from-source). On macOS, verify the
source artifact's signature with `codesign --verify --strict` before and after
copying. This is source-build verification, not official release notarization;
official release signing and notarization remain the [release workflow's](/releasing/)
responsibility. Do not change signing policies or remove quarantine to force an
untrusted artifact to run.

The installation check is read-only and uses the separately provided metrics config:

```sh
metrics_revision=SOURCE_COMMIT
metrics_binary="$HOME/.local/share/gitcrawl/metrics-runtimes/$metrics_revision/gitcrawl"
"$metrics_binary" --version
"$metrics_binary" metrics status \
  --config "$HOME/.local/share/gitcrawl/metrics.json" --json
```

Keep a machine-local installation receipt at the path returned by
`git rev-parse --git-path metrics-runtime-installation.json`. Record the exact
resolved CLI/config/database paths, source commit, artifact hash, signature result,
read-only status, and preservation checks there. Git metadata keeps these private
host details out of the public documentation and PR. The coordinator's handoff
should contain the same exact CLI path. Installation alone does not authorize
collection, imports, scheduling, or a final cutover.

## Hourly collection on macOS

After authorizing local collection, create a separate user LaunchAgent that calls
the pinned native binary directly. Use absolute paths; launchd does not expand
`~`, `$HOME`, or shell variables in a plist. Keep the config and logs outside Git
and shared publication. Give their directories mode `0700` and files mode `0600`.
Create both log files before bootstrapping the job.

The following is a template for `~/Library/LaunchAgents/org.openclaw.gitcrawl.metrics.plist`.
Replace `/Users/you` and `SOURCE_COMMIT` with your actual installation paths:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>org.openclaw.gitcrawl.metrics</string>
  <key>ProgramArguments</key>
  <array>
    <string>/Users/you/.local/share/gitcrawl/metrics-runtimes/SOURCE_COMMIT/gitcrawl</string>
    <string>metrics</string><string>collect</string>
    <string>--config</string><string>/Users/you/.local/share/gitcrawl/metrics.json</string>
    <string>--json</string>
  </array>
  <key>WorkingDirectory</key><string>/Users/you/.local/share/gitcrawl</string>
  <key>EnvironmentVariables</key>
  <dict>
    <key>HOME</key><string>/Users/you</string>
    <key>PATH</key><string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin</string>
    <key>GITCRAWL_NO_UPDATE_CHECK</key><string>1</string>
  </dict>
  <key>StartCalendarInterval</key><dict><key>Minute</key><integer>6</integer></dict>
  <key>KeepAlive</key><false/>
  <key>Umask</key><integer>63</integer>
  <key>StandardOutPath</key><string>/Users/you/.local/share/gitcrawl/metrics-logs/stdout.log</string>
  <key>StandardErrorPath</key><string>/Users/you/.local/share/gitcrawl/metrics-logs/stderr.log</string>
</dict>
</plist>
```

The job runs hourly at minute 6 in the host's local time, while observations use
UTC. `63` is the decimal representation of umask `0077`. The explicit minimal
`PATH` supports the existing native `gh` credential resolver without interactive
shell initialization. Do not put credentials in the plist. The versioned runtime
is not auto-updated; `GITCRAWL_NO_UPDATE_CHECK` also disables release notices.

Validate and bootstrap this new job once, then request one immediate collection:

```sh
metrics_plist="$HOME/Library/LaunchAgents/org.openclaw.gitcrawl.metrics.plist"
plutil -lint "$metrics_plist"
launchctl bootstrap "gui/$(id -u)" "$metrics_plist"
launchctl kickstart "gui/$(id -u)/org.openclaw.gitcrawl.metrics"
launchctl print "gui/$(id -u)/org.openclaw.gitcrawl.metrics"
```

Wait for the job to exit and check its last exit code, private logs, and the latest
`metric_runs` row using read-only SQLite. Confirm all five required counters for
every configured target have non-NULL values at that run's timestamp. A successful
bootstrap or a PID alone is not collection evidence. An unavailable optional clone
report is not a required-counter failure. On a provider failure, inspect the
recorded result before retrying; `KeepAlive` is disabled to avoid rapid restarts.
Existing archive refresh jobs are independent and need no changes.

`collect` and `import` hold a nonblocking native OS lock on
`<database>.writer.lock` from before database initialization through database
close. A second writer exits nonzero before collection or import; `status` remains
read-only and available. Ownership is released even if the process is killed.
The private lock file remains in place: never delete or replace it while writers
can run. Use the locking runtime for every writer; older binaries do not honor
this lock.

## What is collected

Each invocation observes all configured targets at a single UTC timestamp. Run
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
{"type":"event","id":"history:github:release:1","entity":"Example","target":"example/project","kind":"release","ts":"2026-09-14T00:00:00Z","label":"v1","url":"https://github.com/example/project/releases/tag/v1","observed_at":"2026-09-15T00:00:00Z","provenance":"historical-import"}
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
