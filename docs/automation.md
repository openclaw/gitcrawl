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
gitcrawl sync owner/repo --json | jq '{run_id, inserted, updated}'
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

- uses: actions/upload-artifact@v4
  with: { name: triage, path: "*.json" }
```

The artifact gives reviewers a structured view of what changed and how the cluster graph looks today.

## Best practices

- **Set both tokens in a single place.** Either env or `[env]` in `config.toml`. Mixing sources tends to confuse `doctor` reports.
- **Bound the staleness window.** `--sync-if-stale` on every agent-driven search is cheaper than a hot cron loop.
- **Re-cluster after a backfill.** A large `--state all` sync should be followed by `gitcrawl refresh --no-sync` (or just `gitcrawl embed && gitcrawl cluster`) so the durable graph reflects the new content.
