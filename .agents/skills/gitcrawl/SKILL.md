---
name: gitcrawl
description: Use for local GitHub issue/PR archive search, sync freshness, clusters, durable maintainer triage, handoff to Octopool-backed gh reads, and Gitcrawl repo/release work.
---

# Gitcrawl

Use local archive data first for GitHub issue and pull request questions. Browse
or hit live GitHub APIs only when the local archive is stale, missing the
requested scope, or the user asks for current external context.

## Sources

- Config: resolve with `gitcrawl doctor --json`; fresh macOS installs default to
  `~/Library/Application Support/gitcrawl/config.toml`, while Linux honors
  `${XDG_CONFIG_HOME:-~/.config}/gitcrawl/config.toml`
- DB: resolve with `gitcrawl doctor --json`; fresh macOS installs default to
  `~/Library/Application Support/gitcrawl/gitcrawl.db`, while Linux honors
  `${XDG_DATA_HOME:-~/.local/share}/gitcrawl/gitcrawl.db`; portable-store
  installs may point at a configured store database instead of the default local
  DB
- Cache: resolve with `gitcrawl init --json` for new installs or from
  `cache_dir` in config; fresh macOS installs default to
  `~/Library/Caches/gitcrawl`, while Linux honors
  `${XDG_CACHE_HOME:-~/.cache}/gitcrawl`
- Vectors: resolve with `gitcrawl init --json` for new installs or from
  `vector_dir` in config; fresh macOS installs default to
  `~/Library/Application Support/gitcrawl/vectors`, while Linux honors
  `${XDG_DATA_HOME:-~/.local/share}/gitcrawl/vectors`
- Legacy installs may still resolve config, database, cache, vectors, and logs
  under `~/.config/gitcrawl/`; check the active config before assuming a
  migrated path.
- Repo: `openclaw/gitcrawl`; on ClawSweeper this is checked out at `~/clawsweeper-workspace/gitcrawl`
- Preferred CLI: `gitcrawl`; fallback to `go run ./cmd/gitcrawl` from a verified repo checkout if the installed binary is stale

## Freshness

For recent/current questions, check freshness before analysis:

```bash
gitcrawl doctor --json
```

Routine refresh:

```bash
gitcrawl doctor
gitcrawl refresh owner/repo
```

Targeted refresh:

```bash
gitcrawl sync owner/repo --numbers 123,456 --with pr-details
```

`--with pr-details` hydrates PR files, commits, checks, workflow runs, and
review-thread resolution state in the local archive. Use it when the review
needs those details; it does not populate Octopool's separate `gh` cache.

For agent-driven discovery, prefer bounded freshness:

```bash
gitcrawl search issues "query" -R owner/repo --state open --sync-if-stale 5m --json number,title,url
```

## Query Workflow

1. Resolve scope: owner/repo, issue/PR number, cluster id, keyword, label, author, state, or date range.
2. Check freshness for recent/current requests.
3. Use CLI for normal reads; use read-only SQL for precise counts/rankings.
4. Report absolute date spans, repo names, issue/PR numbers, cluster ids, and known gaps.

Common commands:

```bash
gitcrawl search issues "query" -R owner/repo --state open --json number,title,url
gitcrawl clusters owner/repo --sort size --min-size 5
gitcrawl cluster-detail owner/repo --id <id>
gitcrawl threads owner/repo --numbers 123 --include-closed --json
```

For an exact PR, read the archive with `threads` first. `--include-closed`
keeps closed or merged candidates in scope; archive state is not proof of
current GitHub state. Then use bare PATH `gh` for fresh metadata when needed,
including before final merge/comment decisions:

```bash
gh pr view 123 -R owner/repo --json number,title,state,url,isDraft,headRefName,headRefOid
```

Drill down into checks only when needed:

```bash
gh pr checks 123 -R owner/repo --json name,state,bucket,link
```

Keep the existing Octopool-backed `gh` shim and use narrow JSON field lists so
supported reads share its cache. Native `gh` uses `headRefName`/`headRefOid`
for PR refs and `bucket`/`link` for check classification and URLs, not the old
Gitcrawl field names.

`gitcrawl gh` is removed and exits `2` with a migration note. Do not retry its
`status`, `view`, or `checks` recipes, pass its `--live`/`--cached` flags to
`gh`, or rebuild an older Gitcrawl to recover that cache. This is a command
migration, not an authentication failure: do not run `octopool login`, change
tokens, auth, PATH, or config, or bypass the existing shim to repair it. See
[the gh migration guide](../../../docs/gh-shim.md) for ownership and first-time setup.

## SQL

`gitcrawl` does not currently expose a first-class `sql` command. For exact
local archive counts or rankings, use SQLite read-only mode against the
configured DB and prefer CLI commands for normal reads.

Useful examples:

```bash
db="$(gitcrawl doctor --json | jq -r .db_path)"
sqlite3 -readonly "$db" \
  "select count(*) as threads from threads;"
sqlite3 -readonly "$db" \
  "select r.full_name, count(*) as threads from threads t join repositories r on r.id = t.repo_id group by r.full_name order by threads desc limit 20;"
sqlite3 -readonly "$db" \
  "select state, count(*) as threads from threads group by state;"
```

Do not run mutating SQL against the archive. Use local maintainer commands for
overrides instead of writing database rows directly.

When the installed CLI lacks a new feature, build or run from
a verified `openclaw/gitcrawl` checkout before concluding the feature is missing.

## Maintainer Boundaries

`close-thread`, `close-cluster`, exclusions, and canonical-member choices are
local maintainer overrides; they do not write back to GitHub. Use bare PATH
`gh` for authorized GitHub writes; Octopool handles fallback to the real CLI.
Gitcrawl no longer owns the `gh` shim or its configuration.

## Verification

For repo edits, prefer existing Go gates:

```bash
GOWORK=off go test ./...
```

Then run targeted CLI smoke for the touched surface, for example:

```bash
gitcrawl doctor --json
gitcrawl status --json
gitcrawl search issues "test" -R openclaw/gitcrawl --state open --limit 5
gitcrawl threads owner/repo --numbers 123 --include-closed --json
gh pr view 123 -R owner/repo --json number,title,state,url,headRefOid
```
