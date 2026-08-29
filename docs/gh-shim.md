---
title: gh shim
nav_order: 12
permalink: /gh-shim/
---

# gh shim
{: .no_toc }

`gitcrawl gh` moved to Octopool.
{: .fs-6 .fw-300 }

Gitcrawl no longer owns a GitHub CLI compatibility cache. Gitcrawl remains the local per-repo mirror, portable store, search, clustering, and triage tool. Octopool owns the org-authenticated shared GitHub cache and pooled read relay.

## Existing Octopool setup

Keep local discovery in Gitcrawl, then use the existing bare PATH `gh` for fresh GitHub metadata:

```bash
gitcrawl threads owner/repo --numbers 123 --include-closed --json
```

```bash
gh pr view 123 -R owner/repo --json number,title,state,url,isDraft,headRefName,headRefOid
```

```bash
gh pr checks 123 -R owner/repo --json name,state,bucket,link
```

Archive state can lag GitHub; verify current state before a final maintainer action. Keep narrow JSON reads on the existing Octopool-backed `gh` shim so they use its shared cache. Do not bypass it with an absolute path to the real GitHub CLI.

The retired `gitcrawl gh` exits `2`; this migration notice is not an authentication failure. If the shim already works, do not run `octopool login` or change tokens, authentication, PATH, or configuration to replace those recipes. The old Gitcrawl `pr status` readiness/exit-code contract, `--live`/`--cached` flags, and `GITCRAWL_GH_*` cache controls do not apply to bare `gh`. Use native field names: `headRefName`/`headRefOid` for PR refs and `bucket`/`link` for checks, rather than `headRef`/`headSha` or `conclusion`/`detailsUrl`.

## First-time Octopool setup

Only for an operator setting up a new Octopool installation:

```bash
octopool login
octopool gh api repos/openclaw/openclaw/pulls/85341
```

To make existing `gh api ...` callers use Octopool, install or symlink the Octopool binary as `gh` or `octopool-gh`.

Unsupported or mutating commands fall through to the real GitHub CLI from Octopool. `gitcrawl gh ...` now fails with a migration note instead of maintaining a second cache.

## Still in gitcrawl

- `gitcrawl search issues|prs ...` keeps answering from the local SQLite mirror.
- `gitcrawl sync ... --with pr-details` keeps hydrating repo-local PR detail used by search, clustering, and TUI workflows.
- portable stores remain Git-backed repo snapshots; they do not carry the runtime `gh` command cache.
