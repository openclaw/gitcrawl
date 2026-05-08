---
title: Commands reference
nav_order: 15
permalink: /commands/
---

# Commands reference
{: .no_toc }

Complete CLI surface, one row per command. Use as a lookup table; deep documentation lives in the feature pages.
{: .fs-6 .fw-300 }

1. TOC
{:toc}

## Global flags

These work on every command.

| Flag | Default | Description |
| --- | --- | --- |
| `--config <path>` | `$GITCRAWL_CONFIG` or default | Override config path |
| `--format text\|json\|log` | `text` | Output format |
| `--json` | _(off)_ | Shorthand for `--format json` |
| `--no-color` | _(off)_ | Suppress ANSI color |
| `--version` | _(off)_ | Print version and exit (global only) |
| `--help` / `-h` | — | Print usage |

## Setup

| Command | Purpose | Detailed docs |
| --- | --- | --- |
| `gitcrawl init [--db --portable-store --portable-db --store-dir --json]` | Create config, database, runtime directories; optionally clone a portable store | [Installation](/installation/), [Portable stores](/portable-stores/) |
| `gitcrawl doctor [--json]` | Health check for config, database, credentials, model selection, repo/thread counts | [Configuration](/configuration/#gitcrawl-doctor) |
| `gitcrawl metadata [--json]` | Print the crawlkit command/control manifest for launchers and automation | — |
| `gitcrawl status [--json]` | Print read-only archive status, database inventory, and control state | — |
| `gitcrawl configure [--summary-model --embed-model --embedding-basis --json]` | Update model fields in `config.toml` | [Configuration](/configuration/#gitcrawl-configure) |
| `gitcrawl version` | Print version | — |

## Sync

| Command | Purpose | Docs |
| --- | --- | --- |
| `gitcrawl sync owner/repo [--state --since --numbers <refs> --limit --include-comments --include-pr-details --with pr-details --json]` | Sync issues and PRs from GitHub into local SQLite | [Sync](/sync/) |
| `gitcrawl refresh owner/repo [--no-sync --no-embed --no-cluster ...]` | Wrapper that runs sync → embed → cluster | [Refresh and embed](/refresh-and-embed/) |
| `gitcrawl embed owner/repo [--number <ref> --limit --force --include-closed --json]` | Generate OpenAI embeddings for thread documents | [Refresh and embed](/refresh-and-embed/#embed) |
| `gitcrawl runs owner/repo [--kind sync\|embedding\|cluster --limit --json]` | List recorded run history | [Refresh and embed](/refresh-and-embed/#runs) |

## Inspect

| Command | Purpose | Docs |
| --- | --- | --- |
| `gitcrawl threads owner/repo [--include-closed --numbers --limit --json]` | List threads from local cache | — |
| `gitcrawl search owner/repo --query <text> [--mode keyword\|semantic\|hybrid --limit --json]` | Local search (direct mode) | [Search](/search/) |
| `gitcrawl search issues\|prs <query> -R owner/repo [--state --json --limit --sync-if-stale]` | Local search (`gh search` shape) | [Search](/search/#gh-search-compatibility-mode) |
| `gitcrawl neighbors owner/repo --number <ref> [--limit --threshold --json]` | Vector-similar threads to a specific issue/PR | [Clustering](/clustering/#find-similar-threads-neighbors) |

## Thread References

Commands that accept a thread number also accept thread references:

- bare numbers: `123`
- hash references: `#123`
- path references: `issues/123`, `pull/123`
- scoped references: `owner/repo#123`
- full GitHub issue or pull request URLs

This applies to `sync --numbers`, `threads --numbers`, `embed --number`,
`neighbors --number`, all governance `--number` flags, gh-shim
`issue/pr view`, `pr checks`, `pr diff`, and TUI jump input. In gh-shim
`view`/`checks`/`diff`, a full GitHub URL also supplies `owner/repo`, so
`-R owner/repo` is optional.

## Cluster

| Command | Purpose | Docs |
| --- | --- | --- |
| `gitcrawl cluster owner/repo [--threshold --min-size --max-cluster-size --k --cross-kind-threshold --limit --model --basis --include-closed --json]` | Build durable clusters from vectors | [Clustering](/clustering/#generate-clusters) |
| `gitcrawl clusters owner/repo [--sort size\|recent\|oldest --min-size --limit --hide-closed --json]` | Latest-run cluster summary, merged with closed durable rows | [Clustering](/clustering/#list-clusters) |
| `gitcrawl durable-clusters owner/repo [--include-closed --sort --min-size --limit --json]` | Strict durable-cluster audit view | [Clustering](/clustering/#list-clusters) |
| `gitcrawl cluster-detail owner/repo --id <n> [--member-limit --body-chars --hide-closed --json]` | Cluster + members detail | [Clustering](/clustering/#inspect-a-cluster) |
| `gitcrawl cluster-explain owner/repo --id <n> [...]` | Alias for `cluster-detail` | [Clustering](/clustering/#inspect-a-cluster) |

## Governance

| Command | Purpose | Docs |
| --- | --- | --- |
| `gitcrawl close-thread owner/repo --number <ref> [--reason --json]` | Local close on a thread | [Governance](/governance/#local-close) |
| `gitcrawl reopen-thread owner/repo --number <ref> [--json]` | Inverse | — |
| `gitcrawl close-cluster owner/repo --id <n> [--reason --json]` | Local close on a cluster | [Governance](/governance/#local-close) |
| `gitcrawl reopen-cluster owner/repo --id <n> [--json]` | Inverse | — |
| `gitcrawl exclude-cluster-member owner/repo --id <n> --number <ref> [--reason --json]` | Pull a thread out of a cluster | [Governance](/governance/#member-exclusion) |
| `gitcrawl include-cluster-member owner/repo --id <n> --number <ref> [--reason --json]` | Inverse | — |
| `gitcrawl set-cluster-canonical owner/repo --id <n> --number <ref> [--reason --json]` | Pin canonical thread for a cluster | [Governance](/governance/#canonical-member) |

## TUI

| Command | Purpose | Docs |
| --- | --- | --- |
| `gitcrawl tui [owner/repo] [--min-size --sort --limit --hide-closed --json]` | Interactive cluster browser; `--json` emits a snapshot instead of launching the UI | [TUI](/tui/) |

## gh shim

| Command | Purpose | Docs |
| --- | --- | --- |
| `gitcrawl gh search issues\|prs <query> -R owner/repo [...]` | Local-first `gh search` | [gh shim](/gh-shim/) |
| `gitcrawl gh issue view <n-or-url> [-R owner/repo] --json <fields>` | Local-first thread view | [gh shim](/gh-shim/) |
| `gitcrawl gh pr view <n-or-url> [-R owner/repo] --json <fields>` | Same, for PRs (with auto-hydration) | [gh shim](/gh-shim/) |
| `gitcrawl gh issue list -R owner/repo [--state --search --author --assignee --label --json]` | Local-first list | [gh shim](/gh-shim/) |
| `gitcrawl gh pr list -R owner/repo [...]` | Same, for PRs | [gh shim](/gh-shim/) |
| `gitcrawl gh pr checks <n-or-url> [-R owner/repo] --json <fields>` | Cached PR checks (auto-hydrates if stale) | [gh shim](/gh-shim/) |
| `gitcrawl gh pr diff <n-or-url> [-R owner/repo]` | Falls through; cached by head SHA | [gh shim](/gh-shim/) |
| `gitcrawl gh run list -R owner/repo [--branch --commit --json]` | Cached workflow runs | [gh shim](/gh-shim/) |
| `gitcrawl gh run view <run-id> -R owner/repo [--json]` | Same, single run | [gh shim](/gh-shim/) |
| `gitcrawl gh repo view\|list ...` | Falls through; cached briefly | [gh shim](/gh-shim/) |
| `gitcrawl gh release list\|view ...` | Falls through; cached briefly | [gh shim](/gh-shim/#read-only-fallthroughs-cached) |
| `gitcrawl gh workflow list\|view ...` | Falls through; cached briefly | [gh shim](/gh-shim/#read-only-fallthroughs-cached) |
| `gitcrawl gh secret list ...` / `variable get\|list ...` | Falls through; cached briefly | [gh shim](/gh-shim/#read-only-fallthroughs-cached) |
| `gitcrawl gh label list ...` | Falls through; cached briefly | [gh shim](/gh-shim/) |
| `gitcrawl gh api <GET path>` | Falls through; cached briefly (GET-only REST) | [gh shim](/gh-shim/) |
| `gitcrawl gh api graphql -f query=...` | Falls through; read-only queries are cached | [gh shim](/gh-shim/#read-only-fallthroughs-cached) |
| `gitcrawl gh xcache stats [--since <duration>] \| keys \| gc \| flush \| reset \| snapshot [--reset] [--json]` | Cache inspection / housekeeping | [gh shim](/gh-shim/#cache-inspection-xcache) |
| _Anything else_ | Falls through to real `gh` | [gh shim](/gh-shim/) |

The shim binary can be installed standalone by symlinking the `gitcrawl` binary as `gh` or `gitcrawl-gh`.

## Portable stores

| Command | Purpose | Docs |
| --- | --- | --- |
| `gitcrawl portable prune [--body-chars --no-vacuum --json]` | Build a compact portable v2 backup and (optionally) `VACUUM` for publishing | [Portable stores](/portable-stores/#publishing-gitcrawl-portable-prune) |

## Not yet implemented

These appear in `SPEC.md` but currently return a "not implemented" error. They are reserved for future versions:

`summarize`, `key-summaries`, `merge-clusters`, `split-cluster`, `export-sync`, `import-sync`, `validate-sync`, `portable-size`, `sync-status`, `optimize`, `completion`

If you need any of these to land sooner, [open an issue](https://github.com/openclaw/gitcrawl/issues).
