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
| `gitcrawl init [--db --runtime-dir --portable-store --portable-db --store-dir --json]` | Create config and runtime directories; isolate all local paths under one root or clone a portable store | [Configuration](/configuration/#path-selection-edge-cases), [Portable stores](/portable-stores/) |
| `gitcrawl doctor [--json] [--locks]` | Health check for config, database, credentials, model selection, repo/thread counts, and optional SQLite lock diagnostics | [Configuration](/configuration/#gitcrawl-doctor) |
| `gitcrawl metadata [--json]` | Print the crawlkit command/control manifest for launchers and automation | — |
| `gitcrawl status [--json]` | Print read-only archive status, database inventory, and control state | — |
| `gitcrawl configure [--summary-model --embed-model --embedding-basis --json]` | Update model fields in `config.toml` | [Configuration](/configuration/#gitcrawl-configure) |
| `gitcrawl version` | Print version | — |

## Sync

| Command | Purpose | Docs |
| --- | --- | --- |
| `gitcrawl sync owner/repo [--state --since --numbers <refs> --limit --include-comments --include-pr-details --with pr-details --json]` | Sync issues and PRs from GitHub into local SQLite | [Sync](/sync/) |
| `gitcrawl coverage [owner/repo \| --repos owner/a,owner/b] [--min-missing-pr-details N --json]` | Report archive, PR-detail, and enrichment coverage/freshness | — |
| `gitcrawl fill-pr-details owner/repo [--limit --order --batch-size --reserve-rate-limit --include-comments --json-progress --json]` | Hydrate locally missing pull request detail rows in bounded batches | — |
| `gitcrawl refresh owner/repo [--with pr-details --no-sync --no-embed --no-cluster --strict-vectors ...]` | Wrapper that runs sync → embed → cluster | [Refresh and embed](/refresh-and-embed/) |
| `gitcrawl embed owner/repo [--number <ref> --limit --force --include-closed --json]` | Generate OpenAI embeddings for thread documents | [Refresh and embed](/refresh-and-embed/#embed) |
| `gitcrawl runs owner/repo [--kind sync\|embedding\|cluster --limit --json]` | List recorded run history | [Refresh and embed](/refresh-and-embed/#runs) |
| `gitcrawl code index owner/repo [--path --max-file-bytes --max-total-bytes --max-files --json]` | Index tracked text files from a local Git checkout | [Code indexing](/code-index/) |

`fill-pr-details --reserve-rate-limit` defaults to a floor of 1500 remaining
requests. Before every costful GitHub request, Gitcrawl reads `/rate_limit` and
stops when the latest shared-token snapshot shows that dispatching the next
request would cross that floor. The live probe observes other processes and
tools that share the token. This is best-effort because an unrelated consumer
can spend quota between the probe and request; the 1500 default provides
concurrency headroom. Pass `--reserve-rate-limit N` to choose another floor.

For an end-to-end first-run sequence that combines `status --json`, `doctor --json`, `sync --numbers`, bounded `--sync-if-stale` search, `gitcrawl runs`, and Octopool live reads, see the [maintainer archive workflow](/maintainer-archive/).

## Inspect

| Command | Purpose | Docs |
| --- | --- | --- |
| `gitcrawl threads owner/repo [--include-closed --numbers --limit --json]` | List threads from local cache | — |
| `gitcrawl search owner/repo --query <text> [--scope threads\|code\|all --mode keyword\|semantic\|hybrid --limit --json]` | Local thread/source search (direct mode) | [Search](/search/) |
| `gitcrawl search issues\|prs <query> -R owner/repo [--state --json --limit --sync-if-stale]` | Local search (`gh search` shape) | [Search](/search/#gh-search-compatibility-mode) |
| `gitcrawl neighbors owner/repo --number <ref> [--limit --threshold --json]` | Vector-similar threads to a specific issue/PR | [Clustering](/clustering/#find-similar-threads-neighbors) |

## Thread References

Commands that accept a thread number also accept thread references:

- bare numbers: `123`
- hash references: `#123`
- path references: `issues/123`, `pull/123`
- scoped references: `owner/repo#123`
- full GitHub issue or pull request URLs

This applies to `sync --numbers`, `threads --numbers`, `summarize --number`, `embed --number`,
`neighbors --number`, all governance `--number` flags, and TUI jump input.

## Summaries and embeddings

| Command | Purpose |
| --- | --- |
| `gitcrawl summarize owner/repo [--number --limit --force --include-closed --json]` | Generate compact key summaries for current thread revisions |
| `gitcrawl embed owner/repo [--number --limit --force --include-closed --json]` | Generate thread vectors using the configured embedding basis |

## Cluster

| Command | Purpose | Docs |
| --- | --- | --- |
| `gitcrawl cluster owner/repo [--threshold --min-size --max-cluster-size --k --cross-kind-threshold --limit --model --basis --include-closed --strict-vectors --json]` | Build durable clusters from vectors | [Clustering](/clustering/#generate-clusters) |
| `gitcrawl clusters owner/repo [--sort size\|recent\|oldest --min-size --limit --hide-closed --json]` | Latest-run cluster summary, merged with closed durable rows | [Clustering](/clustering/#list-clusters) |
| `gitcrawl clusters-report owner/repo [--sort size\|recent\|oldest --min-size --limit --member-limit --body-chars --hide-closed --json]` | Markdown or JSON report for top display clusters | [Clustering](/clustering/#cluster-report) |
| `gitcrawl durable-clusters owner/repo [--include-closed --sort --min-size --limit --json]` | Strict durable-cluster audit view | [Clustering](/clustering/#list-clusters) |
| `gitcrawl cluster-detail owner/repo --id <n> [--source auto\|run\|durable --member-limit --body-chars --hide-closed --json]` | Cluster + members detail | [Clustering](/clustering/#inspect-a-cluster) |
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
| `gitcrawl tui [owner/repo] [--min-size --sort --layout --limit --hide-closed --json]` | Interactive cluster browser; `--json` emits a snapshot instead of launching the UI | [TUI](/tui/) |

## gh shim

`gitcrawl gh` moved to Octopool and now exits with a migration note.

Use:

```bash
octopool login
octopool gh api repos/openclaw/openclaw/pulls/123
```

`gitcrawl search issues|prs ...` remains the local mirror search path.

## Portable stores

| Command | Purpose | Docs |
| --- | --- | --- |
| `gitcrawl portable prune [--body-chars --no-vacuum --json]` | Build a compact portable v2 backup and (optionally) `VACUUM` for publishing | [Portable stores](/portable-stores/#publishing-gitcrawl-portable-prune) |

## Not yet implemented

These appear in `SPEC.md` but currently return a "not implemented" error. They are reserved for future versions:

`key-summaries`, `merge-clusters`, `split-cluster`, `export-sync`, `import-sync`, `validate-sync`, `portable-size`, `sync-status`, `optimize`, `completion`

If you need any of these to land sooner, [open an issue](https://github.com/openclaw/gitcrawl/issues).
