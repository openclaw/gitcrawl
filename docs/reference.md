---
title: Reference
nav_order: 16
permalink: /reference/
---

# Reference
{: .no_toc }

Lookup tables for paths, environment variables, and defaults.
{: .fs-6 .fw-300 }

1. TOC
{:toc}

## Paths

| Path | Purpose |
| --- | --- |
| `~/.config/gitcrawl/config.toml` | Configuration file |
| `~/.config/gitcrawl/gitcrawl.db` | SQLite database |
| `~/.config/gitcrawl/cache/` | Caches (PR detail, gh-shim fallthrough) |
| `~/.config/gitcrawl/cache/gh-shim/` | gh-shim fallthrough cache |
| `~/.config/gitcrawl/vectors/` | Vector store backing embeddings |
| `~/.config/gitcrawl/logs/` | Operational logs |
| `~/.config/gitcrawl/portable/` | Portable-store checkout (when configured) |

Override the config root with `--config <path>` or `GITCRAWL_CONFIG`.

## Environment variables

### Core

| Variable | Default | Used by | Purpose |
| --- | --- | --- | --- |
| `GITCRAWL_CONFIG` | `~/.config/gitcrawl/config.toml` | All commands | Override config path |
| `GITCRAWL_DB_PATH` | `~/.config/gitcrawl/gitcrawl.db` | All commands | Override database path |
| `GITCRAWL_TUI_LAYOUT` | `columns` | `tui` | Override default wide-screen layout |
| `GITHUB_TOKEN` | _(none)_ | `sync`, `gh` shim | GitHub API token |
| `OPENAI_API_KEY` | _(none)_ | `embed`, `refresh` | OpenAI API key |

### Models

| Variable | Default | Purpose |
| --- | --- | --- |
| `GITCRAWL_SUMMARY_MODEL` | `gpt-5.4` | Summary model (reserved for future commands) |
| `GITCRAWL_EMBED_MODEL` | `text-embedding-3-small` | OpenAI embedding model |
| `GITCRAWL_OPENAI_RETRY_DISABLED` | _(off)_ | Set `1` to disable OpenAI retry/backoff |
| `GITCRAWL_OPENAI_BASE_URL` / `OPENAI_BASE_URL` | OpenAI default | Custom OpenAI endpoint |

### GitHub overrides

| Variable | Default | Purpose |
| --- | --- | --- |
| `GITCRAWL_GITHUB_BASE_URL` / `GITHUB_BASE_URL` | GitHub default | Custom GitHub API endpoint |
| `GH_HOST` | _(none)_ | Included in gh-shim cache key |
| `GH_REPO` | _(none)_ | Default `-R` value; included in gh-shim cache key |

### gh shim

| Variable | Default | Purpose |
| --- | --- | --- |
| `GITCRAWL_GH_PATH` | _(probed)_ | Path to the real `gh` binary |
| `GITCRAWL_GH_LIVE` | _(off)_ | Set `1` to make `gh` shim reads bypass local/fallthrough caches by default |
| `GITCRAWL_GH_LIVENESS_TTL` | `5m` | How long Actions/release reads bypass cache after matching mutations |
| `GITCRAWL_GH_AUTO_HYDRATE` | _(on)_ | Set `0` to disable PR auto-hydration on cache miss |
| `GITCRAWL_GH_CACHE_TTL` | `30s` for most commands | Override fallthrough cache TTL (e.g., `5m`, `1h`) |
| `GITCRAWL_GH_CACHE_ERRORS` | _(on)_ | Set `0` to avoid caching non-zero read-only fallthroughs |

## Configuration defaults

| Field | Default |
| --- | --- |
| `summary_model` | `gpt-5.4` |
| `embed_model` | `text-embedding-3-small` |
| `embed_dimensions` | `1024` |
| `embedding_basis` | `title_original` |
| `batch_size` (embeddings) | `64` |
| `concurrency` (embeddings) | `2` |
| `tui_default_sort` | `size` |
| `tui_default_layout` | `columns` |

## Clustering defaults

| Parameter | Default | Source |
| --- | --- | --- |
| `--threshold` | `0.80` | `cluster`, `refresh` |
| `--cross-kind-threshold` | `0.93` | `cluster`, `refresh` |
| `--min-size` | `1` | `cluster`, `refresh` |
| `--max-cluster-size` | `40` | `cluster`, `refresh` |
| `--k` (nearest-neighbor fanout) | `16` | `cluster`, `refresh` |
| Weak-edge title overlap floor | `0.18` | internal |
| High-confidence edge score | `0.90` | internal |
| Deterministic reference edge score | `0.94` | internal |
| Body-only reference prefix length | `240` chars | internal |

## TUI defaults

| Parameter | Default |
| --- | --- |
| `--min-size` | `5` |
| `--sort` | `size` |
| `--layout` | `columns` |
| Working set limit | `500` rows |
| Refresh interval | `15s` |

## gh shim cache TTLs

| Cache class | TTL |
| --- | --- |
| Most read-only fallthroughs | `5m`-`10m` |
| `gh run list` / run status | `30s` |
| `gh run view --log` / `--log-failed` | `12h` |
| `gh run view --job` | `1m` |
| `gh search ...` | `15m` |
| `gh release ...` | `1h` |
| `gh api` Actions run status | `30s` |
| `gh api` Actions job lists | `1m` active, `12h` completed |
| `gh api` workflow reads | `15m` |
| `gh api` Actions run/job logs | `12h` |
| `gh api` Pages metadata | `15m`-`30m` |
| `gh api` tagged/SHA contents | `7d` |
| `gh pr diff` without stable head SHA | `5m` |
| `gh pr diff` with stable head SHA | `7d` |
| Override | `GITCRAWL_GH_CACHE_TTL` |
| Stale-while-revalidate grace | command-aware; override with `GITCRAWL_GH_STALE_GRACE` |
| Low-budget stale grace | command-aware; override with `GITCRAWL_GH_LOW_BUDGET_STALE_GRACE` |
| Low-budget threshold | `250` remaining shared core requests; override with `GITCRAWL_GH_RATE_LIMIT_LOW_REMAINING` |
| Web fallback | `--web-fallback` / `GITCRAWL_GH_WEB=1` for supported public PR diff, commit/compare media, and contents-at-ref reads |
| Auto web fallback | on by default when shared core remaining is below 50% |
| Cache read failures | on by default; error TTL is capped (`2m` for rate-limit errors); disable with `GITCRAWL_GH_CACHE_ERRORS=0` |

## gh shim cache key composition

A SHA-256 hash of:

- Version tag (`v2`)
- Resolved gitcrawl config path
- Current working directory
- `GH_HOST` env var
- `GH_REPO` env var
- For `gh pr diff`: `pr-diff:owner/repo:number:head-sha` (when head SHA is known)
- Full command argument vector (null-separated)

This isolates sibling checkouts and portable stores while coalescing repeated calls from the same workspace.

## Output formats

| Format | Where to use |
| --- | --- |
| `text` | Human terminal use (default) |
| `json` | Pipelines, scripts, agents (also via `--json`) |
| `log` | Internal structured logging output |

## Exit codes

- `0` — success
- non-zero — usage error, "not implemented" command, or runtime failure

stderr always carries error messages. stdout is reserved for command output.

## File-system layout (worked example)

```
~/.config/gitcrawl/
├── config.toml
├── gitcrawl.db                  # SQLite mirror
├── gitcrawl.db-shm              # SQLite shared-memory file
├── gitcrawl.db-wal              # SQLite write-ahead log
├── cache/
│   ├── gh-shim/                 # gh fallthrough cache; inspect with xcache
│   └── pr/                      # hydrated PR detail blobs
├── vectors/                     # vector store backing embeddings
├── logs/
└── portable/                    # portable-store checkout (optional)
    └── data/
        └── owner__repo.sync.db
```

## See also

- [Configuration](/configuration/) — narrative version of this reference
- [Commands](/commands/) — every command and flag, in one table
- [SPEC.md](https://github.com/openclaw/gitcrawl/blob/main/SPEC.md) — product contract
- [CHANGELOG.md](https://github.com/openclaw/gitcrawl/blob/main/CHANGELOG.md) — what shipped recently
