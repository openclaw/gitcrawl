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
| `~/.config/gitcrawl/cache/` | Local runtime caches |
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
| `GITHUB_TOKEN` | _(none)_ | `sync` | GitHub API token |
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
| `GH_REPO` | _(none)_ | Default repository for compatible local search shapes |

### gh shim

`gitcrawl gh` moved to Octopool. Run `octopool login`, then use `octopool gh ...`.

## Configuration defaults

| Field | Default |
| --- | --- |
| `summary_model` | `gpt-5.4` |
| `embed_model` | `text-embedding-3-small` |
| `embed_dimensions` | `1024` |
| `embedding_basis` | `title_original` |
| `vector_backend` | `exact`; `turbovec` requires Python `turbovec` and dimensions divisible by 8 |
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
│   └── pr/                      # local runtime cache
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
