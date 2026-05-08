---
title: Configuration
nav_order: 5
permalink: /configuration/
---

# Configuration
{: .no_toc }

Where gitcrawl reads settings from, and how to override them.
{: .fs-6 .fw-300 }

1. TOC
{:toc}

## Resolution order

For each setting, gitcrawl looks in this order and uses the first match:

1. CLI flag (e.g., `--config`, `--summary-model`)
2. Environment variable (`GITCRAWL_*`, then standard `GITHUB_TOKEN` / `OPENAI_API_KEY`)
3. `[env]` table inside `config.toml`
4. Top-level config field inside `config.toml`
5. Built-in default

## Default paths

| Path | Purpose |
| --- | --- |
| `~/.config/gitcrawl/config.toml` | Configuration file |
| `~/.config/gitcrawl/gitcrawl.db` | SQLite database |
| `~/.config/gitcrawl/cache/` | Caches (PR detail, gh shim fallthrough) |
| `~/.config/gitcrawl/cache/gh-shim/` | gh shim fallthrough cache |
| `~/.config/gitcrawl/vectors/` | Vector store backing embeddings |
| `~/.config/gitcrawl/logs/` | Operational logs |

Override the config root by setting `GITCRAWL_CONFIG=/path/to/config.toml` or by passing `--config` to any command.

## `config.toml`

`gitcrawl init` writes a minimal config. You can edit it by hand or with `gitcrawl configure`:

```toml
summary_model = "gpt-5.4"
embed_model = "text-embedding-3-small"
embed_dimensions = 1024
embedding_basis = "title_original"

[env]
GITHUB_TOKEN = "<github-token>"
OPENAI_API_KEY = "<openai-api-key>"

[portable_store]
url = "https://github.com/org/portable-store.git"
db_path = "data/openclaw__openclaw.sync.db"
checkout_dir = "/Users/me/.config/gitcrawl/portable"
```

### Notable fields

| Field | Default | Notes |
| --- | --- | --- |
| `summary_model` | `gpt-5.4` | Reserved for future summary commands |
| `embed_model` | `text-embedding-3-small` | OpenAI embedding model |
| `embed_dimensions` | `1024` | Must match the model |
| `embedding_basis` | `title_original` | Only `title_original` is implemented |
| `[env]` | _(empty)_ | Config-backed fallback after real process env for env-derived values such as tokens, DB path, and model overrides |
| `[portable_store]` | _(empty)_ | Used when working from a shared, Git-backed cache |

## Environment variables

### Core

| Variable | Purpose |
| --- | --- |
| `GITCRAWL_CONFIG` | Override config path |
| `GITCRAWL_DB_PATH` | Override database path |
| `GITHUB_TOKEN` | GitHub API token (required for `sync`, `gh` shim fallthroughs) |
| `OPENAI_API_KEY` | OpenAI API key (required for `embed`) |

### Model overrides

| Variable | Purpose |
| --- | --- |
| `GITCRAWL_SUMMARY_MODEL` | Override summary model |
| `GITCRAWL_EMBED_MODEL` | Override embedding model |
| `GITCRAWL_OPENAI_RETRY_DISABLED` | Set to `1` to disable OpenAI retry/backoff |
| `GITCRAWL_OPENAI_BASE_URL` / `OPENAI_BASE_URL` | Custom OpenAI endpoint (e.g., for a proxy) |

### GitHub overrides

| Variable | Purpose |
| --- | --- |
| `GITCRAWL_GITHUB_BASE_URL` / `GITHUB_BASE_URL` | Custom GitHub API endpoint (used by `sync` and the `gh` shim) |
| `GH_HOST` | GitHub host; included in the `gh` shim cache key |
| `GH_REPO` | Default repo when `-R` is omitted; included in the `gh` shim cache key |

### gh shim

| Variable | Purpose |
| --- | --- |
| `GITCRAWL_GH_PATH` | Path to the real `gh` binary used for fallthrough |
| `GITCRAWL_GH_AUTO_HYDRATE` | Set to `0` to disable PR auto-hydration on cache miss |
| `GITCRAWL_GH_CACHE_TTL` | Override fallthrough cache TTL (e.g., `5m`, `1h`) |
| `GITCRAWL_GH_STALE_GRACE` | Override stale-while-revalidate grace for expired successful fallthrough entries |
| `GITCRAWL_GH_CACHE_ERRORS` | Set to `0` to avoid caching non-zero read-only fallthroughs |

If `GITCRAWL_GH_PATH` is unset, the shim probes common Homebrew install paths and then your `PATH`. Set it explicitly when you symlink the gitcrawl binary as `gh` (otherwise the shim will recurse into itself).

## Global flags

These flags work on every command:

| Flag | Default | Description |
| --- | --- | --- |
| `--config <path>` | `$GITCRAWL_CONFIG` or default | Override config path for this invocation |
| `--format text\|json\|log` | `text` | Output format |
| `--json` | _(off)_ | Shorthand for `--format json` |
| `--no-color` | _(off)_ | Suppress ANSI color codes |
| `--version` | _(off)_ | Print version and exit (global only) |

`--json` overrides `--format`. Both are honored on subcommands that produce output.

## `gitcrawl configure`

Interactive-friendly config edits without opening the file:

```bash
gitcrawl configure --summary-model gpt-5.4
gitcrawl configure --embed-model text-embedding-3-small
gitcrawl configure --embedding-basis title_original
gitcrawl configure --json
```

Returns the resolved config path, the values that were updated, and the now-current model selection. See `gitcrawl configure --help`.

## `gitcrawl doctor`

A health check for everything covered above:

```bash
gitcrawl doctor          # human-readable
gitcrawl doctor --json   # for scripts
```

Reports config path and existence, database path, whether `GITHUB_TOKEN` and `OPENAI_API_KEY` are present (and whether they came from env vs. config), the active summary/embed models, the embedding basis, and counts of repositories, threads, open threads, clusters, plus the last sync timestamp. If the API call surface is unsupported (older Go, missing crypto), `api_supported: false` is reported so you can investigate.
