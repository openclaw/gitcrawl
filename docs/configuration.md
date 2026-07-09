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

Linux uses XDG Base Directory paths. macOS uses the platform's `~/Library`
locations unless you set XDG environment variables.

| Platform | Path | Purpose |
| --- | --- | --- |
| Linux | `${XDG_CONFIG_HOME:-~/.config}/gitcrawl/config.toml` | Configuration file |
| Linux | `${XDG_DATA_HOME:-~/.local/share}/gitcrawl/gitcrawl.db` | SQLite database |
| Linux | `${XDG_CACHE_HOME:-~/.cache}/gitcrawl/` | Local caches |
| Linux | `${XDG_DATA_HOME:-~/.local/share}/gitcrawl/vectors/` | Vector store backing embeddings |
| Linux | `${XDG_STATE_HOME:-~/.local/state}/gitcrawl/logs/` | Operational logs |
| macOS | `~/Library/Application Support/gitcrawl/config.toml` | Configuration file |
| macOS | `~/Library/Application Support/gitcrawl/gitcrawl.db` | SQLite database |
| macOS | `~/Library/Caches/gitcrawl/` | Local caches |
| macOS | `~/Library/Application Support/gitcrawl/vectors/` | Vector store backing embeddings |
| macOS | `~/Library/Application Support/gitcrawl/logs/` | Operational logs |

Existing installs with `~/.config/gitcrawl/config.toml` continue to load that
config when the new platform config path does not exist. Override the config
path by setting `GITCRAWL_CONFIG=/path/to/config.toml` or by passing `--config`
to any command.

### Path selection edge cases

- `--config` and `GITCRAWL_CONFIG` are exact config-path overrides. They do not
  move an existing database, cache, vector, log, or portable-store checkout by
  themselves; those paths come from the loaded config or defaults.
- `gitcrawl init --runtime-dir <path>` creates a fully isolated local runtime
  under that directory: `gitcrawl.db`, `cache/`, `vectors/`, and `logs/`.
  This is useful for temporary archives and reproducible tests. Existing
  `init --db` behavior remains database-only. Relative `--db`, `--runtime-dir`,
  and `--store-dir` filesystem paths are anchored to the current working
  directory before they are saved, so later commands keep using the same files.
  `--db` accepts durable filesystem paths, not SQLite `file:` URIs or `:memory:`.
- Absolute XDG environment variables are honored even on macOS. Relative XDG
  values are ignored and gitcrawl falls back to the platform default for that
  path.
- Legacy fallback is per path, not all-or-nothing. If
  `~/.config/gitcrawl/config.toml`, `gitcrawl.db`, `cache/`, `vectors/`, or
  `logs/` exists and the corresponding new platform path does not, gitcrawl
  reuses that legacy path. If the new platform path exists, the new path wins
  for that component.
- Creating `~/Library/Application Support/gitcrawl/config.toml` on macOS opts
  the config file into the new platform location. It does not delete or migrate
  the old `~/.config/gitcrawl/` tree.
- Portable stores default to `<config-dir>/stores/<repo-name>` when
  `--store-dir` is omitted. Existing configs with
  `[portable_store].checkout_dir` keep using that explicit checkout.
- Older docs showed `cache/pr`; current gitcrawl stores PR detail data in
  SQLite tables instead of a `cache/pr` directory.

## `config.toml`

`gitcrawl init` writes a minimal config. You can edit it by hand or with `gitcrawl configure`:

```toml
summary_model = "gpt-5.4"
summary_base_url = "https://custom.example.com/v1"
embed_model = "text-embedding-3-small"
embed_base_url = "https://custom.example.com/v1"
embed_dimensions = 1024
embedding_basis = "title_original"
vector_backend = "exact"

[env]
GITHUB_TOKEN = "<github-token>"
OPENAI_API_KEY = "<openai-api-key>"

[portable_store]
url = "https://github.com/org/portable-store.git"
db_path = "data/openclaw__openclaw.sync.db"
checkout_dir = "/Users/me/Library/Application Support/gitcrawl/stores/portable-store"

[remote]
mode = "cloud"
endpoint = "https://crawl.example.com"
archive = "gitcrawl/org__repo"
token_env = "CRAWL_REMOTE_TOKEN"
```

### Notable fields

| Field | Default | Notes |
| --- | --- | --- |
| `summary_model` | `gpt-5.4` | Reserved for future summary commands |
| `summary_base_url` | _(empty)_ | Custom endpoint for the summary model; falls back to `GITCRAWL_OPENAI_BASE_URL`/`OPENAI_BASE_URL`. Override at runtime with `GITCRAWL_SUMMARY_BASE_URL` |
| `embed_model` | `text-embedding-3-small` | OpenAI embedding model |
| `embed_base_url` | _(empty)_ | Custom endpoint for the embedding model; falls back to `GITCRAWL_OPENAI_BASE_URL`/`OPENAI_BASE_URL`. Override at runtime with `GITCRAWL_EMBED_BASE_URL` |
| `embed_dimensions` | `1024` | Must match the model |
| `embedding_basis` | `title_original` | Only `title_original` is implemented |
| `vector_backend` | `exact` | Semantic search backend: `exact` or optional `turbovec` via Python `turbovec`; turbovec requires dimensions divisible by 8 |
| `[tui].default_sort` | `size` | Default TUI cluster ordering |
| `[tui].default_layout` | `columns` | Default wide-screen TUI layout: `columns`, `right-stack`, or `focus` |
| `[env]` | _(empty)_ | Config-backed fallback after real process env for env-derived values such as tokens, DB path, and model overrides |
| `[portable_store]` | _(empty)_ | Used when working from a shared, Git-backed cache |
| `[remote]` | _(local mode)_ | Worker-backed archive settings for cloud reads and publishing |

Bearer-authenticated remote endpoints must use HTTPS. Plain HTTP is accepted
only for loopback endpoints (`localhost`, `127.0.0.1`, or `::1`) used during
local development. Upgrade any non-local `http://` endpoint to `https://`
before updating gitcrawl.

## Environment variables

### Core

| Variable | Purpose |
| --- | --- |
| `GITCRAWL_CONFIG` | Override config path |
| `GITCRAWL_DB_PATH` | Override database path |
| `GITCRAWL_TUI_LAYOUT` | Override default TUI layout (`columns`, `right-stack`, or `focus`) |
| `GITHUB_TOKEN` | GitHub API token (required for `sync`) |
| `OPENAI_API_KEY` | OpenAI API key (required for `embed`) |

### Model overrides

| Variable | Purpose |
| --- | --- |
| `GITCRAWL_SUMMARY_MODEL` | Override summary model |
| `GITCRAWL_EMBED_MODEL` | Override embedding model |
| `GITCRAWL_VECTOR_BACKEND` | Override semantic vector backend (`exact` or `turbovec`) |
| `GITCRAWL_OPENAI_RETRY_DISABLED` | Set to `1` to disable OpenAI retry/backoff |
| `GITCRAWL_OPENAI_BASE_URL` / `OPENAI_BASE_URL` | Custom OpenAI endpoint (e.g., for a proxy) |
| `GITCRAWL_SUMMARY_BASE_URL` | Custom endpoint for the summary model (falls back to the shared OpenAI endpoint) |
| `GITCRAWL_EMBED_BASE_URL` | Custom endpoint for the embedding model (falls back to the shared OpenAI endpoint) |

#### How the base-URL variables relate

There are three base-URL variables, and they form a fallback chain rather than
competing with each other:

- **`GITCRAWL_OPENAI_BASE_URL`** (or `OPENAI_BASE_URL`) is the **shared** endpoint
  used for *every* OpenAI call — both summaries and embeddings. Set only this and
  both workloads hit the same endpoint. This is the common case (e.g. one proxy
  for everything).
- **`GITCRAWL_SUMMARY_BASE_URL`** is a **per-purpose override** that applies only
  to the summary model.
- **`GITCRAWL_EMBED_BASE_URL`** is a **per-purpose override** that applies only to
  the embedding model.

The per-purpose variables do not replace the shared one — they layer on top of
it. When a per-purpose value is empty, that workload falls back to the shared
endpoint. Resolution order for each purpose:

- **Embeddings:** `GITCRAWL_EMBED_BASE_URL` → `embed_base_url` (config) → `GITCRAWL_OPENAI_BASE_URL` → `OPENAI_BASE_URL`
- **Summaries:** `GITCRAWL_SUMMARY_BASE_URL` → `summary_base_url` (config) → `GITCRAWL_OPENAI_BASE_URL` → `OPENAI_BASE_URL`

Use the per-purpose overrides only when the two workloads must diverge — for
example, running chat/summaries against one provider while embeddings go to a
different host or a local embedding server:

```bash
# Summaries use the shared endpoint; embeddings go to a dedicated one.
export GITCRAWL_OPENAI_BASE_URL="https://openrouter.ai/api/v1"
export GITCRAWL_EMBED_BASE_URL="http://localhost:8080/v1"
```

### GitHub overrides

| Variable | Purpose |
| --- | --- |
| `GITCRAWL_GITHUB_BASE_URL` / `GITHUB_BASE_URL` | Custom GitHub API endpoint used by `sync` |
| `GH_REPO` | Default repository for compatible local search shapes |

## Non-OpenAI / OpenAI-compatible embedding endpoints

`OPENAI_API_KEY` is only used to generate embeddings (`embed`, `refresh`). The
embedding client is a generic OpenAI-compatible client: it sends
`Authorization: Bearer <key>` and POSTs to `<base-url>/embeddings`. Any
compatible endpoint — OpenRouter, a proxy, a local server, or a self-hosted
gateway — works if you override three things:

1. **Base URL** — `GITCRAWL_OPENAI_BASE_URL` (or `OPENAI_BASE_URL`), including
   the version suffix your provider expects (e.g. `/v1`). To point *only*
   embeddings at this endpoint while leaving summaries on the shared one, use
   `GITCRAWL_EMBED_BASE_URL` (or `embed_base_url`) instead — see
   [How the base-URL variables relate](#how-the-base-url-variables-relate).
2. **Model** — `GITCRAWL_EMBED_MODEL` (or `embed_model`). The default
   `text-embedding-3-small` is an OpenAI model name; set a model the provider
   actually serves for embeddings.
3. **Dimensions** — set `embed_dimensions` to match the model's output if it
   differs from `1024`, or if the endpoint does not honor the `dimensions`
   request parameter the way OpenAI does.

By default the key is read from `OPENAI_API_KEY`. To read it from a different
variable instead, set `openai.api_key_env` in `config.toml`:

```toml
embed_model = "text-embedding-3-small"
embed_dimensions = 1024

[openai]
api_key_env = "OPENROUTER_API_KEY"
```

OpenRouter example:

```bash
export GITCRAWL_OPENAI_BASE_URL="https://openrouter.ai/api/v1"
export GITCRAWL_EMBED_MODEL="<an-embedding-model-openrouter-serves>"
export OPENAI_API_KEY="<openrouter-key>"   # or set openai.api_key_env
```

Notes:

- OpenRouter's embeddings coverage is narrower than its chat coverage; confirm
  it serves the embedding model you pick before running `embed`.
- Changing the model changes the vector space. `embed` auto-forces a rebuild
  when `embed_model` changes, so existing rows are re-embedded consistently. If
  you only change `embed_dimensions` (or switch to an endpoint that produces
  different vectors under the same model name), re-run `embed --force` yourself
  so `neighbors` and clustering do not mix incompatible vectors.
- `gitcrawl doctor` reports where the key came from (env vs. config) and the
  active embed model, which is useful for confirming the override took effect.

### gh shim

`gitcrawl gh` moved to Octopool. Run `octopool login`, then use `octopool gh ...` or symlink Octopool as `gh`.

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
gitcrawl doctor --locks --json
```

Reports config path and existence, database path, source/runtime SQLite health, portable-store Git status, last repair action, whether `GITHUB_TOKEN` and `OPENAI_API_KEY` are present (and whether they came from env vs. config), the active summary/embed models, the embedding basis, and counts of repositories, threads, open threads, clusters, plus the last sync timestamp. If the API call surface is unsupported (older Go, missing crypto), `api_supported: false` is reported so you can investigate.
JSON output also reports the active executable and available Go/build metadata under `runtime`, plus read-only schema compatibility under `db_schema` and `source_db_schema`. Portable stores additionally expose `runtime_db_schema` for the runtime mirror. Inspect `state`, `pending_migrations`, `pr_details`, and `next_steps` to distinguish a current store from a pending migration, a newer unsupported schema, or a damaged database. Schema inspection never applies migrations; run a write-path command with the intended binary only after reviewing the reported drift. When the runtime database cannot be opened or its status cannot be read, doctor still writes the JSON diagnostics and includes `runtime_open_error` or `runtime_status_error`, then exits nonzero.

Add `--locks` for read-only SQLite lock diagnostics. The JSON payload includes the database, WAL, SHM, and rollback-journal paths and sizes, read-only open status, `pragma quick_check` status, and best-effort writer-process detection. `safe_read_only_inspection` reflects archive health even when another SQLite connection is open; `writer_activity` separately reports whether `lsof` found another process with writable access to the database. `appears_idle` becomes true only when the archive is healthy, process detection is available, and no writer candidate is found. On platforms without supported process inspection, `process_detection.available` is `false`, `writer_activity` is `unknown`, and `appears_idle` remains false instead of treating missing process data as proof that no writer exists.
