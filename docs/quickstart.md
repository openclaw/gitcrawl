---
title: Quickstart
nav_order: 3
permalink: /quickstart/
---

# Quickstart
{: .no_toc }

Five minutes from clean machine to a populated cluster view.
{: .fs-6 .fw-300 }

1. TOC
{:toc}

## 1. Install and initialize

```bash
# Build (or download a release archive — see Installation).
git clone https://github.com/openclaw/gitcrawl.git
cd gitcrawl
mkdir -p "$HOME/bin"
go build -o "$HOME/bin/gitcrawl" ./cmd/gitcrawl

# Create config + database under the platform default runtime paths.
gitcrawl init
```

Defaults written:

- Linux config: `${XDG_CONFIG_HOME:-~/.config}/gitcrawl/config.toml`
- Linux database/vectors: `${XDG_DATA_HOME:-~/.local/share}/gitcrawl/`
- Linux cache: `${XDG_CACHE_HOME:-~/.cache}/gitcrawl/`
- Linux logs: `${XDG_STATE_HOME:-~/.local/state}/gitcrawl/logs/`
- macOS config/database/vectors/logs: `~/Library/Application Support/gitcrawl/`
- macOS cache: `~/Library/Caches/gitcrawl/`

Existing installs with `~/.config/gitcrawl/config.toml` continue to load that
config when the new platform config path does not exist.

## 2. Set credentials

```bash
export GITHUB_TOKEN="<github-token>"                 # required for sync
export OPENAI_API_KEY="<openai-api-key>"             # required for embeddings
```

Either set them in your shell profile or store them in `config.toml`:

```toml
[env]
GITHUB_TOKEN = "<github-token>"
OPENAI_API_KEY = "<openai-api-key>"
```

`gitcrawl doctor` confirms the credentials are visible and reports their source.

## 3. Sync a repository

```bash
gitcrawl sync openclaw/gitcrawl
```

By default this fetches **open** issues and pull requests, plus a sweep of recently closed rows so the local store does not rot. Add `--include-comments` for review threads, `--include-pr-details` (or `--with pr-details`) for PR files, commits, checks, and workflow runs.

Need exact rows? Use `--numbers`:

```bash
gitcrawl sync openclaw/gitcrawl --numbers 123,456 --include-comments
```

## 4. Embed and cluster

The `refresh` command runs sync → embed → cluster end to end:

```bash
gitcrawl refresh openclaw/gitcrawl
```

You can run the stages individually if you want finer control — see [Refresh and embed](/refresh-and-embed/) and [Clustering](/clustering/).

## 5. Browse clusters

Open the TUI:

```bash
gitcrawl tui openclaw/gitcrawl
# or just `gitcrawl tui` and the most recently synced repo is inferred
```

- `↑`/`↓` navigate clusters, `Enter` opens member detail
- `a` opens the action menu, `#` jumps to a number, `n` loads neighbors, `p` switches repo
- Right-click and mouse wheel work in most terminals

For a non-interactive view:

```bash
gitcrawl clusters openclaw/gitcrawl --sort size --min-size 5
gitcrawl cluster-detail openclaw/gitcrawl --id 12
gitcrawl neighbors openclaw/gitcrawl --number 123 --limit 10
```

## 6. Search the local cache

```bash
gitcrawl search openclaw/gitcrawl --query "download stalls" --mode hybrid
```

The same command also accepts the `gh search` shape, which makes it a drop-in for scripts that already speak `gh`:

```bash
gitcrawl search issues "manifest cache" \
  -R openclaw/gitcrawl \
  --state open \
  --json number,title,state,url,updatedAt,labels \
  --limit 30
```

Add `--sync-if-stale 5m` to refresh the local mirror first when it is older than the duration you tolerate.

## 7. Wire up the `gh` shim (optional)

```bash
gitcrawl search issues "download stalls" -R openclaw/gitcrawl --json number,title,url
octopool login
octopool gh api repos/openclaw/gitcrawl/pulls/123
```

The old `gitcrawl gh` shim moved to Octopool. See [gh shim](/gh-shim/) for the migration note.

## Where to next

- [Concepts](/concepts/) — what threads, durable clusters, and embeddings actually mean
- [Sync](/sync/) — every flag for hydrating the local store
- [Clustering](/clustering/) — tuning the cluster graph for a specific repo
- [Automation](/automation/) — JSON contracts for agents and scripts
