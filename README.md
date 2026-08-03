# gitcrawl 🕷️ — Pull GitHub down to earth

[![CI](https://img.shields.io/github/actions/workflow/status/openclaw/gitcrawl/ci.yml?branch=main&style=flat-square&label=ci)](https://github.com/openclaw/gitcrawl/actions/workflows/ci.yml)
[![GitHub release](https://img.shields.io/github/v/release/openclaw/gitcrawl?style=flat-square)](https://github.com/openclaw/gitcrawl/releases/latest)
[![Platforms](https://img.shields.io/badge/platforms-macOS%20%7C%20Linux%20%7C%20Windows-64748b?style=flat-square)](https://github.com/openclaw/gitcrawl/releases/latest)
[![License](https://img.shields.io/github/license/openclaw/gitcrawl?style=flat-square)](LICENSE)
[![Docs](https://img.shields.io/badge/docs-gitcrawl.sh-2563eb?style=flat-square)](https://gitcrawl.sh)

`gitcrawl` mirrors GitHub issues and pull requests into local SQLite for maintainers and agents. It provides local search, related-thread clustering, JSON output, and a terminal interface without running a local HTTP service.

<img width="1797" height="1096" alt="Gitcrawl terminal interface showing issue and pull request clusters" src="https://github.com/user-attachments/assets/54a0a6cf-5862-451d-9552-5d18656976ff" />

## Install

Homebrew is the smallest install on macOS and Linux:

```sh
brew install openclaw/tap/gitcrawl
```

Prebuilt archives for macOS, Linux, and Windows are available from [GitHub Releases](https://github.com/openclaw/gitcrawl/releases/latest). The [installation guide](docs/installation.md) also covers source builds and update checks; source builds require Go 1.26.5 or newer. A [Docker source build](docs/docker.md) keeps its runtime state under one mounted directory.

Sync needs a GitHub token from `GITHUB_TOKEN` or `gh auth token`. Generating summaries and embeddings also needs `OPENAI_API_KEY`; semantic search and clustering use those stored embeddings, while ordinary sync and keyword search do not.

## Quick start

Initialize the local archive, sync a repository, and query it:

```sh
gitcrawl init
gitcrawl sync openclaw/gitcrawl
gitcrawl search issues "SQLite" -R openclaw/gitcrawl \
  --state open --json number,title,url --limit 5
```

The default sync fetches open issues and pull requests and sweeps recently closed threads. Use `--state all` for an initial historical backfill, `--include-comments` for discussion, or `--with pr-details` for pull request files, commits, checks, and review state.

The [quickstart](docs/quickstart.md) continues through embeddings, clusters, and the terminal UI. For a quota-conscious maintainer workflow, see [Maintainer archive workflow](docs/maintainer-archive.md).

## Search and automation

Direct search supports keyword, semantic, and hybrid modes over one local repository. The `gh search`-shaped form used above lets existing scripts query the SQLite mirror without spending GitHub search quota. Add `--sync-if-stale 5m` when an agent should refresh an old mirror before searching.

Commands support structured output with `--json`. Gitcrawl reserves stdout for results and sends diagnostics to stderr, so its output can feed `jq` or another process directly. See [Search](docs/search.md) and [Automation](docs/automation.md) for the supported shapes.

## Cluster and review

`gitcrawl refresh owner/repo` runs sync, embedding, and clustering in order. Clusters combine vector similarity with direct GitHub references, then preserve maintainer decisions such as local closes, member exclusions, and canonical threads across later runs.

Open `gitcrawl tui [owner/repo]` for the keyboard- and mouse-driven cluster browser. The TUI reads SQLite and refreshes its view every 15 seconds; it does not call GitHub itself. See [Clustering](docs/clustering.md), [Governance](docs/governance.md), and [TUI](docs/tui.md).

## Archive modes

The default local archive keeps configuration, SQLite data, vectors, caches, and logs in platform-native user directories. Exact paths and environment overrides are in [Configuration](docs/configuration.md).

[Portable stores](docs/portable-stores.md) publish a compact SQLite snapshot through Git so multiple machines can share a read-mostly archive. [Cloud archives](docs/cloud-archives.md) configure authenticated reads and snapshot publication through a separately deployed Worker service. Local, portable, and cloud modes keep distinct storage and credential boundaries.

Octopool owns pooled live `gh` reads. Gitcrawl keeps local mirror, search, clustering, and TUI workflows; use `gh` or Octopool for final live verification and GitHub write actions. See the [migration note](docs/gh-shim.md).

## Command map

| Job | Command | Guide |
| --- | --- | --- |
| Check archive health | `gitcrawl status` / `gitcrawl doctor` | [Configuration](docs/configuration.md) |
| Mirror GitHub threads | `gitcrawl sync owner/repo` | [Sync](docs/sync.md) |
| Search threads or indexed code | `gitcrawl search ...` | [Search](docs/search.md) |
| Build and inspect clusters | `gitcrawl refresh`, `clusters`, `tui` | [Clustering](docs/clustering.md) |
| Export a code-free conversation snapshot | `gitcrawl capture owner/repo` | [Capture](docs/capture.md) |

The [command reference](docs/commands.md) lists the complete CLI surface and flags.

## Development

```sh
make build
make test
make check
```

`make check` runs the formatting, vet, vulnerability, dead-code, coverage, smoke, release-script, and snapshot gates used by CI.

## License

MIT — see [LICENSE](LICENSE).
