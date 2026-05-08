---
title: Portable stores
nav_order: 13
permalink: /portable-stores/
---

# Portable stores
{: .no_toc }

A Git-backed publish target for a `gitcrawl.db` plus its derived bodies — share a local cache across agents and machines without running a hosted service.
{: .fs-6 .fw-300 }

1. TOC
{:toc}

## When to use one

- You want every agent on a team to read from a shared, recently synced cache without each agent making its own GitHub calls.
- You want a backup of the SQLite cache that someone else can clone and use immediately.
- You want a deterministic snapshot of "what gitcrawl knew at time T" for reproducible triage.

A portable store is just a Git repository whose contents include a SQLite database. Anyone with read access to the repository can `git clone` it and have a fully populated gitcrawl mirror in seconds.

## Setup: pointing gitcrawl at a portable store

```bash
gitcrawl init \
  --portable-store https://github.com/openclaw/gitcrawl-store.git \
  --portable-db data/openclaw__openclaw.sync.db \
  --store-dir ~/.config/gitcrawl/portable
```

`init` will:

1. Clone the portable store to `--store-dir`
2. Wire `~/.config/gitcrawl/config.toml` to use the database at `--portable-db` inside that checkout
3. Create the runtime cache, vector, and log directories in the standard locations

JSON output reports `portable_store_url`, `portable_store_dir`, and `portable_store: cloned|pulled|reset-pulled` so automation can tell what happened.

## How read-only commands behave

Read-only commands (`search`, `threads`, `clusters`, `cluster-detail`, `neighbors`, the TUI) refresh the portable-store checkout before reading, so they always see the latest published data:

- The refresh is best-effort and non-interactive
- SSH attempts are bounded so an offline remote does not hang the CLI
- Stale SQLite sidecars (WAL, SHM) are cleared after the pull so queries see freshly pulled data
- Local Git pull configuration that tries to rebase onto multiple branch merge refs is handled cleanly

If the remote is unreachable, the read still answers from the local checkout.

## How write commands behave

Write commands (`embed`, `refresh`, `cluster`, neighbor generation) need to persist new data without mutating the published portable store. They open a **writable runtime mirror** alongside the portable checkout so vectors and overrides land in the runtime cache while the portable database remains read-only.

This separation means:

- You can `gitcrawl embed` against a portable store without dirtying the Git checkout
- gh-shim exact-thread auto-hydration writes into the same runtime mirror
- Local cluster overrides (`close-cluster`, exclusions, canonicals) live in the runtime mirror
- Only the publishing workflow writes back into the portable checkout

## Publishing: `gitcrawl portable prune`

```bash
gitcrawl portable prune
gitcrawl portable prune --body-chars 256       # default
gitcrawl portable prune --body-chars 512 --no-vacuum
gitcrawl portable prune --json
```

`prune` converts the database into the portable v2 backup format and (by default) runs SQLite `VACUUM` to reclaim space. The result is a smaller database suitable for committing back to Git.

Portable v2 keeps the data agents most often need for offline GitHub reads:

- Repositories, issues, pull requests, labels, authors, and timestamps
- Compact issue/PR body excerpts plus original body lengths
- Compact comments, reviews, and review-comment excerpts plus original body lengths
- PR details, files, commits, status checks, and workflow runs
- Thread fingerprints used by duplicate and cluster-oriented workflows

It strips the data that is large, easy to regenerate, or mainly useful for exact API replay: raw GitHub JSON, generated documents and FTS indexes, embeddings and vectors, code snapshots and diff blobs, cluster run history, similarity edges, and blob storage. The database records this contract in `portable_metadata` with `schema=gitcrawl-portable-sync-v2`, `includes`, `excluded`, and `capabilities` keys.

| Flag | Default | Description |
| --- | --- | --- |
| `--body-chars <n>` | `256` | Maximum body characters to keep per thread/comment excerpt |
| `--no-vacuum` | _(off)_ | Skip the post-prune `VACUUM` |
| `--json` | _(off)_ | JSON output |

After pruning, commit and push the database file from the portable checkout the way you would for any Git repository.

## A typical publishing flow

```bash
# In the portable store checkout, refresh upstream data into the local runtime mirror.
gitcrawl refresh owner/repo

# Prune for a small, shareable footprint.
gitcrawl portable prune --body-chars 256

# Commit and push using normal Git.
cd ~/.config/gitcrawl/portable
git add data/openclaw__openclaw.sync.db
git commit -m "data: refresh openclaw/gitcrawl"
git push
```

Other agents and machines pull the new commit on their next read-only command.

## Cached search against a portable store

`gitcrawl search` (and the gh-shim's search) work against portable-store data with one wrinkle: when the portable store has been pruned, generated document indexes may not be present. Search falls back to compact thread title/body data automatically — you keep useful results without the publisher needing to ship the full document indexes.

The v2 backup also keeps comments and PR-detail tables, so common shim reads such as `gh issue view --json comments`, `gh pr view --json files,commits,statusCheckRollup`, `gh pr checks`, and `gh run list` can be answered from the shared checkout when those details were synced before publishing.

## Caveats

- The portable store carries the SQLite database. It does not carry the runtime fallthrough cache.
- Vectors regenerated on each consumer's machine after `embed` are not shared; portable pruning removes vector tables from the published database.
- Portable stores are read-mostly. Multiple writers pushing concurrently will race the way any Git workflow does — gate writes through a single publisher or a CI workflow.

## See also

- [Sync](/sync/) — what gets written into the database that ends up in the portable store
- [gh shim](/gh-shim/) — agents reading a shared portable store benefit doubly from the shim's local-first answers
