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
  --portable-db data/openclaw__openclaw.sync.db
```

When `--store-dir` is omitted, gitcrawl clones the store under
`<config-dir>/stores/<repo-name>`. For the example above, a fresh macOS install
uses `~/Library/Application Support/gitcrawl/stores/gitcrawl-store`; a Linux
install using the default config location uses
`~/.config/gitcrawl/stores/gitcrawl-store`. Pass `--store-dir` when you want a
fixed checkout path.

`init` will:

1. Clone the portable store next to the active config, or to `--store-dir` if provided
2. Wire the active `config.toml` to use the database at `--portable-db` inside
   that checkout. With the Linux default config location, that file is
   `~/.config/gitcrawl/config.toml`.
3. Create the runtime cache, vector, and log directories in the standard locations

JSON output reports `portable_store_url`, `portable_store_dir`, and `portable_store: cloned|pulled|reset-pulled` so automation can tell what happened.

Large stores may commit a gzip artifact instead of the raw SQLite file. The
configured `db_path` remains the logical SQLite path, while its sibling
manifest declares `compression: "gzip"`, a relative `archivePath`, and the
archive size and SHA-256. Gitcrawl verifies the compressed artifact, inflates
at most the manifest's declared SQLite size, verifies the existing uncompressed
size and SHA-256, runs SQLite `quick_check`, and only then atomically replaces
the runtime mirror. Legacy raw SQLite stores continue to use the same manifest
without compression fields.

## How read-only commands behave

Read-only commands (`search`, `threads`, `clusters`, `cluster-detail`, `neighbors`, the TUI) refresh the portable-store checkout before reading, so they always see the latest published data:

- The refresh is best-effort and non-interactive
- SSH attempts are bounded so an offline remote does not hang the CLI
- Stale SQLite sidecars (WAL, SHM) are cleared after the pull so queries see freshly pulled data
- Local Git pull configuration that tries to rebase onto multiple branch merge refs is handled cleanly
- SQLite files are copied through a temporary runtime database, checked with `quick_check`, and verified against the portable manifest before replacing the previous runtime mirror
- Manifest-backed gzip SQLite artifacts are verified before inflation and still use the uncompressed database digest as the runtime identity

If the remote is unreachable, the read still answers from the local checkout.

## How write commands behave

Write commands (`sync`, `embed`, `refresh`, `portable prune`, `cluster`, neighbor generation) open a **writable runtime mirror** alongside the portable checkout so new GitHub data, vectors, and overrides persist without partially mutating the published portable store. When this redirect engages, gitcrawl prints one stderr notice naming both the runtime mirror and the checkout database.

JSON output from `sync`, `refresh`, `fill-pr-details`, and `portable prune` makes that destination machine-readable: `db_target` is `runtime-mirror`, `db_target_path` names the database actually written, and `portable_source_db` names the database in the checkout. With a non-portable local database, `db_target` is `direct`, `db_target_path` names that database, and `portable_source_db` is omitted.

This separation means:

- You can `gitcrawl embed` against a portable store without dirtying the Git checkout
- Local cluster overrides (`close-cluster`, exclusions, canonicals) live in the runtime mirror
- Before publishing, even `refresh` writes only to the runtime mirror and leaves the checkout database untouched
- The `portable prune` publishing step is the boundary that writes a validated database and manifest back into the checkout

## Publishing: `gitcrawl portable prune`

```bash
gitcrawl portable prune
gitcrawl portable prune --body-chars 256       # default
gitcrawl portable prune --body-chars 512 --no-vacuum
gitcrawl portable prune --include-sync-failures # opt-in, error text redacted
gitcrawl portable prune --no-publish            # prune only the runtime mirror
gitcrawl portable prune --json
```

`portable prune` validates the pruned runtime-mirror database with SQLite `quick_check` and writes a `.manifest.json` next to it with size and SHA-256 integrity. When the configured `db_path` is inside a portable checkout, prune then stages the database and manifest inside that checkout, validates the staged pair, and replaces the published files with atomic renames while preserving their existing permissions. A failed publish restores the previously published pair; an interrupted one is caught later by manifest validation and the standard repair path, the same way an interrupted `git pull` of the store is. Pass `--no-publish` to leave the pruned result only in the runtime mirror. Consumers use the manifest to reject incomplete or mismatched portable-store downloads before replacing a known-good runtime mirror.

`prune` converts the database into the portable v2 backup format and (by default) runs SQLite `VACUUM` to reclaim space. The result is a smaller database and matching manifest ready to commit from the portable checkout. JSON output includes `published`, which is true only when this copy-back happened, plus `published_db_path` and `published_manifest_path` when published.

Portable v2 keeps the data agents most often need for offline GitHub reads:

- Repositories, issues, pull requests, labels, author login/type/association, and timestamps
- Compact issue/PR body excerpts plus original body lengths
- Compact comments, reviews, and review-comment excerpts plus original body lengths
- PR details, files, commits, status checks, and workflow runs
- Thread revisions, deterministic fingerprints, and key summaries used by duplicate and cluster-oriented workflows

It strips the data that is large, private, easy to regenerate, or mainly useful for exact API replay: raw GitHub JSON, generated documents and FTS indexes, embeddings and vectors, code snapshots and diff blobs (including `pull_request_files.patch`), cluster run history, the sync failure ledger, similarity edges, and blob storage. PR file path, status, line counts, rename metadata, and other current file identity remain. Pass `--include-sync-failures` only when failure history is useful to portable-store readers; the table is retained but every `error_message` is replaced with `[redacted for portable export]`. Once the source schema contains the ledger, pruning securely rewrites the database even with `--no-vacuum` so current, deleted, and historical retry text cannot remain in free pages. An interrupted rewrite remains marked pending and is retried by the next prune. The database records this contract in `portable_metadata` with `schema=gitcrawl-portable-sync-v2`, `includes`, `excluded`, `capabilities`, and `thread_author_profile` keys. The added revision, fingerprint, summary, and author-association fields are additive; the portable schema identifier remains v2 so older readers can continue using the columns and tables they understand.

Portable mirrors retain existing revision-bound key summaries, but do not regenerate them from compact body excerpts. Pruning removes canonical revision evidence blobs, so run a fully hydrated sync in a writable archive before `summarize`; the summary queue requires the exact content-addressed payload bound to the revision.

| Flag | Default | Description |
| --- | --- | --- |
| `--body-chars <n>` | `256` | Maximum body characters to keep per thread/comment excerpt |
| `--no-vacuum` | _(off)_ | Skip size-reclaim `VACUUM`; a present or pending failure ledger still forces a secure rewrite |
| `--include-sync-failures` | _(off)_ | Keep the sync failure ledger while replacing every error message with a redaction marker |
| `--no-publish` | _(off)_ | Prune the runtime mirror without publishing the database and manifest back to a portable checkout |
| `--json` | _(off)_ | JSON output |

After pruning, commit and push both the database and its `.manifest.json` from the portable checkout the way you would for any Git repository.

## Derived generations: `gitcrawl portable export`

`portable export` creates a new, validated database-and-manifest generation from
the configured active database without changing that database. It is generic
artifact production: Gitcrawl owns the consistent SQLite snapshot, semantic
shaping, validation, size budget, digest, and manifest. Promotion into a
repository, replacement of an older generation, Git commits, and publication
remain external operations.

The initial snapshot uses SQLite's online backup API in bounded page chunks, so
committed WAL state is captured consistently and cancellation can be observed
between chunks without compacting the multi-gigabyte source first.
The private working copy disables journaling, synchronous writes, and secure
deletion because it is never exposed and is deleted on any error. Privacy and
durability come from the separate compact generation, full validation, hashing,
fsync, and atomic directory commit.

```bash
gitcrawl --config /path/to/config.toml portable export \
  --profile current-state-v1 \
  --body-chars 32 \
  --output-dir /path/to/artifact.next \
  --database-name openclaw__openclaw.sync.db \
  --public-path data/openclaw__openclaw.sync.db \
  --repository openclaw/openclaw \
  --max-bytes 99999999 \
  --json
```

The required `--profile` currently accepts only `current-state-v1`.
`--output-dir` is also required and must not exist; export builds beside it and
renames the complete pair into place only after validation. The database name
defaults to `gitcrawl.db` and must be a safe basename. `--public-path` defaults
to that name and is a clean relative slash path recorded in the manifest and
`portable_metadata`; it is a logical consumer path, never the source or output
host path. `--body-chars` defaults to `256`. `--max-bytes` is optional and
inclusive, so a deployment requiring a database smaller than 100,000,000 bytes
should pass `99999999` rather than relying on a Gitcrawl-specific hosting limit.
The optional `--repository owner/repo` is semantic export behavior: Gitcrawl
removes all other repositories and their dependent rows from the disposable
snapshot, verifies the exact remaining identity, and records it in the manifest.
Without the flag, multi-repository artifacts remain supported.
The manifest reports every retained table as a sorted `{name, rows}` object.
It includes a singular repository object for scoped exports and for unscoped
artifacts that naturally contain exactly one repository; multi-repository
artifacts omit that singular field.

The `current-state-v1` profile starts with portable v2 shaping and keeps current
repositories, issue and pull-request threads, current comments, compact thread
revisions and fingerprints, pull-request detail/review/check state, workflow
runs, and child observation memberships. It omits comment revision history,
generated thread key summaries, and derived cluster groups, memberships,
lineage, overrides, and closures. PR file identity and change counts remain,
while `pull_request_files.patch` diff payloads are set to `NULL`. It also removes ordinary non-unique indexes
while retaining primary keys, unique constraints, and explicit unique indexes.
The manifest records the exact dropped tables and indexes.

Derived exports record `column_profile=sanitized-compatibility` in SQLite
metadata and `columnProfile: sanitized-compatibility` in the manifest. They keep
the full-schema compatibility columns `repositories.raw_json`,
`threads.raw_json`, and `threads.body` to avoid three full-table SQLite rewrites;
the raw JSON values are empty and `threads.body` contains only `body_excerpt`.
Export rebuilds `threads` once from its stored constrained table definition,
preserving table constraints, explicit unique indexes, triggers, child foreign
keys, and all other columns while omitting ordinary transport indexes. Custom
INSERT/DELETE triggers are retained; unsupported custom UPDATE-trigger semantics
fail closed rather than silently diverging during the bulk copy.
The final `VACUUM INTO` ensures the removed full payload bytes are absent. The
portable schema identifier remains `gitcrawl-portable-sync-v2`, and ordinary
`portable prune` continues to physically drop these columns.

Those history and governance omissions are intentional data loss in the
generation, not a promise that every omission can be rebuilt. Opening the
export writable lets the current Gitcrawl migration recreate missing tables and
ordinary schema indexes, but the omitted historical snapshots, summaries, and
local maintainer decisions do not return. Rebuildable derived state may be
generated again from the retained current data where the relevant command
supports it.

Before removing transport-only indexes, export requires an empty
`foreign_key_check` while those indexes still make relationship proof efficient.
It then creates one compact final generation with `VACUUM INTO`, closes and
removes the larger private working database, and promotes the compact file
within staging. The compact file must pass `quick_check` and full
`integrity_check`; export then enforces the optional finalized byte
budget, hashes the database with SHA-256, writes and fsyncs the manifest, then
re-reads the pair without repeating the unindexed foreign-key scan. Concise
stage progress is written to stderr, including during JSON output. Any failure
or handled interrupt leaves the requested output directory absent and removes
the private staging directory.

| Flag | Default | Description |
| --- | --- | --- |
| `--profile <name>` | _(required)_ | Semantic export profile; currently `current-state-v1` |
| `--output-dir <path>` | _(required)_ | New artifact directory; must not already exist |
| `--database-name <name>` | `gitcrawl.db` | Safe database basename inside the generation |
| `--public-path <path>` | database name | Logical relative slash path recorded in metadata and the manifest |
| `--repository <owner/repo>` | _(unset)_ | Semantically restrict the artifact to exactly one repository |
| `--body-chars <n>` | `256` | Maximum body characters retained in compact excerpts |
| `--max-bytes <n>` | _(unset)_ | Inclusive maximum finalized database size |
| `--json` | _(off)_ | Stable structured result, including local source and output paths |

## A typical publishing flow

```bash
# In the portable store checkout, refresh upstream data into the local runtime mirror.
gitcrawl refresh owner/repo

# Prune for a small, shareable footprint and publish the database plus manifest into the checkout.
gitcrawl portable prune --body-chars 256

# Commit and push using normal Git from the configured portable checkout.
cd ~/Library/Application\ Support/gitcrawl/stores/gitcrawl-store
# Linux default: cd ~/.config/gitcrawl/stores/gitcrawl-store
git add data/openclaw__openclaw.sync.db data/openclaw__openclaw.sync.db.manifest.json
git commit -m "data: refresh openclaw/gitcrawl"
git push
```

Other agents and machines pull the new commit on their next read-only command.

## Cached search against a portable store

`gitcrawl search` works against portable-store data with one wrinkle: when the portable store has been pruned, generated document indexes may not be present. Search falls back to compact thread title/body data automatically — you keep useful results without the publisher needing to ship the full document indexes.

The v2 backup also keeps comments and PR-detail tables for local review, clustering, search, and TUI workflows.

## Caveats

- The portable store carries the SQLite database. It does not carry the Octopool `gh` cache.
- Vectors regenerated on each consumer's machine after `embed` are not shared; portable pruning removes vector tables from the published database.
- Portable stores are read-mostly. Multiple writers pushing concurrently — including concurrent `portable prune` publishers against the same checkout — race the way any Git workflow does; gate writes through a single publisher or a CI workflow.

## See also

- [Sync](/sync/) — what gets written into the database that ends up in the portable store
- [gh shim migration](/gh-shim/) — Octopool owns pooled `gh` reads now
