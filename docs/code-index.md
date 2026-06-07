---
title: Code indexing
nav_order: 9
permalink: /code-index/
---

# Code indexing
{: .no_toc }

Load tracked source files from a local Git checkout into gitcrawl's separate
code-document index.
{: .fs-6 .fw-300 }

1. TOC
{:toc}

## Index a checkout

Mirror the GitHub repository first, then point gitcrawl at a checkout:

```bash
gitcrawl sync owner/repo
gitcrawl code index owner/repo --path /path/to/checkout
```

The repository identity comes from `owner/repo`; `--path` supplies the local
Git worktree. The command records the current `HEAD`, whether tracked files are
dirty, file counts, indexed bytes, and skip counts.

Defaults:

| Flag | Default | Purpose |
| --- | --- | --- |
| `--path` | `.` | Git checkout to scan |
| `--max-file-bytes` | `524288` | Skip larger files |
| `--max-total-bytes` | `268435456` | Refuse unexpectedly large text corpora |
| `--max-files` | `100000` | Refuse unexpectedly large text corpora |

Only stage-0 regular blobs returned by `git ls-files --stage` are considered.
Symlinks, submodules, paths that resolve outside the checkout, other
non-regular files, tracked paths absent from the worktree, NUL-containing
files, invalid UTF-8 files, and oversized files are skipped. Untracked files
are ignored.

Each successful index replaces the previous code snapshot for that repository,
so deleted and renamed paths do not linger in search.

Code indexing requires a normal local database. Portable-store runtime mirrors
are refreshed from their published source and therefore reject `code index`;
the source corpus is never added to the portable payload.

## Search source

```bash
gitcrawl search owner/repo --query "RefreshManifest" --scope code
gitcrawl search owner/repo --query "manifest cache" --scope all
```

Code search uses local SQLite FTS. `--scope all` interleaves thread results and
code results within the requested limit.

## Boundaries

Code documents are deliberately separate from GitHub thread documents. They do
not enter OpenAI embedding requests, semantic neighbors, or durable duplicate
clusters. Portable-store and cloud publish flows also remain focused on GitHub
archive data.
