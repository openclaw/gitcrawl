---
title: Conversation capture
nav_order: 7
permalink: /capture/
---

# Conversation capture
{: .no_toc }

Export a deterministic, code-free issue and pull-request snapshot for another
local tool.
{: .fs-6 .fw-300 }

1. TOC
{:toc}

## Prepare the archive

`capture` reads the local database. It does not call GitHub or fill missing
comments.

`capture` does not support `--github-token-command`. It rejects this selection
before opening the store or writing output, without invoking the command.
Offline quota provenance across managed-credential invocations is not yet
supported; repeating the sync does not resolve this limitation. Static-token
capture is unchanged.

Run a successful sync before each capture:

```bash
gitcrawl sync owner/repo --state all --include-comments
gitcrawl capture owner/repo --output capture.json
```

Keep `--state all` for incremental syncs after the initial backfill; add
`--since` to bound the GitHub update window. Keep `--include-comments` so the
local rows include issue comments, submitted reviews, and review comments.

The command fails when the repository has no successful all-state list sync
covering the requested capture range or no stable GitHub repository ID.
Targeted, limited, open-only, and closed-only syncs cannot serve as repository
freshness anchors. A sync using `--since` can anchor a capture only when its
bound is at or before the capture's `--since` value.

Every exported thread must also have a completed comment observation for that
thread's current GitHub `updated_at` revision. The observation records the exact
database comment IDs returned by GitHub; comments retained in the archive but
absent from that completed observation are not exported. These checks prevent a
partial sync or disappeared upstream comment from leaking stale rows into a
capture.

## Contract

The default and only supported schema is `gitcrawl.capture.v1`:

```bash
gitcrawl capture owner/repo \
  --schema gitcrawl.capture.v1 \
  --output capture.json
```

The top-level object contains:

- `schema`;
- `producer_version`;
- the repository's stable GitHub ID and current `owner/name`;
- `rate_limit`, the last observation made by the same token during sync;
- `synced_at`, taken from the latest successful local sync;
- issues and pull requests ordered by number and kind.

Each thread contains its stable GitHub ID, number, kind, state, title, body,
author, URL, labels, assignees, timestamps, comments, and semantic
`content_hash`. Comments are ordered by source time, kind, and stable ID.

The semantic hash covers the exported thread before `content_hash` is set. It
changes when exported thread or comment content changes.

Repository metadata, sync metadata, threads, and comments are read from one
SQLite transaction so a concurrent sync cannot mix archive generations in one
capture.

`rate_limit` contains only `resource`, `limit`, `remaining`, `reset_at`,
`observed_at`, and nullable `retry_after_seconds`. It never contains the token,
token hash, host, headers, response body, or stderr. Capture resolves the same
token used by the preceding sync only to select its private local observation;
the token does not enter the output. The command fails if that observation is
unavailable.

## Privacy boundary

The capture includes private issue, pull-request, and comment text when the
local archive contains it. Protect the output like the source repository.

The capture excludes:

- repository, thread, and comment `raw_json`;
- diff hunks, inline review paths and positions, and commit IDs;
- PR files, patches, commits, checks, workflows, and review-thread metadata;
- summaries, embeddings, clusters, local governance, and source indexes;
- credentials and local machine paths.

Submitted reviews and review comments come from the normalized `comments`
table. The export does not read code-bearing PR-detail tables.

## Bounded capture

Use `--since` to include threads whose GitHub `updated_at` is at or after an
RFC 3339 instant:

```bash
gitcrawl capture owner/repo \
  --since 2026-07-01T00:00:00Z \
  --output july.json
```

Run the corresponding all-state bounded sync first. `capture` never treats an
absent local row as proof that GitHub has no matching thread.

## Output behavior

Without `--output`, `capture` writes the schema object as JSON to stdout.

With `--output`, Gitcrawl writes a mode `0600` temporary file in the target
directory, flushes it, and renames it over the target path. A failed build or
write leaves no partial target or temporary file.

Use a fresh output path for portable behavior across operating systems.
