---
title: Refresh and embed
nav_order: 7
permalink: /refresh-and-embed/
---

# Refresh and embed
{: .no_toc }

`gitcrawl refresh` is the one command most users want. It runs sync → embed → cluster in order, with the same flags you would use individually.
{: .fs-6 .fw-300 }

1. TOC
{:toc}

## refresh

```bash
gitcrawl refresh owner/repo
```

By default this performs:

1. **Sync** — open + recently closed issues and PRs (see [Sync](/sync/))
2. **Embed** — fill `thread_vectors` for any thread whose document changed
3. **Cluster** — rebuild durable clusters with the standard thresholds

Disable any stage with `--no-sync`, `--no-embed`, `--no-cluster`. The remaining stages still run; failures are reported per stage in the JSON output.

### Stage-specific flags

`refresh` forwards flags through to the underlying stages:

| Forwarded to | Flag |
| --- | --- |
| sync | `--since`, `--state`, `--limit`, `--include-comments`, `--include-pr-details`, `--with pr-details` |
| embed | `--limit` |
| cluster | `--threshold` (0.80), `--min-size` (1), `--max-cluster-size` (40), `--k` (16), `--cross-kind-threshold` (0.93), `--strict-vectors` |

`--include-code` is accepted but currently a no-op.

The cluster stage excludes stale configured vectors and reports incomplete coverage as partial without retiring unprocessed clusters. When no fresh configured vectors exist, the default keeps the established compatible fallback to existing vectors. Use `--strict-vectors` to fail before mutation unless configured coverage is complete, run `gitcrawl embed owner/repo` to repair coverage, or use `--no-cluster` when intentionally refreshing only upstream stages.

### JSON output

```bash
gitcrawl refresh owner/repo --json
```

```json
{
  "repository": "owner/repo",
  "sync": { "selected": 124, "inserted": 12, "updated": 9, "run_id": 42 },
  "embed": { "selected": 21, "embedded": 21, "skipped": 0, "failed": 0, "model": "text-embedding-3-small", "run_id": 43 },
  "cluster": {
    "threshold": 0.8, "cross_kind": 0.93, "min_size": 1, "max_size": 40, "k": 16,
    "vector_count": 312, "edge_count": 1042, "cluster_count": 87, "member_count": 312, "run_id": 44,
    "vector_coverage": {
      "supported": true, "eligible": 312, "covered": 312, "fresh": 312,
      "missing": 0, "stale": 0, "complete": true
    }
  }
}
```

Each stage object mirrors the JSON shape of the standalone command. You can read the per-stage `run_id` later via `gitcrawl runs --kind sync|embedding|cluster`.

## embed

```bash
gitcrawl embed owner/repo
```

Generates OpenAI embeddings for any thread whose document hash has changed since its last embedding. Works through the database in batches (default size 64) with bounded concurrency (default 2).

### Flags

| Flag | Default | Description |
| --- | --- | --- |
| `--number <ref>` | _(any)_ | Embed a single issue/PR by number or copied GitHub URL |
| `--limit <n>` | _(no limit)_ | Maximum rows to embed in this run |
| `--force` | _(off)_ | Re-embed every selected row, ignoring content hash |
| `--include-closed` | _(off)_ | Include closed threads |

`--number` accepts bare numbers, `#123`, `issues/123`, `pull/123`,
`owner/repo#123`, and full GitHub issue or pull request URLs.

### When to `--force`

You should rarely need it. The pipeline auto-forces a rebuild when:

- The configured embedding model changes (`GITCRAWL_EMBED_MODEL` or `embed_model` in config)
- The embedding input rune cap changes (so older, larger-cap vectors are not silently mixed in)

Use `--force` manually if you have manually edited vectors, or want to confirm an output is reproducible from scratch.

### Failure handling

OpenAI errors are retried with backoff unless `GITCRAWL_OPENAI_RETRY_DISABLED=1`. The JSON output includes a `failures` array with batch-level diagnostics (`batch_start`, `batch_end`, `attempts`, `status`, `code`, `message`) so partial failures do not silently drop rows.

Oversized inputs are capped before being sent upstream so a single huge body cannot exceed the model's input limit.

### JSON output

```json
{
  "repository": "owner/repo",
  "model": "text-embedding-3-small",
  "basis": "title_original",
  "selected": 21,
  "embedded": 20,
  "skipped": 0,
  "failed": 1,
  "retries": 3,
  "status": "ok",
  "failures": [
    { "batch_start": 16, "batch_end": 17, "attempts": 3, "status": 429, "type": "rate_limit", "code": "rate_limit_exceeded", "message": "..." }
  ],
  "run_id": 43
}
```

## runs

Inspect what `refresh`, `sync`, `embed`, or `cluster` actually did:

```bash
gitcrawl runs owner/repo --kind sync       # default kind
gitcrawl runs owner/repo --kind embedding
gitcrawl runs owner/repo --kind cluster
```

Each row carries `started_at`, `finished_at`, `status`, and `stats_json` — useful when an agent needs to know whether a sync is fresh enough or whether the last cluster pass converged.

## Cost notes

- **GitHub.** Sync uses standard REST endpoints; the API quota is the dominant cost on busy repos. Use `--include-comments` and `--with pr-details` selectively.
- **OpenAI.** `text-embedding-3-small` is inexpensive but not free. `embed` is bounded by `--limit` if you want to stay under a budget on initial backfills.
- **Disk.** Vectors, generated documents, and raw API payloads grow with the repo. The portable-store flow includes `gitcrawl portable prune` to keep published payloads small while retaining compact comments and PR details — see [Portable stores](/portable-stores/).
