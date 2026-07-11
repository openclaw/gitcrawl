---
title: Clustering
nav_order: 9
permalink: /clustering/
---

# Clustering
{: .no_toc }

Group related issues and pull requests using vector similarity, hardened with deterministic GitHub reference evidence and cross-kind safeguards.
{: .fs-6 .fw-300 }

1. TOC
{:toc}

## How it works

Clustering builds a sparse nearest-neighbor graph over the local vector store. For each thread, gitcrawl picks the top `k` most similar threads (default 16). Edges below the cosine threshold (default 0.80) are dropped. The remaining graph is split into connected components capped at `--max-cluster-size` members.

Two safeguards keep mega-clusters from forming:

- **Title-token overlap.** A weak embedding edge needs concrete shared title tokens (4+ char alphanumeric tokens) unless its similarity is already high (≥ 0.90) or there is direct GitHub reference evidence (`#123`, `pull/123`, `issues/123`).
- **Cross-kind pruning.** Edges connecting issues to pull requests need a higher floor (`--cross-kind-threshold`, default 0.93) than issue↔issue or PR↔PR edges.

GitHub references found in titles or in the first ~240 characters of bodies generate **deterministic reference edges** with score 0.94. Body-only references later in the document are treated as weak evidence (need title-token overlap or other support). Single-digit numbers in prose are ignored as ambiguous; references must be at least two digits or use a fully qualified form.

The result is written to two tables that survive across runs:

- `durable_clusters` — stable cluster rows with stable IDs derived from the representative thread
- `durable_cluster_members` — thread-to-cluster mappings with override metadata

## Generate clusters

```bash
gitcrawl cluster owner/repo
```

The defaults match ghcrawl's tuning so the output is comparable across tools:

| Flag | Default | Description |
| --- | --- | --- |
| `--threshold <float>` | `0.80` | Minimum cosine score for an edge |
| `--cross-kind-threshold <float>` | `0.93` | Minimum cosine score for issue↔PR edges |
| `--min-size <n>` | `1` | Minimum members per emitted cluster |
| `--max-cluster-size <n>` | `40` | Hard cap on cluster size |
| `--k <n>` | `16` | Nearest-neighbor fanout per thread |
| `--limit <n>` | _(no limit)_ | Maximum fresh vector rows to consider; explicitly permits a partial run |
| `--model <name>` | _(config)_ | Embedding model override |
| `--basis <name>` | _(config)_ | Embedding basis override |
| `--include-closed` | _(off)_ | Include closed threads |

Every active vector-backed thread is represented in the result: singleton clusters use `kind = singleton_orphan`, multi-member clusters use `kind = duplicate_candidate`.

Without `--limit`, clustering is fail-closed: it verifies the exact configured model/basis coverage and refuses to mutate durable clusters when vectors are missing or stale. It never falls back to vectors from another model or basis. `--limit` preserves the existing intentional partial-run convention, reports incomplete `vector_coverage` in JSON, excludes stale vectors, and does not retire clusters outside the processed subset.

## List clusters

```bash
gitcrawl clusters owner/repo
gitcrawl clusters owner/repo --sort size --min-size 5
gitcrawl clusters owner/repo --sort recent
gitcrawl clusters owner/repo --hide-closed
```

| Flag | Default | Description |
| --- | --- | --- |
| `--sort recent\|oldest\|size` | `size` | Ordering |
| `--min-size <n>` | _(none)_ | Minimum active member count |
| `--limit <n>` | _(no limit)_ | Maximum cluster rows |
| `--hide-closed` | _(off)_ | Hide locally closed clusters |
| `--include-closed` | _(deprecated)_ | Closed clusters are included by default |

`gitcrawl clusters` shows the latest raw run's clusters first and merges closed durable rows in as historical context. For a strict durable-only audit view (no merging with the latest run), use:

```bash
gitcrawl durable-clusters owner/repo --include-closed
```

GitHub-closed members are hidden from latest-run cluster summaries by default; pass `--include-closed` to see the full historical view.

## Cluster report

```bash
gitcrawl clusters-report owner/repo --limit 10 --min-size 5
gitcrawl clusters-report owner/repo --json
```

| Flag | Default | Description |
| --- | --- | --- |
| `--sort recent\|oldest\|size` | `size` | Ordering |
| `--min-size <n>` | `5` | Minimum active member count |
| `--limit <n>` | `10` | Maximum cluster rows |
| `--member-limit <n>` | `8` | Maximum member rows per cluster |
| `--body-chars <n>` | `240` | Body snippet length per member |
| `--hide-closed` | _(off)_ | Hide locally closed clusters |
| `--json` | _(off)_ | Emit the hydrated report payload |

The Markdown report uses the same display view as `gitcrawl clusters`, then adds an at-a-glance table, per-cluster metadata, member tables, and key snippets.

## Inspect a cluster

```bash
gitcrawl cluster-detail owner/repo --id 123
gitcrawl cluster-explain owner/repo --id 123    # alias
```

| Flag | Default | Description |
| --- | --- | --- |
| `--id <n>` | _(required)_ | Cluster ID |
| `--source auto\|run\|durable` | `auto` | Choose latest raw-run cluster IDs or durable cluster IDs when they collide |
| `--member-limit <n>` | _(no limit)_ | Maximum members to return |
| `--body-chars <n>` | `280` | Body snippet length per member |
| `--hide-closed` | _(off)_ | Hide locally closed members |

`cluster-explain` is the same command — it exists so the verb reads naturally in agent prompts ("explain why these things ended up together").

## Find similar threads (neighbors)

```bash
gitcrawl neighbors owner/repo --number 123 --limit 10
```

| Flag | Default | Description |
| --- | --- | --- |
| `--number <n>` | _(required)_ | Source issue/PR |
| `--limit <n>` | `10` | Maximum neighbors |
| `--threshold <float>` | `0.2` | Minimum cosine score |
| `--include-closed` | _(off)_ | Include closed issue and pull request vectors |

Useful for "what else looks like this?" without committing to a cluster. If the source row is present but lacks a current embedding, the CLI command backfills that one row when an OpenAI key is configured; otherwise it prints the exact `gitcrawl embed --number ... --limit 1` command to run. The TUI's `n` shortcut and "Enter on a member" also load vector neighbors for the selected row.

## Tuning recipes

### My clusters are too greedy

Symptom: unrelated bug reports merged together.

```bash
gitcrawl cluster owner/repo --threshold 0.85 --cross-kind-threshold 0.95
```

Tighter thresholds drop more weak edges. The `--cross-kind-threshold` raise specifically helps when an issue and a PR keep getting glued together because of shared boilerplate.

### My clusters are too sparse

Symptom: clear duplicates landing in separate clusters.

```bash
gitcrawl cluster owner/repo --threshold 0.75 --k 24
```

Lower threshold + higher fanout. Watch for false merges via `cluster-detail`.

### Make a single big cluster smaller

Symptom: one cluster has 40 members and is incoherent.

```bash
gitcrawl cluster owner/repo --max-cluster-size 20
```

Or slice it manually:

```bash
gitcrawl exclude-cluster-member owner/repo --id 12 --number 456 --reason "different repro"
```

See [Governance](/governance/) for the full override workflow.

## Re-clustering and stable IDs

Durable cluster IDs are derived from the representative thread, so they survive re-runs of `cluster` and `refresh`. This means:

- Local closes (`close-cluster`), exclusions, and canonical member overrides persist across re-clustering
- You can safely re-cluster after every refresh without losing maintainer state

Cluster runs are recorded in `run_records` and visible via `gitcrawl runs --kind cluster`.

## See also

- [Governance](/governance/) — close clusters, exclude members, set canonical
- [TUI](/tui/) — the interactive cluster browser
- [Concepts](/concepts/#cluster) — durable clusters and cluster kinds
