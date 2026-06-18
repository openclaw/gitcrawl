---
title: Governance
nav_order: 10
permalink: /governance/
---

# Governance
{: .no_toc }

Maintainer overrides on top of the cluster algorithm. All changes are local; gitcrawl never writes back to GitHub.
{: .fs-6 .fw-300 }

1. TOC
{:toc}

## Why governance exists

The cluster algorithm is good but not perfect. Sometimes it misses an obvious duplicate, or glues two unrelated reports together, or picks a poor representative thread. Governance commands let you correct the result without re-tuning thresholds or re-running embeddings.

Every override is recorded with a reason and persists across `cluster`/`refresh` runs because durable cluster IDs are stable. The TUI exposes the same actions via right-click and the `a` action menu.

## Local close

Mark a thread or a cluster as "handled locally — do not show me this again."

```bash
gitcrawl close-thread owner/repo --number 123 --reason "duplicate handled"
gitcrawl close-thread owner/repo --number https://github.com/owner/repo/issues/123 --reason "duplicate handled"
gitcrawl reopen-thread owner/repo --number 123

gitcrawl close-cluster owner/repo --id 42 --reason "all members handled"
gitcrawl reopen-cluster owner/repo --id 42
```

The reason defaults to `CLI manual close` and is stored alongside the override for audit. Locally closed threads and clusters are filtered out by `--hide-closed` across `clusters`, `cluster-detail`, the TUI, and search.

This **does not** change anything on GitHub. It is purely a local triage signal — useful when you have already commented "duplicate of #X" on the upstream issue and want to clear it from your maintainer view.

JSON output:

```json
{ "repository": "owner/repo", "number": 123, "reason": "duplicate handled", "closed": true }
```

## Member exclusion

Pull a single thread out of a cluster, or pull it back in.

```bash
gitcrawl exclude-cluster-member owner/repo --id 42 --number 456 --reason "different repro"
gitcrawl exclude-cluster-member owner/repo --id 42 --number owner/repo#456 --reason "different repro"
gitcrawl include-cluster-member owner/repo --id 42 --number 456
```

Use this when the algorithm is mostly right but caught one false positive. The override travels with the cluster's stable ID, so re-clustering does not undo your decision.

JSON output:

```json
{
  "repository": "owner/repo",
  "override": { "cluster_id": 42, "thread_number": 456, "kind": "exclude", "reason": "different repro", "created_at": "..." },
  "excluded": true
}
```

## Canonical member

Pin which thread represents the cluster — this is what shows up as the row title in `clusters` and the TUI summary.

```bash
gitcrawl set-cluster-canonical owner/repo --id 42 --number 123 --reason "main tracking issue"
```

The chosen `--number` must already be a member of the cluster. The TUI's right-click menu has a "set canonical" entry that calls this command.

All governance `--number` flags accept the same thread-reference forms as sync:
bare numbers, `#123`, `issues/123`, `pull/123`, `owner/repo#123`, and full
GitHub issue or pull request URLs. The command still applies only to the
`owner/repo` argument you pass to gitcrawl; URL input is accepted so copied
GitHub links can be pasted directly.

## Reopen and undo

There is no separate `undo`. The inverse commands are explicit:

| Action | Inverse |
| --- | --- |
| `close-thread` | `reopen-thread` |
| `close-cluster` | `reopen-cluster` |
| `exclude-cluster-member` | `include-cluster-member` |
| `set-cluster-canonical` | `set-cluster-canonical --number <other>` |

Each call records a fresh override row, so the audit history is preserved.

## Reading overrides

`gitcrawl cluster-detail` returns active overrides as part of the JSON payload, and `gitcrawl runs --kind cluster` lists when each clustering run was performed. To inspect raw override history you can query SQLite directly:

```bash
sqlite3 "$(gitcrawl doctor --json | jq -r .db_path)" \
  "SELECT cluster_id, thread_number, kind, reason, created_at
   FROM cluster_member_overrides ORDER BY created_at DESC LIMIT 20;"
```

(The schema is internal and may change between versions — prefer the JSON outputs from the CLI for stable contracts.)

## Workflow patterns

### "Triage this cluster, then move on"

```bash
gitcrawl cluster-detail owner/repo --id 42 --body-chars 600 | less
# ...read, decide canonical, add labels via gh, comment via gh...
gitcrawl set-cluster-canonical owner/repo --id 42 --number 123
gitcrawl close-cluster owner/repo --id 42 --reason "consolidated under #123"
```

### "This thread doesn't belong here"

```bash
gitcrawl exclude-cluster-member owner/repo --id 42 --number 456 --reason "different repro"
gitcrawl neighbors owner/repo --number 456 --limit 10   # find a better home manually
```

### "I'm done with this issue locally even though upstream is still open"

```bash
gitcrawl close-thread owner/repo --number 789 --reason "answered in chat"
```

The thread stays open on GitHub; only your local triage view hides it.

## What governance does *not* do

- It does not edit, label, comment on, or close GitHub issues. Use `gh` for that.
- It does not retrain embeddings or reshape the underlying graph — it overlays decisions on top of the algorithm output.
- It does not propagate to other gitcrawl installations unless you publish your database via a [portable store](/portable-stores/).
