---
title: TUI
nav_order: 11
permalink: /tui/
---

# TUI
{: .no_toc }

`gitcrawl tui` is the interactive cluster browser. Keyboard-first, mouse-friendly, refreshes from local SQLite every 15 seconds.
{: .fs-6 .fw-300 }

It is also the reference terminal interaction model for the crawl app family.
The shared `crawlkit/tui` browser used by Slack, Discord, and Notion archives
is expected to match its pane focus, sortable headers, mouse/right-click
actions, detail rendering, and status chrome wherever the data model allows it.

1. TOC
{:toc}

## Launching

```bash
gitcrawl tui owner/repo
gitcrawl --no-color tui owner/repo # disable terminal colors
gitcrawl tui                      # infers the most recently updated local repo
gitcrawl tui --min-size 5         # default; show clusters with ≥5 active members
gitcrawl tui --sort recent        # alternate sort
gitcrawl tui --layout focus       # wider detail pane with members below
gitcrawl tui --hide-closed        # focus only on currently open clusters
```

| Flag | Default | Description |
| --- | --- | --- |
| `--min-size <n>` | `5` | Minimum active member count |
| `--sort recent\|oldest\|size` | `size` | Cluster ordering |
| `--layout columns\|right-stack\|focus` | `columns` | Wide-screen pane layout |
| `--limit <n>` | `500` | Working-set cap (rows fetched into the TUI) |
| `--hide-closed` | _(off)_ | Hide locally closed clusters |
| `--include-closed` | _(deprecated)_ | Closed clusters are included by default |
| `--json` | _(off)_ | Emit a non-interactive JSON snapshot instead of launching the UI |

When `--json` is passed, the TUI command produces the same cluster summary the interactive view would render — useful for CI checks or for an agent that wants the same view a human would see.

## Default behavior

The TUI starts at `--min-size 5`, `--sort size`, and `--layout columns` so the first screen is the useful triage workload, not singleton noise. Pass `--min-size 1` when you intentionally want singletons (e.g., looking for orphans), or `--layout focus` when you want the detail pane wider with members below.

The view auto-refreshes from the local store every 15 seconds. There is no GitHub call from the TUI itself — to bring in fresh upstream data, run `gitcrawl sync` (or `refresh`) in another terminal and the TUI picks it up on the next tick.

## Keyboard

| Key | Action |
| --- | --- |
| `↑` / `↓` | Move within the active pane |
| `Tab` / `Shift+Tab` | Switch panes |
| `Enter` | Open detail for selected cluster or member; on a member, loads neighbors first |
| `a` | Open the action menu (cluster or member, depending on focus) |
| `#` | Jump to a specific issue or PR number or copied GitHub issue/PR URL |
| `n` | Load neighbors for the selected issue or PR |
| `p` | Switch between repositories already present in the local store |
| `s` | Cycle sort mode (`size` ↔ `recent` ↔ `oldest`, both directions) |
| `/` | Filter rows by substring |
| `q` | Quit |

The action menu opened with `a` mirrors the right-click menu, so every mouse action has a keyboard equivalent.

Jump input accepts the same thread references as the CLI: bare numbers, `#123`,
`issues/123`, `pull/123`, `owner/repo#123`, and full GitHub issue or pull
request URLs.

## Mouse

Mouse support is built in and works in most modern terminals (iTerm2, Kitty, Alacritty, WezTerm, recent macOS Terminal):

- **Click** a row to select it
- **Double-click** to open detail
- **Wheel** scrolls the focused pane
- **Right-click** opens the cluster or member action menu
- **Trackpad scroll** is buffered to avoid jumpy redraws

If your terminal does not pass through mouse events, all actions remain available via keyboard.

## Action menu

Cluster actions:

- Copy issue/PR URL or number
- Sort cluster members
- Filter to a member subset
- Jump to a referenced issue or PR
- Open canonical thread on GitHub
- Load neighbors for the canonical
- Local close / reopen
- Set canonical member
- Exclude / include member

Member actions:

- Copy URL / number
- Load neighbors
- Open on GitHub
- Local close / reopen this thread
- Exclude from cluster

These map directly onto the [governance](/governance/) commands. Anything you can do interactively, you can also script.

## Display rules

`gitcrawl clusters` and the TUI use the same display rules:

- Latest raw run clusters first
- Closed durable rows merged in as historical context
- Default sort is `size` (largest active membership first)
- GitHub-closed members are hidden from the latest-run view; pass `--include-closed` to see the full historical cluster

For an audit-style view that does not merge with the latest run, use `gitcrawl durable-clusters --include-closed`.

## Tips

- Resize your terminal — the panes reflow.
- A single repo with thousands of threads is fine; the working set is capped at 500 rows so the UI stays snappy.
- Run `gitcrawl refresh owner/repo` periodically in a sibling terminal; the TUI reflects new data on the next 15s tick.
- If the cluster you are looking for is missing, check `--min-size` and `--hide-closed`.
- The status bar at the bottom shows the active sort, filter, repo, and any warnings (e.g., "vector model mismatch — re-run embed").
