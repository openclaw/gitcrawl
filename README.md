# gitcrawl

`gitcrawl` is a local-first GitHub issue and pull request crawler for maintainer triage.

Data stays local in SQLite. The primary runtime surfaces are the CLI, JSON command output, and the terminal UI. There is no local HTTP API.

## Status

Early bootstrap. The implementation is being built in small commits.

## Commands

```bash
gitcrawl init
gitcrawl doctor
gitcrawl sync owner/repo
gitcrawl sync owner/repo --state open
gitcrawl refresh owner/repo
gitcrawl cluster owner/repo --threshold 0.80
gitcrawl clusters owner/repo
gitcrawl durable-clusters owner/repo
gitcrawl cluster-detail owner/repo --id 123
gitcrawl cluster-explain owner/repo --id 123
gitcrawl close-thread owner/repo --number 123 --reason "duplicate handled"
gitcrawl reopen-thread owner/repo --number 123
gitcrawl close-cluster owner/repo --id 123 --reason "handled"
gitcrawl reopen-cluster owner/repo --id 123
gitcrawl exclude-cluster-member owner/repo --id 123 --number 456 --reason "not the same bug"
gitcrawl include-cluster-member owner/repo --id 123 --number 456
gitcrawl set-cluster-canonical owner/repo --id 123 --number 456
gitcrawl neighbors owner/repo --number 123 --limit 10
gitcrawl search owner/repo --query "download stalls"
gitcrawl search issues "download stalls" -R owner/repo --state open --json number,title,state,url,updatedAt,labels --limit 30
gitcrawl search prs "manifest cache" -R owner/repo --state open --json number,title,state,url,updatedAt,isDraft,author --limit 20
gitcrawl search issues "hot loop" -R owner/repo --state open --sync-if-stale 5m --json number,title,url
gitcrawl tui
gitcrawl tui owner/repo
```

`gitcrawl clusters` and `gitcrawl tui` match ghcrawl's display view: latest raw run clusters first, closed durable rows merged as historical context, sorted by size by default. Pass `--hide-closed` to focus only currently open clusters. `gitcrawl durable-clusters` stays on governed durable rows and needs `--include-closed` for inactive rows.
`gitcrawl cluster` and `gitcrawl refresh` build ghcrawl-shaped durable clusters by default (`--threshold 0.80`, `--min-size 1`, `--max-cluster-size 40`, `--k 16`, `--cross-kind-threshold 0.93`): every active vector-backed thread is represented, singleton rows use `singleton_orphan`, multi-member rows use `duplicate_candidate`, and stable IDs are derived from the representative thread. They also add deterministic GitHub reference evidence for direct issue/PR links such as `#123`, `issues/123`, and `pull/123`. Weak embedding edges need concrete title-token overlap unless their similarity is already high, which keeps generic low-confidence bridges from forming unrelated clusters.
`gitcrawl tui` infers the most recently updated local repository when `owner/repo` is omitted. `serve` is intentionally not part of `gitcrawl`.
`gitcrawl sync` fetches open issues and pull requests by default. Pass `--state all` or `--state closed` for explicit backfill workflows; incremental open syncs with `--since` also sweep recently closed items so local open state does not rot.
`gitcrawl search issues|prs` accepts the common `gh search` shape (`<query> -R owner/repo --state open --json fields --limit N`) and answers from the local SQLite cache. It is intended for discovery without spending GitHub REST search quota; use `gh` for final live verification and GitHub write actions. Pass `--sync-if-stale 5m` to perform one metadata sync before the cached search when the local repository mirror is older than that duration.
The TUI starts at `--min-size 5` and `--sort size`, like ghcrawl's saved default, so the first screen is the useful cluster workload instead of singleton noise. Pass `--min-size 1` when you intentionally want singleton clusters. Mouse support is built in: click rows, wheel panes, and right-click for copy, sort, filter, jump, link, neighbor, local close/reopen, and member triage actions. Press `a` to open the same action menu from the keyboard, `#` to jump directly to an issue or PR number, `p` to switch between repositories already present in the local store, or `n` to load neighbors for the selected issue or PR. Enter from the members pane also loads neighbors before opening detail. The TUI quietly refreshes from the local store every 15 seconds.

## Local Defaults

- config: `~/.config/gitcrawl/config.toml`
- database: `~/.config/gitcrawl/gitcrawl.db`
- cache: `~/.config/gitcrawl/cache`
- vectors: `~/.config/gitcrawl/vectors`
- logs: `~/.config/gitcrawl/logs`

## Requirements

- Go 1.26+
- a GitHub token for sync commands
- an OpenAI API key only for summary and embedding commands

## Development

```bash
go test ./...
go build ./cmd/gitcrawl
```
