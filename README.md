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
gitcrawl refresh owner/repo
gitcrawl cluster owner/repo --threshold 0.82
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
gitcrawl tui
gitcrawl tui owner/repo
```

`gitcrawl tui` infers the most recently updated local repository when `owner/repo` is omitted. `serve` is intentionally not part of `gitcrawl`.
The TUI starts at `--min-size 5` so maintainer-significant clusters are visible first; pass `--min-size 1` to include singletons. Mouse support is built in: click rows, wheel panes, and right-click for copy, sort, filter, jump, link, neighbor, local close/reopen, and member triage actions. Press `a` to open the same action menu from the keyboard, `#` to jump directly to an issue or PR number, `p` to switch between repositories already present in the local store, or `n` to load neighbors for the selected issue or PR. Enter from the members pane also loads neighbors before opening detail. The TUI quietly refreshes from the local store every 15 seconds.

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
