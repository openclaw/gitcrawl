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
gitcrawl clusters owner/repo
gitcrawl cluster-detail owner/repo --id 123
gitcrawl search owner/repo --query "download stalls"
gitcrawl tui
gitcrawl tui owner/repo
```

`gitcrawl tui` infers the most recently updated local repository when `owner/repo` is omitted. `serve` is intentionally not part of `gitcrawl`.
The TUI starts at `--min-size 5` so maintainer-significant clusters are visible first; pass `--min-size 1` to include singletons.

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
