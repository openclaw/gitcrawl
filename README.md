# gitcrawl

`gitcrawl` is a local-first GitHub issue and pull request crawler for maintainer triage.

Data stays local in SQLite. The primary runtime surfaces are the CLI, JSON command output, and a future TUI. There is no local HTTP API.

## Status

Early bootstrap. The implementation is being built in small commits.

## Planned Commands

```bash
gitcrawl init
gitcrawl doctor
gitcrawl sync owner/repo
gitcrawl refresh owner/repo
gitcrawl clusters owner/repo --json
gitcrawl cluster-detail owner/repo --id 123 --json
gitcrawl search owner/repo --query "download stalls" --json
gitcrawl tui owner/repo
```

`serve` is intentionally not part of `gitcrawl`.

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
