# gitcrawl Spec

## Product Contract

`gitcrawl` is a local-first GitHub maintainer triage tool written in Go.

The target is a compact, local SQLite workflow for syncing, searching, clustering, and reviewing related GitHub issues and pull requests.

## In Scope

- local SQLite storage
- metadata-first GitHub sync for open issues and pull requests
- optional comment, review, review-comment, and PR code hydration
- canonical thread document building
- FTS search
- OpenAI summaries and embeddings
- deterministic fingerprints
- vector search
- clustering and durable cluster governance
- portable sync export/import
- CLI JSON surfaces for automation and agents
- TUI browsing after core JSON contracts settle

## Out Of Scope

- local HTTP API
- hosted service runtime
- browser web UI
- GitHub write-back actions

## Architecture

- `cmd/gitcrawl`: executable entrypoint
- `internal/cli`: command parsing and output
- `internal/config`: config and env resolution
- `internal/store`: SQLite schema and persistence
- `internal/github`: GitHub API client
- `internal/syncer`: repository sync workflows
- `internal/documents`: canonical document generation
- `internal/openai`: OpenAI summaries and embeddings
- `internal/vector`: vector search abstraction
- `internal/cluster`: similarity and durable cluster governance
- `internal/search`: keyword, semantic, and hybrid search
- `internal/portable`: compact sync export/import
- `internal/tui`: terminal UI

TUI guidance:

- `gitcrawl tui [owner/repo]` is a supported command; omit `owner/repo` to use the most recently updated local repository
- keyboard-first navigation is required
- mouse support is optional polish
- right-click must not be required for primary actions because terminal mouse support is inconsistent
- avoid decorative glyph noise or transient rendering debris in dense panes

## Command Surface

No `serve` command.

Public commands:

- `init`
- `doctor`
- `configure`
- `version`
- `sync`
- `refresh`
- `summarize`
- `key-summaries`
- `embed`
- `cluster`
- `threads`
- `runs`
- `clusters`
- `durable-clusters`
- `cluster-detail`
- `cluster-explain`
- `neighbors`
- `search`
- `close-thread`
- `close-cluster`
- `exclude-cluster-member`
- `include-cluster-member`
- `set-cluster-canonical`
- `merge-clusters`
- `split-cluster`
- `export-sync`
- `import-sync`
- `validate-sync`
- `portable-size`
- `sync-status`
- `optimize`
- `tui`
- `completion`

`search` also supports the common `gh search` read-only shape for cached discovery:

```text
gitcrawl search issues <query> -R owner/repo --state open --json number,title,state,url,updatedAt,labels --limit 30
gitcrawl search prs <query> -R owner/repo --state open --json number,title,state,url,updatedAt,isDraft,author --limit 20
gitcrawl search issues <query> -R owner/repo --state open --sync-if-stale 5m --json number,title,url
```

This compatibility path reads from local SQLite by default. It avoids GitHub REST search quota and is not a replacement for final live `gh` verification before comments, closes, labels, or merges. `--sync-if-stale <duration>` may run one metadata sync first when the repository mirror is older than the requested max age; the search result itself still comes from SQLite.

## Config

Default config path:

```text
~/.config/gitcrawl/config.toml
```

Default database path:

```text
~/.config/gitcrawl/gitcrawl.db
```

Primary environment variables:

- `GITCRAWL_CONFIG`
- `GITHUB_TOKEN`
- `OPENAI_API_KEY`
- `GITCRAWL_DB_PATH`
- `GITCRAWL_SUMMARY_MODEL`
- `GITCRAWL_EMBED_MODEL`

Legacy environment aliases may be supported only when they do not leak old naming into user-facing output.
