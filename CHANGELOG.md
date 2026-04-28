# Changelog

## Unreleased

- Implement `gitcrawl refresh` and `gitcrawl embed` so synced repositories can generate OpenAI embeddings and rebuild durable clusters end to end.
- Add `gitcrawl sync --state open|closed|all` so incremental backups can refresh recently closed issues and pull requests.
- Default `gitcrawl sync` to `--state all`, keeping closed issue and pull request state fresh unless a narrower state is requested.
- Let `gitcrawl search` fall back to compact thread title/body data when portable stores have pruned generated document indexes.
- Refresh clean portable-store checkouts before read-only commands so `search`, `threads`, clusters, and the TUI see freshly published GitHub backup data automatically.
