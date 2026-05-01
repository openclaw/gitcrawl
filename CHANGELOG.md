# Changelog

## Unreleased

- Route config path handling and SQLite openers through `crawlkit` so GitHub archive tooling shares the same foundation as the Slack, Discord, and Notion crawlers.
- Keep the existing `gitcrawl tui` as the family reference terminal interface and add CI smoke coverage for its help surface.

## 0.1.1 - 2026-04-30

- Fix portable store refreshes when local Git pull configuration tries to rebase onto multiple branch merge refs.
- Honor `GITCRAWL_GITHUB_BASE_URL` and `GITHUB_BASE_URL` during `gitcrawl sync`, matching cached search and test-server workflows.
- Fix cached `search issues|prs` against portable stores by using portable-safe thread body and raw JSON columns.
- Keep read-only portable-store commands responsive when the backing Git remote is unavailable by making refresh best-effort and non-interactive with bounded SSH connection attempts.

## 0.1.0 - 2026-04-30

- Add `gitcrawl sync --numbers` for exact issue and pull request hydration, including comment documents, without relying on list ordering or updated-time windows.
- Implement `gitcrawl refresh` and `gitcrawl embed` so synced repositories can generate OpenAI embeddings and rebuild durable clusters end to end.
- Add `gitcrawl sync --state open|closed|all` so incremental backups can refresh recently closed issues and pull requests.
- Default `gitcrawl sync` to `--state all`, keeping closed issue and pull request state fresh unless a narrower state is requested.
- Let `gitcrawl search` fall back to compact thread title/body data when portable stores have pruned generated document indexes.
- Refresh clean portable-store checkouts before read-only commands so `search`, `threads`, clusters, and the TUI see freshly published GitHub backup data automatically.
- Refresh portable-store status and clear stale SQLite sidecars so `doctor` and local queries report freshly pulled backup data instead of stale sync metadata.
- Open writable runtime mirrors for portable-store configs so `gitcrawl embed`, `refresh`, and semantic neighbor generation can persist local vectors without mutating the GitHub backup checkout.
- Show active primary cluster memberships by default in `clusters`, `durable-clusters`, and the TUI, with `--include-closed` reserved for historical audit views.
- Split generated clusters with bounded nearest-neighbor graph safeguards, GitHub reference evidence, and cross-kind score pruning so weak similarity bridges stop merging unrelated reports into one mega-cluster.
- Tighten clustering precision by ignoring ambiguous one-digit prose references and requiring weak embedding edges to share concrete title tokens unless they have high similarity or direct GitHub reference evidence.
- Treat later body-only issue references as weak evidence unless they share title overlap, while still preserving title and lead-body references for canonical issue/PR fix clusters.
- Hide GitHub-closed members from latest-run cluster summaries and details by default; `--include-closed` still shows the full historical cluster.
- Add release plumbing for GitHub release archives via GoReleaser.
