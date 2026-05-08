# gitcrawl

<img width="1797" height="1096" alt="Screenshot 2026-04-30 at 00 45 36" src="https://github.com/user-attachments/assets/54a0a6cf-5862-451d-9552-5d18656976ff" />

`gitcrawl` is a local-first GitHub issue and pull request crawler for maintainer triage. Data stays local in SQLite. The primary runtime surfaces are the CLI, JSON command output, and the terminal UI. There is no local HTTP API.

Full documentation: [gitcrawl.sh](https://gitcrawl.sh)

## Status

Early bootstrap. The implementation is being built in small commits.

## Commands

```bash
gitcrawl init
gitcrawl doctor
gitcrawl metadata --json
gitcrawl status --json
gitcrawl sync owner/repo
gitcrawl sync owner/repo --state open
gitcrawl sync owner/repo --numbers 123,456 --include-comments
gitcrawl sync owner/repo --numbers https://github.com/owner/repo/issues/123 --with pr-details
gitcrawl refresh owner/repo
gitcrawl cluster owner/repo --threshold 0.80
gitcrawl clusters owner/repo
gitcrawl durable-clusters owner/repo
gitcrawl cluster-detail owner/repo --id 123
gitcrawl cluster-explain owner/repo --id 123
gitcrawl close-thread owner/repo --number 123 --reason "duplicate handled"
gitcrawl close-thread owner/repo --number https://github.com/owner/repo/issues/123 --reason "handled"
gitcrawl reopen-thread owner/repo --number 123
gitcrawl close-cluster owner/repo --id 123 --reason "handled"
gitcrawl reopen-cluster owner/repo --id 123
gitcrawl exclude-cluster-member owner/repo --id 123 --number 456 --reason "not the same bug"
gitcrawl include-cluster-member owner/repo --id 123 --number 456
gitcrawl set-cluster-canonical owner/repo --id 123 --number 456
gitcrawl neighbors owner/repo --number 123 --limit 10
gitcrawl neighbors owner/repo --number https://github.com/owner/repo/pull/456 --limit 10
gitcrawl search owner/repo --query "download stalls"
gitcrawl search issues "download stalls" -R owner/repo --state open --json number,title,state,url,updatedAt,labels --limit 30
gitcrawl search prs "manifest cache" -R owner/repo --state open --json number,title,state,url,updatedAt,isDraft,author --limit 20
gitcrawl search issues "hot loop" -R owner/repo --state open --sync-if-stale 5m --json number,title,url
gitcrawl sync owner/repo --numbers 123 --with pr-details
gitcrawl gh search issues "download stalls" -R owner/repo --state open --match comments --json number,title,url
gitcrawl gh pr view 123 -R owner/repo --json number,title,state,url
gitcrawl gh pr view https://github.com/owner/repo/pull/123 --json number,title,state,url
gitcrawl gh pr checks https://github.com/owner/repo/pull/123 --json name,state,conclusion
gitcrawl gh run view 123456789 -R owner/repo --json status,conclusion
gitcrawl gh xcache stats
gitcrawl tui
gitcrawl tui owner/repo
```

`gitcrawl clusters` and `gitcrawl tui` match ghcrawl's display view: latest raw run clusters first, closed durable rows merged as historical context, sorted by size by default. Pass `--hide-closed` to focus only currently open clusters. `gitcrawl durable-clusters` stays on governed durable rows and needs `--include-closed` for inactive rows.
`gitcrawl metadata --json`, `gitcrawl status --json`, and `gitcrawl doctor --json` are crawlkit control surfaces for launchers, local automation, and CI checks. They are read-only and do not mutate archive data.
`gitcrawl cluster` and `gitcrawl refresh` build ghcrawl-shaped durable clusters by default (`--threshold 0.80`, `--min-size 1`, `--max-cluster-size 40`, `--k 16`, `--cross-kind-threshold 0.93`): every active vector-backed thread is represented, singleton rows use `singleton_orphan`, multi-member rows use `duplicate_candidate`, and stable IDs are derived from the representative thread. They also add deterministic GitHub reference evidence for direct issue/PR links such as `#123`, `issues/123`, and `pull/123`. Weak embedding edges need concrete title-token overlap unless their similarity is already high, which keeps generic low-confidence bridges from forming unrelated clusters.
`gitcrawl tui` infers the most recently updated local repository when `owner/repo` is omitted. `serve` is intentionally not part of `gitcrawl`.
`gitcrawl sync` fetches open issues and pull requests by default. Pass `--state all` or `--state closed` for explicit backfill workflows; incremental open syncs with `--since` also sweep recently closed items so local open state does not rot.
Pass `--numbers` to refresh exact issue or pull request rows without relying on list ordering or updated-time windows.
Thread-reference inputs accept bare numbers, `#123`, `issues/123`, `pull/123`, `owner/repo#123`, and full GitHub issue/PR URLs. This applies to sync filters, `--number` flags, governance member commands, neighbor/embed lookups, gh-shim `view`/`checks`/`diff`, and TUI jump input. For gh-shim view/checks/diff, a full GitHub URL also supplies the repository, so `-R owner/repo` can be omitted.
Pass `--with pr-details` or `--include-pr-details` to hydrate pull request files, commits, checks, and workflow runs for local review. The `gh` shim can also auto-hydrate one exact PR on a PR-detail miss, then retry locally.
`gitcrawl search issues|prs` accepts the common `gh search` shape (`<query> -R owner/repo --state open --json fields --limit N`) and answers from the local SQLite cache. It is intended for discovery without spending GitHub REST search quota; use `gh` for final live verification and GitHub write actions. Pass `--sync-if-stale 5m` to perform one metadata sync before the cached search when the local repository mirror is older than that duration.
`gitcrawl gh` is a gh-compatible shim for agent workflows. It answers broad `gh search issues|prs`, `gh issue/pr list`, supported `gh issue/pr view --json` fields, hydrated `gh pr checks`, and hydrated `gh run list/view` from local SQLite, then falls through to the real GitHub CLI for unsupported commands. Local `gh issue/pr list` supports common filters such as `--author`, `--assignee`, and repeated `--label`; empty open issue discovery falls through when the local repo only has targeted sync history. Read-only fallthroughs such as `gh pr diff`, `gh repo view/list`, `gh release list/view`, `gh workflow list/view`, `gh secret list`, `gh variable get/list`, `gh label list`, read-only `gh search` kinds, GET-only REST `gh api` calls, and read-only `gh api graphql` queries use a command-aware persistent cache under `cache/gh-shim`; Actions run/job logs get longer TTLs, completed run/job reads are kept much longer than active CI status, user profile reads get a 7-day TTL, read-only GraphQL gets a 6-hour TTL, and `gh pr diff` entries are keyed by the cached PR head SHA when available. Explicit API paths and explicit repositories share cache entries across sibling checkouts even when agents set different `GH_REPO` values; implicit repo reads stay isolated by `GH_REPO` or current working directory. Cache keys canonicalize common flags such as `-R`/`--repo` and sorted `--json` fields so equivalent agent commands coalesce. Repeat read failures are cached by default so agents do not rediscover the same missing release or workflow, but rate-limit error entries expire quickly; if GitHub rate-limits a refresh and an expired successful entry exists, the shim serves the stale response with a warning instead of failing the read. When another process is refreshing an expired successful entry, peers may serve stale inside a short grace window instead of joining the backend stampede. Set `GITCRAWL_GH_STALE_GRACE=0` to disable stale-while-revalidate, or `GITCRAWL_GH_CACHE_ERRORS=0` to disable error caching. Mutating commands pass through, increment write counters, and invalidate matching cache tags instead of flushing unrelated entries. `gh xcache stats|keys|gc|flush|reset|snapshot` inspects, garbage-collects, clears, resets, or snapshots fallthrough-cache counters, including hit rate plus per-command, per-route, per-key, and `--since` recent-window miss counters. Set `GITCRAWL_GH_PATH` to choose the backend `gh`, and symlink or install the binary as `gh`/`gitcrawl-gh` to run the shim directly.
The TUI starts at `--min-size 5` and `--sort size`, like ghcrawl's saved default, so the first screen is the useful cluster workload instead of singleton noise. Pass `--min-size 1` when you intentionally want singleton clusters. Mouse support is built in: click rows, wheel panes, and right-click for copy, sort, filter, jump, link, neighbor, local close/reopen, and member triage actions. Press `a` to open the same action menu from the keyboard, `#` to jump directly to an issue or PR number, `p` to switch between repositories already present in the local store, or `n` to load neighbors for the selected issue or PR. Enter from the members pane also loads neighbors before opening detail. The TUI quietly refreshes from the local store every 15 seconds.
`gitcrawl tui` remains the reference terminal interaction model for the crawl app family: pane focus, sortable headers, mouse/right-click actions, detail rendering, and status chrome are the behavior the shared `crawlkit/tui` browser is converging on for Slack, Discord, and Notion archives.

## Local Defaults

- config: `~/.config/gitcrawl/config.toml`
- database: `~/.config/gitcrawl/gitcrawl.db`
- cache: `~/.config/gitcrawl/cache`
- vectors: `~/.config/gitcrawl/vectors`
- logs: `~/.config/gitcrawl/logs`

## Requirements

- Go 1.26+
- a GitHub token for sync commands, either via `GITHUB_TOKEN` or `gh auth token`
- an OpenAI API key only for summary and embedding commands

## Install

Install from Homebrew:

```bash
brew install openclaw/tap/gitcrawl
```

Or download a release archive from GitHub releases or build from source:

```bash
git clone https://github.com/openclaw/gitcrawl.git
cd gitcrawl
go build -ldflags "-X github.com/openclaw/gitcrawl/internal/cli.version=$(git describe --tags --always --dirty)" -o bin/gitcrawl ./cmd/gitcrawl
./bin/gitcrawl --version
```

## Development

```bash
go test ./...
go build ./cmd/gitcrawl
go run ./cmd/gitcrawl help tui
```
