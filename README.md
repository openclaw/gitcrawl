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
gitcrawl init --remote https://crawl.openclaw.ai --archive gitcrawl/openclaw__openclaw
gitcrawl remote login --endpoint https://crawl.openclaw.ai --json
gitcrawl remote status --json
gitcrawl remote archives --json
gitcrawl whoami --json
gitcrawl cloud publish --remote https://crawl.openclaw.ai --archive gitcrawl/openclaw__openclaw --json
gitcrawl sync owner/repo
gitcrawl sync owner/repo --state open
gitcrawl sync owner/repo --numbers 123,456 --include-comments
gitcrawl sync owner/repo --numbers https://github.com/owner/repo/issues/123 --with pr-details
gitcrawl refresh owner/repo
gitcrawl cluster owner/repo --threshold 0.80
gitcrawl clusters owner/repo
gitcrawl clusters-report owner/repo --limit 10 --min-size 5
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
octopool login
octopool gh api repos/openclaw/openclaw/pulls/123 --jq .number
gitcrawl tui
gitcrawl tui owner/repo
```

`gitcrawl clusters` and `gitcrawl tui` match ghcrawl's display view: latest raw run clusters first, closed durable rows merged as historical context, sorted by size by default. Pass `--hide-closed` to focus only currently open clusters. `gitcrawl durable-clusters` stays on governed durable rows and needs `--include-closed` for inactive rows.
`gitcrawl metadata --json`, `gitcrawl status --json`, and `gitcrawl doctor --json` are crawlkit control surfaces for launchers, local automation, and CI checks. They are read-only and do not mutate archive data.
`gitcrawl init --remote ... --archive ...` configures a Worker-fronted cloud archive. In `cloud` mode, supported read commands such as `status` and `search owner/repo --query ...` call the remote archive and do not create a local SQLite database. Existing local and Git-backed portable-store workflows remain unchanged.
The remote service is deployed separately from gitcrawl in `openclaw/crawl-remote` with Wrangler. gitcrawl only stores the Worker endpoint/archive in config and calls that service.
`gitcrawl remote login` starts the Worker GitHub OAuth flow, verifies org/team membership server-side, and stores the signed bearer token in the OS keyring.
Use `gitcrawl remote login --github-token-env GITHUB_TOKEN` for non-browser bootstrap; the Worker verifies that GitHub token against the same org/team policy and stores only the returned remote session token locally.
`gitcrawl cloud publish` sends the local SQLite repositories and thread rows to a Worker archive through the role-gated ingest endpoint.
`gitcrawl clusters-report` writes a Markdown report for the top clusters using the same display view, with an at-a-glance table, per-cluster metadata, member tables, and key snippets. Use `--json` for the hydrated report payload.
`gitcrawl cluster` and `gitcrawl refresh` build ghcrawl-shaped durable clusters by default (`--threshold 0.80`, `--min-size 1`, `--max-cluster-size 40`, `--k 16`, `--cross-kind-threshold 0.93`): every active vector-backed thread is represented, singleton rows use `singleton_orphan`, multi-member rows use `duplicate_candidate`, and stable IDs are derived from the representative thread. They also add deterministic GitHub reference evidence for direct issue/PR links such as `#123`, `issues/123`, and `pull/123`. Weak embedding edges need concrete title-token overlap unless their similarity is already high, which keeps generic low-confidence bridges from forming unrelated clusters.
`gitcrawl tui` infers the most recently updated local repository when `owner/repo` is omitted. `serve` is intentionally not part of `gitcrawl`.
`gitcrawl sync` fetches open issues and pull requests by default. Pass `--state all` or `--state closed` for explicit backfill workflows; incremental open syncs with `--since` also sweep recently closed items so local open state does not rot.
Pass `--numbers` to refresh exact issue or pull request rows without relying on list ordering or updated-time windows.
Thread-reference inputs accept bare numbers, `#123`, `issues/123`, `pull/123`, `owner/repo#123`, and full GitHub issue/PR URLs. This applies to sync filters, `--number` flags, governance member commands, neighbor/embed lookups, and TUI jump input.
Pass `--with pr-details` or `--include-pr-details` to hydrate pull request files, commits, checks, workflow runs, and review-thread state for local review.
`gitcrawl search issues|prs` accepts the common `gh search` shape (`<query> -R owner/repo --state open --json fields --limit N`) and answers from the local SQLite cache. It is intended for discovery without spending GitHub REST search quota; use `gh` for final live verification and GitHub write actions. Pass `--sync-if-stale 5m` to perform one metadata sync before the cached search when the local repository mirror is older than that duration.
`gitcrawl gh` moved to Octopool. Run `octopool login`, then use `octopool gh ...` or symlink Octopool as `gh` for the shared org cache and pooled GitHub relay.
The TUI starts at `--min-size 5` and `--sort size`, like ghcrawl's saved default, so the first screen is the useful cluster workload instead of singleton noise. Pass `--min-size 1` when you intentionally want singleton clusters, or `--layout focus` when you want more readable detail text. Mouse support is built in: click rows, wheel panes, and right-click for copy, sort, filter, jump, link, neighbor, local close/reopen, and member triage actions. Press `a` to open the same action menu from the keyboard, `#` to jump directly to an issue or PR number, `p` to switch between repositories already present in the local store, or `n` to load neighbors for the selected issue or PR. Enter from the members pane also loads neighbors before opening detail. The TUI quietly refreshes from the local store every 15 seconds.
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

Check for newer releases manually with:

```bash
gitcrawl check-update
```

Interactive terminal runs also perform a cached daily release check and print a
stderr notice when a newer OpenClaw release is available. Set
`GITCRAWL_NO_UPDATE_CHECK=1` or `CRAWLKIT_NO_UPDATE_CHECK=1` to disable that
passive notice.

Docker:

```bash
docker build -t gitcrawl .
docker run --rm -e GITHUB_TOKEN -v "$PWD/.gitcrawl:/data" gitcrawl sync owner/repo
docker run --rm -v "$PWD/.gitcrawl:/data" gitcrawl search issues "hot loop" -R owner/repo
```

The image stores config, SQLite data, cache, and Git snapshot state under `/data`.

## Development

```bash
go test ./...
go build ./cmd/gitcrawl
go run ./cmd/gitcrawl help tui
```
