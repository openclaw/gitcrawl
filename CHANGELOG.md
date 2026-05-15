# Changelog

## 0.3.5 - Unreleased

- Preserve OpenAI retry backoff defaults when callers provide partial retry overrides.
- Preserve existing comment text in search documents during metadata-only syncs.
- Fail PR-detail syncs when GitHub review-thread hydration fails instead of recording a successful partial refresh.
- Fetch all paginated GitHub review-thread comments instead of keeping only the first review-thread comment page.
- Keep `gh xcache gc` from expiring stable PR diff cache entries with the short fallback TTL while the PR head SHA is unchanged.
- Fall back or fail for unsupported local `gh pr checks` and `gh run` JSON fields instead of silently omitting them.
- Refuse to use the running `gitcrawl` executable as the real `gh` backend, including hard-linked shim paths, to avoid recursive gh-shim fallthrough.
- Report duplicate OpenAI embedding response indexes explicitly instead of letting a later row overwrite an earlier vector.
- Keep cosine similarity stable for very large finite vectors instead of dropping them after float overflow.
- Allow cluster detail reads to target raw-run or durable-cluster IDs explicitly, avoiding collisions between the two ID namespaces.
- Keep active durable cluster representatives on visible open members instead of closed or hidden historical members.
- Avoid holding SQLite write transactions open while hydrating PR details from GitHub.
- Skip PR check-run and workflow-run hydration when GitHub returns no PR head SHA, avoiding broad workflow-run fetches.
- Ignore cluster graph edges whose endpoints are absent from the visible node set, preventing hidden nodes from merging otherwise separate clusters.
- Make direct `gitcrawl search --mode semantic` use query embeddings and `--mode hybrid` combine semantic and keyword hits instead of relabeling keyword-only search.
- Remove the search-only `--sync-if-stale` flag from `gitcrawl refresh` help text.
- Ignore cross-repository `owner/repo#number` references when building deterministic cluster edges for the current repository.
- Reject non-finite CLI float options such as `NaN` before commands can mutate local cluster state.
- Fetch all paginated GitHub check runs and workflow runs instead of only the first 100 rows.
- Fix GitHub Enterprise pagination when API `Link` headers include the `/api/v3` base path, avoiding duplicated paths on follow-up pages.
- Retire durable clusters that disappear from a successful clustering run, while still preserving local close overrides across reclustering.
- Derive the default vector directory from custom database paths, including `GITCRAWL_DB_PATH`, so separate stores do not share embeddings unless `vector_dir` is set explicitly.
- Refuse to refresh a portable store checkout when its Git remote does not match the requested portable store, avoiding accidental resets of unrelated working trees.
- Ignore non-finite vector similarity scores so malformed embeddings cannot surface as neighbors.

## 0.3.4 - 2026-05-14

- Docker: add a local image with `/data` persistence and CI smoke coverage.
- Make the `gh` shim force `GET` for GitHub Search API field calls so `gh api search/* -f q=...` agent invocations do not fall through as `POST`.

## 0.3.3 - 2026-05-11

- Add cache-backed `gh pr status` readiness summaries with compact JSON, agent-oriented exit codes, and exact PR hydration that stores GitHub review threads instead of relying only on flattened review comments.
- Make gh-shim Actions/release reads liveness-aware: broad `gh run list` now falls through to live GitHub unless it is pinned to a commit or cached PR branch, cached CI/release reads print a stderr provenance note, and `--live` bypasses shim/cache state.
- Record short-lived liveness tombstones after mutating `gh run`, `gh workflow`, `gh release`, and matching `gh api` calls so immediate status/release checks bypass stale fallthrough cache entries.
- Expose shim/backend paths, live mode, liveness tombstones, and live bypass counters in `gh xcache stats`.

## 0.3.2 - 2026-05-10

- Move top-level CLI parsing and `gh xcache` argument parsing onto Kong while keeping the broader `gh` shim pass-through compatible with GitHub CLI argument shapes.
- Keep `gh xcache --help` discoverable and make `stats --since`, JSON output, and snapshot reset parsing share one typed parser path.
- Teach the `gh` shim about the shared GitHub token rate-limit budget, serve stale successful reads more aggressively when that pooled budget is low, preserve GitHub CLI `--jq` handling for cached fallthrough reads, and expose low-budget stale hits in `xcache stats`.
- Avoid extra `gh auth token` subprocesses during low-budget cache preflight checks.

## 0.3.1 - 2026-05-08

- Fix gh-shim portable-store auto-hydration so exact issue/PR refreshes write to the runtime mirror instead of dirtying the Git checkout, clear stale portable refresh locks, and make empty open issue discovery fall through when only targeted sync history exists.
- Keep `cluster-detail` aligned with the default cluster list by showing closed historical members unless `--hide-closed` is passed, and fail fast when `GITCRAWL_GH_PATH` points back at the `gitcrawl` shim.

## 0.3.0 - 2026-05-08

- Bump routine release workflow dependencies.
- Add a repo-local `gitcrawl` agent skill for local archive, freshness, gh-shim, cluster, and verification workflows.
- Accept full GitHub issue and pull request URLs anywhere `gitcrawl` expects a thread number, including sync filters, gh-shim views/diffs, governance commands, neighbor lookup, embedding, and TUI jumps.
- Document read-only SQLite query examples in the repo-local agent skill so agents can do exact local archive counts without mutating state.
- Document the crawlkit control surface now available on `main`, including `metadata --json`, `status --json`, and `doctor --json` for local launchers and CI.
- Clarify that `gitcrawl tui` remains the reference terminal browser for the crawl app family while shared `crawlkit/tui` converges on the same panes, sorting, action menus, and status chrome.
- Add command-reference coverage for the read-only metadata/status commands.
- Add broader CLI, gh-shim, TUI, and store regression coverage for the verified release surface.

## 0.2.1 - 2026-05-05

- Improve `gh` shim cache coordination and observability with stale-while-revalidate reads, finer Actions/API TTLs, recent-window stats, top miss keys, and `xcache snapshot`.

## 0.2.0 - 2026-05-05

- Add Homebrew tap installation via `brew install openclaw/tap/gitcrawl`.
- Improve the `gh` shim cache with canonicalized keys, targeted mutation invalidation, stale-on-rate-limit fallback reads, completed-run TTLs, hit-rate stats, counter reset, and issue auto-hydration.
- Add dark-mode support, a theme toggle, and clearer navigation styling to the generated docs site.
- Force embedding refreshes when the embedding input rune cap changes, so stale larger-cap vectors are not reused.
- Expand the `gh` shim with local list filters, PR diff caching by cached head SHA, xcache GC, hit/miss/write counters, and throttled portable-store refreshes to reduce GitHub API pressure across agent sessions.
- Add explicit PR-detail hydration for files, commits, checks, and workflow runs so `gh pr view`, `gh pr checks`, and `gh run list/view` can answer common review reads from the existing SQLite cache.
- Auto-hydrate one exact pull request when local PR detail reads miss or check/run data is stale, using `gh auth token` if `GITHUB_TOKEN` is absent, then retry from SQLite before falling back to live `gh`.
- Cache more ghx-style read-only fallthroughs, including release, workflow, secret, variable, project, ruleset, gist, org, and search reads; cache repeat read failures by default; and clear the fallthrough cache after the corresponding mutating `gh` commands.
- Promote portable backups to the v2 format: keep compact comments, PR files, commits, checks, and workflow runs while stripping raw JSON, generated documents, vectors, clusters, and run history.
- Add crawlkit control metadata/status surfaces with command-local `metadata --json`, `status --json`, and `doctor --json`.
- Include the primary SQLite database inventory in status JSON so local control surfaces can discover archive storage without opening live stores.
- Route config path handling and SQLite openers through `crawlkit` so GitHub archive tooling shares the same foundation as the Slack, Discord, and Notion crawlers.
- Keep shared crawl app TUI nomenclature aligned while `gitcrawl tui` remains the richer cluster-browser reference implementation.
- Keep the existing `gitcrawl tui` as the family reference terminal interface and add CI smoke coverage for its help surface.

## 0.1.2 - 2026-05-01

- Polish the TUI cluster browser interaction model, including separate cluster/member action menus, softer row state colors, stable viewport refresh, bidirectional age sorting, and buffered trackpad scrolling.
- Add OpenAI embedding retry handling for transient failures and cap oversized embedding inputs before sending them upstream.
- Improve GitHub pagination and retry behavior by surfacing page totals and honoring retry and rate-limit response headers.
- Harden human-key hash parsing and tidy the module graph.

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
