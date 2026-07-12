# Changelog

## 0.7.2 - Unreleased

- Order canonical thread state and each hydrated child family with durable pre-fetch generations, preventing delayed metadata, comments, PR details, checks, revisions, or fingerprints from overwriting newer snapshots while preserving legacy read-only archives.

## 0.7.1 - 2026-07-09

- Add local, fail-closed packaging and independent verification for official macOS archives signed as `org.openclaw.gitcrawl` by `Developer ID Application: OpenClaw Foundation (FWJYW4S8P8)`, while keeping CI and cross-platform snapshots credential-free and non-publishing.
- Require Go 1.26.5 for the standard-library TLS security fix and add a vulnerability scan to CI.
- Update CrawlKit to the signed v0.13.4 patch release.

## 0.7.0 - 2026-07-06

- Update CrawlKit to v0.13.3 for TOML recursion, panic, and deeply nested input hardening.
- Resolve relative `gitcrawl init` database, runtime, and portable-store filesystem paths before saving them so later SQLite commands open the intended files instead of failing on an invalid URI, and reject unsupported SQLite URI or in-memory `--db` values.
- Add `gitcrawl doctor --locks --json` for read-only SQLite health, sidecar state, and best-effort writer-process diagnostics, keeping archive health distinct from detected activity. Thanks @TurboTheTurtle.
- Add read-only per-repository archive coverage reporting with JSON/table output, repository and missing-PR-detail filters, hydration counts, and explicit failure-ledger availability. Thanks @TurboTheTurtle.
- Report the active executable/build identity and read-only database schema compatibility in `gitcrawl doctor --json`, including actionable drift diagnostics without applying migrations. Thanks @TurboTheTurtle.
- Document the first-run local maintainer archive workflow, including targeted PR hydration, bounded-staleness search, run inspection, and the Octopool boundary. Thanks @TurboTheTurtle.

## 0.6.4 - 2026-06-23

- Drain large Cloudflare D1 table resets before sending row batches so `gitcrawl cloud publish` can tolerate bounded Worker resets.

## 0.6.3 - 2026-06-23

- Compact portable thread label and assignee metadata during store pruning so `gitcrawl-store` refreshes keep GitHub archive payloads below GitHub blob limits.

## 0.6.2 - 2026-06-19

- Use CrawlKit's shared safe tokenized FTS5 query and SQL LIKE builders, treating `%`, `_`, and backslashes literally in fallback searches, and update CrawlKit to v0.13.0.
- Preserve duplicate PR file entries returned by GitHub, including removed and added files sharing one path, as position-keyed snapshots. Thanks @joshka.
- Add `gitcrawl init --runtime-dir` for fully isolated temporary database, cache, vector, and log paths. Thanks @joshka.

## 0.6.1 - 2026-06-19

- Use platform-native default config, data, cache, and log paths for new installs while preserving existing `~/.config/gitcrawl` installs until the new platform path exists; macOS uses Application Support and Caches, while Linux continues to honor XDG base directory variables, thanks @joshka.
- Update `crawlkit` to v0.12.2 for Windows SQLite path support and security hardening; bearer-authenticated non-local remote endpoints now require HTTPS, while loopback HTTP remains supported for local development.

## 0.6.0 - 2026-06-11

- Update `crawlkit` to v0.12.0.
- Add configurable `vector_backend` selection for semantic search and neighbors, including optional Python `turbovec` support when embedding dimensions are divisible by 8, thanks @vincentkoc.
- Mirror the local SQLite archive into the Worker-backed R2 object store during `gitcrawl cloud publish`, then compress it as a gzip chunk bundle with a manifest so fallback/bootstrap data is smaller and can grow past single-object upload limits.
- Bound portable-store Git subprocess cancellation so stale fetch helpers cannot hang read-only commands, and expose the last portable refresh failure in `gitcrawl doctor --json`.
- Backfill a missing or stale source embedding for `gitcrawl neighbors` with a one-row capped embedding request, and show the exact cheap `gitcrawl embed --number ... --limit 1` recovery command when no OpenAI key is configured.
- Add PR-aware thread indexing for changed paths and commit subjects, plus `gitcrawl code index` and `search --scope code|all` for bounded local full-text search over tracked monorepo source files.

## 0.5.0 - 2026-05-27

- Add Cloudflare remote archive scaffolding with `[remote]` config, `gitcrawl init --remote`, GitHub-backed `remote login` with OAuth or token-env bootstrap, remote identity/archive/status commands, cloud-mode search against Worker named queries, and `gitcrawl cloud publish` ingestion without creating a local SQLite database for readers.
- Add `gh` web fallback reads and move the `gitcrawl gh` compatibility cache to Octopool, with a migration error that points users at `octopool login` and `octopool gh ...`.
- Add an optional TUI focus layout, configurable with `gitcrawl tui --layout focus`, `tui.default_layout`, or `GITCRAWL_TUI_LAYOUT`, thanks @RomneyDa.
- Add `gitcrawl clusters-report` for Markdown or JSON cluster triage reports, and restore TUI detail-pane keyboard and wheel scrolling, thanks @RomneyDa.

## 0.4.5 - 2026-05-23

- Improve gh-shim cache hit rates for API projections, explicit `--cache` reads, broad run-list fallbacks, stable dated searches, `pr checks --watch=false`, common local PR view fields, enriched `pr list --json` fields, and cached pull-request API reads.

## 0.4.4 - 2026-05-23

- Keep the highlighted member selected in the TUI when the cluster list auto-refreshes, is manually refreshed, or is re-sorted/filtered, instead of silently snapping the selection back to the first row.
- Make the TUI detail pane scrollable again with the keyboard, mouse wheel, and trackpad; its viewport content was only set on the render copy, so the live pane never had anything to scroll.

## 0.4.3 - 2026-05-22

- Update `crawlkit` to v0.8.0.
- Retry transient SQLite busy/locked cache writes during sync and keep gh-shim auto-hydration quiet so live fallback reads do not surface scary local cache lock noise.

## 0.4.1 - 2026-05-18

- Add cached release checks with `gitcrawl check-update` and passive terminal
  notices when a newer OpenClaw release is available.

## 0.4.0 - 2026-05-17

- Harden portable-store publishing and reads with manifest integrity checks, temp-DB validation before runtime replacement, stale Git lock cleanup, reclone fallback, and richer `doctor` DB health output.
- Repair malformed portable-store SQLite caches by preserving the bad DB, resetting/pulling the store checkout, and recopying a healthy runtime mirror before local reads continue.
- Replay GitHub request bodies on rate-limit retries so GraphQL POST retries keep their JSON payload.
- Send GitHub Enterprise GraphQL requests to `/api/graphql` when the REST API base URL ends in `/api/v3`.
- Reject durable cluster saves with empty member lists instead of leaving stale active memberships.
- Preserve multiple thread embeddings for the same thread when basis or model differs.
- Preserve OpenAI retry backoff defaults when callers provide partial retry overrides.
- Preserve existing comment text in search documents during metadata-only syncs.
- Keep comment and PR-detail sync writes atomic when GitHub hydration fails, avoiding partial cache rows without a run record.
- Fail PR-detail syncs when GitHub review-thread hydration fails instead of recording a successful partial refresh.
- Consume the shared `openclaw/crawlkit` vector and portable-mirror helpers from gitcrawl.
- Route OpenAI embedding requests through the shared `openclaw/crawlkit/embed` provider while preserving gitcrawl retry and error handling.
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
