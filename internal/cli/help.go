package cli

import (
	"fmt"
)

func (a *App) printUsage() {
	fmt.Fprint(a.Stdout, usageText)
}

func (a *App) printCommandUsage(command string) error {
	if text, ok := commandUsageTexts[command]; ok {
		fmt.Fprint(a.Stdout, text)
		return nil
	}
	switch command {
	case "cluster-explain":
		fmt.Fprint(a.Stdout, commandUsageTexts["cluster-detail"])
		return nil
	case "portable":
		fmt.Fprint(a.Stdout, portableUsageText)
		return nil
	case "tui":
		fmt.Fprint(a.Stdout, tuiUsageText)
		return nil
	default:
		return usageErr(fmt.Errorf("unknown help topic %q", command))
	}
}

const usageText = `gitcrawl mirrors GitHub issues and pull requests into local SQLite for maintainer triage.

Usage:
  gitcrawl [global flags] <command> [command flags]
  gitcrawl help <command>

Global flags:
  --github-token-command <path>  absolute GitHub credential executable (Unix only)
  --config <path>       config path
  --format <mode>      output format: text|json|log
  --json               write JSON output
  --version            print version

Core commands:
  metadata             print crawlkit control metadata
  check-update         check for a newer gitcrawl release
  status               print fast read-only archive status
  metrics              collect, import, or inspect an independent metrics database
  remote status        print remote archive status
  remote archives      list remote archives visible to the current identity
  remote login         authenticate with GitHub org access for a remote archive
  whoami               print remote archive identity
  cloud publish        publish the local archive to a Worker remote
  init                 create config, optionally from a portable store
  doctor               check config, token, and database readiness
  sync                 sync GitHub issue and pull request metadata
  acquire-api          acquire full conversations through the PostgreSQL API
  mint-acquisition-token mint scoped public-repository App credentials (secret JSON)
  discover-parents     export missing parent identities through Octopool
  analytics            repair publication dates, enrich identities, or watch GraphQL updates
  sync-failures        list failed sync hydration attempts
  coverage             report local archive PR-detail completeness
  fill-pr-details      hydrate locally missing pull request detail rows
  refresh              run sync, enrichment, embedding, and clustering pipeline
  summarize            generate key summaries for current thread revisions
  embed                generate OpenAI embeddings for local thread documents
  threads              list local issue and pull request rows
  capture              export a stable code-free conversation snapshot
  delivery             explicitly manage a private report outbox
  code index           index tracked text files from a local Git checkout
  cluster              build durable clusters from local thread vectors
  purge-threads        permanently remove selected local threads and exclude collection
  change-contract      report or manage the archive's reader change contract
  close-thread         locally hide one issue or pull request row
  reopen-thread        clear a local hide for one issue or pull request row
  close-cluster        locally hide one durable cluster
  reopen-cluster       clear a local hide for one durable cluster
  exclude-cluster-member
                       locally remove one row from a durable cluster
  include-cluster-member
                       restore one row to a durable cluster
  set-cluster-canonical
                       set the canonical row for a durable cluster
  clusters             list latest run cluster summaries, with durable fallback
  clusters-report      write a Markdown report for the top clusters
  durable-clusters     list durable cluster groups
  cluster-detail       dump one latest run cluster, with durable fallback
  cluster-explain      alias for cluster-detail
  cluster-candidates   read run-scoped computed candidate generations
  neighbors            list vector-nearest local issue and pull request rows
  search               search local thread and source documents; also supports search issues|prs gh syntax
  gh                   moved to Octopool; prints migration note
  portable prune       prune volatile payloads from a portable store
  portable export      create an immutable derived portable generation
  portable refresh     safely refresh a configured portable subscriber
  tui [owner/repo]     browse clusters in the terminal UI; repo is inferred when omitted

No API server is provided. There is intentionally no serve command.
`

var commandUsageTexts = map[string]string{
	"discover-parents": `gitcrawl discover-parents exports selected REST backing Issue IDs through Octopool.

Usage:
  gitcrawl discover-parents --repo owner/repo --numbers-file PATH [--cursor CURSOR] [--max-pages 20] [--timeout 10m] [--json]
  gitcrawl discover-parents --repo owner/repo --updated-since RFC3339 [--cursor CURSOR] [--page-size 100] [--max-pages 20] [--json]

Uses OCTOPOOL_TOKEN and optional OCTOPOOL_URL/OCTOPOOL_POOL; no personal GitHub fallback or SQLite access.
Output contains selection (input for acquire-api --parent-selection-file), nextCursor and page counts.
numbers-file adds numbersSHA256; updated-since adds updatedSince and observedAt, and includes old threads updated later.
Resume with the same selection or lower bound, page size and nextCursor. Persist each successful output before continuing.
Updated inventory is mutable, not a historical snapshot. Deduplicate identities across outputs; provider bodies are not exported.
The final empty nextCursor means enumeration finished, not that every requested number exists or that report coverage is complete.
Once repository binding succeeds, a provider error emits completed pages and exits nonzero; resume its nextCursor.
Validation, credentials or repository bootstrap failures emit no discovery receipt; preserve the preceding cursor.
`,
	"mint-acquisition-token": `gitcrawl mint-acquisition-token issues one-repository, read-only GitHub App installation tokens.

Usage:
  gitcrawl mint-acquisition-token --app-id ID --installation-id ID --repo owner/repo --repository-id ID --private-key-file PATH
  gitcrawl mint-acquisition-token --app-id ID --installation-id ID --cohort-stdin --private-key-file PATH

Cohort stdin is one JSON array of approved {"id":123,"name":"owner/repo"} entries
for a single organization. It outputs a JSON array of separate scoped credentials.

Run as root with an absolute, root-owned 0400 or 0600 RSA key file. Stdout is credential JSON:
consume it through a private supervisor pipe; never send it to a terminal, log or report.
No config, personal login, refresh daemon or fallback credential is used.
`,
	"acquire-api": `gitcrawl acquire-api persists scoped GitHub conversations through the PostgreSQL API.

Usage:
  gitcrawl acquire-api --endpoint URL --source ID --repo owner/repo --publisher-token-env ENV --reader-token-env ENV --since RFC3339 [--numbers-file PATH] [--max-threads 100] [--timeout 10m] [--json]

Provider authentication defaults to Octopool. With a rooted mint supervisor,
use --github-app-token-stdin for one trusted mint-acquisition-token JSON envelope
on a private pipe, or --github-app-token-env ENV for a named environment
variable. Use either option without OCTOPOOL_* configuration. The short-lived
token must outlast --timeout by 60 seconds.
The collector never reads the App key or refreshes credentials. Direct App reads
reserve half each observed REST/GraphQL budget and refuse private-* repositories.

The first incremental run requires --since. Later runs use the server horizon;
active runs always resume their original scope and checkpoint. Historical
selection requires an established incremental horizon and its unchanged file.
Use --parent-selection-file PATH for audited parent-only identities; this skips
children and keeps the existing horizon. --parent-page-size is capped at 50 and
may be reduced to resume after a response-size failure.
Use --commits --since RFC3339 for a pinned default-branch commit scan with complete
associated-PR attribution. --commit-max-pages (default 100) bounds each invocation.
This requires an established acquisition horizon and does not advance it.
Add --commits-until RFC3339 for an exclusive historical upper bound at or
before that horizon. Finish the backfill with the same bound before resuming
the scheduled cycle.

Use --lifecycle-numbers-file PATH for 1..500 existing PG parents and --since
for the event lower bound. This follows closure/reopen/merge timelines only; it
keeps the completed horizon and does not claim repository-wide coverage.
--lifecycle-max-pages (default 100) bounds each resumable invocation.
Use --cycle to run one admitted reporting turn. The server's active run resumes
first. Completed turns rotate incremental → commits → lifecycle, then historical
if --numbers-file is supplied. Keep that file for retained historical progress.
Commit and lifecycle windows come from the completed incremental turn. An
unfinished turn reports deferredModes. Use --cycle-turns 2..8 to reuse the same
provider session for several turns. The first unfinished turn ends the batch;
JSON for a multi-turn invocation contains a turns array. Thread and page limits
apply to each turn. The durable run can span multiple invocations.
Use --cycle-one-window with --cycle-turns 2..8 to yield before the next
incremental turn once this batch has done work for the current window.
Lifecycle selects changed PostgreSQL parents across bounded batches at the completed
commit pin. --lifecycle-reader-endpoint optionally sends only that candidate query
to a separate reader for the same source. If it requires its own reader credential,
set --lifecycle-reader-token-env NAME. An explicit --lifecycle-numbers-file
restricts the cycle to its fixed cohort of at most 500 parents.
--commit-timeout (default 10m) bounds each run or page commit request, because a
final page waits for server-side publication checks; other API calls keep 30s.
--timeout still bounds the whole invocation.
No SQLite archive is opened and no direct GitHub credential fallback exists.
`,
	"metrics": `gitcrawl metrics collects repository headline metrics in a separate SQLite database.

Usage:
  gitcrawl metrics collect --config /absolute/metrics.json [--json]
  gitcrawl metrics import --config /absolute/metrics.json [--json] < history.ndjson
  gitcrawl metrics status --config /absolute/metrics.json [--json]

The JSON config requires database (absolute path) and targets [{entity,target}].
Optional tokenEnv overrides GITHUB_TOKEN; native gh auth and --github-token-command
are supported. No archive, portable-store, embedding, or model operations occur.
Import validates target scope and IDs atomically. Unknown values remain SQL NULL.
Status is read-only. Clone traffic 403/404 is optional unavailability.
`,
	"metadata": `gitcrawl metadata prints crawlkit control metadata.

Usage:
  gitcrawl metadata [--json]
`,
	"status": `gitcrawl status prints fast read-only archive status.

Usage:
  gitcrawl status [--json]
`,
	"remote": `gitcrawl remote queries a configured Cloudflare-backed archive.

Usage:
  gitcrawl remote status [--json]
  gitcrawl remote archives [--json]
  gitcrawl remote login --endpoint URL [--json]
  gitcrawl remote login --endpoint URL --github-token-env GITHUB_TOKEN [--json]
  gitcrawl remote login --endpoint URL --access-token-env CRAWL_ACCESS_TOKEN [--json]
  gitcrawl remote whoami [--json]
`,
	"cloud": `gitcrawl cloud manages Worker-backed remote archives.

Usage:
  gitcrawl cloud publish --remote URL --archive id [--allow-incomplete | --admission-policy=archive-v1] [--observation-order] [--stage-only] [--json]
`,
	"whoami": `gitcrawl whoami prints the configured remote archive identity.

Usage:
  gitcrawl whoami [--json]
`,
	"check-update": `gitcrawl check-update checks GitHub Releases for a newer gitcrawl build.

Usage:
  gitcrawl check-update [--json] [--force]
`,
	"init": `gitcrawl init creates a local, portable, or cloud archive config.

Usage:
  gitcrawl init [--db path | --runtime-dir path | --portable-store URL | --remote URL --archive id] [--json]
`,
	"configure": `gitcrawl configure updates model fields in the config.

Usage:
  gitcrawl configure [--summary-model name] [--embed-model name] [--embed-base-url url] [--embedding-basis title_original] [--json]
`,
	"doctor": `gitcrawl doctor checks config, token, and database readiness.

Usage:
  gitcrawl doctor [--json] [--locks]
`,
	"sync": `gitcrawl sync mirrors GitHub issue and pull request metadata.

Usage:
  gitcrawl sync owner/repo [--state open|closed|all] [--numbers refs] [--with pr-metadata|pr-details] [--include-comments] [--include-pr-details] [--graphql-history] [--octopool] [--incremental] [--github-app-token-env NAME] [--timeout 15m] [--force] [--json]

pr-metadata fetches only the pull request object; pr-details also hydrates files,
commits, checks, workflows, and review threads. Comments are selected separately.
Unchanged issue comments are reused; --force downloads them again. PR reviews
and PR details are always fetched when selected.
--graphql-history batches exact numbers through GraphQL with no REST fallback.
It requires --state all --include-comments --with pr-metadata, and does not
support --since, --limit, or full pr-details hydration.
--github-app-token-env uses trusted mint-acquisition-token JSON for a bounded
public-repository REST seed, preserving half of observed REST and GraphQL quota.
It supports parents, comments and pr-metadata, without personal auth fallback.
It rejects Octopool, token commands, custom API origins, GraphQL history and
full pr-details. --timeout applies only to this mode (default 15m, maximum 1h);
the token must remain valid beyond the entire invocation. Completed native rows
survive a quota stop; replay the explicit selection with a fresh scoped token.
--octopool routes REST metadata and comments through Octopool using OCTOPOOL_TOKEN,
OCTOPOOL_URL and OCTOPOOL_POOL. It never falls back to direct GitHub requests and
does not support GraphQL history, PR enrichment or --github-token-command.
--incremental requires --octopool --state all --include-comments, and rejects
--since, --numbers and --limit. It resumes the qualified native checkpoint with
one minute of overlap. Missing/legacy checkpoints refuse acquisition: first run
an explicit --since bootstrap with that same profile. Only full successful
pagination/hydration advances through the run's start; initial coverage_since
is retained and does not claim earlier history. No source rearm or rebuild runs.
`,
	"sync-failures": `gitcrawl sync-failures lists failed sync hydration attempts.

Usage:
  gitcrawl sync-failures owner/repo [--include-resolved] [--limit N] [--json]
`,
	"coverage": `gitcrawl coverage reports local archive completeness by repository.

Usage:
  gitcrawl coverage [owner/repo | --repos owner/a,owner/b] [--min-missing-pr-details N] [--json]
`,
	"fill-pr-details": `gitcrawl fill-pr-details hydrates locally missing pull request detail rows.

Usage:
  gitcrawl fill-pr-details owner/repo [--limit N] [--order newest-first|oldest-first|open-first] [--batch-size N] [--reserve-rate-limit N] [--include-comments] [--json-progress] [--json]

Before each GitHub request issued by this command, Gitcrawl refreshes GitHub's
/rate_limit view of the shared token and stops if that observed quota would
cross the reserve. The default floor is 1500 remaining requests, providing
headroom for concurrent consumers. This is best-effort: another process can
spend quota between the probe and request. Pass --reserve-rate-limit N to
choose a different floor.
`,
	"refresh": `gitcrawl refresh runs sync, enrichment, embedding, and clustering.

Usage:
  gitcrawl refresh owner/repo [--state open|closed|all] [--with pr-metadata|pr-details] [--include-pr-details] [--force] [--no-sync] [--no-embed] [--no-cluster] [--strict-vectors] [--json]

--force bypasses unchanged issue-comment reuse during sync.
`,
	"summarize": `gitcrawl summarize generates key summaries for current thread revisions.

Usage:
  gitcrawl summarize owner/repo [--number ref] [--limit N] [--force] [--include-closed] [--json]
`,
	"embed": `gitcrawl embed generates OpenAI embeddings for local thread documents.

Usage:
  gitcrawl embed owner/repo [--number ref] [--limit N] [--force] [--include-closed] [--json]
`,
	"threads": `gitcrawl threads lists local issue and pull request rows.

Usage:
  gitcrawl threads owner/repo [--include-closed] [--numbers refs] [--limit N] [--json]
`,
	"delivery": `gitcrawl delivery explicitly manages native report and cluster custody.

Usage:
  gitcrawl delivery export-clusters --db PATH --repo OWNER/REPO --archive ID --binding ID --seed-id HEX32 --captured-at RFC3339Nano --max-rows N --max-native-bytes N --max-encoded-bytes N --timeout D
  gitcrawl delivery stage-clusters --db PATH --bundle PATH --repo OWNER/REPO --outbox NEW_PATH --origin URL --archive ID --binding ID --max-rows N --max-encoded-bytes N --timeout D [--json]
  gitcrawl delivery cluster-status --outbox PATH --timeout D [--json]
  gitcrawl delivery deliver-clusters --db PATH --outbox PATH --max-parts N --max-rows N --max-encoded-bytes N --timeout D --token-env NAME [--json]
  gitcrawl delivery measure-snapshot --db PATH --repo OWNER/REPO --sha256 HEX64 --timeout D [--json]
  gitcrawl delivery export-snapshot --db PATH --repo OWNER/REPO --sha256 HEX64 --directory NEW_PATH --archive ID --binding ID --seed HEX32 --max-rows N --max-native-bytes N --max-encoded-bytes N --timeout D [--json]
  gitcrawl delivery measure-seed --db PATH --repo OWNER/REPO --timeout D [--json]
  gitcrawl delivery stage-seed --db PATH --repo OWNER/REPO --outbox NEW_PATH --origin URL --archive ID --binding ID --max-rows N --max-native-bytes N --max-encoded-bytes N --timeout D [--json]
  gitcrawl delivery status --outbox PATH --timeout D [--json]
  gitcrawl delivery deliver --db PATH --outbox PATH --max-parts N --max-rows N --max-encoded-bytes N --timeout D --token-env NAME [--json]
  gitcrawl delivery report-status --outbox PATH --timeout D [--json]
  gitcrawl delivery verify-report --outbox PATH --max-rows N --max-encoded-bytes N --timeout D --token-env NAME [--json]
  gitcrawl delivery upgrade --outbox PATH --to-version 2 --max-rows N --max-encoded-bytes N --timeout D [--json]
  gitcrawl delivery upgrade --outbox PATH --to-version 3 --timeout D [--json]
  gitcrawl delivery stage-changes --db PATH --outbox PATH --max-journal-records N --max-affected-keys N --max-lookup-rows N --max-rows N --max-native-bytes N --max-encoded-bytes N --max-parts N --timeout D [--json]
  gitcrawl delivery deliver-changes --db PATH --outbox PATH --max-rows N --max-encoded-bytes N --max-parts N --timeout D --token-env NAME [--json]
  gitcrawl delivery cycle --db PATH --outbox PATH --max-journal-records N --max-affected-keys N --max-lookup-rows N --max-rows N --max-native-bytes N --max-encoded-bytes N --max-parts N --timeout D --token-env NAME [--json]

export-clusters is offline: it reads the native public durable-active/v1 view
and emits JSON containing base64 exact manifest, chunk and completion bytes.
It does not create an outbox, send requests, acquire credentials or advance a
checkpoint. Legacy/partial selection stays explicitly unqualified. captured-at
is a caller-supplied fixed capture timestamp, not provider freshness. Bounds cover
native reads and encoded requests; base64 output adds transport overhead.

stage-clusters exclusively persists a retained export's exact requests. It pins
the operator-selected current source identity, not the historical export inode.
cluster-status validates staged metadata and the ordered receipt prefix.
deliver-clusters validates all saved requests before credentials, sends exact bytes
only to the saved origin, and rechecks current native public/exclusion authority
after token acquisition and on redirects. Explicit retries replay pending bytes.
Status is staged, partial or complete; complete proves receiver acknowledgement,
not upstream freshness or a private report baseline. Cluster bytes counts decoded
wire bytes. No source capture, provider sync or scheduler runs.
Retain the original bundle and any failed owned file; no retry or cleanup runs.

measure-seed reads the same seven-table scope in one read-only transaction without
materializing rows or creating an outbox. It reports rows, scalar/cell types, text
bytes, maximum text/row sizes and source file metadata. Native bytes count NULL
as 1, integers as 9, text as 9 plus stored bytes, and column names once per table.
These are not encoded wire sizes, an RSS estimate, UTF-8 validation or seed
qualification. Keep acquisition stopped while using the measurement for admission;
changed source file size/mtime causes refusal.

Retained snapshot commands require a stopped schema-16 source with the exact
whole-file SHA-256. They use immutable SQLite and reject pending WAL or journal
content. measure-snapshot reports scalar sizes; export-snapshot streams bounded
chunks into a new private directory and writes manifest.json only after the final
source hash check. They never arm a journal, create an outbox or contact a provider.
The seven-table scope includes threads/comments and comments observation evidence,
not PR files/commits/checks/review-thread state. Export is not coverage qualification
or a live-change baseline. Retain partial directories after failure; do not replay
into them. Measurement and export scan/hash the source and require a deadline.

All paths and positive bounds are explicit. Live seed/change commands require native
schema 16 and an armed change
contract; no config discovery, automatic migration, provider sync,
release check, or credential helper runs. stage-seed exclusively creates a new outbox;
failed owned files are retained. status opens the current outbox read-only.
verify-report compares all seven full current API tables with a frozen qualified
schema2/3 outbox under one publication pin. It requires no pending capture or
delivery and checks complete typed rows, not body excerpts. It opens no native
source and writes no application state. Its byte bound covers total response
bytes and retained compact key/hash bytes; each response is at most 8 MiB.
Success proves current-row parity only, not acquisition freshness or history.
deliver sends saved bytes only to the saved destination and checks current native
policy after lazy service-token acquisition. There is no automatic retry.
Output contains custody/count fields only; bytes counts captured native scalar
bytes, not RSS. A qualified baseline means completed seed delivery, not provider
freshness, coverage, or publication approval.

Seed commands retain schema1 behavior. report-status reads schema2/3 qualified
metadata and active capture identity without opening the source or a writer.
upgrade performs only the named next transition: 1->2 validates the completed
seed and builds its compact inventory; 2->3 adds change-capture custody. It never
chains upgrades. The version2 byte bound includes saved requests and receipts.

stage-changes requires schema3, no pending capture, and an already-prepared
native row journal and report indexes. It never builds indexes or rearms the
source. All native/index reads close before exact-predecessor staging. Source
limits bound journal records, affected keys, lookup rows, rows and logical bytes;
encoding limits bound saved requests. The compact baseline digest still requires
a full compact-index scan under the deadline, not a full body reread.

deliver-changes sends one pinned saved capture, with at most --max-parts receipts
per invocation (1..64). It resolves --token-env lazily only for actual HTTP and
rechecks native policy. idle needs no token or source access. Partial/error output
reports only that pinned capture and this invocation's durable receipts; inspect
report-status and resume explicitly. No automatic retry or successor capture runs.

cycle resumes pending changes, captures one bounded interval, then delivers it
under one timeout. A failed or incomplete resume stops before capture. JSON emits
one record per attempted phase (resume, capture, deliver), including partial send
results. Stable empty v2 intervals are idle; initial v1-to-v2 transitions and
advancing empty checkpoints still publish. Bounds apply to each phase; a capture
must fit --max-parts. It never initializes, upgrades, acquires GitHub data or
installs a schedule. Use a single operator/scheduler lock per source/outbox.
`,
	"capture": `gitcrawl capture exports a stable code-free conversation snapshot.

Managed --github-token-command credentials are not supported by offline capture.

Usage:
  gitcrawl capture owner/repo [--schema gitcrawl.capture.v1] [--since RFC3339] [--output path] [--json]
`,
	"search": `gitcrawl search queries local thread documents, or accepts gh-shaped issue and PR search.

Usage:
  gitcrawl search owner/repo --query text [--scope threads|code|all] [--mode keyword|semantic|hybrid] [--limit N] [--json]
  gitcrawl search issues|prs <query> -R owner/repo [--state open|closed|all] [--json fields] [--limit N] [--sync-if-stale duration]
`,
	"code": `gitcrawl code indexes source from a local Git checkout.

Usage:
  gitcrawl code index owner/repo [--path DIR] [--max-file-bytes N] [--max-total-bytes N] [--max-files N] [--json]
`,
	"cluster": `gitcrawl cluster builds durable clusters from local thread vectors.

Usage:
  gitcrawl cluster owner/repo [--threshold N] [--min-size N] [--max-cluster-size N] [--k N] [--cross-kind-threshold N] [--limit N] [--model name] [--basis semantic|references|hybrid] [--include-closed] [--strict-vectors] [--json]
`,
	"clusters": `gitcrawl clusters lists latest display clusters with durable fallback.

Usage:
  gitcrawl clusters owner/repo [--sort size|recent|oldest] [--min-size N] [--limit N] [--hide-closed] [--json]
`,
	"clusters-report": `gitcrawl clusters-report writes a Markdown report for top display clusters.

Usage:
  gitcrawl clusters-report owner/repo [--sort size|recent|oldest] [--min-size N] [--limit N] [--member-limit N] [--body-chars N] [--hide-closed] [--json]
`,
	"durable-clusters": `gitcrawl durable-clusters lists governed durable cluster groups.

Usage:
  gitcrawl durable-clusters owner/repo [--include-closed] [--sort size|recent|oldest] [--min-size N] [--limit N] [--json]
`,
	"cluster-detail": `gitcrawl cluster-detail dumps one cluster and its member rows.

Usage:
  gitcrawl cluster-detail owner/repo --id N [--source auto|run|durable] [--member-limit N] [--body-chars N] [--hide-closed] [--json]
`,
	"cluster-candidates": `gitcrawl cluster-candidates reads computed candidate generations.

Each accepted native cluster run records its complete computed partition as an
immutable generation and selects it. Candidate IDs (GENERATION:ORDINAL) are
run-scoped, never durable cluster IDs; observed_overlaps list shared members
with durable groups and do not claim equivalence. --rollback selects the
previous generation; the next accepted run selects its own again.

Usage:
  gitcrawl cluster-candidates owner/repo [--generation N] [--limit N] [--members] [--json]
  gitcrawl cluster-candidates owner/repo --generations [--limit N] [--json]
  gitcrawl cluster-candidates owner/repo --rollback [--json]
`,
	"neighbors": `gitcrawl neighbors lists vector-nearest local issue and pull request rows.

Usage:
  gitcrawl neighbors owner/repo --number ref [--limit N] [--threshold N] [--include-closed] [--open-at time] [--created-after time|ref] [--merged-after time] [--json]

Time filters take RFC 3339 times or YYYY-MM-DD dates (UTC); --created-after
also takes an issue or pull request reference. --open-at and --merged-after
imply --include-closed. state_as_of records the last complete sync-and-sweep
start, not historical archive completeness. See the clustering guide for limits.
`,
	"runs": `gitcrawl runs lists local pipeline run history.

Usage:
  gitcrawl runs owner/repo [--kind sync|summary|embedding|cluster] [--limit N] [--json]
`,
	"purge-threads": `gitcrawl purge-threads plans owner-directed local removal, never a GitHub deletion.

Usage:
  gitcrawl purge-threads owner/repo --numbers 1,2 [--apply PLAN_ID] [--json]

Preview the exact selection, then pass its plan_id to --apply. Changed plans
are refused. Requires a native local archive and an idle analytics collector.
Blob-backed targets, workflow dependencies, shared clusters and reusable native
IDs are refused. Exclusions are permanent; portable publication is unavailable.
`,

	"change-contract": `gitcrawl change-contract records table changes for read-only consumers.

Usage:
  gitcrawl change-contract status [--json]
  gitcrawl change-contract enable [--retention N] [--json]
  gitcrawl change-contract bump --reason TEXT [--json]
  gitcrawl change-contract disable [--json]

enable installs in-store triggers that journal every insert, update and delete
of tracked tables in the writer's own transaction, including writes by older
binaries. Re-running enable re-arms tables whose triggers a rebuild removed and
advances the generation. bump forces every reader to rebuild after an owner-run
rewrite. disable removes the contract. Requires an existing native local
archive at this binary's schema version; these commands never migrate it. See
docs/change-contract.md for the reader algorithm.
`,

	"close-thread": `gitcrawl close-thread locally hides one issue or pull request row.

Usage:
  gitcrawl close-thread owner/repo --number ref [--reason text] [--json]
`,
	"reopen-thread": `gitcrawl reopen-thread clears a local thread hide.

Usage:
  gitcrawl reopen-thread owner/repo --number ref [--json]
`,
	"close-cluster": `gitcrawl close-cluster locally hides one durable cluster.

Usage:
  gitcrawl close-cluster owner/repo --id N [--reason text] [--json]
`,
	"reopen-cluster": `gitcrawl reopen-cluster clears a local cluster hide.

Usage:
  gitcrawl reopen-cluster owner/repo --id N [--json]
`,
	"exclude-cluster-member": `gitcrawl exclude-cluster-member locally removes one row from a durable cluster.

Usage:
  gitcrawl exclude-cluster-member owner/repo --id N --number ref [--reason text] [--json]
`,
	"include-cluster-member": `gitcrawl include-cluster-member restores one row to a durable cluster.

Usage:
  gitcrawl include-cluster-member owner/repo --id N --number ref [--json]
`,
	"set-cluster-canonical": `gitcrawl set-cluster-canonical sets the canonical row for a durable cluster.

Usage:
  gitcrawl set-cluster-canonical owner/repo --id N --number ref [--reason text] [--json]
`,
	"gh": `gitcrawl gh moved to octopool.

Usage:
  octopool login
  octopool gh <gh command>
`,
}

const tuiUsageText = `gitcrawl tui opens the local terminal cluster browser.

Usage:
  gitcrawl tui [owner/repo] [--limit N] [--min-size N] [--sort recent|oldest|size] [--layout focus|columns|right-stack] [--hide-closed]

If owner/repo is omitted, gitcrawl uses the most recently updated repository in the local database.
The TUI starts with ghcrawl-style cluster display defaults: --min-size 5, --sort size, --layout columns, and closed historical clusters visible. Pass --min-size 1 for singleton clusters or --hide-closed to focus open-only.
Mouse is supported: click rows, wheel panes, right-click for actions, and use the menu for copy/sort/filter/jump/member triage controls.
Press a to open the same action menu from the keyboard.
Press # to jump directly to an issue or PR number.
Press p to switch between repositories already present in the local store.
Press n to load neighbors for the selected issue or PR.
Enter from the members pane also loads neighbors before opening detail.
The TUI quietly refreshes from the local store every 15 seconds and leaves the current status alone when nothing changed.
`

const portableUsageText = `gitcrawl portable manages local portable-store snapshots.

Usage:
  gitcrawl portable refresh --expected-remote URL [--store-dir PATH] [--portable-db PATH] [--branch main] [--git PATH] [--timeout 2m] [--min-free-bytes N] [--max-growth-bytes N] [--json]
  gitcrawl portable prune [--body-chars N] [--no-vacuum] [--include-sync-failures] [--no-publish] [--json]
  gitcrawl portable export --profile current-state-v1 --output-dir PATH [--repository owner/repo] [--since RFC3339] [--database-name NAME] [--public-path PATH] [--body-chars N] [--max-bytes N] [--compression gzip|split] [--max-archive-bytes N] [--consume-source] [--json]

Subcommands:
  refresh             validate and fast-forward a clean configured subscriber
  prune               prune volatile payloads from the configured portable store
  export              create a validated portable artifact in a new directory

The sync failure ledger is excluded by default. --include-sync-failures keeps
the ledger but replaces every error message with a redaction marker. A present
or pending ledger forces a secure database rewrite even with --no-vacuum.
For a portable checkout, prune publishes the database and manifest back into
the checkout by default. --no-publish leaves them only in the runtime mirror.
Export never changes the active database or publishes the artifact. It creates
a complete database and manifest generation at a previously nonexistent path.
With --compression gzip, it commits the gzip archive and manifest without the
uncompressed database; --max-archive-bytes applies to that published archive.
With --compression split, it commits the uncompressed database as ordered
32 MiB parts (NAME.part-000, ...) and the manifest. Git can store a later
generation as a small delta against these parts; --max-archive-bytes caps each
part. Readers reassemble and verify every part and the whole database.
--repository semantically restricts the disposable snapshot to one owner/repo.
Refresh never regenerates config or invokes repair. It requires a matching
origin, clean checkout and manifest-backed artifact. --git requires an absolute
path (or set GITCRAWL_PORTABLE_GIT). Timeout defaults to 2m; free-space reserve
and measured growth budget each default to 2147483648 bytes. Refusals exit
nonzero; JSON distinguishes updated, no-op, refused and partial results.
`
