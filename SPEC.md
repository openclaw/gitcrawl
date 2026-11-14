# gitcrawl Spec

## Product Contract

`gitcrawl` is a local-first GitHub maintainer triage tool written in Go.

The target is a compact, local SQLite workflow for syncing, searching, clustering, and reviewing related GitHub issues and pull requests.

## In Scope

- local SQLite storage
- metadata-first GitHub sync for open issues and pull requests
- optional comment, review, review-comment, PR code, and PR review-thread hydration
- canonical thread document building
- bounded local Git source-document indexing
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
- `internal/cli`: root dispatch, command parsing and output, local/remote runtime adapters, and the terminal browser; command and `tui_*` files group their own responsibilities
- `internal/config`: config and env resolution
- `internal/store`: SQLite schema and persistence
- `internal/github`: GitHub API client
- `internal/syncer`: repository sync workflows
- `internal/documents`: canonical document generation
- `internal/codeindex`: tracked source-file scanning
- `internal/openai`: OpenAI summaries and embeddings
- `internal/vector`: vector search abstraction
- `internal/cluster`: bounded similarity-graph grouping; `internal/store` owns durable cluster persistence and governance
- Search orchestration lives in `internal/cli/search.go`; SQLite keyword queries live in `internal/store/search.go`.
- `internal/portable`: compact snapshot export, profiles, manifests, artifact I/O, and deterministic identity
- `internal/remotedelivery`: private native-report projection, bounded exact seed packing, a separate local seed outbox and explicit policy-checked HTTP delivery; `internal/cli/delivery.go` owns explicit commands, with no schedule activation

TUI guidance:

- `gitcrawl tui [owner/repo]` is a supported command; omit `owner/repo` to use the most recently updated local repository
- keyboard-first navigation is required
- mouse support is optional polish
- right-click must not be required for primary actions because terminal mouse support is inconsistent
- avoid decorative glyph noise or transient rendering debris in dense panes

### Native journal coverage

Native report capture requires armed row-key tracking for all seven selected
tables. Comments retain table ID 15; child observation memberships and global
excluded nodes use appended IDs 38 and 39 in the existing journal. Writable
repair reconciles legacy tracking modes and advances generation; read-only
status reports the stored mode, and capture never repairs the source. A writer
upgrade requires a fresh qualified baseline after rearm. See the
[change contract](docs/change-contract.md#writer-compatibility-before-activation)
for the writer floor; this code does not upgrade a deployed collector.

### Private seed custody

`CreateSeedOutbox` takes an explicit `SeedRegistration`, an unmodified successful
`CaptureNativeReport` result and positive `StageLimits.MaxEncodedBytes`. It calls
the existing private projector and seed packer before claiming a new, literal
absolute path. Registration fixes the service origin, archive, binding ID,
repository and captured physical file identity. Origins must use HTTPS or
literal loopback HTTP; no credentials, query, fragment or path beyond `/`.
This registration records a target, not permission to send private content.

One separate private SQLite file owns three tables: binding, capture and ordered
parts. The complete manifest, chunks and completion request are stored once as
exact BLOB bytes with SHA-256 hashes in one `BEGIN IMMEDIATE` transaction. Each
checked-out write connection uses `synchronous=FULL`. Native physical identity,
repository/scope, full logical cursor, inventory digest and captured row/byte
counts remain local and survive reopen. The logical seed token includes the
inventory digest, so equal native sequence numbers do not collapse changed
inventories. Physical identity never enters the wire requests.

The existing codec owns the 4 MiB request, 500-row chunk and 8,192-chunk limits.
The caller's aggregate byte cap bounds encoded bodies, not SQLite disk usage or
process RSS. Capture, projection and prepared bodies can coexist in memory.
Preparation failure creates no file; after an exclusive 0600 claim, failure
retains the task-created partial file for diagnosis. Existing files are never
replaced or automatically retried. Newly needed parents use 0700 without
changing existing directory modes. `OpenSeedOutbox` recognizes the current
outbox read-only before a writable helper may change permissions or journal
mode. Unknown/native files and other versions are refused; no migration, reset,
rebinding, pruning or automatic cleanup is implemented. The owner must keep the
outbox pathname bound to one physical file throughout its use.

`NextPart` returns the first unreceipted exact body after closing its read
transaction. Multiple workers can see the same part. `RecordReceipt` accepts
only the matching app/archive/binding/batch/cursor/hash and pristine-bootstrap
sequence (part index plus one), in order. Matching replay is a no-op, including
a changed `replayed` flag; original receipt bytes remain retained. The final
receipt, capture completion and saved baseline advance atomically. A qualified
baseline means **completed seed delivery only**, never provider coverage,
freshness or current exclusion authority. The receipt caller owns transport
authentication; recording a receipt itself makes no network request.

Outbox creation and receipt APIs do not open native sources or store
credentials. Delivery is explicit through `DeliverSeed` and the commands below;
there is no scheduler or delta path. Staged bytes or their hashes cannot authorize delivery.
Receiver policy remains independent. Future delta support must extend this one
durable owner through an explicit migration, not introduce another checkpoint.

### Native delivery policy checks

`store.CheckNativeReportDelivery` checks a recorded native anchor and streamed
staged references in one current read transaction. The caller supplies thread
numbers resolved from its staged inventory, plus raw JSON from staged threads
and comments; exclusion-ledger rows are metadata, not content references.
The checker uses the native repository/number exclusion and global node ledger,
with the purge owner's exact SQLite top-level `$.node_id` interpretation and
invalid-JSON-as-`{}` behavior. A repository-scoped ledger digest is insufficient.
Checks bind up to 500 references per SQL statement rather than querying each
archived row. Raw strings remain bound verbatim, without JSON re-encoding.

The physical file, repository ID/scope, schema cookie, epoch, generation and
armed change contract must match. Current sequence must be at least the captured
sequence and the current pruning floor must be sane. Ordinary content progress
and legitimate table-mode sequence gaps remain allowed; this is not a full
recapture or a journal-contiguity check. The existing stable-path precondition
applies, including its accepted transient replacement limitation.

The caller must provide a deadline and positive total row/raw-byte caps.
`MaxRows` counts each supplied number and each raw JSON string, including repeated
references; one row supplied as both counts twice. `MaxBytes` counts raw JSON
bytes, not process RSS. The callback is synchronous, must honor cancellation and
must not retain its yield function or perform HTTP. Any error refuses the entire
check. The native transaction closes before return; success describes that
snapshot, not reusable authorization, provider coverage or a delivery receipt.

The checker issues no SQL writes, migrations or checkpoints. Normal read-only
SQLite access can create or leave WAL/SHM reader sidecars. It does not switch to
immutable mode, change journal mode, checkpoint or unlink source files; current
WAL policy must remain visible. The sender invokes it at outbound effect and
keeps receiver policy and authenticated receipt settlement separate.


### Explicit native seed delivery

`Outbox.DeliverSeed` uses the saved origin, archive and binding, caller-owned
credentials/HTTP client, a deadline and positive part/row/byte limits. Before
credentials, it validates the bounded persisted seed and indexes parent thread
numbers from those saved rows. Current native rows cannot replace that mapping
after a purge. Bodies stream one at a time; limits bound encoded bytes and
staged row counts, not RSS. A default HTTP client has a 30-second timeout; a
supplied client is copied without changing it and remains bounded by context.

A request-local transport calls the native checker after token acquisition and
before each outbound attempt, including allowed redirects. Content chunks check
their staged references. Completion, including replay after a lost response,
checks every staged content chunk in one native read snapshot. An exclusion
added after an earlier chunk was acknowledged therefore holds completion.
The native read transaction closes before HTTP; receiver policy independently
checks the request at the service. A concurrent policy change is not serialized
across the network by the client.

Crawlkit sends exact stored bytes and validates the response; the outbox settles
only matching receipts. Any uncertain HTTP result or receipt error stops the
invocation without automatic retry or restaging. The pending bytes remain for an
explicit retry. The result counts receipt settlements, including replays, rather
than unique remote effects. An already completed outbox returns its saved result
without opening the native source or acquiring credentials. No collector,
scheduler, native checkpoint, upstream fetch or public endpoint is activated.

### Explicit private delivery commands

`delivery stage-seed` requires `--db`, `--repo`, `--outbox NEW_PATH`,
`--origin`, `--archive`, `--binding`, `--max-rows`, `--max-native-bytes`,
`--max-encoded-bytes` and `--timeout`. It identifies the literal native file,
captures through `CaptureNativeReport`, then uses `CreateSeedOutbox` for the
exclusive atomic stage. There is no separate empty registration or init state.

`delivery status --outbox PATH --timeout D` uses `ReadSeedOutboxState` without
opening a writer. `delivery deliver` requires `--db`, `--outbox`, `--max-parts`,
`--max-rows`, `--max-encoded-bytes`, `--timeout` and an explicit `--token-env NAME`.
It uses `Outbox.DeliverSeed` with the lazy environment provider and saved origin,
archive and binding. The invocation's timeout respects an earlier caller deadline.
Every bound must be positive; wire limits additionally cap encoded bytes and parts.
These are data/work bounds, not memory or disk reservations.

All three commands support `--json` and the global output format. They ignore
runtime configuration and bypass release notification, GitHub auth commands,
keyrings, provider sync and schema migration. `--github-token-command` is refused.
Output is limited to status, seed ID, baseline state, native token and counts;
`bytes` means captured native scalar bytes. Success reports cumulative receipts;
a failed send reports only this invocation's recorded receipts with status
`partial`, without a stale baseline. Unknown storage, HTTP and credential errors
are content-free. Context and known native/outbox error classifications survive.
Failed claimed files and uncertain sends remain for explicit reconciliation;
there is no cleanup, automatic retry or implicit source capture during delivery.

`delivery cycle` accepts the combined `stage-changes` and `deliver-changes`
bounds, native path, outbox, token environment name and one positive timeout.
It resumes a pending capture, captures one native interval, then delivers it.
All phases share the deadline. Errors stop immediately; a drain reaching its
part limit exits successfully with `part_limit`, leaving remaining saved work
for the next invocation and never capturing over it. JSON emits separate
`resume`, `capture` and `deliver` phase records with existing custody fields.

Cycle suppresses a zero-row interval only when native From equals Through and
the saved wire anchor is already v2. The initial empty v1-to-v2 transition and
advancing empty checkpoints still publish. Explicit `stage-changes` retains its
existing empty-transition behavior. The cycle opens the existing outbox for
each operation, preserving exact predecessor checks and saved-byte recovery.
Use one external lock per source/outbox; it never initializes, migrates, changes
scope, starts acquisition or installs a schedule.

### Public durable cluster context

`CaptureNativeClusterContext` reads one bounded native schema-16 transaction
using the native durable selection and visible-membership rules. It never repairs
representatives, migrates schema, rearms a journal, reruns analysis or acquires
upstream data. It requires a deadline, physical source-file identity and positive
row/compact-byte limits. Raw repository metadata must explicitly identify the
repository as public. Excluded ordinary members are omitted; an excluded or
missing representative refuses the capture. Under a nonempty global node ledger,
missing node identity also refuses capture. NULL representatives remain NULL.

The public profile `gitcrawl.public-durable-cluster-context/v1` contains four
ordered tables: `selection`, `groups`, `members`, `threads`. Selection keeps
selected and latest-analysis qualification separately, including absent scope
flags as NULL. Threads retain native integer identities, provider node identity,
nullable update time, labels and at most 800 UTF-8 runes of body. The SQL read
bounds the body prefix to 3200 bytes; it never loads raw bodies, embeddings or
model summaries into the projection. Native text, including embedded NUL, remains
lossless within the selected excerpt. The wire profile refuses any text scalar
over 16 KiB rather than truncating it.

`ProjectClusterContext` uses view `durable-active/v1` and canonical inventory
layout `gitcrawl-cluster-context-16-v1`. Scope is exactly the UTF-8 bytes of
`profile + "\n" + view + "\n" + repository + "\n"`. The inventory digest binds
all fixed typed rows and NULLs. It is distinct from the local native capture
digest. Manifest `repository_id` is the native SQLite ID; `selection.github_repo_id`
is the provider identity. The content token is
`gitcrawl-cluster-v1:<scope64hex>:<inventory64hex>`; it invents no journal cursor.
The existing exact-byte seed packer and receipt protocol carry this closed profile.

`delivery export-clusters` requires `--db`, `--repo`, `--archive`, `--binding`,
`--seed-id`, `--captured-at`, `--max-rows`, `--max-native-bytes`,
`--max-encoded-bytes` and `--timeout`. The capture ID is 32 lowercase hex characters;
the fixed timestamp uses RFC3339 and cannot be in the future. It emits JSON with
`profile`, `manifest`, `chunks` and `complete`; byte fields are base64 so decoding
recovers exact request bodies. The command does not create an outbox or send a
request. A sender must persist the complete bundle and replay unchanged bytes,
not recapture a changed archive under the same seed ID. Failed exports return no
partial bundle. No credentials, runtime configuration or provider calls run.

`delivery stage-clusters` requires `--db`, `--bundle`, `--repo`, `--outbox
NEW_PATH`, `--origin`, `--archive`, `--binding`, `--max-rows`,
`--max-encoded-bytes` and `--timeout`. It bounds the JSON envelope before base64
decoding, validates the fixed profile, exact destination, typed rows, descriptor
hashes, canonical inventory and completion, then exclusively creates private
SQLite custody. Existing destinations and sidecars are refused. The original
manifest/chunk/completion bytes are retained without re-encoding; no native
recapture, source migration, credential lookup or HTTP request runs.

The cluster outbox has application identity `GCCO` and version 1, separate from
private seed/report outboxes. It reuses their exclusive creation, FULL transaction
and part insertion owner. Nullable positive `begin_sequence` and per-part
`receipt_json` fields reserve the known subsequent sender contract: begin learns
the authenticated source sequence, later parts must be consecutive. Staging
leaves both NULL. The explicit sender
settles the authenticated begin sequence and each consecutive part receipt in one
FULL transaction. Replayed receipts must match all fields except the replay flag.
Private v1/v2/v3 readers and migrations refuse this format unchanged.

`delivery cluster-status --outbox PATH --timeout D` opens only the outbox
read-only. It checks bounded manifest/custody metadata plus every retained receipt
(up to 4096 bytes each), requiring an exact ordered prefix and source sequence
offset. It reports `staged`, `partial` or `complete`, content identities, counts
and the decimal begin sequence when present. Complete means receiver-acknowledged
snapshot delivery, not current visibility, provider freshness or a private report
baseline. Status is not a full request-body audit. `rows` counts captured
projection rows; `bytes` counts
all decoded request bodies, not base64 overhead or RSS. Both commands honor
`--json` and global output format and return content-free failures.

Physical file identity is recorded at staging for the operator-selected current
source. The export does not carry an inode, so staging cannot prove which file
produced that historical bundle. Store checks the current schema and exact
repository/native/provider identity without reading content or qualifying public
status/exclusion policy. Delivery rechecks that authority. Failed owned
files and original bundles are retained for explicit reconciliation; no automatic
retry, overwrite or cleanup runs.

`delivery deliver-clusters` requires `--db`, `--outbox`, `--max-parts`,
`--max-rows`, `--max-encoded-bytes`, `--timeout` and an explicit `--token-env`
name. It validates the entire saved bundle, including acknowledged parts and
receipts, before credential lookup or network effects. It constructs the existing
Crawlkit seed client from the persisted origin; no credential/config fallback runs.
Each invocation pins that immutable capture. A positive begin receipt establishes
the server sequence offset, so a second snapshot on the same binding need not
start at sequence 1. Each subsequent receipt must match `begin_sequence + index`.
Local receipt writes are atomic and idempotent, including concurrent exact replays.

Store checks current file/repository/provider identity, observed public metadata
and owner exclusions in one read-only snapshot immediately before each request,
including redirects. Every attempt checks all bounded saved thread numbers and
compact node identities; it does not fetch current bodies or infer old identity
from rows that may have been removed. Missing/empty node identity holds when the
global exclusion ledger is nonempty. The full compact reference set is checked
per request rather than cached as authority. No journal anchor is fabricated.

A lost or invalid reply leaves that exact part pending. The invocation stops,
retains custody and reports only its durable settlements; it does not retry or
attach a stale completion claim. The next explicit invocation replays unchanged
bytes. Parent cancellation/deadline and positive part/row/byte limits bound the
run; limits do not claim an RSS or disk cap. Remote diagnostics, source paths and
credential values are excluded from printable delivery errors. There is no
provider acquisition, migration, cleanup or automatic schedule.

A completed inventory proves delivery of this captured view, not upstream
freshness. Historical completeness cannot authorize a current empty-relationship
claim. A receiver must recheck current exclusion policy and independently qualify
freshness before using negative results for structural caching. Recurring cluster
projection updates and scheduled delivery remain separate activation work.

### Private native change-request preparation

`remotedelivery.PreparePrivateChanges` encodes an already projected, deterministic
list of native-key changes into exact private `changes-v1` request bytes. It
validates a canonical v1/v2 From followed by a compatible v2 Through, uses the
existing seven-table profile, refuses repeated native keys, and preserves typed
values and input order. Removals carry only their key; comments cannot retain an
unresolved raw blob reference, and observation families remain comments-only.

Positive total-byte and part limits apply, with the wire ceiling of 64 parts,
500 observations per part and 4 MiB per request. Preparation reserves the largest
legal envelope when splitting; a row near that ceiling can be refused even if a
shorter actual envelope would fit. An empty interval produces one explicit empty
part, allowing a receipt to acknowledge an unchanged scoped inventory. Cursors
chain from the supplied qualified cursor through each exact part ID.

This is encoding only. It does not read source state, stage an outbox, obtain
credentials, send requests, qualify a baseline or enable a timer. The outbox
owner must atomically bind these bytes to the corresponding candidate inventory;
only complete receipt settlement may promote it.

### Saved-seed inventory migration

`remotedelivery.MigrateSeedOutboxV2` explicitly migrates a fully acknowledged v1
seed outbox using positive row/byte limits and a caller deadline. It validates
every saved request and ordered receipt, then builds one compact inventory from
those bytes in a FULL SQLite transaction. No native database or provider is read.
The original seed parts, receipts, capture anchor and v1 native token are retained.
Failure rolls back the inventory schema and checkpoint changes.

Schema 2 adds canonical BLOB keys, row fingerprints and parent/number/node lookup
metadata, without copying full bodies. The private profile supplies its scope
SHA256 to the shared digest as exactly 64 lower-case hexadecimal ASCII bytes.
`ReadReportOutboxState` and `OpenReportOutbox` admit this explicit version; old
seed-only operations refuse it, including handles opened before migration.
The receiver sequence and cursor still describe the qualified baseline. This
migration does not qualify a new native generation or start incremental delivery.

### Native incremental capture and qualified index

`store.CaptureNativeReportChanges` reads a bounded journal interval, expands
changed parent/exclusion dependencies through native keys and the preceding
qualified compact index, and returns exact typed upserts/removals. The outbox
owner must pin that index and the entire baseline state for the call;
`WithQualifiedReportIndex` supplies both from one transaction. Neither read
transaction stays open for staging or network effects.

Prepare native reverse-node indexes before capturing the initial baseline.
Preparation changes the schema cookie; an older anchor is refused rather than
silently rebound. Capture also refuses changed file identity, epoch, generation,
scope, schema, missing tracking/indexes or an expired/regressing pruning floor.

Callers supply a deadline and positive journal-record, affected-key, lookup-row,
output-row and logical-byte limits. Every error returns no partial delta.
Ordinary title/body edits do not enumerate unchanged descendants; parent moves,
renumbering and changed exclusion policy expand the affected old/current
subtrees. Global native node exclusions retain their existing policy semantics.
The returned delta does not advance provider acquisition or qualify delivery.

## Command Surface

No `serve` command.

Public commands:

- `init`
- `delivery stage-seed|status|deliver|export-clusters|stage-clusters|cluster-status`
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
- `clusters-report`
- `durable-clusters`
- `cluster-detail`
- `cluster-explain`
- `neighbors`
- `search`
- `code`
- `gh`
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

`gitcrawl gh` moved to Octopool. Invoking it now prints a migration note:

```text
gitcrawl gh moved to octopool.
Run: octopool login
Then use: octopool gh ... or symlink octopool as gh.
```

Octopool owns the org-authenticated shared GitHub CLI cache and pooled read relay. Gitcrawl keeps the local mirror/search/cluster/TUI product. Use `gitcrawl search issues|prs ...` for local discovery and `octopool gh ...` or a symlinked Octopool binary for pooled `gh` reads.

Gitcrawl portable stores carry repo snapshots only. They do not carry an Octopool or runtime `gh` cache.

## Config

Default config path:

```text
Linux: ${XDG_CONFIG_HOME:-~/.config}/gitcrawl/config.toml
macOS: ~/Library/Application Support/gitcrawl/config.toml
```

Default database path:

```text
Linux: ${XDG_DATA_HOME:-~/.local/share}/gitcrawl/gitcrawl.db
macOS: ~/Library/Application Support/gitcrawl/gitcrawl.db
```

Existing installs with `~/.config/gitcrawl/config.toml` continue to load that
config when the new platform config path does not exist.

Path selection is intentionally incremental: existing legacy config, database,
cache, vector, and log paths are reused per component only while the matching
new platform path does not exist. Explicit `--config`, `GITCRAWL_CONFIG`,
`GITCRAWL_DB_PATH`, and configured portable-store paths remain authoritative;
database paths are durable filesystem paths, never SQLite `file:` URIs or
`:memory:` databases.

Primary environment variables:

- `GITCRAWL_CONFIG`
- `GITHUB_TOKEN`
- `OPENAI_API_KEY`
- `GITCRAWL_DB_PATH`
- `GITCRAWL_SUMMARY_MODEL`
- `GITCRAWL_EMBED_MODEL`

Legacy environment aliases may be supported only when they do not leak old naming into user-facing output.
