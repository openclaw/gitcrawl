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
- `internal/remotedelivery`: private native-report projection, bounded exact seed request packing and a separate local seed outbox; no HTTP delivery or CLI activation

TUI guidance:

- `gitcrawl tui [owner/repo]` is a supported command; omit `owner/repo` to use the most recently updated local repository
- keyboard-first navigation is required
- mouse support is optional polish
- right-click must not be required for primary actions because terminal mouse support is inconsistent
- avoid decorative glyph noise or transient rendering debris in dense panes

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

There is no sender, native source open, credential storage, CLI, scheduler or
delta path in this slice. Before a future sender transmits or replays any part,
the native exclusion owner must validate current policy at that outbound
effect; staged bytes or their hashes cannot authorize delivery. Receiver policy
remains independent. Future delta support must extend this one durable owner
through an explicit migration, not introduce another checkpoint.

### Native delivery policy checks

`store.CheckNativeReportDelivery` checks a recorded native anchor and streamed
staged references in one current read transaction. The caller supplies thread
numbers resolved from its staged inventory, plus raw JSON from staged threads
and comments; exclusion-ledger rows are metadata, not content references.
The checker uses the native repository/number exclusion and global node ledger,
with the purge owner's exact SQLite top-level `$.node_id` interpretation and
invalid-JSON-as-`{}` behavior. A repository-scoped ledger digest is insufficient.

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
WAL policy must remain visible. A future sender must invoke it at outbound effect
and keep receiver policy and authenticated receipt settlement separate.

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
