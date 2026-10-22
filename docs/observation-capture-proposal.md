# Proposed committed-observation capture contract v1

Status: proposed interface for coordinator agreement; no DDL or activation is
authorized by this document. Implementation will use the accepted source commit
`6c9c1bea7d094397dcdfecfb50e693460592a5aa` in the existing checkout. Native history,
provider clocks, exclusions, collector ownership and schedules remain intact.

Proposal revision: 3. This is a reviewable contract, not a claim that a committed
feed already exists. The currently accepted runtime has none of these tables.
The coordinator accepted the final-commit/support/baseline direction below.
This revision corrects the value/observation partition for exact pre-DDL review.

## Meaning of lossless

The feed retains each tracked key's committed observation, including repeated
values and repeated clocks. Separate commits A → B → A yield three observation
sequences. A later reader resolves immutable images and captured support, never
the then-current native head.

A native transaction is the publication boundary. **The accepted unit is the
final committed afterimage per touched native key within a source transaction,
uncoalesced across source commits.** Its temporary SQL steps are
not independently committed observations: for example, writing a review fetch
time and then its membership produces one final sync-row observation. Multiple
writes of the same key inside that transaction publish its final committed row.
This does not coalesce observations across transactions. An insert followed by
delete inside one transaction, with no prior committed row, creates no fictional
present row. A touched row that remains present emits an observation even if its
value and clocks equal the prior committed observation.

This finalization rule applies only to the new capture projection. It does not
change, replace, deduplicate or delete any separately persisted native event,
revision or actual source receipt. Existing revision/event/receipt writers keep
their current semantics and every row they persist. In particular, separate
immutable history rows written in the same transaction remain separate native
rows, even if the enrolled current-state row has only one final afterimage.
Native histories and receipts continue to be captured independently under their
existing reader contracts. No event time, receipt or intermediate provider event
is synthesized from the capture transaction descriptor.

Historical completeness starts at feed installation. Baseline records describe
retained source state, not invented provider observations before installation.

## Enrollment and clock overlay

The initial registry is exactly these six tables. Keys retain their native order.
Every native column is retained; the named clock columns are stored in a separate
overlay rather than duplicated in a large image for each observation.

| Native table | Typed key columns | Clock overlay columns |
|---|---|---|
| `thread_revisions` | `id:INTEGER` | `observation_sequence` |
| `blobs` | `id:INTEGER` | none (empty overlay) |
| `actor_identity_evidence` | `node_id:TEXT` | `observed_at` |
| `actor_profiles` | `node_id:TEXT` | `observed_at` |
| `pull_request_review_threads` | `thread_id:INTEGER`, `review_thread_id:TEXT` | `fetched_at` |
| `pull_request_review_thread_syncs` | `thread_id:INTEGER` | `fetched_at` |

Permission fields, membership, bodies, raw JSON strings, deletion timestamps and
reasons, provider edit timestamps and every creation timestamp remain value
fields. Provider timestamps embedded inside an opaque raw JSON string are
preserved verbatim; the codec does not rewrite that string. No column is classified
as an observation overlay merely because its name ends in `_at`.

The exact value-column registry (in native schema order, after removing overlay
columns) is:

- `thread_revisions`: `id`, `thread_id`, `source_updated_at`, `content_hash`,
  `title_hash`, `body_hash`, `labels_hash`, `raw_json_blob_id`, `created_at`.
- `blobs`: `id`, `sha256`, `media_type`, `compression`, `size_bytes`,
  `storage_kind`, `storage_path`, `inline_text`, `created_at`.
- `actor_identity_evidence`: `node_id`, `actor_node_id`, `login`, `actor_type`,
  `raw_json`.
- `actor_profiles`: `node_id`, `login`, `actor_type`, `name`, `bio`, `url`,
  `created_at`, `raw_json`.
- `pull_request_review_threads`: `thread_id`, `review_thread_id`, `path`, `line`,
  `start_line`, `is_resolved`, `is_outdated`, `viewer_can_resolve`,
  `viewer_can_unresolve`, `viewer_can_reply`, `first_author_login`,
  `first_author_type`, `first_comment_body`, `first_comment_url`,
  `first_comment_created_at`, `first_comment_updated_at`, `comments_json`,
  `raw_json`, `deleted_at`, `deletion_reason`.
- `pull_request_review_thread_syncs`: `thread_id`, `review_thread_ids_json`.

The last column is the deployed analytics extension, not just the base schema's
two-column sync table. Integer columns are `id`, `thread_id`, `raw_json_blob_id`,
`size_bytes`, `observation_sequence`, line positions and boolean flags; opaque
node/review IDs and all remaining enrolled columns have TEXT affinity. NULL
remains allowed exactly where the native schema
allows it. Registry validation covers complete columns, key order, affinity,
nullability, overlay partition and support rules; a new/unrecognized native
column invalidates capability rather than disappearing from capture. The registry
digest is calculated by source code at installation/validation, not a SQL UDF.

The nullable non-key column sets are exact:

| Table | Nullable non-key columns |
|---|---|
| `thread_revisions` | `source_updated_at`, `raw_json_blob_id` |
| `blobs` | `storage_path`, `inline_text` |
| `actor_identity_evidence` | `actor_node_id`, `login`, `actor_type` |
| `actor_profiles` | `name`, `bio`, `url`, `created_at` |
| `pull_request_review_threads` | `path`, `first_author_login`, `first_author_type`, `first_comment_body`, `first_comment_url`, `first_comment_created_at`, `first_comment_updated_at`, `deleted_at`, `deletion_reason` |
| `pull_request_review_thread_syncs` | `review_thread_ids_json` |

All capture keys must be non-NULL and match their registered runtime type.
SQLite's existing non-INTEGER rowid-table primary keys do not themselves enforce
NOT NULL; this is an explicit capture-admission requirement, not an assertion
that the native schema was stricter. An incompatible retained key is reported
and blocks a successful baseline; it is not skipped or silently repaired.

### Evidence for the remaining observation overlays

The following references are at accepted source commit `6c9c1bea`:

| Overlay | Writer evidence and exact meaning |
|---|---|
| `actor_identity_evidence.observed_at`, `actor_profiles.observed_at` | `internal/cli/analytics.go:244–250` supplies local UTC `time.Now()` after the provider call. `internal/store/analytics_source.go:227–279` stores that argument separately from provider fields. It records local observation time, not provider edit/creation time or transaction commit order. |
| `pull_request_review_threads.fetched_at`, `pull_request_review_thread_syncs.fetched_at` | `internal/syncer/syncer.go:1237–1292` supplies the local fetch-start time independently of `first.createdAt`/`first.updatedAt`; review-only `internal/syncer/review_state.go:60` supplies local sync start. Legacy complete-empty backfill at `internal/store/analytics_integrity.go:315–321` preserves the actual retained `last_pulled_at`, not the backfill's wall time. The overlay is the retained observation clock, never a claim of a new provider observation. |
| `thread_revisions.observation_sequence` | `internal/store/thread_observation.go:33–54` allocates a local observation reservation counter. Normal sync reserves before fetch (`internal/syncer/syncer.go:456`); enrichment at `internal/store/thread_enrichment.go:172–220` can reuse the accepted sequence or retain zero for older/legacy evidence. It is an observation-order fact, not the new feed's commit sequence, a timestamp, or proof of a fresh provider call. Its exact final INTEGER is retained on every observation. |

The sequence participates in native stale-observation/selection logic; equal-image
reuse does not authorize dropping it or substituting the feed sequence. Overlays
can repeat or arrive out of timestamp order. The committed feed order comes only
from its sealed transaction/observation coordinates.

`actor_profiles.created_at` is explicitly provider `createdAt`
(`internal/store/analytics_source.go:275`). `thread_revisions.source_updated_at`
is derived from provider thread/comment/review/check/workflow timestamps
(`internal/store/thread_enrichment.go:447–480`). Both are versioned values.
Revision/blob `created_at` originates locally (`thread_enrichment.go:169–170,
342–349,399–412`) but denotes retained row/evidence creation, not a repeated
recheck; it also remains in the value image. Review `deleted_at` remains paired
with `deletion_reason` in the value image, including NULL-to-tombstone changes.

### Projection and cache rule

An equal `image_id` certifies equality only of the registered value columns.
Every observation still reconstructs its exact full row from value plus overlay,
and attaches its captured support image identities and overlays. A projection
depending on observation time, freshness, sequence ordering or support state must
include those dependencies in its cache identity or recompute that part. It must
not treat an overlay-only commit as a missing observation or return an old full
projected record solely because the value image was reused.

Any change in permissions, deletion state, provider edit/creation timestamps,
membership or another value field creates a different image even when the body
hash and observation clock are unchanged. Blob repair can change a dependency
image without changing a revision's blob ID; support bindings likewise take part
in projection reuse. Raw JSON remains a value field even if a field duplicated
inside that opaque string resembles a clock.

The machine-readable column/key/overlay/support registry is
[`observation-capture-registry-v1.json`](observation-capture-registry-v1.json).
Its exact UTF-8 file bytes (including final newline) are the proposed
`registry_json`; `registry_digest` is their SHA-256. This definition does not
depend on JSON object reserialization order or any runtime SQL hash function.

### Typed codec: `sqlite-cells-v1`

Keys are arrays of typed cells, for example
`[["integer","9007199254740993"],["text","RT_example"]]`.
Images and overlays are objects mapping native column names to typed cells.

- NULL: `["null",null]`.
- INTEGER: `["integer","signed-decimal"]`, preserving signed 64-bit values.
- TEXT: `["text","exact text"]`, using JSON escaping for NUL/control characters.
  Text must round-trip UTF-8 exactly; invalid text is an explicit codec error,
  never replacement characters or truncated NUL content.
- BLOB: `["blob","uppercase hexadecimal"]`.
- REAL: `["real","round-trip decimal"]`, generated with SQLite alternate-form
  `printf('%!.26g', value)`; infinity has explicit `Inf`/`-Inf` values. No enrolled
  v1 column currently requires REAL or BLOB, but neither may be silently rounded
  into an INTEGER or coerced into TEXT.

Seq/transaction/image identifiers are transmitted as decimal strings. Image and
clock field sets are disjoint; their union reconstructs the native row. Key cells
must match that reconstructed row. JSON column strings remain TEXT cells rather
than being parsed into a different native value. Invalid encodings fail explicitly.

## Exact proposed tables

All IDs below are SQLite INTEGERs. Sequence, transaction and image allocation use
`INTEGER PRIMARY KEY AUTOINCREMENT`; rolled-back allocations are not observations.
No SHA UDF or global unique constraint on a large JSON value is required.

| Table | Fields and constraints |
|---|---|
| `capture_meta` | `singleton` PK=1; `format_version`=1; `archive_id` TEXT; `epoch` TEXT; `state` (`ready`/`invalidated`); `epoch_start_tx_id`, `epoch_start_seq` INTEGER; `registry_json` TEXT; `registry_digest` TEXT. Start coordinates identify the last committed boundary before this epoch's ordinary observations. Registry defines the above keys, all columns, clock sets and support roles. |
| `capture_commits` | `tx_id` autoincrement PK; `epoch` TEXT; `kind` (`observation`/`owner_purge`/`epoch_reset`); `previous_epoch` nullable TEXT; `reset_reason` nullable TEXT; `first_seq`, `last_seq` nullable INTEGER; `observation_count` INTEGER; `sealed_at` TEXT. Only sealed committed groups are replayable. Zero-observation groups have NULL sequence bounds. |
| `capture_images` | `image_id` autoincrement PK; `image_epoch` TEXT; `table_name`, `key_json` TEXT; `value_json` nullable TEXT; `redaction_epoch`, `redaction_reason` nullable TEXT. Value bytes are immutable except explicit owner redaction to NULL. Images are owned by one native table/key, not globally deduplicated across keys. |
| `capture_heads` | PK (`table_name`,`key_json`); `present` INTEGER; `image_id` nullable FK; `clocks_json` TEXT; `last_seq` nullable INTEGER. Internal current-version locator, never used to resolve an old replay observation. |
| `capture_observations` | `seq` autoincrement PK; `tx_id` FK; `epoch` TEXT; `table_name`, `key_json` TEXT; `operation` (`upsert`/`withdraw`); `image_id` nullable FK; `clocks_json` TEXT; `cause` (`native_write`/`owner_requested`). |
| `capture_supports` | PK (`seq`,`role`,`ordinal`); `image_id` FK; `clocks_json` TEXT. Supporting images and overlays are fixed before the enclosing source transaction commits. |
| `_capture_context` | Singleton internal writer context: `open`, `tx_id`, `mode`. It must be closed in every successfully committed public snapshot. |
| `_capture_dirty` | Internal transaction-local work persisted only within the native transaction: PK (`tx_id`,`table_name`,`key_json`), initial presence and first-touch order. Cleared before commit; it is not the replay log. |

Indexes: commits (`epoch`,`tx_id`); observations (`table_name`,`seq`) and (`tx_id`,`seq`); images
(`table_name`,`key_json`,`image_id`) and (`redaction_epoch`,`image_id`); supports
(`image_id`,`seq`).
The remaining PKs supply narrow point lookups. The primary observation key is the
global replay order; gaps do not imply lost observations. Consumers do not infer
row count or provider chronology from sequence arithmetic.

Immutable observation identity is `(archive_id, observation.epoch, seq)`;
immutable image identity is `(archive_id, image_epoch, image_id)`.
Immutable transaction identity is `(archive_id, commit.epoch, tx_id)`. The active
capability epoch is additionally carried by read/package envelopes; it must not
overwrite the original epoch in retained historical rows.

## Native transaction protocol

1. The capture-aware `Store.WithTx` establishes an internal writer context on its
   pinned SQLite transaction. Ordinary native provider reads remain outside it.
2. Pure-SQL BEFORE/AFTER guards on enrolled tables reject writes outside that
   context. They record initial presence and mark keys
   touched by INSERT, UPDATE and DELETE, including FK cascades. A key change
   marks old and new keys. All native fields, including clocks and permissions,
   participate; there is no body-only change filter.
3. Before `tx.Commit`, finalize every dirty key from the final native row. Compare
   the non-clock value only with that key's current image. Equal values reuse its
   INTEGER image ID; different values allocate a new immutable image. A → B → A
   may allocate another A image rather than searching/indexing large JSON across
   old versions. Always append the accepted committed observation and its exact
   clock overlay, including clock-only/repeat observations.
4. After all dirty images/heads are finalized, resolve support against that same
   final native transaction state. Then write the commit's bounds/count, clear
   internal dirty/context state and commit native rows, images, observations,
   support edges and commit descriptor atomically. Any error rolls all of it back.

`sealed_at` is informational host time immediately before commit, not a fabricated
provider time or a commit-order cursor. SQLite commit visibility plus the sealed
group establishes the boundary. Within a group, deterministic ordering is not a
claim that individual SQL statements were separate provider observations.

The deferred/rollback paths must never commit a context left open or an unsealed
group. Empty groups needed to record an epoch boundary remain valid;
an unrelated native transaction need not create a feed group. A withdrawn row
has NULL `image_id` and an empty clock overlay: it does not invent a final provider
observation time. Physical withdrawal is source-state removal, not a provider
deletion claim. A retained native tombstone is an upsert with its exact reason
and clocks.

A committed PK rekey from K1 to K2 publishes a withdrawal for the initially
present K1 if it is absent at commit, and an upsert for final K2, in the same
group. Initial absence plus final absence produces no present observation.
Temporary keys used and removed entirely inside the transaction do not become
fictional committed rows. Any final replacement at an initially present key
still produces that key's final afterimage. Supporting references use final
native keys and images after all rekeys/repairs finish; old immutable observations
keep their old key, value and support locators.

Current tracked writer APIs already use `Store.WithTx` directly or through their
caller: actor evidence/profiles, thread enrichment/blob writes, full review sync,
targeted review recovery, review tombstones and owner purge. Migration/portable
paths that use their own SQL transactions must explicitly invalidate/detach the
contract before transformation. Unwrapped writers fail closed. Guarding native
REPLACE behavior and every connection's trigger settings is an implementation
gate; it cannot be silently exempted from the contract.

## Supporting images

- `thread_revisions.raw_json_blob_id`: non-NULL references bind role
  `github_blob` to the exact final blob image and its clock overlay from this
  source commit, even when that blob changed later in the SQL statement order.
  An untouched legacy supporting row is materialized into an immutable image
  lazily without fabricating a provider observation.
- Review-sync observations bind ordered role `review_thread` edges for their
  complete `review_thread_ids_json` membership to exact review-row images and
  clocks from the commit. An explicit empty array remains known empty; NULL or
  unavailable membership remains unknown. No missing row becomes an empty set.
  Ordinals are zero-based positions in the exact stored membership array. Every
  member must resolve with the same native `thread_id` and its exact opaque
  `review_thread_id`; no cross-parent or title/number matching is admitted.
- Review-row observations bind the corresponding membership image as role
  `review_membership` when one exists. Standalone legacy absence stays explicit.
  This association does not claim that a retained historical/tombstoned row is
  in the current membership. Membership is determined only from the captured
  array. Single-valued `github_blob`/`review_membership` edges use ordinal zero.
- Missing required blob/member support prevents sealing; it does not resolve
  against a later mutable head. External-file contents referenced by a blob path
  are not SQLite row bytes and are not falsely advertised as immutable inline
  evidence. Existing source/reader storage-kind checks remain applicable.

Support edges belong to the observation `seq`, not to a mutable head or to an
image's latest use. Each edge records the image's own identity plus the exact
support clock overlay at this commit. Identical value-image reuse does not reuse
an older support clock. Edges are the explicit roles above; a consumer never
recursively resolves unlisted dependencies through live tables. In particular,
a review row's `review_membership` image binds the membership array, while a
sync observation's `review_thread` edges bind the member-state images. The
descriptor and all touched observations constitute one publication group.

## One consistent baseline snapshot/package bound to H

Install metadata, writer guards and tracking atomically; existing rows do not
require an immediate image copy. The baseline is subsequently exported from
**one pinned read-only SQLite transaction**, or from a verified immutable package
copied from that exact snapshot. Lazy before-images and independently advancing
native-page snapshots are not part of v1. `capture_baselines` is removed from the
proposal. Native collection continues through WAL while the initial export reads;
the exporter does not hold the source writer lock or modify the source.

1. Open one read-only transaction and establish its snapshot with a capability
   read. Validate archive/epoch/registry, ready state and a closed writer context.
   Select H, the last sealed commit visible in **that same snapshot**, including
   its tx ID and the last observation sequence at or before it. H is immutable
   for the entire export. Installation records a zero-observation epoch boundary
   so even an otherwise empty feed has an unambiguous starting descriptor.
2. From that transaction, export all six enrolled native tables, their exact
   values/clocks and required supporting rows. Every member and supporting blob
   resolves at H, regardless of subsequent live writes. An untouched native row
   need not acquire a source image merely to be exported; its baseline value is
   inline. Native PK ordering determines page boundaries, never JSON text order.
3. Also export the complete retained observation prefix through H, including its
   sealed groups, observation clock overlays and all images named by observations
   or support edges (even if an image was created in a prior epoch).
   This retains A→B→A and equal-clock observations committed between installation
   and the snapshot. Current state at H is not a substitute for those observations.
   On a reset, include the safely retained historical epochs as historical rows
   under the new capability: preserve origin epochs and explicit redaction
   markers. A previously revoked package or stale cursor is never reauthorized.
4. Write a private temporary package, verify table counts and ordered stream
   digests, seal its manifest, close the source read transaction, and publish the
   package atomically. A partial export is not usable. After a crash, finish from
   an already sealed package or discard the incomplete export and start one new
   snapshot; never join pages from different read transactions. Package copying
   must be bounded and measured for WAL/disk/CPU impact before activation; slow
   consumer ingestion happens only after the source transaction has closed.
5. The consumer retains the complete prefix as observation history and the
   baseline as retained native state at H. It must not replay a pre-H frame over
   the H current-state projection. Once the entire baseline and prefix are
   durable, subsequent live replay starts strictly after H, preserving complete
   transaction groups. The consumer publishes/acknowledges H only at that point.

The package manifest contains `format_version`, a random `package_id`,
`archive_id`, `epoch`, `registry_digest`, `epoch_start_tx_id`, `epoch_start_seq`,
`h_tx_id`, `h_seq`, informational `created_at`, and per-stream row counts, byte
counts and SHA-256 digests. Streams include the six native baseline tables,
commit descriptors, observations, referenced images and support edges. Digests
are export verification in source code, not a SQL UDF or a large-JSON index.
Each baseline row is `(table_name, key_json, value_json, clocks_json, supports)`
within that manifest's H. Each support carries role/ordinal and its exact H
key/value/clocks, or a package-local reference to that captured value. A source
image locator is optional for baseline values and cannot replace H validation.
The manifest also includes the ordered retained epoch/reset descriptors, so a
restore with a missing historical segment cannot be presented as a continuous
prefix. Redacted image rows retain identity/reason metadata and NULL value bytes;
they are not valid present-row afterimages for a reader to publish.

The package is a managed private artifact, not a new long-term full-body backup.
Its ownership/lifetime is recorded so owner purge can revoke and dispose affected
packages. Before admission, the reader revalidates the active epoch/capability
and downstream owner barrier; a package from a revoked epoch is rejected.
Unconsumed observation-prefix loss is a coverage gap, never silent truncation.
Pre-installation history remains exactly what native history/event/receipt tables
retain. Those independent datasets and their capture contracts are unchanged.

## Reader protocol and bounds

Each short read begins with `BEGIN` in normal read-only WAL mode, validates
archive/epoch/registry/readiness and closed writer context, and chooses a sealed
commit high-water mark. Read commit descriptors, observation rows, immutable
images and support references from that same snapshot. Never use immutable-file
mode on the live archive.

Replay is uncoalesced and groups whole source commits. A large group can transfer
in bounded chunks, but root must retain its declared bounds/count and publish or
acknowledge it only once complete. The cursor advances to the last fully durable
group, not an unseen high-water mark. Root's per-dataset indexes may optimize
lookups, but cannot silently discard other members of a captured transaction.
No feed pruning or retention-floor advancement is part of v1.

The durable checkpoint includes both `tx_id` and the last observation `seq` in
the bound archive/epoch. Commit paging uses `tx_id`, including groups with zero
observations. A snapshot high-water mark H is a sealed commit descriptor, not
`max(native_id)` or a wall-clock timestamp. Every chunk repeats archive/epoch and
the descriptor; missing/duplicate members or mismatched declared bounds/count
prevent acknowledgement. A reader must revalidate capability when admitting a
completed group for publication; source reset and downstream owner suppression
must also fence already-captured batches. A previously held SQLite snapshot alone
cannot revoke bytes a reader already copied.

## Owner purge, reset and restore

An exact owner purge keeps the existing downstream suppression barrier. In its
native transaction it removes native targets, withdraws their heads, and redacts
exclusive historical capture-image bytes for the exact inventoried scope. The
purge context does not materialize an OLD body solely to delete it. It inventories
all image versions, including no-longer-current keys/parents after a rekey or
repair, rather than only the current head's image. Source baseline packages and
downstream staged/cached copies are part of the existing owner-removal barrier.
Shared supporting images required by unrelated retained observations stay intact;
ambiguous ownership remains a preflight refusal before native mutation.

The admitted native purge remains `PurgeThreads`, with its exact repository,
kind/number, original thread ID and content-node inventory. Its existing
unsupported blob-backed/shared-workflow target refusal is retained. The capture
extension adds these explicit obligations:

| Capture data | Exact purge treatment |
|---|---|
| Thread-revision, review-row and review-sync images | Inventory every retained image whose native parent/identity belongs exclusively to the target, including historical key variants; redact its value bytes. |
| Actor-evidence images | Inventory every version of the exact target content-node identities, including proven key variants; redact its value bytes. |
| Actor-profile images | Preserve shared account profiles, exactly as native owner purge does. Do not infer account ownership from one removed conversation. |
| Blob images | Existing blob-backed target refusal remains; no generic blob garbage collection or guessed shared-blob deletion is introduced. |
| Supporting edges and observations | Preserve metadata/clock audit records; locators to redacted values return explicit revoked evidence. Do not substitute empty content or find an older copy. |
| Heads | Remove live value pointers for removed keys; historical image redaction is independent of head reachability. |
| Baseline packages | Revoke the old epoch first under the owner barrier and dispose affected source-managed packages; never reuse them to rebaseline. |

An implementation must prove the complete historical identity/key closure before
admitting purge. A withdraw/upsert pair alone is not proof that two different
identities are the same owner. If PK rekey or parent reassignment makes historical
ownership ambiguous, purge refuses before deleting anything; it cannot silently
retain exclusive old images or remove unrelated ones. This is a required refusal
fixture and an exact-source-review gate, not permission to guess the closure.

Redaction is the explicit exception to immutable bytes: image identity/key metadata
survives, but the body becomes NULL with `owner_requested` and the new redaction
epoch. It never becomes an empty body, provider deletion, or successful recovery.
The purge rotates the capture epoch atomically and records an `owner_purge` commit
descriptor with its predecessor epoch. A stale reader must stop and acknowledge
the source reset/owner barrier before rebinding. A redacted locator is explicit
unavailable evidence and cannot recover content from another mutable/native head.
Root must also invalidate staged/cached copies under its existing owner policy.
Unrelated historical images and observations remain retained.

Rebuild/restore/fork cannot silently retain the old cursor identity. A supported
restore runs under source ownership, gets a new epoch before readiness, and
requires baseline reconciliation. Forking a new writable lineage also gets a new
archive ID. Commit/image epoch components prevent collisions when a restored
allocator later reuses numbers from a lost future branch. Physical loss of an
unconsumed segment is reported as a coverage gap, never reconstructed timestamps
or invented events.

Schema/trigger mismatch, source replacement, epoch mismatch, missing images or
an unsealed committed writer context are fail-closed capability errors. Source
copies used for portable/cloud publication detach capture capability and its
private retained images before transformation; tracked-source in-place consume
or destructive pruning is refused before ownership transfer. Arbitrary in-place
filesystem restore outside ownership cannot be detected by an in-file UUID alone
and is not a supported continuous-replay operation.

An epoch reset requires a new consistent snapshot/package bound to its own H.
It never repurposes an old baseline package. Image locators reused for unchanged
retained rows keep their original `image_epoch`; current heads are internal
pointers, not a claim that those images were created in the new epoch.
Pre-reset cursors/packages are no longer admitted. A new baseline may retain
sanitized older observations under the new capability, with their original
origin epochs and explicit image-redaction markers; it never grants permission
to reuse pre-purge copied bytes. No ordinary read path bypasses redaction or
falls back to old staged bytes after revocation.

## Current writer coverage and limits

These are source-code references at accepted commit
`6c9c1bea7d094397dcdfecfb50e693460592a5aa`, not line numbers in the older current
working branch:

| Writer/path | Current behavior and proposed coverage |
|---|---|
| `internal/store/store.go:163–190` | `WithTx` / `withTxOnce` is the commit finalization point. No capture logic exists there yet. |
| `internal/store/analytics_source.go:227–280` | Actor evidence/profile upserts can change old keys and clocks; both use `WithTx`. |
| `internal/store/thread_enrichment.go:152–162,308–415` | Enrichment joins or opens a transaction; historical revision pointers/clocks and same-ID blob repairs must all be captured. |
| `internal/store/review_threads.go:38–173` | Review sync writes fetched time, membership and rows inside one transaction; tombstones use the same boundary. |
| `internal/store/analytics_integrity.go:290–340` | Native membership backfill is a second sync-row writer; it also uses `WithTx` and must emit observations. |
| `internal/syncer/review_state.go:36–71` | A review-only batch publishes multiple validated parents in one transaction; group framing must retain that boundary. |
| `internal/syncer/syncer.go:494` | Ordinary full history publishes parent/children/review/enrichment through a native transaction. |
| `internal/store/thread_exclusions.go:433–513` | Owner purge removes enrolled actor rows and cascaded dependents in one transaction; add capture redaction/epoch handling without weakening current unsupported-target refusal. |
| `internal/store/schema_observation.go:284–349`, `tombstone_schema.go` | Table reconstruction/backfill is not a normal new provider observation. A capture-aware migration must invalidate old capability and establish a new epoch before readiness. |
| `internal/store/portable_scope.go`, `portable.go`, `portable_schema.go`; `internal/cli/cloud_sqlite.go:139–180` | Scope/pruning/sanitization transform derived copies or explicitly consumed sources. Detach private capture state first on copies; refuse in-place tracked-source consume in v1. |
| `internal/cli/portable_refresh.go:304`; `internal/portable/export.go:197` | File replacement/ownership transfer is a lineage boundary, not continuation of a path/ID cursor. |

There is no implemented generic `import-sync` writer in the accepted source.
SQLite FK cascades and conflict/update paths are covered by enrolled-table
triggers, not only this list of Go entry points. Direct unwrapped mutation,
trigger disabling and out-of-band filesystem replacement are not supported
continuous-feed operations. Implementation fixtures must exercise each admitted
writer and explicit refusal/reset paths, including REPLACE trigger behavior.

The six-table registry does not claim lossless capture for every other Gitcrawl
table. Existing downstream capture for unenrolled parents, comments, metrics,
and native history tables continues under their existing contracts; no complete
cross-table snapshot of those unenrolled datasets is implied by a feed group.
Captured supporting images cover only the exact roles defined above.

## Acceptance gates before activation

Use small synthetic fixtures with the following required assertions:

| Case | Required proof |
|---|---|
| A→B→A and equal-clock repeats | Three separate source commits yield three ordered observations and preserve their actual overlays, even when two value images are byte-equal. |
| Same-key internal writes | Fetch time, then membership, then row updates in one transaction expose only final metadata; separately persisted native history/event/receipt rows still all exist, byte-for-byte. |
| Native revision independence | Multiple immutable native event/revision rows in one transaction remain distinct rows with their original IDs and clocks. Capture finalization never compacts that history or uses a fabricated receipt in its place. |
| Repair plus dependency ordering | A blob repair before or after the revision update resolves to the same final committed blob image/overlay for that revision observation. A later commit changing the blob cannot alter the earlier locator's bytes. |
| Review membership | Sync image, ordered member images, permissions, comments and overlays all represent the same committed state; unknown and empty remain distinct. Later mutable heads cannot satisfy a missing captured member. |
| PK rekey | Old-key withdrawal and final new-key afterimage share a group; captured metadata/supports follow final keys. Earlier observations and separately retained native histories keep their original identities. A temporary key never becomes a false committed row. |
| Snapshot baseline | While one pinned snapshot exports pages, a second connection updates, inserts, deletes, rekeys and repairs enrolled rows. Every package value/support still equals H; every later committed observation is available after H. |
| Prefix preservation | Commits between installation and H, including A→B→A and repeats, remain in the package's observation history; loading them cannot regress the H current-state projection. |
| Crash and framing | Incomplete packages are rejected; retries use the same sealed package or a wholly new snapshot. Partial/oversized groups do not advance the durable checkpoint. |
| Rollback/cancellation/concurrency | Failed transactions leave neither native nor capture state committed. Serialized successful writers yield complete ordered groups with closed context. |
| Purge | Exclusive afterimages at every inventoried version are redacted, shared accounts/images preserved, stale packages/cursors revoked, and unsupported or ambiguous historical ownership refused before mutation. |
| Restore/reset | New epoch or archive identity is required; same path/max ID cannot pass. Retained historical epochs keep their identity/redaction markers under a fresh baseline capability. Missing prefix/images are reported as gaps, not reconstructed events. |
| Semantic timestamp/value changes | Changing only review `deleted_at`, first-comment creation/edit time, actor `created_at`, revision `source_updated_at`, or revision/blob `created_at` allocates a changed value image. Permission changes likewise invalidate value reuse even with equal body hash and observation clock. |
| Observation overlay reuse | Changing only registered `observed_at`/`fetched_at`/`observation_sequence` may reuse the value image, but appends a full observation and reconstructs the new overlay. Freshness/sequence-dependent projections and support bindings remain current; equal-clock repeats remain separate commits. |
| Codec/schema | Large integers, NULL, control/NUL text, type mismatches and key/image binding are exact; unsupported schema/codec/trigger state fails explicitly. |

Assert indexed change/image lookups and preservation of native IDs/history/clocks.
The fixtures are a planned gate; none have run for this proposal and no production
behavior is claimed implemented by this document.

Root must accept the exact codec, clock sets, commit framing, baseline procedure
and reset/redaction behavior before source DDL. Reader compatibility and exact
source review remain separate activation gates. No provider probes, manual
crawls, live writes, restarts or schedule changes are part of this preparation.
Activation planning must account for every executable that can open this writer
archive: an older writer cannot be permitted to skip capture or silently continue
against a new schema. This draft does not authorize a runtime/job change and does
not claim that the currently installed writer binaries implement the new protocol.
