# Committed observation capture: source compatibility packet

This is an unactivated implementation for reader compatibility review. It is
based on accepted collector commit `6c9c1bea7d094397dcdfecfb50e693460592a5aa` in
the original Gitcrawl checkout, branch `feat/committed-observation-capture`.
The prior `feat/metrics-collection` branch remains intact. No live archive,
collector, schedule, credential, monitor or signing state is changed.

The accepted [r3 contract](observation-capture-proposal.md) and
[registry](observation-capture-registry-v1.json) are unchanged:

- Contract SHA-256: `f01aef7ce64526df7eefb0a6fe6d1229769183bda0e736584910d772a350f5b0`.
- Registry SHA-256: `cd6d17e41da24b2a70bc1e13d908399249066c57c0be80f957c1b9027cd6763d`.

## Installation and writer boundary

`Store.InstallObservationCapture(ctx)` explicitly installs the additive
[DDL](observation-capture-schema.sql), the exact registry, a random archive ID
and epoch, and an initial sealed zero-observation epoch descriptor, all in one
transaction. It admits the exact six native schemas at version 16 and raises
`user_version` to 17. Existing older executables therefore refuse the version;
they cannot silently continue as unfenced writers. There is no auto-install on
`Open` or a read-only open, and no collector schedule/CLI entrypoint calls the
installer in this preparation packet.

On a capture archive, open validates the native and capture schemas and never
runs unfenced legacy migrations. `Store.WithTx` establishes the source frame and
seals it before committing the existing native transaction. Native revision,
event and receipt writers retain their previous behavior. Only keys actually
touched inside that transaction are examined; there is no repeated native table
scan. The parsed registry is reused. DDL validation is cached against SQLite's
schema cookie; registry/epoch/context checks still occur for every frame.
The schema cookie is checked again after the native callback. Native schema
diagnostics recognize a valid capture-owned version 17 as current and report
broken capture as an error; an unmarked future schema retains the existing
unsupported-version rejection.

Native guards reject unframed writes and incompatible cell types. Recursive
triggers are enabled on the pinned writer connection, including REPLACE's
deletion semantics. BEFORE guards track UPDATE/DELETE, while AFTER INSERT tracks
the final auto-allocated integer key. Dirty-key insertion uses an explicit
absence condition because an outer UPSERT can override a trigger's OR IGNORE
conflict policy. First-touch ordering uses indexed SQLite rowid allocation.

Frame IDs are allocated with AUTOINCREMENT. The provisional descriptor is removed
inside the transaction and reinserted only at sealing. Deferred foreign keys from
the open context, dirty keys and observations reject an unsealed COMMIT. A
successful frame contains final afterimages, exact overlays, immutable support
edges and its declared count/sequence bounds atomically. Empty ordinary frames
can leave allocation gaps, which have no provider meaning.

Images compare only against that native key's current image. No SHA UDF or large
JSON uniqueness index is introduced. All creation, provider-edit and deletion
fields remain value fields. Repeated clocks/values across commits still generate
new observations. Equal image bytes alone do not authorize reusing a projection
that depends on an overlay or supporting image.

## Read and baseline interface

The optimized reader can consume the private tables directly in its own pinned
read transaction, following the contract. The source also provides:

- `Store.OpenObservationSnapshot(ctx)`: opens a dedicated read-only connection
  (separate from Crawlkit's one-connection writer pool), establishes one transaction,
  validates capability and pins H. `ObservationSnapshot.Capability` carries
  archive/epoch/registry, epoch-start coordinates and H as decimal strings.
- `ObservationSnapshot.WriteBaseline(ctx, writer)`: emits JSONL for all six native
  baseline datasets plus the entire retained commit/observation/image/support
  prefix from that same snapshot. It includes origin epochs and explicit NULL
  redactions, never current-head reconstruction of an older observation.
- `ObservationSnapshot.Close()`: releases the snapshot/WAL retention. Slow reader
  ingestion/publication should use the sealed copied stream after this release.
- `Store.CheckObservationCapability(ctx, expected)`: revalidates archive, active
  epoch and registry in a new snapshot before reader admission/publication.

Each JSONL envelope is `{stream, row}`. The initial `header` includes format 1,
capability and the exact registry JSON. Native dataset records carry key/value/
clock JSON plus captured support rows. Private feed records use typed cells for
every SQLite field, including decimal-string integer identities. The terminal
`seal` declares per-stream row/byte counts and SHA-256, plus SHA-256 of all
preceding encoded bytes. Counts serialize as decimal strings. An interrupted
stream has no usable seal. The seal does not hash itself.

This low-level source API creates no filesystem baseline package or extra
full-body backup. The receiving reader owns the private package ID, creation
metadata, temporary-file lifecycle, final atomic publication, manifest/seal
verification, and owner-removal barrier. It must retain all pre-H observations
as history while keeping the current-state projection at H, then replay strictly
after H. Reader integration and measured one-time export WAL/disk impact remain
activation gates. Merely calling `WriteBaseline` is not consumer acceptance.

Live replay consumers must validate whole frame bounds/counts and all images/
support edges before acknowledging a frame. Chunking a large frame may bound
transport bytes but cannot advance the checkpoint or publish an incomplete group.

## Purge, restore and derived copies

The existing narrow `PurgeThreads` plan now inventories capture image IDs without
returning bodies. In its native transaction it redacts all exclusive inventoried
images, withdraws removed heads, records owner-requested withdrawals and rotates
the epoch. Shared account profiles remain. Source/native retries and permanent
exclusions retain their existing semantics; owner removal is never recovery or
provider deletion. The existing blob-backed/shared-workflow refusals remain.
The blob refusal also checks historical captured revision images: clearing a
current native pointer cannot make an older blob-backed observation safe to
purge without an ownership-aware blob repair.

A PK or parent-key move records `reset_reason='identity_change'` on its ordinary
commit descriptor. This is an ownership-ambiguity marker, not a fabricated
provider event or an epoch reset. The v1 purge planner conservatively refuses
after any retained such marker; it does not guess historical ownership from
adjacent withdrawal/upsert rows. Supporting images referenced by an unrelated
retained observation also cause preflight refusal. These refusal boundaries are
explicit; no historical body is silently retained after a reported successful
purge.

`Store.ResetObservationCapture(ctx, fork)` establishes a new epoch on an otherwise
valid capture archive after source-owned restore; `fork=true` also assigns a new
archive ID. Retained images/observations remain. It does not repair unknown schema,
recreate a lost segment or claim the extent of unconsumed history loss. Schema
mismatch is a refusal requiring a separately reviewed migration. Arbitrary file
replacement outside source ownership cannot be certified by in-file metadata.

In-place portable pruning/scope reduction and consume-source transfer refuse
capture-owned archives. Frozen portable/cloud copies use explicit detachment,
requiring a distinct file with the same currently valid source lineage. Detachment
also verifies that the supplied SQL handle is attached to that actual copy file.
Detachment
removes private capture tables/guards and restores schema 16 before normal
transformations; existing final VACUUM performs the derived copy's secure rewrite.
The original source remains unchanged. Previously copied reader bytes still need
the downstream owner-removal/suppression barrier before purge.

## Validation and activation hold

Focused synthetic tests cover native writer history, final-key framing, A→B→A,
equal-clock repeats, semantic timestamp/permission changes, clock-only reuse,
both blob-repair orders, review membership and rekey, unknown versus empty,
rollback/cancellation, concurrent writers, sealed restart, missing guards/types,
large IDs/NUL text, pinned baseline/prefix, source copy boundaries and purge/reset.
The frozen registry and generated DDL are checked against the packet files.

No provider probes, manual collection, live schema installation, restart,
deployment, signing, pruning or schedule changes are included. Reader compatibility
and exact source review are required before any activation.
