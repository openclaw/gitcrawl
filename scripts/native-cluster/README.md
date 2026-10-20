# Native available-subset cluster producer

This is the source-owned implementation of the approved
`gitcrawl.cluster-scope.v1` / final-v2 selection contract. Core collection and the
independent official-main code producer retain their existing jobs and binaries.
Installation does not enable the held observation-capture schema.

## Admission and evidence

`ReadClusterAvailability` requires a verified core baseline, explicit public
repository evidence, fresh ordered core/lane frontiers, and successful latest
issue/PR discovery receipts. An incomplete source is admitted only when each
unresolved core obligation is a bounded, single-item `repository.n0` NOT_FOUND
with matching repository identity and retained native item identity where one
exists. Unmaterialized items remain counted obligations, never fabricated rows.
Other failures, ambiguous multi-item failures, unknown visibility or stale
progress still refuse. The bounded read admits at most128 obligations; exceeding
that bound refuses instead of truncating. NOT_FOUND never means deletion.

Unavailable inputs are omitted from new native embedding tasks and the computed
graph. Source history, bodies, vector rows, IDs, retry attempts/backoff, and owner
exclusions are untouched. `source_binding.availability_sha256` binds each request
to the semantic availability set. Pre-submit validation and native import recheck
it and all existing identity, clock, input, model and exclusion bindings. Model
text and the processor-only broker route remain unchanged. No serving vectors or
direct model credentials enter this producer.

## Atomic publication

The original exact native graph algorithm, thresholds, node bounds and completed
pair-cache validation remain. The new producer uses its own
`available-subset-pair-cache` beneath the existing controller state; it does not
delete the previous producer's cache. Observation-only retry changes do not
change the semantic graph signature.

Computation occurs outside owner settlement. The existing controller/runner
locks, bounded settlement and write deadlines, source identity/schema checks,
WAL/FULL/Darwin flush policy, and original analytics restoration remain in place.
The gate file is fully written/fsynced before atomic rename makes it visible.

The final transaction validates current inputs/curation/availability, imports
only validated selected vectors, and uses `SaveAvailableSubsetClusters` when
coverage is partial. This branch does not prune missing memberships, retire
missing groups or delete legacy similarity groups. Existing partial identity
reconciliation preserves old representatives and refuses ambiguous overlaps.
Unchanged ambiguity fingerprints skip another graph pass and require changed
inputs/curation or operator resolution; no automatic merge is authorized.

Scope metadata and private evidence bindings commit in
`cluster_runs.stats_json.native_producer`. `started_at` records final admission
after settlement in RFC3339Nano, with through <= observed_at <= started_at and
five-minute admission freshness. `finished_at` follows it. The state signature
includes scope/evidence metadata, excluding only its self-referential hash.

Available-subset runs explicitly record false provider/full-scope completeness,
true selected-input completeness, cohort counts and retained membership counts.
Only normal provider recovery and original fresh-vector/full-scope gates enable
a full-native run. Partial zero-task exports never authorize full-scope broker
retirement. Existing exact completion/archive/lease guards still apply.

## Native readers

The dedicated reader entrypoint permits only local cluster list/detail/report
commands and disables release/provider probes. A valid v1 durable history marker
makes durable results authoritative for defaults, even if legacy raw rows remain.
Explicit run_cluster access retains legacy ordering and never falls back.
Malformed scope is not legacy absence. Source namespace and run context travel
with results; members retain exact prior-run markers and generation status.
Historical reads validate the recorded admission time, not today's wall clock.
Claw owns its additional access-revision and filtered-count policies.

## Delivery and validation

The Go producer files use the `native_cluster_overlay` build tag and the same
pinned schema16 overlay mechanism as the preceding signed producer. The shared
scope/admission and reader code lives under `internal/store` / `internal/cli`.
Frozen earlier packets are not edited. Deploy immutable signed artifacts outside
the checkout; preserve the900s maintenance cadence and journal-only recovery.
Activation waits for the matching Claw consumer's installed acceptance.

Focused tests cover the shared21-case selection oracle (16 native cases, four
Claw authorization cases, one old-behavior reference), partial/full recovery,
retained representatives/legacy groups, rollback, no-op, availability changes,
bad scopes/anchors, native graph/cache equivalence, and controller lease/crash/
retention behavior. All providers in tests are synthetic. Initial live acceptance
must demonstrate an ordinary committed run and resumed core collection before
claiming the fleet is caught up.
