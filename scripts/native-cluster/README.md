# Native available-subset cluster producer

This is the source-owned implementation of the approved
`gitcrawl.cluster-scope.v2` identity-quarantine contract, with unchanged v1 parsing. Core collection and the
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
`identity-quarantine-pair-cache` beneath the existing controller state; it does not
delete the previous producer's cache. Observation-only retry changes do not
change the semantic graph signature.

Computation occurs outside owner settlement. The existing controller/runner
locks, bounded settlement and write deadlines, source identity/schema checks,
WAL/FULL/Darwin flush policy, and original analytics restoration remain in place.
The gate file is fully written/fsynced before atomic rename makes it visible.

The final transaction validates current inputs/curation/availability, imports
only validated selected vectors, and uses `SaveAvailableSubsetClusters` when
coverage is partial. This branch does not prune missing memberships, retire
missing groups or delete legacy similarity groups. Current active identity relations form closed components with the complete
computed candidate partition. Ambiguous merges, splits and protected targets
quarantine their entire component. Removed historical memberships do not connect
components. The original partial reconciler still refuses ambiguity for every
published target; there is no automatic merge. Complete quarantined row images
are compared before/after tentative writes, including indirect writes and history.
Protected identities outside the computed cohort are also reserved against full
retirement. Full v2 publication preserves legacy similarity rows.

Scope metadata and private evidence bindings commit in
`cluster_runs.stats_json.native_producer`. `started_at` records final admission
after settlement in RFC3339Nano, with through <= observed_at <= started_at and
five-minute admission freshness. `finished_at` follows it. The state signature
includes scope/evidence metadata, excluding only its self-referential hash.

Provider completeness and identity completeness are independent. Scope v2 keeps
selected as the exact fresh-vector computed cohort and splits it into published
and identity_quarantined. An available-subset run can have complete provider
coverage but still incomplete identity publication. Counts and retained markers
remain explicit. With no publishable candidates, status=blocked_identity records
retained_only analysis without replacing the latest successful generation;
unchanged semantic input/state is a no-op, including repeated blocked analysis.
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

## Quarantine proof and reader status

A complete metadata-only candidate partition, active relation graph, exact write
map and quarantined preimage digest are retained before settlement in an atomic,
compressed content-addressed proof file. The logical UTF8 SHA256 is bound by the
scope and native receipt; the archive stores that hash and scope, not a duplicate
full partition on every run. The prior reviewed subset producer cache can be
copied byte-for-byte into the new cache namespace and accepted only with its
explicit producer hash and unchanged algorithm/model/source/exclusion lineage.
Original cache/evidence remains untouched; parent provenance is never relabelled.

Readers keep successful selection separate from latest v2 analysis. Explicit raw
reads have analysis=null and no durable quarantine labels. In a mixed legacy
view, analysis is durable-context only; raw ID collisions cannot inherit it. The
latest successful durable row is the analysis floor even when raw selection is
null. Group identity_quarantine_status and member retention_reason expose holds
without claiming a refreshed generation. Claw adds its owner-removal withholding.

The strict wire schema is in docs/native-cluster-scope-v2.schema.json. Synthetic
writer fixtures cover partial publication, provider-complete identity quarantine,
blocked analysis/no-op, indirect-write rollback and full recovery. Native model
work remains exclusively in the authorized processor broker.

## Computed candidate generations (v3)

After v2 publication and scope validation, the same native transaction records
the complete computed candidate partition of the quarantine plan as an
immutable `gitcrawl.cluster-candidates.v3` generation. The generation is bound
to its run, algorithm, input hash and quarantine proof hash. It stores each
candidate's members, its v2 durable status and target, and observed overlaps
with active durable groups, then moves the per-repository selection pointer to
it. The quarantine postimage hash is checked again after this write. A
generation never writes the six durable history tables, so v2 scope, strict
schema and reader semantics are unchanged. Blocked-identity runs still record
their generation. No-op runs record none. See `docs/clustering.md`
(Computed candidate generations) and `gitcrawl cluster-candidates`.
