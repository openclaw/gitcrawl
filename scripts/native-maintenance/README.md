# Native embedding and cluster maintenance

This is the source for the existing isolated native-owner Python controller. It
uses the configured native binaries and processor broker; model credentials and
serving state are never copied into the native archive. The original core,
900-second cadence, capture holds, packet budgets, and retained-envelope cap of
16 remain unchanged.

Stale source-clock previews preserve their broker request for exact-input reuse.
A successful combined publication now checks retained envelopes for a completed
replacement. Retirement requires every task to match the replacement's exact
input/native hashes, provider thread identity and input-shaping fields, with the
same or later native fetch observation sequence. The completed receipt and native
receipt hashes must agree, and the original archive device/inode/schema/provider
and persisted cluster-run signatures must verify through read-only point queries.
New completion receipts preserve the request header for source binding. A later
timestamp or a zero-task export alone does not authorize retirement.

Changed-input requests also have a full-scope retirement gate. Retained task and
provider identities/observation clocks are read by primary key before native
cluster publication. After a successful publication or a validated unchanged
result, one existing native export with limit 1 must report zero missing/stale
tasks under the same archive identity and policy. The persisted native run
signatures and the retained tasks' unchanged before/after source bindings must
also match. Only then may superseded disposable envelopes retire, including
inputs which legitimately never reappear. A new due task, source change, missing
row, policy change or failed publication keeps them retained. The extra export
is read-only and makes no model call; it runs only when retained work exists.
This gate runs after combined publication as well as the ordinary zero-work
branch, so successful work frees slots without waiting another cadence or
growing the cap.

An immutable proof records the original retained entry before any release. The
controller keeps ownership of a completed pending request until retirement intent
is checkpointed, including the zero-scope gate. Restart resumes this completion
without fetching or resubmitting broker results. The controller respects `native_request_inflight`, and
replays only idempotent release after interruption. A separate immutable receipt
records release before checksum-checked removal of the exclusive temporary
request/result files. Original completion/native receipts and every archive row
remain intact. Incomplete proof and changed files stay retained.

Requests with an older consumer-policy binding are ineligible for the current
zero-scope proof and remain retained without blocking other eligible retirements.
Every completion release must return the expected released status and request
digest before the controller discards pending ownership; an invalid response
keeps its durable release intent and local proof for restart.

Existing receipts lacking the new header have an explicit, single-envelope
recovery path. Under the existing orchestrator lock, invoke the installed script
with its usual config/hash plus:

```
--retire-completed /absolute/native-state/completed-receipt.json \
--retained-digest EXACT_RETAINED_REQUEST_SHA256
```

This path uses the fixed schema16 producer contract and still verifies every task
and the persisted native publication. It performs no export, model work, native
import, collection, or owner settlement. It must not be used with timestamp-only
candidates. Run the ordinary installed controller afterward to resume pending
work through its original privacy, clock, budget, and lifecycle gates.

Run disposable regressions with:

```
python3 -m unittest discover -s scripts/native-maintenance -p 'test_*.py' -v
```

## Completed disposable work at the retained cap

A completed superseded broker envelope can also finish through a private archive
handoff. The exact request/result bytes and bindings are hash-verified, fsynced,
and read back beneath `completed-broker-archive/<request-sha256>/`, with 0700
directories and 0600 files. The manifest explicitly records derived-work
retirement, not native publication. The original entry and a separate retirement
receipt remain immutable recovery evidence; native archive rows are not changed.
Current native/consumer exclusions refuse new copies for excluded tasks. Include
these task-indexed private archives in any later owner-directed evidence purge.

A successor must be durably submitted before older references are released, so
its identical native-input jobs remain reusable. Incomplete results remain queued;
active leases defer release. The retained entry is removed only after archive
verification, a checkpointed release intent, lease-guarded idempotent release,
and a durable receipt. Interrupted copy, pre-intent checkpoint, post-release, and
partial working-file cleanup all resume without losing pending ownership.

Fresh exports align with an actual ordinary core completion receipt (at most
120 seconds of bounded read-only waiting, then explicit deferral). This avoids
exporting partway through the core update. Analytics continues during model work;
the existing controller settles it only at the final prepared native write.
No stale-clock acceptance, cap increase, quota bypass, or repeated identical
failed import is introduced. Packet/compute/write limits and 900-second cadence
stay unchanged.


## Available-subset admission

The scoped native exporter/combined producer now owns the availability proof.
The controller waits for a recent ordinary progress boundary even when provider
coverage is incomplete; it never changes that coverage flag. Native admission
then requires qualified item-level NOT_FOUND evidence and healthy discovery,
and omits unavailable inputs. Before broker admission or pending work resumes,
`embedding-validate` rechecks the availability binding and exact source clocks.
Unqualified scope retains pending work and waits; changed availability obtains a
new request without relabelling the original or resetting its proof.

A partial publication's zero-task export is not full-scope retirement evidence.
The controller and retirement verifier both refuse that shortcut. Exact completed
replacement proof and private completed-envelope archival remain available.
A partial durable identity conflict is explicit operator attention. Its exact
input/curation fingerprint prevents another unchanged graph pass; neither
KeepAlive nor a faster retry cadence is added. The original900s schedule remains.

The consumer must be installed and accepted before activating the new producer.
See `scripts/native-cluster/README.md` for persistence, reader and scope contracts.

## Identity quarantine v2

A fresh partial provider receipt remains eligible; complete=1 is not required for
an isolated unavailable-item scope. The native planner now quarantines closed
current-identity components and can publish unrelated candidates. Retained-only
analysis is reported as identity_quarantined, not combined_published or full
coverage. It never qualifies a full-scope zero-task cleanup. Pending-first broker
reuse, exact source-clock checks, release intent, private archive recovery and the
16-envelope cap are unchanged. Only the native producer can decide admission.

The maintenance job retains its 900-second cadence. Native no-op signatures cover
current graph/curation/quarantine state; no timer resolves an identity conflict.
The v2 wrapper uses a separate cache directory so previous failed-attempt and
complete-cache evidence stays intact through deployment.

When code and maintenance become due together, maintenance probes the existing
controller lock and handoff journal before reading the core boundary. It can
wait up to 120 seconds for one ongoing controller handoff, then up to 120 seconds
for the ordinary core receipt, with a 240-second total ceiling. Without a
handoff, the original 120-second wait applies. A second handoff or expired wait
returns a dated `waiting_core_boundary` reason and uses normal cadence. This does
not restore, stop, or force the core, and holds no controller lock while waiting.
The five-minute source freshness limit and five-second receipt alignment remain
unchanged, as do provider/graph/write budgets. Pending broker work remains first.
A completed pending result waits for the same boundary only when a controller
handoff is actually active; an idle controller keeps the existing fast path.
