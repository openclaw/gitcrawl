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
controller checkpoints release intent, respects `native_request_inflight`, and
replays only idempotent release after interruption. A separate immutable receipt
records release before checksum-checked removal of the exclusive temporary
request/result files. Original completion/native receipts and every archive row
remain intact. Incomplete proof and changed files stay retained.

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
