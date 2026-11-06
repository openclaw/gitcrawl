# Test cleanup ledger

Keep the outcome and its originating PR here. Detailed proofs and historical
validation stay in the linked PRs. Entries describe bounded changes, not a claim
that every test is E2E.

| Change | Outcome and retained coverage |
| --- | --- |
| [Test-only CLI helpers](https://github.com/openclaw/gitcrawl/pull/293) | Removed unused CLI/TUI helpers. Retained active link extraction, palette and command checks. |
| [Invalid CLI arguments](https://github.com/openclaw/gitcrawl/pull/288) | Consolidated 13 duplicate invocations. Retained 101 unique cases and required usage exit 2 with valid temp config. Fixed clustering parse classification. |
| [Launcher status mapping](https://github.com/openclaw/gitcrawl/pull/292) | Removed advertised but unimplemented sync-status alias. Retained supported read-only status mapping. |
| [Test-only portable adapters](https://github.com/openclaw/gitcrawl/pull/295) | Removed validation/staging wrappers. Retained context-aware archive and cancellation checks. |
| [Test-only embedding adapter](https://github.com/openclaw/gitcrawl/pull/298) | Removed text wrapper and weaker raw-text assertion. Retained canonical text, hashing, truncation and task-selection checks. |
| [Finite vector comparisons](https://github.com/openclaw/gitcrawl/pull/305) | Made tolerance checks reject NaN. Retained scalar oracle, dispatch equivalence and immutable-input scenarios. |
| [Test-only viewport adapters](https://github.com/openclaw/gitcrawl/pull/310) | Removed duplicate offset calculations. Test rendered rows and mouse hit testing against actual viewport offsets. |
| [Test-only portable rebuild adapter](https://github.com/openclaw/gitcrawl/pull/311) | Removed rebuild wrapper. Retained seven fixtures through the options-aware implementation and rollback/cancellation checks. |
| [Control output smoke consolidation](https://github.com/openclaw/gitcrawl/pull/312) | Renamed in-process E2E smoke as integration. Consolidated weaker duplicates and required decoded payloads and real archive counts. |
| [Vector query test surface](https://github.com/openclaw/gitcrawl/pull/315) | Removed listing adapter and duplicate fixture. Retained production query filters, migration and vector round trips. |
| [Cluster save modes](https://github.com/openclaw/gitcrawl/pull/316) | Removed test-only coupled save mode. Retained complete/partial/subset/scoped behavior and omission/removal proofs. |
| [Native graph reference validation](https://github.com/openclaw/gitcrawl/pull/317) | Restored build-tagged graph oracle. Added Linux/macOS native overlay gate excluding opt-in volume/CPU experiments. |
| [Obsolete native identity conflict checks](https://github.com/openclaw/gitcrawl/pull/318) | Removed uncalled receipt reader and test-only preflight. Use actual planner and retain publication/quarantine/recovery checks. |
| [Test-only pull request cache wrapper](https://github.com/openclaw/gitcrawl/pull/319) | Removed all-family forwarding writer. Retained replacement, tombstone, ordering, migration and transactional checks. |
| [Shared deterministic reference-edge fixture](https://github.com/openclaw/gitcrawl/pull/320) | Consolidated two standalone fixtures into ten scope cases with exact issue/PR membership and orthogonal vectors. |
| [Review pagination cursor fixtures](https://github.com/openclaw/gitcrawl/pull/321) | Consolidated cursor rejection fixtures. Retained HTTP requests, deadlines, exact counts and nested pagination diagnostics. |
| [Provider HTTP fixture failures](https://github.com/openclaw/gitcrawl/pull/322) | Replaced fatal assertions in handler goroutines with nonfatal diagnostics and HTTP rejection. Retained successful provider fixtures. |
| [CLI HTTP fixture failures](https://github.com/openclaw/gitcrawl/pull/323) | Repaired handler failure reporting without EOF masking. Retained persisted archive and root assertions. |
| [History and coverage fixture failures](https://github.com/openclaw/gitcrawl/pull/324) | Propagate mapper/decoder errors to HTTP handlers. Retain coverage fields, completeness, publish/resume and history assertions. |
| [Portable database cancellation fixture](https://github.com/openclaw/gitcrawl/pull/325) | Added required schema and healthy controls. Require context.Canceled rather than arbitrary missing-table errors. |
| [Inert neighbor golden fixture](https://github.com/openclaw/gitcrawl/pull/326) | Removed byte-identical ineffective limit/threshold golden. Retained four goldens and independent exact-ID filter cases. |
| [Unimplemented command stubs](https://github.com/openclaw/gitcrawl/pull/327) | Removed eleven unavailable dispatch mappings and formatting helper. Unknown-command usage exit 2 now applies. |
| [Unused test-table data](https://github.com/openclaw/gitcrawl/pull/328) | Removed unread jsonShape/readOnly fields and duplicate version rows. Retained unique usage, durability and read-only cases. |
| [CLI JSON document assertions](https://github.com/openclaw/gitcrawl/pull/329) | Decode whole JSON documents and check values. Reject prefixes and extra documents while retaining text/log checks. |
| [Unsupported empty embedding bases](https://github.com/openclaw/gitcrawl/pull/331) | Use supported-basis guard before task selection. Retain supported empty bases and reject legacy/external embedding bases. |
| [Maintenance suite collection](https://github.com/openclaw/gitcrawl/pull/333) | Use fixture-only mixin and move direct runner after classes. Collect each maintenance case once in both runners. |
| [Native script CI coverage](https://github.com/openclaw/gitcrawl/pull/335) | Add native code and maintenance Python discovery to Linux/macOS CI. Retain separate processes and existing tests. |
| [Native producer pipe cleanup](https://github.com/openclaw/gitcrawl/pull/336) | Close producer pipes after early rejection. Test exited and gate-waiting children, binding refusal and owner restoration. |
| [Cluster producer pipe cleanup](https://github.com/openclaw/gitcrawl/pull/337) | Close sibling cluster producer pipes. Verify process exit, handles, failure receipt and no owner lifecycle calls. |
| [Maintenance SQLite cleanup](https://github.com/openclaw/gitcrawl/pull/338) | Explicitly close verification connection. Real SQLite match/mismatch tests check checkpoint behavior and closed handles. |
| [Unimplemented code hydration flags](https://github.com/openclaw/gitcrawl/pull/339) | Reject discarded include-code flags in sync/refresh before config or credentials. Preserve supported stages. |
| [Ignored gh-style search options](https://github.com/openclaw/gitcrawl/pull/340) | Reject ignored match/sort/order flags. Retain supported local search and clustering controls. |
| [Test-maintained forwarding APIs](https://github.com/openclaw/gitcrawl/pull/341) | Remove document/cluster/vector forwarders kept by tests. Retain fixtures using active implementations and check returned errors. |
| [Vector helper process failure status](https://github.com/openclaw/gitcrawl/pull/342) | Remove deferred success exit that masked post-response panic. Retain normal early exit and bridge response checks. |
| [Cluster edge parity rejects NaN](https://github.com/openclaw/gitcrawl/pull/343) | Require finite in-tolerance scores. Retain 96-node scalar oracle, five thresholds, endpoints and 1e-12 tolerance. |
| [Test-only repository scope forwarding method](https://github.com/openclaw/gitcrawl/pull/344) | Remove repository scoping forwarder. Retain eight calls through options-aware implementation and strict FK checks. |
| [Native cache rejection reasons](https://github.com/openclaw/gitcrawl/pull/346) | Require intended completeness/edge-shape errors and checked setup. Retain healthy cache, exclusion invalidation and removal proofs. |
| [Native bridge SQL assertion errors](https://github.com/openclaw/gitcrawl/pull/347) | Check six assertion reads before comparing values. Retain temporary SQLite import, replay, preview and writer checks. |
| [Owner-purge SQL assertion errors](https://github.com/openclaw/gitcrawl/pull/348) | Check 20 assertion reads. Retain purge/refusal/migration fixtures and exact rollback/preservation values. |
| [Analytics-integrity SQL assertion errors](https://github.com/openclaw/gitcrawl/pull/349) | Check 16 assertion reads. Remove unrelated coverage fixture while retaining cross-repository receipt ordering. |
| [Review-sync fixture identities and preservation reads](https://github.com/openclaw/gitcrawl/pull/350) | Use known fixture identities and check seven reads plus snapshot iteration. Retain wrong-node, partial-response and rollback checks. |
| [Purge helper inputs](https://github.com/openclaw/gitcrawl/pull/351) | Remove ignored reason labels and unused testing handle. Retain all seven plan/application calls and assertions. |
| [Native-cluster SQL fixture and assertion errors](https://github.com/openclaw/gitcrawl/pull/352) | Check nine fixture writes and eight assertion reads. Retain replay, rollback, policy/curation and opt-in measured cases. |
| [Analytics CLI fixture and assertion errors](https://github.com/openclaw/gitcrawl/pull/353) | Check SQL/state reads and writes. Retain local HTTP, reopen retry, migration and bounded scheduler checks. |
| [Native code producer fixture errors and CI coverage](https://github.com/openclaw/gitcrawl/pull/354) | Check SQL, chmod and close errors. Add separate native-code overlay pass while preserving cluster overlays and experiments. |
| [Analytics absence assertions require successful reads](https://github.com/openclaw/gitcrawl/pull/355) | Check nine reads before certifying absent evidence. Retain partial-response, owner-exclusion, cancellation and peer-isolation cases. |
| [Remove discarded source-fixture serialization](https://github.com/openclaw/gitcrawl/pull/356) | Remove discarded JSON encoding/import. Retain all three SQLite evidence, repair/replay and actor-identity tests. |
| [Native rollback assertions require successful reads](https://github.com/openclaw/gitcrawl/pull/357) | Check ten subset/quarantine reads before absence or preservation claims. Retain triggers, recovery and optional fixture exports. |
| [Successful sync watermark selection](https://github.com/openclaw/gitcrawl/pull/361) | Check failed-only, success, newer failed, completed and checkpoint transitions. Retain production SQL and reject two previously undetected filter faults. |
| [Closed-store error assertions](https://github.com/openclaw/gitcrawl/pull/363) | Remove three optional-schema probes that do not report closed-store errors. Require each of 14 operations to succeed on its own healthy fixture and report database closure afterward. |
| [Forced summary selection](https://github.com/openclaw/gitcrawl/pull/364) | Require the forced task to identify the already summarized thread and revision. Retain the older pending-task limit and stale-revision checks. |
| [Empty test scaffolding](https://github.com/openclaw/gitcrawl/pull/365) | Remove the config test hook with no setup or teardown. Inline six HTTP-server helper calls and retain each handler, assertion and server close. |
| [Stale workflow snapshot replay](https://github.com/openclaw/gitcrawl/pull/366) | Replace a goroutine that waits until deletion completes with a direct stale replay. Retain skipped-write, run-membership and reservation-sequence assertions. |
| [Allocator migration source maxima](https://github.com/openclaw/gitcrawl/pull/367) | Exercise negative-thread, revision, child-reservation and original workflow maxima. Require the migrated floor and next value; retain evidence repair and reservation backfill. |
| [Portable store cancellation controls](https://github.com/openclaw/gitcrawl/pull/368) | Give each of 15 operations a fresh valid fixture. Require typed cancellation followed by healthy success; prepare repository, blob-pointer removal and excerpt columns only where needed. |
| [Permanent change-contract table IDs](https://github.com/openclaw/gitcrawl/pull/369) | Pin the 37 established table names to their persisted IDs. Retain dense/unique IDs and family references; permit appended IDs. |
| [Portable progress fixture consolidation](https://github.com/openclaw/gitcrawl/pull/370) | Share setup across physical-drop and retained-column cases. Preserve fresh stores and the complete six- and twelve-stage ordered lists. |
| [Cluster-reader early refusals](https://github.com/openclaw/gitcrawl/pull/371) | Isolate all five existing writer/provider cases. Require exact usage refusals before config application, credential lookup, output or file creation. |
| [Code-command preflight errors](https://github.com/openclaw/gitcrawl/pull/372) | Require specific usage errors for eight existing invalid inputs and the missing per-file byte limit. Isolate paths and reject credential lookup, output or runtime file creation. |
| [Portable payload cancellation controls](https://github.com/openclaw/gitcrawl/pull/373) | Isolate vacuum, metadata and canonical-schema fixtures. Require context.Canceled and healthy success; reject cancellation errors that lose their wrapped identity. |
| [Legacy config path fixture consolidation](https://github.com/openclaw/gitcrawl/pull/374) | Share setup and five path assertions across platform-default and XDG-override cases. Retain fresh homes and separate environment cleanup for each case. |
| [Publication source evidence reads](https://github.com/openclaw/gitcrawl/pull/375) | Check all four baseline and result reads before comparing timestamps, revisions or actor identities. Retain dry-run, replay, raw evidence and distinct same-login actors. |
| [Quarantine-reader malformed admission proof](https://github.com/openclaw/gitcrawl/pull/376) | Verify three healthy source modes and a one-row fixture update before requiring the specific status/admission refusal. Reject unrelated reader errors; preserve namespace and history checks. |
| [Analytics discovery provider guard](https://github.com/openclaw/gitcrawl/pull/377) | Use valid isolated config and require zero local-provider requests. Retain 24 queued items, the eight-item retry bound and unchanged attempt count; reject swallowed collection failures. |
| [Thread-list number and limit controls](https://github.com/openclaw/gitcrawl/pull/378) | Add competing rows and separate number-only and number-plus-limit cases. Retain the original repository/thread round trip and original selection; reject either ignored option. |
| [Windows config home isolation](https://github.com/openclaw/gitcrawl/pull/379) | Reuse the platform-aware home fixture for normalization and runtime directories. Run the config package on hosted Windows; retain all existing assertions and filesystem checks. |
| Duplicate fresh-schema smoke test ([PR #380](https://github.com/openclaw/gitcrawl/pull/380)) | Remove the version-only fresh-store check. Retain current-store diagnostics that verify the same persisted version plus capabilities and unchanged bytes; keep legacy, portable and empty-path cases. |
| [Guarded core-request serialization](https://github.com/openclaw/gitcrawl/pull/381) | Make fake quota follow dispatched requests and assert that the in-flight request holds the reserve lock. Retain threshold outcomes and probe/request counts; keep the distinct rate-status lock test. |
| [Duplicate GraphQL review-thread fixtures](https://github.com/openclaw/gitcrawl/pull/382) | Remove three overlapping happy-path fixtures after transferring HTTP and exact comment-slice checks to nested pagination and provider-ID coverage. Keep enterprise, retry-body and invalid-cursor cases. |
| [Retry cancellation readiness](https://github.com/openclaw/gitcrawl/pull/384) | Cancel on the observed retry decision and require a real request. Retain the overflow deadline, exact cap and request-count checks with a five-second bound instead of 50 ms. |
| [Production Darwin flush default](https://github.com/openclaw/gitcrawl/pull/385) | Keep the test-binary opt-out check and run a plain executable against in-memory SQLite on Darwin. Verify both flush flags without forcing the policy; retain reconnect and read-only tests. |
| [Default portable body budget](https://github.com/openclaw/gitcrawl/pull/386) | Replace the pre-canceled budget case with successful zero/negative-budget exports. Check literal defaults in result/manifest and Unicode thread/comment excerpts with original lengths; keep snapshot cancellation coverage. |
| [Obsolete generated GitHub-close update](https://github.com/openclaw/gitcrawl/pull/387) | Remove the unused SQL definition, generated method/parameters and sole smoke caller. Keep the public observation-based close path and its positive, repeated-close and validation checks. |
| [Refused exclusive-rename bytes](https://github.com/openclaw/gitcrawl/pull/388) | Require source and destination file sentinels to survive a refused rename, alongside destination identity. Keep directory refusal and subsequent absent-destination publication checks. |
| [GH compatibility helper duplication](https://github.com/openclaw/gitcrawl/pull/389) | Remove the direct shim-message fixture and repeated positive host mappings. Keep the public migration messages and returned-error check, unsupported-error wrapping, invalid URLs, host/config defaults and configured values. |
| [Change-contract test ownership](https://github.com/openclaw/gitcrawl/pull/390) | Require usage errors before invalid-config access for malformed input; check missing-archive runtime errors and non-creation for valid commands. Remove the duplicate missing-action call and move the closed-DB sanitizer check to its owner using a plain SQL handle. |
| [Broad command coverage smoke](https://github.com/openclaw/gitcrawl/pull/391) | Remove the 15-command nonempty-output fixture and redundant cluster-ID tail check. Keep focused payload/state tests, add exact single/mixed URL selection and exercise oldest-sort acceptance in the existing cluster-membership test. |
| [Fixture handle lifetime](https://github.com/openclaw/gitcrawl/pull/392) | Close both version-test pipe ends and purge-test store/lock handles on early failure. Keep explicit writer closure for EOF and lock release before apply; check the release error. |
| Unsupported-basis fallback assertions | Require the actual neighbor and score; keep custom-basis cluster/refresh and strict refusal checks. Move blank-mode keyword behavior to the no-provider search fixture with exact mode/hits and zero calls; remove the seed-ID tail check. [PR 393](https://github.com/openclaw/gitcrawl/pull/393). |
| [Native cancellation error identity](https://github.com/openclaw/gitcrawl/pull/394) | Separate synthetic insert rollback from cancellation in both overlays; remove the trigger, require context.Canceled, preserve the old generation, and verify the same replacement commits with a healthy context. |

The source audit read 183 Go test files and 20 supplemental test, fixture and
ledger paths at `f3fb1c6674829438f1a20d5d3a39ce35ece06e3e`. CLI calls made in process,
localhost HTTP fixtures, temporary Git remotes and SQLite integration tests are
not live-service or real-terminal E2E. Keep distinct failure, rollback, migration,
provenance and output-format contracts even when their setup looks similar.

The native overlay runner excludes opt-in measured CPU/volume experiments. It
runs cluster and code overlays separately. Shared reader fixtures include
native-only, downstream-only and baseline-reference rows. Their JSON metadata
alone does not establish executed provider admission or downstream coverage.
