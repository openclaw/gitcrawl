# Test cleanup ledger

## Test-only CLI helpers

- Removed the unused `exitErr` constructor and its test of an arbitrary exit
  code 7. CLI command tests still verify real usage and runtime exit codes.
- Removed `firstMarkdownLink`, `selectedColor`, and `selectedFG`, which had no
  production callers, along with their isolated display-helper assertions.
- Retained the active Markdown link extraction, menu palettes, rendered-row
  style tests, and command behavior tests.

This removes test-maintained dead code without changing CLI or TUI behavior.

## Invalid CLI arguments

- Consolidated the overlapping `TestCLIAppUsageBranches` argument table into
  `TestAppOutputModesAndUsageBranches`, removing 13 exact duplicate invocations
  while preserving all 101 existing unique cases.
- Removed the weaker test's missing-config setup. The retained table uses a
  temporary initialized archive and now requires usage exit code 2, so unrelated
  config or archive errors cannot satisfy invalid-argument assertions.
- Added three matching command cases for the shared clustering-shape parser.
  The stronger assertions exposed raw parse errors in `cluster` and `refresh`;
  both now classify those errors as usage errors.
- Isolated the touched test's home and XDG paths under its temporary directory.
- Retained the separate empty-archive runtime-error cases and output-format checks.

This is a bounded argument-test cleanup, not a complete repository audit.
Invalid clustering-shape arguments now return usage exit code 2 instead of
runtime exit code 1. No release artifacts changed.

## Launcher status mapping

- Removed the `legacy-sync-api` manifest entry: its advertised `sync-status`
  command only returns a not-implemented error and was never an alias.
- Strengthened the existing metadata/status test to reject that entry and require
  the supported read-only `status --json` invocation. Reused its temporary archive
  instead of adding a second fixture.
- Isolated the test's home and XDG paths so missing-config status checks use
  temporary default paths.
- Kept the documented future `sync-status` reservation and unrelated commands.

Launcher metadata changes; the supported status command and archive data do not.

## Test-only portable adapters

- Removed `validatePortableArchive` and `stagePortableSQLiteSourceTemp`, whose
  only callers were tests. Production uses their context-aware implementations.
- Updated all nine test/fixture calls to use those same implementations with
  the test context instead of an adapter that substituted `context.Background()`.
- Retained archive size/hash and payload validation, split archive checks,
  portable refresh and transition fixtures, and cancellation checks.

No archive format, staging behavior, or release artifacts changed.

## Test-only embedding adapter

- Removed `embeddingTextForBasis`, whose only callers were tests. Production
  task generation uses `embeddingTextForBasisWithMeta` to retain truncation data.
- Updated unsupported-basis, empty-summary, and canonical-text assertions to
  call that same implementation. Removed the error-only raw-text assertion:
  the canonical-text test already requires the exact selected document text.
- Kept task selection, existing-hash/force, truncation, Unicode-byte-cap, and
  summary-availability coverage.

No embedding selection, text, hashing, or release behavior changed.

## Finite vector comparisons

- Replaced tolerance rejection checks with explicit in-tolerance requirements
  in scalar cosine, prepared cosine/kernel comparisons, the immutable-input
  regression, and neighbor-score comparison. A NaN difference now fails rather
  than satisfying neither side of an ordered comparison.
- Kept all existing vector scenarios and tolerances. Scalar correctness,
  prepared/dispatch equivalence, input ownership, and query ranking exercise
  distinct contracts; none is removed as a duplicate.
- Removed redundant one-sided NaN guards where the difference check now rejects
  non-finite values from either implementation. No assertion framework added.

Validation: temporary Go overlays compare NaN failure probes against the old
and updated tests; vector race tests, full Go tests/static gates, independent
P0–P3 review, and PR CI cover the candidate. No production or live archive changes.

## Test-only viewport adapters

- Removed `clusterVisibleStart` and `memberVisibleStart`, whose only callers
  were mouse-selection tests. Rendering and mouse selection use the stored
  `clusterOff` and `memberOff` values maintained by `keepVisible`.
- The tests now inspect those actual offsets and render each pane before
  clicking. They require the first visible fixture row and exclude the
  preceding row, then retain the first-row mouse-selection assertions.
- This checks agreement between scrolling, rendering, and hit testing without
  keeping separate viewport computations solely for tests.

No terminal layout, mouse behavior, archive data, or release artifacts changed.

## Test-only portable rebuild adapter

- Removed `rebuildPortableCompatibilityThreads`, called only by store tests.
  Production portable pruning uses the implementation with options and stats.
- Updated the seven existing calls to that same implementation with exactly
  the adapter's empty options and fresh stats. No rebuild scenario was removed.
- Retained SQLite proofs for autoincrement, dependent views, index retention,
  trigger/FK rejection, transaction rollback, and cancellation. Progress and
  injected-failure tests already use the options-aware implementation.

No portable schema, pruning, rollback, or release behavior changed.

## Control output smoke consolidation

- Replaced the shallow `TestCLIAppHumanAndLogOutputE2E`; it runs the app in
  process, not an executable or provider workflow. Keep it named as a control
  output integration test and require command headers plus decoded payloads.
- Metadata must identify gitcrawl; status and doctor must reflect the two-thread,
  one-repository synthetic archive. Doctor log output must retain those counts,
  rather than satisfying the old single `=` assertion.
- Consolidated version and portable-help routes into the existing output-mode
  table; retain both JSON flag forms and require the actual version value.
  Existing TUI-help and configure-log checks replace their weaker duplicates.
- Renamed the archive fixture helper: it seeds SQLite and installs no GH shim.
  Isolate HOME and XDG config/cache/data/state roots before init; remove the
  later cache-path override now that initialization is already isolated.

No CLI formatting, provider, archive, or release behavior changed.

## Vector query test surface

- Removed the test-only `ListThreadVectors` adapter. Production callers use
  `ListThreadVectorsFiltered`; vector round-trip and migration tests now do too.
- Consolidated a duplicate listing assertion into the existing round-trip test,
  and removed its unused second vector fixture.
  Keep its default-backend assertion on the existing by-number query.
- Reuse the two-basis fixture to check repository, model, basis, dimension, and
  closed-thread selection through the production listing API. Retain migration,
  binary decoding, thread-ID lookup, and scope-count coverage.

No query implementation, archive schema, or CLI behavior changed.

## Cluster save modes

- Removed the test-only `SaveDurableClusters` API and its unused combination of
  member pruning without cluster retirement. Move the 24 fixture calls to the
  active complete-save API, with the omission-preservation call using partial.
- Collapse the shared writer's coupled pruning/retirement flags into one
  completeness flag. Complete, partial, subset, and scoped callers retain their
  existing behavior and legacy-preservation choices.
- Keep override replay, representative selection, legacy cleanup, retirement,
  rejection/rollback, closed-store, and CLI/TUI proofs. Extend the existing
  partial-member fixture to require removal on a subsequent complete save.

No archive schema or production save-mode semantics changed.

## Native graph reference validation

- Restored the missing `NativeOriginalGraphOracle` entrypoint behind the native
  overlay build tag. It calls the existing Go graph builder with the exact
  reference-test parameters; ordinary builds do not gain an API.
- Added a temporary-overlay runner for the existing synthetic store and native
  command tests, plus exporter/reader compilation. Select their actual test
  names without rerunning the ordinary store suite or the opt-in volume and CPU experiments.
- Run this gate in Linux and macOS CI. Keep graph/cache equivalence, lifecycle,
  admission, quarantine, rollback, and recovery proofs; no test is weakened.

No live archives, producer deployment, controller operation, or release changes.

## Obsolete native identity conflict checks

- Removed `checkIdentityConflict` and its dedicated test: production never
  called this receipt reader, so the test did not prove unchanged-graph skipping.
- Removed the test-only `NativeCheckClusterPlan` preflight API. The subset fixture
  now calls the actual `NativePlanClusters` path and requires ambiguous selected
  IDs and both prior identities to be quarantined with no publication targets.
  Independent candidates must remain publishable.
- Keep the actual publication, quarantine, rollback, and recovery proofs, along
  with conflict diagnostic writing recognized by historical maintenance clients.

No archive schema, controller, deployment, or active publication behavior changed.

## Test-only pull request cache wrapper

- Removed `UpsertPullRequestCache`, whose callers were all test fixtures. Tests
  now call the production `UpsertPullRequestCacheFamilies` writer directly.
- Keep the same five enabled hydration families in every migrated call, including
  empty input semantics. The all-family selection lives in store tests only.
- Retain cache replacement, tombstone, workflow ordering, migration, portable
  pruning, coverage, and transactional rollback checks.

No production writer behavior or archive schema changed.

## Shared deterministic reference-edge fixture

- Removed the two standalone local-reference and cross-repository qualified
  reference fixtures. The existing reference table already covered the same
  local edge; it now retains the qualified cross-repository rejection too.
- Rename the table to cover reference scope generally, keeping all nine existing
  rows and adding the former standalone rejection as its tenth row.
- Keep orthogonal issue/PR vectors so references must supply the edge, retain
  exact edge/cluster counts, and require the positive cluster to contain exactly
  the seeded issue and pull request. Each row uses its own temporary SQLite store.

No production graph behavior or archive schema changed.

## Review pagination cursor fixtures

- Consolidated duplicate empty/repeated review-thread cursor fixtures into one
  table, retaining the real HTTP client, bounded loop guard, deadline, error
  checks, and exact request counts.
- Reuse the nested comment-pagination fixture for empty and repeated cursors.
  The empty nested cursor previously had no rejection proof in these tests.
- Require errors to identify the affected pagination level; keep the comment
  request's thread ID and cursor checks, and all successful pagination tests.

All HTTP responses are synthetic and local. Production pagination is unchanged.

## Provider HTTP fixture failures

- Replaced 43 fatal request assertions in GitHub/OpenAI HTTP fixture callbacks
  with nonfatal errors, HTTP 400, and immediate returns. Every request condition,
  diagnostic, successful response, pagination count, vector check, and root-test
  assertion remains intact.
- Changed two fatal response-encoding assertions (including the shared rate-limit
  fixture writer) to nonfatal errors and returns. A failed response write may
  already have committed headers, so these paths do not attempt a second response.
- Forced GitHub authorization, embedding dimensions, and summary path failures
  reproduce connection EOF before the fix and retain their diagnostics with
  HTTP 400 afterward. Full Go/static gates, provider race tests, independent
  review, and PR CI validate synthetic local fixtures; no live APIs or releases.

## CLI HTTP fixture failures

- Replaced 29 fatal request assertions in CLI HTTP fixture callbacks with
  nonfatal errors, HTTP 400, and immediate returns. Preserve every request
  condition, diagnostic, successful response, persisted archive check, and
  root-test fatal assertion.
- Report the response-encoding failure nonfatally and return without attempting
  another response, since headers may already be committed. A forced write error
  confirms the real refresh handler now returns while retaining its diagnostic.
- Forced refresh, summarize, and stale-cache search failures reproduce EOF
  before the fix and keep their original diagnostics with HTTP 400 afterward.
  Race checks cover all 15 affected CLI tests; full Go/static gates, independent
  code review, and PR CI validate the branch. Local checks after the baseline
  probes use temporary homes/configs/caches and SQLite archives; no live providers
  or releases.

## History and coverage fixture failures

- Return errors from the shared coverage-row mapper instead of invoking fatal
  assertions from HTTP handler goroutines. Both publish fixtures report the error,
  return HTTP 400, and stop before assigning the decoded coverage datasets.
- Keep all coverage columns, numeric conversions, counts, completeness checks,
  and successful publish/resume assertions. The mapper no longer needs a test
  context or a nested assertion closure.
- Replace two fatal history request-decoder assertions with nonfatal errors,
  HTTP 400, and immediate returns; retain successful payloads and root assertions.
- Temporary malformed-row and request overlays exercise the real publish, resume,
  and history clients: original failures produced connection EOF; repaired paths
  preserve diagnostics and return HTTP 400. Full Go/static gates, scoped race
  checks, independent review, and PR CI validate synthetic local fixtures only.

## Portable database cancellation fixture

- Replace the incomplete cancellation-only SQLite fixture with the repository
  and metadata tables required by the tested helpers. Every operation must first
  succeed with an active context, then return an error wrapping `context.Canceled`.
- Preserve all eight helper cases. The old generic-error assertion accepted
  missing-table errors in repository lookup, repository verification, and metadata
  writes, even when those helpers discarded the caller's context.
- Temporary production overlays confirm those three mutations pass the original
  table; all eight context-discarding helper mutations fail the repaired table.
  Full Go/static gates, portable-package race tests, independent review, and CI
  validate synthetic temporary databases only. Production behavior is unchanged.

## Inert neighbor golden fixture

Removed the combined limit/threshold golden case and its byte-identical copy of
the default JSON fixture. Neither flag affected its two results. Retain the
other four output goldens; add independent limit and threshold cases to the
existing filter table, each requiring one exact ID instead of the default two.

Temporary flag-discarding overlays pass the original golden suite and fail
the corresponding new rows. Focused race/static checks, review and full CI
validate synthetic archives only. Production neighbor behavior is unchanged.

## Unimplemented command stubs

Removed eleven unavailable command-name mappings, their error-formatting helper,
and its invented-command unit test. The normal unknown-command usage path now
handles those names with exit code 2. Keep the existing global-command assertion,
update the roadmap text and changelog, and preserve supported command dispatch.

Built-CLI checks cover all eleven refusals with temporary homes; focused race
tests, static checks, independent review and full CI validate the change.

## Unused test-table data

Removed the unread `jsonShape` and `readOnly` fields, including three inert
boolean fixture values. Removed three global-command version rows already
covered by `TestAppOutputModesAndUsageBranches`, whose JSON version assertion
checks the value as well as the key. Retain all distinct help, usage-error,
SQLite durability and read-only access cases. Production behavior is unchanged.

An AST audit inspected 292 anonymous test tables across seven Go repositories.
Focused race tests, a temporary wrong-version overlay, static checks, independent
review and platform CI validate the retained assertions with synthetic data.

## CLI JSON document assertions

Replace substring-only checks in the three JSON output-mode rows with complete
JSON decoding and expected value checks. Keep the same commands and all text,
log and usage-error cases. No production change or new fixture is needed.

Temporary JSON writer overlays adding a non-JSON prefix or a second document
pass the original rows; each is rejected by all three repaired rows under the
race detector. Built-CLI checks, focused race/static gates, independent review
and platform CI validate isolated temporary configuration only.

## Unsupported empty embedding bases

Replace the legacy-only embedding guard with the store's supported-basis check.
Move the weak legacy-basis error assertion into a focused empty-repository test
covering all three supported bases, the legacy name and an external vector basis.
Clustering with external vectors remains supported; empty runs make no API calls.

Built-CLI checks reproduce false success before the fix; the new regression
fails on the original guard. Validate corrected CLI behavior, embedding and
custom-vector race tests, static checks, independent review and full platform CI
with temporary archives and synthetic credentials only.

## Maintenance suite collection

Move shared cycle setup into a fixture-only mixin so each of the 18 maintenance
test methods is collected once. Discovery previously ran 46 cases by inheriting
seven base tests into four other classes. Preserve every method body and helper.

Move the direct runner after all classes: it previously stopped before five
availability/quarantine tests were defined. Both direct and discovery runners
now execute the same 18 unique cases. Validate the complete maintenance Python
suite, static checks, independent review and CI with temporary fixtures only.

## Native script CI coverage

Add native code and maintenance unittest discovery to the existing native-script
CI step on Linux and macOS. The 10 code lifecycle/upstream cases and 60 maintenance
cases previously had no workflow invocation. Keep the cluster overlay check and
all test bodies unchanged; each suite runs in its own Python process.

Validate both suites with isolated temporary home/config/cache directories and
local Git fixtures, then run static checks, independent review and platform CI.
No provider, deployed lifecycle controller or live archive is used by the tests.

## Native producer pipe cleanup

Strengthen the existing wrong-SHA lifecycle case to check both an already-exited
producer and one still waiting for its commit gate. Inspect real subprocess pipe
handles directly instead of relying on garbage-collection ResourceWarnings.
Both cases fail on the original controller because stdout remains open.

Close stdout and stderr in the producer cleanup path, preserving termination,
binding rejection and owner restoration. Validate all native-code cases with
isolated home/config/cache state and local Git fixtures, static checks,
independent review and platform CI. No deployed controller or live archive runs.

## Cluster producer pipe cleanup

Cover early receipt rejection in the sibling cluster lifecycle controller with
real temporary producer subprocesses. Both exited and running children leave
stdout and stderr open before the fix. Check process exit, both closed handles,
no owner-lifecycle calls and the preserved failure receipt.

Close both pipes in the cleanup path and run the new unittest discovery command
in Linux/macOS CI alongside the existing overlay check. Validate the regression,
all native script suites, overlay tests, static checks and independent review.
Fixtures use temporary state only; no deployed controller or archive is used.

## Maintenance SQLite cleanup

Use an explicit closing scope for read-only vector verification. A SQLite
transaction context commits or rolls back without closing its connection. Close
the synthetic coverage fixture connection too, preserving its commit scope.

Add one parameterized real-SQLite case for matching and changed vector bindings.
Verify success/checkpoint metadata and mismatch rejection before checking that
the actual connection rejects further queries. Both closure checks fail before
the fix. Validate native Python suites with ResourceWarnings enabled, static
checks, independent review and platform CI using temporary state only.

## Unimplemented code hydration flags

- Remove the discarded `include-code` flag registrations from sync and refresh.
  Neither command implemented code hydration; accepting the option silently
  ignored its requested behavior. Document the required invocation cleanup.
- One CLI regression exercises both commands with bare, true, and false forms.
  Reject with usage exit 2 before configuration, credential lookup, runtime
  files, or stage output. All six cases fail on the old registrations.
- Preserve supported sync hydration and refresh stages. Validate with focused
  CLI race tests, module/static checks, independent review, real CLI argument
  rejection, and CI; synthetic paths only, no providers or releases.

## Ignored gh-style search options

- Remove the discarded match, sort, and order registrations and normalization
  mappings from gh-style search. Reject unsupported requests instead of silently
  claiming parity; document removal from existing invocations.
- Add six rows to the existing usage table, reusing its seeded SQLite archive
  and exit-code assertions. All six report success before the fix, then reject
  with usage exit 2. No new test harness or fixtures.
- Preserve supported local search controls and native cluster sorting. Validate
  with focused CLI race tests, real CLI rejection, static checks, independent
  review, and CI; synthetic data only, no providers or releases.

## Test-maintained forwarding APIs

- Remove document Build/BuildWithComments, cluster Build, and vector Query:
  internal forwarding APIs retained only by tests and a benchmark. Production
  and native-script consumers use BuildWithContext, BuildWithOptions, and
  QueryWithOptions instead.
- Move the existing callers onto those implementations with the same default
  inputs. Keep every fixture and behavioral assertion; vector tests and the
  benchmark now fail on returned errors instead of silently discarding them.
- Preserve native code/cluster APIs and the independent scalar Cosine oracle.
  Validate affected packages with race tests, module/static checks, independent
  review, and CI. No public module API, providers, live archives, or releases.

## Vector helper process failure status

- Move the helper's success exit from a defer to its normal completion path.
  A panic after writing valid JSON must fail the process and its parent bridge
  test; the deferred success exit previously concealed that failure.
- Preserve request validation, the response, and every existing test body.
  Healthy completion still exits before the test runner can append output.
- Reproduce the false-green parent test with a temporary post-response panic,
  then verify failure after the fix and healthy race-enabled vector tests.
  Validate static checks, independent review, and CI. No production behavior,
  provider calls, live archives, or releases change.

## Cluster edge parity rejects NaN

- Require score deviation to be within tolerance instead of only rejecting
  deviations above it. NaN satisfies neither comparison; the old check silently
  accepted NaN edge scores despite the finite scalar oracle.
- Preserve all 96 synthetic nodes, special input vectors, five thresholds,
  edge-count and endpoint assertions, scalar oracle, and the 1e-12 tolerance.
- A temporary overlay replacing observed scores with NaN passes before the
  change and fails afterward. Validate the normal parity case with the race
  detector, static checks, independent review, and CI. No production behavior,
  provider calls, live archives, or releases change.

## Test-only repository scope forwarding method

- Remove RestrictPortableRepository, an internal forwarding method used only
  by eight test calls. Point those calls at RestrictPortableRepositoryWithOptions
  with the same zero-value options; keep strict foreign-key validation enabled.
- Preserve all fixtures, assertions, cancellation, rollback, future-schema and
  candidate-generation checks. Only calls change in the three test files;
  the production scoping implementation is unchanged.
- Validate the same 27 real temporary-SQLite cases before and after with the
  race detector, static checks, independent review, and CI. No public module
  API, providers, live archives, or releases change.

## Native cache rejection reasons

- Require the four malformed pair-cache cases to reach their intended
  completeness or edge-shape rejection. Previously any error, including a
  missing file or invalid JSON fixture, counted as successful validation.
- Check the saved healthy cache and all setup reads, encodes, writes and final
  directory listing in that test. Preserve the graph fixture, four mutations,
  exclusion invalidation and cache-removal assertions; no production change.
- Temporary Go overlays demonstrate the false greens before the change and
  rejection afterward. Validate healthy cases with the race detector, the
  native overlay suite, static checks, independent review and PR CI. No live
  archives, provider calls, deployment or releases.

## Native bridge SQL assertion errors

- Check all six native bridge assertion-query results before comparing counts,
  timestamps or convergence state. Empty-table and before/after checks previously
  accepted zero values after failed reads, hiding a broken SQL oracle.
- Pass the active test to the shared empty-table assertion so failures belong
  to the calling subtest. Preserve all real SQLite fixtures, import mutations,
  preview/no-write, idempotency, writer and large-packet checks.
- Disposable Go overlays inject missing-table and missing-column reads to show
  the old false greens and targeted failures after the change. Validate healthy
  cases with the race detector, the native overlay suite, static checks,
  independent review and PR CI. No production or live archive changes; no releases.

## Owner-purge SQL assertion errors

- Check all 20 previously ignored assertion-query results in the owner-purge
  tests before comparing retained evidence, empty tables, rollback state or
  coverage. Failed reads must not masquerade as empty results or equal strings.
- Preserve every SQL literal, purge/refusal/migration fixture, mutation and
  expected value. Keep assertion-query errors local so expected purge errors
  are not overwritten.
- Disposable Go overlays inject broken empty-result and before/after reads to
  reproduce the false greens, then verify targeted failures. Validate healthy
  real SQLite cases with the race detector, static checks, independent review
  and PR CI. No production code, live archives, deployment or releases change.

## Analytics-integrity SQL assertion errors

- Check all 16 previously ignored assertion-query results before comparing
  recovery history, membership, migration evidence or coverage. Failed reads
  must not reuse a prior membership value or masquerade as an empty result.
- Remove the unrelated coverage fixture from the repository-before-limit test;
  preserve its scoped receipt and 60 newer receipts from another repository.
  Keep every SQL literal, remaining fixture, mutation and expected value.
- Disposable Go overlays reproduce false greens for rejected-membership,
  incomplete-review and read-only empty-coverage reads. Confirm healthy cases
  with the race detector and verify the repository-ordering regression still
  fails after fixture removal. Run static checks, independent review and PR CI.
  No production code, live archives, deployment or releases change.

## Review-sync fixture identities and preservation reads

- Remove two redundant identity readbacks, JSON parsing and fallback handling
  from the review-only sync test. Use the same PR8/R1 identities already written
  by its fixture, and always exercise the wrong-node rejection assertion.
- Check all seven retained-data assertion queries and snapshot column/iteration
  errors. Failed reads must not reuse membership or resolution values, compare
  two default revision counts, or certify incomplete canonical snapshots.
- Keep the real temporary SQLite sync path, six snapshot tables, partial-response
  and cancellation cases, repository rejection and transactional rollback checks.
  Disposable race-enabled Go overlays reproduce failed-read false greens and
  verify node-identity and canonical-mutation regressions are still rejected.
  Run static checks, independent review and PR CI. No production code, live
  archives, deployment or releases change.

## Purge helper inputs

- Remove the ignored reason-label argument from the purge test helper and all
  seven callers. These labels never reached the purge plan or mutation.
- Remove its testing handle and inert Helper call: the helper neither reports
  test failures nor passes the handle onward. Keep plan creation, error returns
  and plan-ID-based application unchanged, with every fixture and assertion.
- Validate the existing purge cases against temporary SQLite archives with the
  race detector, static checks, independent review and PR CI. No production
  code, live archives, deployment or releases change.
