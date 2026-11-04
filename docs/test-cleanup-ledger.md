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
