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
