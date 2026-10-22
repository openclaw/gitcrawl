# SQLite commit durability

Gitcrawl requests `synchronous=FULL` on every writable connection, after the
SQLite driver applies DSN settings. This upgrades Crawlkit's `NORMAL` default
and also covers replacement connections. Existing `FULL` or stronger settings
are retained. The connection-local setting does not change read-only or immutable
access permissions. Journal mode, schema, cursors and collection budgets are unchanged.

With WAL, `FULL` requests a WAL sync before acknowledging each commit. This
strengthens commit retention during operating-system crashes compared with
`NORMAL`, which may lose recent acknowledged transactions. It does not make a
hardware power-loss guarantee independent of filesystem/device flush behavior.

On Darwin, every connection also requests `fullfsync=ON` and
`checkpoint_fullfsync=ON`, including connections already configured for `FULL`
or `EXTRA`. The bundled Unix VFS then requests `F_FULLFSYNC` for commits and
checkpoints; SQLite falls back to `fsync` if the filesystem rejects it. These
connection-local flags preserve read-only/immutable access restrictions. Their
presence and successful disposable writes do not prove physical power-loss
durability.

Go test binaries (`testing.Testing()`) start with the Darwin flush flags off:
per-commit device flushes made disposable fixture suites about 30 times slower
than their deadlines allow. `synchronous=FULL` still applies in tests, and the
durability tests re-enable the Darwin flags to verify the production policy. A
connection to a file SQLite cannot read as a database returns SQLite's native
error to its caller instead of a durability-policy error.

Validate writer settings on the writer connection, not on a separate read-only
connection. Disposable SIGKILL tests should check both sides of commit: preserve
acknowledged multi-table updates and receipts, and roll back interrupted updates
without advancing their receipt frontier. SIGKILL tests prove process-crash
recovery; they do not simulate an OS crash or physical power failure.

The local scheduled installation uses per-user LaunchAgents. Collection starts
at login; hourly metrics runs use their calendar schedule. The analytics owner
uses `KeepAlive`, while periodic producers keep their normal cadence. A separate
recovery-only job watches the durable handoff journal and restores the original
owner without running a code scan, export, model request or cluster calculation.

Preserve database files and WAL/SHM alongside their recovery evidence. A startup
failure is not permission to remove sidecars, reset cursors, replace a writable
portable mirror, weaken authentication, or activate held capture contracts.
