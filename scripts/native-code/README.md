# Official-main native code producer

The code index follows `https://github.com/openclaw/openclaw.git`, branch `main`.
The previous scheduled producer scanned a development worktree without fetching,
so it could correctly report unchanged while that checkout stayed pinned.

`upstream.py` owns only `<state-dir>/upstream/source.git`, a private bare cache.
It fetches main before owner settlement and retains an immutable metadata receipt
with the exact full commit ID, tree, commit timestamp and observation timestamp.
There is no checkout, reset, source-file execution, or read from a serving Gateway
or development tree. Git configuration/remote and cache ownership are checked.
Fetch failure preserves the previous publication and records failure; a cached
commit is never relabelled as a successful current upstream observation.

The commit scanner reads Git objects directly with `ls-tree`/`cat-file`, validates
blob identities, and applies the existing text/language/size rules. Every document
comes from the one captured commit even if main advances. Worktree state is
irrelevant; committed snapshots have `worktree_dirty=false`. Limits remain
512 KiB per file (larger files skipped), 1 GiB accepted text, 100,000 documents,
45 seconds scan and 90 seconds atomic write. Missing objects, partial reads and
corpus-limit failures refuse the entire publication. The cache has a 4 GiB
admission budget and requires 2 GiB free; it never prunes source archives or
existing recovery material. Reaching that budget is an explicit failed cycle,
not a fresh result. Old owned Git objects remain while cleanup holds apply.

`run_code_update.py` retains the prior native lifecycle: shared controller lock,
preparation while core collection runs, exact no-op without owner settlement,
fsynced handoff journal, brief settlement of the original analytics owner only,
original runner lock and schema16/inode/provider checks, and immediate restoration
of the same analytics job. Its existing journal-only recovery helper remains
compatible. The native SQLite writer opens an existing archive without migration
or creation and retains the delivered FULL/Darwin full-flush policy.

The original `ReplaceCodeSnapshot` transaction publishes snapshot metadata and all
documents together, removing obsolete derived code rows atomically. An interrupted
write leaves the prior generation. The no-op check includes commit, source root,
dirty flag, counts and the complete path/language/content-hash/byte-size set.
There are no schema changes or observation-feed activation. Authoritative H is
the sole destination; writable portable P and all source histories stay separate.

Existing reader fields remain unchanged: `code_snapshots.git_sha`,
`worktree_dirty`, `file_count`, `byte_count`, `indexed_at`, and `code_documents`.
`source_root` is a private cache path. Supplemental `upstream` receipt fields
identify remote, branch, full commit, tree and observation time. Each successful
cycle has its own receipt; `last-code-result.json` provides a code-specific latest
pointer, independent of embedding/cluster receipts sharing the lifecycle directory.
Native code text is not submitted for embedding by this producer.

The isolated CLI/store files use the `native_code_overlay` build tag. Delivery
builds them against the pinned source baseline using the existing held-capture
exclusion overlay; `native_store.go` and its tests map into `internal/store`.
This does not build or install the held source-capture candidate. Install immutable
artifacts outside the checkout and preserve the existing 900-second code cadence,
without KeepAlive. Restore the previous code-job plist/artifact for rollback;
never roll the live database backward.

Focused checks use synthetic repositories and databases:

```
go test ./internal/codeindex -run TestScanCommit
python3 -m unittest discover -s scripts/native-code -p 'test_*.py'
go test -overlay DELIVERY_OVERLAY -tags native_code_overlay ./internal/store -run 'TestNativeProducerCodeNoopAndRollback|TestNativeProducerExistingWriterNeverMigrates|TestNativeCodeWriterFlushAndObsoleteReplacement'
go test -overlay DELIVERY_OVERLAY -tags native_code_overlay ./scripts/native-code/cmd -run TestCodeCommitGate
```

Before live publication, verify the exact reviewed bundle and source receipt,
then retain a native document/count/hash comparison and deleted-path check for
the downstream reader owner. Verify ordinary core progress after restoration.
