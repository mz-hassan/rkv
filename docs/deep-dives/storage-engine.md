# Storage engine deep dive (`crates/kv-engine`)

**Write path.** Writers enqueue a batch; the writer at the head of an idle queue becomes the *leader*, drains the queue (one batch
for `SyncPolicy::Always`, up to 64 otherwise), appends one WAL record per batch, issues **one fsync for the group**, inserts into
the memtable, publishes results and wakes the rest (group commit; model-checked with loom). A failed append/fsync poisons the DB:
all later writes fail (fsyncgate: after an fsync error the on-disk state is unknown).

**Files.** `NNNNNN.wal` (records `[crc32][len][payload]`; recovery keeps the valid prefix), `NNNNNN.sst`, `MANIFEST-N` (a WAL of
version edits; rewritten as a full snapshot on every open), `CURRENT` (swapped by write-tmp + rename).
SSTable: `[data blocks][filter][index][footer]`, each block `body + crc32`; footer has handles + magic; all handles are bounds-checked
against the file size (BUG-007).

**Keys and MVCC.** internal key = `user_key ++ BE(!ts)`; ordering is user key ascending then ts **descending**, compared as two
fields (never raw bytes: variable-length keys). `get_at(key, ts)` returns the newest version <= ts. Files carry `max_ts` so a point
read can stop as soon as the best hit is newer than anything remaining. The transaction layer does *its own* MVCC in key space
(`txn::keys`), the engine's ts is the Raft index.

**Compaction.** Leveled: L0 -> L1 when 4 files; level i -> i+1 by size score (x10 per level); trivial move when nothing overlaps.
Size-tiered: merge >= 4 runs of similar size inside L0. GC rule per user key (watermark W): keep every version newer than W plus the
newest version <= W; drop a tombstone only at the bottom, which means **no deeper level overlaps the union of input and next-level
file ranges** (BUG-001). Output files are cut only at user-key boundaries.

**Backpressure.** `max_immutables` memtables and `l0_stop` L0 files stall writers; `l0_slowdown` adds 1 ms sleeps; stall time is a metric.

**Crash model.** Everything goes through the `Vfs` trait. `MemFs` keeps a *pending* tail per file; `crash(seed)` keeps the synced
prefix, a random prefix of the unsynced tail (torn write) and flips the last surviving byte a quarter of the time.
Model tests compare against `BTreeMap` across random flush/compact/crash/reopen sequences.
