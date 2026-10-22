# Transactions deep dive (`crates/txn`, `kv-client::txn_client`, `kv-server::sm`)

Percolator over the replicated KV: three column families as key prefixes (`d` data@start_ts, `l` lock, `w` write@commit_ts), all
inside the range that owns the user key. Each primitive is a replicated command evaluated deterministically at apply time, so the
conflict check is atomic with the write.

**Protocol.** begin: start_ts from the oracle (fresh, not a cached batch). Reads see commits <= start_ts; a lock with
start_ts' <= start_ts blocks the read. Commit: choose a primary (smallest key), **prewrite** all keys in parallel (per range;
all-or-nothing within a range) - fail on a newer committed write (write-write conflict, first committer wins) or a foreign lock;
get commit_ts; **commit the primary** (the commit point: its range applies the write records and removes the lock atomically);
commit secondaries asynchronously.

**Recovery.** A reader/writer that meets a lock asks the primary's range `CheckTxnStatus`: committed -> commit the secondary at the
same commit_ts; rolled back / TTL expired (decided by the replicated clock in the command) -> roll back, leaving a *rollback
tombstone* so a delayed prewrite of the dead transaction cannot succeed later; alive -> wait. Tests: expired primary, delayed
prewrite after rollback, commit after rollback, idempotent commit/rollback (`txn/src/percolator.rs`).

**Isolation.** Snapshot isolation: write skew is possible and is *measured* (`rkv-bench txn-skew`). Serializable mode converts every
point-read key into a lock-only mutation at prewrite, so a concurrent writer of a read key conflicts. Range scans lock only returned keys.

**Timestamps.** PD oracle (3 s persisted window; a new PD leader starts above the old window) or a client-local HLC.

**GC.** `Gc{safe_point}` keeps the newest version visible at the safe point and drops older write records and data.
