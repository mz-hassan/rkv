# DECISIONS

| # | Decision | Why |
|---|---|---|
| D1 | Rust stable, Cargo workspace, edition 2021 unless toolchain supports 2024 | Required by project; memory safety for storage code |
| D2 | tokio + tonic/prost for the shell only; core crates sync and sans-IO | Same Raft code in sim and prod |
| D3 | Raft: thesis-style with PreVote, CheckQuorum, joint consensus (not single-server) | Joint consensus is the general mechanism and the more impressive one; etcd-style Ready contract |
| D4 | Own skiplist memtable (single writer, concurrent readers) verified with loom | Learning value; fallback `crossbeam-skiplist` |
| D5 | Leveled compaction default, size-tiered as option | RocksDB default; tiered gives amplification comparison |
| D6 | MVCC key encoding with descending timestamp | Natural snapshot reads, shared with txns |
| D7 | Txn protocol: Percolator with TSO, HLC alternative | Matches TiKV; recoverable by lock resolution |
| D8 | Isolation: SI default, serializable via read validation if feasible | Honest anomaly story |
| D9 | Simulator disk model with durable/pending views | Enables torn-write and unsynced-loss testing |
| D10 | Baselines: `rocksdb` crate for S1, etcd for R1 | Standard references |
| D11 | Stretch pick: distributed lock/lease service on the store, plus follower reads and watch | Small scope, strong signal; skip leaderless comparison |
| D12 | Raft log in its own WAL (not the LSM) | Avoids write amplification, simpler crash model |
| D13 | Plain test-time determinism: `ChaCha8Rng` seeded 64-bit | Reproducible failures |
| D14 | Prompt-02 precedence: if docs conflict with prompt 02, prompt 02 wins and is logged here | Per prompt rule |
| D15 | Session 1 skipped web verification of versions (no network tool used); versions resolved by cargo in session 2 | See ASSUMPTIONS.md |

## Decisions and deviations recorded during the build (session 2)

| # | Decision | Why / deviation from the session-1 plan |
|---|---|---|
| D16 | Versions actually used (Cargo.lock): Rust 1.99.1; tokio 1.54, tonic/tonic-build 0.12.3, prost 0.13.5, crc32fast 1.5.3, parking_lot 0.12.5, rand 0.8.8, proptest 1.12, criterion 0.5.1, loom 0.7.2, kube 0.99 + k8s-openapi 0.24, etcd-client 0.14.1 | ASSUMPTIONS.md listed unverified guesses; these are what resolved |
| D17 | Memtable = arena skiplist (safe Rust, index links) under a `RwLock`, not lock-free; loom models the **write-group protocol** instead | a lock-free skiplist needs `unsafe`; the group-commit leader/follower hand-off is the real concurrency risk |
| D18 | SSTable blocks have no prefix compression, restart points or LZ4 | optional in the requirements; keeps the format simple to audit and fuzz |
| D19 | Own hash for Bloom filters (FNV-1a + splitmix) rather than xxhash | no dependency; measured false-positive rate is reported against the theoretical value |
| D20 | Engine timestamps = Raft index; **transaction MVCC lives in the key space** (`d`/`l`/`w` prefixes), as TiKV does | lets one shared engine host many ranges and keeps range snapshots a plain key-range copy |
| D21 | One shared engine per store; ranges are key ranges, split = metadata change (no data copy); each store records the group's bootstrap membership in `m/base/<id>` | cheap splits; Raft needs its base config on restart |
| D22 | Raft transport = gRPC unary RPC carrying batches of encoded messages, one sender task per peer; best-effort (drop on overload) | Raft tolerates loss; avoids head-of-line stalls |
| D23 | PD replicates only metadata (bootstrap, id blocks, TSO window, config); routing/scheduler state is leader-local, rebuilt from heartbeats | like TiKV; per-heartbeat Raft writes would dominate |
| D24 | TSO: persisted 3 s window; new leader starts above the old window; clients take a **fresh** timestamp for begin and commit | external consistency across PD failover |
| D25 | Range merge: freeze right range (`PrepareMerge`), extend left (`CommitMerge`) only after every local replica of the right range applied the freeze point (bounded wait, else fail-stop); requires identical voter sets; admin-driven | the requirements say split *and* merge; this avoids data loss on lagging replicas |
| D26 | Stretch pick = **lease/lock service with fencing tokens** (CAS with TTL; expiry decided in the state machine via a proposer-supplied `now_ms`), plus watch/CDC, TTL, follower reads | small scope, high signal; the leaderless comparison harness was dropped |
| D27 | Real-process fault injection is **in-process at the Raft transport** (`Admin.SetFaults`: block peers, drop %, delay, logical clock rate) plus real `kill -9`/`SIGSTOP` | works on macOS without root/netem/Docker; kernel-level versions are the deferred N1 runs |
| D28 | Skew injection scales the node's logical tick rate (lease reads depend on it); wall-clock skew is not injected | lease validity is measured in ticks |
| D29 | HLC is an alternative timestamp source in the client only (no server-side observation) | documented as only as consistent as client clocks |
| D30 | etcd baseline = `etcd-client` inside `rkv-bench` (default build); RocksDB baseline = optional feature built by `make build-baselines` | identical workload generator for both sides; RocksDB compile is slow |
| D31 | Prompt-02 conflicts: none requiring precedence; scope reductions are exactly those listed in LIMITATIONS.md | - |
