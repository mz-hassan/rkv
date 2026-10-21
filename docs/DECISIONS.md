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
