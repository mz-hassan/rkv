# ASSUMPTIONS (Step B)

Session 1 did **not** do web verification of crate versions. Everything below is **assumed from prior knowledge, NOT verified**. Session 2 must run `cargo search` / `cargo add` (which resolves real versions) and pin whatever resolves, recording it in DECISIONS.md.

| Area | Assumption | Status |
|---|---|---|
| Toolchain | Rust stable, edition 2021 (2024 if toolchain supports it) | unverified |
| Async | `tokio` 1.x, `tonic`/`prost` 0.12–0.14 line, `bytes` 1.x | unverified |
| Checksums | `crc32fast` (WAL/SST), `xxhash-rust` (bloom hashing) | unverified |
| Skiplist | write own lock-free-ish skiplist for learning; `crossbeam-skiplist` as fallback | design choice |
| Testing | `proptest`, `loom`, `shuttle`, `criterion`, `cargo-nextest`, `cargo-fuzz` | unverified |
| Observability | `tracing`, `prometheus` crate | unverified |
| Baselines | `rocksdb` crate (needs clang, present), etcd container (needs Docker) | unverified |
| K8s | `kube-rs` + `k8s-openapi` | unverified |
| Raft | PreVote, CheckQuorum, ReadIndex, leadership transfer, joint consensus as in Ongaro thesis ch. 4, 6, 9 and etcd/raft behaviour | from memory; re-read thesis while implementing |
| YCSB | A 50/50 read/update zipfian; B 95/5; C read-only; D read-latest 95/5 insert; E scan 95/5 insert; F read-modify-write 50/50 | from memory |
| Percolator | primary lock + secondary locks, 2-phase commit via write CF, TSO | from paper |

Hardware assumption: all L0/L1 numbers come from a single Apple M5 laptop (24 GiB), shared with other workloads, so absolute numbers are noisy. Compare only against baselines run in the same session.
