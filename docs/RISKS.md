# RISKS

| Risk | Counter |
|---|---|
| Scope too large (~75 h); half-built looks weak | Strict milestone order with a working, verified artefact at each step; finish-first list below |
| Subtle Raft bugs | Sans-IO + deterministic sim + invariants from day one; every bug logged with regression seed |
| Benchmark noise on a laptop | Repetitions, idle machine, same-session baselines, ranges not points, honest labels |
| "Toy" perception | Baselines (RocksDB, etcd), real-process chaos, mutation-tested checker, fuzzing, BUGS_FOUND log |
| macOS fsync semantics skew results | Use `F_FULLFSYNC` for strict; label every durability setting |
| Docker unavailable | L0 path complete without it; L1 items clearly blocked rather than faked |
| Docker VM overhead for etcd baseline | Prefer native etcd binary; document caveat |
| Unverified crate versions | `cargo` resolves real versions in session 2; log in DECISIONS.md |
| Agent context exhaustion in long build | Milestone commits + `docs/PROGRESS.md` handoff notes |
| Transactions + sharding complexity explosion | Build txn against single-range first, multi-range after split works |

## Finish-first priority (reorders, does not cut)
1. M1 engine + model tests (S1/S2)
2. M2 Raft + simulator + invariants (the centerpiece)
3. M3 linearizability checker + mutation tests
4. M4 replicated server/client, R2 failover numbers
5. M7 transactions (high resume signal), then M6 sharding
6. M8 chaos, then M5 baselines vs etcd
7. M9 stretch, M10 deferred scripts, M11 polish
