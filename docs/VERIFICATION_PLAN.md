# VERIFICATION PLAN

## 1. Invariants (checked continuously in simulation)
| ID | Invariant |
|---|---|
| V-ES | Election safety: at most one leader per term |
| V-LA | Leader append-only |
| V-LM | Log matching: same (index, term) implies identical prefix |
| V-LC | Leader completeness: committed entries appear in all later leaders |
| V-SM | State-machine safety: no two nodes apply different entries at an index |
| V-DUR | No acknowledged write lost after any crash pattern |
| V-LIN | Recorded client histories are linearizable |
| V-DEDUP | Retried (client_id, seq) applied at most once |
| V-BANK | Sum of balances constant for every snapshot read |
| V-SI | No lost update / read skew; write skew only in SI mode |
| V-MONO | Committed index and applied index never decrease on a live node |

## 2. Simulator fault model
Network delay/drop/dup/reorder; symmetric/asymmetric partitions and heals; node crash and restart; disk: unsynced data lost, torn tail writes, fsync failure, slow IO; clock drift per node. Fault schedule generated from the seed by a "nemesis" with configurable intensity profiles (calm, stormy, partition-heavy, crash-heavy).

## 3. Seed exploration and minimization
- `make sim SEEDS=10000` runs seeds in parallel processes; each seed = one simulated scenario of N virtual minutes.
- Failure output: seed, profile, event trace tail. Minimization: delta-debug the nemesis schedule (remove faults, shorten duration) keeping the failure; save to `sim/regressions/<seed>.json` replayed by `cargo test`.
- Report: seeds run, virtual hours, violations. Counts come from run logs only.

## 4. Linearizability checker (`lincheck`)
Wing-Gong with Lowe's memoization (cache on (linearized set bitset, model state)), partitioned by key for the register model; models: register, CAS register, KV map (for multi-key txn use per-key + separate SI checker). Operations with unknown outcome (timeouts) are modeled as possibly-applied (pending forever). **Mutation testing**: inject bugs (stale read from follower, lost ack, double-apply, reordered apply, skipped dedup) into a mock/real implementation; checker must reject each. Optionally cross-check histories with Porcupine (L1, needs Go).

## 5. Property / model-based tests
- Engine vs `BTreeMap` model with ops {put, delete, get, scan, flush, compact, crash+recover, reopen}; crash model drops unsynced bytes.
- Raft: proptest over message schedules at unit level (small clusters).
- `loom` on skiplist, block cache shards, group-commit queue; `shuttle` on larger concurrent pieces.

## 6. Real-process Jepsen-style scenarios (L1)
| Name | Fault | Workload | Invariant |
|---|---|---|---|
| J-kill-leader | kill -9 leader repeatedly | register, CAS | V-LIN, V-DUR |
| J-pause | SIGSTOP/CONT leader and followers | register | V-LIN |
| J-partition-half | toxiproxy majority/minority splits | register, set | V-LIN |
| J-partition-ring | bridge/asymmetric partitions | register | V-LIN |
| J-skew | clock skew (lease reads on) | register | V-LIN; expect violation when skew > epsilon, documented |
| J-diskfull | fill disk quota | append | no corruption, clean error |
| J-bank | kill + partition during transfers | bank | V-BANK, V-SI |
| J-write-skew | none (SI vs serializable) | on-call doctors | skew appears in SI, absent in serializable |
| J-coord-crash | kill client between prewrite and commit | txns | locks resolved, atomicity |
| J-rolling | restart nodes one by one | register | V-DUR, V-LIN |

## 7. Fuzzing
`cargo-fuzz` targets: WAL reader, SSTable/block decoder, manifest decoder, Raft message decoder, protobuf requests. Report: executions, hours, crashes found. Seed corpus from real files.

## 8. Evidence reporting (honest wording)
"N seeds x M simulated hours, 0 invariant violations; K bugs found and fixed (docs/BUGS_FOUND.md)". Real-process results labelled localhost, hardware, commit.

## 9. BUGS_FOUND.md template
```
## BUG-NNN: <title>
- Found by: sim seed / proptest case / fuzz artifact / chaos run (id)
- Symptom / violated invariant:
- Root cause:
- Fix (commit):
- Regression test:
- Why it matters:
```

## Status of this plan (2026-10-22)

Implemented: all invariants V-ES/LA/LM/LC/SM/MONO in `crates/invariants`; V-LIN via `crates/lincheck` in simulation and in real-process
chaos; V-DEDUP via sessions + `NoDedup` mutant; V-BANK/V-SI via `rkv-bench txn-bank`, `txn-skew`, `rkv-chaos bank`; fuzz targets for WAL,
SSTable, manifest, Raft messages, replicated commands, protobuf requests (smoke-run only); loom model of the write-group protocol;
model-based proptests with crash/recovery.
Scenario mapping: J-kill-leader/J-pause/J-partition-half/J-partition-ring/J-skew/J-rolling = `rkv-chaos register --nemesis ...`;
J-bank = `rkv-chaos bank`; J-write-skew = `rkv-bench txn-skew`; J-coord-crash = `rkv-bench lock-recovery`;
**J-diskfull is deferred** (needs OS-level quotas; the N1 dm-flakey run covers disk errors).
Known gap: Figure-8 mutant not caught (BUGS_FOUND.md). Evidence numbers are produced by `make sim` / `make results`, not stated here.
