# Project timeline: what was built, what broke, how it was found

A readable story of the project. For the formal bug write-ups see [BUGS_FOUND.md](BUGS_FOUND.md); for numbers see
[../RESULTS.md](../RESULTS.md); for how to rerun things see [HOW_TO_REPRODUCE.md](HOW_TO_REPRODUCE.md).

## Architecture at a glance

```
 client (kvctl / rkv-bench / TxnClient)
        |  gRPC
        v
 +-------------------------- store process (rkv-server) --------------------------+
 |  range router -> one Raft group per key range ("Multi-Raft")                    |
 |     raft (sans-IO state machine: elections, log replication, membership, reads) |
 |     raft-driver (tokio shell: timers, network, persist-before-send)             |
 |     raft-storage (own write-ahead log)                                          |
 |  state machine per range: MVCC + Percolator transactions on top of the LSM      |
 |  kv-engine: WAL, memtable (skiplist), SSTables + Bloom filters, compaction      |
 +----------------------------------------------------------------------------------+
        ^ heartbeats / commands
 PD (placement driver): timestamp oracle, id allocator, range directory, scheduler
        (splits ranges, balances replicas, drains stores before scale-in)
 operator (Kubernetes): rolling upgrades and scale-out/in of the StatefulSet
```

| Piece | What it does | Where |
|---|---|---|
| Raft | leader election with PreVote/CheckQuorum, log replication, membership change (learners, joint consensus), snapshots, leadership transfer, linearizable reads (ReadIndex / lease) | `crates/raft` |
| Storage engine | LSM tree with group-commit WAL, Bloom filters, leveled or size-tiered compaction | `crates/kv-engine` |
| Transactions | Percolator-style snapshot isolation on top of MVCC, timestamps from PD | `crates/txn`, `crates/kv-server` |
| Placement | range splits/merges, replica balancing, store draining | `crates/pd` |
| Verification | deterministic simulator, linearizability checker, mutation testing, fuzzing, real-process chaos | `crates/sim`, `crates/chaos`, `fuzz/` |

## Timeline

### Phase 1: design and build (finished 2026-10-22)
About 16.5k lines of Rust in 16 crates. The design goal was "small enough to understand, real enough to break": every
protocol (Raft, Percolator, placement) is implemented rather than imported. Raft is written as a pure state machine with no I/O,
which is what makes the deterministic simulator possible.

### Phase 2: testing before benchmarking
- Property tests against a simple model found the first real bug: compaction could drop a delete marker too early and
  resurrect deleted keys (**BUG-001**).
- The simulator runs thousands of seeded fault schedules (partitions, crashes, clock skew) and checks invariants plus
  linearizability. Mutation testing (deliberately breaking Raft) shows the checkers do catch real bugs, with one honest
  exception: the Figure-8 mutant is not caught yet.
- A driver bug where a deposed leader left client requests hanging for 5 s was found by an integration test (**BUG-010**).

### Phase 3: making the benchmarks honest
- A sanity check of the RocksDB comparison exposed that macOS `fsync` in Rust is really a full drive flush, and that the
  RocksDB wrapper never applied its sync option. Without this, the headline comparison would have been unfair (**BUG-009b**).
- A crash-consistency test reported 13,675 "lost writes". It was the test's own ledger file being torn, not data loss. The
  invalid result is kept in `bench/invalid/` and the test was fixed (**BUG-020**).
- Rule adopted: numbers in the results come only from raw result files, each labelled with its scope and fault conditions.

### Phase 4: Azure, simulated multi-machine (night of 2026-10-23/24)
Real multi-VM clusters were too expensive for a student subscription, so one VM hosts several "machines" as Linux network
namespaces, with `tc netem` for latency/loss, `iptables` for partitions and `dm-flakey` for disk faults. About a dozen script
bugs surfaced here (quoting, wrong partition target, mount namespaces hiding the flaky disk: **BUG-011, BUG-012**).

### Phase 5: Kubernetes (K1)
A small operator drives rolling upgrades and scale-out/scale-in of a StatefulSet on `kind`. Operator and deployment bugs:
missing TLS crypto provider, server-side apply erasing the container image, declaring "Ready" too early, a readiness probe
failing on an empty store, a stale Docker layer, and PD's balancer undoing a drain (**BUG-013 to BUG-019**).

### Phase 6: the membership-churn bugs (2026-10-24)
Scaling in under load kept stalling, and each stall had a different cause. In order found:

| Bug | What went wrong | How it was found |
|---|---|---|
| 021 | MVCC GC deleted the only committed value under a rollback marker: **real data loss** | K1 audit could not read 3 of 100 accounts |
| 022 | a store removed from a range could never rejoin it (permanent tombstone) | scale-in stuck for 15 min |
| 023 | PD waited forever on a learner placed on a dead store | draining the second store hung |
| 024 | destroying a replica left stale metadata, so a re-added replica thought it had applied entries it had not | revived replica showed `commit=0 applied=141` |
| 025 | a re-added replica replayed old history, saw its own removal and destroyed itself | learner destroyed 3 times in 8 min |
| 026 | PD's replica GC reused a stale timer and killed fresh learners | local churn test, cycle 2 |

Lesson: each K1 rerun took ~30 minutes on the cloud VM. After the third rerun I wrote `bench/scripts/churn_local.sh`, which
reproduces the same scale-in/scale-out churn with real processes on a laptop in about 2 minutes, and bugs 025 and 026 were
found with it. The final version passes 6 drain/kill/rejoin cycles under bank-transfer load with the total balance conserved.

### Phase 7: benchmarks (2026-10-25)
Run on the Azure VM (S1, R1, R2, D1, D2, soak) and the Mac (crash tests, simulation, chaos, fuzz). Headline numbers are in the
README ("Results") and every table is in [../RESULTS.md](../RESULTS.md). The benchmark runs themselves found more bugs, mostly in
the measuring tools rather than the product:

| Bug | What went wrong | How it was found |
|---|---|---|
| 027 | short scans copied the whole memtable (performance) | YCSB-E ran 18+ minutes while A to D took 10 s |
| 028 | benchmark clusters were never wiped between runs | a server grew to 14 GB on a 1.6 GB dataset |
| 029 | stopping then starting a cluster raced on its ports | "address already in use" / "cluster not ready" |

Other things that went wrong and how they were handled:
- A dedicated-core VM size had zero quota on the student subscription, so everything ran on a burstable one; its single disk
  became the bottleneck (cluster benchmarks are disk-bound, labelled as such).
- Some experiments were trimmed to fit the time budget: the rkv R1 matrix runs 1 and 3 nodes with 128 B values only (etcd also has
  1 KB and 16 KB), failover experiments use 20 trials instead of 100, D1's 64-range case was skipped (it did not finish in 25
  minutes), and the "no batching, no pipelining" ablation was abandoned after 28 minutes.
- A few R2 variants recorded zero completed trials (leader pause, and the partition runs without CheckQuorum); they are in the
  tables as zeros and should not be interpreted.
- The VM's own auto-shutdown (a cost safeguard) fired in the middle of a run, which cost a restart.
- Cloud cost: about $2.3 reported by Azure for this project's resource group at the end (billing lags by a few hours; roughly
  $3 expected), with the VM deallocated afterwards.

## Environment notes
- MacBook: used for development, tests and the overnight correctness suite (simulation, chaos, fuzz).
- Azure (Sweden Central, student subscription): one 4-vCPU burstable VM. A dedicated-core VM size was not available (quota 0),
  so cloud benchmark numbers carry the label "burstable VM". The VM is deallocated when not in use.
