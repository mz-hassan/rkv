# rkv — a distributed, linearizable, transactional key-value store in Rust

> Built from scratch (no off-the-shelf consensus or storage engine in the product) as a deep systems project:
> an **LSM storage engine**, **Raft consensus**, **multi-Raft sharding with a placement driver**, **Percolator-style distributed
> transactions**, and an unusually strong **verification story** (deterministic simulation, a hand-written linearizability checker,
> real-process chaos, fuzzing, property tests).
>
> **Where things stand (2026-10-22):** all the code is written and tested; **no real benchmark has been run yet**. Every benchmark
> is scripted, dry-run-checked and smoke-tested, and is waiting for you to run it (see [Next steps](#next-steps)). `RESULTS.md` is
> generated from raw measurements only — until you run things it says `NOT RUN`, and that is the honest state.

---

## 1. The problem

A single-node database is simple and fast, and it dies with its machine. To survive failures, you replicate; to grow past one
machine, you shard; to keep correctness while doing both, you need consensus, careful storage, and transactions that span machines.
Systems such as etcd, TiKV and CockroachDB solve this, and they are famously hard to get right: the bugs live in rare interleavings of
crashes, partitions, retries and clock behaviour that ordinary tests never produce.

This project asks: **can I build a miniature of that stack, and — more importantly — can I *prove it behaves* under failure?**

| Requirement | What it means here |
|---|---|
| **Durable** | An acknowledged write survives process crashes and (simulated) power loss, per the chosen fsync policy. |
| **Linearizable** | Every single-key operation appears to take effect atomically at one instant between its start and end — even across leader changes, retries, crashes and partitions. |
| **Transactional** | Multi-key, multi-shard transactions with snapshot isolation (and a serializable mode for point reads), recovering from a crashed coordinator. |
| **Scalable** | The key space is split into ranges, each its own Raft group, spread over nodes by a placement driver; ranges split and merge online. |
| **Verifiable** | Failures are injected on purpose, a checker decides whether the observed behaviour was legal, and every discovered bug is logged. |

It is also a *resume-grade learning project*: the documents in `docs/` are written so you can explain every design choice in an
interview (`docs/LEARNING_GUIDE.md`).

## 2. What was built

| Layer | Crate(s) | What it does |
|---|---|---|
| Storage engine | `kv-engine` | WAL with group commit and 4 fsync policies; skiplist memtable; SSTables with Bloom filters + checksums; manifest/recovery; **leveled and size-tiered compaction**; MVCC versions; block cache; write stalls; snapshot export/import; a virtual filesystem with a **power-loss model** (lost/torn unsynced bytes) |
| Consensus | `raft` (sans-IO), `raft-storage`, `raft-driver` | Raft with **PreVote, CheckQuorum, leader stickiness, batching + pipelining, snapshots, joint-consensus membership changes, learners, ReadIndex and lease reads, leadership transfer**; durable log on its own WAL; async tokio driver |
| Replicated KV | `kv-server`, `kv-client`, `kvctl` | gRPC API (get/put/delete/CAS/scan/TTL/watch/transactions); **exactly-once writes** via client sessions; leader redirects and retries; follower reads with bounded staleness; Prometheus `/metrics`, `/healthz`, `/readyz` |
| Sharding | `kv-server`, `pd` | Many Raft groups per node over one shared engine; **range split and merge**; a **placement driver** (itself Raft-replicated) with a timestamp oracle, id allocation, replica repair, balancing, leader balancing, hot/large-range splits and store draining |
| Transactions | `txn`, `kv-client` | **Percolator**: prewrite → commit-primary → commit-secondaries; lock resolution after coordinator crash; SI and a serializable mode; PD oracle or **hybrid logical clock** timestamps |
| Stretch features | `kv-client`, `operator` | **Lease/lock service with fencing tokens**; change feed; **Kubernetes operator** (rolling upgrades, scale-in via PD drain) |
| Verification | `sim`, `invariants`, `lincheck`, `chaos`, `fuzz/` | Deterministic simulator, Raft safety invariants, Wing-Gong/Lowe linearizability checker, real-process Jepsen-style chaos, 6 fuzz targets, property tests, a loom model |
| Measurement | `ycsb` (`rkv-bench`) | YCSB A–F, crash-consistency drivers, cluster / transaction benchmarks, latency histograms, results with honesty labels; optional RocksDB and etcd baselines |
| Packaging | `deploy/`, `bench/scripts/`, `Makefile` | Dockerfile, Compose (3 PD + 3 stores + Prometheus + Grafana), Kubernetes manifests + operator, Linux/cloud scripts, Terraform, one script per benchmark |

~16,600 lines of Rust in 16 crates, **66 automated tests**, clippy `-D warnings` and rustfmt clean.

## 3. How it works (the short tour)

```
              clients ── kv-client (routing cache, retries, sessions, txn coordinator)
                │  gRPC
   ┌────────────┴─────────────── Store (one per node) ─────────────────────────────┐
   │  ranges:  [ "" , m )   [ m , t )   [ t , ∞ )         one Raft group each     │
   │     │ propose / read-index                │                                    │
   │  raft-driver ── sans-IO Raft ── raft-storage (own WAL, fsync before send)      │
   │     │ committed entries                                                        │
   │  RangeSm: deterministic state machine (raw ops, Percolator, split, merge)      │
   │     │ one atomic batch = data + applied index                                  │
   │  kv-engine: WAL → memtable → SSTables (leveled/tiered compaction), shared      │
   └──────────────────────────────────────────────────────────────────────────────┘
                                   ▲ heartbeats / commands
   Placement driver (3 nodes, Raft): timestamp oracle · ids · routing · scheduler
```

* **Sans-IO Raft.** The consensus core has no clock, threads, sockets or disk: it consumes events and returns "persist these, send
  those, apply these". The *same code* runs in production (tokio + gRPC + real files) and inside the simulator (virtual time + a
  modelled lossy network + a disk that tears writes on crash). That is what makes deterministic testing of the real code possible.
* **Persist before send.** A vote or an AppendEntries ack is only sent after the state behind it is durably on disk.
* **Atomic apply.** Applying committed entries writes the data *and* the new applied index in one engine batch, so a crash can never
  leave the state machine ahead of or behind its own bookkeeping. Retried writes carry `(client, seq)`; the replicated session table
  makes a retry return the first outcome instead of applying twice.
* **LSM details that matter.** Internal key = `user_key ++ !timestamp`, ordered newest-first; tombstones are dropped only when no deeper
  level can still hold an older version (the first real bug found, `BUG-001`); the manifest is a log of edits swapped in via `CURRENT`;
  a failed fsync poisons the database (no writes after an unknown disk state).
* **Transactions.** Locks, data and commit records live in the same range as the key. A transaction's commit point is the atomic
  commit of its *primary* key; anyone meeting a stale lock asks the primary what happened and finishes or rolls back the transaction.
  Rollback leaves a tombstone so a delayed prewrite from a dead transaction cannot sneak in.
* **Sharding.** A split is a replicated command that shrinks the parent and starts a child Raft group on the same nodes (no data
  copy: it is one shared engine). A merge freezes the right range, waits until *every* replica has applied the freeze point, then
  extends the left range — so no lagging replica can lose data.

Deeper write-ups: `docs/deep-dives/` (raft, storage engine, transactions, simulation) and `docs/ARCHITECTURE.md`.

## 4. How it is verified — and what has actually been shown

| Technique | What it covers | State |
|---|---|---|
| Model-based property tests (`proptest` vs `BTreeMap`) incl. **crash + recovery**, leveled and tiered | LSM correctness | passing; 300,000 cases per style after fixing the bug it found |
| `loom` model of the write-group protocol | the engine's one hand-rolled concurrent algorithm | passing (all interleavings of 3 writers) |
| **Deterministic simulation** (6 fault profiles × many seeds): partitions, drops, duplicates, crashes with torn writes, fsync failure, clock drift, membership changes | Raft safety invariants on every step + linearizability of every client history + liveness after healing | clean on the unmodified code (earlier 18,000-seed sweep; a 72-run subset runs inside `cargo test`). **Re-run `make sim` for current numbers.** |
| **Mutation testing of the verification itself** (inject bugs, require the suite to catch them) | proves the checkers can fail | caught: stale reads, ack-before-commit, missing dedup, unpersisted vote, vote without log check, commit on a minority. **Not caught: the Raft paper's "Figure 8" commit bug** (documented gap) |
| Linearizability checker unit + mutation tests | Wing-Gong/Lowe with memoisation | passing |
| Real-process chaos (`rkv-chaos`) | `kill -9`, `SIGSTOP`, partitions, ring partitions, clock-rate skew, rolling restarts; histories checked by the same checker; bank-conservation audits under faults | works at smoke scale (e.g. 1,050 ops across 2 leader kills linearizable; 187 transfers across 3 kills, total conserved) |
| Integration tests over real gRPC | failover, restart-from-disk, split, merge, exactly-once, leases, PD-driven replication, cross-range bank transfers | passing |
| Fuzzing (`cargo-fuzz`) | WAL, SSTable, manifest, Raft messages, replicated commands, protobuf requests | built and smoke-run (100k to 1.4M executions each in 8 s, no crash); long runs pending |

**Bugs found along the way are logged in `docs/BUGS_FOUND.md`** with how each was found — including the honest ones that came from
code review or the compiler. The headline one: a compaction bug that resurrected deleted keys, found by the property test.

## 5. Quick start

```bash
scripts/env-check.sh            # what's installed / what to install
make build                      # release binaries -> target/release
make test                       # 66 tests (a few minutes)
make smoke                      # ~1 min: real 3-store + PD cluster; put/get/cas, txn, split, kill -9, failover
```

```bash
scripts/cluster.sh up 3 pd && export PD=127.0.0.1:23790
./target/release/kvctl --pd $PD put user:1 alice
./target/release/kvctl --pd $PD txn put a 100 b 200          # multi-key transaction
./target/release/kvctl --pd $PD lease acquire leader-lock me 5000
./target/release/kvctl --pd $PD status                       # ranges, leaders, replica progress
scripts/cluster.sh down
```

All commands, flags and the reproduction recipe: **`docs/HOW_TO_REPRODUCE.md`**.

## 6. The benchmark plan (written, ready, *not yet run*)

Every row is a script that supports `DRY_RUN=1` (print commands) and `SCALE=smoke` (tiny, throwaway output). Targets are
**hypotheses**, not results. Runtimes are rough estimates for full scale on an idle laptop.

| ID | Question | Command | ~Time | Hypothesis (unverified) |
|---|---|---|---|---|
| S1 | Single-node engine vs RocksDB: YCSB A–F, p50/p99/p99.9, write/read/space amplification, Bloom FPR vs theory, cache hit ratio vs size, fsync-policy trade-off, leveled vs tiered, recovery time vs WAL size | `make bench-s1` (+ `make build-baselines`, `RKV_ROCKS=1`) | 2 h | within 0.3×–1.5× of RocksDB |
| S2 | Do acknowledged writes survive crashes? simulated power loss ×5000, SIGKILL ×500, whole-cluster kill -9 | `make bench-s2` | 30 min | zero lost |
| S3 | Memory under sustained load (soak) | `make bench-soak` | 30 min | RSS flat after warm-up |
| R1 | Replicated throughput/latency: 1/3/5 nodes × mixes × value sizes, batching/pipelining on/off, ReadIndex vs lease vs follower reads, **vs etcd** | `make bench-r1 bench-etcd` | 3 h | 0.5×–1.5× of etcd (localhost, caveats) |
| R2 | Failover unavailability window (p50/p95/p99 over 100 trials) for kill / pause / partition; election-timeout sweep; PreVote & CheckQuorum ablation; snapshot catch-up time | `make bench-r2` | 1–2 h | p50 ≈ 1.5× election timeout |
| R3 | Linearizability under faults: simulation seeds + injected-bug suite + real-process histories | `make bench-r3` (`HISTORIES=200` for the full target) | 2 h (14 h full) | 0 violations; all mutants caught |
| D1 | Sharding: throughput vs 1/4/16/64 ranges, split under load, hot-range handling | `make bench-d1` | 1.5 h | scales until laptop cores saturate |
| D2 | Transactions: contention (Zipf θ) sweep, abort rate, write skew (SI vs serializable), lock recovery after coordinator crash, bank invariant under chaos, HLC vs oracle | `make bench-d2` | 1.5 h | write skew only under SI |
| K1 | Operator-driven rolling upgrade and scale 3→5→3 on kind under a bank workload | `make bench-k1` | 1 h | zero invariant violations |
| N1 | Real network: netem delay/loss, iptables leader isolation ×100, dm-flakey disk faults, flamegraphs | `HOSTS=... deploy/cloud/n1.sh all` | 4 h | deferred (Linux hosts) |

`make results resume` then writes `RESULTS.md` (tables + plots) and `RESUME_BULLETS.generated.md` — bullets only for experiments
that have enough data (`NOT RUN — cannot claim` otherwise).

**Honesty rules baked into the tooling:** raw JSON in `bench/results/<ID>/<ts>/` with hardware, git commit, config and labels
(*localhost vs multi-host*, *simulated vs real faults*, *fsync mode*); no number is typed into any document; smoke runs can never
land in `bench/results`.

## 7. Next steps

Do these in order; each is one command. Use an idle, plugged-in machine (a laptop in a bag will throttle and spoil latency numbers).

1. **Confirm the build on your machine:** `make build test lint dry-run` (≈10 min).
2. **Consensus evidence first** (cheapest, most valuable): `make sim` then `make bench-r3` with the default `HISTORIES`; inspect any failing seed (`rkv-sim` prints it; replays deterministically) and log real findings in `docs/BUGS_FOUND.md`.
3. **Storage:** `make bench-s1 bench-s2`; optionally `make build-baselines && RKV_ROCKS=1 make bench-s1` for the RocksDB comparison.
4. **Replication & recovery:** `make bench-r2 bench-r1`, then `make bench-etcd` for the baseline.
5. **Sharding & transactions:** `make bench-d1 bench-d2 bench-soak`.
6. **Fuzz for an hour or more:** `SECS=900 make fuzz` (6 targets).
7. **Generate the report:** `make results resume`; read `RESULTS.md`; keep the honesty labels with every number you quote.
8. **Optional L1/L2/L3:** install Docker (`brew install docker colima && colima start --cpu 4 --memory 8`) → `make bench-k1`; Linux hosts → `deploy/cloud/n1.sh`; cloud → `deploy/cloud/terraform`.
9. **Close the known gaps:** a targeted scenario for the Figure-8 commit bug; chunked snapshot transfer; PD-scheduled range merges; phantom protection for serializable range scans (all in `docs/LIMITATIONS.md`).
10. **Before an interview:** read `docs/LEARNING_GUIDE.md` and be able to answer its 20 questions from this code (answers point at modules).

## 8. Honest limitations (full list in `docs/LIMITATIONS.md`)

Laptop-only numbers (processes share cores and one SSD) · faults injected in-process, not by kernel tools · `kill -9` is not a
power-cut · lease reads assume bounded clock drift · serializable mode protects point reads only · snapshots are single messages ·
no compression · the operator, Kubernetes manifests, Docker image and cloud scripts are validated statically but have never been
run against a real cluster · the verification suite has one documented blind spot (Figure 8).

## 9. Repository map

```
crates/  kv-engine  raft  raft-storage  raft-driver  txn  kv-server  kv-client  kvctl  pd  sim  invariants  lincheck
         chaos  ycsb  operator  rkv-proto          fuzz/  (6 cargo-fuzz targets)
bench/   scripts/ (one per benchmark, results.py, resume.py)   results/ (raw JSON, git-ignored)   plots/
deploy/  docker/ (Dockerfile, Compose, Prometheus, Grafana)  k8s/ (base, overlays, operator)  cloud/ (netem, iptables, dm-flakey, perf, Terraform)
docs/    REQUIREMENTS ARCHITECTURE VERIFICATION_PLAN BENCHMARKS EXECUTION_MATRIX TASKS RESUME_TEMPLATES RISKS DECISIONS LEARNING_GUIDE
         ENVIRONMENT ASSUMPTIONS HOW_TO_REPRODUCE LIMITATIONS DEFERRED_RUNS BUGS_FOUND PROGRESS  deep-dives/
scripts/ env-check.sh  cluster.sh  smoke.sh
```

## 10. Learning path

Read in this order: `docs/deep-dives/raft.md` → `storage-engine.md` → `transactions.md` → `simulation.md`, then the matching code
(`crates/raft/src/node.rs`, `crates/kv-engine/src/db.rs`, `crates/txn/src/percolator.rs`, `crates/sim/src/world.rs`), then
`docs/LEARNING_GUIDE.md` (reading list mapped to modules + 20 interview questions).
