# rkv — a distributed, linearizable, transactional key-value store in Rust

> Built from scratch (no off-the-shelf consensus or storage engine in the product) as a deep systems project:
> an **LSM storage engine**, **Raft consensus**, **multi-Raft sharding with a placement driver**, **Percolator-style distributed
> transactions**, and an unusually strong **verification story** (deterministic simulation, a hand-written linearizability checker,
> real-process chaos, fuzzing, property tests).
>
> **Where things stand (2026-10-25):** the system is built, tested and benchmarked. 30 issues were logged and fixed along the way
> (16 of them product bugs found by testing; the rest are tooling, test-harness and benchmark-integrity bugs), and every measurement below comes from
> raw result files (`bench/results/`, summarised in [`RESULTS.md`](RESULTS.md)). The story of how it got here, bug by bug, is in
> [`docs/TIMELINE.md`](docs/TIMELINE.md). Open items are listed under [Honest limitations](#8-honest-limitations).

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
| **Deterministic simulation** (6 fault profiles × many seeds): partitions, drops, duplicates, crashes with torn writes, fsync failure, clock drift, membership changes | Raft safety invariants on every step + linearizability of every client history + liveness after healing | **0 violations in 40,000 seeds (289 simulated hours, ~125M client operations checked)** on the unmodified code; a 72-run subset runs inside `cargo test` |
| **Mutation testing of the verification itself** (inject bugs, require the suite to catch them) | proves the checkers can fail | 6 of 7 injected bugs caught (stale reads, ack-before-commit, missing dedup, unpersisted vote, vote without log check, commit on a minority). **Not caught: the Raft paper's "Figure 8" commit bug** (documented gap) |
| Linearizability checker unit + mutation tests | Wing-Gong/Lowe with memoisation | passing |
| Real-process chaos (`rkv-chaos`) | `kill -9`, `SIGSTOP`, partitions, ring partitions, clock-rate skew, rolling restarts; histories checked by the same checker; bank-conservation audits under faults | 7 fault types x 20 histories (about 2.6M operations), 0 linearizability violations; bank totals conserved under leader kills, partitions and mixed faults |
| Integration tests over real gRPC | failover, restart-from-disk, split, merge, exactly-once, leases, PD-driven replication, cross-range bank transfers | passing |
| Fuzzing (`cargo-fuzz`) | WAL, SSTable, manifest, Raft messages, replicated commands, protobuf requests | 6 targets, about 1 hour total (~425M executions), no crash |

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

## 6. Results: what the measurements say

Full tables, plots and honesty labels: **[`RESULTS.md`](RESULTS.md)** (generated from raw files; nothing typed by hand). Cloud numbers
come from one Azure 4-vCPU burstable VM with a single shared disk (all "nodes" are processes on it), so they are
**disk-bound** and say little about a real network cluster. Crash tests ran on the MacBook.

**Storage engine vs RocksDB** (single node, 1M records, fsync off on both sides):

| YCSB | rkv ops/s | RocksDB ops/s | rkv / RocksDB |
|---|---|---|---|
| A (50/50 read/update) | 175,843 | 148,555 | 118% |
| B (95/5) | 675,921 | 730,588 | 93% |
| C (read-only) | 919,222 | 888,114 | 104% |
| D (read latest) | 722,409 | 785,114 | 92% |
| E (short scans) | 13,889 | 154,212 | **9%** |
| F (read-modify-write) | 162,927 | 141,713 | 115% |

Reads and point writes are on par with RocksDB; **scans are the weak spot** (a known gap, see limitations). With fsync on every
write rkv does 649 ops/s against RocksDB's 2,943; with group commit both reach about 3,000. Recovery replays a 321 MB log in 9.6 s.

**Replicated cluster vs etcd** (same workload, same machine, 128 B values, fsync on):

| nodes | mix | rkv ops/s | etcd ops/s | rkv / etcd |
|---|---|---|---|---|
| 3 | write | 1,238 | 1,680 | 74% |
| 3 | 50/50 | 1,119 | 1,561 | 72% |
| 3 | read | 16,060 | 10,946 | 147% |
| 1 | write | 2,377 | 5,270 | 45% |
| 1 | read | 29,166 | 16,415 | 178% |

Reads (ReadIndex) beat etcd; writes are 45 to 74% of etcd. Lease reads (28k ops/s) are 1.8x faster than ReadIndex reads (16k).
On this disk-bound VM, batching and pipelining made little difference to write throughput (1,168 on vs 1,214 / 1,065 with one of them off).

**Failure behaviour:**

| experiment | result |
|---|---|
| leader killed (`kill -9`), 20 trials | writes resume after p50 439 ms, p99 562 ms (election timeout 200 to 400 ms) |
| leader partitioned from the others, 20 trials | p50 368 ms, p99 474 ms |
| election timeout sweep | failover time scales with the timeout: p50 244 / 437 / 696 / 1,392 ms for 100 to 200 / 200 to 400 / 400 to 800 / 800 to 1,600 ms timeouts |
| leader isolated with `iptables` on the cloud VM, 100 trials | p50 361 ms, p99 521 ms |
| lagging replica catching up 100 MB through a snapshot | 1.8 s |
| acknowledged-write loss | 0 in 5,000 simulated power losses, 500 `SIGKILL`s and 20 whole-cluster kills |
| 6 cycles of draining, killing and re-adding two of five stores under load | bank total conserved (100,000 of 100,000), 69,785 transactions |

**Transactions** (bank transfers, 1,000 accounts, PD timestamps): every run conserved the total, including with leaders killed,
partitions and mixed faults. Throughput is modest (11 to 97 commits/s; abort rate 13% at 8 clients to 68% at high contention),
because each transaction takes several consensus round trips and the disk is shared. Snapshot isolation shows write skew in
**500 of 500** doctor-on-call pairs; serializable mode shows **0 of 500**. Recovery after a crashed coordinator takes a median of
524 ms (lock TTL 500 ms). The client-side HLC timestamp mode **does** lose money (8 of 1,000,000): it is documented as only as
consistent as client clocks, and this run measures that cost.

**Sharding:** more ranges did not raise throughput here (write throughput fell from 1,715 ops/s at 1 range to 1,110 at 4 and 623 at
16), because every store shares one disk and each range adds its own log. A range split during a 30-second write workload showed
successful operations in every second (roughly 1,200 to 1,500 ops/s, lowest second 973), with no visible stall at the moment of the
split. This experiment needs separate machines to show scaling.

**Memory soak:** server RSS grew from about 55 MB to 650 to 780 MB during a sustained 10-minute write load, and was still growing
slowly (2 to 9% in the second half), so it is not flat; not investigated further.

### What this does and does not show
* It shows a correct (as far as extensive testing can tell) replicated, transactional store that is competitive with RocksDB on
  point operations and with etcd on reads, and that survives crashes, partitions and membership changes without losing data.
* It does **not** show scaling across machines, behaviour on a real network, or production-grade write performance.

`make results resume` regenerates `RESULTS.md` and `RESUME_BULLETS.generated.md` (bullets only where enough data exists).
**Honesty rules baked into the tooling:** raw JSON in `bench/results/<ID>/<ts>/` with hardware, config and labels (localhost vs
multi-host, simulated vs real faults, fsync mode); no number is typed into any document; smoke runs never land in `bench/results`.

## 7. Next steps (all optional)

1. **Re-run the timing-sensitive benchmarks on a quiet Mac or dedicated cloud hardware** (`scripts/preflight.sh`, then `make bench-s1 bench-r1`). The Azure numbers are disk-bound; local SSD numbers will differ.
2. **Fix the scan gap** (YCSB-E is 9% of RocksDB): the merge iterator does too much work per short scan.
3. **Run a full Kubernetes scale-in** (`make bench-k1`) now that the membership bugs it exposed are fixed; the local churn test (`bench/scripts/churn_local.sh`) already passes 6 cycles.
4. **Long fuzzing** (`SECS=900 make fuzz`) and a targeted scenario for the Raft Figure-8 bug the simulator does not catch.
5. **Real multi-machine network run** (`deploy/cloud/n1.sh`) to see sharding scale.
6. **Before an interview:** read `docs/LEARNING_GUIDE.md` and `docs/TIMELINE.md`.

## 8. Honest limitations

Full list in `docs/LIMITATIONS.md`. The main ones: cluster benchmarks ran with all nodes on one machine and one disk · `kill -9` and
the in-memory disk model are not a real power cut · scans are slow · snapshots are single in-memory messages (large values blow up
memory) · serializable mode protects point reads only · HLC timestamp mode is only as consistent as client clocks · several R2
variants (pause, and the no-CheckQuorum partition runs) recorded zero completed trials and should be ignored · the full Kubernetes
scale-in run never completed end to end (it found six bugs, now fixed and covered by the local churn test) · the verification suite
has one documented blind spot (Raft Figure 8).

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
