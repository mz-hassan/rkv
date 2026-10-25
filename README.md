# rkv

A distributed, transactional key-value store written in Rust. It has its own LSM storage engine, Raft consensus, range
sharding (multi-Raft) with a placement driver, and Percolator-style multi-key transactions. The only external pieces in the
product are tokio, tonic (gRPC) and a few utility crates; RocksDB and etcd are used only as benchmark baselines.

I built it to learn how systems like etcd, TiKV and CockroachDB work by implementing the pieces myself and then trying to break
them.

## Features

- **Storage engine** (`kv-engine`): write-ahead log with group commit, skiplist memtable, SSTables with Bloom filters and
  checksums, leveled and size-tiered compaction, MVCC versions, block cache, crash recovery.
- **Raft** (`raft`, `raft-storage`, `raft-driver`): leader election with PreVote and CheckQuorum, log replication, snapshots,
  membership changes (learners, joint consensus), leadership transfer, ReadIndex and lease reads. The core is a pure state machine
  with no I/O, so the same code runs in production and inside a deterministic simulator.
- **Replicated KV service** (`kv-server`, `kv-client`, `kvctl`): gRPC API for get / put / delete / CAS / scan / TTL / watch,
  exactly-once writes through client sessions, leader redirects, Prometheus metrics and health endpoints.
- **Sharding** (`kv-server`, `pd`): the key space is split into ranges, each its own Raft group. Ranges split and merge online.
  A placement driver (itself Raft-replicated) provides timestamps, ids, routing, replica repair, balancing and store draining.
- **Transactions** (`txn`): Percolator-style prewrite / commit across ranges, snapshot isolation plus a serializable mode for
  point reads, recovery after a crashed coordinator, MVCC garbage collection.
- **Extras**: lease / lock service with fencing tokens, change feed, a small Kubernetes operator (rolling upgrades, scale-in).

About 16,600 lines of Rust in 16 crates.

## Architecture

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

A few design points:

- **Persist before send.** A vote or an AppendEntries ack is sent only after the state behind it is on disk.
- **Atomic apply.** Applying committed entries writes the data and the new applied index in one engine batch. Retried writes carry
  `(client, seq)`, so a retry returns the first result instead of applying twice.
- **Transactions.** A transaction commits when its primary key commits. Anyone who meets a stale lock asks the primary what
  happened and finishes or rolls back the transaction.
- **Splits and merges.** A split is a replicated command that starts a child Raft group on the same nodes. A merge freezes the
  right range, waits until every replica has applied the freeze point, then extends the left range.

More detail is in [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) and [`docs/deep-dives/`](docs/deep-dives/).

## Getting started

Requirements: Rust (stable), `protoc`, and a C toolchain. `scripts/env-check.sh` lists what is missing.

```bash
make build        # release binaries in target/release
make test         # unit, property, simulation and integration tests
make smoke        # ~1 min: real 3-store + PD cluster; put/get/cas, txn, split, kill -9, failover
```

Run a local cluster and talk to it:

```bash
scripts/cluster.sh up 3 pd && export PD=127.0.0.1:23790
./target/release/kvctl --pd $PD put user:1 alice
./target/release/kvctl --pd $PD get user:1
./target/release/kvctl --pd $PD txn put a 100 b 200          # multi-key transaction
./target/release/kvctl --pd $PD lease acquire leader-lock me 5000
./target/release/kvctl --pd $PD status                       # ranges, leaders, replica progress
scripts/cluster.sh down
```

Docker Compose (3 PD + 3 stores + Prometheus + Grafana) and Kubernetes manifests are in `deploy/`.

## Testing

| What | How |
|---|---|
| Storage engine | property tests against a `BTreeMap` model, including crash and recovery; a `loom` model of the write-group protocol |
| Raft | deterministic simulator with partitions, drops, duplicates, crashes with torn writes, fsync failure, clock drift and membership changes; safety invariants checked on every step |
| Linearizability | a Wing-Gong/Lowe checker run over every simulated history and over histories from real-process chaos |
| Checker quality | mutation testing: inject bugs into Raft and require the suite to catch them (6 of 7 caught; the Raft paper's "Figure 8" bug is not) |
| Real processes | `rkv-chaos` injects `kill -9`, `SIGSTOP`, partitions and clock skew into a live cluster and checks the histories and bank-transfer totals |
| Parsers | `cargo-fuzz` targets for the WAL, SSTable, manifest, Raft messages and protobuf requests |
| Integration | failover, restart, split, merge, exactly-once, leases, cross-range transfers over real gRPC |

Results so far: 0 violations in 40,000 simulation seeds (289 simulated hours), 0 linearizability violations in 140 chaos
histories, 0 acknowledged writes lost in 5,000 simulated power losses and 500 `SIGKILL`s, about 425M fuzz executions without a
crash. Every bug found along the way is written up in [`docs/BUGS_FOUND.md`](docs/BUGS_FOUND.md), and the story of how the project
developed is in [`docs/TIMELINE.md`](docs/TIMELINE.md).

## Benchmarks

Full tables are in [`RESULTS.md`](RESULTS.md); how to reproduce them is in
[`docs/HOW_TO_REPRODUCE.md`](docs/HOW_TO_REPRODUCE.md). Cluster numbers were measured on a single 4-vCPU Azure VM where every "node"
is a process sharing one disk, so treat them as disk-bound rather than as a picture of a real multi-machine deployment.

**Single-node engine vs RocksDB** (YCSB, 1M records, fsync off on both sides):

| Workload | rkv ops/s | RocksDB ops/s | rkv / RocksDB |
|---|---|---|---|
| A (50/50 read/update) | 175,843 | 148,555 | 118% |
| B (95/5) | 675,921 | 730,588 | 93% |
| C (read-only) | 919,222 | 888,114 | 104% |
| D (read latest) | 722,409 | 785,114 | 92% |
| E (short scans) | 13,889 | 154,212 | 9% |
| F (read-modify-write) | 162,927 | 141,713 | 115% |

**Replicated cluster vs etcd** (same machine, 128 B values, fsync on):

| Nodes | Mix | rkv ops/s | etcd ops/s |
|---|---|---|---|
| 3 | write | 1,238 | 1,680 |
| 3 | 50/50 | 1,119 | 1,561 |
| 3 | read | 16,060 | 10,946 |
| 1 | write | 2,377 | 5,270 |
| 1 | read | 29,166 | 16,415 |

**Failure behaviour:** after `kill -9` of the leader, writes resume with p50 439 ms / p99 562 ms (20 trials, election timeout
200 to 400 ms). With the leader isolated by `iptables`, p50 is 361 ms / p99 521 ms over 100 trials. A lagging replica catches up
100 MB through a snapshot in 1.8 s. In a 6-cycle test that repeatedly drains, kills and re-adds two of five stores under bank
transfer load, the total balance stayed conserved.

**Transactions:** bank totals were conserved in every run, including with leader kills and partitions. Snapshot isolation shows
write skew in 500 of 500 test pairs, serializable mode in 0 of 500.

## Known limitations

The complete list is in [`docs/LIMITATIONS.md`](docs/LIMITATIONS.md). The main ones:

- Scans are slow (YCSB-E is 9% of RocksDB).
- Cluster benchmarks ran on one machine with one disk; multi-machine scaling was not measured. More ranges did not increase
  throughput there.
- Server memory grew during a 10-minute write soak (about 55 MB to 700 MB) and was not investigated further.
- Snapshots are sent as one in-memory message, so very large values are a problem.
- Serializable mode covers point reads only, and the client-side hybrid-clock timestamp mode is only as consistent as the clocks.
- `kill -9` and the simulated disk are not a real power cut.
- A full scale-in run on a real Kubernetes cluster did not finish; the membership bugs it exposed are fixed and covered by a local
  churn test (`bench/scripts/churn_local.sh`).
- The simulator does not catch the Raft "Figure 8" bug.

## Repository layout

```
crates/   kv-engine  raft  raft-storage  raft-driver  txn  kv-server  kv-client  kvctl  pd
          sim  invariants  lincheck  chaos  ycsb  operator  rkv-proto
fuzz/     cargo-fuzz targets
bench/    benchmark scripts, result generators, plots
deploy/   Docker, Compose, Prometheus/Grafana, Kubernetes manifests and operator, cloud scripts
docs/     architecture, deep dives, bug log, benchmark and reproduction notes
scripts/  env-check.sh  cluster.sh  smoke.sh
```

If you want to read the code, a good order is `docs/deep-dives/raft.md`, `storage-engine.md`, `transactions.md`,
`simulation.md`, then `crates/raft/src/node.rs`, `crates/kv-engine/src/db.rs`, `crates/txn/src/percolator.rs` and
`crates/sim/src/world.rs`.
