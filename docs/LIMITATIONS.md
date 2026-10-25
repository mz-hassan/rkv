# LIMITATIONS (read before quoting any result)

## Scope of the evidence
* All measurements so far are smoke runs; **no benchmark has been run for real**. `RESULTS.md` says `NOT RUN` where data is missing.
* Everything runs on one laptop (Apple M5): "multi-node" means several processes sharing cores, memory bandwidth and one SSD.
  Absolute throughput and latency are laptop numbers; scaling curves flatten when cores run out.
* Network and disk faults are injected **inside the process** (transport-layer partitions/drops/delay/clock-rate; a modelled
  disk in the simulator). Kernel-level `tc netem`, `iptables`, `dm-flakey` and `perf` runs are coded (`deploy/cloud/`) but not executed.
* `kill -9` tests process crashes only: the OS page cache survives, so they are **not** power-loss tests. The simulated
  power-loss model (unsynced bytes lost, torn tail, corrupted last byte) covers that for the engine and Raft log.
* macOS `fsync(2)` does not flush the drive cache; `--strict-fsync` uses `F_FULLFSYNC` (what Rust's `File::sync_data` would call on its own). Results record which was used. RocksDB's `sync=true` and rkv's plain-fsync mode are *similar* but not proven equivalent, so the headline S1 comparison disables fsync on both sides and reports the fsync runs separately.
* Benchmarks are closed-loop; latency is measured under saturation without coordinated-omission correction.

## Consistency
* Single-key operations are linearizable (ReadIndex default). **Lease reads** are linearizable only if clock drift is bounded;
  `rkv-chaos register --nemesis skew-lease` deliberately exceeds the bound and may report a violation (expected, flagged).
* **Follower reads** are bounded-stale, based on time since the last leader message; not linearizable.
* Transactions are snapshot isolation. `Serializable` mode locks/validates the keys a transaction **read by point lookup**;
  range scans lock only the keys they returned, so **phantoms are possible** under a scan-then-insert pattern.
* Timestamps come from PD's oracle (default) or a client-local HLC. HLC mode is only as consistent as client clocks.
* MVCC garbage collection keeps 10 minutes of history (needs PD for the clock); a transaction older than that may fail to read.

## Engine
* No compression and no prefix compression/restart points in blocks (simpler format than the design sketch).
* The memtable is an arena skiplist under a `RwLock` (concurrent readers, one writer), not lock-free.
* One maintenance thread does flush and compaction (no concurrent compactions); compaction rate limit exists as an engine option
  but is not exposed as a server flag.
* TTL: expired values are hidden at read time and swept by the range leader about every 5 s; compaction does not drop them.
* The Raft log has its own WAL and is rewritten when it grows 4 MiB past live data.

## Raft / replication
* Snapshots are one in-memory message (no chunking/streaming): fine for lab-scale ranges, not for tens of GiB.
* Partitions injected at the transport are **per-pair and symmetric** (a node blocking a peer drops both directions); one-way
  partitions are only exercised in the simulator.
* The Figure-8 mutation is not caught by the current fault schedules (see BUGS_FOUND.md).
* No learner-promotion safety check beyond PD's "applied close to commit" heuristic; no automatic membership change on node loss
  other than PD adding replicas when a store is considered dead (15 s without heartbeat).

## Sharding / PD
* Range **merge** is manual (`kvctl merge`), requires identical stable voter sets, and PD does not schedule merges.
* PD's routing table and scheduler state are in the leader's memory and rebuilt from store heartbeats after a PD failover
  (a ~1-2 s window of `no route` answers). Only the cluster metadata (ids, TSO window, config, bootstrap) is Raft-replicated.
* PD is a single TSO source: ~tens of thousands of timestamps/s per client batch, not a scalability target.
* Hot-range handling = split on a write-QPS threshold; there is no hot-key (single key) mitigation.

## Operator / deployment
* The operator, Kubernetes manifests, Docker image and Compose file are validated statically (kubeconform, `docker-compose config`,
  the operator compiles and renders its CRD) but **have not been run against a cluster** (no Docker daemon on the dev machine).
* Terraform is validated with OpenTofu (`validate`), never applied.

## Not implemented
* Per-pair kernel partitions, TLS/auth, OpenTelemetry tracing, SQL, cross-DC placement, online schema changes, a leaderless
  comparison harness (the stretch pick was the lease service instead).

## Large values: snapshots and state copies are held in memory
Found while running R1 with 16 KB values on a 15 GB VM: a single-node server holding ~1.6 GB of values (100k x 16 KB) grew to
~14 GB resident. Building and shipping a Raft snapshot materialises the whole range in memory (and the log, memtables and
message copies add more), so memory is a multiple of the data size. Ranges are meant to split at 96 MB so this is bounded in
normal operation, but a single huge unsplit range is not. For the benchmark, the 16 KB-value runs use 10x fewer records and
ops (for rkv and etcd alike), and the result files record the sizes used. Streaming snapshots would fix it.

## Scans are slow (YCSB-E is 9% of RocksDB)
After removing the per-scan memtable copy (BUG-027), short scans are still about 11x slower than RocksDB (13.9k vs 154k ops/s on a
1M-record dataset). The merge iterator opens and seeks every SSTable for each scan. Not optimised further on purpose.

## Memory grows during sustained load
In a 10-minute write soak, server RSS grew from ~55 MB to 650 to 780 MB and was still rising slowly (2 to 9% over the second half).
It was not investigated; it may be the memtable/block-cache budget filling up rather than a leak, but the run cannot tell.

## Cluster benchmarks are disk-bound
All nodes of the benchmark clusters were processes on one VM with one shared disk (about 60 to 100% of its IOPS budget during
write tests). Throughput numbers for writes, and the sharding experiment (more ranges = lower throughput), mostly measure that disk.

## Some R2 experiments recorded zero trials
`R2-pause`, `R2-partition-nocheckquorum` and `R2-partition-neither` show 0 completed trials in `RESULTS.md`. No conclusion (for
example about PreVote or CheckQuorum under a leader isolation) can be drawn from them.
