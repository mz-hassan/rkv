# REQUIREMENTS

Project: **rkv**, a distributed, linearizable, transactional KV store in Rust. No off-the-shelf consensus or storage engine in the product. RocksDB and etcd appear only as benchmark baselines.

## 1. Tiers (all in scope; tiers set build order)

### Tier 1 Core
| ID | Requirement |
|---|---|
| E1 | WAL with fsync policy {`always`, `group` (group commit), `interval(ms)`, `never`}; CRC per record; torn-tail tolerant recovery |
| E2 | Skiplist memtable, immutable memtable flush, write stall when L0/immutables exceed limits |
| E3 | SSTable: data blocks, index block, Bloom filter, footer, per-block CRC, optional LZ4 |
| E4 | Manifest (versioned edits, atomic CURRENT switch), recovery |
| E5 | Leveled compaction + size-tiered option, tombstone GC, rate limiter |
| E6 | MVCC: key = user_key + descending timestamp; snapshot reads; GC watermark |
| E7 | Range scans/iterators (merging, bounded), block cache (LRU, sharded), metrics |
| E8 | Snapshot export/import (for Raft snapshots) |
| R1 | Sans-IO Raft: election (randomized timeout), PreVote, CheckQuorum, leader stickiness |
| R2 | Replication with batching + pipelining, persistent log via own WAL |
| R3 | Snapshots, InstallSnapshot, log compaction |
| R4 | Membership changes (joint consensus), learner catch-up |
| R5 | Linearizable reads: ReadIndex and lease reads |
| R6 | Client sessions + request dedup (exactly-once writes) |
| R7 | Leadership transfer |
| S1 | gRPC API Get/Put/Delete/Scan/CAS/Txn, leader redirect, client retries, `kvctl` CLI, admin commands |

### Tier 2 Extended
| ID | Requirement |
|---|---|
| D1 | Range-partitioned multi-Raft; split and merge; |
| D2 | Placement driver (Raft-replicated): range map, replica placement, rebalancing, leader balancing, hot-range detection, TSO |
| T1 | Percolator-style multi-key multi-range txns, snapshot isolation, lock resolution after coordinator crash, conflict handling |
| T2 | HLC timestamp alternative to TSO; serializable mode (read-set validation) if feasible, else documented |
| O1 | Prometheus metrics, structured tracing, status CLI, Grafana dashboards, health/readiness |

### Tier 3 Stretch
| ID | Requirement |
|---|---|
| X1 | Watch/CDC streams, TTL, follower reads with bounded staleness |
| X2 | Kubernetes StatefulSet manifests + `kube-rs` operator (lifecycle, rolling upgrade, scale via membership) |
| X3 | Distributed lock/lease service built on the store (chosen for signal at low scope; see DECISIONS) |

## 2. Consistency guarantees (precise)

- **Single-key operations**: linearizable (Get/Put/Delete/CAS), including across leader changes, provided the client retries with session ids. Exactly-once for writes via (client_id, seq) dedup, with the dedup table replicated and snapshotted.
- **Reads**: default ReadIndex (linearizable). Lease reads linearizable only under bounded clock drift (documented assumption: drift rate < configured epsilon). Follower reads: bounded-stale, **not** linearizable.
- **Multi-key transactions**: snapshot isolation (SI). Write skew is **permitted** and tested as an expected anomaly. Serializable mode (if built) forbids it. No lost updates, no dirty reads, no read skew.
- **Not guaranteed**: strict serializability across ranges without TSO; availability during minority partitions; Byzantine tolerance.

## 3. Failure model
Crash-recovery nodes; partitions (symmetric and asymmetric); message loss, duplication, reordering, delay; disk faults (torn writes, loss of unsynced data, fsync failure, disk full); bounded clock drift. **No Byzantine faults, no silent bit rot beyond what CRCs detect.**

## 4. Durability per fsync policy
| Policy | Guarantee |
|---|---|
| `always` | acked write survives process and OS crash (uses `F_FULLFSYNC` on macOS) |
| `group` | same as `always`, fsync amortized across concurrent writers |
| `interval` | survives process crash; may lose last interval on OS/power loss |
| `never` | survives process crash only if OS survives; benchmark-only |
Replicated writes are acked after a majority have persisted with the node's configured policy.

## 5. Non-functional
- Backpressure everywhere: bounded channels, proposal-in-flight limit, write stalls; soak shows bounded RSS.
- Deterministic: same seed gives same simulation trace.
- Same Raft crate in production and simulator.
- `cargo clippy -D warnings`, `cargo fmt --check` clean.
- Targets are hypotheses until measured (see BENCHMARKS.md).

## 6. Non-goals
SQL layer, Byzantine tolerance, cross-datacenter geo-replication logic, auth/TLS beyond optional rustls flag, online schema changes, cloud-managed deployment automation beyond L3 scripts.

## 7. API sketch (protobuf)
```proto
service Kv {
  rpc Get(GetReq) returns (GetResp);        // {key, consistency: LINEARIZABLE|FOLLOWER_STALE(ms)}
  rpc Put(PutReq) returns (PutResp);        // {key, value, ttl_ms, client_id, seq}
  rpc Delete(DelReq) returns (DelResp);
  rpc Scan(ScanReq) returns (stream KvPair);// {start, end, limit, consistency}
  rpc Cas(CasReq) returns (CasResp);        // {key, expect, new}
  rpc Txn(stream TxnOp) returns (TxnResp);
  rpc Watch(WatchReq) returns (stream Event);
}
service Admin { rpc Status; rpc AddLearner; rpc Promote; rpc RemoveNode; rpc TransferLeader; rpc SplitRange; }
service Pd    { rpc Heartbeat; rpc GetRoute; rpc Tso; rpc AllocId; }
// Errors carry a leader hint: NotLeader{leader_id, leader_addr}, RangeMoved{new_route}
```
