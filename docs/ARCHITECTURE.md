# ARCHITECTURE

## 1. Crates
```
kv-engine ◄── raft-storage ◄── kv-server ◄── kv-client ◄── kvctl, ycsb, chaos
raft (no deps on I/O) ◄──────── kv-server, sim
txn ◄── kv-server, kv-client      pd ◄── kv-server, kv-client
sim ──► raft, kv-engine(trait-faulted), lincheck, invariants
```
`raft`, `txn` core logic, and `pd` core logic are sans-IO. Tokio/tonic only appear in `kv-server`, `kv-client`, `pd` binary shells, `chaos`, `operator`.

## 2. Sans-IO Raft
```rust
pub struct RaftNode { /* term, vote, log index/term cache, role, progress, config, ... */ }
pub enum Input  { Tick, Recv(Message), Propose(Vec<u8>, ProposalId), ReadIndex(ReqId),
                  StorageDone(Persisted), ApplyDone(u64), Campaign, TransferLeader(NodeId),
                  ChangeConfig(ConfChange), SnapshotReady(SnapMeta) }
pub struct Ready { pub to_persist: Persist, pub send: Vec<Message>, pub committed: Vec<Entry>,
                   pub read_states: Vec<ReadState>, pub role_change: Option<Role>, ... }
impl RaftNode { pub fn step(&mut self, now: Instant, input: Input) -> Ready; }
```
- Time is only an argument (`now`) and a logical `Tick`. No threads, no `std::time::Instant::now`.
- Contract: caller must durably persist `to_persist` **before** sending `send` messages that depend on it (etcd Ready contract), and reports `StorageDone`.
- Production driver: one tokio task per Raft group owning `RaftNode`, mpsc inbox (bounded), a `Storage` impl (raft-storage), `Transport` impl (tonic). Simulator driver: same loop over virtual network/disk/clock.
- Traits: `Clock`, `Transport`, `LogStorage`, `StateMachine`. Simulator implements them with fault injection.

## 3. Storage engine file formats
**WAL record**: `[crc32c:4][len:4][type:1][payload:len]`; types FULL/FIRST/MIDDLE/LAST over 32 KiB blocks (LevelDB style). Recovery stops at first bad CRC in the tail and truncates; corruption before the tail is an error.

**SSTable**: `[data blocks][filter block][index block][footer]`
- data block: prefix-compressed entries, restart points every 16 keys, trailer `[compression:1][crc32c:4]`
- index block: last key of each data block to BlockHandle(offset,size)
- filter block: Bloom, 10 bits/key default, double hashing from one xxh3
- footer (48 B): filter handle, index handle, magic, version
- internal key: `user_key || (!ts as 8 bytes BE) || kind(1)` so newer versions sort first

**Manifest**: log of VersionEdit records `{add_file(level,id,min,max,size), del_file, log_number, next_file, last_seq}`; `CURRENT` points to active manifest; switch by write-temp + rename + dir fsync.

## 4. Read / write paths
Write: group-commit queue, assign sequence/ts, WAL append (+fsync per policy), memtable insert, ack. Memtable full: becomes immutable, new WAL, background flush to L0, manifest edit, old WAL deleted.
Read: memtable, immutables, L0 (all, newest first), L1+ (binary search one file per level), Bloom check before block read, block cache. MVCC read at ts returns newest version <= ts.

## 5. Compaction
Leveled: L0 trigger at 4 files, level size multiplier 10, pick file with highest overlap score, rate-limited. Size-tiered: merge N similar-size runs. Tombstones dropped only at bottom level or when no older overlapping data; MVCC versions below GC watermark dropped except newest. Amplification measured, not asserted: leveled expected lower read/space, higher write amplification; tiered the opposite.

## 6. Replicated KV state machine
Raft entries: `Command{client_id, seq, op}`. Apply is deterministic; writes go to the engine with ts = raft index (non-txn) so replicas agree. Dedup table updated atomically with apply. Snapshot = engine checkpoint (hard-linked SSTs) + dedup table + config, streamed in chunks.

## 7. Multi-Raft and PD
Key space split into ranges `[start,end)`; each range = Raft group with its own log namespace in a shared engine (column-family-like prefix). Node runs a `Store` multiplexing groups with batched heartbeats. Split: leader proposes `Split{split_key, new_range_id}`; at apply, range metadata is cut and new group is created with the same replicas (initialised from the same applied state); PD learns via heartbeat. Merge: reverse, requires co-located replicas. PD (3-node Raft) holds route table, store stats; schedulers: balance replica count, balance leaders, split hot/large ranges. Clients cache routes, refresh on `RangeMoved`.

## 8. Transactions (Percolator)
Column families inside engine: `default` (data @ start_ts), `lock`, `write` (commit_ts to start_ts).
- Begin: start_ts from TSO (batched allocation, persisted high watermark in PD; HLC variant uses node clock + logical counter).
- Prewrite: per range, check no write newer than start_ts and no lock; write lock (primary pointer, ttl) and data. 
- Commit: commit primary (write record, drop lock) — the commit point; then secondaries async.
- Read: if lock with ts <= read_ts, wait or resolve: check primary; commit it forward or roll back if expired.
- Recovery: any reader/writer resolves stale locks via the primary's state. Coordinator crash tested by killing between prewrite and commit.
- Serializable mode (T2): track read set, validate at prewrite that no commits landed in read keys since start_ts (range reads: predicate/gap check is limited, documented).

## 9. Threading, backpressure
Engine: foreground writers + one flush thread + compaction thread pool (rate-limited). Server: tokio multi-thread runtime; blocking engine calls through `spawn_blocking` or dedicated threads. Bounded: gRPC concurrency limit, Raft inbox, proposals in flight, apply queue, memtable count (write stall), L0 file count (slowdown then stop).

## 10. Simulator
Single-threaded executor, virtual clock, seeded `ChaCha` RNG. Network: per-link latency distribution, drop/dup/reorder, partitions. Disk: in-memory FS with a *durable* vs *pending* view per file; crash discards or tears pending writes; fsync may fail. Nodes crash/restart from durable state. Whole run is a pure function of the seed.
