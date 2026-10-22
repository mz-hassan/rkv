# LEARNING GUIDE

## Reading list mapped to modules
| Source | Module |
|---|---|
| Ongaro, *Consensus: Bridging Theory and Practice* (Raft thesis), ch. 3 to 6, 9 | `raft`: election, replication, membership, snapshots, leadership transfer, PreVote |
| Raft paper (Ongaro & Ousterhout 2014) fig. 2 | `raft` core rules |
| etcd/raft docs and README (Ready contract) | `raft` driver design |
| O'Neil et al., *The Log-Structured Merge-Tree* | `kv-engine` |
| LevelDB `doc/impl.md`, RocksDB wiki (compaction, WAL, block format) | WAL, SSTable, compaction |
| Bloom (1970); Kirsch-Mitzenmacher double hashing | bloom filter |
| Peng & Dabek, *Percolator* | `txn` |
| Corbett et al., *Spanner* (TrueTime context); TiKV and CockroachDB design docs | `pd`, `txn`, multi-Raft |
| Zhou et al., *FoundationDB* (SIGMOD 2021) and Will Wilson's simulation talk | `sim` |
| Jepsen articles (etcd, TiDB analyses) | chaos scenarios |
| Wing & Gong 1993; Lowe 2017; Porcupine blog post | `lincheck` |
| Berenson et al., *A Critique of ANSI SQL Isolation Levels* | SI, write skew |
| Kleppmann, *DDIA* ch. 3, 5, 7, 9 | background |

## Interview Q&A (answers to be filled from code/docs after build)
1. Why must Raft persist before replying to AppendEntries?
2. Why can't a leader commit entries from previous terms by counting replicas?
3. What problem does PreVote solve, and what does it cost?
4. What does CheckQuorum protect against?
5. ReadIndex vs lease reads: correctness conditions and failure under clock skew?
6. Why joint consensus rather than naive config switch?
7. How does the sans-IO design make deterministic simulation possible?
8. How do you implement exactly-once writes across leader failures?
9. Walk through the WAL recovery with a torn tail.
10. Why are keys stored with descending timestamps?
11. Leveled vs tiered compaction: amplification trade-offs, measured by you?
12. How are tombstones garbage-collected safely?
13. How does a Bloom filter reduce read amplification; your measured FPR?
14. How does a range split stay consistent with in-flight Raft proposals?
15. In Percolator, what is the commit point, and how is a crashed coordinator recovered?
16. Why does snapshot isolation allow write skew; how did you demonstrate it?
17. How does your linearizability checker handle timed-out operations?
18. How did you prove the checker works (mutation testing)?
19. What bugs did simulation find that unit tests did not?
20. What are the limits of your evidence (localhost, laptop, simulated faults)?


---

## Answers to the 20 interview questions (with pointers into the code)

1. **Why persist before replying to AppendEntries?** A follower's "yes" lets the leader count the entry toward a quorum; if the follower crashed and forgot it, a committed entry could exist on fewer than a majority. `raft-driver` calls `storage.save(&rd, sync)` before `transport.send` (`drive()`); the contract is in the header of `raft/src/node.rs`; the `NoPersistVote` mutant shows the same rule for votes.
2. **Why not commit previous-term entries by counting replicas?** An old-term entry on a majority can still be overwritten by a later leader (Raft paper Fig. 8). `maybe_commit` requires `log.term_at(q) == self.term`; the `CommitOldTerm` mutant removes it (our simulator does not yet catch it - see BUGS_FOUND).
3. **PreVote?** Stops a partitioned node from inflating the term and deposing a healthy leader on return; cost is one extra round before elections. `Raft::campaign`, `handle_vote_request`; ablation in `bench/scripts/r2.sh` (`--no-prevote`).
4. **CheckQuorum?** A leader cut off from the majority steps down instead of serving stale reads/accepting writes it can never commit. `tick_leader`; ablation `--no-check-quorum`.
5. **ReadIndex vs lease reads?** ReadIndex confirms leadership with a heartbeat round per read batch (no clock assumption). Lease reads skip it while a quorum acked within a window shorter than the followers' vote-stickiness window - correct only if clock drift is bounded. `Raft::read_index`, `Raft::lease_read`; the `skew-lease` chaos nemesis violates the assumption on purpose.
6. **Joint consensus?** Switching directly from C_old to C_new can produce two disjoint majorities. Joint config needs a majority of both. `Membership::quorum/committed_index`, `EnterJoint`/`LeaveJoint`, `propose_conf_change`.
7. **Why sans-IO?** The simulator must control time, ordering and storage; if Raft owns threads/clocks that is impossible. `raft` has no I/O; `sim/src/world.rs` and `raft-driver` are two drivers of the same core.
8. **Exactly-once writes?** Clients tag writes `(client_id, seq)`; the replicated state machine keeps the last seq and result per client and returns the cached result for a retry. Sessions are persisted with the applied index and travel in snapshots (`sm.rs` `dedup`, `session_key`). The `NoDedup` mutant is caught by the checker.
9. **WAL recovery with a torn tail?** Records are `[crc][len][payload]`; replay stops at the first short/corrupt record; the recovered memtable is flushed and a fresh WAL started so appends are never placed after garbage (`wal::read_all`, `Db::open`; `MemFs::crash`).
10. **Why descending timestamps in keys?** So the newest version of a key sorts first and a point read stops at the first hit (`key.rs`); comparison must treat key and timestamp as separate fields because keys have variable length.
11. **Leveled vs tiered?** Leveled: lower read/space amplification, higher write amplification; tiered the opposite. `CompactionStyle`; measured by `S1-compaction-*` (write amp from engine metrics) - run `make bench-s1`.
12. **Safe tombstone GC?** Drop only if no deeper level could hold an older version of any key in the *output* range (`is_bottom_for`) and the tombstone is at/below the GC watermark - the first bug found (BUG-001) was getting this range wrong.
13. **Bloom filters?** One hashed probe per file avoids reading blocks for absent keys; FPR ~0.8% at 10 bits/key. `bloom.rs`; the engine counts negatives vs false positives so `S1` reports measured vs theoretical FPR.
14. **Range split vs in-flight proposals?** The split is itself a replicated command: everything ordered before it applies to the parent, anything after for keys now outside the parent is rejected at apply time as `RangeMoved` and retried by the client after a route refresh (`Cmd::Split`, `exec()` range check). Children start on the same nodes without copying data.
15. **Percolator commit point and coordinator recovery?** The commit point is the atomic commit of the primary key's lock; any reader that meets a secondary lock asks the primary (`CheckTxnStatus`) and then commits or rolls back the secondary (`txn_client::resolve_lock`, `percolator::check_txn_status`). Tested with expired locks and `rkv-bench lock-recovery`.
16. **Why SI allows write skew; demo?** Two transactions read overlapping data and write disjoint keys: no write-write conflict. `rkv-bench txn-skew` runs the on-call-doctors scenario under both modes (expected: skew under snapshot, none under serializable for point reads).
17. **Timed-out operations in the checker?** They are *pending forever*: may have taken effect at any time after invocation or never (`lincheck::step` for `ret == None`; pending reads are dropped).
18. **How do you know the checker works?** Mutation testing: hand-built violating histories, random linearizable histories that must pass, and injected system bugs the simulator must catch (`lincheck` tests, `sim/tests/sweep.rs`).
19. **What did simulation find that unit tests did not?** Honest answer: on the unmutated code it found nothing (0 violations in 18,000 seeds); it found harness weaknesses (BUG-005/006) and it caught every detectable injected bug except Figure 8. The real product bugs came from the model test (BUG-001) and integration tests (BUG-003/004). Say so.
20. **Limits of the evidence?** One laptop, in-process fault injection, no kernel partitions, `kill -9` is not a power cut, closed-loop benchmarks, serializable = point reads only (`docs/LIMITATIONS.md`).
