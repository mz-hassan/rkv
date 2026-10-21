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
