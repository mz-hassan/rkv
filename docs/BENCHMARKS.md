# BENCHMARKS

All targets are `TARGET (hypothesis)`. Nothing here is a result. Every result file records: hardware, OS, commit, seed, date, config, baseline version, localhost vs multi-host, simulated vs real faults. Raw output: `bench/results/<ID>/<timestamp>/*.json`.

Common protocol: warm-up 30 s; measure 60 s; >= 5 repetitions; report median and min-max; latency from HDR histograms (p50/p99/p999) with coordinated-omission correction when open-loop; machine otherwise idle, plugged in; record `F_FULLFSYNC` vs plain fsync.

## S1 Single-node engine vs RocksDB (L0; baseline `rocksdb` crate, same compression, same cache size, same fsync policy)
- Hypothesis: within 0.3x to 1.5x RocksDB throughput on YCSB A-F; honest losses reported.
- Protocol: 10M keys (16 B key, 100 B value), 8 and 32 threads, Zipfian 0.99 and uniform. Workloads A-F. Memtable 64 MiB, cache 256 MiB.
- Metrics: ops/s, p50/p99/p999; write amp = bytes written to disk / user bytes; read amp = blocks read per Get; space amp = disk / live data; Bloom measured FPR vs theoretical 0.82% at 10 bits/key; cache hit ratio vs cache size sweep (16 MiB to 1 GiB); fsync policy sweep; compaction stall time; recovery time vs WAL size (64 MiB to 2 GiB).
- Pass: runs complete, correctness check of final contents, FPR within 2x theoretical.
- Honest statement: "X ops/s vs RocksDB Y on Apple M5 laptop, same settings class, single node".

## S2 Crash consistency (L0 simulated; L1 real kill -9)
- Protocol: write load with unique monotonically-numbered values; kill at random time (simulated power loss in sim, SIGKILL real); restart; verify every acked write present, no corruption, no resurrected deletes. >= 5,000 iterations simulated, >= 500 real.
- Pass: zero violations. Label "simulated power loss" vs "SIGKILL (process crash only)".

## R1 Replicated throughput/latency (L1 localhost; baseline etcd container)
- Matrix: nodes {1,3,5}; mix {100% write, 50/50, 95% read}; value {128 B, 1 KiB, 16 KiB}; batching/pipelining {on, off}; read mode {ReadIndex, lease, follower}.
- etcd: same node count, same value size, same fsync (etcd always fsyncs; use `group`/`always` for rkv), v3 API via `etcdctl`/client at same concurrency.
- Caveats: localhost, shared CPU, no network latency, Docker VM overhead for etcd on macOS. Compare with a native etcd binary if available.
- Hypothesis: 3-node write throughput within 0.5x to 1.5x etcd.

## R2 Fault recovery (L1 real; L0 simulated version too)
- Protocol: 3 and 5 nodes under constant load; fault {kill -9 leader, SIGSTOP leader, isolate leader}; measure unavailability = time from fault to first successful linearizable write. >= 100 trials per fault. Sweep election timeout {150, 300, 600, 1000 ms}; PreVote/CheckQuorum on/off; snapshot catch-up time for new replica vs state size {100 MiB, 1 GiB}; membership change latency impact.
- Output: p50/p95/p99 histogram. Hypothesis: p50 about 1.5x election timeout.

## R3 Linearizability under faults (L0 sim + L1 real)
- Protocol: N histories (>= 10,000 sim, >= 200 real) under partitions/crashes/skew; check each with lincheck. Plus mutation suite (>= 8 injected bugs) all caught.
- Pass: 0 violations on correct code; 100% mutants caught.

## D1 Sharding (L1)
- Protocol: 3 nodes, ranges {1,4,16,64}; YCSB A/C; throughput vs ranges; split of a hot range under load (latency p99 during split, duration); hot-range detection to rebalance time; leader balance skew.
- Hypothesis: near-linear scaling until CPU saturation on laptop (cores are shared, so likely flat past ~8 ranges; report what happens).

## D2 Transactions (L1, L0 sim for invariants)
- Protocol: bank 1,000 accounts, 4-key transfers; Zipfian theta {0, 0.5, 0.9, 0.99}; clients {8, 32, 128}. Metrics: commit throughput, abort rate, p99 latency. Chaos run: kills + partitions; total conserved. Write-skew test SI vs serializable. Lock-resolution time after coordinator kill (>= 100 trials).
- Pass: V-BANK holds in all runs; write skew observed in SI, absent in serializable.

## K1 Kubernetes on kind (L1; requires Docker)
- Rolling upgrade under write load with acked-write ledger: zero loss; scale 3 to 5 to 3 via operator; reconciliation latency. Label "kind, single host".

## N1 Real network (L2, DEFERRED)
- 3 Linux VMs; `tc netem` delay {0, 1, 10, 50 ms} and loss {0, 1%}; `dm-flakey`; `perf`/flamegraphs; repeat R1 and R2. Coded in session 2 with dry-run; results `NOT RUN` until executed.

## M1 Micro-benchmarks (L0)
`criterion`: skiplist insert/get, block decode, bloom lookup, WAL append (policies), Raft step throughput, lincheck ops/s.

## Result generation
`make results` reads only `bench/results/**.json`. Missing file gives `NOT RUN`. No numbers in docs/README except generated sections.
