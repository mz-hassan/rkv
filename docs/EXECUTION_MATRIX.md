# EXECUTION MATRIX

Levels: L0 no externals; L1 local tools (Docker/toxiproxy/kind); L2 Linux hosts; L3 cloud.

| Item | Level | Prerequisites | Command | Runtime | Local dry-run for deferred |
|---|---|---|---|---|---|
| Engine unit + proptest + crash model | L0 | rust | `make test` | ~3 min | n/a |
| loom/shuttle tests | L0 | rust | `make test-loom` | ~5 min | n/a |
| Raft + sim invariants | L0 | rust | `make sim SEEDS=2000` | ~10 min | n/a |
| lincheck + mutation tests | L0 | rust | `cargo test -p lincheck` | ~2 min | n/a |
| Fuzzing | L0 | nightly, cargo-fuzz | `make fuzz SECS=600` | 10 min/target | n/a |
| S1 vs RocksDB | L0 | rust, clang | `make bench-s1` | ~2 h | n/a |
| S2 simulated power loss | L0 | rust | `make bench-s2` | ~20 min | n/a |
| S2 real kill -9 | L1 | rust | `make bench-s2-real` | ~30 min | n/a |
| R1 localhost cluster | L1 | rust (etcd baseline: Docker or etcd binary) | `make bench-r1` | ~3 h | n/a |
| R2 recovery (sim) | L0 | rust | `make bench-r2-sim` | ~15 min | n/a |
| R2 recovery (real) | L1 | rust, toxiproxy | `make bench-r2` | ~1 h | n/a |
| R3 linearizability | L0 + L1 | rust (+ toxiproxy) | `make bench-r3` | ~1 h | n/a |
| D1 sharding | L1 | rust | `make bench-d1` | ~1.5 h | n/a |
| D2 transactions | L0 + L1 | rust | `make bench-d2` | ~1.5 h | n/a |
| Compose clusters, Prometheus, Grafana | L1 | Docker | `make cluster-up` | n/a | n/a |
| Chaos real-process (J-*) | L1 | rust, toxiproxy | `make chaos` | ~1 h | n/a |
| K1 on kind | L1 | Docker, kind, kubectl | `make bench-k1` | ~1 h | `make k8s-lint` (manifests dry-run via `kubectl --dry-run=client`) |
| N1 netem/iptables/dm-flakey/perf | L2 | Linux x3 VMs, root | `make n1 HOSTS=...` | ~4 h | `make n1 DRY_RUN=1` prints and validates commands, runs against a local mock executor |
| Cloud multi-AZ | L3 | cloud account | `deploy/cloud/run.sh` | varies | `terraform validate` + `DRY_RUN=1` |
| Results / plots / resume | L0 | python venv | `make results resume` | ~1 min | n/a |

Cluster-dependent items needing Docker are blocked until Docker is installed; everything in L0 can start immediately after rustup + protoc.
