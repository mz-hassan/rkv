# HOW TO REPRODUCE

Everything below runs on macOS (Apple silicon) or Linux. Times are for the author's laptop; results depend on the machine.
Do the heavy parts on an idle, plugged-in machine (a laptop in a bag will throttle and spoil latency numbers).

## 0. Toolchain
```
scripts/env-check.sh                      # shows what is missing + the install command
brew install rustup protobuf python       # required
rustup-init -y -c clippy -c rustfmt
python3 -m venv .venv && .venv/bin/pip install matplotlib numpy pyyaml     # plots
# optional: brew install etcd docker docker-compose colima kind kubectl kubeconform opentofu
# optional fuzzing: rustup toolchain install nightly --profile minimal && cargo install cargo-fuzz --locked
```

## 1. Build and unit/property/integration tests (~5 min)
```
make build            # release binaries into target/release: rkv-server rkv-pd kvctl rkv-bench rkv-chaos rkv-sim rkv-operator
make test             # 66 tests incl. model tests vs BTreeMap with crash/recovery, PD-driven 4-store cluster, lease, merge
make test-loom        # loom model of the write-group protocol (~40 s)
make lint             # clippy -D warnings + rustfmt --check
make dry-run          # print every benchmark script's commands without executing
```
More property-test cases: `PROPTEST_CASES=300000 cargo test --release -p kv-engine --test model matches_model`

## 2. Smoke (real processes, ~1 min)
```
make smoke            # 3 stores + PD: put/get/cas, cross-range txn, split, kill -9 a store, keep serving
```

## 3. Deterministic simulation (the consensus evidence)
```
make sim SEEDS=2000                       # 6 fault profiles x 2000 seeds -> $OUT/sim-summaries/*.json
./target/release/rkv-sim --seeds 0..500 --profile stormy --duration 20000 --voters 5 --minimize --save-regressions crates/sim/regressions
# a failing seed prints its seed; replay deterministically:  rkv-sim --seeds N..N+1 --profile P --duration D
# prove the harness can fail (each must report failures):
./target/release/rkv-sim --seeds 0..400 --profile stormy --bug vote-ignores-log
#   other --bug values: stale-reads no-dedup ack-early no-persist-vote commit-minority(--voters 5) commit-old-term skip-conflict-check
```
Profiles: `calm stormy partition crash membership skew`.

## 4. Benchmarks (each script has SCALE=smoke|full and DRY_RUN=1)
```
make bench-s1     # engine YCSB A-F, fsync/cache/compaction sweeps, recovery vs WAL size   (add: make build-baselines; RKV_ROCKS=1)
make bench-s2     # crash consistency: simulated power loss x5000, SIGKILL x500, whole-cluster kill -9
make bench-r2     # leader failover windows (100 trials per fault), election timeout sweep, PreVote/CheckQuorum ablation, snapshot catch-up
make bench-r3     # linearizability: sim seeds, injected-bug suite, 200 real-process chaos histories per nemesis
make bench-r1     # replicated throughput/latency; then make bench-etcd for the etcd baseline
make bench-d1     # sharding: ranges 1/4/16/64, split under load, hot range
make bench-d2     # transactions: contention sweep, write skew, lock recovery, bank under chaos, HLC vs oracle
make bench-soak   # RSS of all servers under 30 min of load
make results resume    # RESULTS.md + bench/plots + RESUME_BULLETS.generated.md (numbers only from bench/results/**)
```
Raw output: `bench/results/<ID>/<unix-ts>/result.json` with hardware, git commit, config and honesty labels.
`SCALE=smoke` writes to `/tmp/rkv-smoke-results` instead and never touches `bench/results`.

## 5. Chaos by hand
```
./target/release/rkv-chaos register --nemesis mixed --secs 60 --clients 8 --seed 3     # histories checked by lincheck
./target/release/rkv-chaos bank --nemesis kill-leader --secs 60                           # transfers + conservation audits
./target/release/rkv-chaos failover --fault kill --trials 100
```
Nemeses: `kill-leader pause partition-half ring rolling skew skew-lease mixed`. Logs for each run are kept in `/tmp/rkv-chaos-*`.

## 6. Running a cluster yourself
```
scripts/cluster.sh up 3 pd                # PD + 3 stores; or: scripts/cluster.sh up 3  (static membership, no PD)
export PD=127.0.0.1:23790
./target/release/kvctl --pd $PD put hello world
./target/release/kvctl --pd $PD txn put a 1 b 2
./target/release/kvctl --pd $PD status
./target/release/kvctl --pd $PD presplit user8000      # split ranges; also: split / merge / members / transfer-leader / lease
curl -s localhost:20181/metrics | head                 # Prometheus metrics; /healthz /readyz too
scripts/cluster.sh down
```

## 7. Containers and Kubernetes (needs Docker: `brew install docker colima && colima start --cpu 4 --memory 8`)
```
docker build -t rkv:dev -f deploy/docker/Dockerfile .
docker compose -f deploy/docker/docker-compose.yml up -d     # 3 PD + 3 stores + Prometheus (:9090) + Grafana (:3000, admin/admin)
make bench-k1                                                # kind + operator-driven upgrade/scale under a bank workload
```

## 8. Linux / cloud (deferred): see `deploy/cloud/README.md`
