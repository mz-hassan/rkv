# DEFERRED RUNS (coded, dry-run validated, not executed)

| item | needs | command | local validation done |
|---|---|---|---|
| **K1** rolling upgrade / scale 3-5-3 with the operator on kind | Docker daemon (`brew install docker colima && colima start --cpu 4 --memory 8`), kind, kubectl | `make bench-k1` | `DRY_RUN=1`, manifests pass `kubeconform`, operator builds and prints its CRD |
| **S1 RocksDB baseline** | a ~10 min C++ build | `make build-baselines && RKV_ROCKS=1 make bench-s1` | `rkv-bench` baseline backend compiled (see `target/baselines`) |
| **R1 etcd baseline** | `brew install etcd` (installed on the dev Mac) | `make bench-etcd` | script dry-run; client compiled into the default `rkv-bench` |
| **N1** real-network runs: netem delay/loss, iptables leader isolation x100, dm-flakey disk faults, perf flamegraph | 3-4 Linux hosts with root | `HOSTS="a b c" CLIENT=d deploy/cloud/n1.sh all` (`make bench-n1`) | `DRY_RUN=1` prints the complete 600+ command plan; `bash -n` |
| **L3** cloud (multi-AZ) | cloud account, `tofu apply` in `deploy/cloud/terraform` | `terraform apply -var key_name=... -var ssh_cidr=...` then `n1.sh` | `tofu validate` passes |
| **Fuzz hours** | `rustup toolchain install nightly`, `cargo install cargo-fuzz` (installed) | `SECS=1800 make fuzz` | each target smoke-run for 8 s (>1M execs, no crash) |
| **Full-scale sweeps** (S1/S2/R1/R2/R3/D1/D2/soak) | hours of an idle, plugged-in machine | `make bench-local` (or each `make bench-*`) | every script run at `SCALE=smoke` into a throwaway directory |

Order of execution recommended in `README.md` ("Next steps").
