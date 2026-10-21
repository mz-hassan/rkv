# ENVIRONMENT (audited 2026-10-21)

| Item | State |
|---|---|
| OS | macOS 26.7.1 (Darwin 25.6.0), arm64 |
| CPU / RAM | Apple M5, 24 GiB |
| Free disk | ~349 GiB of 460 GiB |
| Xcode CLT / clang | installed (Apple clang 21) |
| Homebrew | 7.0.9 installed |
| git, make, jq, node 22, python3.13 | installed |
| rustup / rustc / cargo | **MISSING (required)** |
| protoc | **MISSING (required for tonic/prost)** |
| docker | **MISSING** (L1 compose/baselines) |
| kind, kubectl, helm | MISSING (L1 K1 benchmark, stretch) |
| go | MISSING (optional: Porcupine cross-check) |
| k6 / wrk / gnuplot | MISSING (optional) |
| matplotlib | MISSING (required for `make results` plots) |

## Install commands (macOS)

Required:
```
brew install rustup protobuf
rustup-init -y --default-toolchain stable -c clippy -c rustfmt -c llvm-tools-preview
cargo install cargo-nextest cargo-fuzz --locked
python3 -m venv .venv && .venv/bin/pip install matplotlib numpy
```
Optional / later (L1):
```
brew install --cask docker        # then launch Docker Desktop once
brew install kind kubectl helm go k6
cargo install flamegraph --locked # uses dtrace on macOS, needs sudo
rustup toolchain install nightly  # needed by cargo-fuzz and miri
```

## What does NOT work on macOS and the local substitute

| Linux facility | Substitute here |
|---|---|
| `tc netem` (latency/loss) | in-process simulator (L0); toxiproxy (L1) |
| `iptables` partitions | simulator partitions; Docker `network disconnect`; toxiproxy |
| `dm-flakey` disk faults | simulator torn-write / fsync-failure model; fault-injecting `Storage` trait wrapper |
| `perf` | `cargo flamegraph` (dtrace) / samply |
| real multi-VM network | **L2 deferred**; localhost numbers labelled "localhost" |

Note: macOS `fsync` does not flush the drive cache; durable flush needs `fcntl(F_FULLFSYNC)`. The engine's fsync policy must use `F_FULLFSYNC` for "strict" and benchmarks must label which was used.
