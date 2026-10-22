# rkv - distributed transactional KV store.  `make help` lists targets.
SHELL := /bin/bash
export PATH := /opt/homebrew/opt/rustup/bin:$(HOME)/.cargo/bin:$(PATH)
export PROTOC ?= /opt/homebrew/bin/protoc
SEEDS ?= 2000
SECS ?= 600
SCALE ?= full

.PHONY: help build test test-loom sim fuzz bench-local chaos cluster-up cluster-down smoke results resume lint fmt env bench-s1 bench-s2 bench-r1 bench-r2 bench-r3 bench-d1 bench-d2 bench-k1 bench-n1 bench-etcd clean dry-run

help: ## list targets
	@grep -E '^[a-zA-Z0-9_-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-14s %s\n", $$1, $$2}'

env: ## check toolchain, print install commands for what is missing
	@scripts/env-check.sh

build: ## build all binaries (release)
	cargo build --release --workspace

test: ## unit, property, model and integration tests (no cluster benchmarks)
	cargo test --workspace --release

test-loom: ## loom model of the engine's group-commit protocol
	RUSTFLAGS="--cfg loom" cargo test -p kv-engine --test loom_group_commit --release

sim: build ## deterministic simulation: SEEDS=2000 seeds on every nemesis profile
	SCALE=$(SCALE) bash bench/scripts/sim.sh

fuzz: ## fuzz all decoders, SECS per target (needs nightly + cargo-fuzz)
	SECS=$(SECS) bash bench/scripts/fuzz.sh

smoke: build ## 40 s end-to-end smoke with real processes (raw, txn, split, failover)
	scripts/smoke.sh

bench-s1: build ## S1 engine vs RocksDB (RKV_ROCKS=1 for the baseline), fsync/cache/compaction sweeps, recovery
	bash bench/scripts/s1.sh
bench-s2: build ## S2 crash consistency (simulated power loss, SIGKILL, cluster kill -9)
	bash bench/scripts/s2.sh
bench-r1: build ## R1 replicated throughput/latency (+ bench-etcd for the baseline)
	bash bench/scripts/r1.sh
bench-etcd: build ## R1 etcd baseline (cargo build -p ycsb --features etcd-baseline first)
	bash bench/scripts/etcd_baseline.sh
bench-r2: build ## R2 failover windows, election timeout sweep, PreVote/CheckQuorum, snapshot catch-up
	bash bench/scripts/r2.sh
bench-r3: build ## R3 linearizability under faults (simulation + real processes + mutation tests)
	bash bench/scripts/r3.sh
bench-d1: build ## D1 sharding: scaling with ranges, split under load, hot range
	bash bench/scripts/d1.sh
bench-d2: build ## D2 transactions: contention, write skew, lock recovery, bank under chaos
	bash bench/scripts/d2.sh
bench-k1: ## K1 Kubernetes on kind (needs docker/colima, kind, kubectl)
	bash bench/scripts/k1.sh
bench-n1: ## N1 real-network runs on Linux hosts (HOSTS="h1 h2 h3"); DEFERRED
	bash bench/scripts/n1.sh
bench-local: build ## everything runnable on one Mac, in order (hours at SCALE=full)
	bash bench/scripts/run-all.sh
chaos: build ## real-process chaos: kill -9 / pause / partition / ring / skew + linearizability check
	SCALE=$(SCALE) bash bench/scripts/r3.sh

dry-run: ## print every command of every benchmark script without running anything
	@for s in sim s1 s2 r1 r2 r3 d1 d2 k1 n1 etcd_baseline fuzz; do DRY_RUN=1 SCALE=smoke bash bench/scripts/$$s.sh >/dev/null && echo "dry-run ok: $$s" || echo "dry-run FAILED: $$s"; done

cluster-up: build ## local 3-store cluster with a PD (see scripts/cluster.sh)
	scripts/cluster.sh up 3 pd
cluster-down: ## stop the local cluster
	scripts/cluster.sh down

results: ## aggregate bench/results/**/result.json -> RESULTS.md + plots (numbers come ONLY from raw files)
	.venv/bin/python bench/scripts/results.py
resume: results ## generate RESUME_BULLETS.generated.md from real data only
	.venv/bin/python bench/scripts/resume.py

lint: ## clippy (deny warnings) + rustfmt check
	cargo clippy --workspace --all-targets -- -D warnings
	cargo fmt --all -- --check
fmt: ## format the workspace
	cargo fmt --all
clean:
	cargo clean
