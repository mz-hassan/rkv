# distributed-kv-store — project initialization

**What this is:** a from-scratch **distributed, linearizable, transactional key-value store in Rust** — LSM-tree storage engine, Raft consensus (sans-IO, so it runs in a deterministic simulator), multi-Raft sharding with a placement driver, Percolator-style distributed transactions, and an unusually strong verification story (deterministic simulation testing, linearizability checking, Jepsen-style chaos, fuzzing). Built to **diversify the resume** with deep systems work: storage engines, consensus, distributed transactions, correctness under failure.

**Status:** empty on purpose. An AI agent generates the code from the prompts in `prompts/`.

## Folder layout

```
distributed-kv-store/
├── README.md                          ← you are here
├── prompts/
│   ├── 01_requirements_and_plan.md    ← session 1: planning docs only (no code)
│   └── 02_build_codebase.md           ← session 2: build the full codebase, local-first
└── docs/                              ← session 1 writes the plan here (10 docs)
```

## How to run it (two fresh agent sessions)

1. `cd ~/Documents/projects/distributed-kv-store` and start your agent (e.g. `claude`).
2. **Session 1 — plan.** Paste `prompts/01_requirements_and_plan.md`. The agent audits your environment and writes `REQUIREMENTS`, `ARCHITECTURE`, `VERIFICATION_PLAN`, `BENCHMARKS`, `EXECUTION_MATRIX`, `TASKS`, `RESUME_TEMPLATES`, `RISKS`, `DECISIONS` and `LEARNING_GUIDE` into `docs/`.
   - **Review `BENCHMARKS.md`, `VERIFICATION_PLAN.md` and `EXECUTION_MATRIX.md`** — they determine which resume claims you will be able to make.
3. **Session 2 — build.** Start a **fresh** session in the same folder and paste `prompts/02_build_codebase.md`. It reads `docs/` and builds **all tiers** milestone by milestone, running and verifying everything that works locally.
4. Install what the agent reports (typically `rustup`, `protoc`, `cargo-nextest`, `cargo-fuzz`; Docker Desktop, `kind`, `kubectl`, `toxiproxy`; Go only if cross-checking with Porcupine).
5. Run `make test sim bench-local`, then `make results resume` to generate results and resume bullets **from measured data only**.
6. Read `docs/LEARNING_GUIDE.md` before interviews — the project is only worth putting on a resume if you can explain Raft, the LSM design and the verification approach.

## Execution levels

| Level | Needs | Examples |
|---|---|---|
| L0 | nothing external | engine + model tests, Raft in the deterministic simulator, linearizability checker, fuzzing, single-node YCSB |
| L1 | local tools (Docker, toxiproxy, kind) | multi-process clusters, `kill -9`/pause chaos, RocksDB/etcd baselines, Prometheus/Grafana, Kubernetes on kind |
| L2 | Linux hosts / multiple VMs | `tc netem`, `iptables`, `dm-flakey`, `perf`, real-network benchmarks |
| L3 | cloud accounts | multi-AZ deployment and scale runs |

L2/L3 are **fully coded** in session 2 with a dry-run test; exact commands are in `docs/DEFERRED_RUNS.md`.

## Ground rules (baked into the prompts)

- **Full scope:** all tiers (Core, Extended, Stretch) are built; tiers only set the order.
- **No fabricated numbers:** results come from raw JSON in `bench/results/`; unrun experiments print `NOT RUN`.
- **Honesty labels** on every metric (hardware, localhost vs multi-host, simulated vs real faults, baseline versions).
- **Every bug found is logged** in `docs/BUGS_FOUND.md` — these are resume material.

## Definition of done

- `make lint test sim` green; thousands of simulation seeds run with zero invariant violations; linearizability checker validated by mutation tests.
- Benchmarks S1, S2, R1–R3, D1, D2 (and K1 on kind) have real results; L2/L3 items are coded with dry-runs.
- At least 5 resume bullets are claimable from measured data, and you can explain the design in an interview.
