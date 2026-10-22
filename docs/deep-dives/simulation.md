# Deterministic simulation deep dive (`crates/sim`, `invariants`, `lincheck`)

A run is a pure function of `SimConfig` (seed, profile, voters, duration, optional explicit fault schedule). Single thread, virtual
millisecond clock, three seeded RNG streams (network, workload, nemesis). Nodes are the real `Raft` + `RaftStorage` over `MemFs`.
* **Network:** per-message latency, drop, duplicate, reorder (via random latency), symmetric partitions, messages go through the real codec.
* **Disk:** node crash = power loss (`MemFs::crash`): unsynced data lost/torn; injected fsync failure makes the node fail-stop.
* **Clock:** per-node tick rate drift.
* **Nemesis:** explicit, seed-generated list of timed actions (so a failing run can be minimised by deleting actions).
* **Clients:** concurrent register/CAS/delete clients with session ids and retries; operations that time out stay *pending* in the history.
* **Checks:** Raft invariants on every step; log matching every 250 virtual ms; Wing-Gong/Lowe linearizability of the final history
  (with a final read of every key after healing, which detects lost acknowledged writes); liveness after healing.

**Mutation testing** (`--bug`): application bugs (stale reads, no dedup, ack before commit), storage (vote not persisted) and
protocol bugs inside `raft` (`RaftBug`). `crates/sim/tests/sweep.rs` asserts that the harness catches the detectable ones.
Honest gap: the Figure-8 commit bug is not caught (BUGS_FOUND.md).
