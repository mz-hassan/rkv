#!/usr/bin/env python3
"""Generate resume bullets from MEASURED data only. A bullet is emitted only when its evidence meets the
threshold in docs/RESUME_TEMPLATES.md; otherwise the line says "NOT RUN - cannot claim".
Each bullet carries its honesty labels as a comment line."""
import glob, importlib.util, os, re, sys

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("results", os.path.join(HERE, "results.py"))
R = importlib.util.module_from_spec(spec); spec.loader.exec_module(R)
ROOT = R.ROOT
by, sims = R.load()
L = R.latest
out, n_claim = [], 0


def bullet(n, text, labels, ok, need):
    global n_claim
    if ok:
        n_claim += 1
        out.append(f"{n}. {text}\n   <!-- labels: {labels} -->  ({len(text)} chars)")
    else:
        out.append(f"{n}. NOT RUN - cannot claim ({need})")


clean = [s for s in sims if not s.get("bug_injected")]
seeds = sum(s["seeds_run"] for s in clean); hours = sum(s["simulated_hours"] for s in clean); viol = sum(s["failures"] for s in clean)

# 1 architecture (claimable once the code builds and sim ran)
bullet(1, "Built a Raft-replicated, linearizable KV store in Rust (LSM engine, PreVote, snapshots, joint consensus)",
       "implementation exists; see tests", seeds >= 1000, "needs >= 1000 simulated seeds")
# 2 simulation
bullet(2, f"Verified Raft by deterministic simulation: {seeds:,} seeds, {hours:.0f} simulated hrs, {viol} safety violations",
       "simulated faults (network, torn-write power loss, fsync failure, clock drift); same raft crate as production", seeds >= 1000, "run make sim")
# 3 checker mutation
mut = [s for s in sims if s.get("bug_injected")]
bugs = {}
for s in mut:
    b = bugs.setdefault(s["bug_injected"].strip('"'), 0); bugs[s["bug_injected"].strip('"')] = b + s["failures"]
caught = sum(1 for v in bugs.values() if v > 0)
bullet(3, f"Wrote a Wing-Gong/Lowe linearizability checker; it caught {caught}/{len(bugs)} deliberately injected bugs",
       "mutation testing in simulation; bugs listed in RESULTS.md", len(bugs) >= 5, "needs >= 5 injected bugs run")
# 4 vs RocksDB
a, b = L(by, "S1-ycsb-a-rkv"), L(by, "S1-ycsb-a-rocksdb")
bullet(4, f"Engine reaches {100*a['results']['ops_per_sec']/b['results']['ops_per_sec']:.0f}% of RocksDB YCSB-A throughput (single node, same settings)" if a and b else "", "single node, laptop, closed-loop clients; rocksdb crate version in result file", bool(a and b), "S1 for both backends")
# 5 failover
r = L(by, "R2-kill")
ok5 = bool(r and r["results"]["completed_trials"] >= 100)
bullet(5, f"Measured leader failover: p50/p99 = {r['results']['unavailability_ms']['p50']}/{r['results']['unavailability_ms']['p99']} ms over {r['results']['completed_trials']} kill -9 trials" if ok5 else "", "3-node localhost multi-process cluster; client-observed window", ok5, "needs >= 100 trials")
# 6 durability
s2s, s2k = L(by, "S2-sim"), L(by, "S2-sigkill")
ok6 = bool(s2s and s2k and s2s["results"]["iterations"] >= 1000 and s2k["results"]["iterations"] >= 500)
bullet(6, f"Zero acked-write loss across {s2s['results']['iterations'] + s2k['results']['iterations']:,} crash-recovery iterations (simulated power loss + SIGKILL)" if ok6 else "", "simulated power loss (MemFs) + real SIGKILL; neither is a real power-cut test", ok6 and s2s["results"]["violations"] + s2k["results"]["violations"] == 0, "needs >= 1000 sim + >= 500 SIGKILL iterations with 0 violations")
# 7 transactions
banks = [r for i in R.ids(by, "J-bank") for r in by[i]]
ok7 = bool(banks) and all(r["results"]["invariant_ok"] for r in banks)
bullet(7, f"Implemented Percolator transactions (snapshot isolation); bank invariant held across {len(banks)} chaos runs" if ok7 else "", "localhost cluster; real kill -9 / pause; transport-layer partitions", ok7, "run bench/scripts/d2.sh")
# 8 sharding
p1, p4 = L(by, "D1-ranges1-mixa"), L(by, "D1-ranges4-mixa")
bullet(8, f"Multi-Raft range sharding with live splits: {p4['results']['ops_per_sec']/p1['results']['ops_per_sec']:.1f}x throughput 1 -> 4 ranges (localhost)" if p1 and p4 else "", "3 stores on one machine: cores are shared, so scaling is bounded", bool(p1 and p4), "D1")
# 9 vs etcd
e, k = L(by, "R1-etcd-3n-mixw-v128"), L(by, "R1-3n-mixw-v128")
bullet(9, f"Replicated write throughput {k['results']['ops_per_sec']:.0f} ops/s vs etcd {e['results']['ops_per_sec']:.0f} (3 nodes, localhost, fsync on)" if e and k else "", "single machine; etcd native binary; identical workload via etcd-client", bool(e and k), "R1 + etcd baseline")
# 10 fuzz
fz = [L(by, i) for i in R.ids(by, "fuzz-")]
secs = sum(f["results"]["seconds"] for f in fz); crashes = sum(f["results"]["crashes"] for f in fz)
bullet(10, f"Fuzzed WAL/SSTable/Raft/protobuf decoders for {secs/3600:.1f} h ({sum(f['results']['runs'] for f in fz):,} execs)" if fz else "", "libFuzzer via cargo-fuzz", secs >= 3600, "needs >= 1 hour total")
# 11 k8s
k1 = L(by, "K1-rolling-upgrade")
bullet(11, "Wrote a Kubernetes operator (kube-rs) for rolling upgrades with zero acked-write loss on kind" if k1 and k1["results"]["invariant_ok"] else "", "kind, single host", bool(k1 and k1["results"]["invariant_ok"]), "K1")
# 12 bugs: count only entries FOUND BY TESTING (property/model/integration/mutation/smoke/sanity checks). Entries found by code
# review or by the compiler are in BUGS_FOUND.md for honesty but are not claimed as testing wins.
bugs_md = open(os.path.join(ROOT, "docs", "BUGS_FOUND.md")).read() if os.path.exists(os.path.join(ROOT, "docs", "BUGS_FOUND.md")) else ""
entries = re.split(r"^## BUG-", bugs_md, flags=re.M)[1:]
test_kw = re.compile(r"(property test|model-based|integration test|mutation testing|smoke|sanity check|simulation|fault-injection|K1|churn test|cluster test|Azure|benchmark run|real runs|audit|first real run)", re.I)
# harness / tooling / benchmark-integrity entries are real bugs but not product bugs: logged, not claimed
tool_kw = re.compile(r"(\(tooling\)|\(test harness\)|\(benchmark integrity\)|benchmark/tooling|benchmark and cloud-script|benchmark harness|fsync on macOS|disk-fault injection|checker could use unbounded memory|simulator livelock)", re.I)
nb = 0
for e in entries:
    m = re.search(r"\*\*Found by:\*\*(.*?)(?:\n- \*\*|\n\n)", e, re.S)
    if tool_kw.search(e.split("\n", 1)[0]):
        continue
    if m and test_kw.search(m.group(1)) and not re.search(r"^(code review|the compiler)", m.group(1).strip(), re.I):
        nb += 1
bullet(12, f"Found and fixed {nb} bugs through property-based, simulation, integration and cluster testing (product bugs only; root causes logged)" if nb else "", "see docs/BUGS_FOUND.md for how each was found", nb >= 3, "log test-found bugs in docs/BUGS_FOUND.md")

md = "# RESUME_BULLETS.generated.md\n\nGenerated by `make resume` from bench/results only. Lines marked NOT RUN must not go on a resume.\nKeep the honesty labels in interview notes.\n\n" + "\n\n".join(out) + f"\n\n{n_claim} of 12 bullets are claimable from current data.\n"
open(os.path.join(ROOT, "RESUME_BULLETS.generated.md"), "w").write(md)
print(f"wrote RESUME_BULLETS.generated.md ({n_claim}/12 claimable)")
