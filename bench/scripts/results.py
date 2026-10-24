#!/usr/bin/env python3
"""Aggregate raw bench results into RESULTS.md and plots.

Rules (see docs/BENCHMARKS.md): numbers come ONLY from bench/results/**/result.json (and sim summaries);
experiments without data print "NOT RUN". Nothing here is hard-coded.
"""
import glob, json, os, sys, time
from collections import defaultdict

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RES = os.environ.get("RKV_RESULTS_DIR", os.path.join(ROOT, "bench", "results"))
PLOTS = os.path.join(ROOT, "bench", "plots")
OUT = os.environ.get("RKV_RESULTS_MD", os.path.join(ROOT, "RESULTS.md"))


def load():
    """id -> list of results (oldest first)."""
    by = defaultdict(list)
    for f in glob.glob(os.path.join(RES, "**", "result.json"), recursive=True):
        try:
            r = json.load(open(f))
        except Exception:
            continue
        r["_file"] = os.path.relpath(f, ROOT)
        by[r.get("id", "?")].append(r)
    for v in by.values():
        v.sort(key=lambda r: int(r.get("timestamp_unix", 0)))
    sims = []
    for f in glob.glob(os.path.join(RES, "**", "sim-*.json"), recursive=True):
        try:
            sims.append(json.load(open(f)))
        except Exception:
            pass
    return by, sims


def latest(by, id_):
    return by[id_][-1] if by.get(id_) else None


def ids(by, prefix):
    return sorted(i for i in by if i.startswith(prefix))


def fmt(x, nd=1):
    if x is None:
        return "NOT RUN"
    if isinstance(x, float):
        return f"{x:,.{nd}f}"
    if isinstance(x, int):
        return f"{x:,}"
    return str(x)


def us2ms(x):
    return None if x is None else x / 1000.0


def table(headers, rows):
    if not rows:
        return "_NOT RUN_\n"
    out = "| " + " | ".join(headers) + " |\n|" + "|".join("---" for _ in headers) + "|\n"
    for r in rows:
        out += "| " + " | ".join(str(c) for c in r) + " |\n"
    return out + "\n"


def labels_note(rs):
    seen = []
    for r in rs:
        l = r.get("labels", {})
        s = "; ".join(f"{k}: {v}" for k, v in l.items() if k in ("scope", "faults", "baseline", "metric", "caveat", "clients"))
        if s and s not in seen:
            seen.append(s)
    hw = rs[0].get("hardware", {}) if rs else {}
    commit = rs[0].get("git_commit", "?") if rs else "?"
    note = ""
    if seen:
        note += "\n".join(f"> **Labels:** {s}" for s in seen[:3]) + "\n"
    if rs:
        note += f"> **Hardware:** {hw.get('cpu','?')}, {hw.get('cores','?')} cores, {hw.get('os','?')}; commit `{commit}`\n"
    return note + "\n"


def lat(r, key="latency_all"):
    l = r["results"].get(key, {})
    return [fmt(us2ms(l.get("p50_us")), 2), fmt(us2ms(l.get("p99_us")), 2), fmt(us2ms(l.get("p999_us")), 2)]


def section_s1(by):
    o = "## S1 - single-node engine\n\n"
    rows, rs = [], []
    for w in "abcdef":
        for be in ("rkv", "rocksdb"):
            r = latest(by, f"S1-ycsb-{w}-{be}")
            if not r:
                continue
            rs.append(r)
            e = r["results"]["engine"]
            rows.append([w.upper(), be, fmt(r["results"]["ops_per_sec"], 0), *lat(r), fmt(e.get("write_amp_run_phase"), 2), fmt(r["results"].get("space_amp"), 2), fmt(e.get("cache_hit_ratio"), 3)])
    o += "### YCSB A-F (ops/s, latency ms, write amp, space amp)\n\n" + table(["wl", "backend", "ops/s", "p50", "p99", "p99.9", "write amp", "space amp", "cache hit"], rows) + labels_note(rs)
    rows = []
    for w in "abcdef":
        a, b = latest(by, f"S1-ycsb-{w}-rkv"), latest(by, f"S1-ycsb-{w}-rocksdb")
        if a and b:
            rows.append([w.upper(), fmt(a["results"]["ops_per_sec"], 0), fmt(b["results"]["ops_per_sec"], 0), fmt(100 * a["results"]["ops_per_sec"] / b["results"]["ops_per_sec"], 0) + "%"])
    o += "### vs RocksDB (same machine, same settings class)\n\n" + table(["wl", "rkv ops/s", "rocksdb ops/s", "rkv / rocksdb"], rows)
    rows = [[i.replace("S1-fsync-", "").replace("-rkv", ""), fmt(latest(by, i)["results"]["ops_per_sec"], 0), *lat(latest(by, i), "latency_all")] for i in ids(by, "S1-fsync-")]
    o += "### fsync policy trade-off (workload A)\n\n" + table(["policy", "ops/s", "p50 ms", "p99 ms", "p99.9 ms"], rows)
    rows = []
    for i in ids(by, "S1-cache-"):
        r = latest(by, i)
        rows.append([i.replace("S1-cache-", ""), fmt(r["results"]["ops_per_sec"], 0), fmt(r["results"]["engine"]["cache_hit_ratio"], 3)])
    o += "### block cache size vs hit ratio (read-only, Zipfian)\n\n" + table(["cache", "ops/s", "hit ratio"], rows)
    rows = []
    for i in ids(by, "S1-compaction-"):
        r = latest(by, i); e = r["results"]["engine"]
        rows.append([i.replace("S1-compaction-", ""), fmt(e["write_amp_run_phase"], 2), fmt(r["results"]["space_amp"], 2), fmt(e["blocks_read"], 0), fmt(r["results"]["ops_per_sec"], 0)])
    o += "### leveled vs size-tiered\n\n" + table(["style", "write amp", "space amp", "blocks read", "ops/s"], rows)
    rows = []
    for i in ids(by, "S1-ycsb-c-rkv") + ids(by, "S1-ycsb-a-rkv"):
        e = latest(by, i)["results"]["engine"]
        rows.append([i, fmt(e["bloom_measured_fpr"] * 100, 2) + "%", fmt(e["bloom_theoretical_fpr"] * 100, 2) + "%"])
    o += "### Bloom filter false-positive rate (measured vs theoretical at 10 bits/key)\n\n" + table(["run", "measured", "theoretical"], rows)
    rows = []
    for i in ids(by, "S1-recovery-"):
        r = latest(by, i)["results"]
        rows.append([i.replace("S1-recovery-", ""), fmt(r["wal_bytes"] / 1e6, 1) + " MB", fmt(r["recovery_secs"], 2) + " s", r["data_intact"]])
    o += "### recovery time vs WAL size\n\n" + table(["target", "WAL", "recovery", "data intact"], rows)
    return o


def section_s2(by, sims):
    o = "## S2 - crash consistency\n\n"
    rows = []
    for i in ("S2-sim", "S2-sigkill", "S2-cluster"):
        r = latest(by, i)
        if not r:
            rows.append([i, "NOT RUN", "", ""]); continue
        x = r["results"]
        checked = x.get("acked_keys_checked") or x.get("acked_keys_checked_cumulative") or x.get("acked_writes")
        bad = x.get("violations", x.get("lost_acked_writes"))
        rows.append([i, x.get("iterations", r["config"].get("iterations")), checked, bad])
    return o + table(["experiment", "iterations", "acked keys checked", "violations / lost"], rows) + labels_note([latest(by, i) for i in ("S2-sim", "S2-sigkill", "S2-cluster") if latest(by, i)])


def section_r1(by):
    o = "## R1 - replicated throughput and latency\n\n"
    rows, rs = [], []
    for i in ids(by, "R1-"):
        r = latest(by, i); rs.append(r)
        x = r["results"]
        rows.append([i, r["config"].get("backend", "rkv"), fmt(x["ops_per_sec"], 0), *lat(r), x["errors"]])
    o += table(["run", "backend", "ops/s", "p50 ms", "p99 ms", "p99.9 ms", "errors"], rows)
    rows = []
    for n in (1, 3, 5):
        for mix in "war":
            for sz in (128, 1024, 16384):
                a, b = latest(by, f"R1-{n}n-mix{mix}-v{sz}"), latest(by, f"R1-etcd-{n}n-mix{mix}-v{sz}")
                if a and b:
                    rows.append([n, mix, sz, fmt(a["results"]["ops_per_sec"], 0), fmt(b["results"]["ops_per_sec"], 0), fmt(100 * a["results"]["ops_per_sec"] / b["results"]["ops_per_sec"], 0) + "%"])
    o += "### rkv vs etcd (same workload, localhost, same machine)\n\n" + table(["nodes", "mix", "value B", "rkv ops/s", "etcd ops/s", "rkv / etcd"], rows) + labels_note(rs)
    return o


def section_r2(by):
    o = "## R2 - fault recovery\n\n"
    rows, rs = [], []
    for i in ids(by, "R2-") :
        r = latest(by, i)
        if r["bench"] != "failover-window":
            continue
        rs.append(r)
        u = r["results"]["unavailability_ms"]
        c = r["config"]
        rows.append([i, c["fault"], r["results"]["completed_trials"], f"{c['election_tick']*c['tick_ms']}-{2*c['election_tick']*c['tick_ms']}", c["prevote"], c["check_quorum"], u["p50"], u["p95"], u["p99"], u["max"]])
    o += "### unavailability window after a leader fault (client-observed, ms)\n\n" + table(["run", "fault", "trials", "election timeout ms", "prevote", "checkquorum", "p50", "p95", "p99", "max"], rows) + labels_note(rs)
    rows = []
    for i in ids(by, "R2-catchup"):
        x = latest(by, i)["results"]; c = latest(by, i)["config"]
        rows.append([c["data_mb"], fmt(x.get("catchup_secs"), 2), x["caught_up"]])
    o += "### snapshot catch-up of a lagging replica\n\n" + table(["data MB", "catch-up s", "caught up"], rows)
    return o


def section_r3(by, sims):
    o = "## R3 - linearizability under faults\n\n### deterministic simulation (same Raft code as production)\n\n"
    agg = defaultdict(lambda: dict(runs=0, seeds=0, fail=0, hours=0.0, ops=0, elections=0, crashes=0))
    mut = defaultdict(lambda: dict(runs=0, seeds=0, fail=0))
    for s in sims:
        if s.get("bug_injected"):
            m = mut[(s["bug_injected"], s["profile"])]
            m["runs"] += 1; m["seeds"] += s["seeds_run"]; m["fail"] += s["failures"]
        else:
            a = agg[s["profile"]]
            a["runs"] += 1; a["seeds"] += s["seeds_run"]; a["fail"] += s["failures"]; a["hours"] += s["simulated_hours"]
            a["ops"] += s["ops_total"]; a["elections"] += s["elections"]; a["crashes"] += s["crashes"]
    rows = [[p, a["seeds"], f"{a['hours']:.1f}", f"{a['ops']:,}", f"{a['elections']:,}", f"{a['crashes']:,}", a["fail"]] for p, a in sorted(agg.items())]
    tot = sum(a["seeds"] for a in agg.values()); hrs = sum(a["hours"] for a in agg.values())
    o += table(["profile", "seeds", "simulated hours", "client ops checked", "elections", "node crashes", "violations"], rows)
    if rows:
        o += f"**Total: {tot:,} seeds, {hrs:.1f} simulated hours.**\n\n"
    rows = [[b, p, m["seeds"], m["fail"], "CAUGHT" if m["fail"] > 0 else "NOT caught"] for (b, p), m in sorted(mut.items())]
    o += "### mutation testing of the verification suite (injected bugs; each should be CAUGHT)\n\n" + table(["injected bug", "profile", "seeds", "seeds that failed", "verdict"], rows)
    rows = []
    for nem in ("kill-leader", "pause", "partition-half", "ring", "rolling", "mixed", "skew"):
        rs = [r for i in ids(by, f"J-{nem}-seed") for r in by[i]]
        if rs:
            viol = sum(1 for r in rs if r["results"]["violation"])
            ops = sum(r["results"]["ops"] for r in rs)
            rows.append([nem, len(rs), f"{ops:,}", viol])
    o += "### real-process chaos histories checked by lincheck\n\n" + table(["nemesis", "histories", "ops", "violations"], rows)
    return o


def section_d1(by):
    o = "## D1 - sharding\n\n"
    rows, rs = [], []
    for i in ids(by, "D1-ranges"):
        r = latest(by, i); rs.append(r)
        rows.append([i, fmt(r["results"]["ops_per_sec"], 0), *lat(r)])
    o += table(["run", "ops/s", "p50 ms", "p99 ms", "p99.9 ms"], rows) + labels_note(rs)
    for i in ("D1-split-under-load", "D1-hot-range"):
        r = latest(by, i)
        if r:
            tl = r["results"].get("timeline", [])
            o += f"**{i}** per-second timeline: " + ", ".join(f"{t['sec']}s:{t['ok']}ok/{t['max_latency_us']/1000:.0f}msmax" for t in tl) + "\n\n"
        else:
            o += f"**{i}**: NOT RUN\n\n"
    return o


def section_d2(by):
    o = "## D2 - transactions\n\n"
    rows, rs = [], []
    for i in ids(by, "D2-bank"):
        r = latest(by, i); rs.append(r)
        x = r["results"]; c = r["config"]
        rows.append([i, c["isolation"], c["theta"], c["clients"], fmt(x["commits_per_sec"], 0), fmt(100 * x["abort_rate"], 1) + "%", fmt(us2ms(x["commit_latency"].get("p99_us")), 1), x["invariant_ok"]])
    o += table(["run", "isolation", "theta", "clients", "commits/s", "abort rate", "p99 ms", "bank invariant"], rows) + labels_note(rs)
    r = latest(by, "D2-write-skew")
    rows = [[k, v["pairs"], v["write_skew_violations"]] for k, v in r["results"].items()] if r else []
    o += "### write skew (on-call doctors)\n\n" + table(["isolation", "pairs", "skewed pairs"], rows)
    r = latest(by, "D2-lock-recovery")
    if r:
        l = r["results"]["time_to_unblocked_read"]
        o += f"### lock recovery after coordinator crash (ttl {r['config']['lock_ttl_ms']} ms, {r['results']['trials']} trials)\n\np50 {fmt(us2ms(l['p50_us']),0)} ms, p99 {fmt(us2ms(l['p99_us']),0)} ms, failures {r['results']['failures']}\n\n"
    else:
        o += "### lock recovery\n\n_NOT RUN_\n\n"
    rows = []
    for i in ids(by, "J-bank"):
        for r in by[i]:
            x = r["results"]
            rows.append([i, x["committed_transfers"], x["faults"], x["bad_audits"], x["invariant_ok"]])
    o += "### bank invariant under chaos\n\n" + table(["run", "transfers", "faults", "bad audits", "invariant ok"], rows)
    return o


def section_soak(by):
    o = "## S3 - soak (memory under sustained load)\n\n"
    r = latest(by, "S3-soak")
    if not r:
        return o + "_NOT RUN_\n\n"
    rows = [[p, v["samples"], f"{v['rss_kb_start']//1024} MB", f"{v['rss_kb_end']//1024} MB", f"{v['rss_kb_max']//1024} MB", f"{v['second_half_growth_pct']}%"] for p, v in r["results"]["per_process"].items()]
    return o + table(["pid", "samples", "RSS start", "RSS end", "RSS max", "growth in 2nd half"], rows) + labels_note([r])


def section_n1(by):
    o = "## N1 - kernel-level faults on Linux (netem, iptables, dm-flakey, perf)\n\n"
    rows, rs = [], []
    for i in ids(by, "N1-r1-"):
        r = latest(by, i); rs.append(r); x = r["results"]
        import re as _re
        m = _re.search(r"election timeout (\d+)x", r["labels"].get("scope", ""))
        rows.append([i.replace("N1-r1-", ""), (m.group(1) + "x20ms") if m else "10x20ms", fmt(x["ops_per_sec"], 0), *lat(r), x["errors"]])
    o += "### R1 under injected network delay / loss (tc netem)\n\n" + table(["setting", "election timeout", "ops/s", "p50 ms", "p99 ms", "p99.9 ms", "errors"], rows) + labels_note(rs)
    r = latest(by, "N1-r2-partition")
    if r:
        u = r["results"]["unavailability_ms"]
        o += f"### leader isolation with iptables ({r['results']['completed_trials']} trials)\n\nclient-observed window: p50 {u['p50']} ms, p95 {u['p95']} ms, p99 {u['p99']} ms, max {u['max']} ms\n\n" + labels_note([r])
    else:
        o += "### leader isolation with iptables\n\n_NOT RUN_\n\n"
    r = latest(by, "N1-disk-flakey")
    if r:
        x = r["results"]
        o += f"### disk faults (dm-flakey on one node)\n\nclient ok ops {x['ok_ops']:,}, client errors {x['errors']}; the faulty node must fail-stop (see logs) while the others keep serving.\n\n" + labels_note([r])
    else:
        o += "### disk faults\n\n_NOT RUN_\n\n"
    return o


def section_misc(by):
    o = "## K1 - Kubernetes (kind)\n\n"
    r = latest(by, "K1-rolling-upgrade")
    o += "_NOT RUN_ (needs Docker/colima + kind)\n\n" if not r else f"bank invariant ok: {r['results']['invariant_ok']}; commits {r['results']['commits']}\n\n"
    o += "## N1 on separate real machines\n\n_NOT RUN_ (the simulated-machine version above ran on one Azure VM; real multi-VM runs are deferred, see docs/DEFERRED_RUNS.md)\n\n"
    o += "## Fuzzing\n\n"
    rows = []
    for i in ids(by, "fuzz-"):
        r = latest(by, i)["results"]
        rows.append([i.replace("fuzz-", ""), f"{r['runs']:,}", f"{r['seconds']:.0f}", r["crashes"]])
    o += table(["target", "executions", "seconds", "crashes"], rows)
    return o


def plots(by):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except Exception:
        return ["_matplotlib not installed: run `python3 -m venv .venv && .venv/bin/pip install matplotlib numpy`; plots skipped_"]
    os.makedirs(PLOTS, exist_ok=True)
    made = []
    # S1 latency percentiles
    xs, p50, p99, p999 = [], [], [], []
    for w in "abcdef":
        r = latest(by, f"S1-ycsb-{w}-rkv")
        if r:
            l = r["results"]["latency_all"]; xs.append(w.upper()); p50.append(l["p50_us"] / 1000); p99.append(l["p99_us"] / 1000); p999.append(l["p999_us"] / 1000)
    if xs:
        fig, ax = plt.subplots(figsize=(6, 3.5)); n = range(len(xs))
        ax.bar([i - 0.25 for i in n], p50, 0.25, label="p50"); ax.bar(list(n), p99, 0.25, label="p99"); ax.bar([i + 0.25 for i in n], p999, 0.25, label="p99.9")
        ax.set_xticks(list(n)); ax.set_xticklabels(xs); ax.set_yscale("log"); ax.set_ylabel("ms"); ax.set_title("S1 engine latency by YCSB workload"); ax.legend()
        fig.tight_layout(); fig.savefig(os.path.join(PLOTS, "s1_latency.png"), dpi=130); plt.close(fig); made.append("s1_latency.png")
    # R1 throughput vs nodes
    fig, ax = plt.subplots(figsize=(6, 3.5)); any_ = False
    for mix in "war":
        for be, pre in (("rkv", "R1-"), ("etcd", "R1-etcd-")):
            pts = [(n, latest(by, f"{pre}{n}n-mix{mix}-v128")) for n in (1, 3, 5)]
            pts = [(n, r["results"]["ops_per_sec"]) for n, r in pts if r]
            if pts:
                any_ = True; ax.plot([p[0] for p in pts], [p[1] for p in pts], marker="o", linestyle="-" if be == "rkv" else "--", label=f"{be} mix={mix}")
    if any_:
        ax.set_xlabel("nodes"); ax.set_ylabel("ops/s"); ax.set_title("R1 throughput (128 B values, localhost)"); ax.legend(fontsize=7)
        fig.tight_layout(); fig.savefig(os.path.join(PLOTS, "r1_throughput.png"), dpi=130); made.append("r1_throughput.png")
    plt.close(fig)
    # R2 unavailability histograms
    fig, ax = plt.subplots(figsize=(6, 3.5)); any_ = False
    for fault in ("kill", "pause", "partition"):
        r = latest(by, f"R2-{fault}")
        if r:
            any_ = True; ax.hist(r["results"]["samples_ms"], bins=30, alpha=0.5, label=fault)
    if any_:
        ax.set_xlabel("unavailability window (ms)"); ax.set_ylabel("trials"); ax.set_title("R2 failover window"); ax.legend()
        fig.tight_layout(); fig.savefig(os.path.join(PLOTS, "r2_failover.png"), dpi=130); made.append("r2_failover.png")
    plt.close(fig)
    # D1 throughput vs ranges
    pts = []
    for n in (1, 4, 16, 64):
        r = latest(by, f"D1-ranges{n}-mixa")
        if r:
            pts.append((n, r["results"]["ops_per_sec"]))
    if pts:
        fig, ax = plt.subplots(figsize=(6, 3.5)); ax.plot(*zip(*pts), marker="o"); ax.set_xscale("log", base=2); ax.set_xlabel("ranges"); ax.set_ylabel("ops/s"); ax.set_title("D1 throughput vs ranges (3 stores, localhost)")
        fig.tight_layout(); fig.savefig(os.path.join(PLOTS, "d1_scaling.png"), dpi=130); plt.close(fig); made.append("d1_scaling.png")
    # D1 timeline
    r = latest(by, "D1-split-under-load")
    if r and r["results"].get("timeline"):
        tl = r["results"]["timeline"]
        fig, ax = plt.subplots(figsize=(6, 3.5)); ax.plot([t["sec"] for t in tl], [t["ok"] for t in tl], label="ok ops/s"); ax2 = ax.twinx(); ax2.plot([t["sec"] for t in tl], [t["max_latency_us"] / 1000 for t in tl], color="C1", label="max latency ms")
        ax.set_xlabel("seconds"); ax.set_title("D1 split under load"); fig.tight_layout(); fig.savefig(os.path.join(PLOTS, "d1_split_timeline.png"), dpi=130); plt.close(fig); made.append("d1_split_timeline.png")
    # D2 contention
    rows = []
    for i in ids(by, "D2-bank-theta"):
        r = latest(by, i); rows.append((r["config"]["theta"], r["config"]["clients"], r["results"]["commits_per_sec"], r["results"]["abort_rate"]))
    if rows:
        fig, ax = plt.subplots(figsize=(6, 3.5)); thetas = sorted({r[0] for r in rows})
        for c in sorted({r[1] for r in rows}):
            ax.plot(thetas, [next((x[3] * 100 for x in rows if x[0] == t and x[1] == c), None) for t in thetas], marker="o", label=f"{c} clients")
        ax.set_xlabel("Zipf theta"); ax.set_ylabel("abort rate %"); ax.set_title("D2 abort rate vs contention"); ax.legend()
        fig.tight_layout(); fig.savefig(os.path.join(PLOTS, "d2_contention.png"), dpi=130); plt.close(fig); made.append("d2_contention.png")
    return made


def main():
    by, sims = load()
    md = f"# RESULTS (generated {time.strftime('%Y-%m-%d %H:%M', time.gmtime())} UTC)\n\n"
    md += "> Generated by `make results` from raw files in `bench/results/`. Nothing in this file is typed by hand.\n"
    md += "> `NOT RUN` means no raw data exists for that experiment: no claim can be made from it.\n"
    md += f"> Raw result files found: {sum(len(v) for v in by.values())}; simulation summaries: {len(sims)}.\n\n"
    for f in (section_s1, section_s2, section_soak, section_r1, section_r2, section_r3, section_d1, section_d2, section_n1):
        try:
            md += f(by, sims) if f in (section_s2, section_r3) else f(by)
        except Exception as e:  # a malformed result must not hide the rest
            md += f"_section {f.__name__} failed: {e}_\n\n"
    md += section_misc(by)
    made = plots(by)
    md += "\n## Plots\n\n" + ("\n".join(f"![{m}](bench/plots/{m})" for m in made) if made and not made[0].startswith("_") else (made[0] if made else "_no data to plot yet_")) + "\n"
    open(OUT, "w").write(md)
    print(f"wrote {OUT} ({len(md)} bytes), {len(made)} plots")


if __name__ == "__main__":
    main()
