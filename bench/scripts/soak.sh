#!/usr/bin/env bash
# Soak: sustained mixed load for SOAK_SECS while sampling every server's RSS; the result records the series and the
# growth after warm-up, so "no unbounded memory growth" is a measurement (backpressure: bounded inboxes, proposal limits,
# write stalls). Pass criterion is judged in RESULTS.md: RSS growth after warm-up should flatten.
source "$(dirname "$0")/lib.sh"; need rkv-bench rkv-server; banner "soak"
SECS=$(pick 12 "${SOAK_SECS:-1800}"); INTERVAL=$(pick 2 10)
cluster up 3; wait_ready
if [ "$DRY_RUN" != 1 ]; then
  SAMPLES=$(mktemp)
  ( for _ in $(seq 1 $((SECS/INTERVAL))); do sleep $INTERVAL; for p in $(cat "${RKV_CLUSTER_DIR:-/tmp/rkv-cluster}/pids"); do echo "$(date +%s) $p $(ps -o rss= -p $p 2>/dev/null || echo 0)"; done; done > $SAMPLES ) &
  SAMPLER=$!
fi
run $BIN/rkv-bench cluster --seeds "$(scripts/cluster.sh env 3 | grep SEEDS | cut -d= -f2)" --workload a --records $(pick 500 100000) --secs $SECS --clients $(pick 8 64) --value-size 512 --id S3-soak-load --out $OUT
if [ "$DRY_RUN" != 1 ]; then
  wait $SAMPLER || true
  python3 - "$SAMPLES" "$OUT" "$SECS" <<'PY'
import sys, json, os, time, subprocess, collections
samples, out, secs = sys.argv[1], sys.argv[2], int(sys.argv[3])
by = collections.defaultdict(list)
for l in open(samples):
    t, p, rss = l.split()
    by[p].append((int(t), int(rss)))
res = {}
for p, xs in by.items():
    n = len(xs); half = xs[n // 2:]
    first, last = half[0][1], half[-1][1]
    res[p] = {"samples": n, "rss_kb_start": xs[0][1], "rss_kb_end": xs[-1][1], "rss_kb_max": max(r for _, r in xs),
              "second_half_growth_kb": last - first, "second_half_growth_pct": round(100.0 * (last - first) / max(first, 1), 2)}
ts = str(int(time.time())); d = os.path.join(out, "S3-soak", ts); os.makedirs(d, exist_ok=True)
json.dump({"id": "S3-soak", "bench": "soak-rss", "timestamp_unix": ts, "config": {"seconds": secs, "nodes": 3}, "results": {"per_process": res, "series": {p: xs for p, xs in by.items()}},
           "labels": {"scope": "localhost multi-process cluster", "faults": "none", "judgement": "growth in the second half of the run should be small relative to RSS (flat); the series is stored for inspection"},
           "hardware": {}, "git_commit": subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True).stdout.strip()}, open(os.path.join(d, "result.json"), "w"), indent=1)
print("soak:", {p: (r["rss_kb_end"] // 1024, "MB end,", r["second_half_growth_pct"], "% growth in 2nd half") for p, r in res.items()})
PY
fi
cluster down
