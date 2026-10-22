#!/usr/bin/env python3
"""Turn /tmp/n1_r2_windows_ms.txt (one integer ms per line) into a bench/results result.json so `make results` picks it up."""
import json, os, subprocess, sys, time
src, out = sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else "bench/results"
xs = sorted(int(l) for l in open(src) if l.strip())
pct = lambda p: xs[min(len(xs) - 1, max(0, int(round(len(xs) * p / 100 + 0.4999)) - 1))]
ts = str(int(time.time())); d = os.path.join(out, "N1-r2-partition", ts); os.makedirs(d, exist_ok=True)
json.dump({"id": "N1-r2-partition", "bench": "failover-window", "timestamp_unix": ts, "config": {"fault": "partition", "trials": len(xs), "election_tick": 10, "tick_ms": 20, "prevote": True, "check_quorum": True},
           "results": {"completed_trials": len(xs), "failed_trials": 0, "unavailability_ms": {"p50": pct(50), "p95": pct(95), "p99": pct(99), "max": xs[-1], "mean": sum(xs) / len(xs)}, "samples_ms": xs},
           "labels": {"scope": "multi-VM real network", "faults": "iptables leader isolation"}, "hardware": {}, "git_commit": subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True).stdout.strip()},
          open(os.path.join(d, "result.json"), "w"), indent=2)
print("wrote", d)
