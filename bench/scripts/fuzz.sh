#!/usr/bin/env bash
# Fuzz every decoder. SECS per target (default 600). Needs: rustup toolchain install nightly; cargo install cargo-fuzz.
source "$(dirname "$0")/lib.sh"; banner fuzz
SECS="${SECS:-$(pick 5 600)}"
cd fuzz
for t in wal_reader sstable_reader manifest_decoder raft_message repl_command proto_requests; do
  if [ "$DRY_RUN" = 1 ]; then run cargo +nightly fuzz run $t -- -max_total_time=$SECS -timeout=10 -rss_limit_mb=4096; continue; fi
  log=$(mktemp); echo "+ cargo +nightly fuzz run $t (log $log)"
  cargo +nightly fuzz run $t -- -max_total_time=$SECS -timeout=10 -rss_limit_mb=4096 >$log 2>&1 || true
  crashes=$(ls artifacts/$t 2>/dev/null | wc -l | tr -d ' ')
  python3 - "$t" "$log" "$crashes" "$OUT" <<'PY'
import sys, re, json, os, time, subprocess
t, log, crashes, out = sys.argv[1:5]
txt = open(log).read()
m = re.search(r"Done (\d+) runs in (\d+) second", txt)
runs, secs = (int(m.group(1)), int(m.group(2))) if m else (0, 0)
d = os.path.join("..", out, f"fuzz-{t}", str(int(time.time()))); os.makedirs(d, exist_ok=True)
json.dump({"id": f"fuzz-{t}", "bench": "fuzz", "timestamp_unix": str(int(time.time())), "config": {"target": t}, "results": {"runs": runs, "seconds": secs, "crashes": int(crashes)},
           "labels": {"scope": "libFuzzer (cargo-fuzz, nightly), single machine"}, "hardware": {}, "git_commit": subprocess.run(["git","rev-parse","--short","HEAD"],capture_output=True,text=True).stdout.strip()}, open(os.path.join(d, "result.json"), "w"), indent=2)
print(f"{t}: {runs} runs in {secs}s, {crashes} crash artifacts")
PY
done
