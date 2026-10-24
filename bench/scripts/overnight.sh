#!/usr/bin/env bash
# Correctness evidence that does NOT depend on a quiet machine (nothing here measures speed): simulation sweeps, injected-bug
# suite, crash consistency, real-process chaos, bank-under-faults, fuzzing. ~4-5 h. Logs: $LOGS. Safe to leave unattended.
#   JOBS=3 HISTORIES=20 FUZZ_SECS=600 bash bench/scripts/overnight.sh
source "$(dirname "$0")/lib.sh"; need rkv-sim rkv-chaos rkv-bench
LOGS="${LOGS:-/tmp/rkv-overnight}"; mkdir -p "$LOGS"; JOBS="${JOBS:-3}"; SEED_BASE="${SEED_BASE:-0}"; HISTORIES="${HISTORIES:-20}"; FUZZ_SECS="${FUZZ_SECS:-600}"
{ echo "started $(date)"; scripts/preflight.sh || true; uname -a; sysctl -n machdep.cpu.brand_string 2>/dev/null; } > "$LOGS/context.txt" 2>&1
step() { local name="$1"; shift; echo "=== $name  $(date +%H:%M:%S)"; if "$@" > "$LOGS/$name.log" 2>&1; then echo "    ok"; else echo "    exit $? (see $LOGS/$name.log)"; fi; }
step build cargo build --release --workspace
step tests-workspace cargo test --release --workspace --no-fail-fast
for p in calm stormy partition crash membership skew; do
  step sim-$p nice -n 10 $BIN/rkv-sim --seeds $SEED_BASE..$((SEED_BASE+5000)) --profile $p --duration 20000 --jobs $JOBS --minimize --save-regressions crates/sim/regressions --out $OUT/sim-summaries
done
for b in stale-reads no-dedup ack-early no-persist-vote vote-ignores-log; do
  step mutant-$b nice -n 10 $BIN/rkv-sim --seeds $SEED_BASE..$((SEED_BASE+1000)) --profile stormy --duration 15000 --jobs $JOBS --bug $b --out $OUT/sim-mutants || true
done
step mutant-commit-minority nice -n 10 $BIN/rkv-sim --seeds $SEED_BASE..$((SEED_BASE+1000)) --profile partition --duration 15000 --voters 5 --jobs $JOBS --bug commit-minority --out $OUT/sim-mutants || true
step mutant-commit-old-term nice -n 10 $BIN/rkv-sim --seeds $SEED_BASE..$((SEED_BASE+3000)) --profile crash --duration 20000 --voters 5 --jobs $JOBS --bug commit-old-term --out $OUT/sim-mutants || true
step s2-sim nice -n 10 $BIN/rkv-bench s2-sim --iterations 5000 --id S2-sim --out $OUT
step s2-sigkill nice -n 10 $BIN/rkv-bench s2-driver --iterations 500 --sync always --id S2-sigkill --out $OUT
step s2-cluster nice -n 10 $BIN/rkv-chaos cluster-crash --iterations 20 --id S2-cluster --out $OUT
for nem in kill-leader pause partition-half ring rolling mixed skew; do
  for s in $(seq 1 "$HISTORIES"); do
    step chaos-$nem-$s nice -n 10 $BIN/rkv-chaos register --nemesis $nem --secs 30 --clients 8 --seed $s --id J-$nem-seed$s --out $OUT
  done
done
for nem in kill-leader partition-half mixed; do
  step bank-$nem nice -n 10 $BIN/rkv-chaos bank --nemesis $nem --secs 60 --accounts 20 --clients 8 --id J-bank-$nem --out $OUT
done
[ "$FUZZ_SECS" = 0 ] || step fuzz env SECS=$FUZZ_SECS bash bench/scripts/fuzz.sh
echo "=== ALL DONE $(date +%H:%M:%S)" | tee "$LOGS/ALL-DONE"
