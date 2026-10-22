#!/usr/bin/env bash
# R3: linearizability under faults. (a) deterministic simulation: many seeds, every nemesis profile;
#     (b) the checker's own mutation tests; (c) real-process chaos histories; (d) injected bugs MUST be caught.
source "$(dirname "$0")/lib.sh"; need rkv-sim rkv-chaos; banner R3
SEEDS=$(pick 50 5000)
for p in calm stormy partition crash membership skew; do
  run $BIN/rkv-sim --seeds 0..$SEEDS --profile $p --duration 20000 --jobs ${JOBS:-4} --out $OUT/sim-summaries --minimize
done
# mutation testing: each of these MUST report failures (exit code ignored here)
for b in stale-reads no-dedup ack-early no-persist-vote vote-ignores-log; do
  run $BIN/rkv-sim --seeds 0..$(pick 30 1000) --profile stormy --duration 15000 --jobs ${JOBS:-4} --bug $b --out $OUT/sim-mutants || true
done
run $BIN/rkv-sim --seeds 0..$(pick 30 1000) --profile partition --duration 15000 --voters 5 --jobs ${JOBS:-4} --bug commit-minority --out $OUT/sim-mutants || true
run cargo test --release -p lincheck
# real processes
N=$(pick 1 200)
for nem in kill-leader pause partition-half ring rolling mixed skew; do
  for s in $(seq 1 $N); do
    run $BIN/rkv-chaos register --nemesis $nem --secs $(pick 8 30) --clients 8 --seed $s --id J-$nem-seed$s --out $OUT
  done
done
