#!/usr/bin/env bash
# Deterministic simulation sweep (the "thousands of seeds" evidence). Summaries -> $OUT/sim-summaries.
source "$(dirname "$0")/lib.sh"; need rkv-sim; banner sim
SEEDS=$(pick 100 10000)
for p in calm stormy partition crash membership skew; do
  run $BIN/rkv-sim --seeds 0..$SEEDS --profile $p --duration 20000 --jobs ${JOBS:-4} --out $OUT/sim-summaries --minimize
done
