#!/usr/bin/env bash
# R2: failover unavailability windows (>=100 trials per fault), election-timeout sweep, PreVote/CheckQuorum effect, snapshot catch-up.
source "$(dirname "$0")/lib.sh"; need rkv-chaos rkv-server; banner R2
TR=$(pick 2 100)
for fault in kill pause partition; do
  run $BIN/rkv-chaos failover --fault $fault --trials $TR --id R2-$fault --out $OUT
done
# election timeout sensitivity: timeout = tick_ms x election_tick (randomised to 2x)
for et in $(pick "5" "5 10 20 40"); do
  run $BIN/rkv-chaos failover --fault kill --trials $TR --election-tick $et --tick-ms 20 --id R2-kill-et$et --out $OUT
done
# PreVote / CheckQuorum ablation under a leader isolation
run $BIN/rkv-chaos failover --fault partition --trials $TR --no-prevote --id R2-partition-noprevote --out $OUT
run $BIN/rkv-chaos failover --fault partition --trials $TR --no-check-quorum --id R2-partition-nocheckquorum --out $OUT
run $BIN/rkv-chaos failover --fault partition --trials $TR --no-prevote --no-check-quorum --id R2-partition-neither --out $OUT
# lagging replica catch-up through a snapshot
for mb in $(pick "4" "100 1000"); do
  run $BIN/rkv-chaos catchup --mb $mb --id R2-catchup-${mb}mb --out $OUT
done
