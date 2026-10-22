#!/usr/bin/env bash
# D1: sharding. Throughput vs ranges, range split under load, hot-range detection and rebalancing.
source "$(dirname "$0")/lib.sh"; need rkv-bench rkv-server rkv-pd kvctl; banner D1
REC=$(pick 300 200000); CL=$(pick 8 64); SECS=$(pick 4 30)
PD=127.0.0.1:23790
for ranges in $(pick "1 4" "1 4 16 64"); do
  cluster up 3 pd; sleep 6
  # pre-split the hashed key space "user<16 hex>" into $ranges equal ranges
  keys=""; for i in $(seq 1 $((ranges-1))); do keys+=" user$(printf '%04x' $((i*65536/ranges)))"; done
  [ "$ranges" -gt 1 ] && run $BIN/kvctl --pd $PD presplit $keys
  for mix in a r; do
    run $BIN/rkv-bench cluster --pd $PD --workload $mix --records $REC --secs $SECS --clients $CL --id D1-ranges$ranges-mix$mix --cluster-label "3 stores, $ranges ranges" --out $OUT
  done
  cluster down
done
# split under load: per-second timeline shows latency/throughput impact
cluster up 3 pd; sleep 6
if [ "$DRY_RUN" != 1 ]; then
  ( sleep $((SECS/3)); $BIN/kvctl --pd $PD presplit user8000 ) &
fi
run $BIN/rkv-bench cluster --pd $PD --workload a --records $REC --secs $SECS --clients $CL --timeline --id D1-split-under-load --out $OUT
wait || true; cluster down
# hot range: tell PD to split ranges that exceed a write rate, then hammer a tiny skewed key set
cluster up 3 pd; sleep 6
run $BIN/kvctl --pd $PD pd-config hot_write_qps $(pick 50 2000)
run $BIN/rkv-bench cluster --pd $PD --workload a --records $REC --secs $SECS --theta 1.2 --clients $CL --timeline --id D1-hot-range --out $OUT
run $BIN/kvctl --pd $PD pd-ranges
cluster down
