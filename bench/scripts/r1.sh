#!/usr/bin/env bash
# R1: replicated throughput/latency. nodes x mix x value size x batching/pipelining x read mode; etcd baseline via etcd_baseline.sh.
source "$(dirname "$0")/lib.sh"; need rkv-bench rkv-server; banner R1
REC=$(pick 300 100000); OPS=$(pick 600 100000); CL=$(pick 4 32); SECS=0
for n in $(pick "3" "1 3 5"); do
  for sz in $(pick "128" "128 1024 16384"); do
    for mix in w a r; do   # w=100% write, a=50/50, r=100% read
      cluster up $n; wait_ready
      run $BIN/rkv-bench cluster --seeds "$(scripts/cluster.sh env $n | grep SEEDS | cut -d= -f2)" --workload $mix --records $REC --ops $OPS --clients $CL --value-size $sz \
        --id R1-${n}n-mix$mix-v$sz --cluster-label "rkv ${n} nodes localhost" --out $OUT
      cluster down
    done
  done
done
# batching / pipelining on vs off (3 nodes, 100% write, 128 B)
for variant in "on:" "nobatch:--max-msg-entries 1" "nopipe:--max-inflight 1" "neither:--max-msg-entries 1 --max-inflight 1"; do
  name=${variant%%:*}; flags=${variant#*:}
  RKV_SERVER_ARGS="$flags" cluster up 3; wait_ready
  run $BIN/rkv-bench cluster --seeds "$(scripts/cluster.sh env 3 | grep SEEDS | cut -d= -f2)" --workload w --records $REC --ops $OPS --clients $CL --id R1-batching-$name --out $OUT
  cluster down
done
# read modes: ReadIndex vs lease (needs --lease-reads) vs follower-stale
for mode in linearizable lease stale:1000; do
  extra=""; [ "$mode" = lease ] && extra="--lease-reads"
  RKV_SERVER_ARGS="$extra" cluster up 3; wait_ready
  run $BIN/rkv-bench cluster --seeds "$(scripts/cluster.sh env 3 | grep SEEDS | cut -d= -f2)" --workload r --records $REC --ops $OPS --clients $CL --read-mode $mode --id R1-readmode-${mode%%:*} --out $OUT
  cluster down
done
echo "etcd baseline: run bench/scripts/etcd_baseline.sh with the same SCALE"
