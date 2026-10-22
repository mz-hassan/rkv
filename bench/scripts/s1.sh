#!/usr/bin/env bash
# S1: single-node engine. YCSB A-F, fsync/cache/compaction sweeps, recovery vs WAL size. Add RocksDB baseline with RKV_ROCKS=1 (needs the rocksdb-baseline build).
source "$(dirname "$0")/lib.sh"; need rkv-bench; banner S1
R=$(pick 3000 10000000); O=$(pick 3000 5000000); T=$(pick 2 8)
backends="rkv"; [ "${RKV_ROCKS:-0}" = 1 ] && backends="rkv rocksdb"
for be in $backends; do
  B=$BIN/rkv-bench; [ "$be" = rocksdb ] && B=$BIN/rkv-bench-rocksdb   # built by `make build-baselines`
  for w in a b c d e f; do
    run $B engine --backend $be --id S1-ycsb-$w-$be --workload $w --records $R --ops $O --threads $T --sync group --out $OUT
  done
  # fsync policy trade-off (workload A)
  for sync in always group interval=100 never; do
    run $B engine --backend $be --id S1-fsync-${sync%%=*}-$be --workload a --records $R --ops $(pick 3000 500000) --threads $T --sync $sync --out $OUT
  done
done
# block cache size vs hit ratio (read-only, zipfian)
for mb in 16 64 256 1024; do
  run $BIN/rkv-bench engine --id S1-cache-${mb}mb --workload c --records $R --ops $O --threads $T --cache-mb $mb --out $OUT
done
# leveled vs size-tiered: write/read/space amplification
for c in leveled tiered; do
  run $BIN/rkv-bench engine --id S1-compaction-$c --workload a --records $R --ops $O --threads $T --compaction $c --out $OUT
done
# recovery time vs WAL size
for mb in $(pick "4 8" "64 256 1024 2048"); do
  run $BIN/rkv-bench recovery --mb $mb --id S1-recovery-${mb}mb --out $OUT
done
