#!/usr/bin/env bash
# S2: crash consistency. Simulated power loss (MemFs), real SIGKILL of a writer process, and whole-cluster kill -9.
source "$(dirname "$0")/lib.sh"; need rkv-bench rkv-chaos; banner S2
run $BIN/rkv-bench s2-sim --iterations $(pick 100 5000) --id S2-sim --out $OUT
run $BIN/rkv-bench s2-driver --iterations $(pick 3 500) --sync always --id S2-sigkill --out $OUT
run $BIN/rkv-chaos cluster-crash --iterations $(pick 2 20) --id S2-cluster --out $OUT
