#!/usr/bin/env bash
# End-to-end smoke test with real processes: raw KV, transactions across ranges, split, failover. ~40 s.
set -euo pipefail
cd "$(dirname "$0")/.."
BIN="${RKV_BIN_DIR:-./target/release}"
export RKV_CLUSTER_DIR=/tmp/rkv-smoke-cluster
trap 'scripts/cluster.sh down >/dev/null 2>&1 || true' EXIT
scripts/cluster.sh up 3 pd
PD=127.0.0.1:23790
sleep 6   # PD bootstraps range 1 and replicates it to 3 stores
K="$BIN/kvctl --pd $PD"
$K put hello world && [ "$($K get hello)" = "world" ] && echo "raw put/get ok"
$K cas hello world there | grep -q swapped && echo "cas ok"
$K txn put acct1 100 acct2 100 | grep -q committed && echo "txn ok"
$K split 1 acct2 >/dev/null && sleep 2 && $K txn get acct1 acct2 && echo "split + cross-range txn read ok"
$K pd-ranges
# kill -9 the store holding most leaders; the cluster must keep serving
pid=$(sed -n 2p "$RKV_CLUSTER_DIR/pids"); kill -9 "$pid"; sleep 3
$K put after-failover ok && [ "$($K get after-failover)" = "ok" ] && echo "failover ok"
echo "SMOKE PASSED"
