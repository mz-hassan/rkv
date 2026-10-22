#!/usr/bin/env bash
# Inside the K1 flow. A bank workload runs for the whole procedure; the operator upgrades the image (rkv:dev -> rkv:v2),
# scales 3 -> 5 -> 3; afterwards the bank invariant is audited by the load generator (exit code 1 on violation).
source "$(dirname "$0")/lib.sh"; banner "K1 rolling upgrade + scale"
phase() { kubectl -n rkv get rkvcluster demo -o jsonpath='{.status.phase}' 2>/dev/null; }
wait_phase() { # $1 = expected phase, $2 = max seconds
  local t0=$(date +%s); while [ "$(phase)" != "$1" ] && [ $(( $(date +%s) - t0 )) -lt "$2" ]; do sleep 2; done
  echo "phase $1 reached after $(( $(date +%s) - t0 ))s"
}
run kubectl -n rkv port-forward svc/rkv-pd 23790:2379 &
PF=$!; trap 'kill $PF 2>/dev/null || true' EXIT
[ "$DRY_RUN" = 1 ] || sleep 3
run $BIN/rkv-bench txn-bank --pd 127.0.0.1:23790 --accounts 100 --clients 8 --secs 240 --id K1-rolling-upgrade --scope-label "kind (single host, 3 workers)" --out $OUT &
LOAD=$!
[ "$DRY_RUN" = 1 ] || sleep 20
echo "--- rolling upgrade (operator holds the StatefulSet partition and releases one pod at a time)"
run kubectl -n rkv patch rkvcluster demo --type merge -p '{"spec":{"image":"rkv:v2"}}'
[ "$DRY_RUN" = 1 ] || { wait_phase Upgrading 60; wait_phase Ready 900; }
echo "--- scale out 3 -> 5"
run kubectl -n rkv patch rkvcluster demo --type merge -p '{"spec":{"storeReplicas":5}}'
[ "$DRY_RUN" = 1 ] || { sleep 5; wait_phase Ready 600; }
echo "--- scale in 5 -> 3 (PD drains each store before its pod is removed)"
run kubectl -n rkv patch rkvcluster demo --type merge -p '{"spec":{"storeReplicas":3}}'
[ "$DRY_RUN" = 1 ] || { sleep 5; wait_phase Ready 900; }
run kubectl -n rkv get rkvcluster demo -o jsonpath='{.status}'
wait $LOAD
