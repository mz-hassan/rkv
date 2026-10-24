#!/usr/bin/env bash
# Inside the K1 flow. The bank workload runs as a POD INSIDE the cluster (PD hands out in-cluster DNS names, which are not
# reachable from outside). The operator upgrades the image (rkv:dev -> rkv:v2), scales 3 -> 5 -> 3; the load generator audits the
# bank invariant at the end and its result.json is pulled out of the pod log.
source "$(dirname "$0")/lib.sh"; banner "K1 rolling upgrade + scale"
phase() { kubectl -n rkv get rkvcluster demo -o jsonpath='{.status.phase}' 2>/dev/null; }
wait_phase() { # $1 = expected phase, $2 = max seconds
  local t0=$(date +%s); while [ "$(phase)" != "$1" ] && [ $(( $(date +%s) - t0 )) -lt "$2" ]; do sleep 2; done
  echo "phase $1 reached after $(( $(date +%s) - t0 ))s (status: $(kubectl -n rkv get rkvcluster demo -o jsonpath='{.status.message}' 2>/dev/null))"
}
# wait until the operator has NOTICED the change (phase leaves Ready) - otherwise a stale "Ready" ends the wait immediately
left_ready() { local t0=$(date +%s); while [ "$(phase)" = Ready ] && [ $(( $(date +%s) - t0 )) -lt "${1:-60}" ]; do sleep 1; done; }
ready_pods() { # wait until the store StatefulSet has $1 ready pods
  local t0=$(date +%s); while [ "$(kubectl -n rkv get sts rkv-store -o jsonpath='{.status.readyReplicas}' 2>/dev/null)" != "$1" ] && [ $(( $(date +%s) - t0 )) -lt "${2:-600}" ]; do sleep 2; done
  echo "store pods ready: $(kubectl -n rkv get sts rkv-store -o jsonpath='{.status.readyReplicas}')/$1 after $(( $(date +%s) - t0 ))s"
}
ranges_per_store() { kubectl -n rkv exec rkv-store-0 -- kvctl --pd "$PDS" pd-ranges 2>/dev/null | grep '^store' | awk '{print "store " $2 ": " $5}' | tr '\n' ' '; echo; }
PDS="rkv-pd-0.rkv-pd.rkv.svc.cluster.local:2379,rkv-pd-1.rkv-pd.rkv.svc.cluster.local:2379,rkv-pd-2.rkv-pd.rkv.svc.cluster.local:2379"
SECS="${K1_LOAD_SECS:-300}"
run kubectl -n rkv delete pod loadgen --ignore-not-found
run kubectl -n rkv run loadgen --image=rkv:dev --image-pull-policy=IfNotPresent --restart=Never --command -- sh -c \
  "rkv-bench txn-bank --pd $PDS --accounts 100 --clients 8 --secs $SECS --id K1-rolling-upgrade --scope-label 'kind in-VM cluster, operator-driven upgrade and scale' --out /tmp/res; echo ===RESULT-BEGIN===; cat /tmp/res/K1-rolling-upgrade/*/result.json; echo; echo ===RESULT-END==="
[ "$DRY_RUN" = 1 ] || sleep 30
echo "--- rolling upgrade (operator holds the StatefulSet partition and releases one pod at a time)"
run kubectl -n rkv patch rkvcluster demo --type merge -p '{"spec":{"image":"rkv:v2"}}'
[ "$DRY_RUN" = 1 ] || { left_ready 60; wait_phase Ready 900; }
echo "--- scale out 3 -> 5"
run kubectl -n rkv patch rkvcluster demo --type merge -p '{"spec":{"storeReplicas":5}}'
[ "$DRY_RUN" = 1 ] || { left_ready 60; ready_pods 5 600; wait_phase Ready 600; }
echo "--- split the keyspace so PD has several ranges to spread over the 5 stores, then let it balance"
run kubectl -n rkv exec rkv-store-0 -- kvctl --pd $PDS presplit bank00000025 bank00000050 bank00000075
[ "$DRY_RUN" = 1 ] || { sleep 90; echo "ranges per store: $(ranges_per_store)"; }
echo "--- scale in 5 -> 3 (PD drains each store before its pod is removed)"
run kubectl -n rkv patch rkvcluster demo --type merge -p '{"spec":{"storeReplicas":3}}'
[ "$DRY_RUN" = 1 ] || { left_ready 60; ready_pods 3 900; wait_phase Ready 900; echo "ranges per store after scale-in: $(ranges_per_store)"; }
run kubectl -n rkv get rkvcluster demo -o jsonpath='{.status}'
if [ "$DRY_RUN" != 1 ]; then
  kubectl -n rkv wait --for=jsonpath='{.status.phase}'=Succeeded pod/loadgen --timeout=$((SECS+300))s || true
  kubectl -n rkv logs loadgen > "$OUT/k1-loadgen.log" 2>&1 || true
  d="$OUT/K1-rolling-upgrade/$(date +%s)"; mkdir -p "$d"
  sed -n '/===RESULT-BEGIN===/,/===RESULT-END===/p' "$OUT/k1-loadgen.log" | sed '1d;$d' > "$d/result.json"
  if grep -q '"invariant_ok": true' "$d/result.json"; then echo "K1: bank invariant held through upgrade + scale"; else echo "K1: INVARIANT NOT CONFIRMED (see $OUT/k1-loadgen.log)"; exit 1; fi
fi
