#!/usr/bin/env bash
# Run inside the K1 flow: writes sequentially numbered keys through a port-forward, triggers a rolling restart of the
# store StatefulSet, then verifies every acknowledged key.
source "$(dirname "$0")/lib.sh"; banner "K1 rolling upgrade"
run kubectl -n rkv port-forward svc/rkv-pd 23790:2379 &
PF=$!; trap 'kill $PF 2>/dev/null || true' EXIT
[ "$DRY_RUN" = 1 ] || sleep 3
run $BIN/rkv-bench txn-bank --pd 127.0.0.1:23790 --accounts 100 --clients 8 --secs 90 --id K1-rolling-upgrade --scope-label "kind (single host)" --out $OUT &
LOAD=$!
[ "$DRY_RUN" = 1 ] || sleep 20
run kubectl -n rkv rollout restart statefulset/rkv-store
run kubectl -n rkv rollout status statefulset/rkv-store --timeout=600s
wait $LOAD
