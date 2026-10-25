#!/usr/bin/env bash
# Local membership-churn test (the scale-in/scale-out half of K1, without Kubernetes).
#   1 PD + 5 stores as real processes, bank-transfer load running the whole time, then CYCLES times:
#     drain store 5 (PD evict_store) -> wait until it holds no range -> kill it
#     drain store 4 likewise
#     restart both with their OLD data directories and let PD balance replicas back onto them
#   Finally the bank invariant (total balance conserved) is audited by the load generator.
# Exercises: learner add/promote, replica removal + destroy, re-adding a store to a range it was removed from, leadership
# transfer off a draining store, MVCC GC interplay. Usage: churn_local.sh [CYCLES=3] [DRAIN_TIMEOUT_S=120]
set -uo pipefail
cd "$(dirname "$0")/../.."
BIN=./target/release
DIR=${RKV_CHURN_DIR:-/tmp/rkv-churn}
CYCLES=${1:-3}; DRAIN_TO=${2:-120}
PD=127.0.0.1:23790
rm -rf "$DIR"; mkdir -p "$DIR"
declare -A PID

start_store() { # id
  local i=$1
  "$BIN/rkv-server" --id "$i" --addr "127.0.0.1:$((20160+i))" --http "127.0.0.1:$((20180+i))" --data "$DIR/s$i" --pd "$PD" >>"$DIR/s$i.log" 2>&1 &
  PID[$i]=$!
}
stop_store() { kill -9 "${PID[$1]}" 2>/dev/null; wait "${PID[$1]}" 2>/dev/null; true; }
cleanup() { for i in 1 2 3 4 5; do stop_store "$i"; done; kill -9 "${PD_PID:-0}" "${LOAD_PID:-0}" 2>/dev/null; true; }
trap cleanup EXIT
# on failure the cluster is left running for inspection (KEEP_ON_FAIL=1, default); stop it with: pkill -f "$DIR"
fail() {
  echo "CHURN FAILED: $*"; echo "--- PD view:"; "$BIN/kvctl" --pd "$PD" pd-ranges 2>&1 | head -20
  [ "${KEEP_ON_FAIL:-1}" = 1 ] && { trap - EXIT; echo "(cluster left running in $DIR)"; }
  exit 1
}
ranges_on() { "$BIN/kvctl" --pd "$PD" pd-ranges 2>/dev/null | grep -cE "^range .*peers=.*\"$1L?\"" ; }
drain() { # store id
  "$BIN/kvctl" --pd "$PD" pd-config evict_store "$1" >/dev/null
  local t=0
  while [ "$(ranges_on "$1")" != "0" ]; do
    sleep 2; t=$((t+2)); [ $t -ge "$DRAIN_TO" ] && fail "store $1 did not drain within ${DRAIN_TO}s ($(ranges_on "$1") ranges left)"
  done
  echo "  store $1 drained in ${t}s"
}

"$BIN/rkv-pd" --id 1 --addr "$PD" --data "$DIR/pd" >"$DIR/pd.log" 2>&1 & PD_PID=$!
sleep 1
for i in 1 2 3 4 5; do start_store "$i"; done
for _ in $(seq 1 60); do curl -sf "http://127.0.0.1:20181/readyz" >/dev/null 2>&1 && break; sleep 0.5; done
SEEDS=$(for i in 1 2 3 4 5; do printf '127.0.0.1:%s,' $((20160+i)); done | sed 's/,$//')
sleep 3
"$BIN/kvctl" --pd "$PD" --seeds "$SEEDS" presplit bank00000025 bank00000050 bank00000075 || fail "presplit"
sleep 15 # let PD spread the four ranges

LOAD_SECS=$((CYCLES * 200 + 120))
"$BIN/rkv-bench" txn-bank --pd "$PD" --seeds "$SEEDS" --accounts 100 --clients 8 --secs "$LOAD_SECS" \
  --id churn-local --scope-label "localhost PD + 5 stores, membership churn" --out "$DIR/res" >"$DIR/load.log" 2>&1 &
LOAD_PID=$!
sleep 10

for c in $(seq 1 "$CYCLES"); do
  echo "== cycle $c: drain 5, drain 4, bring both back"
  drain 5; stop_store 5
  drain 4; stop_store 4
  sleep 5
  "$BIN/kvctl" --pd "$PD" pd-config evict_store 0 >/dev/null
  start_store 4; start_store 5        # same data dirs: they come back holding stale replicas
  t=0; while [ "$(ranges_on 5)" = "0" ] || [ "$(ranges_on 4)" = "0" ]; do
    sleep 3; t=$((t+3)); [ $t -ge 120 ] && fail "balancer did not place replicas back on stores 4/5 (4:$(ranges_on 4) 5:$(ranges_on 5))"
  done
  echo "  rebalanced in ${t}s (store4=$(ranges_on 4) store5=$(ranges_on 5) ranges)"
  sleep 10
done

echo "== waiting for the load generator and its audit"
wait "$LOAD_PID"; rc=$?
grep -E "VIOLATED|AUDIT" "$DIR/load.log" && fail "bank invariant / audit (see $DIR/load.log)"
[ $rc -eq 0 ] || fail "load generator exited $rc (see $DIR/load.log)"
echo "CHURN OK: $CYCLES cycles, bank invariant held"
grep -E '"commits"|"audit_total"|"other_errors"' "$DIR"/res/churn-local/*/result.json 2>/dev/null
