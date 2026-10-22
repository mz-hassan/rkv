#!/usr/bin/env bash
# Local multi-process cluster (real processes, localhost).
#   scripts/cluster.sh up [STORES=3] [pd]      start (with "pd": 1 PD + STORES stores; without: static membership)
#   scripts/cluster.sh down | status | env
# State lives in $RKV_CLUSTER_DIR (default /tmp/rkv-cluster). Ports: PD 23790, stores 20161.., http 20181..
set -euo pipefail
cd "$(dirname "$0")/.."
BIN="${RKV_BIN_DIR:-./target/release}"
DIR="${RKV_CLUSTER_DIR:-/tmp/rkv-cluster}"
cmd="${1:-status}"; N="${2:-3}"; MODE="${3:-static}"
PDADDR="127.0.0.1:23790"
extra=${RKV_SERVER_ARGS:-}
case "$cmd" in
  up)
    [ -x "$BIN/rkv-server" ] || { echo "build first: make build"; exit 1; }
    "$0" down >/dev/null 2>&1 || true
    mkdir -p "$DIR"; : > "$DIR/pids"
    peers=""; for i in $(seq 1 "$N"); do peers+="${peers:+,}$i=127.0.0.1:$((20160+i))"; done
    if [ "$MODE" = "pd" ]; then
      "$BIN/rkv-pd" --id 1 --addr "$PDADDR" --data "$DIR/pd" >"$DIR/pd.log" 2>&1 & echo $! >> "$DIR/pids"; sleep 1
    fi
    for i in $(seq 1 "$N"); do
      if [ "$MODE" = "pd" ]; then topo=(--pd "$PDADDR"); else topo=(--peers "$peers"); fi
      # shellcheck disable=SC2086
      "$BIN/rkv-server" --id "$i" --addr "127.0.0.1:$((20160+i))" --http "127.0.0.1:$((20180+i))" --data "$DIR/s$i" "${topo[@]}" $extra >"$DIR/s$i.log" 2>&1 &
      echo $! >> "$DIR/pids"
    done
    echo "started $N stores ($MODE) in $DIR; waiting for a leader..."
    for _ in $(seq 1 60); do curl -sf "http://127.0.0.1:20181/readyz" >/dev/null 2>&1 && { echo ready; break; }; sleep 0.5; done
    echo "seeds: $(for i in $(seq 1 "$N"); do printf '127.0.0.1:%s,' $((20160+i)); done | sed 's/,$//')"
    if [ "$MODE" = "pd" ]; then echo "pd: $PDADDR"; fi
    ;;
  down)
    if [ -f "$DIR/pids" ]; then while read -r p; do kill -9 "$p" 2>/dev/null || true; done < "$DIR/pids"; rm -f "$DIR/pids"; fi
    echo "cluster stopped" ;;
  status)
    for i in 1 2 3 4 5; do curl -sf "http://127.0.0.1:$((20180+i))/readyz" >/dev/null 2>&1 && echo "store $i: ready" || true; done ;;
  env)
    echo "export RKV_SEEDS=$(for i in $(seq 1 "$N"); do printf '127.0.0.1:%s,' $((20160+i)); done | sed 's/,$//')"; echo "export RKV_PD=$PDADDR" ;;
  *) echo "usage: $0 up|down|status|env [STORES] [pd]"; exit 2 ;;
esac
