#!/usr/bin/env bash
# tc netem wrapper (Linux only). usage: netem.sh HOST add DELAY_MS [LOSS_PCT] [IFACE]  |  netem.sh HOST clear [IFACE]
source "$(dirname "$0")/lib.sh"
host="$1"; act="$2"
case "$act" in
  add)   d="$3"; loss="${4:-0}"; dev="${5:-eth0}"
         R "$host" "tc qdisc del dev $dev root 2>/dev/null || true; tc qdisc add dev $dev root netem delay ${d}ms $((d/10+1))ms loss ${loss}%" ;;
  clear) dev="${3:-eth0}"; R "$host" "tc qdisc del dev $dev root 2>/dev/null || true" ;;
  *) echo "usage"; exit 2 ;;
esac
