#!/usr/bin/env bash
# CPU flamegraph of rkv-server on a Linux host with perf. usage: perf_flame.sh HOST SECONDS OUT.svg
# Needs: linux-tools (perf) and https://github.com/brendangregg/FlameGraph in ~/FlameGraph on the host.
source "$(dirname "$0")/lib.sh"
host="$1"; secs="${2:-30}"; out="${3:-flame.svg}"
R "$host" "perf record -F 99 -g -p \$(pgrep -f rkv-server | head -1) -o /tmp/perf.data -- sleep $secs && perf script -i /tmp/perf.data | ~/FlameGraph/stackcollapse-perf.pl | ~/FlameGraph/flamegraph.pl > /tmp/flame.svg"
X scp $SSH_OPTS "$SSH_USER@$host:/tmp/flame.svg" "$out"
