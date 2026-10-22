#!/usr/bin/env bash
# Shared helpers for benchmark scripts.
#   SCALE=full (default) | smoke     smoke = tiny sizes, results go to a throwaway dir (never to bench/results)
#   DRY_RUN=1                        print every command, run nothing
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
export PATH="/opt/homebrew/opt/rustup/bin:$HOME/.cargo/bin:$PATH"
BIN="${RKV_BIN_DIR:-./target/release}"
SCALE="${SCALE:-full}"
DRY_RUN="${DRY_RUN:-0}"
if [ "$SCALE" = smoke ]; then OUT="${RKV_SMOKE_DIR:-/tmp/rkv-smoke-results}"; else OUT="${RKV_RESULTS_DIR:-bench/results}"; fi
export OUT BIN SCALE DRY_RUN
mkdir -p "$OUT"
pick() { if [ "$SCALE" = smoke ]; then echo "$1"; else echo "$2"; fi; }   # pick SMOKE FULL
run() { echo "+ $*"; if [ "$DRY_RUN" = 1 ]; then return 0; fi; "$@"; }
need() { for b in "$@"; do [ "$DRY_RUN" = 1 ] || [ -x "$BIN/$b" ] || { echo "missing $BIN/$b - run: make build"; exit 1; }; done; }
cluster() { run scripts/cluster.sh "$@"; }
banner() { echo; echo "=== $* (SCALE=$SCALE DRY_RUN=$DRY_RUN OUT=$OUT) ==="; }
wait_ready() { [ "$DRY_RUN" = 1 ] && return 0; for _ in $(seq 1 120); do curl -sf "http://127.0.0.1:20181/readyz" >/dev/null 2>&1 && return 0; sleep 0.5; done; echo "cluster not ready"; return 1; }
