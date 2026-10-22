#!/usr/bin/env bash
# Run every locally-runnable benchmark in order. Honours SCALE/DRY_RUN. ETA at SCALE=full: many hours.
source "$(dirname "$0")/lib.sh"; banner "run-all"
for s in sim s1 s2 r2 r3 r1 d1 d2; do
  echo "--- $s"; bash "$(dirname "$0")/$s.sh" || echo "!! $s failed (continuing)"
done
bash "$(dirname "$0")/etcd_baseline.sh" || echo "!! etcd baseline skipped/failed"
echo "now: make results resume"
