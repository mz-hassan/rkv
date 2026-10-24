#!/usr/bin/env bash
# N1: kernel-level fault injection and real-network benchmarks on Linux.
#   MODE=netns deploy/cloud/n1.sh [check|r1|r2|disk|flame|all]        # simulated machines on this Linux box (see netns.sh)
#   HOSTS="a b c" CLIENT=d SSH_USER=u deploy/cloud/n1.sh [step]       # real machines over ssh
#   DRY_RUN=1 prints the whole plan without executing (validated by `make dry-run`).
# Results are written by rkv-bench / kvctl on the client "host" into $RESULTS (default /tmp/results); copy them to bench/results.
source "$(dirname "$0")/lib.sh"
D="$(cd "$(dirname "$0")" && pwd)"
BIN="${RKV_BIN_DIR:-./target/release}"; STEP="${1:-all}"; RESULTS="${RESULTS:-/tmp/results}"
SCALE="${SCALE:-full}"
if [ "$MODE" = netns ]; then HOSTS="${HOSTS:-10.77.0.11 10.77.0.12 10.77.0.13}"; CLIENT="${CLIENT:-10.77.0.14}"; else HOSTS="${HOSTS:-10.0.0.11 10.0.0.12 10.0.0.13}"; CLIENT="${CLIENT:-10.0.0.14}"; fi
label="$([ "$MODE" = netns ] && echo 'simulated machines (network namespaces on ONE Linux VM, burstable CPU; kernel netem/iptables are real, CPU and disk are shared)' || echo 'multi-machine real network')"
pick() { if [ "$SCALE" = smoke ]; then echo "$1"; else echo "$2"; fi; }
set -- $HOSTS; H1=$1
peers=""; i=1; for h in $HOSTS; do peers+="${peers:+,}$i=$h:20160"; i=$((i+1)); done
seeds=$(for h in $HOSTS; do printf '%s:20160,' $h; done | sed 's/,$//')

check() {
  echo "### capability check (fail fast before anything expensive)"
  [ "$DRY_RUN" = 1 ] && { echo "+ would check: ip netns, sch_netem, iptables, dm_flakey, perf, binaries"; return 0; }
  bad=0
  for b in rkv-server kvctl rkv-bench; do [ -x "$BIN/$b" ] || { echo "MISSING $BIN/$b (cargo build --release)"; bad=1; }; done
  command -v tc >/dev/null || { echo "MISSING tc (iproute2)"; bad=1; }
  command -v iptables >/dev/null || { echo "MISSING iptables"; bad=1; }
  sudo modprobe sch_netem 2>/dev/null && echo "ok: sch_netem" || { echo "MISSING kernel module sch_netem"; bad=1; }
  sudo modprobe dm_flakey 2>/dev/null && echo "ok: dm_flakey" || echo "note: dm_flakey unavailable -> 'disk' step will be skipped (apt install linux-modules-extra-\$(uname -r))"
  command -v perf >/dev/null && echo "ok: perf" || echo "note: perf missing -> 'flame' step will be skipped"
  [ "$MODE" = netns ] && { bash "$D/netns.sh" up 4 && bash "$D/netns.sh" ping && echo "ok: namespaces reach each other"; }
  [ $bad = 0 ] || { echo "CHECK FAILED"; exit 3; }
  echo "CHECK OK"
}
deploy() {
  echo "### deploy binaries and start the cluster"
  R $H1 "pkill -9 rkv-server || true; sleep 1"      # stop any previous run first (processes started via sudo belong to root)
  for h in $HOSTS $CLIENT; do
    C $h $BIN/rkv-server /tmp/rkv-server; C $h $BIN/kvctl /tmp/kvctl; C $h $BIN/rkv-bench /tmp/rkv-bench
    R $h "chmod +x /tmp/rkv-server /tmp/kvctl /tmp/rkv-bench; mkdir -p $RESULTS"
  done
  i=1; for h in $HOSTS; do
    R $h "rm -rf /var/lib/rkv/n$i; mkdir -p /var/lib/rkv/n$i; nohup /tmp/rkv-server --id $i --addr $h:20160 --http $h:20180 --data /var/lib/rkv/n$i --peers $peers --election-tick ${ELECTION_TICK:-10} --strict-fsync > /var/log/rkv-n$i.log 2>&1 < /dev/null &"
    i=$((i+1)); done
  [ "$DRY_RUN" = 1 ] || sleep 8
  R $CLIENT "/tmp/kvctl --seeds $seeds put n1-probe ok && /tmp/kvctl --seeds $seeds get n1-probe" || { echo "cluster did not come up; see /var/log/rkv-n*.log"; exit 4; }
}
r1() {
  echo "### N1/R1: throughput and latency vs injected network delay and loss"
  for delay in ${DELAYS:-$(pick "0 10" "0 1 10 50")}; do
    # election timeout must exceed a few round trips or the leader loses its quorum to late heartbeats: scale it with the injected delay
    et=10; [ "$delay" -ge 10 ] && et=20; [ "$delay" -ge 50 ] && et=60
    ELECTION_TICK=$et deploy
    for loss in ${LOSSES:-$(pick "0" "0 1")}; do
    for h in $HOSTS; do bash "$D/netem.sh" $h add $delay $loss; done
    R $CLIENT "/tmp/rkv-bench cluster --seeds $seeds --workload a --records $(pick 2000 50000) --secs $(pick 10 60) --clients $(pick 8 64) --id N1-r1-d${delay}ms-l${loss}pct --scope-label '$label; tc netem delay=${delay}ms loss=${loss}% (election timeout ${et}x20ms)' --out $RESULTS"
  done; done
  for h in $HOSTS; do bash "$D/netem.sh" $h clear; done
}
r2() {
  echo "### N1/R2: leader isolation with iptables, client-observed window ($(pick 5 100) trials)"
  R $CLIENT "rm -f $RESULTS/n1_r2_windows_ms.txt"
  for t in $(seq 1 $(pick 5 100)); do
    R $CLIENT "/tmp/kvctl --seeds $seeds put probe v >/dev/null"
    # the leader's address, read from kvctl status ("store N @ addr" lines followed by range lines containing LEADER)
    L=$(R $CLIENT "/tmp/kvctl --seeds $seeds status | awk '/^store/{a=\$4} /LEADER/{print a; exit}' | cut -d: -f1" || true)
    [ -n "$L" ] || L=$H1
    others=$(for h in $HOSTS; do [ "$h" = "$L" ] || printf '%s ' $h; done)
    echo "trial $t: isolating leader $L from: $others" >&2
    bash "$D/iptables_partition.sh" $L isolate "$others"
    R $CLIENT "s=\$(date +%s%N); until /tmp/kvctl --seeds $seeds put probe t$t >/dev/null 2>&1; do sleep 0.02; done; echo \$(( (\$(date +%s%N)-s)/1000000 )) >> $RESULTS/n1_r2_windows_ms.txt"
    bash "$D/iptables_partition.sh" $L heal; [ "$DRY_RUN" = 1 ] || sleep 3
  done
  echo "windows (ms) are in $RESULTS/n1_r2_windows_ms.txt; convert with: python3 deploy/cloud/wrap_windows.py $RESULTS/n1_r2_windows_ms.txt bench/results"
}
disk() {
  echo "### N1/disk: node 1's data on a dm-flakey device (periodic IO errors): the node must fail-stop, never ack lost data"
  [ "$DRY_RUN" = 1 ] || sudo modprobe dm_flakey 2>/dev/null || { echo "dm_flakey unavailable: skipping"; return 0; }
  bash "$D/dmflakey.sh" $H1 up /var/lib/flaky.img 512 5 2
  R $H1 "pkill -9 -f '[r]kv-server --id 1' || true"      # separate call: a combined command line would match its own pattern
  R $H1 "mkdir -p /mnt/flaky/rkv; nohup /tmp/rkv-server --id 1 --addr $H1:20160 --http $H1:20180 --data /mnt/flaky/rkv --peers $peers --strict-fsync > /var/log/rkv-flaky.log 2>&1 < /dev/null &"
  R $CLIENT "/tmp/rkv-bench cluster --seeds $seeds --workload w --records $(pick 500 5000) --secs $(pick 15 60) --clients 16 --id N1-disk-flakey --fault-label 'dm-flakey up=5s down=2s on node 1' --scope-label '$label' --out $RESULTS"
  bash "$D/dmflakey.sh" $H1 down
}
flame() {
  command -v perf >/dev/null || [ "$DRY_RUN" = 1 ] || { echo "perf missing: skipping"; return 0; }
  secs=$(pick 10 30)
  # profile a server while the cluster is under write load
  R $CLIENT "nohup /tmp/rkv-bench cluster --seeds $seeds --workload w --records 2000 --secs $((secs+8)) --clients 16 --id N1-flame-load --out $RESULTS > /dev/null 2>&1 < /dev/null &"
  [ "$DRY_RUN" = 1 ] || sleep 4
  bash "$D/perf_flame.sh" $H1 $secs "${RESULTS}/n1_flame.svg"
}

case "$STEP" in
  check) check ;;
  r1) check; r1 ;; r2) check; deploy; r2 ;; disk) check; deploy; disk ;; flame) check; deploy; flame ;;
  all) check; deploy; r1; deploy; r2; disk; flame ;;
  *) echo "usage: n1.sh [check|r1|r2|disk|flame|all]"; exit 2 ;;
esac
echo "done. results in $RESULTS on the client host"
