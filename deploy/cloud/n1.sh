#!/usr/bin/env bash
# N1 (L2): real-network benchmarks on separate Linux machines.
#   HOSTS="10.0.0.11 10.0.0.12 10.0.0.13" CLIENT=10.0.0.14 deploy/cloud/n1.sh [r1|r2|disk|flame|all]
#   DRY_RUN=1 prints the whole plan without touching any host (validated by `make dry-run`).
# Results are written on the client host by rkv-bench / kvctl into bench/results and copied back.
source "$(dirname "$0")/lib.sh"
HOSTS="${HOSTS:-10.0.0.11 10.0.0.12 10.0.0.13}"; CLIENT="${CLIENT:-10.0.0.14}"
BIN="${RKV_BIN_DIR:-./target/release}"; STEP="${1:-all}"
set -- $HOSTS; H1=$1
peers=""; i=1; for h in $HOSTS; do peers+="${peers:+,}$i=$h:20160"; i=$((i+1)); done
seeds=$(for h in $HOSTS; do printf '%s:20160,' $h; done | sed 's/,$//')

deploy() {
  echo "### deploy binaries and start the cluster"
  i=1
  for h in $HOSTS $CLIENT; do
    C $h $BIN/rkv-server /tmp/rkv-server; C $h $BIN/kvctl /tmp/kvctl; C $h $BIN/rkv-bench /tmp/rkv-bench; R $h "chmod +x /tmp/rkv-* /tmp/kvctl; mkdir -p /var/lib/rkv"
  done
  i=1; for h in $HOSTS; do
    R $h "pkill -9 rkv-server || true; rm -rf /var/lib/rkv/*; nohup /tmp/rkv-server --id $i --addr $h:20160 --http $h:20180 --data /var/lib/rkv --peers $peers --strict-fsync > /var/log/rkv.log 2>&1 &"
    i=$((i+1)); done
  [ "$DRY_RUN" = 1 ] || sleep 8
}
r1() {
  echo "### N1/R1: throughput and latency vs injected network delay and loss"
  for delay in 0 1 10 50; do for loss in 0 1; do
    for h in $HOSTS; do bash "$(dirname "$0")/netem.sh" $h add $delay $loss; done
    R $CLIENT "/tmp/rkv-bench cluster --seeds $seeds --workload a --records 50000 --secs 60 --clients 64 --id N1-r1-d${delay}ms-l${loss}pct --scope-label 'multi-VM real network + tc netem delay=${delay}ms loss=${loss}%' --out /tmp/results"
  done; done
  for h in $HOSTS; do bash "$(dirname "$0")/netem.sh" $h clear; done
}
r2() {
  echo "### N1/R2: leader isolation with iptables, client-observed window (100 trials)"
  for t in $(seq 1 100); do
    R $CLIENT "/tmp/kvctl --seeds $seeds put probe v >/dev/null"
    R $CLIENT "L=\$(/tmp/kvctl --seeds $seeds status | awk '/LEADER/{f=1} /^store/{s=\$2} f{print s; exit}'); echo leader=\$L"
    others=$(echo $HOSTS | tr ' ' '\n' | tail -n +2 | tr '\n' ' ')
    bash "$(dirname "$0")/iptables_partition.sh" $H1 isolate "$others"
    R $CLIENT "s=\$(date +%s%N); until /tmp/kvctl --seeds $seeds put probe t$t >/dev/null 2>&1; do sleep 0.02; done; echo \$(( (\$(date +%s%N)-s)/1000000 )) >> /tmp/n1_r2_windows_ms.txt"
    bash "$(dirname "$0")/iptables_partition.sh" $H1 heal; [ "$DRY_RUN" = 1 ] || sleep 3
  done
  echo "windows_ms: scp from $CLIENT:/tmp/n1_r2_windows_ms.txt then compute p50/p95/p99 (bench/scripts/results.py reads result.json; wrap with deploy/cloud/wrap_windows.py)"
}
disk() {
  echo "### N1/disk: store data on a dm-flakey device (periodic IO errors) - process must fail-stop, never ack lost data"
  bash "$(dirname "$0")/dmflakey.sh" $H1 up /var/lib/flaky.img 512 5 2
  R $H1 "pkill -9 rkv-server || true; nohup /tmp/rkv-server --id 1 --addr $H1:20160 --data /mnt/flaky/rkv --peers $peers --strict-fsync > /var/log/rkv-flaky.log 2>&1 &"
  R $CLIENT "/tmp/rkv-bench cluster --seeds $seeds --workload w --records 5000 --secs 60 --clients 16 --id N1-disk-flakey --fault-label 'dm-flakey up=5s down=2s on node 1' --out /tmp/results"
  bash "$(dirname "$0")/dmflakey.sh" $H1 down
}
flame() { bash "$(dirname "$0")/perf_flame.sh" $H1 30 "bench/plots/n1_flame_$H1.svg"; }

case "$STEP" in
  r1) deploy; r1 ;; r2) deploy; r2 ;; disk) deploy; disk ;; flame) flame ;;
  all) deploy; r1; r2; disk; flame ;;
  *) echo "usage: n1.sh [r1|r2|disk|flame|all]"; exit 2 ;;
esac
echo "copy results back:  scp -r $SSH_USER@$CLIENT:/tmp/results/* bench/results/"
