#!/usr/bin/env bash
# R1 baseline: local etcd cluster (native binary, fsync on) driven by the SAME rkv-bench workload through etcd-client.
# Build the bench with the baseline client first:  cargo build --release -p ycsb --features etcd-baseline
# Caveats to report: single machine, shared CPU/disk, no network; etcd v3 API vs rkv gRPC; default etcd tuning.
source "$(dirname "$0")/lib.sh"; need rkv-bench; banner "R1 etcd baseline"
command -v etcd >/dev/null || [ "$DRY_RUN" = 1 ] || { echo "etcd missing: brew install etcd"; exit 1; }
REC=$(pick 300 100000); OPS=$(pick 600 100000); CL=$(pick 4 32)
D=/tmp/rkv-etcd; PIDS=/tmp/rkv-etcd.pids
stop() {
  if [ -f $PIDS ]; then
    while read -r p; do kill "$p" 2>/dev/null || true; done < $PIDS
    # wait until every member has really exited, otherwise the next cluster hits "address already in use" / "already bootstrapped"
    while read -r p; do for _ in $(seq 1 100); do kill -0 "$p" 2>/dev/null || break; sleep 0.1; done; done < $PIDS
    rm -f $PIDS
  fi
  sleep 1
}
trap stop EXIT
for n in $(pick "3" "${ETCD_NODES:-1 3 5}"); do
  run rm -rf $D; mkdir -p $D; : > $PIDS
  cluster_str=""; for i in $(seq 1 $n); do cluster_str+="${cluster_str:+,}n$i=http://127.0.0.1:$((23800+i))"; done
  eps=""; for i in $(seq 1 $n); do eps+="${eps:+,}127.0.0.1:$((23700+i))"; done
  for i in $(seq 1 $n); do
    if [ "$DRY_RUN" = 1 ]; then echo "+ etcd --name n$i ..."; continue; fi
    etcd --name n$i --data-dir $D/n$i --listen-client-urls http://127.0.0.1:$((23700+i)) --advertise-client-urls http://127.0.0.1:$((23700+i)) \
      --listen-peer-urls http://127.0.0.1:$((23800+i)) --initial-advertise-peer-urls http://127.0.0.1:$((23800+i)) --initial-cluster "$cluster_str" --initial-cluster-state new >$D/n$i.log 2>&1 &
    echo $! >> $PIDS
  done
  [ "$DRY_RUN" = 1 ] || sleep 4
  for sz in $(pick "128" "128 1024 16384"); do
    # 16 KB values: 10x fewer records/ops (the snapshot path holds a whole range in memory, see LIMITATIONS.md); same for etcd
    if [ "$sz" -ge 16384 ]; then RECX=$((REC/10)); OPSX=$((OPS/10)); else RECX=$REC; OPSX=$OPS; fi
    for mix in w a r; do
      run $BIN/rkv-bench cluster --backend etcd --etcd-endpoints $eps --workload $mix --records $RECX --ops $OPSX --clients $CL --value-size $sz \
        --id R1-etcd-${n}n-mix$mix-v$sz --cluster-label "etcd $(etcd --version 2>/dev/null | head -1) ${n} members localhost" --out $OUT
    done
  done
  stop
done
