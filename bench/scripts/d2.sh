#!/usr/bin/env bash
# D2: transactions. Contention sweep, write skew (snapshot vs serializable), coordinator-crash lock recovery, bank under chaos.
source "$(dirname "$0")/lib.sh"; need rkv-bench rkv-chaos rkv-server rkv-pd; banner D2
PD=127.0.0.1:23790; ACC=$(pick 200 1000); OPS=$(pick 300 20000)
cluster up 3 pd; sleep 6
for theta in 0 0.5 0.9 0.99; do
  for cl in $(pick "4" "8 32 128"); do
    run $BIN/rkv-bench txn-bank --pd $PD --accounts $ACC --clients $cl --theta $theta --keys-per-txn 4 --ops $OPS --id D2-bank-theta$theta-c$cl --out $OUT
  done
done
run $BIN/rkv-bench txn-bank --pd $PD --accounts $ACC --clients 32 --theta 0.9 --isolation serializable --ops $OPS --id D2-bank-serializable --out $OUT
run $BIN/rkv-bench txn-skew --pd $PD --pairs $(pick 30 500) --id D2-write-skew --out $OUT
run $BIN/rkv-bench lock-recovery --pd $PD --trials $(pick 5 100) --ttl-ms 500 --id D2-lock-recovery --out $OUT
cluster down
for nem in kill-leader partition-half mixed; do
  run $BIN/rkv-chaos bank --nemesis $nem --secs $(pick 10 60) --accounts 20 --clients 8 --id J-bank-$nem --out $OUT
done
