#!/bin/sh
# Container entrypoint. Derives node identity from the StatefulSet pod name (rkv-store-2 -> store id 3).
#   RKV_ROLE=pd|store            RKV_PD_ENDPOINTS=host:port,...       RKV_PD_PEERS=1=host:port,2=...  (pd role)
#   RKV_NAMESPACE_SUFFIX=.rkv.svc.cluster.local   (appended to pod DNS names on Kubernetes)
set -eu
ROLE="${RKV_ROLE:-store}"
POD="${POD_NAME:-$(hostname)}"
ORD="${POD##*-}"
ID=$((ORD + 1))
SUFFIX="${RKV_NAMESPACE_SUFFIX:-}"
case "$ROLE" in
  pd)
    SVC="${RKV_PD_SERVICE:-rkv-pd}"
    exec rkv-pd --id "$ID" --addr "0.0.0.0:2379" --data /data --peers "$RKV_PD_PEERS"
    ;;
  store)
    SVC="${RKV_STORE_SERVICE:-rkv-store}"
    ADV="${POD}.${SVC}${SUFFIX}:20160"
    exec rkv-server --id "$ID" --addr "0.0.0.0:20160" --advertise "$ADV" --http "0.0.0.0:20180" --data /data --pd "$RKV_PD_ENDPOINTS" ${RKV_EXTRA_ARGS:-}
    ;;
  *) exec "$@" ;;
esac
