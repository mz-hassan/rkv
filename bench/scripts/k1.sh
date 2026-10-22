#!/usr/bin/env bash
# K1: Kubernetes on kind (needs Docker/colima + kind + kubectl). Rolling upgrade under load with an acked-write ledger,
# scale 3->5->3 via the operator, operator reconciliation time. See deploy/k8s/README.md.
source "$(dirname "$0")/lib.sh"; banner K1
if [ "$DRY_RUN" != 1 ]; then
  command -v docker >/dev/null && docker info >/dev/null 2>&1 || { echo "K1 NOT RUN: no Docker daemon (brew install docker colima && colima start)"; exit 3; }
  command -v kind >/dev/null && command -v kubectl >/dev/null || { echo "K1 NOT RUN: need kind and kubectl"; exit 3; }
fi
run docker build -t rkv:dev -f deploy/docker/Dockerfile .
run kind create cluster --name rkv --config deploy/k8s/kind-config.yaml
run kind load docker-image rkv:dev --name rkv
run kubectl apply -k deploy/k8s/overlays/kind
run kubectl -n rkv rollout status statefulset/rkv-pd --timeout=300s
run kubectl -n rkv rollout status statefulset/rkv-store --timeout=300s
# rolling upgrade under write load; the load generator keeps a ledger of acknowledged writes
run bench/scripts/k1_rolling_upgrade.sh
run kind delete cluster --name rkv
