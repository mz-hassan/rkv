#!/usr/bin/env bash
# K1: Kubernetes on kind (needs Docker/colima + kind + kubectl). Operator-driven rolling upgrade under load with a
# conservation audit, scale 3 -> 5 -> 3 via PD-drained membership changes, operator reconciliation time.
source "$(dirname "$0")/lib.sh"; banner K1
if [ "$DRY_RUN" != 1 ]; then
  command -v docker >/dev/null && docker info >/dev/null 2>&1 || { echo "K1 NOT RUN: no Docker daemon (brew install docker colima && colima start --cpu 4 --memory 8)"; exit 3; }
  command -v kind >/dev/null && command -v kubectl >/dev/null || { echo "K1 NOT RUN: need kind and kubectl"; exit 3; }
fi
# PREBUILT=1: package binaries already built in target/release (fast; Linux only) instead of compiling inside Docker
if [ "${PREBUILT:-0}" = 1 ]; then
  # --no-cache: with the legacy Docker builder a COPY of a rebuilt binary can silently reuse a stale layer (the cluster then runs old code)
  run docker build --no-cache -t rkv:dev -f deploy/docker/Dockerfile.prebuilt .
  run docker build --no-cache -t rkv-operator:dev -f deploy/docker/Dockerfile.operator.prebuilt .
else
  run docker build -t rkv:dev -f deploy/docker/Dockerfile .
  run docker build -t rkv-operator:dev -f deploy/docker/Dockerfile.operator .
fi
run docker tag rkv:dev rkv:v2
run kind create cluster --name rkv --config ${KIND_CONFIG:-deploy/k8s/kind-config.yaml}
run kind load docker-image rkv:dev rkv:v2 rkv-operator:dev --name rkv
run kubectl apply -k deploy/k8s/overlays/kind
run kubectl apply -k deploy/k8s/operator
run kubectl apply -f deploy/k8s/operator/sample-cluster.yaml
run kubectl -n rkv rollout status statefulset/rkv-pd --timeout=300s
run kubectl -n rkv rollout status statefulset/rkv-store --timeout=300s
run bench/scripts/k1_rolling_upgrade.sh
run kind delete cluster --name rkv
