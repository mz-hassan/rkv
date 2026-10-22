# Kubernetes deployment

* `base/` - namespace, headless services, PD StatefulSet (3), store StatefulSet (3), PodDisruptionBudgets.
* `overlays/kind/` - small PVCs for a local kind cluster; `overlays/cloud/` - zone spreading, bigger disks.
* `operator/` - the `RkvCluster` CRD, RBAC and the operator Deployment (crate `crates/operator`).
* `kind-config.yaml` - 1 control plane + 3 workers.

Quick path (needs Docker or colima, kind, kubectl): `make bench-k1` runs `bench/scripts/k1.sh`, which builds the image, creates the
kind cluster, applies `overlays/kind`, runs a bank workload while performing a rolling restart, and audits the invariant.

Validate manifests offline: `kubectl kustomize deploy/k8s/overlays/kind | kubeconform -ignore-missing-schemas -summary`.
