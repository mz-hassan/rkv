#![allow(clippy::result_large_err, clippy::type_complexity, clippy::too_many_arguments)]
//! rkv-operator: manages an `RkvCluster` custom resource.
//!
//!   * rolling upgrade: change `spec.image`; the operator patches the store StatefulSet with `partition = replicas`
//!     (nothing restarts), then lowers the partition one pod at a time, each step gated on cluster health read from PD
//!     (all stores alive, every range has a leader and >= min(3, replicas) voters).
//!   * scale out: raise `spec.storeReplicas`; the PD scheduler moves replicas onto the new store by itself.
//!   * scale in: the operator asks PD to drain the highest-numbered store (`evict_store`); only when it holds zero
//!     ranges does it reduce the StatefulSet, so Raft membership never drops below quorum.
//!
//! `rkv-operator crd` prints the CRD (kubectl apply -f -).
use ::kv_client::PdClient;
use futures::StreamExt;
use k8s_openapi::api::apps::v1::StatefulSet;
use kube::api::{Api, Patch, PatchParams};
use kube::runtime::controller::{Action, Controller};
use kube::runtime::watcher::Config;
use kube::{Client, CustomResource, CustomResourceExt, ResourceExt};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(CustomResource, Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[kube(group = "rkv.io", version = "v1alpha1", kind = "RkvCluster", namespaced, status = "RkvClusterStatus", shortname = "rkv")]
#[serde(rename_all = "camelCase")]
pub struct RkvClusterSpec {
    pub image: String,
    #[serde(default = "three")]
    pub store_replicas: i32,
    #[serde(default = "three")]
    pub replication: i32,
}

fn three() -> i32 {
    3
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RkvClusterStatus {
    pub phase: String,
    pub message: String,
    pub ready_stores: i32,
    pub image: String,
    pub last_reconcile_ms: u64,
}

struct Ctx {
    client: Client,
}

#[derive(Debug)]
enum Error {
    Kube(kube::Error),
    Other(String),
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Kube(e) => write!(f, "{e}"),
            Error::Other(s) => write!(f, "{s}"),
        }
    }
}
impl std::error::Error for Error {}
impl From<kube::Error> for Error {
    fn from(e: kube::Error) -> Self {
        Error::Kube(e)
    }
}

/// Cluster health as seen by PD.
struct Health {
    stores_alive: usize,
    ranges: usize,
    ranges_ok: bool,
    store_ranges: std::collections::HashMap<u64, u64>,
}

async fn pd_health(ns: &str, pd_replicas: usize, min_voters: usize) -> Result<Health, Error> {
    let addrs = (0..pd_replicas).map(|i| format!("rkv-pd-{i}.rkv-pd.{ns}.svc.cluster.local:2379")).collect();
    let pd = PdClient::new(addrs);
    let all = pd.all_ranges().await.map_err(Error::Other)?;
    let mut store_ranges = std::collections::HashMap::new();
    for r in &all.ranges {
        for p in &r.peers {
            *store_ranges.entry(p.store_id).or_insert(0u64) += 1;
        }
    }
    Ok(Health {
        stores_alive: all.stores.iter().filter(|s| s.alive).count(),
        ranges: all.ranges.len(),
        ranges_ok: !all.ranges.is_empty()
            && all
                .ranges
                .iter()
                .all(|r| r.leader_store != 0 && r.peers.iter().filter(|p| !p.learner).count() >= min_voters && r.peers.iter().all(|p| !p.learner)),
        store_ranges,
    })
}

fn sts_image(s: &StatefulSet) -> String {
    s.spec.as_ref().and_then(|s| s.template.spec.as_ref()).and_then(|p| p.containers.first()).and_then(|c| c.image.clone()).unwrap_or_default()
}

#[allow(unused_assignments)]
async fn reconcile(obj: Arc<RkvCluster>, ctx: Arc<Ctx>) -> Result<Action, Error> {
    let t0 = Instant::now();
    let ns = obj.namespace().unwrap_or_else(|| "rkv".into());
    let stores: Api<StatefulSet> = Api::namespaced(ctx.client.clone(), &ns);
    let clusters: Api<RkvCluster> = Api::namespaced(ctx.client.clone(), &ns);
    let sts = stores.get("rkv-store").await?;
    let cur = sts.spec.as_ref().and_then(|s| s.replicas).unwrap_or(0);
    let ready = sts.status.as_ref().and_then(|s| s.ready_replicas).unwrap_or(0);
    let updated = sts.status.as_ref().and_then(|s| s.updated_replicas).unwrap_or(0);
    let partition = sts.spec.as_ref().and_then(|s| s.update_strategy.as_ref()).and_then(|u| u.rolling_update.as_ref()).and_then(|r| r.partition).unwrap_or(0);
    let want = obj.spec.store_replicas;
    let min_voters = (obj.spec.replication.max(1) as usize).min(cur.max(1) as usize);
    let pp = PatchParams::apply("rkv-operator").force();
    let mut phase = "Ready".to_string();
    let mut message = String::new();
    let mut requeue = Duration::from_secs(30);

    let patch_sts = |body: serde_json::Value| {
        let stores = stores.clone();
        let pp = pp.clone();
        async move { stores.patch("rkv-store", &pp, &Patch::Apply(&body)).await }
    };
    let apply_body = |replicas: i32, image: Option<&str>, partition: i32| {
        let mut b = json!({ "apiVersion": "apps/v1", "kind": "StatefulSet", "metadata": { "name": "rkv-store", "namespace": ns },
            "spec": { "replicas": replicas, "updateStrategy": { "type": "RollingUpdate", "rollingUpdate": { "partition": partition } } } });
        if let Some(img) = image {
            b["spec"]["template"] = json!({ "spec": { "containers": [{ "name": "store", "image": img }] } });
        }
        b
    };

    if want > cur {
        // scale out: the PD scheduler spreads replicas onto the new stores
        patch_sts(apply_body(want, None, partition)).await?;
        phase = "ScalingOut".into();
        message = format!("storeReplicas {cur} -> {want}");
        requeue = Duration::from_secs(5);
    } else if want < cur {
        // scale in: drain the highest store, then remove its pod
        let victim = cur as u64; // pod ordinal cur-1 == store id cur
        let pd = PdClient::new((0..3).map(|i| format!("rkv-pd-{i}.rkv-pd.{ns}.svc.cluster.local:2379")).collect());
        let _ = pd.set_config("evict_store", &victim.to_string()).await;
        let h = pd_health(&ns, 3, 1).await?;
        let left = h.store_ranges.get(&victim).copied().unwrap_or(0);
        if left == 0 {
            patch_sts(apply_body(cur - 1, None, partition)).await?;
            let _ = pd.set_config("evict_store", "0").await;
            message = format!("store {victim} drained and removed");
        } else {
            message = format!("draining store {victim}: {left} ranges left");
        }
        phase = "ScalingIn".into();
        requeue = Duration::from_secs(5);
    } else if sts_image(&sts) != obj.spec.image {
        // start a rolling upgrade: change the template but hold every pod back
        patch_sts(apply_body(cur, Some(&obj.spec.image), cur)).await?;
        phase = "Upgrading".into();
        message = format!("image -> {}; holding all {cur} pods", obj.spec.image);
        requeue = Duration::from_secs(3);
    } else if partition > 0 || updated < cur {
        // advance the upgrade by one pod when the cluster is healthy
        phase = "Upgrading".into();
        let healthy = ready == cur && pd_health(&ns, 3, min_voters).await.map(|h| h.stores_alive as i32 == cur && h.ranges_ok).unwrap_or(false);
        if healthy && partition > 0 {
            patch_sts(apply_body(cur, None, partition - 1)).await?;
            message = format!("released pod {} (partition {} -> {})", partition - 1, partition, partition - 1);
        } else {
            message = format!("waiting for health before releasing the next pod (ready {ready}/{cur}, partition {partition})");
        }
        requeue = Duration::from_secs(4);
    } else {
        let h = pd_health(&ns, 3, min_voters).await;
        message = match h {
            Ok(h) => format!("{} stores alive, {} ranges, ranges healthy: {}", h.stores_alive, h.ranges, h.ranges_ok),
            Err(e) => format!("PD not reachable: {e}"),
        };
    }

    let status = RkvClusterStatus {
        phase: phase.clone(),
        message: message.clone(),
        ready_stores: ready,
        image: sts_image(&sts),
        last_reconcile_ms: t0.elapsed().as_millis() as u64,
    };
    clusters.patch_status(&obj.name_any(), &PatchParams::default(), &Patch::Merge(json!({ "status": status }))).await?;
    tracing::info!(cluster = %obj.name_any(), %phase, %message, reconcile_ms = status.last_reconcile_ms, "reconciled");
    Ok(Action::requeue(requeue))
}

fn error_policy(_obj: Arc<RkvCluster>, err: &Error, _ctx: Arc<Ctx>) -> Action {
    tracing::warn!("reconcile failed: {err}");
    Action::requeue(Duration::from_secs(10))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into())).init();
    if std::env::args().nth(1).as_deref() == Some("crd") {
        println!("{}", serde_json::to_string_pretty(&RkvCluster::crd())?);
        return Ok(());
    }
    let client = Client::try_default().await?;
    let clusters: Api<RkvCluster> = Api::all(client.clone());
    tracing::info!("rkv-operator started");
    Controller::new(clusters, Config::default())
        .run(reconcile, error_policy, Arc::new(Ctx { client }))
        .for_each(|r| async move {
            if let Err(e) = r {
                tracing::warn!("controller error: {e}");
            }
        })
        .await;
    Ok(())
}
