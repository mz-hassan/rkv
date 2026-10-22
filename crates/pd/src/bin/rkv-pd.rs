//! rkv-pd --id 1 --addr 127.0.0.1:2379 --data ./pd1 --peers 1=127.0.0.1:2379,2=...,3=...
use pd::{service, Pd, PdConfig};
use raft_driver::GroupOptions;
use std::collections::BTreeMap;

fn arg(a: &[String], n: &str) -> Option<String> {
    a.iter().position(|x| x == n).and_then(|i| a.get(i + 1).cloned())
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into())).init();
    let a: Vec<String> = std::env::args().collect();
    let id: u64 = arg(&a, "--id").expect("--id").parse().unwrap();
    let addr = arg(&a, "--addr").expect("--addr");
    let data = arg(&a, "--data").unwrap_or_else(|| format!("./pd{id}"));
    let peers: BTreeMap<u64, String> = arg(&a, "--peers")
        .map(|p| p.split(',').filter_map(|x| x.split_once('=')).map(|(k, v)| (k.parse().unwrap(), v.to_string())).collect())
        .unwrap_or_else(|| [(id, addr.clone())].into_iter().collect());
    let pd = Pd::start(PdConfig { id, addr: addr.clone(), data_dir: data, peers, group: GroupOptions::default() }).await.expect("start pd");
    tracing::info!(pd = id, %addr, "rkv-pd listening");
    service::serve(pd, addr.parse().unwrap()).await.expect("serve");
}
