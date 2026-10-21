//! rkv-server --id 1 --addr 127.0.0.1:20160 --data ./data/1 --peers 1=127.0.0.1:20160,2=...,3=... [--pd host:port,...]
//!            [--http 127.0.0.1:20180] [--fsync always|group|interval|never] [--strict-fsync] [--split-bytes N]
use kv_server::{http, serve, Store, StoreConfig};
use kv_engine::SyncPolicy;
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
    let data = arg(&a, "--data").unwrap_or_else(|| format!("./data/{id}"));
    let mut cfg = StoreConfig::new(id, &addr, &data);
    if let Some(p) = arg(&a, "--peers") {
        let m: BTreeMap<u64, String> = p.split(',').filter_map(|x| x.split_once('=')).map(|(k, v)| (k.parse().unwrap(), v.to_string())).collect();
        cfg.static_peers = m;
    }
    if let Some(p) = arg(&a, "--pd") {
        cfg.pd_addrs = p.split(',').map(|s| s.to_string()).collect();
    }
    cfg.strict_fsync = a.iter().any(|x| x == "--strict-fsync");
    cfg.engine.sync = match arg(&a, "--fsync").as_deref() {
        Some("always") => SyncPolicy::Always,
        Some("never") => SyncPolicy::Never,
        Some(s) if s.starts_with("interval") => SyncPolicy::Interval(s.trim_start_matches("interval").trim_start_matches('=').parse().unwrap_or(100)),
        _ => SyncPolicy::Group,
    };
    if let Some(s) = arg(&a, "--split-bytes") {
        cfg.split_threshold_bytes = s.parse().unwrap();
    }
    if let Some(s) = arg(&a, "--election-tick") {
        cfg.group.election_tick = s.parse().unwrap();
    }
    if let Some(s) = arg(&a, "--tick-ms") {
        cfg.group.tick_ms = s.parse().unwrap();
    }
    if let Some(s) = arg(&a, "--snapshot-threshold") {
        cfg.group.snapshot_threshold = s.parse().unwrap();
    }
    cfg.group.sync = !a.iter().any(|x| x == "--no-raft-fsync");
    cfg.group.pre_vote = !a.iter().any(|x| x == "--no-prevote");
    cfg.group.check_quorum = !a.iter().any(|x| x == "--no-check-quorum");
    cfg.group.lease_reads = a.iter().any(|x| x == "--lease-reads");
    let store = Store::open(cfg).await.expect("open store");
    if let Some(h) = arg(&a, "--http") {
        tokio::spawn(http::serve(store.clone(), h));
    }
    tracing::info!(store = id, %addr, "rkv-server listening");
    serve(store, addr.parse().expect("--addr host:port")).await.expect("serve");
}
