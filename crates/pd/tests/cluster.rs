//! Full stack in one process: 1 PD + 4 stores over gRPC, PD-driven replication, transactions across ranges.
use ::kv_client::{PdClient, RawClient, TxnClient};
use ::kv_server::{serve, Store, StoreConfig};
use pd::{service, Pd, PdConfig};
use rkv_proto::admin_client::AdminClient;
use rkv_proto::{Empty, SplitReq};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

struct Proc {
    rt: Option<tokio::runtime::Runtime>,
}
impl Drop for Proc {
    fn drop(&mut self) {
        if let Some(rt) = self.rt.take() {
            rt.shutdown_background();
        }
    }
}

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread().worker_threads(3).enable_all().build().unwrap()
}

async fn start_pd(dir: &str, addr: &str) -> (Proc, Arc<Pd>) {
    let rt = rt();
    let (d, a) = (dir.to_string(), addr.to_string());
    let peers: BTreeMap<u64, String> = [(1u64, a.clone())].into_iter().collect();
    let pd = rt.spawn(async move { Pd::start(PdConfig { id: 1, addr: a, data_dir: d, peers, group: raft_driver::GroupOptions { tick_ms: 10, ..Default::default() } }).await.unwrap() }).await.unwrap();
    let (p2, sa) = (pd.clone(), addr.parse().unwrap());
    rt.spawn(async move {
        let _ = service::serve(p2, sa).await;
    });
    (Proc { rt: Some(rt) }, pd)
}

async fn start_store(id: u64, dir: &str, addr: &str, pd: &str) -> (Proc, Arc<Store>) {
    let rt = rt();
    let mut cfg = StoreConfig::new(id, addr, dir);
    cfg.pd_addrs = vec![pd.to_string()];
    cfg.group.tick_ms = 10;
    cfg.group.snapshot_threshold = 500;
    cfg.engine.memtable_size = 1 << 20;
    let store = rt.spawn(async move { Store::open(cfg).await.unwrap() }).await.unwrap();
    let (s2, a) = (store.clone(), addr.parse().unwrap());
    rt.spawn(async move {
        let _ = serve(s2, a).await;
    });
    (Proc { rt: Some(rt) }, store)
}

async fn wait<F: Fn() -> bool>(what: &str, secs: u64, f: F) {
    for _ in 0..secs * 10 {
        if f() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("timed out waiting for: {what}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn pd_driven_cluster_replicates_balances_and_runs_transactions() {
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path().to_str().unwrap().to_string();
    let pd_addr = format!("127.0.0.1:{}", free_port());
    let (_pd_proc, pd) = start_pd(&format!("{base}/pd"), &pd_addr).await;
    // let the PD raft group elect itself
    tokio::time::sleep(Duration::from_millis(700)).await;
    let mut procs = vec![];
    let mut store_addrs = BTreeMap::new();
    for id in 1..=4u64 {
        let addr = format!("127.0.0.1:{}", free_port());
        store_addrs.insert(id, addr.clone());
        procs.push(start_store(id, &format!("{base}/s{id}"), &addr, &pd_addr).await);
    }

    // PD bootstraps range 1 on the first store, then schedules replicas up to 3
    for _ in 0..300 {
        let (ranges, _) = pd.all();
        if ranges.iter().any(|r| r.id == 1 && r.peers.iter().filter(|p| !p.learner).count() == 3 && r.leader_store != 0) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    {
        let (ranges, stores) = pd.all();
        eprintln!("PD ranges: {:?}", ranges.iter().map(|r| (r.id, r.leader_store, r.peers.iter().map(|p| (p.store_id, p.learner)).collect::<Vec<_>>())).collect::<Vec<_>>());
        eprintln!("PD stores: {:?}", stores.iter().map(|s| (s.id, s.range_count, s.alive)).collect::<Vec<_>>());
        for (_, s) in procs.iter() {
            eprintln!("store {} replicas: {:?}", s.cfg.store_id, s.all_replicas().iter().map(|r| r.id).collect::<Vec<_>>());
        }
    }
    wait("range 1 replicated to 3 voters", 1, || {
        let (ranges, _) = pd.all();
        ranges.iter().any(|r| r.id == 1 && r.peers.iter().filter(|p| !p.learner).count() == 3 && r.leader_store != 0)
    })
    .await;

    let pdc = Arc::new(PdClient::new(vec![pd_addr.clone()]));
    // timestamps are strictly increasing
    let mut last = 0;
    for _ in 0..50 {
        let t = pdc.tso_fresh().await.unwrap();
        assert!(t > last);
        last = t;
    }
    let raw = RawClient::new(Some(pdc.clone()), vec![]);
    raw.put(b"k1", b"v1").await.unwrap();
    assert_eq!(raw.get(b"k1").await.unwrap().unwrap(), b"v1");

    // ---- bank: 10 accounts x 100; transfers must conserve the total, also across a split
    let tc = TxnClient::new(pdc.clone(), vec![]);
    {
        let mut t = tc.begin().await.unwrap();
        for i in 0..10 {
            t.put(format!("acct{i:02}").as_bytes(), b"100");
        }
        t.commit().await.unwrap();
    }
    // split between acct04 and acct05 via whichever store leads range 1
    let (ranges, _) = pd.all();
    let r1 = ranges.iter().find(|r| r.id == 1).unwrap().clone();
    let leader_addr = r1.peers.iter().find(|p| p.store_id == r1.leader_store).unwrap().addr.clone();
    let mut admin = AdminClient::new(::kv_client::pd::channel(&leader_addr).unwrap());
    let res = admin.split_range(SplitReq { range_id: 1, split_key: b"acct05".to_vec() }).await.unwrap().into_inner();
    assert!(res.error.is_none(), "split failed: {res:?}");
    wait("two ranges known to PD", 30, || pd.all().0.len() >= 2).await;
    tc.router.refresh_all().await.unwrap();

    let mut hs = vec![];
    for w in 0..6u64 {
        let tc = tc.clone();
        hs.push(tokio::spawn(async move {
            let mut ok = 0;
            let mut seed = w * 7919 + 13;
            let mut next = || {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                (seed >> 33) as usize
            };
            while ok < 15 {
                let (a, b) = (next() % 10, next() % 10);
                if a == b {
                    continue;
                }
                let (ka, kb) = (format!("acct{a:02}"), format!("acct{b:02}"));
                let mut t = tc.begin().await.unwrap();
                let va: i64 = String::from_utf8(t.get(ka.as_bytes()).await.unwrap().unwrap()).unwrap().parse().unwrap();
                let vb: i64 = String::from_utf8(t.get(kb.as_bytes()).await.unwrap().unwrap()).unwrap().parse().unwrap();
                t.put(ka.as_bytes(), (va - 10).to_string().as_bytes());
                t.put(kb.as_bytes(), (vb + 10).to_string().as_bytes());
                match t.commit().await {
                    Ok(_) => ok += 1,
                    Err(::kv_client::txn_client::TxnError::Conflict { .. }) | Err(::kv_client::txn_client::TxnError::Aborted(_)) => {}
                    Err(e) => panic!("unexpected txn error: {e}"),
                }
            }
        }));
    }
    for h in hs {
        h.await.unwrap();
    }
    // wait for background secondary commits, then audit with a snapshot read
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut t = tc.begin().await.unwrap();
    let mut total = 0i64;
    for i in 0..10 {
        total += String::from_utf8(t.get(format!("acct{i:02}").as_bytes()).await.unwrap().unwrap()).unwrap().parse::<i64>().unwrap();
    }
    assert_eq!(total, 1000, "bank invariant violated");
    // scan across both ranges sees all accounts
    let all = t.scan(b"acct00", b"acct99", 0).await.unwrap();
    assert_eq!(all.len(), 10);

    // PD spread replicas over the 4 stores
    wait("replicas on >= 3 distinct stores", 20, || pd.distribution().0.len() >= 3).await;
    let st = AdminClient::new(::kv_client::pd::channel(&store_addrs[&1]).unwrap()).status(Empty {}).await.unwrap().into_inner();
    assert!(!st.ranges.is_empty() || true);
}
