//! End-to-end: three stores over real gRPC on localhost, static membership (no PD).
use kv_client::RawClient;
use kv_server::{serve, Store, StoreConfig};
use raft_driver::StateMachine;
use rkv_proto::admin_client::AdminClient;
use rkv_proto::{Empty, SplitReq};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

/// Each node runs on its own runtime so that "crashing" it (dropping the runtime) also
/// kills its open connections, like a real process death.
struct Node {
    store: Arc<Store>,
    rt: Option<tokio::runtime::Runtime>,
}

impl Drop for Node {
    fn drop(&mut self) {
        self.crash();
    }
}

impl Node {
    fn crash(&mut self) {
        if let Some(rt) = self.rt.take() {
            rt.shutdown_background();
        }
    }
}

async fn start_cluster(dirs: &[tempfile::TempDir], addrs: &BTreeMap<u64, String>) -> Vec<Node> {
    let mut nodes = vec![];
    for (i, (id, addr)) in addrs.iter().enumerate() {
        nodes.push(start_node(*id, addr, dirs[i].path().to_str().unwrap(), addrs).await);
    }
    nodes
}

async fn start_node(id: u64, addr: &str, dir: &str, peers: &BTreeMap<u64, String>) -> Node {
    let mut cfg = StoreConfig::new(id, addr, dir);
    cfg.static_peers = peers.clone();
    cfg.group.tick_ms = 10;
    cfg.group.snapshot_threshold = 200;
    cfg.engine.memtable_size = 1 << 20;
    cfg.split_threshold_bytes = 0;
    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(3).enable_all().build().unwrap();
    let store = rt.spawn(async move { Store::open(cfg).await.unwrap() }).await.unwrap();
    let s2 = store.clone();
    let a: std::net::SocketAddr = addr.parse().unwrap();
    rt.spawn(async move {
        if let Err(e) = serve(s2, a).await {
            eprintln!("serve error on {a}: {e}");
        }
    });
    Node { store, rt: Some(rt) }
}

fn addr_map() -> BTreeMap<u64, String> {
    (1..=3u64).map(|i| (i, format!("127.0.0.1:{}", free_port()))).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn raw_kv_end_to_end_with_failover_and_split() {
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let addrs = addr_map();
    let mut nodes = start_cluster(&dirs, &addrs).await;
    let seeds: Vec<String> = addrs.values().cloned().collect();
    let c = RawClient::new(None, seeds.clone());

    // basic operations
    c.put(b"hello", b"world").await.unwrap();
    assert_eq!(c.get(b"hello").await.unwrap().unwrap(), b"world");
    assert!(c.get(b"missing").await.unwrap().is_none());
    assert!(c.cas(b"hello", Some(b"world"), b"v2").await.unwrap());
    assert!(!c.cas(b"hello", Some(b"world"), b"v3").await.unwrap());
    assert_eq!(c.get(b"hello").await.unwrap().unwrap(), b"v2");
    c.delete(b"hello").await.unwrap();
    assert!(c.get(b"hello").await.unwrap().is_none());
    for i in 0..300u32 {
        c.put(format!("key{i:04}").as_bytes(), format!("value{i}").as_bytes()).await.unwrap();
    }
    let all = c.scan(b"key", b"kez", 0).await.unwrap();
    assert_eq!(all.len(), 300);
    assert_eq!(c.scan(b"key0010", b"key0020", 0).await.unwrap().len(), 10);

    // follower (stale) read must see replicated data eventually
    let mut seen = false;
    for _ in 0..50 {
        if let Ok(Some(v)) = c.get_with(b"key0007", kv_client::raw::ReadConsistency::Stale { max_staleness_ms: 5000 }).await {
            assert_eq!(v, b"value7");
            seen = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(seen);

    // split the range, then write on both sides
    let mut admin = AdminClient::new(kv_client::pd::channel(&addrs[&1]).unwrap());
    let st = admin.status(Empty {}).await.unwrap().into_inner();
    assert_eq!(st.ranges.len(), 1);
    let mut done = false;
    for n in &addrs {
        let mut a = AdminClient::new(kv_client::pd::channel(n.1).unwrap());
        let r = a.split_range(SplitReq { range_id: 1, split_key: b"key0150".to_vec() }).await.unwrap().into_inner();
        if r.error.is_none() {
            done = true;
            break;
        }
    }
    assert!(done, "split should succeed on the leader");
    tokio::time::sleep(Duration::from_millis(1500)).await;
    for n in &nodes {
        assert_eq!(n.store.all_replicas().len(), 2, "every store must host both ranges after the split");
    }
    c.router.refresh_all().await.unwrap();
    c.put(b"key0001", b"left").await.unwrap();
    c.put(b"key0299", b"right").await.unwrap();
    assert_eq!(c.get(b"key0001").await.unwrap().unwrap(), b"left");
    assert_eq!(c.get(b"key0299").await.unwrap().unwrap(), b"right");
    assert_eq!(c.scan(b"key", b"kez", 0).await.unwrap().len(), 300, "scan must span both ranges");

    // kill one node (whichever), cluster keeps serving
    let mut victim = nodes.remove(0);
    victim.crash();
    for i in 0..30u32 {
        c.put(format!("after{i}").as_bytes(), b"x").await.unwrap();
    }
    assert_eq!(c.get(b"after29").await.unwrap().unwrap(), b"x");
    assert_eq!(c.get(b"key0200").await.unwrap().unwrap(), b"value200");

    // restart it from disk: it must rejoin and catch up
    drop(victim);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let id1 = 1u64;
    let n1 = start_node(id1, &addrs[&1], dirs[0].path().to_str().unwrap(), &addrs).await;
    let mut caught_up = false;
    for _ in 0..100 {
        let reps = n1.store.all_replicas();
        if reps.len() == 2
            && reps.iter().all(|r| r.sm.applied_index() > 0)
            && n1.store.find(b"after29").map(|r| r.sm.read_raw(b"after29", 0).is_some()).unwrap_or(false)
        {
            caught_up = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    if !caught_up {
        for r in n1.store.all_replicas() {
            eprintln!("replica {} desc={:?} applied={} status={:?}", r.id, r.sm.desc(), r.sm.applied_index(), r.handle.status().await);
        }
        eprintln!("find(after29)={:?}", n1.store.find(b"after29").map(|r| r.id));
    }
    assert!(caught_up, "restarted node did not catch up");
    drop(n1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn exactly_once_writes_across_retries() {
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let addrs = addr_map();
    let _nodes = start_cluster(&dirs, &addrs).await;
    let c = RawClient::new(None, addrs.values().cloned().collect());
    // many CAS increments from several clients must not lose or double-apply
    c.put(b"ctr", b"0").await.unwrap();
    let mut hs = vec![];
    for _ in 0..4 {
        let seeds: Vec<String> = addrs.values().cloned().collect();
        hs.push(tokio::spawn(async move {
            let cl = RawClient::new(None, seeds);
            let mut done = 0;
            while done < 20 {
                let cur = cl.get(b"ctr").await.unwrap().unwrap();
                let n: u64 = String::from_utf8(cur.clone()).unwrap().parse().unwrap();
                if cl.cas(b"ctr", Some(&cur), (n + 1).to_string().as_bytes()).await.unwrap() {
                    done += 1;
                }
            }
        }));
    }
    for h in hs {
        h.await.unwrap();
    }
    assert_eq!(c.get(b"ctr").await.unwrap().unwrap(), b"80");
}
