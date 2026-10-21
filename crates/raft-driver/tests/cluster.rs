use kv_engine::MemFs;
use raft::{Membership, Role};
use raft_driver::mem::{MemTransport, Net};
use raft_driver::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Durable-style counter state machine: sum of all u64 payloads.
struct SumSm {
    state: Mutex<(u64, u64)>, // (applied, sum)
}
impl StateMachine for SumSm {
    fn apply_batch(&self, es: &[(u64, u64, Option<Vec<u8>>)]) -> Vec<Vec<u8>> {
        let mut s = self.state.lock().unwrap();
        es.iter()
            .map(|(i, _, d)| {
                if let Some(d) = d {
                    s.1 += u64::from_le_bytes(d[..8].try_into().unwrap());
                }
                s.0 = *i;
                s.1.to_le_bytes().to_vec()
            })
            .collect()
    }
    fn applied_index(&self) -> u64 {
        self.state.lock().unwrap().0
    }
    fn snapshot(&self) -> Vec<u8> {
        self.state.lock().unwrap().1.to_le_bytes().to_vec()
    }
    fn restore(&self, index: u64, data: &[u8]) {
        *self.state.lock().unwrap() = (index, u64::from_le_bytes(data[..8].try_into().unwrap()));
    }
}

struct Cluster {
    net: Arc<Net>,
    sms: Vec<Arc<SumSm>>,
    fs: Vec<Arc<MemFs>>,
    handles: Vec<GroupHandle>,
}

fn opts() -> GroupOptions {
    GroupOptions { tick_ms: 5, election_tick: 10, snapshot_threshold: 50, ..GroupOptions::default() }
}

fn start(n: u64) -> Cluster {
    let net = Arc::new(Net::default());
    let mut c = Cluster { net: net.clone(), sms: vec![], fs: vec![], handles: vec![] };
    for id in 1..=n {
        let fs = MemFs::new();
        let sm = Arc::new(SumSm { state: Mutex::new((0, 0)) });
        let h = spawn_group(1, id, Membership::new(1..=n), fs.as_vfs(), "/g1", sm.clone(), Arc::new(MemTransport { net: net.clone(), from: id }), opts()).unwrap();
        net.handles.lock().unwrap().insert((id, 1), h.clone());
        c.sms.push(sm);
        c.fs.push(fs);
        c.handles.push(h);
    }
    c
}

async fn find_leader(c: &Cluster) -> usize {
    for _ in 0..400 {
        for (i, h) in c.handles.iter().enumerate() {
            if let Some(s) = h.status().await {
                if s.role == Role::Leader {
                    return i;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("no leader elected");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn elects_replicates_reads_and_survives_leader_loss() {
    let c = start(3);
    let l = find_leader(&c).await;
    for i in 1..=100u64 {
        c.handles[l].propose(i.to_le_bytes().to_vec()).await.unwrap();
    }
    let idx = c.handles[l].read_barrier().await.unwrap();
    assert!(idx > 0);
    assert_eq!(c.sms[l].state.lock().unwrap().1, 5050);
    // follower rejects proposals with a leader hint
    let f = (l + 1) % 3;
    assert!(matches!(c.handles[f].propose(vec![0; 8]).await, Err(DriverError::NotLeader(_))));
    // stop the leader and make sure a new one is elected and continues
    c.net.handles.lock().unwrap().remove(&(l as u64 + 1, 1));
    c.handles[l].stop().await;
    let mut ok = false;
    for _ in 0..300 {
        for (i, h) in c.handles.iter().enumerate() {
            if i == l {
                continue;
            }
            if h.propose(1000u64.to_le_bytes().to_vec()).await.is_ok() {
                ok = true;
                break;
            }
        }
        if ok {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(ok, "cluster did not recover after leader stop");
    // snapshot threshold of 50 entries has been crossed: log was compacted
    let s = c.handles[(l + 1) % 3].status().await.unwrap();
    assert!(s.snapshot_index > 0, "expected log compaction, got {s:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_recovers_from_disk() {
    let c = start(3);
    let l = find_leader(&c).await;
    for i in 1..=30u64 {
        c.handles[l].propose(i.to_le_bytes().to_vec()).await.unwrap();
    }
    // restart a follower: new group over the same disk with a fresh (volatile) state machine
    let f = (l + 1) % 3;
    c.handles[f].stop().await;
    c.fs[f].crash(9);
    let sm = Arc::new(SumSm { state: Mutex::new((0, 0)) });
    let h = spawn_group(1, f as u64 + 1, Membership::new(1..=3), c.fs[f].as_vfs(), "/g1", sm.clone(), Arc::new(MemTransport { net: c.net.clone(), from: f as u64 + 1 }), opts()).unwrap();
    c.net.handles.lock().unwrap().insert((f as u64 + 1, 1), h);
    c.handles[l].propose(1u64.to_le_bytes().to_vec()).await.unwrap();
    for _ in 0..300 {
        if sm.state.lock().unwrap().1 == 466 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("restarted node did not catch up: {:?}", sm.state.lock().unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn membership_change_adds_a_node() {
    // 3 voters + a 4th node that starts knowing only the original membership
    let net = Arc::new(Net::default());
    let mut hs = vec![];
    let mut sms = vec![];
    for id in 1..=4u64 {
        let fs = MemFs::new();
        let sm = Arc::new(SumSm { state: Mutex::new((0, 0)) });
        let h = spawn_group(1, id, Membership::new(1..=3), fs.as_vfs(), "/g1", sm.clone(), Arc::new(MemTransport { net: net.clone(), from: id }), opts()).unwrap();
        net.handles.lock().unwrap().insert((id, 1), h.clone());
        hs.push(h);
        sms.push(sm);
    }
    let c = Cluster { net, sms, fs: vec![], handles: hs };
    let l = find_leader(&c).await;
    for i in 1..=60u64 {
        c.handles[l].propose(i.to_le_bytes().to_vec()).await.unwrap();
    }
    let _ = AtomicU64::new(0).load(Ordering::Relaxed);
    // add node 4 as a learner, wait for catch-up (via snapshot since the log was compacted), then promote
    c.handles[l].change_membership([1, 2, 3].into_iter().collect(), [4].into_iter().collect()).await.unwrap();
    for _ in 0..500 {
        if c.sms[3].state.lock().unwrap().1 == 1830 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(c.sms[3].state.lock().unwrap().1, 1830, "learner did not catch up");
    let mut promoted = false;
    for _ in 0..200 {
        if c.handles[l].change_membership([1, 2, 3, 4].into_iter().collect(), Default::default()).await.is_ok() {
            promoted = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(promoted);
    for _ in 0..300 {
        let s = c.handles[l].status().await.unwrap();
        if s.membership.voters.len() == 4 && !s.membership.is_joint() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("membership change never completed");
}
