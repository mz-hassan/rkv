//! Real-process cluster management for chaos tests: spawn, kill -9, SIGSTOP/SIGCONT,
//! Raft-traffic partitions/drops/delay and clock-rate skew (via the Admin.SetFaults RPC).
use ::kv_client::pd::channel;
use rkv_proto::admin_client::AdminClient;
use rkv_proto::{Empty, FaultsReq, RangeStatus};
use std::collections::BTreeMap;
use std::process::{Child, Command, Stdio};

pub fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

pub struct StoreProc {
    pub id: u64,
    pub addr: String,
    pub dir: String,
    pub child: Option<Child>,
    pub paused: bool,
}

pub struct PdProc {
    pub id: u64,
    pub addr: String,
    pub dir: String,
    pub child: Option<Child>,
}

pub struct ClusterOpts {
    pub stores: u64,
    pub pd_nodes: u64, // 0 = static membership, no PD
    pub extra_server_args: Vec<String>,
    pub election_tick: u64,
    pub tick_ms: u64,
}

impl Default for ClusterOpts {
    fn default() -> Self {
        ClusterOpts { stores: 3, pd_nodes: 0, extra_server_args: vec![], election_tick: 10, tick_ms: 20 }
    }
}

pub struct Cluster {
    pub root: String,
    pub bin: String,
    pub opts: ClusterOpts,
    pub stores: Vec<StoreProc>,
    pub pds: Vec<PdProc>,
}

fn kill_proc(child: &mut Option<Child>) {
    if let Some(mut c) = child.take() {
        let _ = c.kill(); // SIGKILL
        let _ = c.wait();
    }
}

impl Cluster {
    pub fn new(root: &str, opts: ClusterOpts) -> Cluster {
        let bin = std::env::var("RKV_BIN_DIR").unwrap_or_else(|_| "./target/release".into());
        let _ = std::fs::remove_dir_all(root);
        std::fs::create_dir_all(root).unwrap();
        let stores = (1..=opts.stores)
            .map(|id| StoreProc { id, addr: format!("127.0.0.1:{}", free_port()), dir: format!("{root}/s{id}"), child: None, paused: false })
            .collect();
        let pds = (1..=opts.pd_nodes).map(|id| PdProc { id, addr: format!("127.0.0.1:{}", free_port()), dir: format!("{root}/pd{id}"), child: None }).collect();
        Cluster { root: root.to_string(), bin, opts, stores, pds }
    }

    pub fn pd_addrs(&self) -> Vec<String> {
        self.pds.iter().map(|p| p.addr.clone()).collect()
    }
    pub fn store_addrs(&self) -> Vec<String> {
        self.stores.iter().map(|s| s.addr.clone()).collect()
    }

    fn log(&self, name: &str) -> Stdio {
        std::fs::OpenOptions::new().create(true).append(true).open(format!("{}/{name}.log", self.root)).map(Stdio::from).unwrap_or(Stdio::null())
    }

    pub fn start_pd(&mut self, idx: usize) {
        let peers = self.pds.iter().map(|p| format!("{}={}", p.id, p.addr)).collect::<Vec<_>>().join(",");
        let p = &self.pds[idx];
        let child = Command::new(format!("{}/rkv-pd", self.bin))
            .args(["--id", &p.id.to_string(), "--addr", &p.addr, "--data", &p.dir, "--peers", &peers])
            .stdout(self.log(&format!("pd{}", p.id)))
            .stderr(self.log(&format!("pd{}", p.id)))
            .spawn()
            .expect("spawn rkv-pd (build with cargo build --release)");
        self.pds[idx].child = Some(child);
    }

    pub fn start_store(&mut self, idx: usize) {
        let mut args: Vec<String> = vec![];
        let s = &self.stores[idx];
        args.extend(["--id".into(), s.id.to_string(), "--addr".into(), s.addr.clone(), "--data".into(), s.dir.clone()]);
        if self.opts.pd_nodes > 0 {
            args.extend(["--pd".into(), self.pd_addrs().join(",")]);
        } else {
            args.extend(["--peers".into(), self.stores.iter().map(|x| format!("{}={}", x.id, x.addr)).collect::<Vec<_>>().join(",")]);
        }
        args.extend(["--election-tick".into(), self.opts.election_tick.to_string(), "--tick-ms".into(), self.opts.tick_ms.to_string()]);
        args.extend(self.opts.extra_server_args.clone());
        let name = format!("s{}", s.id);
        let child =
            Command::new(format!("{}/rkv-server", self.bin)).args(&args).stdout(self.log(&name)).stderr(self.log(&name)).spawn().expect("spawn rkv-server");
        self.stores[idx].child = Some(child);
        self.stores[idx].paused = false;
    }

    pub async fn start_all(&mut self) {
        for i in 0..self.pds.len() {
            self.start_pd(i);
        }
        if !self.pds.is_empty() {
            tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
        }
        for i in 0..self.stores.len() {
            self.start_store(i);
        }
    }

    pub fn kill9_store(&mut self, idx: usize) {
        if self.stores[idx].paused {
            self.resume_store(idx);
        }
        kill_proc(&mut self.stores[idx].child);
    }
    pub fn kill9_pd(&mut self, idx: usize) {
        kill_proc(&mut self.pds[idx].child);
    }
    pub fn pause_store(&mut self, idx: usize) {
        if let Some(c) = &self.stores[idx].child {
            let _ = Command::new("kill").args(["-STOP", &c.id().to_string()]).status();
            self.stores[idx].paused = true;
        }
    }
    pub fn resume_store(&mut self, idx: usize) {
        if let Some(c) = &self.stores[idx].child {
            let _ = Command::new("kill").args(["-CONT", &c.id().to_string()]).status();
        }
        self.stores[idx].paused = false;
    }
    pub fn is_up(&self, idx: usize) -> bool {
        self.stores[idx].child.is_some()
    }

    pub async fn set_faults(&self, idx: usize, blocked_ids: &[u64], drop_pct: u32, delay_ms: u32, clock_pct: u32) {
        if !self.is_up(idx) || self.stores[idx].paused {
            return;
        }
        if let Ok(ch) = channel(&self.stores[idx].addr) {
            let _ = AdminClient::new(ch).set_faults(FaultsReq { blocked_stores: blocked_ids.to_vec(), drop_pct, delay_ms, clock_rate_pct: clock_pct }).await;
        }
    }

    /// Partition into groups (store ids): traffic between groups is cut.
    pub async fn partition(&self, groups: &[Vec<u64>]) {
        for (i, s) in self.stores.iter().enumerate() {
            let mine = groups.iter().find(|g| g.contains(&s.id));
            let blocked: Vec<u64> = self.stores.iter().map(|o| o.id).filter(|o| *o != s.id && mine.map(|g| !g.contains(o)).unwrap_or(true)).collect();
            self.set_faults(i, &blocked, 0, 0, 100).await;
        }
    }
    pub async fn heal(&self) {
        for i in 0..self.stores.len() {
            self.set_faults(i, &[], 0, 0, 100).await;
        }
    }

    /// (store index, ranges) for every reachable store.
    pub async fn statuses(&self) -> Vec<(usize, u64, Vec<RangeStatus>)> {
        let mut out = vec![];
        for (i, s) in self.stores.iter().enumerate() {
            if !self.is_up(i) || s.paused {
                continue;
            }
            if let Ok(ch) = channel(&s.addr) {
                if let Ok(Ok(r)) = tokio::time::timeout(std::time::Duration::from_secs(2), AdminClient::new(ch).status(Empty {})).await {
                    out.push((i, s.id, r.into_inner().ranges));
                }
            }
        }
        out
    }

    /// Store index currently leading range `range_id` (or any range if 0).
    pub async fn leader_of(&self, range_id: u64) -> Option<usize> {
        for (i, _, ranges) in self.statuses().await {
            for r in ranges {
                if r.is_leader && (range_id == 0 || r.info.as_ref().map(|x| x.id) == Some(range_id)) {
                    return Some(i);
                }
            }
        }
        None
    }

    pub async fn wait_ready(&self, secs: u64) -> bool {
        for _ in 0..secs * 5 {
            let st = self.statuses().await;
            let up = st.iter().filter(|(_, _, r)| !r.is_empty()).count();
            let leaders = st.iter().flat_map(|(_, _, r)| r.iter()).filter(|r| r.is_leader).count();
            let voters_ok = st
                .iter()
                .flat_map(|(_, _, r)| r.iter())
                .filter_map(|r| r.info.as_ref())
                .any(|i| i.peers.iter().filter(|p| !p.learner).count() >= self.opts.stores.min(3) as usize);
            if up >= self.opts.stores.min(3) as usize && leaders >= 1 && voters_ok {
                return true;
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
        false
    }

    pub fn shutdown(&mut self) {
        for i in 0..self.stores.len() {
            self.kill9_store(i);
        }
        for i in 0..self.pds.len() {
            self.kill9_pd(i);
        }
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[allow(dead_code)] // handy when debugging a run
pub fn summarize(st: &[(usize, u64, Vec<RangeStatus>)]) -> BTreeMap<u64, Vec<String>> {
    let mut m = BTreeMap::new();
    for (_, sid, rs) in st {
        for r in rs {
            m.entry(r.info.as_ref().map(|i| i.id).unwrap_or(0)).or_insert_with(Vec::new).push(format!(
                "s{sid}:{}@{}",
                if r.is_leader { "L" } else { "f" },
                r.applied
            ));
        }
    }
    m
}
