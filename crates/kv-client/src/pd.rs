//! Placement-driver client with automatic leader failover.
use crate::{ERR_NOT_LEADER};
use parking_lot::Mutex;
use rkv_proto::pd_client::PdClient as Grpc;
use rkv_proto::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tonic::transport::{Channel, Endpoint};

pub struct PdClient {
    addrs: Mutex<Vec<String>>,
    cur: AtomicUsize,
    chans: Mutex<HashMap<String, Channel>>,
    tso: Mutex<(u64, u32)>, // next, remaining
}

pub fn channel(addr: &str) -> Result<Channel, String> {
    let uri = if addr.starts_with("http") { addr.to_string() } else { format!("http://{addr}") };
    Ok(Endpoint::from_shared(uri).map_err(|e| e.to_string())?.connect_timeout(Duration::from_secs(1)).timeout(Duration::from_secs(5)).tcp_nodelay(true).connect_lazy())
}

macro_rules! pd_call {
    ($self:ident, $method:ident, $req:expr) => {{
        let mut last = String::from("no PD address");
        let mut out = None;
        let n_addrs = $self.addrs.lock().len();
        for _attempt in 0..(n_addrs * 3 + 3) {
            let addr = $self.pick();
            let ch = match $self.chan(&addr) {
                Ok(c) => c,
                Err(e) => {
                    last = e;
                    $self.next();
                    continue;
                }
            };
            let mut c = Grpc::new(ch);
            match c.$method($req.clone()).await {
                Ok(r) => {
                    let r = r.into_inner();
                    if let Some(e) = &r.error {
                        if e.code == ERR_NOT_LEADER {
                            match e.leader.as_ref().filter(|l| !l.addr.is_empty()) {
                                Some(l) => $self.set_leader(&l.addr),
                                None => $self.next(),
                            }
                            last = format!("not leader: {}", e.msg);
                            tokio::time::sleep(Duration::from_millis(50)).await;
                            continue;
                        }
                    }
                    out = Some(r);
                    break;
                }
                Err(s) => {
                    last = s.to_string();
                    $self.next();
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
        out.ok_or(last)
    }};
}

impl PdClient {
    pub fn new(addrs: Vec<String>) -> PdClient {
        PdClient { addrs: Mutex::new(addrs), cur: AtomicUsize::new(0), chans: Mutex::new(HashMap::new()), tso: Mutex::new((0, 0)) }
    }
    fn pick(&self) -> String {
        let a = self.addrs.lock();
        a[self.cur.load(Ordering::Relaxed) % a.len().max(1)].clone()
    }
    fn next(&self) {
        self.cur.fetch_add(1, Ordering::Relaxed);
    }
    fn set_leader(&self, addr: &str) {
        let mut a = self.addrs.lock();
        match a.iter().position(|x| x == addr) {
            Some(i) => self.cur.store(i, Ordering::Relaxed),
            None => {
                a.push(addr.to_string());
                self.cur.store(a.len() - 1, Ordering::Relaxed)
            }
        }
    }
    fn chan(&self, addr: &str) -> Result<Channel, String> {
        let mut c = self.chans.lock();
        if let Some(ch) = c.get(addr) {
            return Ok(ch.clone());
        }
        let ch = channel(addr)?;
        c.insert(addr.to_string(), ch.clone());
        Ok(ch)
    }

    pub async fn heartbeat(&self, req: HeartbeatReq) -> Result<HeartbeatResp, String> {
        pd_call!(self, heartbeat, req)
    }
    pub async fn get_route(&self, req: RouteReq) -> Result<RouteResp, String> {
        pd_call!(self, get_route, req)
    }
    pub async fn all_ranges(&self) -> Result<AllRangesResp, String> {
        let req = Empty {};
        pd_call!(self, get_all_ranges, req)
    }
    pub async fn alloc_id(&self, count: u32) -> Result<u64, String> {
        let req = AllocIdReq { count };
        let r = pd_call!(self, alloc_id, req)?;
        Ok(r.first)
    }
    pub async fn bootstrap(&self, store: StoreInfo) -> Result<RouteResp, String> {
        let req = BootstrapReq { store: Some(store) };
        pd_call!(self, bootstrap, req)
    }
    pub async fn set_config(&self, key: &str, value: &str) -> Result<AdminResp, String> {
        let req = SetConfigReq { key: key.into(), value: value.into() };
        pd_call!(self, set_config, req)
    }
    /// One timestamp. Timestamps are fetched from PD in batches of 256 and handed out locally.
    pub async fn tso(&self) -> Result<u64, String> {
        {
            let mut t = self.tso.lock();
            if t.1 > 0 {
                let ts = t.0;
                t.0 += 1;
                t.1 -= 1;
                return Ok(ts);
            }
        }
        // NOTE: a batch must never be reused across a long idle period by writers that
        // need fresh timestamps; commit timestamps use `tso_fresh`.
        let req = TsoReq { count: 256 };
        let r = pd_call!(self, tso, req)?;
        let mut t = self.tso.lock();
        *t = (r.first + 1, r.count.saturating_sub(1));
        Ok(r.first)
    }
    /// A timestamp guaranteed to be allocated AFTER this call started (no local batch).
    pub async fn tso_fresh(&self) -> Result<u64, String> {
        let req = TsoReq { count: 1 };
        let r = pd_call!(self, tso, req)?;
        Ok(r.first)
    }
}
