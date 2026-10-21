//! Range routing cache with leader discovery, redirect handling and retries.
use crate::pd::{channel, PdClient};
use crate::*;
use parking_lot::{Mutex, RwLock};
use rkv_proto::admin_client::AdminClient;
use rkv_proto::kv_client::KvClient;
use rkv_proto::*;
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Channel;

#[derive(Debug, Clone)]
pub enum ClientError {
    /// could not reach a serving replica; for writes the outcome is UNKNOWN
    Unavailable(String),
    /// the key set no longer belongs to one range (only from `exec_ff`): regroup and retry
    RangeMoved,
    Other(String),
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientError::Unavailable(s) => write!(f, "unavailable: {s}"),
            ClientError::RangeMoved => write!(f, "range moved"),
            ClientError::Other(s) => write!(f, "{s}"),
        }
    }
}
impl std::error::Error for ClientError {}

pub trait HasError {
    fn error(&self) -> Option<&Error>;
}
macro_rules! has_error {
    ($($t:ty),*) => { $(impl HasError for $t { fn error(&self) -> Option<&Error> { self.error.as_ref() } })* };
}
has_error!(GetResp, WriteResp, CasResp, ScanResp, TxnGetResp, TxnScanResp, PrewriteResp, CommitResp, CheckTxnStatusResp);

pub struct Router {
    pd: Option<Arc<PdClient>>,
    seeds: Vec<String>,
    ranges: RwLock<BTreeMap<Vec<u8>, RangeInfo>>,
    leaders: RwLock<HashMap<u64, u64>>, // range -> leader store
    stores: RwLock<HashMap<u64, String>>,
    chans: Mutex<HashMap<String, Channel>>,
    pub retries: std::sync::atomic::AtomicU64,
}

fn covers(r: &RangeInfo, key: &[u8]) -> bool {
    key >= r.start.as_slice() && (r.end.is_empty() || key < r.end.as_slice())
}

impl Router {
    pub fn new(pd: Option<Arc<PdClient>>, seeds: Vec<String>) -> Arc<Router> {
        Arc::new(Router { pd, seeds, ranges: RwLock::new(BTreeMap::new()), leaders: RwLock::new(HashMap::new()), stores: RwLock::new(HashMap::new()), chans: Mutex::new(HashMap::new()), retries: Default::default() })
    }

    pub fn chan(&self, addr: &str) -> Channel {
        let mut c = self.chans.lock();
        if let Some(ch) = c.get(addr) {
            return ch.clone();
        }
        let ch = channel(addr).expect("valid address");
        c.insert(addr.to_string(), ch.clone());
        ch
    }

    pub fn store_addr(&self, id: u64) -> Option<String> {
        self.stores.read().get(&id).cloned()
    }

    fn learn(&self, r: &RangeInfo) {
        for p in &r.peers {
            if !p.addr.is_empty() {
                self.stores.write().insert(p.store_id, p.addr.clone());
            }
        }
        if r.leader_store != 0 {
            self.leaders.write().insert(r.id, r.leader_store);
        }
        let mut m = self.ranges.write();
        // evict overlapping stale entries
        let stale: Vec<Vec<u8>> = m.iter().filter(|(_, o)| o.id != r.id && o.version <= r.version && overlaps(o, r)).map(|(k, _)| k.clone()).collect();
        for k in stale {
            m.remove(&k);
        }
        // do not replace newer knowledge of the same range with older
        if let Some(old) = m.get(&r.start) {
            if old.id == r.id && old.version > r.version {
                return;
            }
        }
        m.retain(|_, o| o.id != r.id);
        m.insert(r.start.clone(), r.clone());
    }

    pub fn invalidate(&self, range_id: u64) {
        self.ranges.write().retain(|_, r| r.id != range_id);
        self.leaders.write().remove(&range_id);
    }

    fn cached(&self, key: &[u8]) -> Option<RangeInfo> {
        let m = self.ranges.read();
        m.range(..=key.to_vec()).next_back().filter(|(_, r)| covers(r, key)).map(|(_, r)| r.clone())
    }

    pub async fn locate(&self, key: &[u8]) -> Result<RangeInfo, ClientError> {
        if let Some(r) = self.cached(key) {
            return Ok(r);
        }
        self.fetch(key).await?;
        self.cached(key).ok_or_else(|| ClientError::Unavailable(format!("no range covers key {:?}", String::from_utf8_lossy(key))))
    }

    async fn fetch(&self, key: &[u8]) -> Result<(), ClientError> {
        if let Some(pd) = &self.pd {
            let r = pd.get_route(RouteReq { key: key.to_vec(), by_id: false, range_id: 0 }).await.map_err(ClientError::Unavailable)?;
            if let Some(info) = r.range {
                self.learn(&info);
                return Ok(());
            }
        }
        // no PD (or PD has no answer): ask the stores directly
        let mut addrs: Vec<String> = self.seeds.clone();
        addrs.extend(self.stores.read().values().cloned());
        let mut last = "no seed stores".to_string();
        for a in addrs {
            let mut c = AdminClient::new(self.chan(&a));
            match c.status(Empty {}).await {
                Ok(r) => {
                    for rs in r.into_inner().ranges {
                        if let Some(mut info) = rs.info {
                            if rs.is_leader {
                                info.leader_store = r_store(&info, &a);
                            }
                            self.learn(&info);
                        }
                    }
                    return Ok(());
                }
                Err(e) => last = e.to_string(),
            }
        }
        Err(ClientError::Unavailable(last))
    }

    pub async fn refresh_all(&self) -> Result<(), ClientError> {
        if let Some(pd) = &self.pd {
            let r = pd.all_ranges().await.map_err(ClientError::Unavailable)?;
            self.ranges.write().clear();
            for s in &r.stores {
                self.stores.write().insert(s.id, s.addr.clone());
            }
            for info in r.ranges {
                self.learn(&info);
            }
            return Ok(());
        }
        self.ranges.write().clear();
        self.fetch(b"").await
    }

    pub fn all_cached(&self) -> Vec<RangeInfo> {
        self.ranges.read().values().cloned().collect()
    }

    /// The range after `r` in key order (for scans), if any.
    pub async fn next_range(&self, r: &RangeInfo) -> Result<Option<RangeInfo>, ClientError> {
        if r.end.is_empty() {
            return Ok(None);
        }
        Ok(Some(self.locate(&r.end).await?))
    }

    /// Execute a keyed request with redirect/retry handling.
    /// `f` receives a client for the chosen replica and a `Context` for the located range.
    pub async fn exec<R, F, Fut>(&self, key: &[u8], read_mode: u32, max_stale_ms: u64, f: F) -> Result<R, ClientError>
    where
        F: Fn(KvClient<Channel>, Context) -> Fut,
        Fut: Future<Output = Result<tonic::Response<R>, tonic::Status>>,
        R: HasError,
    {
        self.exec_opts(key, read_mode, max_stale_ms, false, f).await
    }

    /// Like `exec`, but returns `ClientError::RangeMoved` right after refreshing the route
    /// when the server says the request no longer fits one range (multi-key requests).
    pub async fn exec_ff<R, F, Fut>(&self, key: &[u8], f: F) -> Result<R, ClientError>
    where
        F: Fn(KvClient<Channel>, Context) -> Fut,
        Fut: Future<Output = Result<tonic::Response<R>, tonic::Status>>,
        R: HasError,
    {
        self.exec_opts(key, 0, 0, true, f).await
    }

    async fn exec_opts<R, F, Fut>(&self, key: &[u8], read_mode: u32, max_stale_ms: u64, fail_fast: bool, f: F) -> Result<R, ClientError>
    where
        F: Fn(KvClient<Channel>, Context) -> Fut,
        Fut: Future<Output = Result<tonic::Response<R>, tonic::Status>>,
        R: HasError,
    {
        let mut last = String::new();
        for attempt in 0..24u32 {
            if attempt > 0 {
                self.retries.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis((15 * attempt as u64).min(250))).await;
            }
            let info = match self.locate(key).await {
                Ok(i) => i,
                Err(e) => {
                    last = e.to_string();
                    continue;
                }
            };
            // pick a replica: cached leader first (unless a stale read allows any), else rotate through peers
            let leader = self.leaders.read().get(&info.id).copied();
            let target = if read_mode == 2 {
                info.peers.get(rand::random::<usize>() % info.peers.len().max(1)).map(|p| p.store_id)
            } else {
                match leader {
                    Some(l) if attempt < 3 => Some(l),
                    _ => info.peers.iter().filter(|p| !p.learner).nth(attempt as usize % info.peers.iter().filter(|p| !p.learner).count().max(1)).map(|p| p.store_id),
                }
            };
            let Some(store) = target else {
                self.invalidate(info.id);
                continue;
            };
            let addr = info.peers.iter().find(|p| p.store_id == store).map(|p| p.addr.clone()).or_else(|| self.store_addr(store));
            let Some(addr) = addr else {
                self.invalidate(info.id);
                continue;
            };
            let ctx = Context { range_id: info.id, version: info.version, read_mode, max_staleness_ms: max_stale_ms };
            match f(KvClient::new(self.chan(&addr)).max_decoding_message_size(256 << 20), ctx).await {
                Err(s) => {
                    last = format!("rpc to store {store} ({addr}): {}", s.message());
                    self.leaders.write().remove(&info.id);
                    continue;
                }
                Ok(resp) => {
                    let r = resp.into_inner();
                    match r.error() {
                        None => {
                            self.leaders.write().insert(info.id, store);
                            return Ok(r);
                        }
                        Some(e) => {
                            last = format!("code {} {}", e.code, e.msg);
                            match e.code {
                                ERR_NOT_LEADER => {
                                    if let Some(l) = e.leader.as_ref().filter(|l| l.store_id != 0) {
                                        self.leaders.write().insert(info.id, l.store_id);
                                        if !l.addr.is_empty() {
                                            self.stores.write().insert(l.store_id, l.addr.clone());
                                        }
                                    } else {
                                        self.leaders.write().remove(&info.id);
                                    }
                                }
                                ERR_RANGE_MOVED | ERR_NO_RANGE => {
                                    self.invalidate(info.id);
                                    for r in &e.ranges {
                                        self.learn(r);
                                    }
                                    if fail_fast && e.code == ERR_RANGE_MOVED {
                                        return Err(ClientError::RangeMoved);
                                    }
                                }
                                ERR_BUSY | ERR_UNKNOWN_OUTCOME => {}
                                _ => return Err(ClientError::Other(format!("server error {}: {}", e.code, e.msg))),
                            }
                        }
                    }
                }
            }
        }
        Err(ClientError::Unavailable(last))
    }
}

fn overlaps(a: &RangeInfo, b: &RangeInfo) -> bool {
    let a_before_b = !a.end.is_empty() && a.end <= b.start;
    let b_before_a = !b.end.is_empty() && b.end <= a.start;
    !(a_before_b || b_before_a)
}

fn r_store(info: &RangeInfo, addr: &str) -> u64 {
    info.peers.iter().find(|p| p.addr == addr).map(|p| p.store_id).unwrap_or(0)
}
