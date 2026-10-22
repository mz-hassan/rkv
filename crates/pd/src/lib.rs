//! Placement driver: replicated cluster metadata (bootstrap, ids, TSO window, config) over
//! its own Raft group, plus leader-local routing table and scheduler.
use ::kv_client::now_ms;
use kv_engine::{OsFs, Vfs};
use ::kv_server::transport::{AddrBook, GrpcTransport};
use parking_lot::{Mutex, RwLock};
use raft::codec::{R, W};
use raft::{Membership, Role};
use raft_driver::{spawn_group, DriverError, GroupHandle, GroupOptions, StateMachine};
use rkv_proto::*;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex as AsyncMutex;

pub mod service;

const LOGICAL_BITS: u32 = 18;
const TSO_WINDOW_MS: u64 = 3000;
const STORE_DEAD_MS: u64 = 15_000;

// ------------------------------------------------------------------ replicated state

#[derive(Clone, Default, Debug)]
struct Persist {
    bootstrapped: bool,
    next_id: u64,
    tso_upper: u64,
    config: BTreeMap<String, String>,
    stores: BTreeMap<u64, String>,
    first_range_store: u64,
}

pub struct PdSm {
    st: Mutex<Persist>,
    applied: AtomicU64,
}

enum PdCmd {
    Bootstrap { store_id: u64, addr: String },
    AllocIds(u64),
    TsoUpper(u64),
    SetConfig(String, String),
}

impl PdCmd {
    fn encode(&self) -> Vec<u8> {
        let mut w = W::default();
        match self {
            PdCmd::Bootstrap { store_id, addr } => {
                w.u8(1);
                w.u64(*store_id);
                w.bytes(addr.as_bytes());
            }
            PdCmd::AllocIds(n) => {
                w.u8(2);
                w.u64(*n);
            }
            PdCmd::TsoUpper(t) => {
                w.u8(3);
                w.u64(*t);
            }
            PdCmd::SetConfig(k, v) => {
                w.u8(4);
                w.bytes(k.as_bytes());
                w.bytes(v.as_bytes());
            }
        }
        w.0
    }
    fn decode(b: &[u8]) -> Option<PdCmd> {
        let mut r = R(b);
        Some(match r.u8()? {
            1 => PdCmd::Bootstrap { store_id: r.u64()?, addr: String::from_utf8(r.bytes()?).ok()? },
            2 => PdCmd::AllocIds(r.u64()?),
            3 => PdCmd::TsoUpper(r.u64()?),
            4 => PdCmd::SetConfig(String::from_utf8(r.bytes()?).ok()?, String::from_utf8(r.bytes()?).ok()?),
            _ => return None,
        })
    }
}

impl PdSm {
    fn new() -> Arc<PdSm> {
        Arc::new(PdSm { st: Mutex::new(Persist { next_id: 1000, ..Persist::default() }), applied: AtomicU64::new(0) })
    }
}

impl StateMachine for PdSm {
    fn apply_batch(&self, entries: &[(u64, u64, Option<Vec<u8>>)]) -> Vec<Vec<u8>> {
        let mut st = self.st.lock();
        let mut out = vec![];
        for (idx, _, data) in entries {
            let mut res = vec![];
            if let Some(d) = data {
                match PdCmd::decode(d) {
                    Some(PdCmd::Bootstrap { store_id, addr }) => {
                        if st.bootstrapped {
                            res = vec![0];
                        } else {
                            st.bootstrapped = true;
                            st.first_range_store = store_id;
                            st.stores.insert(store_id, addr);
                            res = vec![1];
                        }
                    }
                    Some(PdCmd::AllocIds(n)) => {
                        let first = st.next_id;
                        st.next_id += n;
                        res = first.to_le_bytes().to_vec();
                    }
                    Some(PdCmd::TsoUpper(t)) => {
                        if t > st.tso_upper {
                            st.tso_upper = t;
                        }
                        res = st.tso_upper.to_le_bytes().to_vec();
                    }
                    Some(PdCmd::SetConfig(k, v)) => {
                        st.config.insert(k, v);
                    }
                    None => {}
                }
            }
            self.applied.store(*idx, Ordering::SeqCst);
            out.push(res);
        }
        out
    }
    // volatile state machine: Raft replays the log (and snapshot) after a restart
    fn applied_index(&self) -> u64 {
        self.applied.load(Ordering::SeqCst)
    }
    fn snapshot(&self) -> Vec<u8> {
        let st = self.st.lock();
        let mut w = W::default();
        w.bool(st.bootstrapped);
        w.u64(st.next_id);
        w.u64(st.tso_upper);
        w.u64(st.first_range_store);
        w.u64(st.config.len() as u64);
        for (k, v) in &st.config {
            w.bytes(k.as_bytes());
            w.bytes(v.as_bytes());
        }
        w.u64(st.stores.len() as u64);
        for (k, v) in &st.stores {
            w.u64(*k);
            w.bytes(v.as_bytes());
        }
        w.0
    }
    fn restore(&self, index: u64, data: &[u8]) {
        let mut r = R(data);
        let p = (|| -> Option<Persist> {
            let mut p = Persist { bootstrapped: r.bool()?, next_id: r.u64()?, tso_upper: r.u64()?, first_range_store: r.u64()?, ..Persist::default() };
            for _ in 0..r.u64()? {
                p.config.insert(String::from_utf8(r.bytes()?).ok()?, String::from_utf8(r.bytes()?).ok()?);
            }
            for _ in 0..r.u64()? {
                p.stores.insert(r.u64()?, String::from_utf8(r.bytes()?).ok()?);
            }
            Some(p)
        })();
        *self.st.lock() = p.expect("corrupt PD snapshot");
        self.applied.store(index, Ordering::SeqCst);
    }
}

// ------------------------------------------------------------------ leader-local state

#[derive(Clone)]
pub struct RangeEntry {
    pub info: RangeInfo,
    pub term: u64,
    pub bytes: u64,
    pub write_qps: u64,
    pub read_qps: u64,
    pub commit: u64,
    pub updated_ms: u64,
}

#[derive(Clone)]
pub struct StoreEntry {
    pub info: StoreInfo,
    pub last_seen_ms: u64,
}

#[derive(Default)]
struct Mem {
    term: u64,
    stores: HashMap<u64, StoreEntry>,
    ranges: BTreeMap<Vec<u8>, RangeEntry>,
    /// (range, store) -> (applied, commit, at_ms) for non-leader replicas
    reports: HashMap<(u64, u64), (u64, u64, u64)>,
    queue: HashMap<u64, Vec<PdCommand>>,
    pending: HashMap<u64, (u32, u64, Vec<(u64, bool)>, u64)>, // range -> (kind, since_ms, peers signature, version)
    gc_sent: HashMap<(u64, u64), u64>,
    /// (range, store) -> first time a replica was seen that the leader does not list as a peer
    unlisted_since: HashMap<(u64, u64), u64>,
}

struct TsoState {
    phys: u64,
    logical: u64,
    window_upper: u64,
    term: u64,
}

pub struct PdConfig {
    pub id: u64,
    pub addr: String,
    pub data_dir: String,
    pub peers: BTreeMap<u64, String>,
    pub group: GroupOptions,
}

pub struct Pd {
    pub id: u64,
    pub addr: String,
    pub handle: GroupHandle,
    sm: Arc<PdSm>,
    pub addrs: AddrBook,
    mem: Mutex<Mem>,
    tso: AsyncMutex<TsoState>,
    id_block: AsyncMutex<(u64, u64, u64)>, // next, limit, term
    pub transport: Arc<GrpcTransport>,
}

fn perr(code: u32, msg: &str) -> Error {
    Error { code, msg: msg.into(), leader: None, ranges: vec![] }
}

impl Pd {
    pub async fn start(cfg: PdConfig) -> std::io::Result<Arc<Pd>> {
        std::fs::create_dir_all(&cfg.data_dir)?;
        let addrs: AddrBook = Arc::new(RwLock::new(cfg.peers.iter().map(|(k, v)| (*k, v.clone())).collect::<HashMap<_, _>>()));
        let transport = GrpcTransport::new(cfg.id, addrs.clone());
        let sm = PdSm::new();
        let fs: Arc<dyn Vfs> = OsFs::new_arc(false);
        let handle = spawn_group(0, cfg.id, Membership::new(cfg.peers.keys().copied()), fs, &format!("{}/raft", cfg.data_dir), sm.clone(), transport.clone(), cfg.group.clone())?;
        let pd = Arc::new(Pd {
            id: cfg.id,
            addr: cfg.addr,
            handle,
            sm,
            addrs,
            mem: Mutex::new(Mem::default()),
            tso: AsyncMutex::new(TsoState { phys: 0, logical: 0, window_upper: 0, term: 0 }),
            id_block: AsyncMutex::new((0, 0, 0)),
            transport,
        });
        let p2 = pd.clone();
        tokio::spawn(async move { p2.schedule_loop().await });
        Ok(pd)
    }

    pub fn deliver(&self, msg: raft::Message) {
        self.handle.step(msg);
    }

    /// Err(NOT_LEADER) unless this PD is the Raft leader; resets leader-local state on term change.
    async fn ensure_leader(&self) -> Result<u64, Error> {
        let st = self.handle.status().await.ok_or(perr(6, "pd stopped"))?;
        if st.role != Role::Leader {
            let leader = st.leader.map(|l| Peer { store_id: l, addr: self.addrs.read().get(&l).cloned().unwrap_or_default(), learner: false });
            return Err(Error { code: 1, msg: "pd not leader".into(), leader, ranges: vec![] });
        }
        let mut m = self.mem.lock();
        if m.term != st.term {
            *m = Mem { term: st.term, ..Mem::default() };
        }
        Ok(st.term)
    }

    async fn propose(&self, c: PdCmd) -> Result<Vec<u8>, Error> {
        match tokio::time::timeout(Duration::from_secs(3), self.handle.propose(c.encode())).await {
            Err(_) => Err(perr(5, "pd proposal timed out")),
            Ok(Err(DriverError::NotLeader(l))) => Err(Error { code: 1, msg: "pd not leader".into(), leader: l.map(|l| Peer { store_id: l, addr: self.addrs.read().get(&l).cloned().unwrap_or_default(), learner: false }), ranges: vec![] }),
            Ok(Err(e)) => Err(perr(6, &format!("{e:?}"))),
            Ok(Ok(r)) => Ok(r),
        }
    }

    // ---------------------------------------------------------- TSO / ids

    pub async fn tso(&self, count: u32) -> Result<u64, Error> {
        let term = self.ensure_leader().await?;
        let count = count.max(1) as u64;
        let mut t = self.tso.lock().await;
        if t.term != term {
            // new leadership: never reuse anything the previous leader may have issued
            let upper = self.sm.st.lock().tso_upper;
            t.phys = upper + 1;
            t.logical = 0;
            t.window_upper = upper;
            t.term = term;
        }
        for _ in 0..8 {
            let now = now_ms();
            if now > t.phys {
                t.phys = now;
                t.logical = 0;
            }
            if t.logical + count >= (1 << LOGICAL_BITS) {
                t.phys += 1;
                t.logical = 0;
                continue;
            }
            if t.phys + 1 >= t.window_upper {
                let want = t.phys + TSO_WINDOW_MS;
                let r = self.propose(PdCmd::TsoUpper(want)).await?;
                t.window_upper = u64::from_le_bytes(r[..8].try_into().map_err(|_| perr(6, "bad tso reply"))?);
                continue;
            }
            let first = (t.phys << LOGICAL_BITS) | t.logical;
            t.logical += count;
            return Ok(first);
        }
        Err(perr(6, "could not allocate timestamps"))
    }

    pub async fn alloc_id(&self, count: u32) -> Result<u64, Error> {
        let term = self.ensure_leader().await?;
        let count = count.max(1) as u64;
        let mut b = self.id_block.lock().await;
        if b.2 != term || b.0 + count > b.1 {
            let block = count.max(1000);
            let r = self.propose(PdCmd::AllocIds(block)).await?;
            let first = u64::from_le_bytes(r[..8].try_into().map_err(|_| perr(6, "bad id reply"))?);
            *b = (first, first + block, term);
        }
        let first = b.0;
        b.0 += count;
        Ok(first)
    }

    pub async fn set_config(&self, k: &str, v: &str) -> Result<(), Error> {
        self.ensure_leader().await?;
        self.propose(PdCmd::SetConfig(k.into(), v.into())).await.map(|_| ())
    }

    fn cfg_u64(&self, key: &str, default: u64) -> u64 {
        self.sm.st.lock().config.get(key).and_then(|v| v.parse().ok()).unwrap_or(default)
    }

    // ---------------------------------------------------------- bootstrap / heartbeat / routing

    pub async fn bootstrap(&self, store: StoreInfo) -> Result<Option<RangeInfo>, Error> {
        let term = self.ensure_leader().await?;
        let r = self.propose(PdCmd::Bootstrap { store_id: store.id, addr: store.addr.clone() }).await?;
        let first = r.first() == Some(&1);
        let mut m = self.mem.lock();
        m.term = term;
        if first {
            let info = RangeInfo { id: 1, start: vec![], end: vec![], version: 1, conf_ver: 1, peers: vec![Peer { store_id: store.id, addr: store.addr.clone(), learner: false }], leader_store: store.id };
            m.ranges.insert(vec![], RangeEntry { info: info.clone(), term: 0, bytes: 0, write_qps: 0, read_qps: 0, commit: 0, updated_ms: now_ms() });
            return Ok(Some(info));
        }
        Ok(None)
    }

    pub async fn heartbeat(&self, req: HeartbeatReq) -> Result<HeartbeatResp, Error> {
        self.ensure_leader().await?;
        let now = now_ms();
        let mut m = self.mem.lock();
        if let Some(s) = req.store {
            m.stores.insert(s.id, StoreEntry { info: s, last_seen_ms: now });
        }
        let sid = req.ranges.first().and_then(|_| m.stores.iter().find(|(_, e)| e.last_seen_ms == now).map(|(k, _)| *k));
        let _ = sid;
        for rr in req.ranges {
            let Some(info) = rr.info else { continue };
            let store_id = info.peers.iter().find(|p| p.store_id != 0 && false).map(|p| p.store_id).unwrap_or(0);
            let _ = store_id;
            if rr.is_leader {
                let accept = match m.ranges.get(&info.start) {
                    Some(e) if e.info.id == info.id => info.version > e.info.version || (info.version == e.info.version && rr.term >= e.term),
                    Some(e) => info.version >= e.info.version,
                    None => true,
                };
                if accept {
                    let stale: Vec<Vec<u8>> = m.ranges.iter().filter(|(_, e)| e.info.id != info.id && overlaps(&e.info, &info) && e.info.version <= info.version).map(|(k, _)| k.clone()).collect();
                    for k in stale {
                        m.ranges.remove(&k);
                    }
                    m.ranges.retain(|_, e| e.info.id != info.id);
                    m.ranges.insert(info.start.clone(), RangeEntry { info, term: rr.term, bytes: rr.approx_bytes, write_qps: rr.write_qps, read_qps: rr.read_qps, commit: rr.commit, updated_ms: now });
                }
            }
        }
        Ok(HeartbeatResp { error: None, commands: vec![], stores: m.stores.values().map(|s| s.info.clone()).collect() })
    }

    /// Heartbeat entry point that also knows the sending store (extracted from `store`).
    pub async fn heartbeat_full(&self, req: HeartbeatReq) -> Result<HeartbeatResp, Error> {
        let sender = req.store.as_ref().map(|s| s.id).unwrap_or(0);
        // record per-replica progress of non-leader replicas for learner promotion and replica GC
        {
            self.ensure_leader().await?;
            let now = now_ms();
            let mut m = self.mem.lock();
            for rr in &req.ranges {
                if let Some(info) = &rr.info {
                    if !rr.is_leader {
                        m.reports.insert((info.id, sender), (rr.applied, rr.commit, now));
                    }
                }
            }
            // replica GC: a store reports a replica of a range whose leader-reported peers exclude it
            let mut gc = vec![];
            let mut unlisted = std::mem::take(&mut m.unlisted_since);
            for rr in &req.ranges {
                let Some(info) = &rr.info else { continue };
                if let Some(e) = m.ranges.values().find(|e| e.info.id == info.id) {
                    let known = !e.info.peers.is_empty() && now.saturating_sub(e.updated_ms) < 5000;
                    let listed = e.info.peers.iter().any(|p| p.store_id == sender);
                    if listed {
                        unlisted.remove(&(info.id, sender));
                    } else if known && e.info.version >= info.version {
                        // a brand-new learner reports its replica before the leader lists it: only
                        // garbage-collect replicas that have stayed unlisted for a while
                        let since = *unlisted.entry((info.id, sender)).or_insert(now);
                        let last = m.gc_sent.get(&(info.id, sender)).copied().unwrap_or(0);
                        if now.saturating_sub(since) > 30_000 && now.saturating_sub(last) > 10_000 {
                            gc.push(PdCommand { kind: 6, range_id: info.id, store_id: sender, split_key: vec![] });
                        }
                    }
                }
            }
            m.unlisted_since = unlisted;
            for c in &gc {
                m.gc_sent.insert((c.range_id, sender), now);
            }
            if !gc.is_empty() {
                m.queue.entry(sender).or_default().extend(gc);
            }
        }
        let mut resp = self.heartbeat(req).await?;
        let mut m = self.mem.lock();
        resp.commands = m.queue.remove(&sender).unwrap_or_default();
        Ok(resp)
    }

    pub fn route(&self, key: &[u8]) -> Option<RangeInfo> {
        let m = self.mem.lock();
        m.ranges.range(..=key.to_vec()).next_back().filter(|(_, e)| key >= e.info.start.as_slice() && (e.info.end.is_empty() || key < e.info.end.as_slice())).map(|(_, e)| e.info.clone())
    }
    pub fn route_by_id(&self, id: u64) -> Option<RangeInfo> {
        self.mem.lock().ranges.values().find(|e| e.info.id == id).map(|e| e.info.clone())
    }
    pub fn all(&self) -> (Vec<RangeInfo>, Vec<StoreInfo>) {
        let m = self.mem.lock();
        let now = now_ms();
        (m.ranges.values().map(|e| e.info.clone()).collect(), m.stores.values().map(|s| StoreInfo { alive: now.saturating_sub(s.last_seen_ms) < STORE_DEAD_MS, ..s.info.clone() }).collect())
    }
    pub async fn leader_check(&self) -> Result<(), Error> {
        self.ensure_leader().await.map(|_| ())
    }

    // ---------------------------------------------------------- scheduler

    async fn schedule_loop(self: Arc<Self>) {
        loop {
            tokio::time::sleep(Duration::from_millis(1000)).await;
            if self.ensure_leader().await.is_err() {
                continue;
            }
            self.schedule();
        }
    }

    /// One scheduling round: at most one new operation per range, a few cluster-wide.
    pub fn schedule(&self) {
        let replication = self.cfg_u64("replication", 3) as usize;
        let split_bytes = self.cfg_u64("split_bytes", 96 << 20);
        let hot_wq = self.cfg_u64("hot_write_qps", 0);
        let max_ops = self.cfg_u64("max_concurrent_ops", 4) as usize;
        let balance = self.cfg_u64("balance", 1) == 1;
        // drain a store (used by the operator before scale-in): treated like a dead store so replicas move away
        let evict = self.cfg_u64("evict_store", 0);
        let now = now_ms();
        let mut m = self.mem.lock();
        {
            // an operation is finished as soon as the range's membership or version changed
            let sigs: HashMap<u64, (Vec<(u64, bool)>, u64)> = m.ranges.values().map(|e| (e.info.id, (peer_sig(&e.info), e.info.version))).collect();
            m.pending.retain(|rid, (_, since, sig, ver)| now.saturating_sub(*since) < 20_000 && sigs.get(rid).map(|(s, v)| s == sig && v == ver).unwrap_or(true));
        }
        let alive: Vec<u64> = m.stores.iter().filter(|(id, s)| now.saturating_sub(s.last_seen_ms) < STORE_DEAD_MS && **id != evict).map(|(id, _)| *id).collect();
        let is_alive = |id: u64| alive.contains(&id);
        // replica counts per store (from the routing table)
        let mut counts: HashMap<u64, usize> = alive.iter().map(|s| (*s, 0)).collect();
        let mut leaders: HashMap<u64, usize> = alive.iter().map(|s| (*s, 0)).collect();
        for e in m.ranges.values() {
            for p in &e.info.peers {
                *counts.entry(p.store_id).or_default() += 1;
            }
            *leaders.entry(e.info.leader_store).or_default() += 1;
        }
        let entries: Vec<RangeEntry> = m.ranges.values().cloned().collect();
        let mut new_cmds: Vec<(u64, PdCommand)> = vec![];
        let mut new_pending: Vec<(u64, u32)> = vec![];
        for e in &entries {
            if m.pending.len() + new_pending.len() >= max_ops {
                break;
            }
            let rid = e.info.id;
            if m.pending.contains_key(&rid) || e.info.leader_store == 0 || now.saturating_sub(e.updated_ms) > 10_000 {
                continue;
            }
            let leader = e.info.leader_store;
            let voters: Vec<&Peer> = e.info.peers.iter().filter(|p| !p.learner).collect();
            let learners: Vec<&Peer> = e.info.peers.iter().filter(|p| p.learner).collect();
            let alive_voters = voters.iter().filter(|p| is_alive(p.store_id)).count();
            let cmd = |kind: u32, store: u64, key: Vec<u8>| (leader, PdCommand { kind, range_id: rid, store_id: store, split_key: key });
            // 1. promote caught-up learners
            if let Some(l) = learners.iter().find(|l| {
                m.reports.get(&(rid, l.store_id)).map(|(applied, _, at)| now.saturating_sub(*at) < 5000 && applied + 100 >= e.commit).unwrap_or(false)
            }) {
                new_cmds.push(cmd(2, l.store_id, vec![]));
                new_pending.push((rid, 2));
                continue;
            }
            if !learners.is_empty() {
                continue; // wait for learners to catch up
            }
            // 1b. leader sits on the store being drained: hand leadership to a healthy voter first
            if evict != 0 && leader == evict {
                if let Some(t) = voters.iter().find(|p| p.store_id != evict && is_alive(p.store_id)) {
                    new_cmds.push(cmd(4, t.store_id, vec![]));
                    new_pending.push((rid, 4));
                    continue;
                }
            }
            // 2. too few live replicas: add one on the least loaded store that doesn't have the range
            if alive_voters < replication {
                if let Some(target) = alive.iter().filter(|s| !e.info.peers.iter().any(|p| p.store_id == **s)).min_by_key(|s| counts.get(s).copied().unwrap_or(0)) {
                    new_cmds.push(cmd(1, *target, vec![]));
                    new_pending.push((rid, 1));
                    *counts.entry(*target).or_default() += 1;
                    continue;
                }
            }
            // 3. too many replicas (or dead ones once enough are alive): remove
            if voters.len() > replication || (alive_voters >= replication && voters.len() > alive_voters) {
                let victim = voters.iter().filter(|p| p.store_id != leader).max_by_key(|p| (!is_alive(p.store_id), counts.get(&p.store_id).copied().unwrap_or(0))).map(|p| p.store_id);
                if let Some(v) = victim {
                    new_cmds.push(cmd(3, v, vec![]));
                    new_pending.push((rid, 3));
                    continue;
                }
            }
            // 4. size / hot-spot split
            if e.bytes > split_bytes || (hot_wq > 0 && e.write_qps > hot_wq) {
                new_cmds.push(cmd(5, 0, vec![]));
                new_pending.push((rid, 5));
                continue;
            }
        }
        // 5. replica balance (one move per round)
        if balance && alive.len() > replication && m.pending.len() + new_pending.len() < max_ops {
            let (maxs, maxn) = counts.iter().max_by_key(|(s, n)| (**n, **s)).map(|(s, n)| (*s, *n)).unwrap();
            let (mins, minn) = counts.iter().min_by_key(|(s, n)| (**n, **s)).map(|(s, n)| (*s, *n)).unwrap();
            if maxn >= minn + 2 {
                if let Some(e) = entries.iter().find(|e| {
                    !m.pending.contains_key(&e.info.id) && !new_pending.iter().any(|(r, _)| *r == e.info.id) && e.info.peers.iter().any(|p| p.store_id == maxs) && !e.info.peers.iter().any(|p| p.store_id == mins) && e.info.leader_store != 0 && e.info.peers.iter().all(|p| !p.learner)
                }) {
                    new_cmds.push((e.info.leader_store, PdCommand { kind: 1, range_id: e.info.id, store_id: mins, split_key: vec![] }));
                    new_pending.push((e.info.id, 1));
                }
            }
        }
        // 6. leader balance (one transfer per round)
        if balance && alive.len() > 1 && m.pending.len() + new_pending.len() < max_ops {
            let (maxs, maxl) = leaders.iter().filter(|(s, _)| is_alive(**s)).max_by_key(|(s, n)| (**n, **s)).map(|(s, n)| (*s, *n)).unwrap_or((0, 0));
            let (mins, minl) = leaders.iter().filter(|(s, _)| is_alive(**s)).min_by_key(|(s, n)| (**n, **s)).map(|(s, n)| (*s, *n)).unwrap_or((0, 0));
            if maxl >= minl + 2 {
                if let Some(e) = entries.iter().find(|e| e.info.leader_store == maxs && !m.pending.contains_key(&e.info.id) && !new_pending.iter().any(|(r, _)| *r == e.info.id) && e.info.peers.iter().any(|p| p.store_id == mins && !p.learner)) {
                    new_cmds.push((maxs, PdCommand { kind: 4, range_id: e.info.id, store_id: mins, split_key: vec![] }));
                    new_pending.push((e.info.id, 4));
                }
            }
        }
        for (store, c) in new_cmds {
            m.queue.entry(store).or_default().push(c);
        }
        for (r, k) in new_pending {
            let (sig, ver) = entries.iter().find(|e| e.info.id == r).map(|e| (peer_sig(&e.info), e.info.version)).unwrap_or_default();
            m.pending.insert(r, (k, now, sig, ver));
        }
    }

    /// Ranges per store and leaders per store (for tests/diagnostics).
    pub fn distribution(&self) -> (HashMap<u64, usize>, HashMap<u64, usize>) {
        let m = self.mem.lock();
        let mut rc = HashMap::new();
        let mut lc = HashMap::new();
        for e in m.ranges.values() {
            for p in &e.info.peers {
                *rc.entry(p.store_id).or_default() += 1;
            }
            *lc.entry(e.info.leader_store).or_default() += 1;
        }
        (rc, lc)
    }
}

fn peer_sig(i: &RangeInfo) -> Vec<(u64, bool)> {
    let mut v: Vec<(u64, bool)> = i.peers.iter().map(|p| (p.store_id, p.learner)).collect();
    v.sort();
    v
}

fn overlaps(a: &RangeInfo, b: &RangeInfo) -> bool {
    let a_before_b = !a.end.is_empty() && a.end <= b.start;
    let b_before_a = !b.end.is_empty() && b.end <= a.start;
    !(a_before_b || b_before_a)
}
