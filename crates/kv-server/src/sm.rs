//! The per-range replicated state machine over the shared storage engine.
use crate::cmd::{Cmd, Resp};
use kv_engine::{Db, WriteBatch};
use parking_lot::{Mutex, RwLock};
use raft::codec::{R, W};
use raft_driver::StateMachine;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};
use txn::keys::*;
use txn::percolator::{self, Batch, TxnStatus, TxnStore};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RangeDesc {
    pub id: u64,
    pub start: Vec<u8>,
    pub end: Vec<u8>, // empty = +inf
    pub version: u64,
}

impl RangeDesc {
    pub fn contains(&self, key: &[u8]) -> bool {
        key >= self.start.as_slice() && (self.end.is_empty() || key < self.end.as_slice())
    }
    pub fn encode(&self) -> Vec<u8> {
        let mut w = W::default();
        w.u64(self.id);
        w.bytes(&self.start);
        w.bytes(&self.end);
        w.u64(self.version);
        w.0
    }
    pub fn decode(b: &[u8]) -> Option<RangeDesc> {
        let mut r = R(b);
        Some(RangeDesc { id: r.u64()?, start: r.bytes()?, end: r.bytes()?, version: r.u64()? })
    }
}

#[derive(Clone, Debug)]
pub struct Event {
    pub index: u64,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub deleted: bool,
}

#[derive(Debug)]
pub struct SplitEvent {
    pub parent: RangeDesc,
    pub child: RangeDesc,
}

/// Engine view with the not-yet-written changes of the current apply batch layered on top.
pub struct Overlay<'a> {
    db: &'a Db,
    pub pending: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
}

impl<'a> Overlay<'a> {
    fn new(db: &'a Db) -> Self {
        Overlay { db, pending: BTreeMap::new() }
    }
    fn apply(&mut self, b: Batch) {
        for (k, v) in b.ops {
            self.pending.insert(k, v);
        }
    }
    fn put(&mut self, k: Vec<u8>, v: Vec<u8>) {
        self.pending.insert(k, Some(v));
    }
    fn del(&mut self, k: Vec<u8>) {
        self.pending.insert(k, None);
    }
}

impl TxnStore for Overlay<'_> {
    fn get(&self, k: &[u8]) -> Option<Vec<u8>> {
        if let Some(v) = self.pending.get(k) {
            return v.clone();
        }
        self.db.get(k).ok().flatten()
    }
    fn scan(&self, start: &[u8], end: &[u8], limit: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        let over: Vec<(&Vec<u8>, &Option<Vec<u8>>)> = self.pending.range(start.to_vec()..end.to_vec()).collect();
        let want = if limit == usize::MAX { 0 } else { limit + over.len() };
        let mut m: BTreeMap<Vec<u8>, Vec<u8>> = self.db.scan(start, end, want).unwrap_or_default().into_iter().collect();
        for (k, v) in over {
            match v {
                Some(v) => {
                    m.insert(k.clone(), v.clone());
                }
                None => {
                    m.remove(k);
                }
            }
        }
        m.into_iter().take(limit).collect()
    }
}

pub struct RangeSm {
    pub db: Arc<Db>,
    pub id: u64,
    desc: RwLock<RangeDesc>,
    applied: AtomicU64,
    sessions: Mutex<BTreeMap<u64, (u64, Vec<u8>)>>,
    pub events: broadcast::Sender<Arc<Event>>,
    split_tx: mpsc::UnboundedSender<SplitEvent>,
    pub approx_bytes: AtomicU64,
    pub writes: AtomicU64,
}

fn applied_key(id: u64) -> Vec<u8> {
    meta_key("applied", id)
}
fn range_key(id: u64) -> Vec<u8> {
    meta_key("range", id)
}
fn session_key(id: u64, client: u64) -> Vec<u8> {
    let mut k = meta_key("session", id);
    k.extend_from_slice(&client.to_be_bytes());
    k
}

/// Load all persisted range descriptors (used at store startup).
pub fn load_ranges(db: &Db) -> Vec<RangeDesc> {
    let start = vec![CF_META];
    let mut end = start.clone();
    end[0] += 1;
    let prefix = {
        let mut p = vec![CF_META];
        p.extend_from_slice(b"range/");
        p
    };
    let mut pend = prefix.clone();
    *pend.last_mut().unwrap() += 1;
    db.scan(&prefix, &pend, 0).unwrap_or_default().into_iter().filter_map(|(_, v)| RangeDesc::decode(&v)).collect()
}

pub fn persist_range(db: &Db, d: &RangeDesc) -> std::io::Result<()> {
    let mut b = WriteBatch::new();
    b.put(&range_key(d.id), &d.encode());
    db.write(b)
}

impl RangeSm {
    pub fn new(db: Arc<Db>, desc: RangeDesc, split_tx: mpsc::UnboundedSender<SplitEvent>) -> Arc<RangeSm> {
        let id = desc.id;
        let applied = db.get(&applied_key(id)).ok().flatten().map(|v| u64::from_le_bytes(v[..8].try_into().unwrap())).unwrap_or(0);
        // the persisted descriptor wins over what the caller thinks (a split may have shrunk us)
        let desc = db.get(&range_key(id)).ok().flatten().and_then(|v| RangeDesc::decode(&v)).unwrap_or(desc);
        let mut sessions = BTreeMap::new();
        let mut sp = session_key(id, 0);
        sp.truncate(sp.len() - 8);
        let mut se = sp.clone();
        *se.last_mut().unwrap() += 1;
        for (k, v) in db.scan(&sp, &se, 0).unwrap_or_default() {
            if k.len() >= 8 && v.len() >= 8 {
                let client = u64::from_be_bytes(k[k.len() - 8..].try_into().unwrap());
                sessions.insert(client, (u64::from_le_bytes(v[..8].try_into().unwrap()), v[8..].to_vec()));
            }
        }
        let (events, _) = broadcast::channel(8192);
        let sm = Arc::new(RangeSm {
            db,
            id,
            desc: RwLock::new(desc),
            applied: AtomicU64::new(applied),
            sessions: Mutex::new(sessions),
            events,
            split_tx,
            approx_bytes: AtomicU64::new(0),
            writes: AtomicU64::new(0),
        });
        sm.recompute_size();
        sm
    }

    pub fn desc(&self) -> RangeDesc {
        self.desc.read().clone()
    }

    pub fn recompute_size(&self) {
        let d = self.desc();
        let (s, e) = cf_range(CF_RAW, &d.start, &d.end);
        let mut total = 0u64;
        // sample-free exact count; fine at lab scale
        if let Ok(items) = self.db.scan(&s, &e, 0) {
            total += items.iter().map(|(k, v)| (k.len() + v.len()) as u64).sum::<u64>();
        }
        let (s, e) = cf_range(CF_DATA, &d.start, &d.end);
        if let Ok(items) = self.db.scan(&s, &e, 0) {
            total += items.iter().map(|(k, v)| (k.len() + v.len()) as u64).sum::<u64>();
        }
        self.approx_bytes.store(total, Ordering::Relaxed);
    }

    /// Median user key of the range (a good split point), if the range has enough keys.
    pub fn sample_split_key(&self) -> Option<Vec<u8>> {
        let d = self.desc();
        let mut keys = std::collections::BTreeSet::new();
        for cf in [CF_RAW, CF_WRITE, CF_LOCK] {
            let (s, e) = cf_range(cf, &d.start, &d.end);
            for (k, _) in self.db.scan(&s, &e, 0).unwrap_or_default() {
                if let Some((uk, _)) = dec(&k[1..]) {
                    keys.insert(uk);
                }
            }
        }
        if keys.len() < 2 {
            return None;
        }
        let mid = keys.iter().nth(keys.len() / 2).cloned()?;
        if mid.as_slice() > d.start.as_slice() {
            Some(mid)
        } else {
            None
        }
    }

    // ------------------------------------------------------------ reads (not replicated)

    pub fn read_raw(&self, key: &[u8], now_ms: u64) -> Option<Vec<u8>> {
        let v = self.db.get(&raw_key(key)).ok().flatten()?;
        let (exp, val) = parse_raw_value(&v)?;
        if exp != 0 && exp <= now_ms {
            return None;
        }
        Some(val.to_vec())
    }

    pub fn scan_raw(&self, start: &[u8], end: &[u8], limit: usize, now_ms: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
        let d = self.desc();
        let s = if start < d.start.as_slice() { d.start.clone() } else { start.to_vec() };
        let e = if end.is_empty() || (!d.end.is_empty() && d.end.as_slice() < end) { d.end.clone() } else { end.to_vec() };
        let (es, ee) = cf_range(CF_RAW, &s, &e);
        let mut out = vec![];
        for (k, v) in self.db.scan(&es, &ee, 0).unwrap_or_default() {
            if let (Some((uk, _)), Some((exp, val))) = (dec(&k[1..]), parse_raw_value(&v)) {
                if exp != 0 && exp <= now_ms {
                    continue;
                }
                out.push((uk, val.to_vec()));
                if limit > 0 && out.len() >= limit {
                    break;
                }
            }
        }
        out
    }

    pub fn txn_get(&self, key: &[u8], ts: u64) -> Result<Option<Vec<u8>>, percolator::Lock> {
        let ov = Overlay::new(&self.db);
        percolator::read(&ov, key, ts)
    }

    /// Snapshot scan at `ts`; Err(lock) at the first blocking lock.
    pub fn txn_scan(&self, start: &[u8], end: &[u8], limit: usize, ts: u64) -> Result<Vec<(Vec<u8>, Vec<u8>)>, (Vec<u8>, percolator::Lock)> {
        let d = self.desc();
        let s = if start < d.start.as_slice() { d.start.clone() } else { start.to_vec() };
        let e = if end.is_empty() || (!d.end.is_empty() && d.end.as_slice() < end) { d.end.clone() } else { end.to_vec() };
        let ov = Overlay::new(&self.db);
        // candidate keys = keys that have a write record or a lock
        let mut keys = std::collections::BTreeSet::new();
        for cf in [CF_WRITE, CF_LOCK] {
            let (es, ee) = cf_range(cf, &s, &e);
            for (k, _) in self.db.scan(&es, &ee, 0).unwrap_or_default() {
                if let Some((uk, _)) = dec(&k[1..]) {
                    keys.insert(uk);
                }
            }
        }
        let mut out = vec![];
        for k in keys {
            match percolator::read(&ov, &k, ts) {
                Ok(Some(v)) => {
                    out.push((k, v));
                    if limit > 0 && out.len() >= limit {
                        break;
                    }
                }
                Ok(None) => {}
                Err(l) => return Err((k, l)),
            }
        }
        Ok(out)
    }

    // ------------------------------------------------------------ apply

    fn exec(
        &self,
        index: u64,
        cmd: Cmd,
        ov: &mut Overlay,
        events: &mut Vec<Event>,
        splits: &mut Vec<SplitEvent>,
        sessions: &mut BTreeMap<u64, (u64, Vec<u8>)>,
    ) -> Resp {
        let d = self.desc();
        if cmd.keys().iter().any(|k| !d.contains(k)) {
            return Resp::RangeMoved;
        }
        let dedup = |client: u64, seq: u64, sessions: &BTreeMap<u64, (u64, Vec<u8>)>| -> Option<Resp> {
            if client == 0 {
                return None;
            }
            match sessions.get(&client) {
                Some((s, r)) if seq <= *s => Some(Resp::decode(r).unwrap_or(Resp::Ok)),
                _ => None,
            }
        };
        let record = |client: u64, seq: u64, r: &Resp, sessions: &mut BTreeMap<u64, (u64, Vec<u8>)>, ov: &mut Overlay, id: u64| {
            if client != 0 {
                let enc = r.encode();
                let mut v = seq.to_le_bytes().to_vec();
                v.extend_from_slice(&enc);
                ov.put(session_key(id, client), v);
                sessions.insert(client, (seq, enc));
            }
        };
        match cmd {
            Cmd::RawPut { key, value, expire_ms, client, seq } => {
                if let Some(r) = dedup(client, seq, sessions) {
                    return r;
                }
                self.approx_bytes.fetch_add((key.len() + value.len()) as u64, Ordering::Relaxed);
                ov.put(raw_key(&key), raw_value(expire_ms, &value));
                events.push(Event { index, key, value, deleted: false });
                let r = Resp::Ok;
                record(client, seq, &r, sessions, ov, self.id);
                r
            }
            Cmd::RawDelete { key, client, seq } => {
                if let Some(r) = dedup(client, seq, sessions) {
                    return r;
                }
                ov.del(raw_key(&key));
                events.push(Event { index, key, value: vec![], deleted: true });
                let r = Resp::Ok;
                record(client, seq, &r, sessions, ov, self.id);
                r
            }
            Cmd::RawCas { key, expect, new, client, seq } => {
                if let Some(r) = dedup(client, seq, sessions) {
                    return r;
                }
                let cur = ov.get(&raw_key(&key)).and_then(|v| parse_raw_value(&v).map(|(_, x)| x.to_vec()));
                let ok = cur == expect;
                if ok {
                    ov.put(raw_key(&key), raw_value(0, &new));
                    events.push(Event { index, key, value: new, deleted: false });
                }
                let r = Resp::Cas(ok);
                record(client, seq, &r, sessions, ov, self.id);
                r
            }
            Cmd::Prewrite { muts, primary, start_ts, ttl_ms, read_keys } => {
                let mut b = Batch::default();
                let mut errs = vec![];
                for m in &muts {
                    if let Err(e) = percolator::prewrite(ov, &mut b, &m.key, m.op, &m.value, &primary, start_ts, ttl_ms) {
                        errs.push(e);
                    }
                }
                for k in &read_keys {
                    if let Err(e) = percolator::validate_read(ov, k, start_ts) {
                        errs.push(e);
                    }
                }
                if errs.is_empty() {
                    self.approx_bytes.fetch_add(muts.iter().map(|m| (m.key.len() + m.value.len()) as u64).sum::<u64>(), Ordering::Relaxed);
                    ov.apply(b);
                    Resp::Ok
                } else {
                    Resp::KeyErrors(errs)
                }
            }
            Cmd::Commit { keys, start_ts, commit_ts } => {
                let mut b = Batch::default();
                for k in &keys {
                    if let Err(e) = percolator::commit(ov, &mut b, k, start_ts, commit_ts) {
                        return Resp::KeyErrors(vec![e]);
                    }
                    // CDC: committed puts/deletes
                    if let Some(l) = percolator::get_lock(ov, k) {
                        if l.start_ts == start_ts {
                            match l.op {
                                1 => events.push(Event {
                                    index,
                                    key: k.clone(),
                                    value: ov.get(&versioned_key(CF_DATA, k, start_ts)).unwrap_or_default(),
                                    deleted: false,
                                }),
                                2 => events.push(Event { index, key: k.clone(), value: vec![], deleted: true }),
                                _ => {}
                            }
                        }
                    }
                }
                ov.apply(b);
                Resp::Ok
            }
            Cmd::Rollback { keys, start_ts } => {
                let mut b = Batch::default();
                for k in &keys {
                    if let Err(e) = percolator::rollback(ov, &mut b, k, start_ts) {
                        return Resp::KeyErrors(vec![e]);
                    }
                }
                ov.apply(b);
                Resp::Ok
            }
            Cmd::CheckTxnStatus { primary, start_ts, now_ms, rollback_if_not_exist } => {
                let mut b = Batch::default();
                let st = percolator::check_txn_status(ov, &mut b, &primary, start_ts, now_ms, rollback_if_not_exist);
                ov.apply(b);
                match st {
                    TxnStatus::Locked { ttl_ms } => Resp::TxnStatus { action: 0, commit_ts: 0, ttl_ms },
                    TxnStatus::Committed { commit_ts } => Resp::TxnStatus { action: 1, commit_ts, ttl_ms: 0 },
                    TxnStatus::RolledBack => Resp::TxnStatus { action: 2, commit_ts: 0, ttl_ms: 0 },
                    TxnStatus::NotExist => Resp::TxnStatus { action: 3, commit_ts: 0, ttl_ms: 0 },
                }
            }
            Cmd::ResolveLock { keys, start_ts, commit_ts } => {
                let mut b = Batch::default();
                for k in &keys {
                    let has_lock = percolator::get_lock(ov, k).map(|l| l.start_ts == start_ts).unwrap_or(false);
                    if !has_lock {
                        continue; // already resolved
                    }
                    if commit_ts > 0 {
                        if let Some(l) = percolator::get_lock(ov, k) {
                            match l.op {
                                1 => events.push(Event {
                                    index,
                                    key: k.clone(),
                                    value: ov.get(&versioned_key(CF_DATA, k, start_ts)).unwrap_or_default(),
                                    deleted: false,
                                }),
                                2 => events.push(Event { index, key: k.clone(), value: vec![], deleted: true }),
                                _ => {}
                            }
                        }
                        let _ = percolator::commit(ov, &mut b, k, start_ts, commit_ts);
                    } else {
                        let _ = percolator::rollback(ov, &mut b, k, start_ts);
                    }
                }
                ov.apply(b);
                Resp::Ok
            }
            Cmd::Split { split_key, new_range_id } => {
                let valid = split_key.as_slice() > d.start.as_slice() && (d.end.is_empty() || split_key.as_slice() < d.end.as_slice());
                if !valid {
                    return Resp::Failed("invalid split key".into());
                }
                let left = RangeDesc { id: d.id, start: d.start.clone(), end: split_key.clone(), version: d.version + 1 };
                let right = RangeDesc { id: new_range_id, start: split_key, end: d.end.clone(), version: d.version + 1 };
                ov.put(range_key(left.id), left.encode());
                ov.put(range_key(right.id), right.encode());
                *self.desc.write() = left.clone();
                splits.push(SplitEvent { parent: left, child: right });
                Resp::Ok
            }
            Cmd::SweepExpired { now_ms, limit } => {
                let (s, e) = cf_range(CF_RAW, &d.start, &d.end);
                let mut n = 0;
                for (k, v) in ov.scan(&s, &e, usize::MAX) {
                    if let Some((exp, _)) = parse_raw_value(&v) {
                        if exp != 0 && exp <= now_ms {
                            ov.del(k);
                            n += 1;
                            if n >= limit {
                                break;
                            }
                        }
                    }
                }
                Resp::Ok
            }
            Cmd::Gc { safe_point, limit } => {
                let (s, e) = cf_range(CF_WRITE, &d.start, &d.end);
                let mut seen = std::collections::BTreeSet::new();
                for (k, _) in ov.scan(&s, &e, usize::MAX) {
                    if let Some((uk, _)) = key_ts(&k) {
                        if seen.len() as u32 >= limit && !seen.contains(&uk) {
                            break;
                        }
                        seen.insert(uk);
                    }
                }
                let mut b = Batch::default();
                for uk in seen {
                    percolator::gc_key(ov, &mut b, &uk, safe_point);
                }
                ov.apply(b);
                Resp::Ok
            }
        }
    }
}

impl StateMachine for RangeSm {
    fn apply_batch(&self, entries: &[(u64, u64, Option<Vec<u8>>)]) -> Vec<Vec<u8>> {
        let mut ov = Overlay::new(&self.db);
        let mut results = Vec::with_capacity(entries.len());
        let mut events = vec![];
        let mut splits = vec![];
        let mut sessions = self.sessions.lock().clone();
        let mut last = 0;
        for (index, _term, data) in entries {
            last = *index;
            let r = match data {
                None => Resp::Ok,
                Some(d) => match Cmd::decode(d) {
                    Some(c) => self.exec(*index, c, &mut ov, &mut events, &mut splits, &mut sessions),
                    None => Resp::Failed("undecodable command".into()),
                },
            };
            results.push(r.encode());
        }
        self.writes.fetch_add(entries.len() as u64, Ordering::Relaxed);
        ov.put(applied_key(self.id), last.to_le_bytes().to_vec());
        let mut wb = WriteBatch::new();
        for (k, v) in &ov.pending {
            match v {
                Some(v) => wb.put(k, v),
                None => wb.delete(k),
            };
        }
        if let Err(e) = self.db.write(wb) {
            // The state machine cannot make progress without durable writes; fail-stop.
            panic!("range {} failed to persist applied batch: {e}", self.id);
        }
        *self.sessions.lock() = sessions;
        self.applied.store(last, Ordering::SeqCst);
        for e in events {
            let _ = self.events.send(Arc::new(e));
        }
        for s in splits {
            let _ = self.split_tx.send(s);
        }
        results
    }

    fn applied_index(&self) -> u64 {
        self.applied.load(Ordering::SeqCst)
    }

    fn snapshot(&self) -> Vec<u8> {
        let d = self.desc();
        let mut w = W::default();
        w.bytes(&d.encode());
        let sessions = self.sessions.lock().clone();
        w.u64(sessions.len() as u64);
        for (c, (s, r)) in &sessions {
            w.u64(*c);
            w.u64(*s);
            w.bytes(r);
        }
        let mut items = vec![];
        for cf in RANGE_CFS {
            let (s, e) = cf_range(cf, &d.start, &d.end);
            items.extend(self.db.scan(&s, &e, 0).unwrap_or_default());
        }
        w.u64(items.len() as u64);
        for (k, v) in items {
            w.bytes(&k);
            w.bytes(&v);
        }
        w.0
    }

    fn restore(&self, index: u64, data: &[u8]) {
        let mut r = R(data);
        let parsed = (|| -> Option<(RangeDesc, BTreeMap<u64, (u64, Vec<u8>)>, Vec<(Vec<u8>, Vec<u8>)>)> {
            let d = RangeDesc::decode(&r.bytes()?)?;
            let mut sessions = BTreeMap::new();
            for _ in 0..r.u64()? {
                sessions.insert(r.u64()?, (r.u64()?, r.bytes()?));
            }
            let mut items = vec![];
            for _ in 0..r.u64()? {
                items.push((r.bytes()?, r.bytes()?));
            }
            Some((d, sessions, items))
        })();
        let Some((d, sessions, items)) = parsed else { panic!("range {}: corrupt snapshot", self.id) };
        let old = self.desc();
        let mut wb = WriteBatch::new();
        // clear the union of the old and the snapshot's key ranges
        let lo = old.start.clone().min(d.start.clone());
        let hi = if old.end.is_empty() || d.end.is_empty() { vec![] } else { old.end.clone().max(d.end.clone()) };
        for cf in RANGE_CFS {
            let (s, e) = cf_range(cf, &lo, &hi);
            for (k, _) in self.db.scan(&s, &e, 0).unwrap_or_default() {
                wb.delete(&k);
            }
        }
        for (k, v) in items {
            wb.put(&k, &v);
        }
        wb.put(&range_key(d.id), &d.encode());
        wb.put(&applied_key(self.id), &index.to_le_bytes());
        for (c, (s, rr)) in &sessions {
            let mut v = s.to_le_bytes().to_vec();
            v.extend_from_slice(rr);
            wb.put(&session_key(self.id, *c), &v);
        }
        self.db.write(wb).expect("restore snapshot");
        *self.desc.write() = d;
        *self.sessions.lock() = sessions;
        self.applied.store(index, Ordering::SeqCst);
        self.recompute_size();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kv_engine::{MemFs, Options};
    use txn::percolator::MutOp;

    fn mk(id: u64, start: &[u8], end: &[u8]) -> (Arc<RangeSm>, mpsc::UnboundedReceiver<SplitEvent>) {
        let fs = MemFs::new();
        let db = Arc::new(Db::open(Options { background: false, ..Options::default() }, fs.as_vfs(), "/db").unwrap());
        let (tx, rx) = mpsc::unbounded_channel();
        (RangeSm::new(db, RangeDesc { id, start: start.to_vec(), end: end.to_vec(), version: 1 }, tx), rx)
    }
    fn ap(sm: &RangeSm, idx: u64, c: Cmd) -> Resp {
        Resp::decode(&sm.apply_batch(&[(idx, 1, Some(c.encode()))])[0]).unwrap()
    }

    #[test]
    fn raw_ops_dedup_and_range_check() {
        let (sm, _rx) = mk(1, b"a", b"m");
        let put = |v: &[u8], seq| Cmd::RawPut { key: b"b".to_vec(), value: v.to_vec(), expire_ms: 0, client: 7, seq };
        assert_eq!(ap(&sm, 1, put(b"1", 1)), Resp::Ok);
        assert_eq!(ap(&sm, 2, put(b"2", 2)), Resp::Ok);
        // a retried (older) request must not re-apply
        assert_eq!(ap(&sm, 3, put(b"1", 1)), Resp::Ok);
        assert_eq!(sm.read_raw(b"b", 0).unwrap(), b"2");
        // outside the range
        assert_eq!(ap(&sm, 4, Cmd::RawPut { key: b"z".to_vec(), value: b"x".to_vec(), expire_ms: 0, client: 0, seq: 0 }), Resp::RangeMoved);
        assert_eq!(ap(&sm, 5, Cmd::RawCas { key: b"b".to_vec(), expect: Some(b"2".to_vec()), new: b"3".to_vec(), client: 7, seq: 3 }), Resp::Cas(true));
        assert_eq!(ap(&sm, 6, Cmd::RawCas { key: b"b".to_vec(), expect: Some(b"2".to_vec()), new: b"4".to_vec(), client: 7, seq: 4 }), Resp::Cas(false));
        assert_eq!(sm.applied_index(), 6);
    }

    #[test]
    fn commands_in_one_batch_see_each_other() {
        let (sm, _rx) = mk(1, b"", b"");
        let muts = vec![crate::cmd::Mut { op: MutOp::Put, key: b"k".to_vec(), value: b"v".to_vec() }];
        let batch = vec![
            (1, 1, Some(Cmd::Prewrite { muts, primary: b"k".to_vec(), start_ts: 10, ttl_ms: 1000, read_keys: vec![] }.encode())),
            (2, 1, Some(Cmd::Commit { keys: vec![b"k".to_vec()], start_ts: 10, commit_ts: 11 }.encode())),
        ];
        let res = sm.apply_batch(&batch);
        assert_eq!(Resp::decode(&res[0]).unwrap(), Resp::Ok);
        assert_eq!(Resp::decode(&res[1]).unwrap(), Resp::Ok);
        assert_eq!(sm.txn_get(b"k", 20).unwrap().unwrap(), b"v");
    }

    #[test]
    fn split_snapshot_restore_roundtrip() {
        let (sm, mut rx) = mk(1, b"", b"");
        for (i, k) in [b"a", b"f", b"m", b"t"].iter().enumerate() {
            ap(&sm, i as u64 + 1, Cmd::RawPut { key: k.to_vec(), value: b"v".to_vec(), expire_ms: 0, client: 1, seq: i as u64 + 1 });
        }
        assert_eq!(ap(&sm, 10, Cmd::Split { split_key: b"m".to_vec(), new_range_id: 2 }), Resp::Ok);
        let ev = rx.try_recv().unwrap();
        assert_eq!((ev.parent.end.clone(), ev.child.start.clone(), ev.child.id), (b"m".to_vec(), b"m".to_vec(), 2));
        assert_eq!(ap(&sm, 11, Cmd::RawPut { key: b"t".to_vec(), value: b"x".to_vec(), expire_ms: 0, client: 0, seq: 0 }), Resp::RangeMoved);
        // snapshot contains only the left range's data
        let snap = sm.snapshot();
        let (sm2, _rx2) = mk(1, b"", b"");
        sm2.restore(11, &snap);
        assert_eq!(sm2.desc().end, b"m".to_vec());
        assert_eq!(sm2.read_raw(b"f", 0).unwrap(), b"v");
        assert!(sm2.read_raw(b"t", 0).is_none() || !sm2.desc().contains(b"t"));
        assert_eq!(sm2.applied_index(), 11);
    }

    #[test]
    fn ttl_hides_and_sweeps() {
        let (sm, _rx) = mk(1, b"", b"");
        ap(&sm, 1, Cmd::RawPut { key: b"t".to_vec(), value: b"v".to_vec(), expire_ms: 100, client: 0, seq: 0 });
        assert!(sm.read_raw(b"t", 50).is_some());
        assert!(sm.read_raw(b"t", 150).is_none());
        ap(&sm, 2, Cmd::SweepExpired { now_ms: 150, limit: 100 });
        assert!(sm.db.get(&raw_key(b"t")).unwrap().is_none());
    }
}
