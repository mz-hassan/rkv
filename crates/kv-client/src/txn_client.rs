//! Percolator transaction client: buffered writes, snapshot reads with lock
//! resolution, two-phase commit across ranges, optional serializable validation.
use crate::pd::PdClient;
use crate::router::{ClientError, Router};
use rkv_proto::*;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IsolationLevel {
    /// snapshot isolation: write skew is possible
    Snapshot,
    /// read keys are locked/validated at prewrite: write skew is prevented for point reads
    /// (range scans lock returned keys only; phantoms remain possible - see LIMITATIONS.md)
    Serializable,
}

#[derive(Debug, Clone)]
pub enum TxnError {
    /// first-committer-wins write conflict
    Conflict {
        key: Vec<u8>,
    },
    /// the transaction was rolled back (e.g. by lock resolution after a timeout)
    Aborted(String),
    /// the commit outcome could not be determined
    Unknown(String),
    Client(ClientError),
}

impl From<ClientError> for TxnError {
    fn from(e: ClientError) -> Self {
        TxnError::Client(e)
    }
}
impl std::fmt::Display for TxnError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for TxnError {}

pub struct TxnClient {
    pub router: Arc<Router>,
    pub pd: Arc<PdClient>,
    pub iso: IsolationLevel,
    pub lock_ttl_ms: u64,
    /// commit secondary keys in the background after the primary commit point
    pub async_secondaries: bool,
}

impl TxnClient {
    pub fn new(pd: Arc<PdClient>, seeds: Vec<String>) -> Arc<TxnClient> {
        let router = Router::new(Some(pd.clone()), seeds);
        Arc::new(TxnClient { router, pd, iso: IsolationLevel::Snapshot, lock_ttl_ms: 3000, async_secondaries: true })
    }
    pub fn with_isolation(pd: Arc<PdClient>, seeds: Vec<String>, iso: IsolationLevel) -> Arc<TxnClient> {
        let router = Router::new(Some(pd.clone()), seeds);
        Arc::new(TxnClient { router, pd, iso, lock_ttl_ms: 3000, async_secondaries: true })
    }

    pub async fn begin(self: &Arc<Self>) -> Result<Txn, TxnError> {
        // fresh timestamp: the snapshot must include everything committed before begin() was called
        let ts = self.pd.tso_fresh().await.map_err(|e| TxnError::Client(ClientError::Unavailable(e)))?;
        Ok(Txn { c: self.clone(), start_ts: ts, writes: BTreeMap::new(), reads: BTreeSet::new() })
    }

    async fn now_ms(&self) -> Result<u64, TxnError> {
        let ts = self.pd.tso_fresh().await.map_err(|e| TxnError::Client(ClientError::Unavailable(e)))?;
        Ok(txn::percolator::physical_ms(ts))
    }

    /// Resolve a lock found by a reader/writer by consulting the primary. Returns after
    /// making progress (the lock is resolved, or we waited for its owner).
    pub async fn resolve_lock(&self, lock: &LockInfo) -> Result<(), TxnError> {
        let mut rine = false;
        for attempt in 0..40u32 {
            let now = self.now_ms().await?;
            let (primary, start_ts) = (lock.primary.clone(), lock.start_ts);
            let st = self
                .router
                .exec(&lock.primary, 0, 0, |mut c, ctx| {
                    let req = CheckTxnStatusReq { ctx: Some(ctx), primary: primary.clone(), start_ts, now_ms: now, rollback_if_not_exist: rine };
                    async move { c.check_txn_status(req).await }
                })
                .await?;
            let commit_ts = match st.action {
                0 => {
                    // owner is alive: wait (bounded by its ttl)
                    tokio::time::sleep(Duration::from_millis((20 * (attempt as u64 + 1)).min(st.ttl_ms.max(10)).min(200))).await;
                    continue;
                }
                1 => st.commit_ts,
                2 => 0,
                _ => {
                    // no lock and no record on the primary: the owner may not have prewritten it yet
                    if attempt >= 3 {
                        rine = true;
                    }
                    tokio::time::sleep(Duration::from_millis(30)).await;
                    continue;
                }
            };
            let key = lock.key.clone();
            self.router
                .exec(&lock.key, 0, 0, |mut c, ctx| {
                    let req = ResolveLockReq { ctx: Some(ctx), start_ts, commit_ts, keys: vec![key.clone()] };
                    async move { c.resolve_lock(req).await }
                })
                .await?;
            return Ok(());
        }
        Err(TxnError::Aborted("lock resolution did not converge".into()))
    }
}

pub struct Txn {
    c: Arc<TxnClient>,
    pub start_ts: u64,
    writes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    reads: BTreeSet<Vec<u8>>,
}

impl Txn {
    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        self.writes.insert(key.to_vec(), Some(value.to_vec()));
    }
    pub fn delete(&mut self, key: &[u8]) {
        self.writes.insert(key.to_vec(), None);
    }

    pub async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, TxnError> {
        if let Some(w) = self.writes.get(key) {
            return Ok(w.clone());
        }
        self.reads.insert(key.to_vec());
        let ts = self.start_ts;
        for _ in 0..60 {
            let k = key.to_vec();
            let r = self
                .c
                .router
                .exec(key, 0, 0, |mut c, ctx| {
                    let req = TxnGetReq { ctx: Some(ctx), key: k.clone(), ts };
                    async move { c.txn_get(req).await }
                })
                .await?;
            if let Some(l) = r.lock {
                self.c.resolve_lock(&l).await?;
                continue;
            }
            return Ok(if r.found { Some(r.value) } else { None });
        }
        Err(TxnError::Aborted("read blocked by locks".into()))
    }

    pub async fn scan(&mut self, start: &[u8], end: &[u8], limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>, TxnError> {
        let mut merged: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();
        let mut cur = start.to_vec();
        loop {
            let info = self.c.router.locate(&cur).await?;
            let ts = self.start_ts;
            let mut done = false;
            for _ in 0..60 {
                let (s, e) = (cur.clone(), end.to_vec());
                let r = self
                    .c
                    .router
                    .exec(&cur, 0, 0, |mut c, ctx| {
                        let req = TxnScanReq { ctx: Some(ctx), start: s.clone(), end: e.clone(), limit: 0, ts };
                        async move { c.txn_scan(req).await }
                    })
                    .await?;
                if let Some(l) = r.lock {
                    self.c.resolve_lock(&l).await?;
                    continue;
                }
                for p in r.pairs {
                    merged.insert(p.key, Some(p.value));
                }
                done = true;
                break;
            }
            if !done {
                return Err(TxnError::Aborted("scan blocked by locks".into()));
            }
            if info.end.is_empty() || (!end.is_empty() && info.end.as_slice() >= end) {
                break;
            }
            cur = info.end.clone();
        }
        for (k, v) in self.writes.range(start.to_vec()..) {
            if !end.is_empty() && k.as_slice() >= end {
                break;
            }
            merged.insert(k.clone(), v.clone());
        }
        let mut out: Vec<(Vec<u8>, Vec<u8>)> = merged.into_iter().filter_map(|(k, v)| v.map(|v| (k, v))).collect();
        for (k, _) in &out {
            self.reads.insert(k.clone());
        }
        if limit > 0 {
            out.truncate(limit);
        }
        Ok(out)
    }

    pub async fn rollback(self) -> Result<(), TxnError> {
        self.rollback_keys(&self.writes.keys().cloned().collect::<Vec<_>>()).await
    }

    async fn rollback_keys(&self, keys: &[Vec<u8>]) -> Result<(), TxnError> {
        for (k, group) in self.group_keys(keys).await? {
            let _ = k;
            let st = self.start_ts;
            let g = group.clone();
            let r = self
                .c
                .router
                .exec_ff(&group[0], |mut c, ctx| {
                    let req = RollbackReq { ctx: Some(ctx), keys: g.clone(), start_ts: st };
                    async move { c.rollback(req).await }
                })
                .await;
            if let Err(ClientError::RangeMoved) = r {
                // regroup key by key (rare)
                for key in group {
                    let st = self.start_ts;
                    let kk = vec![key.clone()];
                    let _ = self
                        .c
                        .router
                        .exec(&key, 0, 0, |mut c, ctx| {
                            let req = RollbackReq { ctx: Some(ctx), keys: kk.clone(), start_ts: st };
                            async move { c.rollback(req).await }
                        })
                        .await;
                }
            }
        }
        Ok(())
    }

    /// Group keys by the range that currently owns them: [(range_id, keys)].
    async fn group_keys(&self, keys: &[Vec<u8>]) -> Result<Vec<(u64, Vec<Vec<u8>>)>, ClientError> {
        let mut groups: BTreeMap<u64, Vec<Vec<u8>>> = BTreeMap::new();
        for k in keys {
            let info = self.c.router.locate(k).await?;
            groups.entry(info.id).or_default().push(k.clone());
        }
        Ok(groups.into_iter().collect())
    }

    /// Two-phase commit. Returns the commit timestamp.
    pub async fn commit(mut self) -> Result<u64, TxnError> {
        let serializable = self.c.iso == IsolationLevel::Serializable;
        // keys read but not written become lock-only mutations under serializable isolation
        let mut muts: BTreeMap<Vec<u8>, (u32, Vec<u8>)> = self
            .writes
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    match v {
                        Some(v) => (1u32, v.clone()),
                        None => (2u32, vec![]),
                    },
                )
            })
            .collect();
        if serializable {
            for k in std::mem::take(&mut self.reads) {
                muts.entry(k).or_insert((3, vec![]));
            }
        }
        if self.writes.is_empty() {
            return Ok(self.start_ts); // read-only: nothing to commit
        }
        let primary = self.writes.keys().next().unwrap().clone();
        let keys: Vec<Vec<u8>> = muts.keys().cloned().collect();

        // ---- phase 1: prewrite (retry regrouping if a split races with us)
        let mut attempt = 0;
        'outer: loop {
            attempt += 1;
            if attempt > 8 {
                let _ = self.rollback_keys(&keys).await;
                return Err(TxnError::Aborted("prewrite could not settle on a stable range grouping".into()));
            }
            let groups = self.group_keys(&keys).await?;
            let mut futs = vec![];
            for (_, g) in groups {
                let mutations: Vec<Mutation> = g.iter().map(|k| Mutation { op: muts[k].0, key: k.clone(), value: muts[k].1.clone() }).collect();
                let c = self.c.clone();
                let (primary, st, ttl) = (primary.clone(), self.start_ts, self.c.lock_ttl_ms);
                futs.push(tokio::spawn(async move {
                    let first = g[0].clone();
                    loop {
                        let r = c
                            .router
                            .exec_ff(&first, |mut cl, ctx| {
                                let req = PrewriteReq {
                                    ctx: Some(ctx),
                                    mutations: mutations.clone(),
                                    primary: primary.clone(),
                                    start_ts: st,
                                    ttl_ms: ttl,
                                    read_keys: vec![],
                                    now_ms: 0,
                                };
                                async move { cl.prewrite(req).await }
                            })
                            .await;
                        match r {
                            Ok(resp) if resp.errors.is_empty() => return Ok(()),
                            Ok(resp) => {
                                // locked by someone else: resolve and retry; write conflict: abort
                                let mut conflict = None;
                                for e in &resp.errors {
                                    if let Some(l) = &e.locked {
                                        if l.start_ts != st {
                                            c.resolve_lock(l).await?;
                                            continue;
                                        }
                                    }
                                    if e.write_conflict || e.locked.is_none() {
                                        conflict = Some(e.key.clone());
                                    }
                                }
                                if let Some(k) = conflict {
                                    return Err(TxnError::Conflict { key: k });
                                }
                            }
                            Err(e) => return Err(TxnError::Client(e)),
                        }
                    }
                }));
            }
            let mut result = Ok(());
            let mut moved = false;
            for f in futs {
                match f.await {
                    Ok(Ok(())) => {}
                    Ok(Err(TxnError::Client(ClientError::RangeMoved))) => moved = true,
                    Ok(Err(e)) => result = Err(e),
                    Err(e) => result = Err(TxnError::Aborted(format!("task failed: {e}"))),
                }
            }
            if let Err(e) = result {
                let _ = self.rollback_keys(&keys).await;
                return Err(e);
            }
            if moved {
                continue 'outer; // re-group and prewrite again (idempotent)
            }
            break;
        }

        // ---- phase 2: commit. The commit point is the primary's range applying Commit.
        let commit_ts = self.c.pd.tso_fresh().await.map_err(TxnError::Unknown)?;
        let groups = self.group_keys(&keys).await?;
        let primary_info = self.c.router.locate(&primary).await?;
        let mut secondaries = vec![];
        let mut primary_keys = vec![];
        for (rid, g) in groups {
            if rid == primary_info.id {
                primary_keys = g;
            } else {
                secondaries.push(g);
            }
        }
        if primary_keys.is_empty() {
            primary_keys = vec![primary.clone()];
        }
        let mut committed = false;
        for _ in 0..6 {
            let (pk, st) = (primary_keys.clone(), self.start_ts);
            let r = self
                .c
                .router
                .exec_ff(&primary_keys[0], |mut c, ctx| {
                    let req = CommitReq { ctx: Some(ctx), keys: pk.clone(), start_ts: st, commit_ts };
                    async move { c.commit(req).await }
                })
                .await;
            match r {
                Ok(resp) => {
                    if let Some(ke) = resp.key_error {
                        let _ = ke;
                        return Err(TxnError::Aborted("primary lock lost (txn rolled back by a resolver)".into()));
                    }
                    committed = true;
                    break;
                }
                Err(ClientError::RangeMoved) => {
                    // the primary's keys may now be split across ranges: commit them one at a time
                    let mut ok = true;
                    for k in primary_keys.clone() {
                        let st = self.start_ts;
                        let kk = vec![k.clone()];
                        let res = self
                            .c
                            .router
                            .exec(&k, 0, 0, |mut c, ctx| {
                                let req = CommitReq { ctx: Some(ctx), keys: kk.clone(), start_ts: st, commit_ts };
                                async move { c.commit(req).await }
                            })
                            .await;
                        match res {
                            Ok(resp) if resp.key_error.is_none() => {}
                            _ => ok = false,
                        }
                    }
                    if ok {
                        committed = true;
                        break;
                    }
                }
                Err(_) => {
                    // outcome unknown: ask the primary
                    let now = self.c.now_ms().await.unwrap_or(0);
                    let (p, st) = (primary.clone(), self.start_ts);
                    if let Ok(s) = self
                        .c
                        .router
                        .exec(&primary, 0, 0, |mut c, ctx| {
                            let req = CheckTxnStatusReq { ctx: Some(ctx), primary: p.clone(), start_ts: st, now_ms: now, rollback_if_not_exist: false };
                            async move { c.check_txn_status(req).await }
                        })
                        .await
                    {
                        match s.action {
                            1 => {
                                committed = true;
                                break;
                            }
                            2 => return Err(TxnError::Aborted("rolled back".into())),
                            _ => {}
                        }
                    }
                }
            }
        }
        if !committed {
            return Err(TxnError::Unknown("could not determine commit outcome of the primary".into()));
        }
        // secondaries: not part of the commit point; failures are repaired by lock resolution
        let c = self.c.clone();
        let st = self.start_ts;
        let sec_task = async move {
            for g in secondaries {
                let first = g[0].clone();
                let gg = g.clone();
                let r = c
                    .router
                    .exec_ff(&first, |mut cl, ctx| {
                        let req = CommitReq { ctx: Some(ctx), keys: gg.clone(), start_ts: st, commit_ts };
                        async move { cl.commit(req).await }
                    })
                    .await;
                if let Err(ClientError::RangeMoved) = r {
                    for k in g {
                        let kk = vec![k.clone()];
                        let _ = c
                            .router
                            .exec(&k, 0, 0, |mut cl, ctx| {
                                let req = CommitReq { ctx: Some(ctx), keys: kk.clone(), start_ts: st, commit_ts };
                                async move { cl.commit(req).await }
                            })
                            .await;
                    }
                }
            }
        };
        if self.c.async_secondaries {
            tokio::spawn(sec_task);
        } else {
            sec_task.await;
        }
        Ok(commit_ts)
    }
}
