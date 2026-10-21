//! Raw KV client: linearizable single-key operations with exactly-once writes.
use crate::router::{ClientError, Router};
use crate::{now_ms, PdClient};
use rkv_proto::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub struct RawClient {
    pub router: Arc<Router>,
    client_id: u64,
    seq: AtomicU64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadConsistency {
    Linearizable,
    Lease,
    /// served by any replica whose last contact with the leader is within the bound
    Stale { max_staleness_ms: u64 },
}

impl ReadConsistency {
    fn parts(self) -> (u32, u64) {
        match self {
            ReadConsistency::Linearizable => (0, 0),
            ReadConsistency::Lease => (1, 0),
            ReadConsistency::Stale { max_staleness_ms } => (2, max_staleness_ms),
        }
    }
}

impl RawClient {
    pub fn new(pd: Option<Arc<PdClient>>, seeds: Vec<String>) -> RawClient {
        let router = Router::new(pd, seeds);
        // unique per client instance: sessions are keyed by it
        let client_id = (rand::random::<u64>() | 1) & 0x7FFF_FFFF_FFFF_FFFF;
        RawClient { router, client_id, seq: AtomicU64::new(1) }
    }
    pub fn with_router(router: Arc<Router>) -> RawClient {
        RawClient { router, client_id: (rand::random::<u64>() | 1) & 0x7FFF_FFFF_FFFF_FFFF, seq: AtomicU64::new(1) }
    }
    pub fn client_id(&self) -> u64 {
        self.client_id
    }
    fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::SeqCst)
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ClientError> {
        self.get_with(key, ReadConsistency::Linearizable).await
    }

    pub async fn get_with(&self, key: &[u8], rc: ReadConsistency) -> Result<Option<Vec<u8>>, ClientError> {
        let (mode, stale) = rc.parts();
        let k = key.to_vec();
        let r = self.router.exec(key, mode, stale, |mut c, ctx| {
            let req = GetReq { ctx: Some(ctx), key: k.clone() };
            async move { c.get(req).await }
        }).await?;
        Ok(if r.found { Some(r.value) } else { None })
    }

    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), ClientError> {
        self.put_ttl(key, value, 0).await
    }

    pub async fn put_ttl(&self, key: &[u8], value: &[u8], ttl_ms: u64) -> Result<(), ClientError> {
        let (cid, seq) = (self.client_id, self.next_seq());
        let (k, v) = (key.to_vec(), value.to_vec());
        self.router.exec(key, 0, 0, |mut c, ctx| {
            let req = PutReq { ctx: Some(ctx), key: k.clone(), value: v.clone(), client_id: cid, seq, ttl_ms };
            async move { c.put(req).await }
        }).await?;
        Ok(())
    }

    pub async fn delete(&self, key: &[u8]) -> Result<(), ClientError> {
        let (cid, seq) = (self.client_id, self.next_seq());
        let k = key.to_vec();
        self.router.exec(key, 0, 0, |mut c, ctx| {
            let req = DeleteReq { ctx: Some(ctx), key: k.clone(), client_id: cid, seq };
            async move { c.delete(req).await }
        }).await?;
        Ok(())
    }

    /// Compare-and-swap; returns whether the swap happened.
    pub async fn cas(&self, key: &[u8], expect: Option<&[u8]>, new: &[u8]) -> Result<bool, ClientError> {
        let (cid, seq) = (self.client_id, self.next_seq());
        let (k, n, e) = (key.to_vec(), new.to_vec(), expect.map(|e| e.to_vec()));
        let r = self.router.exec(key, 0, 0, |mut c, ctx| {
            let req = CasReq { ctx: Some(ctx), key: k.clone(), has_expect: e.is_some(), expect: e.clone().unwrap_or_default(), new_value: n.clone(), client_id: cid, seq };
            async move { c.cas(req).await }
        }).await?;
        Ok(r.success)
    }

    /// Scan [start, end) across ranges; `limit` 0 = unlimited.
    pub async fn scan(&self, start: &[u8], end: &[u8], limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ClientError> {
        let mut out = vec![];
        let mut cur = start.to_vec();
        loop {
            let info = self.router.locate(&cur).await?;
            let (s, e) = (cur.clone(), end.to_vec());
            let remaining = if limit == 0 { 0 } else { (limit - out.len()) as u32 };
            let r = self.router.exec(&cur, 0, 0, |mut c, ctx| {
                let req = ScanReq { ctx: Some(ctx), start: s.clone(), end: e.clone(), limit: remaining };
                async move { c.scan(req).await }
            }).await?;
            out.extend(r.pairs.into_iter().map(|p| (p.key, p.value)));
            if limit > 0 && out.len() >= limit {
                out.truncate(limit);
                return Ok(out);
            }
            if info.end.is_empty() || (!end.is_empty() && info.end.as_slice() >= end) {
                return Ok(out);
            }
            cur = info.end.clone();
        }
    }

    pub fn now_ms() -> u64 {
        now_ms()
    }
}
