//! gRPC services: Kv (raw + transactional), RaftTransport ingress, Admin.
use crate::cmd::{Cmd, Mut, Resp};
use crate::sm::RangeDesc;
use crate::store::{now_ms, Replica, Store};
use raft::Role;
use rkv_proto::admin_server::Admin;
use rkv_proto::kv_server::Kv;
use rkv_proto::raft_transport_server::RaftTransport;
use rkv_proto::*;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use txn::percolator::{KeyError, MutOp};

pub struct KvSvc(pub Arc<Store>);
pub struct RaftSvc(pub Arc<Store>);
pub struct AdminSvc(pub Arc<Store>);

type Res<T> = Result<Response<T>, Status>;

fn e(code: u32, msg: &str) -> Error {
    Error { code, msg: msg.into(), leader: None, ranges: vec![] }
}

fn lock_info(key: &[u8], l: &txn::percolator::Lock) -> LockInfo {
    LockInfo { key: key.to_vec(), primary: l.primary.clone(), start_ts: l.start_ts, ttl_ms: l.ttl_ms }
}

fn key_error(k: &KeyError) -> rkv_proto::KeyError {
    let mut out = rkv_proto::KeyError::default();
    match k {
        KeyError::Locked { key, lock } => {
            out.key = key.clone();
            out.locked = Some(lock_info(key, lock));
        }
        KeyError::WriteConflict { key, conflict_ts } => {
            out.key = key.clone();
            out.write_conflict = true;
            out.conflict_ts = *conflict_ts;
        }
        KeyError::TxnNotFound { key } => {
            out.key = key.clone();
            out.other = "txn not found (rolled back)".into();
        }
        KeyError::AlreadyCommitted { key, commit_ts } => {
            out.key = key.clone();
            out.other = format!("already committed at {commit_ts}");
            out.conflict_ts = *commit_ts;
        }
    }
    out
}

impl Store {
    /// Route + epoch check. Returns the replica serving `key`.
    async fn route(&self, ctx: &Option<Context>, key: &[u8]) -> Result<Arc<Replica>, Error> {
        self.metrics.requests.fetch_add(1, Ordering::Relaxed);
        let Some(rep) = self.find(key) else {
            let ranges = self.all_replicas().iter().map(|r| self.range_info_with(r, None)).collect();
            return Err(Error { code: 4, msg: "no range for key on this store".into(), leader: None, ranges });
        };
        if let Some(c) = ctx {
            let d = rep.sm.desc();
            if (c.range_id != 0 && c.range_id != d.id) || (c.version != 0 && c.version != d.version) {
                let info = self.range_info(&rep).await;
                return Err(Error { code: 2, msg: "range moved".into(), leader: None, ranges: vec![info] });
            }
        }
        Ok(rep)
    }

    async fn range_moved(&self, rep: &Replica) -> Error {
        let info = self.range_info(rep).await;
        Error { code: 2, msg: "range moved".into(), leader: None, ranges: vec![info] }
    }

    /// Apply the read-consistency policy; on success the replica's state machine is safe to read.
    async fn read_gate(&self, rep: &Arc<Replica>, ctx: &Option<Context>) -> Result<(), Error> {
        rep.reads.fetch_add(1, Ordering::Relaxed);
        let (mode, stale) = ctx.as_ref().map(|c| (c.read_mode, c.max_staleness_ms)).unwrap_or((0, 0));
        if mode == 2 {
            let st = rep.handle.status().await.ok_or(e(5, "stopped"))?;
            if st.role == Role::Leader {
                return Ok(());
            }
            let last = rep.handle.last_contact_ms.load(Ordering::Relaxed);
            if last > 0 && now_ms().saturating_sub(last) <= stale {
                return Ok(());
            }
            return Err(Error { code: 1, msg: "replica too stale".into(), leader: self.leader_peer(st.leader), ranges: vec![] });
        }
        match tokio::time::timeout(Duration::from_secs(3), rep.handle.read_barrier()).await {
            Err(_) => Err(e(5, "read barrier timed out")),
            Ok(Err(err)) => Err(self.driver_err(err)),
            Ok(Ok(_)) => Ok(()),
        }
    }

    async fn write(&self, ctx: &Option<Context>, key: &[u8], cmd: Cmd) -> Result<Resp, Error> {
        let rep = self.route(ctx, key).await?;
        match self.propose(&rep, cmd).await? {
            Resp::RangeMoved => Err(self.range_moved(&rep).await),
            Resp::Failed(m) => Err(e(6, &m)),
            Resp::Busy => Err(e(3, "range is frozen for a merge; retry")),
            r => Ok(r),
        }
    }
}

fn werr(error: Error) -> WriteResp {
    WriteResp { error: Some(error) }
}

#[tonic::async_trait]
impl Kv for KvSvc {
    async fn get(&self, req: Request<GetReq>) -> Res<GetResp> {
        let r = req.into_inner();
        let s = &self.0;
        let out = async {
            let rep = s.route(&r.ctx, &r.key).await?;
            s.read_gate(&rep, &r.ctx).await?;
            if !rep.sm.desc().contains(&r.key) {
                return Err(s.range_moved(&rep).await);
            }
            Ok(rep.sm.read_raw(&r.key, now_ms()))
        }
        .await;
        Ok(Response::new(match out {
            Ok(Some(v)) => GetResp { error: None, found: true, value: v },
            Ok(None) => GetResp { error: None, found: false, value: vec![] },
            Err(er) => GetResp { error: Some(er), found: false, value: vec![] },
        }))
    }

    async fn put(&self, req: Request<PutReq>) -> Res<WriteResp> {
        let r = req.into_inner();
        let expire = if r.ttl_ms > 0 { now_ms() + r.ttl_ms } else { 0 };
        let cmd = Cmd::RawPut { key: r.key.clone(), value: r.value, expire_ms: expire, client: r.client_id, seq: r.seq };
        Ok(Response::new(match self.0.write(&r.ctx, &r.key, cmd).await {
            Ok(_) => WriteResp { error: None },
            Err(er) => werr(er),
        }))
    }

    async fn delete(&self, req: Request<DeleteReq>) -> Res<WriteResp> {
        let r = req.into_inner();
        let cmd = Cmd::RawDelete { key: r.key.clone(), client: r.client_id, seq: r.seq };
        Ok(Response::new(match self.0.write(&r.ctx, &r.key, cmd).await {
            Ok(_) => WriteResp { error: None },
            Err(er) => werr(er),
        }))
    }

    async fn cas(&self, req: Request<CasReq>) -> Res<CasResp> {
        let r = req.into_inner();
        let now = now_ms();
        let cmd = Cmd::RawCas {
            key: r.key.clone(),
            expect: if r.has_expect { Some(r.expect) } else { None },
            new: r.new_value,
            client: r.client_id,
            seq: r.seq,
            expire_ms: if r.ttl_ms > 0 { now + r.ttl_ms } else { 0 },
            now_ms: now,
        };
        Ok(Response::new(match self.0.write(&r.ctx, &r.key, cmd).await {
            Ok(Resp::Cas(b)) => CasResp { error: None, success: b },
            Ok(_) => CasResp { error: Some(e(6, "unexpected response")), success: false },
            Err(er) => CasResp { error: Some(er), success: false },
        }))
    }

    async fn scan(&self, req: Request<ScanReq>) -> Res<ScanResp> {
        let r = req.into_inner();
        let s = &self.0;
        let out = async {
            let rep = s.route(&r.ctx, &r.start).await?;
            s.read_gate(&rep, &r.ctx).await?;
            Ok::<_, Error>(rep.sm.scan_raw(&r.start, &r.end, r.limit as usize, now_ms()))
        }
        .await;
        Ok(Response::new(match out {
            Ok(v) => ScanResp { error: None, pairs: v.into_iter().map(|(key, value)| Pair { key, value }).collect() },
            Err(er) => ScanResp { error: Some(er), pairs: vec![] },
        }))
    }

    async fn txn_get(&self, req: Request<TxnGetReq>) -> Res<TxnGetResp> {
        let r = req.into_inner();
        let s = &self.0;
        let out = async {
            let rep = s.route(&r.ctx, &r.key).await?;
            s.read_gate(&rep, &r.ctx).await?;
            Ok::<_, Error>(rep.sm.txn_get(&r.key, r.ts))
        }
        .await;
        Ok(Response::new(match out {
            Ok(Ok(Some(v))) => TxnGetResp { error: None, found: true, value: v, lock: None },
            Ok(Ok(None)) => TxnGetResp { error: None, found: false, value: vec![], lock: None },
            Ok(Err(l)) => TxnGetResp { error: None, found: false, value: vec![], lock: Some(lock_info(&r.key, &l)) },
            Err(er) => TxnGetResp { error: Some(er), found: false, value: vec![], lock: None },
        }))
    }

    async fn txn_scan(&self, req: Request<TxnScanReq>) -> Res<TxnScanResp> {
        let r = req.into_inner();
        let s = &self.0;
        let out = async {
            let rep = s.route(&r.ctx, &r.start).await?;
            s.read_gate(&rep, &r.ctx).await?;
            Ok::<_, Error>(rep.sm.txn_scan(&r.start, &r.end, r.limit as usize, r.ts))
        }
        .await;
        Ok(Response::new(match out {
            Ok(Ok(v)) => TxnScanResp { error: None, pairs: v.into_iter().map(|(key, value)| Pair { key, value }).collect(), lock: None },
            Ok(Err((k, l))) => TxnScanResp { error: None, pairs: vec![], lock: Some(lock_info(&k, &l)) },
            Err(er) => TxnScanResp { error: Some(er), pairs: vec![], lock: None },
        }))
    }

    async fn prewrite(&self, req: Request<PrewriteReq>) -> Res<PrewriteResp> {
        let r = req.into_inner();
        let Some(first) = r.mutations.first().map(|m| m.key.clone()) else { return Ok(Response::new(PrewriteResp { error: None, errors: vec![] })) };
        let muts = r
            .mutations
            .iter()
            .map(|m| Mut {
                op: match m.op {
                    1 => MutOp::Put,
                    2 => MutOp::Delete,
                    _ => MutOp::Lock,
                },
                key: m.key.clone(),
                value: m.value.clone(),
            })
            .collect();
        let cmd = Cmd::Prewrite { muts, primary: r.primary, start_ts: r.start_ts, ttl_ms: r.ttl_ms, read_keys: r.read_keys };
        Ok(Response::new(match self.0.write(&r.ctx, &first, cmd).await {
            Ok(Resp::Ok) => PrewriteResp { error: None, errors: vec![] },
            Ok(Resp::KeyErrors(es)) => PrewriteResp { error: None, errors: es.iter().map(key_error).collect() },
            Ok(_) => PrewriteResp { error: Some(e(6, "unexpected response")), errors: vec![] },
            Err(er) => PrewriteResp { error: Some(er), errors: vec![] },
        }))
    }

    async fn commit(&self, req: Request<CommitReq>) -> Res<CommitResp> {
        let r = req.into_inner();
        let Some(first) = r.keys.first().cloned() else { return Ok(Response::new(CommitResp { error: None, key_error: None })) };
        let cmd = Cmd::Commit { keys: r.keys, start_ts: r.start_ts, commit_ts: r.commit_ts };
        Ok(Response::new(match self.0.write(&r.ctx, &first, cmd).await {
            Ok(Resp::Ok) => CommitResp { error: None, key_error: None },
            Ok(Resp::KeyErrors(es)) => CommitResp { error: None, key_error: es.first().map(key_error) },
            Ok(_) => CommitResp { error: Some(e(6, "unexpected response")), key_error: None },
            Err(er) => CommitResp { error: Some(er), key_error: None },
        }))
    }

    async fn rollback(&self, req: Request<RollbackReq>) -> Res<WriteResp> {
        let r = req.into_inner();
        let Some(first) = r.keys.first().cloned() else { return Ok(Response::new(WriteResp { error: None })) };
        Ok(Response::new(match self.0.write(&r.ctx, &first, Cmd::Rollback { keys: r.keys, start_ts: r.start_ts }).await {
            Ok(Resp::KeyErrors(_)) | Ok(_) => WriteResp { error: None },
            Err(er) => werr(er),
        }))
    }

    async fn check_txn_status(&self, req: Request<CheckTxnStatusReq>) -> Res<CheckTxnStatusResp> {
        let r = req.into_inner();
        let cmd = Cmd::CheckTxnStatus { primary: r.primary.clone(), start_ts: r.start_ts, now_ms: r.now_ms, rollback_if_not_exist: r.rollback_if_not_exist };
        Ok(Response::new(match self.0.write(&r.ctx, &r.primary, cmd).await {
            Ok(Resp::TxnStatus { action, commit_ts, ttl_ms }) => CheckTxnStatusResp { error: None, action, commit_ts, ttl_ms },
            Ok(_) => CheckTxnStatusResp { error: Some(e(6, "unexpected response")), action: 0, commit_ts: 0, ttl_ms: 0 },
            Err(er) => CheckTxnStatusResp { error: Some(er), action: 0, commit_ts: 0, ttl_ms: 0 },
        }))
    }

    async fn resolve_lock(&self, req: Request<ResolveLockReq>) -> Res<WriteResp> {
        let r = req.into_inner();
        let Some(first) = r.keys.first().cloned() else { return Ok(Response::new(WriteResp { error: None })) };
        Ok(Response::new(match self.0.write(&r.ctx, &first, Cmd::ResolveLock { keys: r.keys, start_ts: r.start_ts, commit_ts: r.commit_ts }).await {
            Ok(_) => WriteResp { error: None },
            Err(er) => werr(er),
        }))
    }

    type WatchStream = Pin<Box<dyn tokio_stream::Stream<Item = Result<Event, Status>> + Send>>;

    async fn watch(&self, req: Request<WatchReq>) -> Res<Self::WatchStream> {
        let r = req.into_inner();
        let rep = self.0.route(&r.ctx, &r.start).await.map_err(|er| Status::failed_precondition(er.msg))?;
        let mut rx = rep.sm.events.subscribe();
        let (tx, out) = tokio::sync::mpsc::channel(1024);
        let (start, end) = (r.start, r.end);
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(ev) =>
                    {
                        #[allow(clippy::collapsible_if)]
                        if ev.key.as_slice() >= start.as_slice() && (end.is_empty() || ev.key.as_slice() < end.as_slice()) {
                            if tx.send(Ok(Event { index: ev.index, key: ev.key.clone(), value: ev.value.clone(), deleted: ev.deleted })).await.is_err() {
                                return;
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        let _ = tx.send(Err(Status::data_loss("watcher lagged; resync required"))).await;
                        return;
                    }
                    Err(_) => return,
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(out))))
    }
}

#[tonic::async_trait]
impl RaftTransport for RaftSvc {
    async fn send(&self, req: Request<RaftBatch>) -> Res<Empty> {
        for m in req.into_inner().msgs {
            if self.0.transport.faults.should_drop(m.from_store) {
                continue; // injected partition: incoming direction
            }
            self.0.deliver(m.range_id, m.from_store, &m.data);
        }
        Ok(Response::new(Empty {}))
    }
}

fn aerr(code: u32, msg: &str) -> AdminResp {
    AdminResp { error: Some(e(code, msg)), msg: msg.into() }
}
fn aok(msg: &str) -> AdminResp {
    AdminResp { error: None, msg: msg.into() }
}

#[tonic::async_trait]
impl Admin for AdminSvc {
    async fn status(&self, _: Request<Empty>) -> Res<StatusResp> {
        let s = &self.0;
        let mut ranges = vec![];
        for rep in s.all_replicas() {
            let Some(st) = rep.handle.status().await else { continue };
            ranges.push(RangeStatus {
                info: Some(s.range_info_with(&rep, Some(&st))),
                term: st.term,
                commit: st.commit,
                applied: st.applied,
                is_leader: st.role == Role::Leader,
                approx_bytes: rep.sm.approx_bytes.load(Ordering::Relaxed),
                write_qps: 0,
                read_qps: 0,
            });
        }
        Ok(Response::new(StatusResp { store_id: s.cfg.store_id, addr: s.cfg.addr.clone(), ranges }))
    }

    async fn transfer_leader(&self, req: Request<TransferLeaderReq>) -> Res<AdminResp> {
        let r = req.into_inner();
        let Some(rep) = self.0.replica(r.range_id) else { return Ok(Response::new(aerr(4, "no such range here"))) };
        Ok(Response::new(match rep.handle.transfer_leader(r.to_store).await {
            Ok(()) => aok("transfer started"),
            Err(er) => AdminResp { error: Some(self.0.driver_err(er)), msg: "transfer failed".into() },
        }))
    }

    async fn change_membership(&self, req: Request<MembershipReq>) -> Res<AdminResp> {
        let r = req.into_inner();
        let Some(rep) = self.0.replica(r.range_id) else { return Ok(Response::new(aerr(4, "no such range here"))) };
        let voters = r.voters.iter().copied().collect();
        let learners = r.learners.iter().copied().collect();
        // make sure new members have a replica to receive the log
        if let Some(st) = rep.handle.status().await {
            let info = self.0.range_info_with(&rep, Some(&st));
            for n in r.voters.iter().chain(r.learners.iter()) {
                if !st.membership.contains(*n) {
                    let addr = self.0.addr_of(*n);
                    if addr.is_empty() {
                        return Ok(Response::new(aerr(6, &format!("unknown address for store {n}"))));
                    }
                    if let Ok(ch) = ::kv_client::pd::channel(&addr) {
                        let mut ai = info.clone();
                        ai.peers.push(Peer { store_id: *n, addr, learner: true });
                        let _ = rkv_proto::admin_client::AdminClient::new(ch).create_replica(CreateReplicaReq { range_id: r.range_id, info: Some(ai) }).await;
                    }
                }
            }
        }
        Ok(Response::new(match rep.handle.change_membership(voters, learners).await {
            Ok(()) => aok("membership change proposed"),
            Err(er) => AdminResp { error: Some(self.0.driver_err(er)), msg: "membership change failed".into() },
        }))
    }

    async fn split_range(&self, req: Request<SplitReq>) -> Res<AdminResp> {
        let r = req.into_inner();
        let Some(rep) = self.0.replica(r.range_id) else { return Ok(Response::new(aerr(4, "no such range here"))) };
        Ok(Response::new(match self.0.split_range(&rep, if r.split_key.is_empty() { None } else { Some(r.split_key) }).await {
            Ok(()) => aok("split applied"),
            Err(m) => aerr(6, &m),
        }))
    }

    async fn merge_range(&self, req: Request<MergeReq>) -> Res<MergeResp> {
        let r = req.into_inner();
        let s = &self.0;
        let fail = |er: Error| Ok(Response::new(MergeResp { error: Some(er), msg: String::new(), final_index: 0, right_end: vec![] }));
        let (Some(left), Some(right)) = (s.replica(r.left_id), s.replica(r.right_id)) else {
            return fail(e(4, "this store does not host both ranges"));
        };
        match r.phase {
            // phase 1: freeze the right range (must lead it) after checking both groups have identical, stable membership
            1 => {
                let (Some(ls), Some(rs)) = (left.handle.status().await, right.handle.status().await) else { return fail(e(6, "range stopped")) };
                if ls.membership.is_joint()
                    || rs.membership.is_joint()
                    || ls.membership.voters != rs.membership.voters
                    || !ls.membership.learners.is_empty()
                    || !rs.membership.learners.is_empty()
                {
                    return fail(e(6, "ranges must have identical, stable voter sets to merge (move replicas first)"));
                }
                if left.sm.desc().end != right.sm.desc().start {
                    return fail(e(6, "ranges are not adjacent"));
                }
                match s.propose(&right, Cmd::PrepareMerge).await {
                    Ok(Resp::MergeReady { final_index, end }) => {
                        Ok(Response::new(MergeResp { error: None, msg: "right range frozen".into(), final_index, right_end: end }))
                    }
                    Ok(other) => fail(e(6, &format!("unexpected response {other:?}"))),
                    Err(er) => fail(er),
                }
            }
            // phase 2: extend the left range (must lead it)
            2 => match s.propose(&left, Cmd::CommitMerge { right_id: r.right_id, right_end: r.right_end.clone(), final_index: r.final_index }).await {
                Ok(Resp::Ok) => Ok(Response::new(MergeResp { error: None, msg: "merged".into(), final_index: r.final_index, right_end: r.right_end })),
                Ok(Resp::Failed(m)) => fail(e(6, &m)),
                Ok(other) => fail(e(6, &format!("unexpected response {other:?}"))),
                Err(er) => fail(er),
            },
            // phase 3: abort, unfreeze the right range
            _ => match s.propose(&right, Cmd::AbortMerge).await {
                Ok(_) => Ok(Response::new(MergeResp { error: None, msg: "merge aborted".into(), final_index: 0, right_end: vec![] })),
                Err(er) => fail(er),
            },
        }
    }

    async fn set_faults(&self, req: Request<FaultsReq>) -> Res<AdminResp> {
        let r = req.into_inner();
        let f = &self.0.transport.faults;
        *f.blocked.write() = r.blocked_stores.into_iter().collect();
        f.drop_pct.store(r.drop_pct as u64, Ordering::Relaxed);
        f.delay_ms.store(r.delay_ms as u64, Ordering::Relaxed);
        self.0.cfg.group.clock_rate.store(if r.clock_rate_pct == 0 { 100 } else { r.clock_rate_pct as u64 }, Ordering::Relaxed);
        Ok(Response::new(aok("faults updated")))
    }

    async fn create_replica(&self, req: Request<CreateReplicaReq>) -> Res<AdminResp> {
        let r = req.into_inner();
        let Some(info) = r.info else { return Ok(Response::new(aerr(6, "missing info"))) };
        Ok(Response::new(match self.0.create_replica_from_info(&info) {
            Ok(_) => aok("replica created"),
            Err(er) => aerr(6, &er.to_string()),
        }))
    }
}

pub fn _unused(_: RangeDesc) {}
