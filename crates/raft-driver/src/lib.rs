//! Async runtime shell around the sans-IO `raft` crate: one tokio task per Raft
//! group. It owns the persist-before-send contract, applies committed entries to
//! a `StateMachine`, resolves proposals and linearizable reads, and takes snapshots.
use kv_engine::Vfs;
use raft::{Config, EntryData, Membership, Message, NodeId, ProposeError, Raft, Role};
use raft_storage::RaftStorage;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

pub trait StateMachine: Send + Sync + 'static {
    /// Apply a batch of committed entries atomically together with the new
    /// applied index. `data` is None for noop/conf entries. Returns one result per entry.
    fn apply_batch(&self, entries: &[(u64, u64, Option<Vec<u8>>)]) -> Vec<Vec<u8>>;
    /// Last applied index durably recorded by the state machine (0 if fresh).
    fn applied_index(&self) -> u64;
    /// Serialize the state as of `applied_index()`.
    fn snapshot(&self) -> Vec<u8>;
    /// Replace all state with the snapshot, recording `index` as applied.
    fn restore(&self, index: u64, data: &[u8]);
}

pub trait Transport: Send + Sync + 'static {
    /// Best-effort, non-blocking send.
    fn send(&self, group: u64, msg: Message);
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DriverError {
    NotLeader(Option<NodeId>),
    Busy,
    ConfChangePending,
    Invalid(String),
    /// the proposal's log slot was taken by another term's entry; the outcome is unknown
    Overwritten,
    Stopped,
}

impl From<ProposeError> for DriverError {
    fn from(e: ProposeError) -> DriverError {
        match e {
            ProposeError::NotLeader(l) => DriverError::NotLeader(l),
            ProposeError::Busy => DriverError::Busy,
            ProposeError::ConfChangePending => DriverError::ConfChangePending,
            ProposeError::TransferInProgress => DriverError::Busy,
            ProposeError::InvalidConfChange(s) => DriverError::Invalid(s),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Status {
    pub id: NodeId,
    pub role: Role,
    pub term: u64,
    pub leader: Option<NodeId>,
    pub commit: u64,
    pub applied: u64,
    pub last_index: u64,
    pub snapshot_index: u64,
    pub membership: Membership,
}

enum Cmd {
    Step(Message),
    Propose(Vec<u8>, oneshot::Sender<Result<Vec<u8>, DriverError>>),
    /// linearizable read barrier: resolves with an index the state machine has already applied
    ReadBarrier(oneshot::Sender<Result<u64, DriverError>>),
    Conf { voters: BTreeSet<NodeId>, learners: BTreeSet<NodeId>, resp: oneshot::Sender<Result<(), DriverError>> },
    Transfer(NodeId, oneshot::Sender<Result<(), DriverError>>),
    Status(oneshot::Sender<Status>),
    Stop,
}

#[derive(Clone)]
pub struct GroupHandle {
    pub group: u64,
    tx: mpsc::Sender<Cmd>,
    pub proposals: Arc<AtomicU64>,
    /// wall-clock ms of the last Append/Heartbeat/Snapshot received from a leader
    pub last_contact_ms: Arc<AtomicU64>,
}

#[derive(Clone)]
pub struct GroupOptions {
    pub tick_ms: u64,
    pub election_tick: u64,
    pub heartbeat_tick: u64,
    pub pre_vote: bool,
    pub check_quorum: bool,
    pub snapshot_threshold: u64,
    pub sync: bool,
    pub inbox: usize,
    pub lease_reads: bool,
    pub max_uncommitted: u64,
    /// logical clock speed in percent (100 = nominal); chaos tests change it to emulate clock skew
    pub clock_rate: Arc<AtomicU64>,
}

impl Default for GroupOptions {
    fn default() -> Self {
        GroupOptions { tick_ms: 20, election_tick: 10, heartbeat_tick: 2, pre_vote: true, check_quorum: true, snapshot_threshold: 20_000, sync: true, inbox: 4096, lease_reads: false, max_uncommitted: 4096, clock_rate: Arc::new(AtomicU64::new(100)) }
    }
}

impl GroupHandle {
    pub fn step(&self, m: Message) {
        // inbox is bounded: raft tolerates message loss, so drop under overload
        let _ = self.tx.try_send(Cmd::Step(m));
    }
    pub async fn propose(&self, data: Vec<u8>) -> Result<Vec<u8>, DriverError> {
        let (t, r) = oneshot::channel();
        self.tx.send(Cmd::Propose(data, t)).await.map_err(|_| DriverError::Stopped)?;
        r.await.map_err(|_| DriverError::Stopped)?
    }
    pub async fn read_barrier(&self) -> Result<u64, DriverError> {
        let (t, r) = oneshot::channel();
        self.tx.send(Cmd::ReadBarrier(t)).await.map_err(|_| DriverError::Stopped)?;
        r.await.map_err(|_| DriverError::Stopped)?
    }
    pub async fn change_membership(&self, voters: BTreeSet<NodeId>, learners: BTreeSet<NodeId>) -> Result<(), DriverError> {
        let (t, r) = oneshot::channel();
        self.tx.send(Cmd::Conf { voters, learners, resp: t }).await.map_err(|_| DriverError::Stopped)?;
        r.await.map_err(|_| DriverError::Stopped)?
    }
    pub async fn transfer_leader(&self, to: NodeId) -> Result<(), DriverError> {
        let (t, r) = oneshot::channel();
        self.tx.send(Cmd::Transfer(to, t)).await.map_err(|_| DriverError::Stopped)?;
        r.await.map_err(|_| DriverError::Stopped)?
    }
    pub async fn status(&self) -> Option<Status> {
        let (t, r) = oneshot::channel();
        self.tx.send(Cmd::Status(t)).await.ok()?;
        r.await.ok()
    }
    pub async fn stop(&self) {
        let _ = self.tx.send(Cmd::Stop).await;
    }
}

struct Pending {
    term: u64,
    tx: oneshot::Sender<Result<Vec<u8>, DriverError>>,
}

/// Start a Raft group task. `initial` is the bootstrap membership (used only when the
/// group has no persisted state).
pub fn spawn_group(
    group: u64,
    id: NodeId,
    initial: Membership,
    fs: Arc<dyn Vfs>,
    dir: &str,
    sm: Arc<dyn StateMachine>,
    transport: Arc<dyn Transport>,
    opts: GroupOptions,
) -> std::io::Result<GroupHandle> {
    let (storage, mut init) = RaftStorage::open(fs, dir)?;
    // the state machine is durable: tell raft where it is. If the raft log was compacted
    // beyond the state machine (crash between snapshot write and apply), restore from the snapshot.
    let sm_applied = sm.applied_index();
    if let (Some(meta), Some(data)) = (&init.snapshot, &init.snapshot_data) {
        if sm_applied < meta.index {
            sm.restore(meta.index, data);
        }
    }
    init.applied = sm.applied_index();
    let mut cfg = Config::new(id);
    cfg.election_tick = opts.election_tick;
    cfg.heartbeat_tick = opts.heartbeat_tick;
    cfg.pre_vote = opts.pre_vote;
    cfg.check_quorum = opts.check_quorum;
    cfg.max_uncommitted = opts.max_uncommitted;
    cfg.seed = id.wrapping_mul(0x9E37) ^ group.wrapping_mul(0x85EB) ^ std::process::id() as u64;
    let raft = Raft::new(cfg, initial, init);
    let (tx, rx) = mpsc::channel(opts.inbox);
    let last_contact_ms = Arc::new(AtomicU64::new(0));
    let handle = GroupHandle { group, tx, proposals: Arc::new(AtomicU64::new(0)), last_contact_ms: last_contact_ms.clone() };
    let task = GroupTask { group, raft, storage, sm, transport, opts, pending: BTreeMap::new(), reads: BTreeMap::new(), read_waiters: vec![], next_ctx: 1, last_contact_ms };
    tokio::spawn(task.run(rx));
    Ok(handle)
}

struct GroupTask {
    group: u64,
    raft: Raft,
    storage: RaftStorage,
    sm: Arc<dyn StateMachine>,
    transport: Arc<dyn Transport>,
    opts: GroupOptions,
    pending: BTreeMap<u64, Pending>,
    reads: BTreeMap<u64, oneshot::Sender<Result<u64, DriverError>>>,
    read_waiters: Vec<(u64, oneshot::Sender<Result<u64, DriverError>>)>,
    next_ctx: u64,
    last_contact_ms: Arc<AtomicU64>,
}

impl GroupTask {
    async fn run(mut self, mut rx: mpsc::Receiver<Cmd>) {
        let mut ticker = tokio::time::interval(Duration::from_millis(self.opts.tick_ms));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut acc: u64 = 0;
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    acc += self.opts.clock_rate.load(Ordering::Relaxed);
                    while acc >= 100 {
                        acc -= 100;
                        self.raft.tick();
                    }
                }
                cmd = rx.recv() => {
                    let Some(cmd) = cmd else { break };
                    if !self.handle(cmd) { break; }
                    // drain whatever else is queued so one fsync covers a whole batch (group commit)
                    for _ in 0..1024 {
                        match rx.try_recv() {
                            Ok(c) => { if !self.handle(c) { self.fail_all(DriverError::Stopped); return; } }
                            Err(_) => break,
                        }
                    }
                }
            }
            if !self.drive() {
                tracing::error!(group = self.group, "raft driver fatal storage error; stopping group");
                break;
            }
        }
        self.fail_all(DriverError::Stopped);
    }

    fn fail_all(&mut self, e: DriverError) {
        for (_, p) in std::mem::take(&mut self.pending) {
            let _ = p.tx.send(Err(e.clone()));
        }
        for (_, r) in std::mem::take(&mut self.reads) {
            let _ = r.send(Err(e.clone()));
        }
        for (_, r) in std::mem::take(&mut self.read_waiters) {
            let _ = r.send(Err(e.clone()));
        }
    }

    fn handle(&mut self, cmd: Cmd) -> bool {
        match cmd {
            Cmd::Step(m) => {
                if matches!(m.body, raft::MsgBody::Append { .. } | raft::MsgBody::Heartbeat { .. } | raft::MsgBody::Snapshot(_)) && m.term >= self.raft.term() {
                    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
                    self.last_contact_ms.store(now, Ordering::Relaxed);
                }
                self.raft.step(m)
            }
            Cmd::Propose(data, tx) => match self.raft.propose(data) {
                Ok((term, idx)) => {
                    self.pending.insert(idx, Pending { term, tx });
                }
                Err(e) => {
                    let _ = tx.send(Err(e.into()));
                }
            },
            Cmd::ReadBarrier(tx) => {
                if self.opts.lease_reads {
                    if let Some(idx) = self.raft.lease_read() {
                        self.read_waiters.push((idx, tx));
                        return true;
                    }
                }
                let ctx = self.next_ctx;
                self.next_ctx += 1;
                match self.raft.read_index(ctx) {
                    Ok(()) => {
                        self.reads.insert(ctx, tx);
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e.into()));
                    }
                }
            }
            Cmd::Conf { voters, learners, resp } => {
                let r = self.raft.propose_conf_change(voters, learners).map(|_| ()).map_err(DriverError::from);
                let _ = resp.send(r);
            }
            Cmd::Transfer(to, resp) => {
                let _ = resp.send(self.raft.transfer_leader(to).map_err(DriverError::from));
            }
            Cmd::Status(tx) => {
                let r = &self.raft;
                let _ = tx.send(Status {
                    id: r.id(),
                    role: r.role(),
                    term: r.term(),
                    leader: r.leader(),
                    commit: r.commit_index(),
                    applied: r.applied_index(),
                    last_index: r.last_index(),
                    snapshot_index: r.snapshot_index(),
                    membership: r.membership().clone(),
                });
            }
            Cmd::Stop => return false,
        }
        true
    }

    /// Returns false on a fatal storage error.
    fn drive(&mut self) -> bool {
        let was_leader = self.raft.role() == Role::Leader;
        while self.raft.has_ready() {
            let rd = self.raft.ready();
            if self.storage.save(&rd, self.opts.sync).is_err() {
                return false; // fail-stop: the state of the log on disk is unknown
            }
            if let Some(s) = &rd.install_snapshot {
                self.sm.restore(s.meta.index, &s.data);
                for (_, p) in std::mem::take(&mut self.pending) {
                    let _ = p.tx.send(Err(DriverError::Overwritten));
                }
            }
            for m in &rd.messages {
                self.transport.send(self.group, m.clone());
            }
            if !rd.committed.is_empty() {
                let batch: Vec<(u64, u64, Option<Vec<u8>>)> = rd
                    .committed
                    .iter()
                    .map(|e| (e.index, e.term, if let EntryData::Normal(d) = &e.data { Some(d.clone()) } else { None }))
                    .collect();
                let results = self.sm.apply_batch(&batch);
                for ((idx, term, _), res) in batch.iter().zip(results) {
                    if let Some(p) = self.pending.remove(idx) {
                        let _ = p.tx.send(if p.term == *term { Ok(res) } else { Err(DriverError::Overwritten) });
                    }
                }
            }
            for rs in &rd.read_states {
                if let Some(tx) = self.reads.remove(&rs.ctx) {
                    self.read_waiters.push((rs.index, tx));
                }
            }
            self.raft.advance(&rd);
        }
        // release reads whose index has been applied
        let applied = self.raft.applied_index();
        let mut i = 0;
        while i < self.read_waiters.len() {
            if self.read_waiters[i].0 <= applied {
                let (idx, tx) = self.read_waiters.swap_remove(i);
                let _ = tx.send(Ok(idx));
            } else {
                i += 1;
            }
        }
        // lost leadership: pending proposals/reads may never resolve
        if was_leader && self.raft.role() != Role::Leader {
            let hint = self.raft.leader();
            for (_, p) in std::mem::take(&mut self.pending) {
                let _ = p.tx.send(Err(DriverError::NotLeader(hint)));
            }
            for (_, r) in std::mem::take(&mut self.reads) {
                let _ = r.send(Err(DriverError::NotLeader(hint)));
            }
            for (_, r) in std::mem::take(&mut self.read_waiters) {
                let _ = r.send(Err(DriverError::NotLeader(hint)));
            }
        }
        // log compaction
        let applied = self.raft.applied_index();
        if applied.saturating_sub(self.raft.snapshot_index()) >= self.opts.snapshot_threshold && self.sm.applied_index() == applied {
            let data = self.sm.snapshot();
            if self.raft.compact(applied, data).is_ok() {
                while self.raft.has_ready() {
                    let rd = self.raft.ready();
                    if self.storage.save(&rd, self.opts.sync).is_err() {
                        return false;
                    }
                    for m in &rd.messages {
                        self.transport.send(self.group, m.clone());
                    }
                    self.raft.advance(&rd);
                }
            }
        }
        true
    }
}

/// In-process transport for tests: routes messages between groups by (node, group).
pub mod mem {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    pub struct Net {
        pub handles: Mutex<BTreeMap<(NodeId, u64), GroupHandle>>,
        pub blocked: Mutex<BTreeSet<(NodeId, NodeId)>>,
    }

    pub struct MemTransport {
        pub net: Arc<Net>,
        pub from: NodeId,
    }

    impl Transport for MemTransport {
        fn send(&self, group: u64, msg: Message) {
            let blocked = self.net.blocked.lock().unwrap().contains(&(msg.from, msg.to));
            if blocked {
                return;
            }
            if let Some(h) = self.net.handles.lock().unwrap().get(&(msg.to, group)) {
                h.step(msg);
            }
        }
    }
}
