//! Deterministic simulation: virtual time, network and disk faults, node
//! crash/restart, simulated clients recording a linearizability history.
//! The result of a run is a pure function of (SimConfig).
use crate::model::*;
use crate::nemesis::{self, Action, Profile};
use invariants::RaftInvariants;
use kv_engine::MemFs;
use lincheck::{OpKind, OpResult, Recorder, Verdict};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use raft::{Config, EntryData, InitialState, Membership, Message, ProposeError, Raft, Role};
use raft_storage::RaftStorage;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadMode {
    ReadIndex,
    Lease,
}

/// Deliberately injected bugs, used to prove the checkers can fail.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Bug {
    /// serve reads from any node's local state without consulting Raft
    StaleReads,
    /// disable client-session dedup
    NoDedup,
    /// acknowledge writes as soon as the leader appends them, before commit
    AckEarly,
    /// do not persist the hard state (term/vote): a restarted node may vote twice
    NoPersistVote,
    /// protocol-level mutations inside the raft crate
    Raft(raft::RaftBug),
}

#[derive(Clone, Debug)]
pub struct SimConfig {
    pub seed: u64,
    pub nodes: u64,
    pub initial_voters: u64,
    pub duration_ms: u64,
    pub profile: Profile,
    pub clients: usize,
    pub keys: usize,
    pub read_mode: ReadMode,
    pub pre_vote: bool,
    pub check_quorum: bool,
    pub election_tick: u64,
    pub snapshot_threshold: u64,
    pub bug: Option<Bug>,
    pub schedule: Option<Vec<(u64, Action)>>,
    pub lin_budget: u64,
}

impl SimConfig {
    pub fn new(seed: u64, profile: Profile) -> SimConfig {
        SimConfig {
            seed,
            nodes: 5,
            initial_voters: 3,
            duration_ms: 20_000,
            profile,
            clients: 5,
            keys: 3,
            read_mode: if profile == Profile::Skew { ReadMode::Lease } else { ReadMode::ReadIndex },
            pre_vote: true,
            check_quorum: true,
            election_tick: 10,
            snapshot_threshold: 25,
            bug: None,
            schedule: None,
            lin_budget: 200_000,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct SimStats {
    pub events: u64,
    pub msgs_sent: u64,
    pub msgs_dropped: u64,
    pub msgs_duplicated: u64,
    pub leaders_elected: u64,
    pub max_term: u64,
    pub crashes: u64,
    pub restarts: u64,
    pub snapshots_installed: u64,
    pub snapshots_taken: u64,
    pub ops_total: u64,
    pub ops_completed: u64,
    pub ops_pending: u64,
    pub committed_entries: usize,
    pub invariant_checks: u64,
    pub lin_states: u64,
    pub completed_after_heal: u64,
    pub conf_changes: u64,
}

#[derive(Clone, Debug)]
pub struct SimResult {
    pub seed: u64,
    pub profile: Profile,
    pub safety_violations: Vec<String>,
    pub lin: Verdict,
    pub liveness_ok: bool,
    pub stats: SimStats,
    pub schedule: Vec<(u64, Action)>,
}

impl SimResult {
    pub fn ok(&self) -> bool {
        self.safety_violations.is_empty() && matches!(self.lin, Verdict::Linearizable | Verdict::Inconclusive { .. })
    }
}

// ---------------------------------------------------------------- events

#[derive(Clone, Debug)]
enum RespBody {
    Write(CmdResult),
    Read(Option<Vec<u8>>),
    NotLeader(Option<u64>),
    Retry,
}

#[derive(Clone, Debug)]
enum Req {
    Write(Cmd),
    Read(Vec<u8>),
}

#[derive(Clone, Debug)]
enum Ev {
    Tick { node: u64, inc: u64 },
    Deliver(Vec<u8>),
    ClientStep { c: usize },
    ClientReq { node: u64, c: usize, op: u64, attempt: u32, req: Req },
    ClientResp { c: usize, op: u64, body: RespBody },
    ClientTimeout { c: usize, op: u64, attempt: u32 },
    Nemesis(Action),
    Check,
    Heal,
}

struct PendingW {
    term: u64,
    c: usize,
    op: u64,
}

struct PendingR {
    c: usize,
    op: u64,
    key: Vec<u8>,
}

struct Node {
    fs: Arc<MemFs>,
    up: bool,
    raft: Option<Raft>,
    storage: Option<RaftStorage>,
    sm: Sm,
    applied: u64,
    pending_w: BTreeMap<u64, PendingW>,
    pending_r: BTreeMap<u64, PendingR>,
    read_wait: Vec<(u64, PendingR)>,
    rate: f64,
    inc: u64,
    last_role: Role,
    fail_syncs: u32,
}

struct CurOp {
    op: u64,
    hist_id: u64,
    seq: u64,
    kind: CKind,
    key: Vec<u8>,
    attempts: u32,
}

#[derive(Clone)]
enum CKind {
    Put(Vec<u8>),
    Del,
    Cas(Option<Vec<u8>>, Vec<u8>),
    Read,
}

struct Client {
    id: u64,
    cur: Option<CurOp>,
    seq: u64,
}

pub struct Sim {
    cfg: SimConfig,
    now: u64,
    seq: u64,
    queue: BTreeMap<(u64, u64), Ev>,
    nodes: BTreeMap<u64, Node>,
    net_rng: ChaCha8Rng,
    wl_rng: ChaCha8Rng,
    blocked: HashSet<(u64, u64)>,
    drop_p: f64,
    dup_p: f64,
    delay: (u64, u64),
    inv: RaftInvariants,
    rec: Recorder,
    clients: Vec<Client>,
    next_proc: u64,
    next_op: u64,
    next_val: u64,
    next_ctx: u64,
    stats: SimStats,
    heal_time: u64,
    extra_violations: Vec<String>,
    membership0: Membership,
}

fn key_name(i: usize) -> Vec<u8> {
    format!("k{i}").into_bytes()
}

impl Sim {
    pub fn new(cfg: SimConfig) -> Sim {
        let voters: Vec<u64> = (1..=cfg.initial_voters).collect();
        let membership0 = Membership::new(voters);
        let mut s = Sim {
            now: 0,
            seq: 0,
            queue: BTreeMap::new(),
            nodes: BTreeMap::new(),
            net_rng: ChaCha8Rng::seed_from_u64(cfg.seed ^ 0x1111),
            wl_rng: ChaCha8Rng::seed_from_u64(cfg.seed ^ 0x2222),
            blocked: HashSet::new(),
            drop_p: 0.0,
            dup_p: 0.0,
            delay: (1, 5),
            inv: RaftInvariants::new(),
            rec: Recorder::default(),
            clients: vec![],
            next_proc: 1000,
            next_op: 0,
            next_val: 1,
            next_ctx: 1,
            stats: SimStats::default(),
            heal_time: u64::MAX,
            extra_violations: vec![],
            membership0,
            cfg,
        };
        for id in 1..=s.cfg.nodes {
            s.nodes.insert(
                id,
                Node {
                    fs: MemFs::new(),
                    up: false,
                    raft: None,
                    storage: None,
                    sm: Sm::default(),
                    applied: 0,
                    pending_w: BTreeMap::new(),
                    pending_r: BTreeMap::new(),
                    read_wait: vec![],
                    rate: 1.0,
                    inc: 0,
                    last_role: Role::Follower,
                    fail_syncs: 0,
                },
            );
            s.start_node(id);
        }
        for c in 0..s.cfg.clients {
            let id = s.next_proc;
            s.next_proc += 1;
            s.clients.push(Client { id, cur: None, seq: 0 });
            let t = 200 + s.wl_rng.gen_range(0..100);
            s.at(t, Ev::ClientStep { c });
        }
        let sched = match &s.cfg.schedule {
            Some(v) => v.clone(),
            None => nemesis::generate(s.cfg.seed, s.cfg.profile, s.cfg.duration_ms, s.cfg.nodes),
        };
        s.cfg.schedule = Some(sched.clone());
        for (t, a) in sched {
            if t < s.cfg.duration_ms {
                s.at(t, Ev::Nemesis(a));
            }
        }
        s.at(s.cfg.duration_ms, Ev::Heal);
        s.at(100, Ev::Check);
        s
    }

    fn at(&mut self, t: u64, e: Ev) {
        self.seq += 1;
        self.queue.insert((t, self.seq), e);
    }

    // ---------------------------------------------------------- nodes

    fn start_node(&mut self, id: u64) {
        let membership0 = self.membership0.clone();
        let (pre_vote, check_quorum, et) = (self.cfg.pre_vote, self.cfg.check_quorum, self.cfg.election_tick);
        let no_dedup = self.cfg.bug == Some(Bug::NoDedup);
        let bug = self.cfg.bug;
        let seed = self.cfg.seed;
        let n = self.nodes.get_mut(&id).unwrap();
        let (storage, mut init) = match RaftStorage::open(n.fs.as_vfs(), "/raft") {
            Ok(x) => x,
            Err(e) => {
                self.extra_violations.push(format!("node {id} failed to recover from disk: {e}"));
                return;
            }
        };
        let mut sm = match &init.snapshot_data {
            Some(d) => Sm::decode(d).unwrap_or_default(),
            None => Sm::default(),
        };
        sm.no_dedup = no_dedup;
        init.applied = init.snapshot.as_ref().map(|s| s.index).unwrap_or(0);
        let mut cfg = Config::new(id);
        cfg.pre_vote = pre_vote;
        cfg.check_quorum = check_quorum;
        cfg.election_tick = et;
        cfg.seed = seed.wrapping_mul(31).wrapping_add(id * 7919 + n.inc);
        if let Some(Bug::Raft(b)) = bug {
            cfg.bug = Some(b);
        }
        n.applied = init.applied;
        n.raft = Some(Raft::new(cfg, membership0, init));
        n.storage = Some(storage);
        n.sm = sm;
        n.up = true;
        n.pending_w.clear();
        n.pending_r.clear();
        n.read_wait.clear();
        n.last_role = Role::Follower;
        n.inc += 1;
        let inc = n.inc;
        let interval = ((10.0 * n.rate).max(1.0)) as u64;
        self.inv.node_restarted(id);
        let jitter = self.net_rng.gen_range(0..interval);
        self.at(self.now + jitter + 1, Ev::Tick { node: id, inc });
    }

    fn crash_node(&mut self, id: u64) {
        let seed = self.net_rng.gen::<u64>();
        let n = self.nodes.get_mut(&id).unwrap();
        if !n.up {
            return;
        }
        n.up = false;
        n.raft = None;
        n.storage = None;
        n.pending_w.clear();
        n.pending_r.clear();
        n.read_wait.clear();
        n.fs.crash(seed); // power loss: unsynced data lost/torn
        n.inc += 1;
        self.stats.crashes += 1;
    }

    fn send_msg(&mut self, m: Message) {
        self.stats.msgs_sent += 1;
        if self.blocked.contains(&(m.from, m.to)) || self.net_rng.gen::<f64>() < self.drop_p {
            self.stats.msgs_dropped += 1;
            return;
        }
        let copies = if self.net_rng.gen::<f64>() < self.dup_p {
            self.stats.msgs_duplicated += 1;
            2
        } else {
            1
        };
        let bytes = raft::codec::encode_message(&m);
        for _ in 0..copies {
            let d = self.net_rng.gen_range(self.delay.0..=self.delay.1.max(self.delay.0));
            self.at(self.now + d, Ev::Deliver(bytes.clone()));
        }
    }

    fn respond(&mut self, c: usize, op: u64, body: RespBody) {
        if self.net_rng.gen::<f64>() < self.drop_p {
            return;
        }
        let d = self.net_rng.gen_range(1..=4);
        self.at(self.now + d, Ev::ClientResp { c, op, body });
    }

    /// Process all outputs of a node's Raft instance.
    fn drive(&mut self, id: u64) {
        loop {
            let rd = {
                let n = self.nodes.get_mut(&id).unwrap();
                if !n.up {
                    return;
                }
                let r = n.raft.as_mut().unwrap();
                if !r.has_ready() {
                    break;
                }
                r.ready()
            };
            let mut rd = rd;
            if self.cfg.bug == Some(Bug::NoPersistVote) {
                rd.hard_state = None;
            }
            // 1. persist (fail-stop on error)
            {
                let n = self.nodes.get_mut(&id).unwrap();
                let fail = if n.fail_syncs > 0 {
                    n.fail_syncs -= 1;
                    true
                } else {
                    false
                };
                if fail {
                    n.fs.fail_next_syncs(1);
                }
                let res = n.storage.as_mut().unwrap().save(&rd, true);
                if res.is_err() {
                    n.fs.set_fail_all_syncs(false);
                    self.crash_node(id);
                    self.at(self.now + 300, Ev::Nemesis(Action::Restart(id)));
                    return;
                }
            }
            // 2. snapshot install
            if let Some(s) = &rd.install_snapshot {
                let n = self.nodes.get_mut(&id).unwrap();
                let mut sm = Sm::decode(&s.data).unwrap_or_default();
                sm.no_dedup = self.cfg.bug == Some(Bug::NoDedup);
                n.sm = sm;
                n.applied = s.meta.index;
                n.pending_w.clear();
                self.stats.snapshots_installed += 1;
            }
            // 3. send messages
            for m in rd.messages.clone() {
                self.send_msg(m);
            }
            // 4. apply committed entries
            for e in &rd.committed {
                self.inv.on_apply(id, e);
                let mut resp: Option<(usize, u64, RespBody)> = None;
                {
                    let n = self.nodes.get_mut(&id).unwrap();
                    if let EntryData::Normal(bytes) = &e.data {
                        if let Some(cmd) = Cmd::decode(bytes) {
                            let res = n.sm.apply(&cmd);
                            if let Some(p) = n.pending_w.remove(&e.index) {
                                resp = Some(if p.term == e.term {
                                    (p.c, p.op, RespBody::Write(res))
                                } else {
                                    (p.c, p.op, RespBody::Retry)
                                });
                            }
                        }
                    }
                    n.applied = e.index;
                }
                if let Some((c, op, b)) = resp {
                    if self.cfg.bug != Some(Bug::AckEarly) {
                        self.respond(c, op, b);
                    }
                }
            }
            // 5. read states
            for rs in &rd.read_states {
                let n = self.nodes.get_mut(&id).unwrap();
                if let Some(p) = n.pending_r.remove(&rs.ctx) {
                    n.read_wait.push((rs.index, p));
                }
            }
            // 6. serve reads that are now safe
            self.serve_reads(id);
            // 7. advance + snapshot
            {
                let thr = self.cfg.snapshot_threshold;
                let n = self.nodes.get_mut(&id).unwrap();
                let r = n.raft.as_mut().unwrap();
                r.advance(&rd);
                if n.applied.saturating_sub(r.snapshot_index()) >= thr && n.applied <= r.applied_index() {
                    let data = n.sm.encode();
                    if r.compact(n.applied, data).is_ok() {
                        self.stats.snapshots_taken += 1;
                    }
                }
            }
            self.after_step(id);
        }
        self.after_step(id);
    }

    fn serve_reads(&mut self, id: u64) {
        let n = self.nodes.get_mut(&id).unwrap();
        let applied = n.applied;
        let mut ready = vec![];
        n.read_wait.retain(|(idx, p)| {
            if *idx <= applied {
                ready.push((p.c, p.op, n.sm.kv.get(&p.key).cloned()));
                false
            } else {
                true
            }
        });
        for (c, op, v) in ready {
            self.respond(c, op, RespBody::Read(v));
        }
    }

    fn after_step(&mut self, id: u64) {
        let n = self.nodes.get_mut(&id).unwrap();
        let Some(r) = n.raft.as_ref() else { return };
        let role = r.role();
        let became_leader = role == Role::Leader && n.last_role != Role::Leader;
        let lost_leader = role != Role::Leader && n.last_role == Role::Leader;
        n.last_role = role;
        if lost_leader {
            n.pending_r.clear();
            n.read_wait.clear();
        }
        let r = n.raft.as_ref().unwrap();
        self.stats.max_term = self.stats.max_term.max(r.term());
        if became_leader {
            self.stats.leaders_elected += 1;
            self.inv.on_become_leader(r);
        }
        self.inv.check_node(r);
    }

    // ---------------------------------------------------------- clients

    fn client_step(&mut self, c: usize) {
        if self.clients[c].cur.is_some() {
            return;
        }
        let key_i = self.wl_rng.gen_range(0..self.cfg.keys);
        let key = key_name(key_i);
        let r = self.wl_rng.gen_range(0..100);
        let kind = if r < 38 {
            let v = self.next_val;
            self.next_val += 1;
            CKind::Put(v.to_be_bytes().to_vec())
        } else if r < 45 {
            CKind::Del
        } else if r < 62 {
            let v = self.next_val;
            self.next_val += 1;
            // expected value is usually something plausible
            let exp = if self.wl_rng.gen_bool(0.3) { None } else { Some(self.wl_rng.gen_range(1..self.next_val.max(2)).to_be_bytes().to_vec()) };
            CKind::Cas(exp, v.to_be_bytes().to_vec())
        } else {
            CKind::Read
        };
        let ok = match &kind {
            CKind::Put(v) => OpKind::Write(v.clone()),
            CKind::Del => OpKind::Delete,
            CKind::Cas(e, n) => OpKind::Cas(e.clone(), n.clone()),
            CKind::Read => OpKind::Read,
        };
        let cl = &mut self.clients[c];
        cl.seq += 1;
        let hist_id = self.rec.invoke(cl.id, &key, ok, self.now);
        let op = self.next_op;
        self.next_op += 1;
        self.stats.ops_total += 1;
        cl.cur = Some(CurOp { op, hist_id, seq: cl.seq, kind, key, attempts: 0 });
        self.send_attempt(c, None);
    }

    fn send_attempt(&mut self, c: usize, target: Option<u64>) {
        let (cid, cur) = {
            let cl = &mut self.clients[c];
            let Some(cur) = cl.cur.as_mut() else { return };
            cur.attempts += 1;
            (cl.id, (cur.op, cur.seq, cur.kind.clone(), cur.key.clone(), cur.attempts))
        };
        let (op, seq, kind, key, attempt) = cur;
        let req = match kind {
            CKind::Put(v) => Req::Write(Cmd::Put { client: cid, seq, key, val: v }),
            CKind::Del => Req::Write(Cmd::Del { client: cid, seq, key }),
            CKind::Cas(e, n) => Req::Write(Cmd::Cas { client: cid, seq, key, exp: e, new: n }),
            CKind::Read => Req::Read(key),
        };
        let node = target.unwrap_or_else(|| self.wl_rng.gen_range(1..=self.cfg.nodes));
        let d = self.net_rng.gen_range(1..=4);
        if !self.blocked.contains(&(0, node)) && self.net_rng.gen::<f64>() >= self.drop_p {
            self.at(self.now + d, Ev::ClientReq { node, c, op, attempt, req });
        }
        self.at(self.now + 400, Ev::ClientTimeout { c, op, attempt });
    }

    fn handle_req(&mut self, node: u64, c: usize, op: u64, req: Req) {
        let bug = self.cfg.bug;
        let mode = self.cfg.read_mode;
        let ctx = self.next_ctx;
        let n = self.nodes.get_mut(&node).unwrap();
        if !n.up {
            return;
        }
        match req {
            Req::Write(cmd) => {
                let r = n.raft.as_mut().unwrap();
                match r.propose(cmd.encode()) {
                    Ok((term, idx)) => {
                        n.pending_w.insert(idx, PendingW { term, c, op });
                        if bug == Some(Bug::AckEarly) {
                            let res = match cmd {
                                Cmd::Cas { .. } => CmdResult::Cas(true),
                                _ => CmdResult::Ok,
                            };
                            self.respond(c, op, RespBody::Write(res));
                        }
                    }
                    Err(ProposeError::NotLeader(h)) => self.respond(c, op, RespBody::NotLeader(h)),
                    Err(_) => self.respond(c, op, RespBody::Retry),
                }
            }
            Req::Read(key) => {
                if bug == Some(Bug::StaleReads) {
                    let v = n.sm.kv.get(&key).cloned();
                    self.respond(c, op, RespBody::Read(v));
                    return;
                }
                let r = n.raft.as_mut().unwrap();
                match mode {
                    ReadMode::ReadIndex => match r.read_index(ctx) {
                        Ok(()) => {
                            self.next_ctx += 1;
                            n.pending_r.insert(ctx, PendingR { c, op, key });
                        }
                        Err(ProposeError::NotLeader(h)) => self.respond(c, op, RespBody::NotLeader(h)),
                        Err(_) => self.respond(c, op, RespBody::Retry),
                    },
                    ReadMode::Lease => match r.lease_read() {
                        Some(idx) => n.read_wait.push((idx, PendingR { c, op, key })),
                        None => {
                            let h = r.leader();
                            self.respond(c, op, RespBody::NotLeader(h))
                        }
                    },
                }
            }
        }
        self.drive(node);
    }

    fn handle_resp(&mut self, c: usize, op: u64, body: RespBody) {
        let (hist_id, kind) = match &self.clients[c].cur {
            Some(cur) if cur.op == op => (cur.hist_id, cur.kind.clone()),
            _ => return, // stale response
        };
        match body {
            RespBody::NotLeader(hint) => {
                let t = self.now + 15;
                let _ = t;
                let target = hint.filter(|h| *h >= 1 && *h <= self.cfg.nodes);
                // retry shortly (keeps the same attempt budget)
                self.send_attempt(c, target);
            }
            RespBody::Retry => self.send_attempt(c, None),
            RespBody::Write(res) => {
                let result = match (&kind, res) {
                    (CKind::Cas(..), CmdResult::Cas(b)) => OpResult::CasOk(b),
                    _ => OpResult::WriteOk,
                };
                self.finish(c, hist_id, result);
            }
            RespBody::Read(v) => self.finish(c, hist_id, OpResult::ReadOk(v)),
        }
    }

    fn finish(&mut self, c: usize, hist_id: u64, result: OpResult) {
        self.rec.complete(hist_id, self.now, result);
        self.clients[c].cur = None;
        self.stats.ops_completed += 1;
        if self.now >= self.heal_time {
            self.stats.completed_after_heal += 1;
        }
        let think = self.wl_rng.gen_range(3..30);
        self.at(self.now + think, Ev::ClientStep { c });
    }

    fn client_timeout(&mut self, c: usize, op: u64, attempt: u32) {
        let (cur_op, attempts) = match &self.clients[c].cur {
            Some(cur) => (cur.op, cur.attempts),
            None => return,
        };
        if cur_op != op || attempts != attempt {
            return;
        }
        if attempts >= 6 {
            // give up: the operation stays pending (outcome unknown) forever; become a new process
            self.clients[c].cur = None;
            self.clients[c].id = self.next_proc;
            self.clients[c].seq = 0;
            self.next_proc += 1;
            let t = self.now + self.wl_rng.gen_range(3..30);
            self.at(t, Ev::ClientStep { c });
        } else {
            self.send_attempt(c, None);
        }
    }

    // ---------------------------------------------------------- nemesis

    fn apply_action(&mut self, a: Action) {
        let n = self.cfg.nodes;
        match a {
            Action::Partition(groups) => {
                self.blocked.clear();
                for (gi, g) in groups.iter().enumerate() {
                    for (hi, h) in groups.iter().enumerate() {
                        if gi != hi {
                            for x in g {
                                for y in h {
                                    self.blocked.insert((*x, *y));
                                }
                            }
                        }
                    }
                }
            }
            Action::OneWay(a, b) => {
                self.blocked.insert((a, b));
            }
            Action::Isolate(x) => {
                self.blocked.clear();
                for y in 1..=n {
                    if y != x {
                        self.blocked.insert((x, y));
                        self.blocked.insert((y, x));
                    }
                }
            }
            Action::Heal => {
                self.blocked.clear();
                self.drop_p = 0.0;
                self.dup_p = 0.0;
                self.delay = (1, 5);
            }
            Action::Crash(id) => self.crash_node(id),
            Action::Restart(id) => {
                if let Some(nd) = self.nodes.get(&id) {
                    if !nd.up {
                        self.start_node(id);
                        self.stats.restarts += 1;
                    }
                }
            }
            Action::SetDrop(p) => self.drop_p = p,
            Action::SetDup(p) => self.dup_p = p,
            Action::SetDelay(lo, hi) => self.delay = (lo, hi),
            Action::Drift(id, r) => {
                if let Some(nd) = self.nodes.get_mut(&id) {
                    nd.rate = r;
                }
            }
            Action::TransferLeader => {
                let ids: Vec<u64> = self.nodes.keys().copied().collect();
                for id in &ids {
                    let leader_member = {
                        let nd = &self.nodes[id];
                        nd.up && nd.raft.as_ref().map(|r| r.is_leader()).unwrap_or(false)
                    };
                    if leader_member {
                        let voters: Vec<u64> = self.nodes[id].raft.as_ref().unwrap().membership().voters.iter().copied().filter(|v| v != id).collect();
                        if !voters.is_empty() {
                            let t = voters[self.net_rng.gen_range(0..voters.len())];
                            let _ = self.nodes.get_mut(id).unwrap().raft.as_mut().unwrap().transfer_leader(t);
                            self.drive(*id);
                        }
                        break;
                    }
                }
            }
            Action::FailSync(id) => {
                if let Some(nd) = self.nodes.get_mut(&id) {
                    nd.fail_syncs = 1;
                }
            }
            Action::ConfChange { voters, learners } => {
                let ids: Vec<u64> = self.nodes.keys().copied().collect();
                for id in ids {
                    let is_leader = {
                        let nd = &self.nodes[&id];
                        nd.up && nd.raft.as_ref().map(|r| r.is_leader()).unwrap_or(false)
                    };
                    if is_leader {
                        let v: BTreeSet<u64> = voters.iter().copied().collect();
                        let l: BTreeSet<u64> = learners.iter().copied().collect();
                        if self.nodes.get_mut(&id).unwrap().raft.as_mut().unwrap().propose_conf_change(v, l).is_ok() {
                            self.stats.conf_changes += 1;
                        }
                        self.drive(id);
                        break;
                    }
                }
            }
        }
    }

    // ---------------------------------------------------------- main loop

    pub fn run(mut self) -> SimResult {
        let end = self.cfg.duration_ms + 6_000;
        while let Some((&(t, k), _)) = self.queue.iter().next() {
            if t > end {
                break;
            }
            let ev = self.queue.remove(&(t, k)).unwrap();
            self.now = t;
            self.stats.events += 1;
            match ev {
                Ev::Tick { node, inc } => {
                    let Some(nd) = self.nodes.get_mut(&node) else { continue };
                    if !nd.up || nd.inc != inc {
                        continue;
                    }
                    nd.raft.as_mut().unwrap().tick();
                    let interval = ((10.0 * nd.rate).max(1.0)) as u64;
                    self.at(self.now + interval, Ev::Tick { node, inc });
                    self.drive(node);
                }
                Ev::Deliver(bytes) => {
                    let Some(m) = raft::codec::decode_message(&bytes) else {
                        self.extra_violations.push("codec: own message failed to decode".into());
                        continue;
                    };
                    let to = m.to;
                    if self.blocked.contains(&(m.from, to)) {
                        self.stats.msgs_dropped += 1;
                        continue;
                    }
                    let Some(nd) = self.nodes.get_mut(&to) else { continue };
                    if !nd.up {
                        continue;
                    }
                    nd.raft.as_mut().unwrap().step(m);
                    self.drive(to);
                }
                Ev::ClientStep { c } => self.client_step(c),
                Ev::ClientReq { node, c, op, attempt: _, req } => self.handle_req(node, c, op, req),
                Ev::ClientResp { c, op, body } => self.handle_resp(c, op, body),
                Ev::ClientTimeout { c, op, attempt } => self.client_timeout(c, op, attempt),
                Ev::Nemesis(a) => self.apply_action(a),
                Ev::Check => {
                    let live: Vec<&Raft> = self.nodes.values().filter(|n| n.up).filter_map(|n| n.raft.as_ref()).collect();
                    self.inv.check_log_matching(&live);
                    self.at(self.now + 250, Ev::Check);
                }
                Ev::Heal => {
                    self.heal_time = self.now;
                    self.apply_action(Action::Heal);
                    for id in 1..=self.cfg.nodes {
                        self.apply_action(Action::Restart(id));
                        if let Some(nd) = self.nodes.get_mut(&id) {
                            nd.rate = 1.0;
                            nd.fail_syncs = 0;
                        }
                    }
                }
            }
        }
        // final checks
        let live: Vec<&Raft> = self.nodes.values().filter(|n| n.up).filter_map(|n| n.raft.as_ref()).collect();
        self.inv.check_log_matching(&live);
        let history = self.rec.ops.clone();
        let (lin, lstats) = lincheck::check(&history, self.cfg.lin_budget);
        self.stats.lin_states = lstats.states_explored;
        self.stats.ops_pending = history.iter().filter(|o| o.ret.is_none()).count() as u64;
        self.stats.committed_entries = self.inv.committed_len();
        self.stats.invariant_checks = self.inv.checks;
        let mut violations = std::mem::take(&mut self.inv.violations);
        violations.extend(std::mem::take(&mut self.extra_violations));
        SimResult {
            seed: self.cfg.seed,
            profile: self.cfg.profile,
            safety_violations: violations,
            lin,
            liveness_ok: self.stats.completed_after_heal > 0,
            stats: self.stats,
            schedule: self.cfg.schedule.clone().unwrap_or_default(),
        }
    }
}

pub fn run_seed(cfg: SimConfig) -> SimResult {
    Sim::new(cfg).run()
}
