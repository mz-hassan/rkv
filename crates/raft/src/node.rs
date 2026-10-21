//! Sans-IO Raft. No threads, clocks, sockets or disks in here. The driver:
//!   1. feeds events: `tick()`, `step(msg)`, `propose(..)`, `read_index(..)`, ...
//!   2. while `has_ready()`: `rd = ready()`; PERSIST rd (snapshot, truncation, entries,
//!      hard state) durably; THEN send `rd.messages`; THEN apply `rd.committed`;
//!      then call `advance(&rd)`.
//! Persist-before-send is what makes votes and acks safe.
use crate::log::RaftLog;
use crate::types::*;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

#[derive(Clone, Debug)]
pub struct Config {
    pub id: NodeId,
    pub election_tick: u64,
    pub heartbeat_tick: u64,
    pub pre_vote: bool,
    pub check_quorum: bool,
    pub max_msg_entries: usize,
    pub max_inflight: usize,
    pub max_uncommitted: u64,
    pub seed: u64,
    /// MUTATION TESTING ONLY: deliberately break the protocol (see `RaftBug`).
    pub bug: Option<RaftBug>,
}

/// Deliberate protocol bugs used to prove the simulator's invariants can fail.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RaftBug {
    /// grant votes without the log up-to-date check (violates leader completeness)
    VoteIgnoresLog,
    /// commit as soon as ONE follower has acknowledged (violates durability/safety)
    CommitMinority,
    /// commit entries from old terms by counting replicas (the Raft paper's Figure 8 bug)
    CommitOldTerm,
    /// truncate the log on a mismatch even below the commit index (instead of asserting)
    SkipConflictCheck,
}

impl Config {
    pub fn new(id: NodeId) -> Config {
        Config {
            id,
            election_tick: 10,
            heartbeat_tick: 1,
            pre_vote: true,
            check_quorum: true,
            max_msg_entries: 64,
            max_inflight: 32,
            max_uncommitted: 4096,
            seed: id,
            bug: None,
        }
    }
}

/// What the driver recovered from disk at startup.
#[derive(Clone, Debug, Default)]
pub struct InitialState {
    pub hard_state: HardState,
    pub snapshot: Option<SnapshotMeta>,
    pub entries: Vec<Entry>, // after the snapshot, contiguous
    /// Last index the state machine has durably applied (for durable state machines).
    pub applied: u64,
    pub snapshot_data: Option<Vec<u8>>,
}

#[derive(Default, Debug)]
pub struct Ready {
    pub hard_state: Option<HardState>,
    pub truncate_from: Option<u64>,
    pub entries: Vec<Entry>,
    /// Snapshot received from the leader: persist AND restore the state machine from it.
    pub install_snapshot: Option<Snapshot>,
    /// Local snapshot (from `compact`): persist and discard log below it.
    pub save_snapshot: Option<Snapshot>,
    pub messages: Vec<Message>,
    pub committed: Vec<Entry>,
    pub read_states: Vec<ReadState>,
}

impl Ready {
    fn is_empty(&self) -> bool {
        self.hard_state.is_none()
            && self.truncate_from.is_none()
            && self.entries.is_empty()
            && self.install_snapshot.is_none()
            && self.save_snapshot.is_none()
            && self.messages.is_empty()
            && self.committed.is_empty()
            && self.read_states.is_empty()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PState {
    Probe,
    Replicate,
    Snapshot(u64),
}

#[derive(Clone, Debug)]
struct Progress {
    matched: u64,
    next: u64,
    state: PState,
    paused: bool,
    inflight: VecDeque<u64>,
    recent_active: bool,
    last_ack_tick: u64,
    read_ack: u64,
    snap_tick: u64,
    stall: u64,
}

impl Progress {
    fn new(next: u64) -> Progress {
        Progress { matched: 0, next, state: PState::Probe, paused: false, inflight: VecDeque::new(), recent_active: false, last_ack_tick: 0, read_ack: 0, snap_tick: 0, stall: 0 }
    }
}

pub struct Raft {
    cfg: Config,
    id: NodeId,
    term: u64,
    vote: Option<NodeId>,
    role: Role,
    leader: Option<NodeId>,
    log: RaftLog,
    commit: u64,
    applied_cursor: u64, // last index handed to the driver in a Ready
    persisted: u64,
    membership: Membership,
    base_membership: Membership,
    conf_history: Vec<(u64, Membership)>,
    // timers
    ticks: u64,
    election_elapsed: u64,
    heartbeat_elapsed: u64,
    rand_timeout: u64,
    rng: ChaCha8Rng,
    // election
    votes: BTreeMap<NodeId, bool>,
    // leader state
    prs: BTreeMap<NodeId, Progress>,
    pending_conf_index: u64,
    transferee: Option<NodeId>,
    transfer_elapsed: u64,
    read_seq: u64,
    pending_reads: VecDeque<(u64 /*ctx*/, u64 /*seq*/, u64 /*index*/)>,
    waiting_reads: Vec<u64>, // ctxs waiting for a committed entry in our term
    // outputs
    hs_dirty: bool,
    msgs: Vec<Message>,
    unstable: Vec<Entry>,
    truncate_from: Option<u64>,
    snapshot: Option<Snapshot>, // latest local snapshot (for sending)
    to_install: Option<Snapshot>,
    to_save: Option<Snapshot>,
    read_states: Vec<ReadState>,
}

impl Raft {
    pub fn new(cfg: Config, membership: Membership, init: InitialState) -> Raft {
        let (snap_index, snap_term, base) = match &init.snapshot {
            Some(m) => (m.index, m.term, m.membership.clone()),
            None => (0, 0, membership.clone()),
        };
        let log = RaftLog::new(snap_index, snap_term, init.entries.clone());
        // recover membership from the log: latest conf entry wins
        let mut r = Raft {
            id: cfg.id,
            term: init.hard_state.term,
            vote: init.hard_state.vote,
            role: Role::Follower,
            leader: None,
            log,
            commit: snap_index,
            applied_cursor: init.applied.max(snap_index),
            persisted: snap_index + init.entries.len() as u64,
            membership: base.clone(),
            base_membership: base,
            conf_history: vec![],
            ticks: 0,
            election_elapsed: 0,
            heartbeat_elapsed: 0,
            rand_timeout: 0,
            rng: ChaCha8Rng::seed_from_u64(cfg.seed ^ 0x9e3779b97f4a7c15),
            votes: BTreeMap::new(),
            prs: BTreeMap::new(),
            pending_conf_index: 0,
            transferee: None,
            transfer_elapsed: 0,
            read_seq: 0,
            pending_reads: VecDeque::new(),
            waiting_reads: vec![],
            hs_dirty: false,
            msgs: vec![],
            unstable: vec![],
            truncate_from: None,
            snapshot: init.snapshot.clone().map(|m| Snapshot { meta: m, data: init.snapshot_data.clone().unwrap_or_default() }),
            to_install: None,
            to_save: None,
            read_states: vec![],
            cfg,
        };
        for i in r.log.first_index()..=r.log.last_index() {
            if let Some(e) = r.log.get(i).cloned() {
                r.apply_conf_entry(&e);
            }
        }
        r.reset_timeout();
        r
    }

    // ------------------------------------------------------------ accessors
    pub fn id(&self) -> NodeId {
        self.id
    }
    pub fn term(&self) -> u64 {
        self.term
    }
    pub fn role(&self) -> Role {
        self.role
    }
    pub fn leader(&self) -> Option<NodeId> {
        self.leader
    }
    pub fn commit_index(&self) -> u64 {
        self.commit
    }
    pub fn last_index(&self) -> u64 {
        self.log.last_index()
    }
    pub fn first_index(&self) -> u64 {
        self.log.first_index()
    }
    pub fn applied_index(&self) -> u64 {
        self.applied_cursor
    }
    pub fn snapshot_index(&self) -> u64 {
        self.log.snap_index
    }
    pub fn term_at(&self, i: u64) -> Option<u64> {
        self.log.term_at(i)
    }
    pub fn entry(&self, i: u64) -> Option<&Entry> {
        self.log.get(i)
    }
    pub fn membership(&self) -> &Membership {
        &self.membership
    }
    pub fn is_leader(&self) -> bool {
        self.role == Role::Leader
    }
    pub fn vote(&self) -> Option<NodeId> {
        self.vote
    }
    pub fn leader_transferee(&self) -> Option<NodeId> {
        self.transferee
    }
    /// Peers' replication progress (leader only): (match, next).
    pub fn progress(&self, id: NodeId) -> Option<(u64, u64)> {
        self.prs.get(&id).map(|p| (p.matched, p.next))
    }

    fn promotable(&self) -> bool {
        self.membership.is_voter(self.id)
    }

    fn reset_timeout(&mut self) {
        self.rand_timeout = self.cfg.election_tick + self.rng.gen_range(0..self.cfg.election_tick);
    }

    // ------------------------------------------------------------ role changes
    fn reset(&mut self, term: u64) {
        if self.term != term {
            self.term = term;
            self.vote = None;
            self.hs_dirty = true;
        }
        self.leader = None;
        self.election_elapsed = 0;
        self.heartbeat_elapsed = 0;
        self.votes.clear();
        self.transferee = None;
        self.pending_reads.clear();
        self.waiting_reads.clear();
        self.reset_timeout();
        self.prs.clear();
    }

    fn become_follower(&mut self, term: u64, leader: Option<NodeId>) {
        self.reset(term);
        self.role = Role::Follower;
        self.leader = leader;
    }

    fn become_pre_candidate(&mut self) {
        // term and vote are NOT changed by a pre-vote campaign
        self.votes.clear();
        self.leader = None;
        self.role = Role::PreCandidate;
        self.election_elapsed = 0;
        self.reset_timeout();
    }

    fn become_candidate(&mut self) {
        let t = self.term + 1;
        self.reset(t);
        self.role = Role::Candidate;
        self.vote = Some(self.id);
        self.hs_dirty = true;
    }

    fn become_leader(&mut self) {
        let t = self.term;
        self.reset(t);
        self.role = Role::Leader;
        self.leader = Some(self.id);
        let next = self.log.last_index() + 1;
        for n in self.membership.all_nodes() {
            self.prs.insert(n, Progress::new(next));
        }
        if let Some(p) = self.prs.get_mut(&self.id) {
            p.recent_active = true;
        }
        // conservative: block conf changes until our noop (and anything before it) commits
        self.pending_conf_index = self.log.last_index();
        self.append_entries(vec![EntryData::Noop]);
        self.broadcast_append();
    }

    /// `skip_pre`: go straight to a real election. `transfer`: leadership-transfer
    /// campaign, which bypasses voters' leader-stickiness.
    fn campaign(&mut self, skip_pre: bool, transfer: bool) {
        let pre = self.cfg.pre_vote && !skip_pre;
        let (term, kind_pre) = if pre {
            self.become_pre_candidate();
            (self.term + 1, true)
        } else {
            self.become_candidate();
            (self.term, false)
        };
        self.votes.insert(self.id, true);
        if self.vote_result() == Some(true) {
            if pre {
                self.campaign(true, false);
            } else {
                self.become_leader();
            }
            return;
        }
        let (li, lt) = (self.log.last_index(), self.log.last_term());
        for n in self.membership.all_voters() {
            if n == self.id {
                continue;
            }
            self.msgs.push(Message { from: self.id, to: n, term, body: MsgBody::RequestVote { pre: kind_pre, last_index: li, last_term: lt, transfer } });
        }
    }

    /// Some(true) = won, Some(false) = lost, None = undecided
    fn vote_result(&self) -> Option<bool> {
        let granted = |n: NodeId| self.votes.get(&n) == Some(&true);
        let rejected = |n: NodeId| self.votes.get(&n) == Some(&false);
        if self.membership.quorum(&granted) {
            return Some(true);
        }
        // lost if a quorum can no longer be formed
        let possible = |n: NodeId| !rejected(n);
        if !self.membership.quorum(&possible) {
            return Some(false);
        }
        None
    }

    // ------------------------------------------------------------ ticking
    pub fn tick(&mut self) {
        self.ticks += 1;
        match self.role {
            Role::Leader => self.tick_leader(),
            _ => {
                self.election_elapsed += 1;
                if self.promotable() && self.election_elapsed >= self.rand_timeout {
                    self.election_elapsed = 0;
                    self.campaign(false, false);
                }
            }
        }
    }

    fn tick_leader(&mut self) {
        self.election_elapsed += 1;
        self.heartbeat_elapsed += 1;
        if self.transferee.is_some() {
            self.transfer_elapsed += 1;
            if self.transfer_elapsed >= self.cfg.election_tick {
                self.transferee = None;
            }
        }
        if self.election_elapsed >= self.cfg.election_tick {
            self.election_elapsed = 0;
            if self.cfg.check_quorum {
                let active = |n: NodeId| n == self.id || self.prs.get(&n).map(|p| p.recent_active).unwrap_or(false);
                if !self.membership.quorum(&active) {
                    let t = self.term;
                    self.become_follower(t, None);
                    return;
                }
            }
            let id = self.id;
            for (n, p) in self.prs.iter_mut() {
                if *n != id {
                    p.recent_active = false;
                }
            }
        }
        // snapshot sends that were never acknowledged are retried
        let (ticks, et) = (self.ticks, self.cfg.election_tick);
        for p in self.prs.values_mut() {
            if let PState::Snapshot(_) = p.state {
                if ticks.saturating_sub(p.snap_tick) > et * 3 {
                    p.state = PState::Probe;
                    p.paused = false;
                }
            }
        }
        if self.heartbeat_elapsed >= self.cfg.heartbeat_tick {
            self.heartbeat_elapsed = 0;
            self.broadcast_heartbeat();
            // also retry stuck/probing followers
            let peers: Vec<NodeId> = self.prs.keys().copied().filter(|n| *n != self.id).collect();
            let last = self.log.last_index();
            for n in peers {
                if let Some(p) = self.prs.get_mut(&n) {
                    if p.state == PState::Probe {
                        p.paused = false;
                    }
                    // pipelined messages may have been lost: fall back to probing
                    if p.state == PState::Replicate && p.matched < last && !p.inflight.is_empty() {
                        p.stall += 1;
                        if p.stall > 3 {
                            p.state = PState::Probe;
                            p.next = p.matched + 1;
                            p.inflight.clear();
                            p.paused = false;
                            p.stall = 0;
                        }
                    }
                }
                self.send_append(n);
            }
        }
    }

    // ------------------------------------------------------------ proposals
    pub fn propose(&mut self, data: Vec<u8>) -> Result<(u64, u64), ProposeError> {
        self.check_can_propose()?;
        if self.log.uncommitted_len(self.commit) >= self.cfg.max_uncommitted {
            return Err(ProposeError::Busy);
        }
        let idx = self.append_entries(vec![EntryData::Normal(data)]);
        self.broadcast_append();
        Ok((self.term, idx))
    }

    fn check_can_propose(&self) -> Result<(), ProposeError> {
        if self.role != Role::Leader {
            return Err(ProposeError::NotLeader(self.leader));
        }
        if self.transferee.is_some() {
            return Err(ProposeError::TransferInProgress);
        }
        Ok(())
    }

    /// Change membership. Uses joint consensus unless only learners change.
    pub fn propose_conf_change(&mut self, voters: BTreeSet<NodeId>, learners: BTreeSet<NodeId>) -> Result<(u64, u64), ProposeError> {
        self.check_can_propose()?;
        if self.membership.is_joint() || self.pending_conf_index > self.commit {
            return Err(ProposeError::ConfChangePending);
        }
        if voters.is_empty() {
            return Err(ProposeError::InvalidConfChange("no voters".into()));
        }
        if voters.intersection(&learners).next().is_some() {
            return Err(ProposeError::InvalidConfChange("node both voter and learner".into()));
        }
        let data = if voters == self.membership.voters {
            EntryData::ConfSet(Membership { voters, outgoing: BTreeSet::new(), learners })
        } else {
            EntryData::ConfEnterJoint { voters, learners }
        };
        let idx = self.append_entries(vec![data]);
        self.pending_conf_index = idx;
        self.broadcast_append();
        Ok((self.term, idx))
    }

    fn append_entries(&mut self, datas: Vec<EntryData>) -> u64 {
        let mut idx = self.log.last_index();
        for d in datas {
            idx += 1;
            let e = Entry { term: self.term, index: idx, data: d };
            self.apply_conf_entry(&e);
            self.log.append(e.clone());
            self.unstable.push(e);
        }
        if self.membership.all_nodes().len() == 1 || self.role == Role::Leader {
            // nothing: own match is updated by advance() once persisted
        }
        idx
    }

    /// Config takes effect when the entry is APPENDED (not committed).
    fn apply_conf_entry(&mut self, e: &Entry) {
        let new = match &e.data {
            EntryData::ConfEnterJoint { voters, learners } => Membership { voters: voters.clone(), outgoing: self.membership.voters.clone(), learners: learners.clone() },
            EntryData::ConfLeaveJoint => Membership { voters: self.membership.voters.clone(), outgoing: BTreeSet::new(), learners: self.membership.learners.clone() },
            EntryData::ConfSet(m) => m.clone(),
            _ => return,
        };
        self.conf_history.push((e.index, new.clone()));
        self.set_membership(new);
    }

    fn set_membership(&mut self, m: Membership) {
        self.membership = m;
        if self.role == Role::Leader {
            let next = self.log.last_index() + 1;
            let all = self.membership.all_nodes();
            for n in &all {
                self.prs.entry(*n).or_insert_with(|| Progress::new(next));
            }
            self.prs.retain(|n, _| all.contains(n));
        }
    }

    fn membership_at(&self, index: u64) -> Membership {
        self.conf_history.iter().rev().find(|(i, _)| *i <= index).map(|(_, m)| m.clone()).unwrap_or_else(|| self.base_membership.clone())
    }

    fn rebuild_membership_after_truncate(&mut self, from: u64) {
        self.conf_history.retain(|(i, _)| *i < from);
        let m = self.conf_history.last().map(|(_, m)| m.clone()).unwrap_or_else(|| self.base_membership.clone());
        self.membership = m;
    }

    // ------------------------------------------------------------ reads
    /// Request a linearizable read point (ReadIndex). Result arrives in `Ready.read_states`.
    pub fn read_index(&mut self, ctx: u64) -> Result<(), ProposeError> {
        if self.role != Role::Leader {
            return Err(ProposeError::NotLeader(self.leader));
        }
        if self.log.term_at(self.commit) != Some(self.term) {
            // must have committed an entry in our term first
            self.waiting_reads.push(ctx);
            return Ok(());
        }
        self.start_read(ctx);
        Ok(())
    }

    fn start_read(&mut self, ctx: u64) {
        if self.membership.voters.len() == 1 && self.membership.outgoing.is_empty() && self.membership.voters.contains(&self.id) {
            self.read_states.push(ReadState { ctx, index: self.commit });
            return;
        }
        self.read_seq += 1;
        let seq = self.read_seq;
        self.pending_reads.push_back((ctx, seq, self.commit));
        self.broadcast_heartbeat();
    }

    /// Lease read: valid while a quorum acknowledged us within `lease_ticks` of OUR clock.
    /// Safe only if clock drift is bounded (followers will not vote for another
    /// candidate for at least `election_tick` of THEIR ticks after hearing from us).
    pub fn lease_read(&self) -> Option<u64> {
        if self.role != Role::Leader || self.log.term_at(self.commit) != Some(self.term) {
            return None;
        }
        let lease = self.cfg.election_tick / 2;
        let ok = |n: NodeId| n == self.id || self.prs.get(&n).map(|p| self.ticks.saturating_sub(p.last_ack_tick) <= lease && p.last_ack_tick > 0).unwrap_or(false);
        if self.membership.quorum(&ok) {
            Some(self.commit)
        } else {
            None
        }
    }

    // ------------------------------------------------------------ leadership transfer
    pub fn transfer_leader(&mut self, to: NodeId) -> Result<(), ProposeError> {
        if self.role != Role::Leader {
            return Err(ProposeError::NotLeader(self.leader));
        }
        if to == self.id || !self.membership.is_voter(to) {
            return Err(ProposeError::InvalidConfChange("bad transfer target".into()));
        }
        self.transferee = Some(to);
        self.transfer_elapsed = 0;
        self.election_elapsed = 0;
        let caught_up = self.prs.get(&to).map(|p| p.matched == self.log.last_index()).unwrap_or(false);
        if caught_up {
            self.msgs.push(Message { from: self.id, to, term: self.term, body: MsgBody::TimeoutNow });
        } else {
            self.send_append(to);
        }
        Ok(())
    }

    // ------------------------------------------------------------ sending
    fn broadcast_append(&mut self) {
        let peers: Vec<NodeId> = self.prs.keys().copied().filter(|n| *n != self.id).collect();
        for n in peers {
            self.send_append(n);
        }
    }

    fn broadcast_heartbeat(&mut self) {
        let peers: Vec<NodeId> = self.prs.keys().copied().filter(|n| *n != self.id).collect();
        for n in peers {
            let commit = self.prs[&n].matched.min(self.commit);
            self.msgs.push(Message { from: self.id, to: n, term: self.term, body: MsgBody::Heartbeat { commit, read_seq: self.read_seq } });
        }
    }

    fn send_append(&mut self, to: NodeId) {
        let (max_msg, max_inflight) = (self.cfg.max_msg_entries, self.cfg.max_inflight);
        let last = self.log.last_index();
        let (next, state, paused, full) = match self.prs.get(&to) {
            Some(p) => (p.next, p.state, p.paused, p.inflight.len() >= max_inflight),
            None => return,
        };
        match state {
            PState::Snapshot(_) => return,
            PState::Probe if paused => return,
            PState::Replicate if full => return,
            _ => {}
        }
        let prev = next - 1;
        let prev_term = self.log.term_at(prev);
        if prev_term.is_none() {
            // needed entries were compacted: ship the snapshot
            if let Some(s) = self.snapshot.clone() {
                let idx = s.meta.index;
                self.msgs.push(Message { from: self.id, to, term: self.term, body: MsgBody::Snapshot(s) });
                let ticks = self.ticks;
                let p = self.prs.get_mut(&to).unwrap();
                p.state = PState::Snapshot(idx);
                p.snap_tick = ticks;
            }
            return;
        }
        let entries = self.log.slice(next, last + 1, max_msg);
        let is_probe = state == PState::Probe;
        if entries.is_empty() && state == PState::Replicate {
            // nothing new; commit index travels on heartbeats
            return;
        }
        let n = entries.len() as u64;
        self.msgs.push(Message {
            from: self.id,
            to,
            term: self.term,
            body: MsgBody::Append { prev_index: prev, prev_term: prev_term.unwrap(), entries, commit: self.commit },
        });
        let p = self.prs.get_mut(&to).unwrap();
        if is_probe {
            p.paused = true;
        } else if n > 0 {
            p.next += n;
            p.inflight.push_back(p.next - 1);
        }
    }

    fn send(&mut self, to: NodeId, body: MsgBody) {
        self.msgs.push(Message { from: self.id, to, term: self.term, body });
    }

    // ------------------------------------------------------------ message handling
    fn in_lease(&self) -> bool {
        self.cfg.check_quorum && self.leader.is_some() && self.election_elapsed < self.cfg.election_tick
    }

    pub fn step(&mut self, m: Message) {
        // ---- term handling
        if m.term > self.term {
            match &m.body {
                MsgBody::RequestVote { transfer, pre, .. } => {
                    if self.in_lease() && !*transfer {
                        // leader stickiness: ignore disruptive vote requests while we hear from a leader
                        return;
                    }
                    if *pre {
                        // a pre-vote never changes our term
                    } else {
                        self.become_follower(m.term, None);
                    }
                }
                MsgBody::VoteResp { pre: true, granted: true } => {
                    // response to our pre-vote carries the hypothetical future term
                }
                MsgBody::Append { .. } | MsgBody::Heartbeat { .. } | MsgBody::Snapshot(_) => self.become_follower(m.term, Some(m.from)),
                _ => self.become_follower(m.term, None),
            }
        } else if m.term < self.term {
            match &m.body {
                MsgBody::Append { .. } | MsgBody::Heartbeat { .. } if self.cfg.check_quorum || self.cfg.pre_vote => {
                    // tell a stale leader about the newer term so it steps down
                    self.send(m.from, MsgBody::AppendResp { success: false, index: 0, hint: 0 });
                }
                MsgBody::RequestVote { pre: true, .. } => {
                    self.send(m.from, MsgBody::VoteResp { pre: true, granted: false });
                }
                _ => {}
            }
            return;
        }

        match m.body.clone() {
            MsgBody::RequestVote { pre, last_index, last_term, transfer } => self.handle_vote_request(&m, pre, last_index, last_term, transfer),
            MsgBody::VoteResp { pre, granted } => self.handle_vote_resp(&m, pre, granted),
            _ => match self.role {
                Role::Leader => self.step_leader(m),
                Role::Candidate | Role::PreCandidate => {
                    if matches!(m.body, MsgBody::Append { .. } | MsgBody::Heartbeat { .. } | MsgBody::Snapshot(_)) {
                        let t = self.term;
                        self.become_follower(t, Some(m.from));
                        self.step_follower(m);
                    }
                }
                Role::Follower => self.step_follower(m),
            },
        }
    }

    fn handle_vote_request(&mut self, m: &Message, pre: bool, last_index: u64, last_term: u64, transfer: bool) {
        let ignore_log = self.cfg.bug == Some(RaftBug::VoteIgnoresLog);
        let up_to_date = ignore_log || last_term > self.log.last_term() || (last_term == self.log.last_term() && last_index >= self.log.last_index());
        let can_vote = if pre {
            self.vote == Some(m.from) || (self.vote.is_none() && self.leader.is_none()) || m.term > self.term
        } else {
            self.vote == Some(m.from) || (self.vote.is_none() && self.leader.is_none())
        };
        // stickiness for `transfer` was already bypassed in step(); a leader never votes in its own term
        let _ = transfer;
        let granted = can_vote && up_to_date && self.role != Role::Leader;
        if granted {
            if pre {
                self.send_with_term(m.from, m.term, MsgBody::VoteResp { pre: true, granted: true });
            } else {
                self.election_elapsed = 0;
                self.vote = Some(m.from);
                self.hs_dirty = true;
                self.send(m.from, MsgBody::VoteResp { pre: false, granted: true });
            }
        } else if pre {
            self.send(m.from, MsgBody::VoteResp { pre: true, granted: false });
        } else {
            self.send(m.from, MsgBody::VoteResp { pre: false, granted: false });
        }
    }

    fn send_with_term(&mut self, to: NodeId, term: u64, body: MsgBody) {
        self.msgs.push(Message { from: self.id, to, term, body });
    }

    fn handle_vote_resp(&mut self, m: &Message, pre: bool, granted: bool) {
        match (self.role, pre) {
            (Role::PreCandidate, true) | (Role::Candidate, false) => {}
            _ => return,
        }
        if !self.membership.is_voter(m.from) {
            return;
        }
        self.votes.insert(m.from, granted);
        match self.vote_result() {
            Some(true) => {
                if pre {
                    self.campaign(true, false);
                } else {
                    self.become_leader();
                }
            }
            Some(false) => {
                let t = self.term;
                self.become_follower(t, None);
            }
            None => {}
        }
    }

    fn step_follower(&mut self, m: Message) {
        match m.body {
            MsgBody::Append { prev_index, prev_term, entries, commit } => {
                self.election_elapsed = 0;
                self.leader = Some(m.from);
                self.handle_append(m.from, prev_index, prev_term, entries, commit);
            }
            MsgBody::Heartbeat { commit, read_seq } => {
                self.election_elapsed = 0;
                self.leader = Some(m.from);
                // commit is already min(leader.match, leader.commit): safe to adopt
                if commit > self.commit && commit <= self.log.last_index() {
                    self.commit = commit;
                }
                self.send(m.from, MsgBody::HeartbeatResp { read_seq });
            }
            MsgBody::Snapshot(s) => {
                self.election_elapsed = 0;
                self.leader = Some(m.from);
                self.handle_snapshot(m.from, s);
            }
            MsgBody::TimeoutNow => {
                if self.promotable() {
                    self.campaign(true, true);
                }
            }
            _ => {}
        }
    }

    fn handle_append(&mut self, from: NodeId, prev_index: u64, prev_term: u64, entries: Vec<Entry>, commit: u64) {
        if prev_index < self.commit {
            // already committed that far; tell the leader where we are
            let c = self.commit;
            self.send(from, MsgBody::AppendResp { success: true, index: c, hint: 0 });
            return;
        }
        if self.log.term_at(prev_index) != Some(prev_term) {
            let hint = if prev_index > self.log.last_index() {
                self.log.last_index()
            } else {
                let t = self.log.term_at(prev_index).unwrap_or(0);
                self.log.first_index_of_term(t, prev_index).saturating_sub(1)
            };
            self.send(from, MsgBody::AppendResp { success: false, index: prev_index, hint });
            return;
        }
        let last_new = prev_index + entries.len() as u64;
        for e in entries {
            if e.index <= self.log.last_index() {
                if self.log.term_at(e.index) == Some(e.term) {
                    continue; // already have it
                }
                if self.cfg.bug != Some(RaftBug::SkipConflictCheck) {
                    assert!(e.index > self.commit, "raft safety: attempt to truncate committed entry {} (commit {})", e.index, self.commit);
                }
                self.truncate_log_from(e.index);
            }
            self.apply_conf_entry(&e);
            self.log.append(e.clone());
            self.unstable.push(e);
        }
        let new_commit = commit.min(last_new);
        if new_commit > self.commit {
            self.commit = new_commit;
        }
        self.send(from, MsgBody::AppendResp { success: true, index: last_new, hint: 0 });
    }

    fn truncate_log_from(&mut self, from: u64) {
        self.log.truncate_from(from);
        self.unstable.retain(|e| e.index < from);
        self.truncate_from = Some(self.truncate_from.map(|t| t.min(from)).unwrap_or(from));
        self.persisted = self.persisted.min(from - 1);
        self.rebuild_membership_after_truncate(from);
    }

    fn handle_snapshot(&mut self, from: NodeId, s: Snapshot) {
        let idx = s.meta.index;
        if idx <= self.commit {
            let c = self.commit;
            self.send(from, MsgBody::AppendResp { success: true, index: c, hint: 0 });
            return;
        }
        if self.log.term_at(idx) == Some(s.meta.term) {
            // we already have the entry the snapshot covers: just fast-forward commit
            self.commit = idx;
            self.send(from, MsgBody::AppendResp { success: true, index: idx, hint: 0 });
            return;
        }
        // restore from snapshot: all of our log is replaced
        self.log.reset_to_snapshot(idx, s.meta.term);
        self.unstable.clear();
        self.truncate_from = None;
        self.commit = idx;
        self.applied_cursor = idx;
        self.persisted = idx;
        self.base_membership = s.meta.membership.clone();
        self.conf_history.clear();
        self.membership = s.meta.membership.clone();
        self.snapshot = Some(s.clone());
        self.to_install = Some(s);
        self.send(from, MsgBody::AppendResp { success: true, index: idx, hint: 0 });
    }

    fn step_leader(&mut self, m: Message) {
        let ticks = self.ticks;
        let from = m.from;
        if let Some(p) = self.prs.get_mut(&from) {
            p.recent_active = true;
            p.last_ack_tick = ticks;
        } else {
            return; // not a member
        }
        match m.body {
            MsgBody::AppendResp { success, index, hint } => {
                if success {
                    let p = self.prs.get_mut(&from).unwrap();
                    while p.inflight.front().map(|x| *x <= index).unwrap_or(false) {
                        p.inflight.pop_front();
                    }
                    let advanced = index > p.matched;
                    if advanced {
                        p.matched = index;
                        p.stall = 0;
                    }
                    p.next = p.next.max(index + 1);
                    match p.state {
                        PState::Probe => {
                            p.state = PState::Replicate;
                            p.paused = false;
                            p.next = p.matched + 1;
                            p.inflight.clear();
                        }
                        PState::Snapshot(si) if p.matched >= si => {
                            p.state = PState::Probe;
                            p.paused = false;
                            p.next = p.matched + 1;
                        }
                        _ => {}
                    }
                    if self.maybe_commit() {
                        self.after_commit_advance();
                        self.broadcast_append();
                    } else if advanced {
                        self.send_append(from);
                    }
                    if self.transferee == Some(from) && self.prs[&from].matched == self.log.last_index() {
                        let t = self.term;
                        self.send_with_term(from, t, MsgBody::TimeoutNow);
                    }
                } else {
                    let p = self.prs.get_mut(&from).unwrap();
                    match p.state {
                        PState::Replicate => {
                            if index <= p.matched {
                                return; // stale
                            }
                            p.state = PState::Probe;
                            p.next = p.matched + 1;
                            p.inflight.clear();
                            p.paused = false;
                        }
                        PState::Probe => {
                            if index + 1 != p.next {
                                return; // stale rejection
                            }
                            p.next = (hint + 1).min(p.next - 1).max(p.matched + 1).max(1);
                            p.paused = false;
                        }
                        PState::Snapshot(_) => return,
                    }
                    self.send_append(from);
                }
            }
            MsgBody::HeartbeatResp { read_seq } => {
                {
                    let p = self.prs.get_mut(&from).unwrap();
                    p.read_ack = p.read_ack.max(read_seq);
                    p.paused = false;
                    if p.state == PState::Probe {
                        p.paused = false;
                    }
                }
                self.release_reads();
                let (matched, last) = (self.prs[&from].matched, self.log.last_index());
                if matched < last {
                    self.send_append(from);
                }
            }
            _ => {}
        }
    }

    fn release_reads(&mut self) {
        while let Some((ctx, seq, idx)) = self.pending_reads.front().cloned() {
            let id = self.id;
            let ok = |n: NodeId| n == id || self.prs.get(&n).map(|p| p.read_ack >= seq).unwrap_or(false);
            if self.membership.quorum(&ok) {
                self.read_states.push(ReadState { ctx, index: idx });
                self.pending_reads.pop_front();
            } else {
                break;
            }
        }
    }

    fn maybe_commit(&mut self) -> bool {
        let id = self.id;
        let persisted = self.persisted;
        let prs = &self.prs;
        let matched = |n: NodeId| if n == id { persisted } else { prs.get(&n).map(|p| p.matched).unwrap_or(0) };
        let mut q = self.membership.committed_index(&matched);
        if self.cfg.bug == Some(RaftBug::CommitMinority) {
            q = self.prs.iter().filter(|(n, _)| **n != id).map(|(_, p)| p.matched).max().unwrap_or(0);
        }
        let term_ok = self.cfg.bug == Some(RaftBug::CommitOldTerm) || self.log.term_at(q) == Some(self.term);
        if q > self.commit && term_ok && q <= self.log.last_index() {
            self.commit = q;
            true
        } else {
            false
        }
    }

    fn after_commit_advance(&mut self) {
        // reads waiting for a committed entry in our term can start now
        if !self.waiting_reads.is_empty() && self.log.term_at(self.commit) == Some(self.term) {
            for ctx in std::mem::take(&mut self.waiting_reads) {
                self.start_read(ctx);
            }
        }
        // auto-leave joint consensus once the entering entry is committed
        if self.role == Role::Leader && self.membership.is_joint() && self.pending_conf_index <= self.commit {
            let idx = self.append_entries(vec![EntryData::ConfLeaveJoint]);
            self.pending_conf_index = idx;
            self.broadcast_append();
        }
        // a leader that is no longer a voter steps down once the config excluding it commits
        if self.role == Role::Leader && !self.membership.is_voter(self.id) && self.pending_conf_index <= self.commit {
            let t = self.term;
            self.become_follower(t, None);
        }
    }

    // ------------------------------------------------------------ snapshots / compaction
    /// The state machine produced a snapshot at `index` (must be <= applied).
    pub fn compact(&mut self, index: u64, data: Vec<u8>) -> Result<(), String> {
        if index > self.applied_cursor {
            return Err(format!("cannot compact past applied index {} > {}", index, self.applied_cursor));
        }
        if index <= self.log.snap_index {
            return Ok(());
        }
        let term = self.log.term_at(index).ok_or("index not in log")?;
        let membership = self.membership_at(index);
        let snap = Snapshot { meta: SnapshotMeta { index, term, membership: membership.clone() }, data };
        self.log.compact_to(index, term);
        self.base_membership = membership;
        self.conf_history.retain(|(i, _)| *i > index);
        self.snapshot = Some(snap.clone());
        self.to_save = Some(snap);
        Ok(())
    }

    // ------------------------------------------------------------ ready / advance
    pub fn has_ready(&self) -> bool {
        self.hs_dirty
            || !self.msgs.is_empty()
            || !self.unstable.is_empty()
            || self.truncate_from.is_some()
            || self.to_install.is_some()
            || self.to_save.is_some()
            || !self.read_states.is_empty()
            || self.commit > self.applied_cursor
    }

    pub fn ready(&mut self) -> Ready {
        let mut rd = Ready::default();
        if self.hs_dirty {
            rd.hard_state = Some(HardState { term: self.term, vote: self.vote });
            self.hs_dirty = false;
        }
        rd.truncate_from = self.truncate_from.take();
        rd.entries = std::mem::take(&mut self.unstable);
        rd.install_snapshot = self.to_install.take();
        rd.save_snapshot = self.to_save.take();
        rd.messages = std::mem::take(&mut self.msgs);
        rd.read_states = std::mem::take(&mut self.read_states);
        // committed entries (the driver persists rd.entries before applying)
        let lo = self.applied_cursor + 1;
        if self.commit >= lo && rd.install_snapshot.is_none() {
            rd.committed = self.log.slice(lo, self.commit + 1, 1024);
        }
        debug_assert!(!rd.is_empty() || !self.has_ready());
        rd
    }

    /// Driver finished persisting + sending + applying `rd`.
    pub fn advance(&mut self, rd: &Ready) {
        if let Some(last) = rd.entries.last() {
            self.persisted = self.persisted.max(last.index).min(self.log.last_index());
        }
        if let Some(s) = &rd.install_snapshot {
            self.persisted = self.persisted.max(s.meta.index);
        }
        if let Some(last) = rd.committed.last() {
            self.applied_cursor = self.applied_cursor.max(last.index);
            // leader stepping down when removed from config
            for e in &rd.committed {
                if e.data.is_conf() {
                    // pending conf bookkeeping handled in after_commit_advance
                }
            }
        }
        if self.role == Role::Leader {
            if self.maybe_commit() {
                self.after_commit_advance();
                self.broadcast_append();
            }
            self.release_reads();
        }
    }
}
