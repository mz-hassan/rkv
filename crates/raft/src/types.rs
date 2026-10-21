//! Wire/storage types for the sans-IO Raft implementation.
use std::collections::BTreeSet;

pub type NodeId = u64;

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct HardState {
    pub term: u64,
    pub vote: Option<NodeId>,
}

/// Cluster membership. `outgoing` non-empty means we are in the joint
/// configuration C(old,new): decisions need a majority of BOTH sets.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Membership {
    pub voters: BTreeSet<NodeId>,
    pub outgoing: BTreeSet<NodeId>,
    pub learners: BTreeSet<NodeId>,
}

impl Membership {
    pub fn new(voters: impl IntoIterator<Item = NodeId>) -> Membership {
        Membership { voters: voters.into_iter().collect(), outgoing: BTreeSet::new(), learners: BTreeSet::new() }
    }
    pub fn is_joint(&self) -> bool {
        !self.outgoing.is_empty()
    }
    pub fn is_voter(&self, id: NodeId) -> bool {
        self.voters.contains(&id) || self.outgoing.contains(&id)
    }
    pub fn contains(&self, id: NodeId) -> bool {
        self.is_voter(id) || self.learners.contains(&id)
    }
    pub fn all_voters(&self) -> BTreeSet<NodeId> {
        self.voters.union(&self.outgoing).copied().collect()
    }
    pub fn all_nodes(&self) -> BTreeSet<NodeId> {
        let mut s = self.all_voters();
        s.extend(self.learners.iter().copied());
        s
    }
    fn majority(set: &BTreeSet<NodeId>, pred: &dyn Fn(NodeId) -> bool) -> bool {
        if set.is_empty() {
            return true;
        }
        set.iter().filter(|n| pred(**n)).count() > set.len() / 2
    }
    /// True if `pred` holds for a quorum (of both halves when joint).
    pub fn quorum(&self, pred: &dyn Fn(NodeId) -> bool) -> bool {
        Self::majority(&self.voters, pred) && Self::majority(&self.outgoing, pred)
    }
    /// Highest index acknowledged by a quorum (min over both halves when joint).
    pub fn committed_index(&self, matched: &dyn Fn(NodeId) -> u64) -> u64 {
        fn one(set: &BTreeSet<NodeId>, matched: &dyn Fn(NodeId) -> u64) -> u64 {
            if set.is_empty() {
                return u64::MAX;
            }
            let mut v: Vec<u64> = set.iter().map(|n| matched(*n)).collect();
            v.sort_unstable_by(|a, b| b.cmp(a));
            v[set.len() / 2]
        }
        one(&self.voters, matched).min(one(&self.outgoing, matched))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EntryData {
    Noop,
    Normal(Vec<u8>),
    /// Enter joint consensus: new voters/learners; old voters become `outgoing`.
    ConfEnterJoint { voters: BTreeSet<NodeId>, learners: BTreeSet<NodeId> },
    ConfLeaveJoint,
    /// Replace membership directly (only legal if the voter set is unchanged, or at bootstrap).
    ConfSet(Membership),
}

impl EntryData {
    pub fn is_conf(&self) -> bool {
        matches!(self, EntryData::ConfEnterJoint { .. } | EntryData::ConfLeaveJoint | EntryData::ConfSet(_))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub term: u64,
    pub index: u64,
    pub data: EntryData,
}

impl Entry {
    pub fn size(&self) -> usize {
        24 + match &self.data {
            EntryData::Normal(d) => d.len(),
            _ => 0,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotMeta {
    pub index: u64,
    pub term: u64,
    pub membership: Membership,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Snapshot {
    pub meta: SnapshotMeta,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MsgBody {
    RequestVote { pre: bool, last_index: u64, last_term: u64, transfer: bool },
    VoteResp { pre: bool, granted: bool },
    Append { prev_index: u64, prev_term: u64, entries: Vec<Entry>, commit: u64 },
    AppendResp { success: bool, index: u64, hint: u64 },
    Heartbeat { commit: u64, read_seq: u64 },
    HeartbeatResp { read_seq: u64 },
    Snapshot(Snapshot),
    TimeoutNow,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Message {
    pub from: NodeId,
    pub to: NodeId,
    pub term: u64,
    pub body: MsgBody,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadState {
    pub ctx: u64,
    pub index: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Follower,
    PreCandidate,
    Candidate,
    Leader,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProposeError {
    NotLeader(Option<NodeId>),
    TransferInProgress,
    Busy,
    ConfChangePending,
    InvalidConfChange(String),
}
