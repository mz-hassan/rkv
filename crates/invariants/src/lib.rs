#![allow(clippy::type_complexity, clippy::too_many_arguments, clippy::new_without_default)]
//! Safety invariants asserted continuously during simulation and chaos runs.
pub mod bank;

use raft::{Entry, NodeId, Raft, Role};
use std::collections::{BTreeMap, HashMap};

#[derive(Default)]
pub struct RaftInvariants {
    leaders_by_term: BTreeMap<u64, NodeId>,
    /// every entry that some node has applied (hence committed), by index
    committed: BTreeMap<u64, Entry>,
    last_seen: HashMap<NodeId, (u64, u64, u64)>, // node -> (term, last_index, last_term) while leading
    commit_seen: HashMap<NodeId, u64>,
    pub violations: Vec<String>,
    pub checks: u64,
}

impl RaftInvariants {
    pub fn new() -> RaftInvariants {
        RaftInvariants::default()
    }

    fn fail(&mut self, msg: String) {
        if self.violations.len() < 20 {
            self.violations.push(msg);
        }
    }

    /// A node restarted: its volatile trackers reset.
    pub fn node_restarted(&mut self, id: NodeId) {
        self.last_seen.remove(&id);
        self.commit_seen.remove(&id);
    }

    /// V-ES and V-LC: call whenever a node becomes leader.
    pub fn on_become_leader(&mut self, n: &Raft) {
        self.checks += 1;
        let (id, term) = (n.id(), n.term());
        if let Some(prev) = self.leaders_by_term.insert(term, id) {
            if prev != id {
                self.fail(format!("election safety: term {term} has two leaders {prev} and {id}"));
            }
        }
        // leader completeness: every committed entry must be in the new leader's log
        let mut errs = vec![];
        for (i, e) in self.committed.range(..) {
            if *i <= n.snapshot_index() {
                continue; // compacted into a snapshot that was itself built from applied state
            }
            match n.entry(*i) {
                Some(le) if le == e => {}
                Some(le) => errs.push(format!("leader completeness: leader {id} term {term} has {:?} at {i}, committed {:?}", le, e)),
                None => errs.push(format!("leader completeness: leader {id} term {term} lacks committed index {i} (last {})", n.last_index())),
            }
        }
        for e in errs.into_iter().take(3) {
            self.fail(e);
        }
    }

    /// V-SM: no two nodes apply different entries at the same index.
    pub fn on_apply(&mut self, node: NodeId, e: &Entry) {
        self.checks += 1;
        match self.committed.get(&e.index) {
            Some(prev) if prev != e => {
                self.fail(format!("state machine safety: node {node} applied {:?} at {} but {:?} was applied elsewhere", e, e.index, prev))
            }
            Some(_) => {}
            None => {
                self.committed.insert(e.index, e.clone());
            }
        }
    }

    /// V-LA, V-MONO and basic sanity; call after every step on a node.
    pub fn check_node(&mut self, n: &Raft) {
        self.checks += 1;
        let id = n.id();
        if n.applied_index() > n.commit_index() && n.applied_index() > n.snapshot_index() {
            self.fail(format!("node {id}: applied {} > commit {}", n.applied_index(), n.commit_index()));
        }
        if n.commit_index() > n.last_index() && n.commit_index() > n.snapshot_index() {
            self.fail(format!("node {id}: commit {} > last_index {}", n.commit_index(), n.last_index()));
        }
        let prev = self.commit_seen.insert(id, n.commit_index()).unwrap_or(0);
        if n.commit_index() < prev {
            self.fail(format!("node {id}: commit index went backwards {prev} -> {}", n.commit_index()));
        }
        if n.role() == Role::Leader {
            let (term, last) = (n.term(), n.last_index());
            if let Some((pt, pl, plt)) = self.last_seen.get(&id).copied() {
                if pt == term && pl > n.snapshot_index() && n.term_at(pl) != Some(plt) {
                    self.fail(format!("leader append-only: leader {id} term {term} changed entry at {pl}"));
                }
            }
            self.last_seen.insert(id, (term, last, n.term_at(last).unwrap_or(0)));
        } else {
            self.last_seen.remove(&id);
        }
    }

    /// V-LM: if two logs contain an entry with the same index and term, the logs are identical up to there.
    pub fn check_log_matching(&mut self, nodes: &[&Raft]) {
        self.checks += 1;
        for (ai, a) in nodes.iter().enumerate() {
            for b in nodes.iter().skip(ai + 1) {
                let lo = a.first_index().max(b.first_index());
                let hi = a.last_index().min(b.last_index());
                if lo > hi {
                    continue;
                }
                let mut top = None;
                for i in (lo..=hi).rev() {
                    if a.term_at(i) == b.term_at(i) && a.term_at(i).is_some() {
                        top = Some(i);
                        break;
                    }
                }
                if let Some(t) = top {
                    for i in lo..=t {
                        if a.entry(i) != b.entry(i) {
                            self.fail(format!("log matching: nodes {} and {} agree at ({t}) but differ at index {i}", a.id(), b.id()));
                            return;
                        }
                    }
                }
            }
        }
    }

    pub fn committed_len(&self) -> usize {
        self.committed.len()
    }
}
