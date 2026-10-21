//! In-memory view of the Raft log (persisted prefix + unstable suffix).
use crate::types::*;

pub struct RaftLog {
    pub snap_index: u64,
    pub snap_term: u64,
    entries: Vec<Entry>, // entries[i].index == snap_index + 1 + i
}

impl RaftLog {
    pub fn new(snap_index: u64, snap_term: u64, entries: Vec<Entry>) -> RaftLog {
        let l = RaftLog { snap_index, snap_term, entries };
        debug_assert!(l.entries.iter().enumerate().all(|(i, e)| e.index == snap_index + 1 + i as u64));
        l
    }
    pub fn first_index(&self) -> u64 {
        self.snap_index + 1
    }
    pub fn last_index(&self) -> u64 {
        self.snap_index + self.entries.len() as u64
    }
    pub fn last_term(&self) -> u64 {
        self.term_at(self.last_index()).unwrap_or(self.snap_term)
    }
    pub fn term_at(&self, index: u64) -> Option<u64> {
        if index == self.snap_index {
            return Some(self.snap_term);
        }
        if index < self.snap_index || index > self.last_index() {
            return None;
        }
        Some(self.entries[(index - self.snap_index - 1) as usize].term)
    }
    pub fn get(&self, index: u64) -> Option<&Entry> {
        if index <= self.snap_index || index > self.last_index() {
            return None;
        }
        Some(&self.entries[(index - self.snap_index - 1) as usize])
    }
    /// Entries in [lo, hi) limited to `max` entries.
    pub fn slice(&self, lo: u64, hi: u64, max: usize) -> Vec<Entry> {
        if lo > self.last_index() || lo <= self.snap_index {
            return vec![];
        }
        let hi = hi.min(self.last_index() + 1);
        let hi = hi.min(lo + max as u64);
        self.entries[(lo - self.snap_index - 1) as usize..(hi - self.snap_index - 1) as usize].to_vec()
    }
    pub fn append(&mut self, e: Entry) {
        debug_assert_eq!(e.index, self.last_index() + 1);
        self.entries.push(e);
    }
    /// Remove all entries with index >= `from`.
    pub fn truncate_from(&mut self, from: u64) {
        if from <= self.snap_index {
            self.entries.clear();
        } else if from <= self.last_index() {
            self.entries.truncate((from - self.snap_index - 1) as usize);
        }
    }
    /// Drop entries up to and including `index` (becomes the new snapshot point).
    pub fn compact_to(&mut self, index: u64, term: u64) {
        if index <= self.snap_index {
            return;
        }
        if index >= self.last_index() {
            self.entries.clear();
        } else {
            self.entries.drain(..(index - self.snap_index) as usize);
        }
        self.snap_index = index;
        self.snap_term = term;
    }
    pub fn reset_to_snapshot(&mut self, index: u64, term: u64) {
        self.entries.clear();
        self.snap_index = index;
        self.snap_term = term;
    }
    /// Index of the first entry in the log with the given term, searching down from `from`.
    pub fn first_index_of_term(&self, term: u64, from: u64) -> u64 {
        let mut i = from;
        while i > self.snap_index + 1 && self.term_at(i - 1) == Some(term) {
            i -= 1;
        }
        i
    }
    pub fn uncommitted_len(&self, commit: u64) -> u64 {
        self.last_index().saturating_sub(commit)
    }
}
