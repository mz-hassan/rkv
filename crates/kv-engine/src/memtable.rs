//! Skiplist memtable. Nodes live in an arena (`Vec`) and link by index, so the
//! structure is 100% safe Rust. Writers are serialized by the caller (the
//! write-group leader); readers use the RwLock read side concurrently.
use crate::key::{cmp_ikey, make_ikey, ts_of, ukey, Entry, KIND_DEL};
use parking_lot::RwLock;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::cmp::Ordering;
use std::sync::atomic::{AtomicUsize, Ordering as AO};

const MAX_H: usize = 16;
const NIL: u32 = u32::MAX;

struct Node {
    e: Entry,
    next: [u32; MAX_H],
}

struct List {
    nodes: Vec<Node>,
    head: [u32; MAX_H],
    height: usize,
    rng: ChaCha8Rng,
}

pub struct Memtable {
    l: RwLock<List>,
    size: AtomicUsize,
    pub id: u64,
}

impl Memtable {
    pub fn new(id: u64) -> Memtable {
        Memtable {
            l: RwLock::new(List { nodes: vec![], head: [NIL; MAX_H], height: 1, rng: ChaCha8Rng::seed_from_u64(id ^ 0x5eed) }),
            size: AtomicUsize::new(0),
            id,
        }
    }

    pub fn approx_size(&self) -> usize {
        self.size.load(AO::Relaxed)
    }
    pub fn len(&self) -> usize {
        self.l.read().nodes.len()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[allow(clippy::needless_range_loop)] // levels index several parallel arrays
    pub fn insert(&self, e: Entry) {
        self.size.fetch_add(e.ikey.len() + e.value.len() + 48, AO::Relaxed);
        let mut l = self.l.write();
        let mut h = 1;
        while h < MAX_H && l.rng.gen_range(0..4) == 0 {
            h += 1;
        }
        // find predecessors at every level
        let mut prev = [NIL; MAX_H]; // NIL = head
        let mut cur = NIL;
        for lvl in (0..l.height.max(h)).rev() {
            loop {
                let nx = if cur == NIL { l.head[lvl] } else { l.nodes[cur as usize].next[lvl] };
                if nx != NIL && cmp_ikey(&l.nodes[nx as usize].e.ikey, &e.ikey) == Ordering::Less {
                    cur = nx;
                } else {
                    break;
                }
            }
            prev[lvl] = cur;
        }
        let idx = l.nodes.len() as u32;
        let mut node = Node { e, next: [NIL; MAX_H] };
        for lvl in 0..h {
            node.next[lvl] = if prev[lvl] == NIL { l.head[lvl] } else { l.nodes[prev[lvl] as usize].next[lvl] };
        }
        l.nodes.push(node);
        for lvl in 0..h {
            if prev[lvl] == NIL {
                l.head[lvl] = idx;
            } else {
                l.nodes[prev[lvl] as usize].next[lvl] = idx;
            }
        }
        if h > l.height {
            l.height = h;
        }
    }

    fn seek_idx(l: &List, ikey: &[u8]) -> u32 {
        let mut cur = NIL;
        for lvl in (0..l.height).rev() {
            loop {
                let nx = if cur == NIL { l.head[lvl] } else { l.nodes[cur as usize].next[lvl] };
                if nx != NIL && cmp_ikey(&l.nodes[nx as usize].e.ikey, ikey) == Ordering::Less {
                    cur = nx;
                } else {
                    break;
                }
            }
        }
        if cur == NIL {
            l.head[0]
        } else {
            l.nodes[cur as usize].next[0]
        }
    }

    /// Newest version of `uk` with ts <= `ts`.
    pub fn get(&self, uk: &[u8], ts: u64) -> Option<Entry> {
        let l = self.l.read();
        let i = Self::seek_idx(&l, &make_ikey(uk, ts));
        if i == NIL {
            return None;
        }
        let e = &l.nodes[i as usize].e;
        if ukey(&e.ikey) == uk && ts_of(&e.ikey) <= ts {
            Some(e.clone())
        } else {
            None
        }
    }

    /// All entries with ukey in [start, end) (end empty = unbounded), in order.
    pub fn range(&self, start: &[u8], end: &[u8]) -> Vec<Entry> {
        let l = self.l.read();
        let mut i = Self::seek_idx(&l, &make_ikey(start, u64::MAX));
        let mut out = vec![];
        while i != NIL {
            let e = &l.nodes[i as usize].e;
            if !end.is_empty() && ukey(&e.ikey) >= end {
                break;
            }
            out.push(e.clone());
            i = l.nodes[i as usize].next[0];
        }
        out
    }

    /// Like `range` but copies at most `max` entries. The flag says whether more entries in range were left behind, so callers
    /// that merge several sources know this one is incomplete beyond its last returned key.
    pub fn range_n(&self, start: &[u8], end: &[u8], max: usize) -> (Vec<Entry>, bool) {
        let l = self.l.read();
        let mut i = Self::seek_idx(&l, &make_ikey(start, u64::MAX));
        let mut out = vec![];
        while i != NIL {
            let e = &l.nodes[i as usize].e;
            if !end.is_empty() && ukey(&e.ikey) >= end {
                break;
            }
            if out.len() >= max {
                return (out, true);
            }
            out.push(e.clone());
            i = l.nodes[i as usize].next[0];
        }
        (out, false)
    }

    pub fn all(&self) -> Vec<Entry> {
        self.range(&[], &[])
    }

    pub fn has_tombstone_for(&self, uk: &[u8]) -> bool {
        self.range_n(uk, &[], 1).0.first().map(|e| e.ukey() == uk && e.kind == KIND_DEL).unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key::KIND_PUT;
    #[test]
    fn ordered_and_versioned() {
        let m = Memtable::new(1);
        for i in (0..500u32).rev() {
            m.insert(Entry::new(format!("k{i:04}").as_bytes(), 1, KIND_PUT, b"a"));
            m.insert(Entry::new(format!("k{i:04}").as_bytes(), 5, KIND_PUT, b"b"));
        }
        let all = m.all();
        assert_eq!(all.len(), 1000);
        assert!(all.windows(2).all(|w| w[0] < w[1]));
        assert_eq!(m.get(b"k0042", 3).unwrap().value, b"a");
        assert_eq!(m.get(b"k0042", 9).unwrap().value, b"b");
        assert!(m.get(b"k0042", 0).is_none());
        assert!(m.get(b"zzz", 9).is_none());
        assert_eq!(m.range(b"k0010", b"k0013").len(), 6);
    }
}
