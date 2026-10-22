//! YCSB workloads A-F.
use crate::dist::{key_for, value_for, Zipf};
use rand::Rng;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Dist {
    Uniform,
    Zipfian,
    Latest,
}

#[derive(Clone, Debug)]
pub struct Workload {
    pub name: char,
    pub read: f64,
    pub update: f64,
    pub insert: f64,
    pub scan: f64,
    pub rmw: f64,
    pub dist: Dist,
}

impl Workload {
    pub fn by_name(c: char) -> Option<Workload> {
        let w = |name, read, update, insert, scan, rmw, dist| Workload { name, read, update, insert, scan, rmw, dist };
        Some(match c.to_ascii_lowercase() {
            'a' => w('a', 0.5, 0.5, 0.0, 0.0, 0.0, Dist::Zipfian),
            'b' => w('b', 0.95, 0.05, 0.0, 0.0, 0.0, Dist::Zipfian),
            'c' => w('c', 1.0, 0.0, 0.0, 0.0, 0.0, Dist::Zipfian),
            'd' => w('d', 0.95, 0.0, 0.05, 0.0, 0.0, Dist::Latest),
            'e' => w('e', 0.0, 0.0, 0.05, 0.95, 0.0, Dist::Zipfian),
            'f' => w('f', 0.5, 0.0, 0.0, 0.0, 0.5, Dist::Zipfian),
            // not part of YCSB: 100% updates / 100% reads on uniform keys, for replication benchmarks
            'w' => w('w', 0.0, 1.0, 0.0, 0.0, 0.0, Dist::Uniform),
            'r' => w('r', 1.0, 0.0, 0.0, 0.0, 0.0, Dist::Uniform),
            _ => return None,
        })
    }
    pub fn write_fraction(&self) -> f64 {
        self.update + self.insert + self.rmw
    }
}

#[derive(Debug)]
pub enum Op {
    Read(Vec<u8>),
    Update(Vec<u8>, Vec<u8>),
    Insert(Vec<u8>, Vec<u8>),
    Scan(Vec<u8>, usize),
    Rmw(Vec<u8>, Vec<u8>),
}

impl Op {
    pub fn kind(&self) -> &'static str {
        match self {
            Op::Read(_) => "read",
            Op::Update(..) => "update",
            Op::Insert(..) => "insert",
            Op::Scan(..) => "scan",
            Op::Rmw(..) => "rmw",
        }
    }
}

/// Shared across threads: the number of records inserted so far.
pub struct OpGen {
    pub w: Workload,
    pub zipf: Zipf,
    pub inserted: Arc<AtomicU64>,
    pub value_size: usize,
    pub records: u64,
}

impl OpGen {
    pub fn new(w: Workload, records: u64, theta: f64, value_size: usize) -> OpGen {
        OpGen {
            zipf: Zipf::new(records.max(2), if w.dist == Dist::Uniform { 0.0 } else { theta }),
            w,
            inserted: Arc::new(AtomicU64::new(records)),
            value_size,
            records,
        }
    }

    fn pick<R: Rng>(&self, rng: &mut R) -> u64 {
        let n = self.inserted.load(Ordering::Relaxed).max(1);
        match self.w.dist {
            Dist::Latest => {
                // newest records are the hottest: offset from the latest insert, zipfian
                let off = self.zipf.next(rng).min(n - 1);
                n - 1 - off
            }
            _ => self.zipf.next_scrambled(rng) % n,
        }
    }

    pub fn next<R: Rng>(&self, rng: &mut R) -> Op {
        let x: f64 = rng.gen();
        let w = &self.w;
        if x < w.read {
            Op::Read(key_for(self.pick(rng)))
        } else if x < w.read + w.update {
            let i = self.pick(rng);
            Op::Update(key_for(i), value_for(rng.gen(), self.value_size))
        } else if x < w.read + w.update + w.insert {
            let i = self.inserted.fetch_add(1, Ordering::Relaxed);
            Op::Insert(key_for(i), value_for(i, self.value_size))
        } else if x < w.read + w.update + w.insert + w.scan {
            Op::Scan(key_for(self.pick(rng)), rng.gen_range(1..=100))
        } else {
            let i = self.pick(rng);
            Op::Rmw(key_for(i), value_for(rng.gen(), self.value_size))
        }
    }
}
