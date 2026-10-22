//! Sharded LRU block cache keyed by (file_id, block offset).
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

const SHARDS: usize = 16;

struct Shard {
    cap: usize,
    used: usize,
    tick: u64,
    map: HashMap<(u64, u64), (Arc<Vec<u8>>, u64)>,
}

pub struct BlockCache {
    shards: Vec<Mutex<Shard>>,
    pub hits: AtomicU64,
    pub misses: AtomicU64,
}

impl BlockCache {
    pub fn new(capacity_bytes: usize) -> BlockCache {
        let cap = (capacity_bytes / SHARDS).max(1);
        BlockCache {
            shards: (0..SHARDS).map(|_| Mutex::new(Shard { cap, used: 0, tick: 0, map: HashMap::new() })).collect(),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }
    fn shard(&self, k: (u64, u64)) -> &Mutex<Shard> {
        let h = k.0.wrapping_mul(0x9E3779B97F4A7C15) ^ k.1.wrapping_mul(0xC2B2AE3D27D4EB4F);
        &self.shards[(h >> 32) as usize % SHARDS]
    }
    pub fn get(&self, file: u64, off: u64) -> Option<Arc<Vec<u8>>> {
        let mut s = self.shard((file, off)).lock();
        s.tick += 1;
        let t = s.tick;
        match s.map.get_mut(&(file, off)) {
            Some(e) => {
                e.1 = t;
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(e.0.clone())
            }
            None => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }
    pub fn insert(&self, file: u64, off: u64, block: Arc<Vec<u8>>) {
        let mut s = self.shard((file, off)).lock();
        s.tick += 1;
        let t = s.tick;
        s.used += block.len();
        if let Some(old) = s.map.insert((file, off), (block, t)) {
            s.used -= old.0.len();
        }
        while s.used > s.cap && s.map.len() > 1 {
            // evict least recently used (O(n) scan per shard; shards are small)
            let victim = s.map.iter().min_by_key(|(_, v)| v.1).map(|(k, _)| *k).unwrap();
            if let Some(v) = s.map.remove(&victim) {
                s.used -= v.0.len();
            }
        }
    }
    pub fn hit_ratio(&self) -> f64 {
        let h = self.hits.load(Ordering::Relaxed) as f64;
        let m = self.misses.load(Ordering::Relaxed) as f64;
        if h + m == 0.0 {
            0.0
        } else {
            h / (h + m)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn evicts_lru() {
        let c = BlockCache::new(16 * 100);
        for i in 0..2000u64 {
            c.insert(1, i, Arc::new(vec![0u8; 50]));
        }
        let total: usize = c.shards.iter().map(|s| s.lock().used).sum();
        assert!(total <= 16 * 100 + 16 * 50);
        assert!(c.get(1, 1999).is_some());
        assert!(c.get(1, 0).is_none());
    }
}
