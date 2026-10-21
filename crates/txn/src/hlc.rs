//! Hybrid logical clock: an alternative to a centralized timestamp oracle.
//! Timestamp = physical ms << LOGICAL_BITS | logical counter; always monotonic,
//! and `observe` merges timestamps from other nodes (causality).
use crate::percolator::LOGICAL_BITS;

pub struct Hlc {
    last: u64,
}

impl Hlc {
    pub fn new() -> Hlc {
        Hlc { last: 0 }
    }
    /// `now_ms` is the node's wall clock (injected for determinism).
    pub fn now(&mut self, now_ms: u64) -> u64 {
        let phys = now_ms << LOGICAL_BITS;
        self.last = if phys > self.last { phys } else { self.last + 1 };
        self.last
    }
    pub fn observe(&mut self, remote: u64) {
        if remote > self.last {
            self.last = remote;
        }
    }
}

impl Default for Hlc {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn monotonic_even_if_clock_goes_back() {
        let mut h = Hlc::new();
        let a = h.now(1000);
        let b = h.now(1000);
        let c = h.now(900);
        assert!(a < b && b < c);
        h.observe(5000 << LOGICAL_BITS);
        assert!(h.now(1000) > 5000 << LOGICAL_BITS);
    }
}
