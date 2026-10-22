//! Log-linear latency histogram (HDR-like): 5% max relative error, mergeable.
#[derive(Clone)]
pub struct Hist {
    buckets: Vec<u64>,
    pub count: u64,
    pub sum_us: u64,
    pub max_us: u64,
}

const SUB: usize = 32; // sub-buckets per power of two

fn index(us: u64) -> usize {
    if us < SUB as u64 {
        return us as usize;
    }
    let lg = 63 - us.leading_zeros() as usize; // >= 5
    let shift = lg - 5;
    let sub = ((us >> shift) as usize) - SUB; // 0..SUB
    SUB + (lg - 5) * SUB + sub
}

fn value_at(idx: usize) -> u64 {
    if idx < SUB {
        return idx as u64;
    }
    let g = (idx - SUB) / SUB;
    let sub = (idx - SUB) % SUB;
    ((SUB + sub) as u64) << g
}

impl Hist {
    pub fn new() -> Hist {
        Hist { buckets: vec![0; SUB + 60 * SUB], count: 0, sum_us: 0, max_us: 0 }
    }
    pub fn record_us(&mut self, us: u64) {
        let i = index(us).min(self.buckets.len() - 1);
        self.buckets[i] += 1;
        self.count += 1;
        self.sum_us += us;
        self.max_us = self.max_us.max(us);
    }
    pub fn merge(&mut self, o: &Hist) {
        for (a, b) in self.buckets.iter_mut().zip(&o.buckets) {
            *a += *b;
        }
        self.count += o.count;
        self.sum_us += o.sum_us;
        self.max_us = self.max_us.max(o.max_us);
    }
    pub fn percentile_us(&self, p: f64) -> u64 {
        if self.count == 0 {
            return 0;
        }
        let target = ((p / 100.0) * self.count as f64).ceil().max(1.0) as u64;
        let mut seen = 0;
        for (i, c) in self.buckets.iter().enumerate() {
            seen += c;
            if seen >= target {
                return value_at(i).min(self.max_us);
            }
        }
        self.max_us
    }
    pub fn mean_us(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.sum_us as f64 / self.count as f64
        }
    }
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "count": self.count, "mean_us": self.mean_us(),
            "p50_us": self.percentile_us(50.0), "p90_us": self.percentile_us(90.0), "p99_us": self.percentile_us(99.0),
            "p999_us": self.percentile_us(99.9), "max_us": self.max_us,
        })
    }
}

impl Default for Hist {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn percentiles_are_within_error_bound() {
        let mut h = Hist::new();
        for v in 1..=100_000u64 {
            h.record_us(v);
        }
        for (p, want) in [(50.0, 50_000.0), (99.0, 99_000.0), (99.9, 99_900.0)] {
            let got = h.percentile_us(p) as f64;
            assert!((got - want).abs() / want < 0.04, "p{p}: {got} vs {want}");
        }
        let mut a = Hist::new();
        a.record_us(10);
        let mut b = Hist::new();
        b.record_us(1000);
        a.merge(&b);
        assert_eq!(a.count, 2);
    }
}
