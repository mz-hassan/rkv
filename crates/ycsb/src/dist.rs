//! Key distributions: uniform, YCSB scrambled Zipfian (Gray et al.), "latest".
use rand::Rng;

pub fn fnv64(mut x: u64) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for _ in 0..8 {
        h ^= x & 0xff;
        h = h.wrapping_mul(0x100000001b3);
        x >>= 8;
    }
    h
}

pub fn key_for(i: u64) -> Vec<u8> {
    format!("user{:016x}", fnv64(i)).into_bytes()
}

pub fn value_for(i: u64, len: usize) -> Vec<u8> {
    let mut v = Vec::with_capacity(len);
    let mut x = fnv64(i ^ 0xdead_beef);
    while v.len() < len {
        v.extend_from_slice(&x.to_le_bytes());
        x = fnv64(x);
    }
    v.truncate(len);
    v
}

#[derive(Clone)]
pub struct Zipf {
    n: u64,
    theta: f64,
    alpha: f64,
    zetan: f64,
    eta: f64,
}

fn zeta(n: u64, theta: f64) -> f64 {
    (1..=n).map(|i| 1.0 / (i as f64).powf(theta)).sum()
}

impl Zipf {
    /// Items 0..n with skew `theta` (0 = uniform-ish, 0.99 = YCSB default).
    pub fn new(n: u64, theta: f64) -> Zipf {
        let zetan = zeta(n.min(10_000_000), theta); // exact for small n; capped for speed
        let zeta2 = zeta(2, theta);
        let alpha = 1.0 / (1.0 - theta);
        let eta = (1.0 - (2.0 / n as f64).powf(1.0 - theta)) / (1.0 - zeta2 / zetan);
        Zipf { n, theta, alpha, zetan, eta }
    }
    pub fn next<R: Rng>(&self, rng: &mut R) -> u64 {
        if self.theta <= 0.0 {
            return rng.gen_range(0..self.n);
        }
        let u: f64 = rng.gen();
        let uz = u * self.zetan;
        if uz < 1.0 {
            return 0;
        }
        if uz < 1.0 + 0.5f64.powf(self.theta) {
            return 1;
        }
        ((self.n as f64) * (self.eta * u - self.eta + 1.0).powf(self.alpha)) as u64 % self.n
    }
    /// Scrambled: spread hot items over the key space (as YCSB does).
    pub fn next_scrambled<R: Rng>(&self, rng: &mut R) -> u64 {
        fnv64(self.next(rng)) % self.n
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand_chacha::ChaCha8Rng;
    #[test]
    fn zipf_is_skewed_and_in_range() {
        let z = Zipf::new(1000, 0.99);
        let mut rng = ChaCha8Rng::seed_from_u64(1);
        let mut c = vec![0u32; 1000];
        for _ in 0..100_000 {
            let k = z.next(&mut rng) as usize;
            assert!(k < 1000);
            c[k] += 1;
        }
        assert!(c[0] > 10_000, "item 0 should dominate, got {}", c[0]);
        assert!(c[0] > c[500] * 20);
        let z0 = Zipf::new(1000, 0.0);
        let mut c0 = vec![0u32; 1000];
        for _ in 0..100_000 {
            c0[z0.next(&mut rng) as usize] += 1;
        }
        assert!(*c0.iter().max().unwrap() < 300);
    }
}
