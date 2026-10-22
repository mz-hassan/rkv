//! Bloom filter with double hashing (Kirsch-Mitzenmacher) from one 64-bit hash.
pub struct Bloom;

fn hash64(data: &[u8]) -> u64 {
    // FNV-1a followed by a 64-bit finalizer (splitmix) for good bit dispersion.
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in data {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h ^= h >> 30;
    h = h.wrapping_mul(0xbf58476d1ce4e5b9);
    h ^= h >> 27;
    h = h.wrapping_mul(0x94d049bb133111eb);
    h ^ (h >> 31)
}

impl Bloom {
    /// Build a filter for `keys`. Layout: bits ++ [k:1].
    pub fn build(keys: &[Vec<u8>], bits_per_key: usize) -> Vec<u8> {
        let nbits = (keys.len() * bits_per_key).max(64);
        let nbytes = nbits.div_ceil(8);
        let nbits = nbytes * 8;
        let k = ((bits_per_key as f64) * 0.69).round().clamp(1.0, 30.0) as u8;
        let mut v = vec![0u8; nbytes + 1];
        for key in keys {
            let h = hash64(key);
            let (mut a, d) = (h, (h >> 33) | 1);
            for _ in 0..k {
                let bit = (a % nbits as u64) as usize;
                v[bit / 8] |= 1 << (bit % 8);
                a = a.wrapping_add(d);
            }
        }
        v[nbytes] = k;
        v
    }

    pub fn may_contain(filter: &[u8], key: &[u8]) -> bool {
        if filter.len() < 2 {
            return true;
        }
        let nbytes = filter.len() - 1;
        let k = filter[nbytes];
        if k == 0 || k > 30 {
            return true;
        }
        let nbits = (nbytes * 8) as u64;
        let h = hash64(key);
        let (mut a, d) = (h, (h >> 33) | 1);
        for _ in 0..k {
            let bit = (a % nbits) as usize;
            if filter[bit / 8] & (1 << (bit % 8)) == 0 {
                return false;
            }
            a = a.wrapping_add(d);
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn no_false_negatives_and_fpr_reasonable() {
        let keys: Vec<Vec<u8>> = (0..10_000u32).map(|i| format!("key{i}").into_bytes()).collect();
        let f = Bloom::build(&keys, 10);
        assert!(keys.iter().all(|k| Bloom::may_contain(&f, k)));
        let fp = (0..100_000u32).filter(|i| Bloom::may_contain(&f, format!("other{i}").as_bytes())).count();
        let fpr = fp as f64 / 100_000.0;
        assert!(fpr < 0.02, "fpr {fpr}"); // theoretical ~0.8%
    }
}
