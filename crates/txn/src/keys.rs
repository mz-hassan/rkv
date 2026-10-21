//! Engine key schema shared by server, snapshots and the transaction layer.
//!   raw    'r' ++ enc(key)                      -> [expire_ms:8][value]
//!   data   'd' ++ enc(key) ++ !start_ts         -> value
//!   lock   'l' ++ enc(key)                      -> Lock
//!   write  'w' ++ enc(key) ++ !commit_ts        -> WriteRec
//!   meta   'm' ++ ...                           (not part of any range)
//! `enc` is memcomparable and prefix-free, so range [a,b) of user keys maps to
//! engine range [cf++enc(a), cf++enc(b)) in every column family.

pub const CF_RAW: u8 = b'r';
pub const CF_DATA: u8 = b'd';
pub const CF_LOCK: u8 = b'l';
pub const CF_WRITE: u8 = b'w';
pub const CF_META: u8 = b'm';
pub const RANGE_CFS: [u8; 4] = [CF_RAW, CF_DATA, CF_LOCK, CF_WRITE];

/// Order-preserving, prefix-free encoding: 0x00 -> 00 FF, terminator 00 01.
pub fn enc(key: &[u8]) -> Vec<u8> {
    let mut o = Vec::with_capacity(key.len() + 2);
    for &b in key {
        o.push(b);
        if b == 0 {
            o.push(0xFF);
        }
    }
    o.extend_from_slice(&[0, 1]);
    o
}

/// Decode a key from the start of `b`; returns (key, bytes consumed).
pub fn dec(b: &[u8]) -> Option<(Vec<u8>, usize)> {
    let mut o = vec![];
    let mut i = 0;
    while i < b.len() {
        if b[i] == 0 {
            match b.get(i + 1)? {
                0xFF => {
                    o.push(0);
                    i += 2;
                }
                1 => return Some((o, i + 2)),
                _ => return None,
            }
        } else {
            o.push(b[i]);
            i += 1;
        }
    }
    None
}

pub fn raw_key(k: &[u8]) -> Vec<u8> {
    cf_key(CF_RAW, k)
}
pub fn lock_key(k: &[u8]) -> Vec<u8> {
    cf_key(CF_LOCK, k)
}
pub fn cf_key(cf: u8, k: &[u8]) -> Vec<u8> {
    let mut v = vec![cf];
    v.extend_from_slice(&enc(k));
    v
}
pub fn versioned_key(cf: u8, k: &[u8], ts: u64) -> Vec<u8> {
    let mut v = cf_key(cf, k);
    v.extend_from_slice(&(!ts).to_be_bytes());
    v
}
/// Parse the timestamp suffix of a data/write key.
pub fn key_ts(engine_key: &[u8]) -> Option<(Vec<u8>, u64)> {
    let (uk, n) = dec(&engine_key[1..])?;
    let rest = engine_key.get(1 + n..1 + n + 8)?;
    Some((uk, !u64::from_be_bytes(rest.try_into().ok()?)))
}
/// Engine key range of user-key range [start, end) in one cf. Empty end = unbounded.
pub fn cf_range(cf: u8, start: &[u8], end: &[u8]) -> (Vec<u8>, Vec<u8>) {
    let s = cf_key(cf, start);
    let e = if end.is_empty() { vec![cf + 1] } else { cf_key(cf, end) };
    (s, e)
}
/// First engine key past every version of `k` in `cf` (exclusive upper bound for per-key scans).
pub fn cf_key_end(cf: u8, k: &[u8]) -> Vec<u8> {
    let mut v = cf_key(cf, k);
    v.extend_from_slice(&[0xFF; 9]);
    v
}
pub fn meta_key(name: &str, id: u64) -> Vec<u8> {
    let mut v = vec![CF_META];
    v.extend_from_slice(name.as_bytes());
    v.push(b'/');
    v.extend_from_slice(&id.to_be_bytes());
    v
}

pub fn raw_value(expire_ms: u64, v: &[u8]) -> Vec<u8> {
    let mut o = expire_ms.to_le_bytes().to_vec();
    o.extend_from_slice(v);
    o
}
pub fn parse_raw_value(v: &[u8]) -> Option<(u64, &[u8])> {
    Some((u64::from_le_bytes(v.get(..8)?.try_into().ok()?), &v[8..]))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn enc_preserves_order_and_roundtrips() {
        let keys: Vec<Vec<u8>> = vec![vec![], vec![0], vec![0, 0], vec![0, 1], vec![1], b"a".to_vec(), b"a\0".to_vec(), b"a\0b".to_vec(), b"ab".to_vec(), b"b".to_vec(), vec![0xFF], vec![0xFF, 0xFF]];
        let mut sorted = keys.clone();
        sorted.sort();
        let mut by_enc = keys.clone();
        by_enc.sort_by_key(|k| enc(k));
        assert_eq!(sorted, by_enc);
        for k in &keys {
            let e = enc(k);
            assert_eq!(dec(&e).unwrap(), (k.clone(), e.len()));
        }
    }
    #[test]
    fn versions_sort_newest_first_within_key() {
        let a5 = versioned_key(CF_WRITE, b"a", 5);
        let a9 = versioned_key(CF_WRITE, b"a", 9);
        let a0 = versioned_key(CF_WRITE, b"a\0", 1);
        assert!(a9 < a5 && a5 < a0);
        assert_eq!(key_ts(&a9).unwrap(), (b"a".to_vec(), 9));
        assert!(cf_key_end(CF_WRITE, b"a") > a5 && cf_key_end(CF_WRITE, b"a") < a0);
    }
}
