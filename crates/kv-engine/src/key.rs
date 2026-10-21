//! Internal key = user_key ++ big-endian(!ts). Ordering: user_key ascending,
//! then ts DESCENDING (newest version first). Because user keys vary in length
//! we must compare the two parts separately, never the raw bytes.
use std::cmp::Ordering;

pub const TS_LEN: usize = 8;
pub const KIND_PUT: u8 = 1;
pub const KIND_DEL: u8 = 0;

pub fn make_ikey(ukey: &[u8], ts: u64) -> Vec<u8> {
    let mut v = Vec::with_capacity(ukey.len() + TS_LEN);
    v.extend_from_slice(ukey);
    v.extend_from_slice(&(!ts).to_be_bytes());
    v
}

pub fn ukey(ikey: &[u8]) -> &[u8] {
    &ikey[..ikey.len() - TS_LEN]
}

pub fn ts_of(ikey: &[u8]) -> u64 {
    let mut b = [0u8; 8];
    b.copy_from_slice(&ikey[ikey.len() - TS_LEN..]);
    !u64::from_be_bytes(b)
}

pub fn cmp_ikey(a: &[u8], b: &[u8]) -> Ordering {
    ukey(a).cmp(ukey(b)).then_with(|| a[a.len() - TS_LEN..].cmp(&b[b.len() - TS_LEN..]))
}

/// One versioned entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub ikey: Vec<u8>,
    pub kind: u8,
    pub value: Vec<u8>,
}

impl Entry {
    pub fn new(ukey: &[u8], ts: u64, kind: u8, value: &[u8]) -> Entry {
        Entry { ikey: make_ikey(ukey, ts), kind, value: value.to_vec() }
    }
    pub fn ukey(&self) -> &[u8] {
        ukey(&self.ikey)
    }
    pub fn ts(&self) -> u64 {
        ts_of(&self.ikey)
    }
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&(self.ikey.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.ikey);
        out.push(self.kind);
        out.extend_from_slice(&(self.value.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.value);
    }
    /// Decode one entry; returns (entry, bytes consumed) or None on short/garbled input.
    pub fn decode(buf: &[u8]) -> Option<(Entry, usize)> {
        let kl = u32::from_le_bytes(buf.get(0..4)?.try_into().ok()?) as usize;
        if kl < TS_LEN {
            return None;
        }
        let ikey = buf.get(4..4 + kl)?.to_vec();
        let kind = *buf.get(4 + kl)?;
        let vl = u32::from_le_bytes(buf.get(5 + kl..9 + kl)?.try_into().ok()?) as usize;
        let value = buf.get(9 + kl..9 + kl + vl)?.to_vec();
        Some((Entry { ikey, kind, value }, 9 + kl + vl))
    }
}

impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Entry {
    fn cmp(&self, other: &Self) -> Ordering {
        cmp_ikey(&self.ikey, &other.ikey)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn newest_first_and_prefix_safe() {
        let a = make_ikey(b"a", 5);
        let a2 = make_ikey(b"a", 9);
        let ab = make_ikey(b"ab", 1);
        assert_eq!(cmp_ikey(&a2, &a), Ordering::Less);
        assert_eq!(cmp_ikey(&a, &ab), Ordering::Less);
        assert_eq!(ts_of(&a2), 9);
    }
    #[test]
    fn roundtrip() {
        let e = Entry::new(b"k", 3, KIND_PUT, b"v");
        let mut b = vec![];
        e.encode_into(&mut b);
        assert_eq!(Entry::decode(&b).unwrap().0, e);
    }
}
