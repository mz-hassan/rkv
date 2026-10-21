//! Versions (immutable file-set snapshots) and the manifest edit log.
use crate::sstable::SstReader;
use std::io;
use std::sync::Arc;

pub struct FileMeta {
    pub id: u64,
    pub size: u64,
    pub smallest: Vec<u8>, // internal keys
    pub largest: Vec<u8>,
    pub max_ts: u64,
    pub reader: Arc<SstReader>,
}

impl FileMeta {
    pub fn small_u(&self) -> &[u8] {
        crate::key::ukey(&self.smallest)
    }
    pub fn large_u(&self) -> &[u8] {
        crate::key::ukey(&self.largest)
    }
}

#[derive(Clone)]
pub struct Version {
    pub levels: Vec<Vec<Arc<FileMeta>>>,
    level_max: Vec<u64>,
}

impl Version {
    pub fn new(n: usize) -> Version {
        Version { levels: vec![vec![]; n], level_max: vec![0; n] }
    }
    pub fn level_size(&self, l: usize) -> u64 {
        self.levels[l].iter().map(|f| f.size).sum()
    }
    pub fn level_max_ts(&self, l: usize) -> u64 {
        self.level_max[l]
    }
    pub fn total_files(&self) -> usize {
        self.levels.iter().map(|l| l.len()).sum()
    }
    /// Files in `level` whose user-key range overlaps [lo, hi] (inclusive).
    pub fn overlapping(&self, level: usize, lo: &[u8], hi: &[u8]) -> Vec<Arc<FileMeta>> {
        self.levels[level].iter().filter(|f| f.large_u() >= lo && f.small_u() <= hi).cloned().collect()
    }
    pub fn normalize(&mut self) {
        self.levels[0].sort_by(|a, b| b.max_ts.cmp(&a.max_ts).then(b.id.cmp(&a.id)));
        for l in 1..self.levels.len() {
            self.levels[l].sort_by(|a, b| crate::key::cmp_ikey(&a.smallest, &b.smallest));
        }
        self.level_max = self.levels.iter().map(|l| l.iter().map(|f| f.max_ts).max().unwrap_or(0)).collect();
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Edit {
    AddFile { level: u32, id: u64, size: u64, smallest: Vec<u8>, largest: Vec<u8>, max_ts: u64 },
    DelFile { level: u32, id: u64 },
    LogNumber(u64),
    NextFile(u64),
    LastSeq(u64),
}

fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    out.extend_from_slice(&(b.len() as u32).to_le_bytes());
    out.extend_from_slice(b);
}

pub fn encode_edits(edits: &[Edit]) -> Vec<u8> {
    let mut o = vec![];
    for e in edits {
        match e {
            Edit::AddFile { level, id, size, smallest, largest, max_ts } => {
                o.push(1);
                o.extend_from_slice(&level.to_le_bytes());
                o.extend_from_slice(&id.to_le_bytes());
                o.extend_from_slice(&size.to_le_bytes());
                o.extend_from_slice(&max_ts.to_le_bytes());
                put_bytes(&mut o, smallest);
                put_bytes(&mut o, largest);
            }
            Edit::DelFile { level, id } => {
                o.push(2);
                o.extend_from_slice(&level.to_le_bytes());
                o.extend_from_slice(&id.to_le_bytes());
            }
            Edit::LogNumber(n) => {
                o.push(3);
                o.extend_from_slice(&n.to_le_bytes());
            }
            Edit::NextFile(n) => {
                o.push(4);
                o.extend_from_slice(&n.to_le_bytes());
            }
            Edit::LastSeq(n) => {
                o.push(5);
                o.extend_from_slice(&n.to_le_bytes());
            }
        }
    }
    o
}

fn bad() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "bad manifest record")
}

pub fn decode_edits(mut b: &[u8]) -> io::Result<Vec<Edit>> {
    fn u32_(b: &mut &[u8]) -> io::Result<u32> {
        let v = u32::from_le_bytes(b.get(..4).ok_or_else(bad)?.try_into().unwrap());
        *b = &b[4..];
        Ok(v)
    }
    fn u64_(b: &mut &[u8]) -> io::Result<u64> {
        let v = u64::from_le_bytes(b.get(..8).ok_or_else(bad)?.try_into().unwrap());
        *b = &b[8..];
        Ok(v)
    }
    fn bytes(b: &mut &[u8]) -> io::Result<Vec<u8>> {
        let n = u32_(b)? as usize;
        let v = b.get(..n).ok_or_else(bad)?.to_vec();
        *b = &b[n..];
        Ok(v)
    }
    let mut out = vec![];
    while !b.is_empty() {
        let tag = b[0];
        b = &b[1..];
        out.push(match tag {
            1 => {
                let level = u32_(&mut b)?;
                let id = u64_(&mut b)?;
                let size = u64_(&mut b)?;
                let max_ts = u64_(&mut b)?;
                let smallest = bytes(&mut b)?;
                let largest = bytes(&mut b)?;
                Edit::AddFile { level, id, size, smallest, largest, max_ts }
            }
            2 => Edit::DelFile { level: u32_(&mut b)?, id: u64_(&mut b)? },
            3 => Edit::LogNumber(u64_(&mut b)?),
            4 => Edit::NextFile(u64_(&mut b)?),
            5 => Edit::LastSeq(u64_(&mut b)?),
            _ => return Err(bad()),
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn edit_roundtrip() {
        let e = vec![
            Edit::AddFile { level: 1, id: 7, size: 99, smallest: b"a".to_vec(), largest: b"z".to_vec(), max_ts: 5 },
            Edit::DelFile { level: 0, id: 3 },
            Edit::LogNumber(4),
            Edit::NextFile(10),
            Edit::LastSeq(77),
        ];
        assert_eq!(decode_edits(&encode_edits(&e)).unwrap(), e);
    }
}
