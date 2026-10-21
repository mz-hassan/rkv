//! SSTable file format.
//!
//!   [data block]* [filter block] [index block] [footer]
//!   block   = entries ++ crc32(entries):4          (entries as in key::Entry::encode)
//!   index   = block whose entries are (last_ikey of data block, value = offset:8 ++ size:8)
//!   filter  = bloom bytes ++ crc32:4 (over user keys)
//!   footer  = filter_off:8 filter_len:8 index_off:8 index_len:8 count:8 magic:8   (48 bytes)
//!
//! Compression is not implemented (documented in DECISIONS.md).
use crate::bloom::Bloom;
use crate::cache::BlockCache;
use crate::key::{cmp_ikey, make_ikey, ts_of, ukey, Entry};
use crate::vfs::VFile;
use std::cmp::Ordering;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering as AO};
use std::sync::Arc;

const MAGIC: u64 = 0x524b565f53535431; // "RKV_SST1"
const FOOTER: usize = 48;

fn corrupt(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.to_string())
}

pub struct SstBuilder {
    f: Arc<dyn VFile>,
    block_size: usize,
    bits_per_key: usize,
    cur: Vec<u8>,
    cur_last: Vec<u8>,
    index: Vec<Entry>,
    ukeys: Vec<Vec<u8>>,
    offset: u64,
    pub count: u64,
    pub smallest: Vec<u8>,
    pub largest: Vec<u8>,
}

impl SstBuilder {
    pub fn new(f: Arc<dyn VFile>, block_size: usize, bits_per_key: usize) -> SstBuilder {
        SstBuilder {
            f,
            block_size,
            bits_per_key,
            cur: vec![],
            cur_last: vec![],
            index: vec![],
            ukeys: vec![],
            offset: 0,
            count: 0,
            smallest: vec![],
            largest: vec![],
        }
    }

    pub fn add(&mut self, e: &Entry) -> io::Result<()> {
        if self.count == 0 {
            self.smallest = e.ikey.clone();
        }
        debug_assert!(self.count == 0 || cmp_ikey(&self.largest, &e.ikey) == Ordering::Less);
        self.largest = e.ikey.clone();
        if self.ukeys.last().map(|k| k.as_slice()) != Some(e.ukey()) {
            self.ukeys.push(e.ukey().to_vec());
        }
        e.encode_into(&mut self.cur);
        self.cur_last = e.ikey.clone();
        self.count += 1;
        if self.cur.len() >= self.block_size {
            self.flush_block()?;
        }
        Ok(())
    }

    fn write_block(&mut self, mut body: Vec<u8>) -> io::Result<(u64, u64)> {
        let crc = crc32fast::hash(&body);
        body.extend_from_slice(&crc.to_le_bytes());
        let off = self.offset;
        self.f.append(&body)?;
        self.offset += body.len() as u64;
        Ok((off, body.len() as u64))
    }

    fn flush_block(&mut self) -> io::Result<()> {
        if self.cur.is_empty() {
            return Ok(());
        }
        let body = std::mem::take(&mut self.cur);
        let (off, size) = self.write_block(body)?;
        let mut v = off.to_le_bytes().to_vec();
        v.extend_from_slice(&size.to_le_bytes());
        self.index.push(Entry { ikey: self.cur_last.clone(), kind: 1, value: v });
        Ok(())
    }

    /// Finish the file; returns total size. Caller must sync.
    pub fn finish(mut self) -> io::Result<u64> {
        self.flush_block()?;
        let filter = Bloom::build(&self.ukeys, self.bits_per_key);
        let (foff, flen) = self.write_block(filter)?;
        let mut ib = vec![];
        for e in &self.index {
            e.encode_into(&mut ib);
        }
        let (ioff, ilen) = self.write_block(ib)?;
        let mut foot = vec![];
        for v in [foff, flen, ioff, ilen, self.count, MAGIC] {
            foot.extend_from_slice(&v.to_le_bytes());
        }
        self.f.append(&foot)?;
        self.offset += foot.len() as u64;
        Ok(self.offset)
    }
}

pub struct SstReader {
    pub id: u64,
    f: Arc<dyn VFile>,
    index: Vec<(Vec<u8>, u64, u64)>, // last ikey, offset, size
    filter: Vec<u8>,
    cache: Option<Arc<BlockCache>>,
    pub count: u64,
    pub size: u64,
    pub blocks_read: AtomicU64,
    pub bloom_negatives: AtomicU64,
    pub bloom_positives: AtomicU64,
    pub bloom_false_positives: AtomicU64,
}

fn check_block(raw: &[u8]) -> io::Result<&[u8]> {
    if raw.len() < 4 {
        return Err(corrupt("short block"));
    }
    let (body, c) = raw.split_at(raw.len() - 4);
    if crc32fast::hash(body) != u32::from_le_bytes(c.try_into().unwrap()) {
        return Err(corrupt("block crc mismatch"));
    }
    Ok(body)
}

fn decode_entries(mut b: &[u8]) -> io::Result<Vec<Entry>> {
    let mut v = vec![];
    while !b.is_empty() {
        let (e, n) = Entry::decode(b).ok_or_else(|| corrupt("bad entry"))?;
        v.push(e);
        b = &b[n..];
    }
    Ok(v)
}

impl SstReader {
    pub fn open(id: u64, f: Arc<dyn VFile>, cache: Option<Arc<BlockCache>>) -> io::Result<SstReader> {
        let size = f.len();
        if (size as usize) < FOOTER {
            return Err(corrupt("sst too small"));
        }
        let foot = f.read_at(size - FOOTER as u64, FOOTER)?;
        let n = |i: usize| u64::from_le_bytes(foot[i * 8..i * 8 + 8].try_into().unwrap());
        if n(5) != MAGIC {
            return Err(corrupt("bad sst magic"));
        }
        let filter_raw = f.read_at(n(0), n(1) as usize)?;
        let filter = check_block(&filter_raw)?.to_vec();
        let index_raw = f.read_at(n(2), n(3) as usize)?;
        let ents = decode_entries(check_block(&index_raw)?)?;
        let index = ents
            .into_iter()
            .map(|e| {
                (e.ikey, u64::from_le_bytes(e.value[0..8].try_into().unwrap()), u64::from_le_bytes(e.value[8..16].try_into().unwrap()))
            })
            .collect();
        Ok(SstReader {
            id,
            f,
            index,
            filter,
            cache,
            count: n(4),
            size,
            blocks_read: AtomicU64::new(0),
            bloom_negatives: AtomicU64::new(0),
            bloom_positives: AtomicU64::new(0),
            bloom_false_positives: AtomicU64::new(0),
        })
    }

    fn read_block(&self, bi: usize) -> io::Result<Arc<Vec<u8>>> {
        let (_, off, size) = &self.index[bi];
        if let Some(c) = &self.cache {
            if let Some(b) = c.get(self.id, *off) {
                return Ok(b);
            }
        }
        self.blocks_read.fetch_add(1, AO::Relaxed);
        let raw = self.f.read_at(*off, *size as usize)?;
        if raw.len() != *size as usize {
            return Err(corrupt("short read"));
        }
        let body = Arc::new(check_block(&raw)?.to_vec());
        if let Some(c) = &self.cache {
            c.insert(self.id, *off, body.clone());
        }
        Ok(body)
    }

    pub fn may_contain(&self, uk: &[u8]) -> bool {
        let r = Bloom::may_contain(&self.filter, uk);
        if !r {
            self.bloom_negatives.fetch_add(1, AO::Relaxed);
        }
        r
    }

    fn first_block_ge(&self, ikey: &[u8]) -> usize {
        self.index.partition_point(|(last, _, _)| cmp_ikey(last, ikey) == Ordering::Less)
    }

    /// Newest version of `uk` with ts <= `ts` in this file.
    pub fn get(&self, uk: &[u8], ts: u64) -> io::Result<Option<Entry>> {
        if !self.may_contain(uk) {
            return Ok(None);
        }
        let target = make_ikey(uk, ts);
        let bi = self.first_block_ge(&target);
        let mut found = None;
        if bi < self.index.len() {
            let blk = self.read_block(bi)?;
            let mut b = &blk[..];
            while !b.is_empty() {
                let (e, n) = Entry::decode(b).ok_or_else(|| corrupt("bad entry"))?;
                b = &b[n..];
                if cmp_ikey(&e.ikey, &target) != Ordering::Less {
                    if ukey(&e.ikey) == uk && ts_of(&e.ikey) <= ts {
                        found = Some(e);
                    }
                    break;
                }
            }
        }
        if found.is_some() {
            self.bloom_positives.fetch_add(1, AO::Relaxed);
        } else {
            self.bloom_false_positives.fetch_add(1, AO::Relaxed);
        }
        Ok(found)
    }

    /// Iterator over entries with ukey in [start, end) (empty end = unbounded).
    pub fn iter(self: &Arc<Self>, start: &[u8], end: &[u8]) -> SstIter {
        let bi = self.first_block_ge(&make_ikey(start, u64::MAX));
        SstIter { r: self.clone(), bi, buf: vec![], pos: 0, start: start.to_vec(), end: end.to_vec(), err: None, done: false }
    }
}

pub struct SstIter {
    r: Arc<SstReader>,
    bi: usize,
    buf: Vec<Entry>,
    pos: usize,
    start: Vec<u8>,
    end: Vec<u8>,
    pub err: Option<io::Error>,
    done: bool,
}

impl Iterator for SstIter {
    type Item = Entry;
    fn next(&mut self) -> Option<Entry> {
        loop {
            if self.done {
                return None;
            }
            if self.pos < self.buf.len() {
                let e = self.buf[self.pos].clone();
                self.pos += 1;
                if e.ukey() < self.start.as_slice() {
                    continue;
                }
                if !self.end.is_empty() && e.ukey() >= self.end.as_slice() {
                    self.done = true;
                    return None;
                }
                return Some(e);
            }
            if self.bi >= self.r.index.len() {
                self.done = true;
                return None;
            }
            match self.r.read_block(self.bi).and_then(|b| decode_entries(&b)) {
                Ok(v) => {
                    self.buf = v;
                    self.pos = 0;
                    self.bi += 1;
                }
                Err(e) => {
                    self.err = Some(e);
                    self.done = true;
                    return None;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key::KIND_PUT;
    use crate::vfs::MemFs;

    fn build(n: u32) -> Arc<SstReader> {
        let fs = MemFs::new().as_vfs();
        let f = fs.create("/t.sst").unwrap();
        let mut b = SstBuilder::new(f.clone(), 256, 10);
        for i in 0..n {
            b.add(&Entry::new(format!("k{i:05}").as_bytes(), 7, KIND_PUT, format!("v{i}").as_bytes())).unwrap();
        }
        b.finish().unwrap();
        Arc::new(SstReader::open(1, f, None).unwrap())
    }

    #[test]
    fn point_and_scan() {
        let r = build(1000);
        assert_eq!(r.get(b"k00500", 10).unwrap().unwrap().value, b"v500");
        assert!(r.get(b"k00500", 3).unwrap().is_none());
        assert!(r.get(b"nope", 10).unwrap().is_none());
        let all: Vec<_> = r.iter(b"", b"").collect();
        assert_eq!(all.len(), 1000);
        let part: Vec<_> = r.iter(b"k00010", b"k00020").collect();
        assert_eq!(part.len(), 10);
    }

    #[test]
    fn corruption_detected() {
        let fs = MemFs::new();
        let v = fs.as_vfs();
        let f = v.create("/t.sst").unwrap();
        let mut b = SstBuilder::new(f.clone(), 256, 10);
        for i in 0..100u32 {
            b.add(&Entry::new(format!("k{i:05}").as_bytes(), 1, KIND_PUT, b"x")).unwrap();
        }
        b.finish().unwrap();
        // flip a byte in the first block by rewriting the file
        let mut data = f.read_at(0, f.len() as usize).unwrap();
        data[10] ^= 0xff;
        let f2 = v.create("/t2.sst").unwrap();
        f2.append(&data).unwrap();
        let r = Arc::new(SstReader::open(2, f2, None).unwrap());
        let it = r.iter(b"", b"");
        let n = it.count();
        assert!(n < 100);
    }
}

impl SstBuilder {
    /// Bytes written so far plus the pending block (for output-file rolling).
    pub fn offset_bytes(&self) -> u64 {
        self.offset + self.cur.len() as u64
    }
}
