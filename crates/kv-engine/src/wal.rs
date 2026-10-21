//! Write-ahead log. Record = [crc32:4][len:4][payload:len], crc covers len+payload.
//! Recovery reads records until the first short/bad record (torn tail) and
//! reports how many valid bytes there were.
use crate::vfs::VFile;
use std::io;
use std::sync::Arc;

pub struct WalWriter {
    f: Arc<dyn VFile>,
}

impl WalWriter {
    pub fn new(f: Arc<dyn VFile>) -> WalWriter {
        WalWriter { f }
    }
    pub fn encode(payload: &[u8]) -> Vec<u8> {
        let mut rec = Vec::with_capacity(8 + payload.len());
        rec.extend_from_slice(&[0u8; 4]);
        rec.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        rec.extend_from_slice(payload);
        let crc = crc32fast::hash(&rec[4..]);
        rec[..4].copy_from_slice(&crc.to_le_bytes());
        rec
    }
    pub fn append(&self, payload: &[u8]) -> io::Result<u64> {
        let rec = Self::encode(payload);
        self.f.append(&rec)?;
        Ok(rec.len() as u64)
    }
    pub fn append_raw(&self, bytes: &[u8]) -> io::Result<()> {
        self.f.append(bytes)
    }
    pub fn sync(&self) -> io::Result<()> {
        self.f.sync()
    }
    pub fn len(&self) -> u64 {
        self.f.len()
    }
    pub fn is_empty(&self) -> bool {
        self.f.is_empty()
    }
}

/// Returns (records, valid_len). Stops at the first invalid record.
pub fn read_all(f: &dyn VFile) -> io::Result<(Vec<Vec<u8>>, u64)> {
    let total = f.len() as usize;
    let data = f.read_at(0, total)?;
    let mut out = vec![];
    let mut pos = 0usize;
    while pos + 8 <= data.len() {
        let crc = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
        let len = u32::from_le_bytes(data[pos + 4..pos + 8].try_into().unwrap()) as usize;
        if pos + 8 + len > data.len() {
            break;
        }
        if crc32fast::hash(&data[pos + 4..pos + 8 + len]) != crc {
            break;
        }
        out.push(data[pos + 8..pos + 8 + len].to_vec());
        pos += 8 + len;
    }
    Ok((out, pos as u64))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vfs::MemFs;
    #[test]
    fn torn_tail_is_ignored() {
        let fs = MemFs::new();
        let v = fs.as_vfs();
        let f = v.create("/w").unwrap();
        let w = WalWriter::new(f.clone());
        w.append(b"one").unwrap();
        w.append(b"two").unwrap();
        w.sync().unwrap();
        w.append(b"three-unsynced").unwrap();
        for seed in 0..50 {
            let fs2 = MemFs::new();
            let v2 = fs2.as_vfs();
            let f2 = v2.create("/w").unwrap();
            let w2 = WalWriter::new(f2.clone());
            w2.append(b"one").unwrap();
            w2.append(b"two").unwrap();
            w2.sync().unwrap();
            w2.append(b"three-unsynced").unwrap();
            fs2.crash(seed);
            let (recs, _) = read_all(&*f2).unwrap();
            assert!(recs.len() >= 2 && recs[0] == b"one" && recs[1] == b"two");
            if recs.len() == 3 {
                assert_eq!(recs[2], b"three-unsynced");
            }
        }
    }
}
