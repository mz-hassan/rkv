//! Durable Raft log/hard-state/snapshot storage on its own WAL.
//!
//! WAL records (framed by kv-engine's WAL framing: crc32 + len):
//!   1 HardState(term, vote)   2 Entries(n, entries...)   3 Truncate(from)   4 SnapRef(meta)
//! Snapshot payloads live in `snap-<index>.dat` (crc-protected, written tmp+rename
//! BEFORE the SnapRef record that points at it).
use kv_engine::vfs::{VFile, Vfs};
use kv_engine::wal::{self, WalWriter};
use raft::codec::*;
use raft::{Entry, HardState, InitialState, Ready, Snapshot, SnapshotMeta};
use std::io;
use std::sync::Arc;

const REC_HS: u8 = 1;
const REC_ENTRIES: u8 = 2;
const REC_TRUNC: u8 = 3;
const REC_SNAP: u8 = 4;

pub struct RaftStorage {
    fs: Arc<dyn Vfs>,
    dir: String,
    wal: Arc<dyn VFile>,
    hs: HardState,
    snap: Option<SnapshotMeta>,
    snap_data: Option<Vec<u8>>,
    entries: Vec<Entry>,
    /// rewrite the WAL when it exceeds this many bytes beyond the live data
    pub rewrite_slack: u64,
}

fn bad(m: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, m.to_string())
}

fn wal_path(dir: &str) -> String {
    format!("{dir}/raft.wal")
}
fn snap_path(dir: &str, idx: u64) -> String {
    format!("{dir}/snap-{idx:020}.dat")
}

impl RaftStorage {
    /// Open (recovering from a torn tail if needed).
    pub fn open(fs: Arc<dyn Vfs>, dir: &str) -> io::Result<(RaftStorage, InitialState)> {
        fs.mkdir(dir)?;
        let wp = wal_path(dir);
        let mut hs = HardState::default();
        let mut snap: Option<SnapshotMeta> = None;
        let mut entries: Vec<Entry> = vec![];
        let mut needs_rewrite = false;
        let file = if fs.exists(&wp) {
            let f = fs.open(&wp)?;
            let (recs, valid) = wal::read_all(&*f)?;
            if valid < f.len() {
                needs_rewrite = true; // torn/corrupt tail: rewrite so new appends are reachable
            }
            for r in recs {
                let mut rd = R(&r);
                match rd.u8().ok_or_else(|| bad("empty record"))? {
                    REC_HS => {
                        let term = rd.u64().ok_or_else(|| bad("hs"))?;
                        let has = rd.bool().ok_or_else(|| bad("hs"))?;
                        let v = rd.u64().ok_or_else(|| bad("hs"))?;
                        hs = HardState { term, vote: if has { Some(v) } else { None } };
                    }
                    REC_ENTRIES => {
                        let n = rd.u64().ok_or_else(|| bad("n"))?;
                        for _ in 0..n {
                            let e = get_entry(&mut rd).ok_or_else(|| bad("entry"))?;
                            let last = snap.as_ref().map(|s| s.index).unwrap_or(0) + entries.len() as u64;
                            if e.index != last + 1 {
                                return Err(bad("non-contiguous entries in raft wal"));
                            }
                            entries.push(e);
                        }
                    }
                    REC_TRUNC => {
                        let from = rd.u64().ok_or_else(|| bad("trunc"))?;
                        let base = snap.as_ref().map(|s| s.index).unwrap_or(0);
                        if from <= base {
                            entries.clear();
                        } else {
                            entries.truncate((from - base - 1) as usize);
                        }
                    }
                    REC_SNAP => {
                        let index = rd.u64().ok_or_else(|| bad("snap"))?;
                        let term = rd.u64().ok_or_else(|| bad("snap"))?;
                        let membership = get_membership(&mut rd).ok_or_else(|| bad("snap"))?;
                        let base = snap.as_ref().map(|s| s.index).unwrap_or(0);
                        if index > base {
                            // drop entries covered by the snapshot; if we don't have the entry at
                            // `index` with the same term the whole log is superseded
                            let keep_from = if index >= base + entries.len() as u64 { None } else { Some((index - base) as usize) };
                            match keep_from {
                                Some(k) if entries[k - 1].term == term => {
                                    entries.drain(..k);
                                }
                                _ => entries.clear(),
                            }
                            snap = Some(SnapshotMeta { index, term, membership });
                        }
                    }
                    _ => return Err(bad("unknown raft wal record")),
                }
            }
            f
        } else {
            fs.create(&wp)?
        };
        let snap_data = match &snap {
            Some(s) => {
                let f = fs.open(&snap_path(dir, s.index))?;
                let raw = f.read_at(0, f.len() as usize)?;
                if raw.len() < 4 || crc32fast::hash(&raw[4..]) != u32::from_le_bytes(raw[..4].try_into().unwrap()) {
                    return Err(bad("snapshot data crc mismatch"));
                }
                Some(raw[4..].to_vec())
            }
            None => None,
        };
        let mut st = RaftStorage { fs, dir: dir.to_string(), wal: file, hs: hs.clone(), snap: snap.clone(), snap_data: snap_data.clone(), entries: entries.clone(), rewrite_slack: 4 << 20 };
        if needs_rewrite {
            st.rewrite()?;
        }
        let init = InitialState { hard_state: hs, snapshot: snap, entries, applied: 0, snapshot_data: snap_data };
        Ok((st, init))
    }

    pub fn hard_state(&self) -> &HardState {
        &self.hs
    }
    pub fn snapshot(&self) -> Option<Snapshot> {
        self.snap.clone().map(|m| Snapshot { meta: m, data: self.snap_data.clone().unwrap_or_default() })
    }
    pub fn last_index(&self) -> u64 {
        self.snap.as_ref().map(|s| s.index).unwrap_or(0) + self.entries.len() as u64
    }
    pub fn wal_len(&self) -> u64 {
        self.wal.len()
    }

    fn write_snapshot_file(&self, s: &Snapshot) -> io::Result<()> {
        let tmp = format!("{}/snap.tmp", self.dir);
        let f = self.fs.create(&tmp)?;
        let mut body = crc32fast::hash(&s.data).to_le_bytes().to_vec();
        body.extend_from_slice(&s.data);
        f.append(&body)?;
        f.sync()?;
        self.fs.rename(&tmp, &snap_path(&self.dir, s.meta.index))
    }

    fn rec_hs(hs: &HardState) -> Vec<u8> {
        let mut w = W::default();
        w.u8(REC_HS);
        w.u64(hs.term);
        w.bool(hs.vote.is_some());
        w.u64(hs.vote.unwrap_or(0));
        w.0
    }
    fn rec_snap(m: &SnapshotMeta) -> Vec<u8> {
        let mut w = W::default();
        w.u8(REC_SNAP);
        w.u64(m.index);
        w.u64(m.term);
        put_membership(&mut w, &m.membership);
        w.0
    }
    fn rec_entries(es: &[Entry]) -> Vec<u8> {
        let mut w = W::default();
        w.u8(REC_ENTRIES);
        w.u64(es.len() as u64);
        for e in es {
            put_entry(&mut w, e);
        }
        w.0
    }

    /// Durably persist everything in `rd` (call BEFORE sending rd.messages).
    pub fn save(&mut self, rd: &Ready, sync: bool) -> io::Result<()> {
        let mut buf: Vec<u8> = vec![];
        let mut frame = |p: &[u8], buf: &mut Vec<u8>| buf.extend_from_slice(&WalWriter::encode(p));
        let mut new_snap: Option<Snapshot> = None;
        if let Some(s) = rd.install_snapshot.as_ref().or(rd.save_snapshot.as_ref()) {
            self.write_snapshot_file(s)?;
            frame(&Self::rec_snap(&s.meta), &mut buf);
            new_snap = Some(s.clone());
        }
        if let Some(from) = rd.truncate_from {
            let mut w = W::default();
            w.u8(REC_TRUNC);
            w.u64(from);
            frame(&w.0, &mut buf);
        }
        if !rd.entries.is_empty() {
            frame(&Self::rec_entries(&rd.entries), &mut buf);
        }
        if let Some(hs) = &rd.hard_state {
            frame(&Self::rec_hs(hs), &mut buf);
        }
        if buf.is_empty() {
            return Ok(());
        }
        self.wal.append(&buf)?;
        if sync {
            self.wal.sync()?;
        }
        // mirror in memory
        if let Some(s) = new_snap {
            let base = self.snap.as_ref().map(|x| x.index).unwrap_or(0);
            if s.meta.index > base {
                let k = (s.meta.index - base) as usize;
                if k <= self.entries.len() && self.entries[k - 1].term == s.meta.term {
                    self.entries.drain(..k);
                } else {
                    self.entries.clear();
                }
                let old = self.snap.as_ref().map(|x| x.index);
                self.snap = Some(s.meta.clone());
                self.snap_data = Some(s.data);
                if let Some(o) = old {
                    let _ = self.fs.remove(&snap_path(&self.dir, o));
                }
            }
        }
        if let Some(from) = rd.truncate_from {
            let base = self.snap.as_ref().map(|s| s.index).unwrap_or(0);
            if from <= base {
                self.entries.clear();
            } else {
                self.entries.truncate((from - base - 1) as usize);
            }
        }
        for e in &rd.entries {
            self.entries.push(e.clone());
        }
        if let Some(hs) = &rd.hard_state {
            self.hs = hs.clone();
        }
        let live: u64 = self.entries.iter().map(|e| e.size() as u64 + 8).sum();
        if self.wal.len() > live + self.rewrite_slack {
            self.rewrite()?;
        }
        Ok(())
    }

    /// Replace the WAL with a minimal one holding only live state.
    pub fn rewrite(&mut self) -> io::Result<()> {
        let tmp = format!("{}/raft.wal.new", self.dir);
        let f = self.fs.create(&tmp)?;
        let mut buf = vec![];
        if let Some(m) = &self.snap {
            buf.extend_from_slice(&WalWriter::encode(&Self::rec_snap(m)));
        }
        buf.extend_from_slice(&WalWriter::encode(&Self::rec_hs(&self.hs)));
        for chunk in self.entries.chunks(256) {
            buf.extend_from_slice(&WalWriter::encode(&Self::rec_entries(chunk)));
        }
        f.append(&buf)?;
        f.sync()?;
        self.fs.rename(&tmp, &wal_path(&self.dir))?;
        self.wal = f;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kv_engine::MemFs;
    use raft::EntryData;

    fn ent(i: u64, t: u64) -> Entry {
        Entry { term: t, index: i, data: EntryData::Normal(vec![i as u8; 10]) }
    }

    #[test]
    fn persists_truncates_and_recovers_from_torn_writes() {
        for seed in 0..40u64 {
            let fs = MemFs::new();
            let v = fs.as_vfs();
            let (mut st, _) = RaftStorage::open(v.clone(), "/r").unwrap();
            let rd = Ready { hard_state: Some(HardState { term: 2, vote: Some(1) }), entries: (1..=5).map(|i| ent(i, 1)).collect(), ..Ready::default() };
            st.save(&rd, true).unwrap();
            let rd = Ready { truncate_from: Some(4), entries: vec![ent(4, 2), ent(5, 2)], ..Ready::default() };
            st.save(&rd, true).unwrap();
            // unsynced extra
            let rd = Ready { entries: vec![ent(6, 2)], ..Ready::default() };
            st.save(&rd, false).unwrap();
            fs.crash(seed);
            let (mut st2, init) = RaftStorage::open(v.clone(), "/r").unwrap();
            assert_eq!(init.hard_state, HardState { term: 2, vote: Some(1) });
            assert!(init.entries.len() >= 5);
            assert_eq!(init.entries[3].term, 2);
            // appending after recovery must work and survive another crash
            let next = init.entries.len() as u64 + 1;
            st2.save(&Ready { entries: vec![ent(next, 3)], ..Ready::default() }, true).unwrap();
            fs.crash(seed + 1000);
            let (_, init2) = RaftStorage::open(v, "/r").unwrap();
            assert_eq!(init2.entries.len() as u64, next);
        }
    }

    #[test]
    fn snapshot_roundtrip() {
        let fs = MemFs::new();
        let v = fs.as_vfs();
        let (mut st, _) = RaftStorage::open(v.clone(), "/r").unwrap();
        st.save(&Ready { entries: (1..=10).map(|i| ent(i, 1)).collect(), ..Ready::default() }, true).unwrap();
        let snap = Snapshot { meta: SnapshotMeta { index: 6, term: 1, membership: raft::Membership::new([1, 2, 3]) }, data: b"state".to_vec() };
        st.save(&Ready { save_snapshot: Some(snap), ..Ready::default() }, true).unwrap();
        fs.crash(1);
        let (_, init) = RaftStorage::open(v, "/r").unwrap();
        assert_eq!(init.snapshot.unwrap().index, 6);
        assert_eq!(init.entries.len(), 4);
        assert_eq!(init.snapshot_data.unwrap(), b"state");
    }
}
