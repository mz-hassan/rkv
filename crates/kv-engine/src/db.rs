//! The LSM database: write path (group commit), read path, flush, leveled and
//! size-tiered compaction, recovery.
use crate::cache::BlockCache;
use crate::key::*;
use crate::memtable::Memtable;
use crate::sstable::{SstBuilder, SstIter, SstReader};
use crate::version::*;
use crate::vfs::Vfs;
use crate::wal::{self, WalWriter};
use parking_lot::{Condvar, Mutex};
use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AO};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncPolicy {
    /// fsync every write batch individually.
    Always,
    /// one fsync per write group (group commit).
    Group,
    /// fsync if at least this many ms passed since the last sync.
    Interval(u64),
    /// never fsync the WAL (only on rotation).
    Never,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompactionStyle {
    Leveled,
    SizeTiered,
}

#[derive(Clone, Debug)]
pub struct Options {
    pub memtable_size: usize,
    pub block_size: usize,
    pub bloom_bits_per_key: usize,
    pub l0_compaction_trigger: usize,
    pub l0_slowdown: usize,
    pub l0_stop: usize,
    pub level_base_bytes: u64,
    pub level_multiplier: u64,
    pub max_levels: usize,
    pub target_file_size: u64,
    pub cache_bytes: usize,
    pub sync: SyncPolicy,
    /// run flush/compaction on a background thread (false = inline, deterministic)
    pub background: bool,
    pub compaction: CompactionStyle,
    pub max_immutables: usize,
    pub tier_min_merge: usize,
    /// compaction write rate limit in bytes/sec
    pub compaction_rate_limit: Option<u64>,
    pub strict_fsync: bool,
}

impl Default for Options {
    fn default() -> Options {
        Options {
            memtable_size: 4 << 20,
            block_size: 4096,
            bloom_bits_per_key: 10,
            l0_compaction_trigger: 4,
            l0_slowdown: 8,
            l0_stop: 12,
            level_base_bytes: 16 << 20,
            level_multiplier: 10,
            max_levels: 7,
            target_file_size: 4 << 20,
            cache_bytes: 32 << 20,
            sync: SyncPolicy::Group,
            background: true,
            compaction: CompactionStyle::Leveled,
            max_immutables: 2,
            tier_min_merge: 4,
            compaction_rate_limit: None,
            strict_fsync: true,
        }
    }
}

#[derive(Default)]
pub struct Metrics {
    pub user_bytes: AtomicU64,
    pub wal_bytes: AtomicU64,
    pub flush_bytes: AtomicU64,
    pub compact_read: AtomicU64,
    pub compact_write: AtomicU64,
    pub stall_micros: AtomicU64,
    pub write_batches: AtomicU64,
    pub write_groups: AtomicU64,
    pub syncs: AtomicU64,
    pub flushes: AtomicU64,
    pub compactions: AtomicU64,
    pub gets: AtomicU64,
}

#[derive(Debug, Clone)]
pub struct Stats {
    pub level_files: Vec<usize>,
    pub level_bytes: Vec<u64>,
    pub cache_hit_ratio: f64,
    pub blocks_read: u64,
    pub bloom_negatives: u64,
    pub bloom_false_positives: u64,
    pub bloom_true_positives: u64,
    pub write_amp: f64,
    pub space_bytes: u64,
}

pub struct BatchOp {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>, // None = delete
    pub ts: Option<u64>,
}

#[derive(Default)]
pub struct WriteBatch {
    pub ops: Vec<BatchOp>,
}

impl WriteBatch {
    pub fn new() -> WriteBatch {
        WriteBatch::default()
    }
    pub fn put(&mut self, k: &[u8], v: &[u8]) -> &mut Self {
        self.ops.push(BatchOp { key: k.to_vec(), value: Some(v.to_vec()), ts: None });
        self
    }
    pub fn put_at(&mut self, k: &[u8], v: &[u8], ts: u64) -> &mut Self {
        self.ops.push(BatchOp { key: k.to_vec(), value: Some(v.to_vec()), ts: Some(ts) });
        self
    }
    pub fn delete(&mut self, k: &[u8]) -> &mut Self {
        self.ops.push(BatchOp { key: k.to_vec(), value: None, ts: None });
        self
    }
    pub fn delete_at(&mut self, k: &[u8], ts: u64) -> &mut Self {
        self.ops.push(BatchOp { key: k.to_vec(), value: None, ts: Some(ts) });
        self
    }
}

struct Imm {
    mem: Arc<Memtable>,
    wal_no: u64,
}

struct State {
    mem: Arc<Memtable>,
    imms: Vec<Imm>, // oldest first
    version: Arc<Version>,
    wal: Arc<WalWriter>,
    wal_no: u64,
    next_file: u64,
    seq: u64,
    manifest: WalWriter,
    bg_err: Option<String>,
    last_sync: Instant,
    compact_ptr: Vec<Vec<u8>>,
    maintenance_running: bool,
}

struct Waiter {
    batch: Mutex<Option<WriteBatch>>,
    res: Mutex<Option<Result<(), String>>>,
}

struct Wq {
    q: VecDeque<Arc<Waiter>>,
    active: bool,
}

pub struct Inner {
    opts: Options,
    fs: Arc<dyn Vfs>,
    dir: String,
    cache: Arc<BlockCache>,
    st: Mutex<State>,
    cv: Condvar,
    wq: Mutex<Wq>,
    wq_cv: Condvar,
    pub metrics: Metrics,
    gc_watermark: AtomicU64,
    visible_seq: AtomicU64,
    closed: AtomicBool,
}

pub struct Db {
    inner: Arc<Inner>,
    bg: Mutex<Option<std::thread::JoinHandle<()>>>,
}

fn path(dir: &str, no: u64, ext: &str) -> String {
    format!("{dir}/{no:06}.{ext}")
}

fn err_s(e: io::Error) -> String {
    e.to_string()
}

fn encode_batch(entries: &[Entry]) -> Vec<u8> {
    let mut p = Vec::new();
    p.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for e in entries {
        e.encode_into(&mut p);
    }
    p
}

fn decode_batch(p: &[u8]) -> Option<Vec<Entry>> {
    let n = u32::from_le_bytes(p.get(..4)?.try_into().ok()?) as usize;
    let mut b = &p[4..];
    let mut v = Vec::with_capacity(n);
    for _ in 0..n {
        let (e, used) = Entry::decode(b)?;
        v.push(e);
        b = &b[used..];
    }
    Some(v)
}

impl Db {
    pub fn open(opts: Options, fs: Arc<dyn Vfs>, dir: &str) -> io::Result<Db> {
        fs.mkdir(dir)?;
        let cache = Arc::new(BlockCache::new(opts.cache_bytes));
        let cur_path = format!("{dir}/CURRENT");
        let mut version = Version::new(opts.max_levels);
        let (mut log_number, mut next_file, mut last_seq) = (0u64, 2u64, 0u64);
        if fs.exists(&cur_path) {
            let cf = fs.open(&cur_path)?;
            let name = String::from_utf8_lossy(&cf.read_at(0, cf.len() as usize)?).trim().to_string();
            let mf = fs.open(&format!("{dir}/{name}"))?;
            let (recs, _) = wal::read_all(&*mf)?;
            let mut files: HashMap<u64, (u32, u64, u64, Vec<u8>, Vec<u8>, u64)> = HashMap::new();
            for r in recs {
                for e in decode_edits(&r)? {
                    match e {
                        Edit::AddFile { level, id, size, smallest, largest, max_ts } => {
                            files.insert(id, (level, id, size, smallest, largest, max_ts));
                        }
                        Edit::DelFile { id, .. } => {
                            files.remove(&id);
                        }
                        Edit::LogNumber(n) => log_number = n,
                        Edit::NextFile(n) => next_file = next_file.max(n),
                        Edit::LastSeq(n) => last_seq = last_seq.max(n),
                    }
                }
            }
            for (_, (level, id, size, smallest, largest, max_ts)) in files {
                let f = fs.open(&path(dir, id, "sst"))?;
                let reader = Arc::new(SstReader::open(id, f, Some(cache.clone()))?);
                version.levels[level as usize].push(Arc::new(FileMeta { id, size, smallest, largest, max_ts, reader }));
            }
            version.normalize();
        }
        // replay WALs
        let mut wals: Vec<u64> =
            fs.list(dir)?.iter().filter_map(|n| n.strip_suffix(".wal").and_then(|s| s.parse::<u64>().ok())).filter(|n| *n >= log_number).collect();
        wals.sort();
        let recovered = Memtable::new(0);
        for n in &wals {
            let f = fs.open(&path(dir, *n, "wal"))?;
            let (recs, _) = wal::read_all(&*f)?;
            for r in recs {
                if let Some(es) = decode_batch(&r) {
                    for e in es {
                        last_seq = last_seq.max(e.ts());
                        recovered.insert(e);
                    }
                }
                next_file = next_file.max(*n + 1);
            }
            next_file = next_file.max(*n + 1);
        }
        let inner_tmp = (opts.clone(), fs.clone());
        let _ = inner_tmp;
        // flush recovered data to an L0 file
        if !recovered.is_empty() {
            let id = next_file;
            next_file += 1;
            let meta = write_sst_file(&opts, &fs, dir, id, recovered.all().into_iter(), &cache)?;
            if let Some(m) = meta {
                version.levels[0].push(Arc::new(m));
                version.normalize();
            }
        }
        let wal_no = next_file;
        next_file += 1;
        let manifest_no = next_file;
        next_file += 1;
        let wal_file = fs.create(&path(dir, wal_no, "wal"))?;
        // fresh manifest = full snapshot
        let mname = format!("MANIFEST-{manifest_no:06}");
        let mfile = fs.create(&format!("{dir}/{mname}"))?;
        let manifest = WalWriter::new(mfile);
        let mut edits = vec![Edit::LogNumber(wal_no), Edit::NextFile(next_file), Edit::LastSeq(last_seq)];
        for (l, fl) in version.levels.iter().enumerate() {
            for f in fl {
                edits.push(Edit::AddFile {
                    level: l as u32,
                    id: f.id,
                    size: f.size,
                    smallest: f.smallest.clone(),
                    largest: f.largest.clone(),
                    max_ts: f.max_ts,
                });
            }
        }
        manifest.append(&encode_edits(&edits))?;
        manifest.sync()?;
        let tmp = format!("{dir}/CURRENT.tmp");
        let t = fs.create(&tmp)?;
        t.append(mname.as_bytes())?;
        t.sync()?;
        fs.rename(&tmp, &cur_path)?;
        // cleanup obsolete files
        let live: std::collections::HashSet<u64> = version.levels.iter().flatten().map(|f| f.id).collect();
        for n in fs.list(dir)? {
            let del = if let Some(s) = n.strip_suffix(".wal") {
                s.parse::<u64>().map(|x| x != wal_no).unwrap_or(false)
            } else if let Some(s) = n.strip_suffix(".sst") {
                s.parse::<u64>().map(|x| !live.contains(&x)).unwrap_or(false)
            } else if n.starts_with("MANIFEST-") {
                n != mname
            } else {
                false
            };
            if del {
                let _ = fs.remove(&format!("{dir}/{n}"));
            }
        }
        let st = State {
            mem: Arc::new(Memtable::new(wal_no)),
            imms: vec![],
            version: Arc::new(version),
            wal: Arc::new(WalWriter::new(wal_file)),
            wal_no,
            next_file,
            seq: last_seq,
            manifest,
            bg_err: None,
            last_sync: Instant::now(),
            compact_ptr: vec![vec![]; opts.max_levels],
            maintenance_running: false,
        };
        let inner = Arc::new(Inner {
            opts: opts.clone(),
            fs,
            dir: dir.to_string(),
            cache,
            st: Mutex::new(st),
            cv: Condvar::new(),
            wq: Mutex::new(Wq { q: VecDeque::new(), active: false }),
            wq_cv: Condvar::new(),
            metrics: Metrics::default(),
            gc_watermark: AtomicU64::new(u64::MAX),
            visible_seq: AtomicU64::new(last_seq),
            closed: AtomicBool::new(false),
        });
        let bg = if opts.background {
            let i2 = inner.clone();
            Some(std::thread::Builder::new().name("kv-maint".into()).spawn(move || i2.maintenance_loop()).unwrap())
        } else {
            None
        };
        let db = Db { inner, bg: Mutex::new(bg) };
        if !opts.background {
            db.inner.run_maintenance_inline();
        }
        Ok(db)
    }

    pub fn options(&self) -> &Options {
        &self.inner.opts
    }
    pub fn metrics(&self) -> &Metrics {
        &self.inner.metrics
    }
    pub fn set_gc_watermark(&self, ts: u64) {
        self.inner.gc_watermark.store(ts, AO::SeqCst);
    }
    /// Highest committed timestamp visible to readers.
    pub fn visible_seq(&self) -> u64 {
        self.inner.visible_seq.load(AO::SeqCst)
    }

    pub fn put(&self, k: &[u8], v: &[u8]) -> io::Result<()> {
        let mut b = WriteBatch::new();
        b.put(k, v);
        self.write(b)
    }
    pub fn delete(&self, k: &[u8]) -> io::Result<()> {
        let mut b = WriteBatch::new();
        b.delete(k);
        self.write(b)
    }

    pub fn write(&self, batch: WriteBatch) -> io::Result<()> {
        self.inner.write(batch).map_err(io::Error::other)
    }

    pub fn get(&self, k: &[u8]) -> io::Result<Option<Vec<u8>>> {
        self.get_at(k, self.visible_seq())
    }
    pub fn get_at(&self, k: &[u8], ts: u64) -> io::Result<Option<Vec<u8>>> {
        self.inner.get_at(k, ts)
    }
    pub fn scan(&self, start: &[u8], end: &[u8], limit: usize) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan_at(start, end, self.visible_seq(), limit)
    }
    pub fn scan_at(&self, start: &[u8], end: &[u8], ts: u64, limit: usize) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.inner.scan_at(start, end, ts, limit)
    }

    /// Flush the memtable and wait until all immutables are persisted.
    pub fn flush(&self) -> io::Result<()> {
        self.inner.force_flush().map_err(io::Error::other)
    }
    /// Run compactions until none is needed.
    pub fn compact_until_idle(&self) -> io::Result<()> {
        self.inner.compact_until_idle().map_err(io::Error::other)
    }
    pub fn sync_wal(&self) -> io::Result<()> {
        let w = self.inner.st.lock().wal.clone();
        w.sync()
    }

    pub fn stats(&self) -> Stats {
        let (ver, _) = self.inner.sources_ver();
        let mut s = Stats {
            level_files: ver.levels.iter().map(|l| l.len()).collect(),
            level_bytes: (0..ver.levels.len()).map(|l| ver.level_size(l)).collect(),
            cache_hit_ratio: self.inner.cache.hit_ratio(),
            blocks_read: 0,
            bloom_negatives: 0,
            bloom_false_positives: 0,
            bloom_true_positives: 0,
            write_amp: 0.0,
            space_bytes: 0,
        };
        for f in ver.levels.iter().flatten() {
            s.blocks_read += f.reader.blocks_read.load(AO::Relaxed);
            s.bloom_negatives += f.reader.bloom_negatives.load(AO::Relaxed);
            s.bloom_false_positives += f.reader.bloom_false_positives.load(AO::Relaxed);
            s.bloom_true_positives += f.reader.bloom_positives.load(AO::Relaxed);
            s.space_bytes += f.size;
        }
        let m = &self.inner.metrics;
        let user = m.user_bytes.load(AO::Relaxed) as f64;
        if user > 0.0 {
            s.write_amp = (m.wal_bytes.load(AO::Relaxed) + m.flush_bytes.load(AO::Relaxed) + m.compact_write.load(AO::Relaxed)) as f64 / user;
        }
        s
    }

    /// Export every entry (all versions) as encoded chunks of about `chunk_bytes`.
    pub fn export_snapshot(&self, chunk_bytes: usize) -> io::Result<Vec<Vec<u8>>> {
        let (mem, imms, ver) = self.inner.sources();
        let mut srcs: Vec<Src> = vec![Src::Vec(mem.all().into_iter())];
        for i in imms.iter().rev() {
            srcs.push(Src::Vec(i.mem.all().into_iter()));
        }
        for f in ver.levels.iter().flatten() {
            srcs.push(Src::Sst(f.reader.iter(&[], &[])));
        }
        let mut merged = Merge::new(srcs);
        let mut chunks = vec![];
        let mut cur = vec![];
        let mut last: Option<Vec<u8>> = None;
        while let Some(e) = merged.next_entry() {
            if last.as_deref() == Some(e.ikey.as_slice()) {
                continue;
            }
            last = Some(e.ikey.clone());
            e.encode_into(&mut cur);
            if cur.len() >= chunk_bytes {
                chunks.push(std::mem::take(&mut cur));
            }
        }
        merged.check()?;
        if !cur.is_empty() {
            chunks.push(cur);
        }
        Ok(chunks)
    }

    /// Build a fresh database at `dir` (must not contain a DB) from snapshot chunks.
    pub fn import_snapshot(opts: Options, fs: Arc<dyn Vfs>, dir: &str, chunks: &[Vec<u8>]) -> io::Result<Db> {
        let db = Db::open(opts, fs, dir)?;
        let mut batch_entries: Vec<Entry> = vec![];
        let mut max_ts = 0;
        for c in chunks {
            let mut b = &c[..];
            while !b.is_empty() {
                let (e, n) = Entry::decode(b).ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad snapshot chunk"))?;
                b = &b[n..];
                max_ts = max_ts.max(e.ts());
                batch_entries.push(e);
            }
        }
        let mut wb = WriteBatch::new();
        for e in batch_entries {
            if e.kind == KIND_PUT {
                wb.put_at(e.ukey(), &e.value, e.ts());
            } else {
                wb.delete_at(e.ukey(), e.ts());
            }
            if wb.ops.len() >= 1000 {
                db.write(std::mem::take(&mut wb))?;
            }
        }
        if !wb.ops.is_empty() {
            db.write(wb)?;
        }
        db.flush()?;
        Ok(db)
    }

    /// Stop background work. Data stays recoverable from the WAL.
    pub fn close(&self) {
        self.inner.closed.store(true, AO::SeqCst);
        self.inner.cv.notify_all();
        if let Some(h) = self.bg.lock().take() {
            let _ = h.join();
        }
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        self.close();
    }
}

// ------------------------------------------------------------ merging

enum Src {
    Vec(std::vec::IntoIter<Entry>),
    Sst(SstIter),
}
impl Src {
    fn next(&mut self) -> Option<Entry> {
        match self {
            Src::Vec(i) => i.next(),
            Src::Sst(i) => i.next(),
        }
    }
    fn err(&mut self) -> Option<io::Error> {
        match self {
            Src::Vec(_) => None,
            Src::Sst(i) => i.err.take(),
        }
    }
}

struct HeapItem {
    e: Entry,
    src: usize,
}
impl PartialEq for HeapItem {
    fn eq(&self, o: &Self) -> bool {
        self.cmp(o) == std::cmp::Ordering::Equal
    }
}
impl Eq for HeapItem {}
impl PartialOrd for HeapItem {
    fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for HeapItem {
    // BinaryHeap is a max-heap: reverse so the smallest key (then lowest src) pops first
    fn cmp(&self, o: &Self) -> std::cmp::Ordering {
        o.e.cmp(&self.e).then(o.src.cmp(&self.src))
    }
}

struct Merge {
    srcs: Vec<Src>,
    heap: BinaryHeap<HeapItem>,
}
impl Merge {
    fn new(mut srcs: Vec<Src>) -> Merge {
        let mut heap = BinaryHeap::new();
        for (i, s) in srcs.iter_mut().enumerate() {
            if let Some(e) = s.next() {
                heap.push(HeapItem { e, src: i });
            }
        }
        Merge { srcs, heap }
    }
    fn next_entry(&mut self) -> Option<Entry> {
        let it = self.heap.pop()?;
        if let Some(e) = self.srcs[it.src].next() {
            self.heap.push(HeapItem { e, src: it.src });
        }
        Some(it.e)
    }
    fn check(&mut self) -> io::Result<()> {
        for s in self.srcs.iter_mut() {
            if let Some(e) = s.err() {
                return Err(e);
            }
        }
        Ok(())
    }
}

fn write_sst_file(
    opts: &Options,
    fs: &Arc<dyn Vfs>,
    dir: &str,
    id: u64,
    entries: impl Iterator<Item = Entry>,
    cache: &Arc<BlockCache>,
) -> io::Result<Option<FileMeta>> {
    let f = fs.create(&path(dir, id, "sst"))?;
    let mut b = SstBuilder::new(f.clone(), opts.block_size, opts.bloom_bits_per_key);
    let mut max_ts = 0;
    for e in entries {
        max_ts = max_ts.max(e.ts());
        b.add(&e)?;
    }
    if b.count == 0 {
        drop(b);
        fs.remove(&path(dir, id, "sst"))?;
        return Ok(None);
    }
    let (smallest, largest) = (b.smallest.clone(), b.largest.clone());
    let size = b.finish()?;
    f.sync()?;
    let reader = Arc::new(SstReader::open(id, f, Some(cache.clone()))?);
    Ok(Some(FileMeta { id, size, smallest, largest, max_ts, reader }))
}

// ------------------------------------------------------------ Inner

struct Job {
    level: usize,
    out_level: usize,
    inputs: Vec<Arc<FileMeta>>,
    next: Vec<Arc<FileMeta>>,
    drop_tombstones: bool,
    split_files: bool,
}

impl Inner {
    fn sources(&self) -> (Arc<Memtable>, Vec<Arc<Imm>>, Arc<Version>) {
        let st = self.st.lock();
        let imms = st.imms.iter().map(|i| Arc::new(Imm { mem: i.mem.clone(), wal_no: i.wal_no })).collect();
        (st.mem.clone(), imms, st.version.clone())
    }
    fn sources_ver(&self) -> (Arc<Version>, Arc<Memtable>) {
        let st = self.st.lock();
        (st.version.clone(), st.mem.clone())
    }

    // ---------------- write path

    fn write(&self, batch: WriteBatch) -> Result<(), String> {
        if batch.ops.is_empty() {
            return Ok(());
        }
        let w = Arc::new(Waiter { batch: Mutex::new(Some(batch)), res: Mutex::new(None) });
        let mut q = self.wq.lock();
        q.q.push_back(w.clone());
        loop {
            if let Some(r) = w.res.lock().take() {
                return r;
            }
            let leader = !q.active && q.q.front().map(|f| Arc::ptr_eq(f, &w)).unwrap_or(false);
            if leader {
                q.active = true;
                let take = if self.opts.sync == SyncPolicy::Always { 1 } else { q.q.len().min(64) };
                let group: Vec<Arc<Waiter>> = q.q.drain(..take).collect();
                drop(q);
                let res = self.write_group(&group);
                for g in &group {
                    *g.res.lock() = Some(res.clone());
                }
                let mut q2 = self.wq.lock();
                q2.active = false;
                drop(q2);
                self.wq_cv.notify_all();
                return w.res.lock().take().unwrap();
            }
            self.wq_cv.wait(&mut q);
        }
    }

    fn write_group(&self, group: &[Arc<Waiter>]) -> Result<(), String> {
        self.make_room()?;
        let (wal, mem, mut seq) = {
            let st = self.st.lock();
            if let Some(e) = &st.bg_err {
                return Err(e.clone());
            }
            (st.wal.clone(), st.mem.clone(), st.seq)
        };
        let mut records: Vec<Vec<Entry>> = vec![];
        let mut user_bytes = 0u64;
        for g in group {
            let b = g.batch.lock().take().unwrap();
            let mut es = Vec::with_capacity(b.ops.len());
            for op in b.ops {
                let ts = match op.ts {
                    Some(t) => {
                        seq = seq.max(t);
                        t
                    }
                    None => {
                        seq += 1;
                        seq
                    }
                };
                user_bytes += (op.key.len() + op.value.as_ref().map(|v| v.len()).unwrap_or(0)) as u64;
                es.push(match op.value {
                    Some(v) => Entry::new(&op.key, ts, KIND_PUT, &v),
                    None => Entry::new(&op.key, ts, KIND_DEL, &[]),
                });
            }
            records.push(es);
        }
        let mut buf = vec![];
        for r in &records {
            buf.extend_from_slice(&WalWriter::encode(&encode_batch(r)));
        }
        let io_res: io::Result<()> = (|| {
            wal.append_raw(&buf)?;
            let do_sync = match self.opts.sync {
                SyncPolicy::Always | SyncPolicy::Group => true,
                SyncPolicy::Interval(ms) => {
                    let mut st = self.st.lock();
                    if st.last_sync.elapsed() >= Duration::from_millis(ms) {
                        st.last_sync = Instant::now();
                        true
                    } else {
                        false
                    }
                }
                SyncPolicy::Never => false,
            };
            if do_sync {
                wal.sync()?;
                self.metrics.syncs.fetch_add(1, AO::Relaxed);
            }
            Ok(())
        })();
        if let Err(e) = io_res {
            // After a failed fsync the WAL state is unknown: refuse further writes.
            let msg = format!("wal write failed: {e}");
            self.st.lock().bg_err = Some(msg.clone());
            return Err(msg);
        }
        for r in records {
            for e in r {
                mem.insert(e);
            }
        }
        self.metrics.wal_bytes.fetch_add(buf.len() as u64, AO::Relaxed);
        self.metrics.user_bytes.fetch_add(user_bytes, AO::Relaxed);
        self.metrics.write_batches.fetch_add(group.len() as u64, AO::Relaxed);
        self.metrics.write_groups.fetch_add(1, AO::Relaxed);
        self.st.lock().seq = seq;
        self.visible_seq.store(seq, AO::SeqCst);
        Ok(())
    }

    /// Rotate the memtable if full, stalling when the background cannot keep up.
    fn make_room(&self) -> Result<(), String> {
        let t0 = Instant::now();
        let mut stalled = false;
        let mut slowed = false;
        loop {
            let mut st = self.st.lock();
            if let Some(e) = &st.bg_err {
                return Err(e.clone());
            }
            let l0 = st.version.levels[0].len();
            if !slowed && self.opts.background && l0 >= self.opts.l0_slowdown && l0 < self.opts.l0_stop {
                slowed = true;
                drop(st);
                std::thread::sleep(Duration::from_millis(1));
                continue;
            }
            if st.mem.approx_size() < self.opts.memtable_size {
                break;
            }
            if self.opts.background && (st.imms.len() >= self.opts.max_immutables || l0 >= self.opts.l0_stop) {
                stalled = true;
                self.cv.notify_all();
                self.cv.wait_for(&mut st, Duration::from_millis(20));
                continue;
            }
            // rotate
            let new_no = st.next_file;
            st.next_file += 1;
            let f = self.fs.create(&path(&self.dir, new_no, "wal")).map_err(err_s)?;
            st.wal.sync().map_err(err_s)?; // old WAL must be durable before data moves on
            let old = std::mem::replace(&mut st.mem, Arc::new(Memtable::new(new_no)));
            let old_no = st.wal_no;
            st.imms.push(Imm { mem: old, wal_no: old_no });
            st.wal = Arc::new(WalWriter::new(f));
            st.wal_no = new_no;
            drop(st);
            if self.opts.background {
                self.cv.notify_all();
            } else {
                self.run_maintenance_inline();
            }
        }
        if stalled {
            self.metrics.stall_micros.fetch_add(t0.elapsed().as_micros() as u64, AO::Relaxed);
        }
        Ok(())
    }

    // ---------------- read path

    fn get_at(&self, uk: &[u8], ts: u64) -> io::Result<Option<Vec<u8>>> {
        self.metrics.gets.fetch_add(1, AO::Relaxed);
        let (mem, imms, ver) = self.sources();
        let mut best: Option<Entry> = None;
        let consider = |e: Entry, best: &mut Option<Entry>| {
            if best.as_ref().map(|b| e.ts() > b.ts()).unwrap_or(true) {
                *best = Some(e);
            }
        };
        if let Some(e) = mem.get(uk, ts) {
            consider(e, &mut best);
        }
        for i in imms.iter().rev() {
            if let Some(e) = i.mem.get(uk, ts) {
                consider(e, &mut best);
            }
        }
        for f in &ver.levels[0] {
            if best.as_ref().map(|b| b.ts() >= f.max_ts).unwrap_or(false) {
                break; // files sorted by max_ts desc: nothing newer remains
            }
            if f.small_u() <= uk && uk <= f.large_u() {
                if let Some(e) = f.reader.get(uk, ts)? {
                    consider(e, &mut best);
                }
            }
        }
        for l in 1..ver.levels.len() {
            if best.as_ref().map(|b| b.ts() >= ver.level_max_ts(l)).unwrap_or(false) {
                continue;
            }
            let files = &ver.levels[l];
            let i = files.partition_point(|f| f.large_u() < uk);
            if i < files.len() && files[i].small_u() <= uk {
                if let Some(e) = files[i].reader.get(uk, ts)? {
                    consider(e, &mut best);
                }
            }
        }
        Ok(best.and_then(|e| if e.kind == KIND_PUT { Some(e.value) } else { None }))
    }

    fn scan_at(&self, start: &[u8], end: &[u8], ts: u64, limit: usize) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let (mem, imms, ver) = self.sources();
        // Copying a whole memtable per scan made short scans O(memtable). With a limit, copy only a bounded prefix of each
        // memtable and retry with a larger one if the merge reaches the point where a truncated memtable stops being complete.
        let mut cap = if limit > 0 { limit.saturating_mul(4).max(64) } else { usize::MAX };
        loop {
            let mut srcs = vec![];
            let mut bound: Option<Vec<u8>> = None; // smallest last-key among truncated memtable sources
            let mut add_mem = |m: &Memtable, srcs: &mut Vec<Src>| {
                let (v, truncated) = m.range_n(start, end, cap);
                if truncated {
                    if let Some(last) = v.last() {
                        let lk = last.ukey().to_vec();
                        if bound.as_ref().map(|b| lk < *b).unwrap_or(true) {
                            bound = Some(lk);
                        }
                    }
                }
                srcs.push(Src::Vec(v.into_iter()));
            };
            add_mem(&mem, &mut srcs);
            for i in imms.iter().rev() {
                add_mem(&i.mem, &mut srcs);
            }
            for f in ver.levels.iter().flatten() {
                if !end.is_empty() && f.small_u() >= end {
                    continue;
                }
                if f.large_u() < start {
                    continue;
                }
                srcs.push(Src::Sst(f.reader.iter(start, end)));
            }
            let mut m = Merge::new(srcs);
            let mut out: Vec<(Vec<u8>, Vec<u8>)> = vec![];
            let mut cur_done: Option<Vec<u8>> = None;
            let mut incomplete = false;
            while let Some(e) = m.next_entry() {
                if let Some(b) = &bound {
                    if e.ukey() >= b.as_slice() {
                        incomplete = true; // a truncated memtable may hold more versions of this key and later keys
                        break;
                    }
                }
                if cur_done.as_deref() == Some(e.ukey()) || e.ts() > ts {
                    continue;
                }
                cur_done = Some(e.ukey().to_vec());
                if e.kind == KIND_PUT {
                    out.push((e.ukey().to_vec(), e.value));
                    if limit > 0 && out.len() >= limit {
                        break;
                    }
                }
            }
            m.check()?;
            if incomplete {
                cap = cap.saturating_mul(4);
                continue;
            }
            return Ok(out);
        }
    }

    // ---------------- maintenance (flush + compaction), single worker

    fn maintenance_loop(self: &Arc<Self>) {
        loop {
            if self.closed.load(AO::SeqCst) {
                return;
            }
            let did = self.maintenance_step();
            match did {
                Ok(true) => {}
                Ok(false) => {
                    let mut st = self.st.lock();
                    if self.closed.load(AO::SeqCst) {
                        return;
                    }
                    self.cv.wait_for(&mut st, Duration::from_millis(50));
                }
                Err(e) => {
                    self.st.lock().bg_err = Some(format!("maintenance failed: {e}"));
                    self.cv.notify_all();
                    return;
                }
            }
        }
    }

    fn run_maintenance_inline(&self) {
        loop {
            match self.maintenance_step() {
                Ok(true) => {}
                Ok(false) => return,
                Err(e) => {
                    self.st.lock().bg_err = Some(format!("maintenance failed: {e}"));
                    return;
                }
            }
        }
    }

    /// One unit of work. Returns whether anything was done.
    fn maintenance_step(&self) -> Result<bool, String> {
        {
            let mut st = self.st.lock();
            if st.maintenance_running {
                return Ok(false);
            }
            st.maintenance_running = true;
        }
        let r = (|| -> Result<bool, String> {
            if self.flush_one().map_err(err_s)? {
                return Ok(true);
            }
            if let Some(job) = self.pick_compaction() {
                self.run_compaction(job).map_err(err_s)?;
                return Ok(true);
            }
            Ok(false)
        })();
        self.st.lock().maintenance_running = false;
        self.cv.notify_all();
        r
    }

    fn log_edits(&self, st: &mut State, edits: &[Edit]) -> io::Result<()> {
        st.manifest.append(&encode_edits(edits))?;
        st.manifest.sync()
    }

    fn flush_one(&self) -> io::Result<bool> {
        let (imm, wal_after, id) = {
            let mut st = self.st.lock();
            if st.imms.is_empty() {
                return Ok(false);
            }
            let after = st.imms.get(1).map(|i| i.wal_no).unwrap_or(st.wal_no);
            let id = st.next_file;
            st.next_file += 1;
            (Imm { mem: st.imms[0].mem.clone(), wal_no: st.imms[0].wal_no }, after, id)
        };
        let meta = write_sst_file(&self.opts, &self.fs, &self.dir, id, imm.mem.all().into_iter(), &self.cache)?;
        let mut st = self.st.lock();
        let mut edits = vec![Edit::LogNumber(wal_after), Edit::NextFile(st.next_file), Edit::LastSeq(st.seq)];
        let mut v = (*st.version).clone();
        if let Some(m) = meta {
            self.metrics.flush_bytes.fetch_add(m.size, AO::Relaxed);
            edits.push(Edit::AddFile { level: 0, id: m.id, size: m.size, smallest: m.smallest.clone(), largest: m.largest.clone(), max_ts: m.max_ts });
            v.levels[0].push(Arc::new(m));
            v.normalize();
        }
        self.log_edits(&mut st, &edits)?;
        st.version = Arc::new(v);
        st.imms.remove(0);
        self.metrics.flushes.fetch_add(1, AO::Relaxed);
        let _ = self.fs.remove(&path(&self.dir, imm.wal_no, "wal"));
        drop(st);
        self.cv.notify_all();
        Ok(true)
    }

    fn pick_compaction(&self) -> Option<Job> {
        let mut st = self.st.lock();
        let ver = st.version.clone();
        match self.opts.compaction {
            CompactionStyle::SizeTiered => {
                let mut files: Vec<Arc<FileMeta>> = ver.levels[0].clone();
                files.sort_by_key(|f| f.size);
                let min = self.opts.tier_min_merge;
                // bucket by size: same bucket if within 2x of the bucket's smallest
                let mut i = 0;
                while i < files.len() {
                    let base = files[i].size.max(1);
                    let mut j = i;
                    while j < files.len() && files[j].size <= base * 2 {
                        j += 1;
                    }
                    if j - i >= min {
                        let inputs: Vec<_> = files[i..j].to_vec();
                        let all = inputs.len() == ver.levels[0].len();
                        return Some(Job { level: 0, out_level: 0, inputs, next: vec![], drop_tombstones: all, split_files: false });
                    }
                    i = j;
                }
                None
            }
            CompactionStyle::Leveled => {
                if ver.levels[0].len() >= self.opts.l0_compaction_trigger {
                    let inputs = ver.levels[0].clone();
                    let lo = inputs.iter().map(|f| f.small_u().to_vec()).min().unwrap();
                    let hi = inputs.iter().map(|f| f.large_u().to_vec()).max().unwrap();
                    let next = ver.overlapping(1, &lo, &hi);
                    let drop = self.is_bottom_for(&ver, 1, &inputs, &next);
                    return Some(Job { level: 0, out_level: 1, inputs, next, drop_tombstones: drop, split_files: true });
                }
                let mut best: Option<(f64, usize)> = None;
                for l in 1..ver.levels.len() - 1 {
                    let target = self.opts.level_base_bytes as f64 * (self.opts.level_multiplier as f64).powi(l as i32 - 1);
                    let score = ver.level_size(l) as f64 / target;
                    if score >= 1.0 && best.map(|b| score > b.0).unwrap_or(true) {
                        best = Some((score, l));
                    }
                }
                let (_, l) = best?;
                let files = &ver.levels[l];
                let ptr = st.compact_ptr[l].clone();
                let f = files.iter().find(|f| f.small_u() > ptr.as_slice()).unwrap_or(&files[0]).clone();
                st.compact_ptr[l] = f.large_u().to_vec();
                let next = ver.overlapping(l + 1, f.small_u(), f.large_u());
                let drop = self.is_bottom_for(&ver, l + 1, std::slice::from_ref(&f), &next);
                Some(Job { level: l, out_level: l + 1, inputs: vec![f], next, drop_tombstones: drop, split_files: true })
            }
        }
    }

    /// Tombstones may only be dropped if no deeper level holds data in the key range the OUTPUT will cover. That range is the
    /// union of the input files AND the overlapping next-level files (which can extend past the inputs' own range): checking only
    /// the inputs' range lets an old version in a deeper level resurrect a deleted key (found by the model test).
    fn is_bottom_for(&self, ver: &Version, out_level: usize, inputs: &[Arc<FileMeta>], next: &[Arc<FileMeta>]) -> bool {
        let all = inputs.iter().chain(next.iter());
        let lo = all.clone().map(|f| f.small_u().to_vec()).min().unwrap_or_default();
        let hi = all.map(|f| f.large_u().to_vec()).max().unwrap_or_default();
        self.is_bottom(ver, out_level, &lo, &hi)
    }

    /// True if no level deeper than `out_level` overlaps [lo, hi].
    fn is_bottom(&self, ver: &Version, out_level: usize, lo: &[u8], hi: &[u8]) -> bool {
        (out_level + 1..ver.levels.len()).all(|l| ver.overlapping(l, lo, hi).is_empty())
    }

    fn run_compaction(&self, job: Job) -> io::Result<()> {
        // trivial move
        if job.next.is_empty() && job.level != job.out_level && job.inputs.len() == 1 {
            let f = job.inputs[0].clone();
            let mut st = self.st.lock();
            let edits = vec![
                Edit::DelFile { level: job.level as u32, id: f.id },
                Edit::AddFile {
                    level: job.out_level as u32,
                    id: f.id,
                    size: f.size,
                    smallest: f.smallest.clone(),
                    largest: f.largest.clone(),
                    max_ts: f.max_ts,
                },
            ];
            self.log_edits(&mut st, &edits)?;
            let mut v = (*st.version).clone();
            v.levels[job.level].retain(|x| x.id != f.id);
            v.levels[job.out_level].push(f);
            v.normalize();
            st.version = Arc::new(v);
            self.metrics.compactions.fetch_add(1, AO::Relaxed);
            return Ok(());
        }
        let all_inputs: Vec<Arc<FileMeta>> = job.inputs.iter().chain(job.next.iter()).cloned().collect();
        let read_bytes: u64 = all_inputs.iter().map(|f| f.size).sum();
        let srcs: Vec<Src> = all_inputs.iter().map(|f| Src::Sst(f.reader.iter(&[], &[]))).collect();
        let mut m = Merge::new(srcs);
        let watermark = self.gc_watermark.load(AO::SeqCst);
        let target = if job.split_files { self.opts.target_file_size } else { u64::MAX };

        let mut outputs: Vec<FileMeta> = vec![];
        let mut cur: Option<(u64, Arc<dyn crate::vfs::VFile>, SstBuilder, u64)> = None;
        let mut last_uk: Option<Vec<u8>> = None;
        let mut kept_below_wm = false;
        let mut written = 0u64;
        let t0 = Instant::now();
        let mut cur_bytes = 0u64;
        let finish = |cur: &mut Option<(u64, Arc<dyn crate::vfs::VFile>, SstBuilder, u64)>, outputs: &mut Vec<FileMeta>| -> io::Result<u64> {
            if let Some((id, f, b, max_ts)) = cur.take() {
                if b.count == 0 {
                    self.fs.remove(&path(&self.dir, id, "sst"))?;
                    return Ok(0);
                }
                let (sm, lg) = (b.smallest.clone(), b.largest.clone());
                let size = b.finish()?;
                f.sync()?;
                let reader = Arc::new(SstReader::open(id, f, Some(self.cache.clone()))?);
                outputs.push(FileMeta { id, size, smallest: sm, largest: lg, max_ts, reader });
                return Ok(size);
            }
            Ok(0)
        };
        while let Some(e) = m.next_entry() {
            let new_key = last_uk.as_deref() != Some(e.ukey());
            if new_key {
                last_uk = Some(e.ukey().to_vec());
                kept_below_wm = false;
            }
            // MVCC garbage collection
            if e.ts() <= watermark {
                if kept_below_wm {
                    continue;
                }
                kept_below_wm = true;
                if e.kind == KIND_DEL && job.drop_tombstones {
                    continue;
                }
            }
            // roll output file only at user-key boundaries
            if new_key && cur.as_ref().map(|c| c.2.offset_bytes() >= target).unwrap_or(false) {
                let sz = finish(&mut cur, &mut outputs)?;
                written += sz;
                cur_bytes = 0;
            }
            if cur.is_none() {
                let id = {
                    let mut st = self.st.lock();
                    let id = st.next_file;
                    st.next_file += 1;
                    id
                };
                let f = self.fs.create(&path(&self.dir, id, "sst"))?;
                cur = Some((id, f.clone(), SstBuilder::new(f, self.opts.block_size, self.opts.bloom_bits_per_key), 0));
            }
            let c = cur.as_mut().unwrap();
            c.3 = c.3.max(e.ts());
            c.2.add(&e)?;
            cur_bytes += (e.ikey.len() + e.value.len()) as u64;
            if let Some(rate) = self.opts.compaction_rate_limit {
                let total = written + cur_bytes;
                let expect = Duration::from_secs_f64(total as f64 / rate as f64);
                if expect > t0.elapsed() {
                    std::thread::sleep(expect - t0.elapsed());
                }
            }
        }
        m.check()?;
        written += finish(&mut cur, &mut outputs)?;
        // install
        let mut st = self.st.lock();
        let mut edits = vec![Edit::NextFile(st.next_file), Edit::LastSeq(st.seq)];
        let mut v = (*st.version).clone();
        for f in &job.inputs {
            edits.push(Edit::DelFile { level: job.level as u32, id: f.id });
            v.levels[job.level].retain(|x| x.id != f.id);
        }
        for f in &job.next {
            edits.push(Edit::DelFile { level: job.out_level as u32, id: f.id });
            v.levels[job.out_level].retain(|x| x.id != f.id);
        }
        for o in outputs {
            edits.push(Edit::AddFile {
                level: job.out_level as u32,
                id: o.id,
                size: o.size,
                smallest: o.smallest.clone(),
                largest: o.largest.clone(),
                max_ts: o.max_ts,
            });
            v.levels[job.out_level].push(Arc::new(o));
        }
        v.normalize();
        self.log_edits(&mut st, &edits)?;
        st.version = Arc::new(v);
        drop(st);
        for f in &all_inputs {
            let _ = self.fs.remove(&path(&self.dir, f.id, "sst"));
        }
        self.metrics.compact_read.fetch_add(read_bytes, AO::Relaxed);
        self.metrics.compact_write.fetch_add(written, AO::Relaxed);
        self.metrics.compactions.fetch_add(1, AO::Relaxed);
        self.cv.notify_all();
        Ok(())
    }

    fn force_flush(&self) -> Result<(), String> {
        {
            let mut st = self.st.lock();
            if let Some(e) = &st.bg_err {
                return Err(e.clone());
            }
            if !st.mem.is_empty() {
                let new_no = st.next_file;
                st.next_file += 1;
                let f = self.fs.create(&path(&self.dir, new_no, "wal")).map_err(err_s)?;
                st.wal.sync().map_err(err_s)?;
                let old = std::mem::replace(&mut st.mem, Arc::new(Memtable::new(new_no)));
                let old_no = st.wal_no;
                st.imms.push(Imm { mem: old, wal_no: old_no });
                st.wal = Arc::new(WalWriter::new(f));
                st.wal_no = new_no;
            }
        }
        if self.opts.background {
            self.cv.notify_all();
            loop {
                let mut st = self.st.lock();
                if let Some(e) = &st.bg_err {
                    return Err(e.clone());
                }
                if st.imms.is_empty() {
                    return Ok(());
                }
                self.cv.wait_for(&mut st, Duration::from_millis(20));
            }
        } else {
            self.run_maintenance_inline();
            match &self.st.lock().bg_err {
                Some(e) => Err(e.clone()),
                None => Ok(()),
            }
        }
    }

    fn compact_until_idle(&self) -> Result<(), String> {
        self.force_flush()?;
        if self.opts.background {
            loop {
                {
                    let st = self.st.lock();
                    if let Some(e) = &st.bg_err {
                        return Err(e.clone());
                    }
                    let busy = st.maintenance_running;
                    drop(st);
                    if !busy && self.st.lock().imms.is_empty() {
                        // ask for work; if nothing to pick we're idle
                        if self.pick_compaction_peek().is_none() {
                            return Ok(());
                        }
                    }
                }
                self.cv.notify_all();
                std::thread::sleep(Duration::from_millis(5));
            }
        } else {
            self.run_maintenance_inline();
            match &self.st.lock().bg_err {
                Some(e) => Err(e.clone()),
                None => Ok(()),
            }
        }
    }

    fn pick_compaction_peek(&self) -> Option<()> {
        // side-effect-free check mirroring pick_compaction
        let st = self.st.lock();
        let ver = st.version.clone();
        match self.opts.compaction {
            CompactionStyle::SizeTiered => {
                let mut sizes: Vec<u64> = ver.levels[0].iter().map(|f| f.size).collect();
                sizes.sort();
                let mut i = 0;
                while i < sizes.len() {
                    let base = sizes[i].max(1);
                    let mut j = i;
                    while j < sizes.len() && sizes[j] <= base * 2 {
                        j += 1;
                    }
                    if j - i >= self.opts.tier_min_merge {
                        return Some(());
                    }
                    i = j;
                }
                None
            }
            CompactionStyle::Leveled => {
                if ver.levels[0].len() >= self.opts.l0_compaction_trigger {
                    return Some(());
                }
                for l in 1..ver.levels.len() - 1 {
                    let target = self.opts.level_base_bytes as f64 * (self.opts.level_multiplier as f64).powi(l as i32 - 1);
                    if ver.level_size(l) as f64 / target >= 1.0 {
                        return Some(());
                    }
                }
                None
            }
        }
    }
}
