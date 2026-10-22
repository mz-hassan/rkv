//! Virtual filesystem. The engine only touches disk through this trait so the
//! deterministic simulator can substitute `MemFs`, which models unsynced-data
//! loss and torn writes on crash.
use parking_lot::Mutex;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;

pub trait VFile: Send + Sync {
    fn append(&self, data: &[u8]) -> io::Result<()>;
    /// Durably flush appended data (F_FULLFSYNC on macOS for OsFs).
    fn sync(&self) -> io::Result<()>;
    fn read_at(&self, off: u64, len: usize) -> io::Result<Vec<u8>>;
    fn len(&self) -> u64;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub trait Vfs: Send + Sync {
    /// Create (truncate) a file for appending.
    fn create(&self, path: &str) -> io::Result<Arc<dyn VFile>>;
    /// Open an existing file (reads and appends).
    fn open(&self, path: &str) -> io::Result<Arc<dyn VFile>>;
    fn exists(&self, path: &str) -> bool;
    fn remove(&self, path: &str) -> io::Result<()>;
    /// Atomic rename; durable once it returns (modelled; dir fsync on OsFs).
    fn rename(&self, from: &str, to: &str) -> io::Result<()>;
    /// File names (not paths) directly inside `dir`.
    fn list(&self, dir: &str) -> io::Result<Vec<String>>;
    fn mkdir(&self, dir: &str) -> io::Result<()>;
}

// ---------------------------------------------------------------- OsFs

pub struct OsFs;

struct OsFile {
    f: std::fs::File,
    len: Mutex<u64>,
    strict: bool,
}

#[cfg(unix)]
fn pread(f: &std::fs::File, buf: &mut [u8], off: u64) -> io::Result<usize> {
    use std::os::unix::fs::FileExt;
    f.read_at(buf, off)
}

impl VFile for OsFile {
    fn append(&self, data: &[u8]) -> io::Result<()> {
        use std::io::Write;
        let mut l = self.len.lock();
        (&self.f).write_all(data)?;
        *l += data.len() as u64;
        Ok(())
    }
    fn sync(&self) -> io::Result<()> {
        #[cfg(target_os = "macos")]
        {
            use std::os::unix::io::AsRawFd;
            if self.strict {
                // fsync on macOS does not flush the drive cache; F_FULLFSYNC does.
                let r = unsafe { libc_fullfsync(self.f.as_raw_fd()) };
                return if r == -1 { Err(io::Error::last_os_error()) } else { Ok(()) };
            }
        }
        let _ = self.strict;
        self.f.sync_data()
    }
    fn read_at(&self, off: u64, len: usize) -> io::Result<Vec<u8>> {
        let mut buf = vec![0u8; len];
        let mut done = 0;
        while done < len {
            let n = pread(&self.f, &mut buf[done..], off + done as u64)?;
            if n == 0 {
                buf.truncate(done);
                break;
            }
            done += n;
        }
        Ok(buf)
    }
    fn len(&self) -> u64 {
        *self.len.lock()
    }
}

#[cfg(target_os = "macos")]
extern "C" {
    fn fcntl(fd: i32, cmd: i32, ...) -> i32;
}
#[cfg(target_os = "macos")]
unsafe fn libc_fullfsync(fd: i32) -> i32 {
    const F_FULLFSYNC: i32 = 51;
    fcntl(fd, F_FULLFSYNC)
}

impl OsFs {
    /// `strict` = use F_FULLFSYNC on macOS. Benchmarks must record this.
    pub fn new_arc(strict: bool) -> Arc<dyn Vfs> {
        Arc::new(OsFsImpl { strict })
    }
}

struct OsFsImpl {
    strict: bool,
}

impl Vfs for OsFsImpl {
    fn create(&self, path: &str) -> io::Result<Arc<dyn VFile>> {
        let f = std::fs::OpenOptions::new().create(true).write(true).read(true).truncate(true).open(path)?;
        Ok(Arc::new(OsFile { f, len: Mutex::new(0), strict: self.strict }))
    }
    fn open(&self, path: &str) -> io::Result<Arc<dyn VFile>> {
        use std::io::Seek;
        let mut f = std::fs::OpenOptions::new().read(true).append(true).open(path)?;
        let len = f.seek(std::io::SeekFrom::End(0))?;
        Ok(Arc::new(OsFile { f, len: Mutex::new(len), strict: self.strict }))
    }
    fn exists(&self, path: &str) -> bool {
        std::path::Path::new(path).exists()
    }
    fn remove(&self, path: &str) -> io::Result<()> {
        std::fs::remove_file(path)
    }
    fn rename(&self, from: &str, to: &str) -> io::Result<()> {
        std::fs::rename(from, to)?;
        if let Some(p) = std::path::Path::new(to).parent() {
            if let Ok(d) = std::fs::File::open(p) {
                let _ = d.sync_all();
            }
        }
        Ok(())
    }
    fn list(&self, dir: &str) -> io::Result<Vec<String>> {
        let mut v = vec![];
        for e in std::fs::read_dir(dir)? {
            v.push(e?.file_name().to_string_lossy().into_owned());
        }
        Ok(v)
    }
    fn mkdir(&self, dir: &str) -> io::Result<()> {
        std::fs::create_dir_all(dir)
    }
}

// ---------------------------------------------------------------- MemFs

struct MemData {
    data: Vec<u8>,
    synced: usize,
}

#[derive(Default)]
struct MemState {
    files: BTreeMap<String, Arc<MemFileInner>>,
    fail_next_syncs: u32,
    fail_all_syncs: bool,
}

struct MemFileInner {
    d: Mutex<MemData>,
}

pub struct MemFs {
    st: Mutex<MemState>,
}

struct MemFile {
    inner: Arc<MemFileInner>,
    fs: Arc<MemFs>,
}

impl MemFs {
    pub fn new() -> Arc<MemFs> {
        Arc::new(MemFs { st: Mutex::new(MemState::default()) })
    }

    /// Simulate power loss. For each file the synced prefix always survives;
    /// of the pending (unsynced) tail a random prefix survives (torn write),
    /// and with probability 1/4 its last surviving byte is corrupted.
    pub fn crash(&self, seed: u64) {
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        let st = self.st.lock();
        for f in st.files.values() {
            let mut d = f.d.lock();
            let pending = d.data.len() - d.synced;
            let keep = if pending == 0 { 0 } else { rng.gen_range(0..=pending) };
            let newlen = d.synced + keep;
            d.data.truncate(newlen);
            if keep > 0 && rng.gen_range(0..4) == 0 {
                let last = newlen - 1;
                d.data[last] ^= 0xff;
            }
            d.synced = d.data.len();
        }
    }

    /// Process crash only (SIGKILL): all written data survives (page cache).
    pub fn process_crash(&self) {}

    pub fn fail_next_syncs(&self, n: u32) {
        self.st.lock().fail_next_syncs = n;
    }
    pub fn set_fail_all_syncs(&self, b: bool) {
        self.st.lock().fail_all_syncs = b;
    }
    pub fn total_bytes(&self) -> usize {
        self.st.lock().files.values().map(|f| f.d.lock().data.len()).sum()
    }
}

/// Wrapper so `Arc<MemFs>` can be used as `Arc<dyn Vfs>`.
pub struct MemFsHandle(pub Arc<MemFs>);

impl VFile for MemFile {
    fn append(&self, data: &[u8]) -> io::Result<()> {
        self.inner.d.lock().data.extend_from_slice(data);
        Ok(())
    }
    fn sync(&self) -> io::Result<()> {
        {
            let mut st = self.fs.st.lock();
            if st.fail_all_syncs {
                return Err(io::Error::other("injected fsync failure"));
            }
            if st.fail_next_syncs > 0 {
                st.fail_next_syncs -= 1;
                return Err(io::Error::other("injected fsync failure"));
            }
        }
        let mut d = self.inner.d.lock();
        d.synced = d.data.len();
        Ok(())
    }
    fn read_at(&self, off: u64, len: usize) -> io::Result<Vec<u8>> {
        let d = self.inner.d.lock();
        let off = off as usize;
        if off >= d.data.len() {
            return Ok(vec![]);
        }
        let end = (off + len).min(d.data.len());
        Ok(d.data[off..end].to_vec())
    }
    fn len(&self) -> u64 {
        self.inner.d.lock().data.len() as u64
    }
}

impl Vfs for MemFsHandle {
    fn create(&self, path: &str) -> io::Result<Arc<dyn VFile>> {
        let inner = Arc::new(MemFileInner { d: Mutex::new(MemData { data: vec![], synced: 0 }) });
        self.0.st.lock().files.insert(path.to_string(), inner.clone());
        Ok(Arc::new(MemFile { inner, fs: self.0.clone() }))
    }
    fn open(&self, path: &str) -> io::Result<Arc<dyn VFile>> {
        let st = self.0.st.lock();
        match st.files.get(path) {
            Some(i) => Ok(Arc::new(MemFile { inner: i.clone(), fs: self.0.clone() })),
            None => Err(io::Error::new(io::ErrorKind::NotFound, path.to_string())),
        }
    }
    fn exists(&self, path: &str) -> bool {
        self.0.st.lock().files.contains_key(path)
    }
    fn remove(&self, path: &str) -> io::Result<()> {
        self.0.st.lock().files.remove(path);
        Ok(())
    }
    fn rename(&self, from: &str, to: &str) -> io::Result<()> {
        let mut st = self.0.st.lock();
        match st.files.remove(from) {
            Some(f) => {
                // rename is atomic and durable, but data not fsynced stays unsynced
                st.files.insert(to.to_string(), f);
                Ok(())
            }
            None => Err(io::Error::new(io::ErrorKind::NotFound, from.to_string())),
        }
    }
    fn list(&self, dir: &str) -> io::Result<Vec<String>> {
        let prefix = if dir.ends_with('/') { dir.to_string() } else { format!("{dir}/") };
        let st = self.0.st.lock();
        Ok(st.files.keys().filter_map(|k| k.strip_prefix(&prefix)).filter(|r| !r.contains('/')).map(|s| s.to_string()).collect())
    }
    fn mkdir(&self, _dir: &str) -> io::Result<()> {
        Ok(())
    }
}

impl MemFs {
    pub fn as_vfs(self: &Arc<Self>) -> Arc<dyn Vfs> {
        Arc::new(MemFsHandle(self.clone()))
    }
}
