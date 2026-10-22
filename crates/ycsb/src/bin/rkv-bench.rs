#![allow(clippy::result_large_err, clippy::type_complexity, clippy::too_many_arguments)]
//! rkv-bench: benchmark and verification driver.
//!   engine            S1: single-node engine YCSB (+ optional RocksDB baseline with --features rocksdb-baseline)
//!   s2-sim            S2: crash consistency under simulated power loss (MemFs: unsynced data lost/torn)
//!   s2-driver         S2: crash consistency under real kill -9 (spawns s2-writer children)
//!   recovery          S1: engine recovery time vs WAL size (kill without flush, reopen, replay)
//!   cluster           R1/D1: YCSB against a running cluster through the client library
//!   txn-bank          D2: bank transfers with Zipfian contention (conservation audited)
//!   txn-skew          D2: write-skew detection, snapshot vs serializable
//!   lock-recovery     D2: time for readers to recover from a crashed transaction coordinator
//! Every subcommand accepts --out DIR (default bench/results) and --id NAME, and prints the result path.
use kv_engine::{CompactionStyle, Db, MemFs, Options, OsFs, SyncPolicy};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use ycsb::dist::{key_for, value_for};
use ycsb::hist::Hist;
use ycsb::report::write_result;
use ycsb::workload::{Op, OpGen, Workload};

struct Args(Vec<String>);
impl Args {
    fn get(&self, n: &str) -> Option<String> {
        self.0.iter().position(|x| x == n).and_then(|i| self.0.get(i + 1).cloned())
    }
    fn s(&self, n: &str, d: &str) -> String {
        self.get(n).unwrap_or_else(|| d.to_string())
    }
    fn u(&self, n: &str, d: u64) -> u64 {
        self.get(n).map(|v| v.parse().unwrap_or_else(|_| panic!("bad value for {n}"))).unwrap_or(d)
    }
    fn f(&self, n: &str, d: f64) -> f64 {
        self.get(n).map(|v| v.parse().unwrap()).unwrap_or(d)
    }
    fn flag(&self, n: &str) -> bool {
        self.0.iter().any(|x| x == n)
    }
}

fn parse_sync(s: &str) -> SyncPolicy {
    match s {
        "always" => SyncPolicy::Always,
        "never" => SyncPolicy::Never,
        s if s.starts_with("interval") => SyncPolicy::Interval(s.trim_start_matches("interval").trim_start_matches('=').parse().unwrap_or(100)),
        _ => SyncPolicy::Group,
    }
}

// ------------------------------------------------------------------ S1: engine

#[allow(dead_code)]
trait SyncKv: Send + Sync {
    fn get(&self, k: &[u8]) -> bool;
    fn put(&self, k: &[u8], v: &[u8]);
    /// bulk load: many puts under one durability point
    fn put_batch(&self, kvs: &[(Vec<u8>, Vec<u8>)]) {
        for (k, v) in kvs {
            self.put(k, v);
        }
    }
    fn scan(&self, k: &[u8], n: usize) -> usize;
    fn settle(&self);
    fn stats(&self) -> Value;
    fn name(&self) -> String;
}

struct RkvKv {
    db: Db,
    base: kv_engine::Stats,
    m0: [u64; 4],
}

impl SyncKv for RkvKv {
    fn get(&self, k: &[u8]) -> bool {
        self.db.get(k).unwrap().is_some()
    }
    fn put(&self, k: &[u8], v: &[u8]) {
        self.db.put(k, v).unwrap()
    }
    fn put_batch(&self, kvs: &[(Vec<u8>, Vec<u8>)]) {
        let mut b = kv_engine::WriteBatch::new();
        for (k, v) in kvs {
            b.put(k, v);
        }
        self.db.write(b).unwrap();
    }
    fn scan(&self, k: &[u8], n: usize) -> usize {
        self.db.scan(k, &[], n).unwrap().len()
    }
    fn settle(&self) {
        self.db.compact_until_idle().unwrap();
    }
    fn stats(&self) -> Value {
        let s = self.db.stats();
        let m = self.db.metrics();
        let g = |a: &AtomicU64| a.load(Ordering::Relaxed);
        let (user, wal, flush, comp) =
            (g(&m.user_bytes) - self.m0[0], g(&m.wal_bytes) - self.m0[1], g(&m.flush_bytes) - self.m0[2], g(&m.compact_write) - self.m0[3]);
        let absent = s.bloom_negatives + s.bloom_false_positives;
        json!({
            "write_amp_run_phase": if user > 0 { (wal + flush + comp) as f64 / user as f64 } else { 0.0 },
            "write_amp_total": s.write_amp,
            "level_files": s.level_files, "level_bytes": s.level_bytes, "space_bytes": s.space_bytes,
            "blocks_read": s.blocks_read.saturating_sub(self.base.blocks_read),
            "cache_hit_ratio": s.cache_hit_ratio,
            "bloom_negatives": s.bloom_negatives, "bloom_false_positives": s.bloom_false_positives, "bloom_true_positives": s.bloom_true_positives,
            "bloom_measured_fpr": if absent > 0 { s.bloom_false_positives as f64 / absent as f64 } else { 0.0 },
            "bloom_theoretical_fpr": 0.6185f64.powi(self.db.options().bloom_bits_per_key as i32),
            "stall_micros": g(&m.stall_micros), "compactions": g(&m.compactions), "flushes": g(&m.flushes),
        })
    }
    fn name(&self) -> String {
        "rkv-engine".into()
    }
}

#[cfg(feature = "rocksdb-baseline")]
struct RocksKv {
    db: rocksdb::DB,
}
#[cfg(feature = "rocksdb-baseline")]
impl SyncKv for RocksKv {
    fn get(&self, k: &[u8]) -> bool {
        self.db.get(k).unwrap().is_some()
    }
    fn put(&self, k: &[u8], v: &[u8]) {
        self.db.put(k, v).unwrap()
    }
    fn scan(&self, k: &[u8], n: usize) -> usize {
        self.db.iterator(rocksdb::IteratorMode::From(k, rocksdb::Direction::Forward)).take(n).count()
    }
    fn settle(&self) {
        self.db.compact_range::<&[u8], &[u8]>(None, None);
    }
    fn stats(&self) -> Value {
        json!({ "rocksdb_stats": self.db.property_value("rocksdb.stats").ok().flatten() })
    }
    fn name(&self) -> String {
        "rocksdb".into()
    }
}

fn open_backend(a: &Args, dir: &str, sync: SyncPolicy) -> (Arc<dyn SyncKv>, Value) {
    let cache_mb = a.u("--cache-mb", 64);
    let mem_mb = a.u("--memtable-mb", 16);
    match a.s("--backend", "rkv").as_str() {
        #[cfg(feature = "rocksdb-baseline")]
        "rocksdb" => {
            let mut o = rocksdb::Options::default();
            o.create_if_missing(true);
            o.set_write_buffer_size((mem_mb << 20) as usize);
            let cache = rocksdb::Cache::new_lru_cache((cache_mb << 20) as usize);
            let mut bo = rocksdb::BlockBasedOptions::default();
            bo.set_block_cache(&cache);
            bo.set_bloom_filter(10.0, false);
            bo.set_block_size(4096);
            o.set_block_based_table_factory(&bo);
            o.set_compression_type(rocksdb::DBCompressionType::None); // same as rkv: no compression
            let mut wo = rocksdb::WriteOptions::default();
            wo.set_sync(matches!(sync, SyncPolicy::Always));
            let _ = wo;
            let db = rocksdb::DB::open(&o, dir).unwrap();
            (
                Arc::new(RocksKv { db }),
                json!({"backend": "rocksdb", "crate": "rocksdb 0.22", "note": "sync policy approximated: group commit not separately configurable"}),
            )
        }
        _ => {
            let opts = Options {
                memtable_size: (mem_mb << 20) as usize,
                cache_bytes: (cache_mb << 20) as usize,
                sync,
                compaction: if a.s("--compaction", "leveled") == "tiered" { CompactionStyle::SizeTiered } else { CompactionStyle::Leveled },
                strict_fsync: a.flag("--strict-fsync"),
                ..Options::default()
            };
            let strict = opts.strict_fsync;
            let db = Db::open(opts.clone(), OsFs::new_arc(strict), dir).unwrap();
            let base = db.stats();
            let m = db.metrics();
            let m0 = [
                m.user_bytes.load(Ordering::Relaxed),
                m.wal_bytes.load(Ordering::Relaxed),
                m.flush_bytes.load(Ordering::Relaxed),
                m.compact_write.load(Ordering::Relaxed),
            ];
            (
                Arc::new(RkvKv { db, base, m0 }),
                json!({"backend": "rkv-engine", "memtable_mb": mem_mb, "cache_mb": cache_mb, "compaction": format!("{:?}", opts.compaction), "sync": format!("{sync:?}"), "full_fsync_macos": strict}),
            )
        }
    }
}

fn run_engine(a: &Args) {
    let id = a.s("--id", "S1");
    let wl = Workload::by_name(a.s("--workload", "a").chars().next().unwrap()).expect("workload a-f");
    let records = a.u("--records", 100_000);
    let ops = a.u("--ops", 200_000);
    let secs = a.u("--secs", 0);
    let threads = a.u("--threads", 4) as usize;
    let vsize = a.u("--value-size", 100) as usize;
    let theta = a.f("--theta", 0.99);
    let sync = parse_sync(&a.s("--sync", "group"));
    let dir = a.s("--dir", &format!("/tmp/rkv-bench-{}-{}", id, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (kv, backend_info) = open_backend(a, &dir, sync);

    // ---- load
    let t0 = Instant::now();
    let per = records / threads as u64;
    std::thread::scope(|s| {
        for t in 0..threads as u64 {
            let kv = kv.clone();
            s.spawn(move || {
                let lo = t * per;
                let hi = if t == threads as u64 - 1 { records } else { lo + per };
                for chunk in (lo..hi).collect::<Vec<_>>().chunks(200) {
                    let batch: Vec<(Vec<u8>, Vec<u8>)> = chunk.iter().map(|i| (key_for(*i), value_for(*i, vsize))).collect();
                    kv.put_batch(&batch);
                }
            });
        }
    });
    let load_secs = t0.elapsed().as_secs_f64();
    kv.settle();

    // ---- warm-up
    let gen = Arc::new(OpGen::new(wl.clone(), records, theta, vsize));
    let warm = a.u("--warmup-ops", ops / 10);
    exec_phase(&kv, &gen, threads, warm, 0, 1);

    // ---- measure
    let t1 = Instant::now();
    let (hists, total) = exec_phase(&kv, &gen, threads, ops, secs, 2);
    let run_secs = t1.elapsed().as_secs_f64();
    let mut per_kind = serde_json::Map::new();
    let mut all = Hist::new();
    for (k, h) in &hists {
        per_kind.insert(k.to_string(), h.to_json());
        all.merge(h);
    }
    let body = json!({
        "bench": "engine-ycsb",
        "config": { "workload": wl.name.to_string(), "records": records, "ops": total, "threads": threads, "value_size": vsize, "theta": theta, "warmup_ops": warm, "backend": backend_info },
        "results": {
            "load_secs": load_secs, "load_ops_per_sec": records as f64 / load_secs,
            "run_secs": run_secs, "ops_per_sec": total as f64 / run_secs,
            "latency_all": all.to_json(), "latency_by_op": per_kind,
            "engine": kv.stats(),
            "space_amp": space_amp(&kv.stats(), records, vsize),
        },
    });
    let labels = json!({
        "scope": "single node, local disk, closed-loop clients (latency measured under saturation; no coordinated-omission correction)",
        "hardware": "see hardware field (laptop; other processes may share the machine)",
        "faults": "none",
        "baseline": a.s("--backend", "rkv"),
    });
    let p = write_result(&a.s("--out", "bench/results"), &id, body, labels).unwrap();
    println!("{p}");
    let _ = std::fs::remove_dir_all(&dir);
}

fn space_amp(st: &Value, records: u64, vsize: usize) -> f64 {
    let live = records as f64 * (20.0 + vsize as f64);
    st["space_bytes"].as_f64().map(|s| s / live).unwrap_or(0.0)
}

fn exec_phase(kv: &Arc<dyn SyncKv>, gen: &Arc<OpGen>, threads: usize, ops: u64, secs: u64, salt: u64) -> (BTreeMap<&'static str, Hist>, u64) {
    let done = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let deadline = if secs > 0 { Some(Instant::now() + Duration::from_secs(secs)) } else { None };
    let results: Vec<BTreeMap<&'static str, Hist>> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..threads)
            .map(|t| {
                let (kv, gen, done, stop) = (kv.clone(), gen.clone(), done.clone(), stop.clone());
                s.spawn(move || {
                    let mut rng = ChaCha8Rng::seed_from_u64(salt * 1000 + t as u64);
                    let mut h: BTreeMap<&'static str, Hist> = BTreeMap::new();
                    loop {
                        if let Some(d) = deadline {
                            if Instant::now() >= d {
                                break;
                            }
                        } else if done.fetch_add(1, Ordering::Relaxed) >= ops {
                            break;
                        }
                        if stop.load(Ordering::Relaxed) {
                            break;
                        }
                        let op = gen.next(&mut rng);
                        let kind = op.kind();
                        let t0 = Instant::now();
                        match op {
                            Op::Read(k) => {
                                kv.get(&k);
                            }
                            Op::Update(k, v) | Op::Insert(k, v) => kv.put(&k, &v),
                            Op::Scan(k, n) => {
                                kv.scan(&k, n);
                            }
                            Op::Rmw(k, v) => {
                                kv.get(&k);
                                kv.put(&k, &v);
                            }
                        }
                        h.entry(kind).or_default().record_us(t0.elapsed().as_micros() as u64);
                    }
                    h
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let mut merged: BTreeMap<&'static str, Hist> = BTreeMap::new();
    let mut total = 0;
    for r in results {
        for (k, h) in r {
            total += h.count;
            merged.entry(k).or_default().merge(&h);
        }
    }
    (merged, total)
}

fn run_recovery(a: &Args) {
    let mb = a.u("--mb", 64);
    let dir = a.s("--dir", &format!("/tmp/rkv-recovery-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    // memtable big enough that nothing is flushed: the whole dataset lives in the WAL
    let opts = Options { memtable_size: (mb as usize + 64) << 20, sync: SyncPolicy::Never, ..Options::default() };
    let db = Db::open(opts.clone(), OsFs::new_arc(false), &dir).unwrap();
    let n = mb * 1024 * 1024 / 148;
    for i in 0..n {
        db.put(&key_for(i), &value_for(i, 128)).unwrap();
    }
    db.sync_wal().unwrap();
    let wal_bytes: u64 = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".wal"))
        .map(|e| e.metadata().map(|m| m.len()).unwrap_or(0))
        .sum();
    std::mem::forget(db); // emulate a crash: no flush, no clean close
    let t = Instant::now();
    let db2 = Db::open(Options { background: false, ..opts }, OsFs::new_arc(false), &dir).unwrap();
    let secs = t.elapsed().as_secs_f64();
    let ok = db2.get(&key_for(n - 1)).unwrap().is_some() && db2.get(&key_for(0)).unwrap().is_some();
    let body = json!({ "bench": "engine-recovery", "config": { "target_mb": mb }, "results": { "records": n, "wal_bytes": wal_bytes, "recovery_secs": secs, "data_intact": ok } });
    let labels = json!({ "scope": "single node, local disk", "faults": "process abandoned without flush (WAL replay path); not a power-loss test" });
    println!("{}", write_result(&a.s("--out", "bench/results"), &a.s("--id", &format!("S1-recovery-{mb}mb")), body, labels).unwrap());
    let _ = std::fs::remove_dir_all(&dir);
}

// ------------------------------------------------------------------ S2: crash consistency

fn run_s2_sim(a: &Args) {
    let iters = a.u("--iterations", 2000);
    let mut violations = 0u64;
    let mut acked_total = 0u64;
    let mut rng = ChaCha8Rng::seed_from_u64(a.u("--seed", 1));
    let t0 = Instant::now();
    let opts = Options {
        memtable_size: 4096,
        block_size: 256,
        l0_compaction_trigger: 2,
        level_base_bytes: 8192,
        target_file_size: 4096,
        max_levels: 4,
        sync: SyncPolicy::Group,
        background: false,
        ..Options::default()
    };
    for it in 0..iters {
        let fs = MemFs::new();
        let vfs = fs.as_vfs();
        let db = Db::open(opts.clone(), vfs.clone(), "/db").unwrap();
        let mut acked: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();
        let n = rng.gen_range(10..200);
        for i in 0..n {
            let k = format!("k{:03}", rng.gen_range(0..60)).into_bytes();
            if rng.gen_bool(0.2) {
                db.delete(&k).unwrap();
                acked.insert(k, None);
            } else {
                let v = format!("v{it}-{i}").into_bytes();
                db.put(&k, &v).unwrap();
                acked.insert(k, Some(v));
            }
            if rng.gen_bool(0.02) {
                db.flush().unwrap();
            }
        }
        acked_total += acked.len() as u64;
        db.close();
        drop(db);
        fs.crash(rng.gen()); // power loss: unsynced bytes lost/torn
        let db = Db::open(opts.clone(), vfs, "/db").unwrap();
        for (k, v) in &acked {
            if db.get(k).unwrap() != *v {
                violations += 1;
            }
        }
    }
    let body = json!({
        "bench": "crash-consistency-simulated",
        "config": { "iterations": iters, "fsync_policy": "Group (acked == synced)" },
        "results": { "iterations": iters, "acked_keys_checked": acked_total, "violations": violations, "wall_secs": t0.elapsed().as_secs_f64() },
    });
    let labels = json!({ "faults": "SIMULATED power loss (MemFs drops/tears unsynced data); NOT real hardware", "scope": "storage engine only, in-memory filesystem model" });
    println!("{}", write_result(&a.s("--out", "bench/results"), &a.s("--id", "S2-sim"), body, labels).unwrap());
    if violations > 0 {
        std::process::exit(1);
    }
}

fn run_s2_writer(a: &Args) {
    let dir = a.s("--dir", "");
    let ledger = a.s("--ledger", "");
    let sync = parse_sync(&a.s("--sync", "always"));
    let db = Db::open(Options { sync, memtable_size: 1 << 20, ..Options::default() }, OsFs::new_arc(a.flag("--strict-fsync")), &dir).unwrap();
    use std::io::Write;
    let mut lf = std::fs::OpenOptions::new().create(true).append(true).open(&ledger).unwrap();
    let start = std::fs::read_to_string(&ledger).map(|s| s.lines().count() as u64).unwrap_or(0);
    let mut i = start;
    loop {
        let k = format!("key{i:010}");
        let v = format!("value-{i}-{}", "x".repeat((i % 200) as usize));
        db.put(k.as_bytes(), v.as_bytes()).unwrap();
        // ack observed: record it (ledger ⊆ acked, which is the safe direction)
        writeln!(lf, "{i}").unwrap();
        i += 1;
    }
}

fn run_s2_driver(a: &Args) {
    let iters = a.u("--iterations", 50);
    let dir = a.s("--dir", &format!("/tmp/rkv-s2-{}", std::process::id()));
    let sync = a.s("--sync", "always");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let ledger = format!("{dir}/ledger.txt");
    let dbdir = format!("{dir}/db");
    let exe = std::env::current_exe().unwrap();
    let mut rng = ChaCha8Rng::seed_from_u64(a.u("--seed", 7));
    let (mut violations, mut checked) = (0u64, 0u64);
    let mut recov = Hist::new();
    for _ in 0..iters {
        let mut child = std::process::Command::new(&exe)
            .args(["s2-writer", "--dir", &dbdir, "--ledger", &ledger, "--sync", &sync])
            .stdout(std::process::Stdio::null())
            .spawn()
            .unwrap();
        std::thread::sleep(Duration::from_millis(rng.gen_range(a.u("--min-ms", 40)..a.u("--max-ms", 400))));
        child.kill().unwrap(); // SIGKILL at a random point under write load
        child.wait().unwrap();
        let t = Instant::now();
        let db = Db::open(Options { sync: SyncPolicy::Group, ..Options::default() }, OsFs::new_arc(false), &dbdir).unwrap();
        recov.record_us(t.elapsed().as_micros() as u64);
        let ids: Vec<u64> = std::fs::read_to_string(&ledger).unwrap_or_default().lines().filter_map(|l| l.parse().ok()).collect();
        for i in &ids {
            checked += 1;
            let k = format!("key{i:010}");
            let want = format!("value-{i}-{}", "x".repeat((*i % 200) as usize));
            if db.get(k.as_bytes()).unwrap().as_deref() != Some(want.as_bytes()) {
                violations += 1;
            }
        }
        db.close();
    }
    let total_acked = std::fs::read_to_string(&ledger).map(|s| s.lines().count()).unwrap_or(0);
    let body = json!({
        "bench": "crash-consistency-sigkill",
        "config": { "iterations": iters, "fsync_policy": sync },
        "results": { "iterations": iters, "acked_keys_checked_cumulative": checked, "distinct_acked_keys": total_acked, "violations": violations, "recovery_time": recov.to_json() },
    });
    let labels = json!({ "faults": "REAL kill -9 (SIGKILL) of the writer process; page cache survives, so this does NOT test OS/power loss", "scope": "storage engine only, local disk" });
    println!("{}", write_result(&a.s("--out", "bench/results"), &a.s("--id", "S2-sigkill"), body, labels).unwrap());
    let _ = std::fs::remove_dir_all(&dir);
    if violations > 0 {
        std::process::exit(1);
    }
}

// ------------------------------------------------------------------ cluster / txn benchmarks

fn clients(a: &Args) -> (Option<Arc<kv_client::PdClient>>, Vec<String>) {
    let pd = a.get("--pd").map(|p| Arc::new(kv_client::PdClient::new(p.split(',').map(|s| s.to_string()).collect())));
    let seeds = a.get("--seeds").map(|s| s.split(',').map(|x| x.to_string()).collect()).unwrap_or_default();
    (pd, seeds)
}

fn read_consistency(s: &str) -> kv_client::raw::ReadConsistency {
    use kv_client::raw::ReadConsistency::*;
    if s == "lease" {
        Lease
    } else if let Some(ms) = s.strip_prefix("stale:") {
        Stale { max_staleness_ms: ms.parse().unwrap_or(1000) }
    } else {
        Linearizable
    }
}

/// Client abstraction so the same workload can target rkv or an etcd baseline.
enum Be {
    Rkv(kv_client::RawClient),
    #[cfg(feature = "etcd-baseline")]
    Etcd(etcd_client::Client),
}

impl Be {
    async fn put(&self, k: &[u8], v: &[u8]) -> Result<(), String> {
        match self {
            Be::Rkv(c) => c.put(k, v).await.map_err(|e| e.to_string()),
            #[cfg(feature = "etcd-baseline")]
            Be::Etcd(c) => c.clone().put(k, v, None).await.map(|_| ()).map_err(|e| e.to_string()),
        }
    }
    async fn get(&self, k: &[u8], rc: kv_client::raw::ReadConsistency) -> Result<(), String> {
        match self {
            Be::Rkv(c) => c.get_with(k, rc).await.map(|_| ()).map_err(|e| e.to_string()),
            #[cfg(feature = "etcd-baseline")]
            Be::Etcd(c) => c.clone().get(k, None).await.map(|_| ()).map_err(|e| e.to_string()), // linearizable by default
        }
    }
    async fn scan(&self, k: &[u8], n: usize) -> Result<(), String> {
        match self {
            Be::Rkv(c) => c.scan(k, &[], n).await.map(|_| ()).map_err(|e| e.to_string()),
            #[cfg(feature = "etcd-baseline")]
            Be::Etcd(c) => {
                c.clone().get(k, Some(etcd_client::GetOptions::new().with_from_key().with_limit(n as i64))).await.map(|_| ()).map_err(|e| e.to_string())
            }
        }
    }
}

async fn make_be(backend: &str, endpoints: &[String], router: &Arc<kv_client::Router>) -> Be {
    match backend {
        #[cfg(feature = "etcd-baseline")]
        "etcd" => Be::Etcd(etcd_client::Client::connect(endpoints, None).await.expect("connect to etcd")),
        "rkv" => Be::Rkv(kv_client::RawClient::with_router(router.clone())),
        other => {
            let _ = endpoints;
            panic!("backend {other} not available in this build (rebuild with the matching cargo feature)")
        }
    }
}

async fn run_cluster(a: &Args) {
    let (pd, seeds) = clients(a);
    let wl = Workload::by_name(a.s("--workload", "a").chars().next().unwrap()).expect("workload a-f");
    let records = a.u("--records", 10_000);
    let ops = a.u("--ops", 20_000);
    let secs = a.u("--secs", 0);
    let nclients = a.u("--clients", 16) as usize;
    let vsize = a.u("--value-size", 128) as usize;
    let rc = read_consistency(&a.s("--read-mode", "linearizable"));
    let router = kv_client::Router::new(pd.clone(), seeds.clone());
    let backend = a.s("--backend", "rkv");
    let etcd_eps: Vec<String> = a.get("--etcd-endpoints").map(|s| s.split(',').map(|x| x.to_string()).collect()).unwrap_or_default();
    // load
    let t0 = Instant::now();
    let per = records / nclients as u64;
    let mut hs = vec![];
    for t in 0..nclients as u64 {
        let c = make_be(&backend, &etcd_eps, &router).await;
        hs.push(tokio::spawn(async move {
            let (lo, hi) = (t * per, if t == nclients as u64 - 1 { records } else { t * per + per });
            for i in lo..hi {
                c.put(&key_for(i), &value_for(i, vsize)).await.expect("load put");
            }
        }));
    }
    for h in hs {
        h.await.unwrap();
    }
    let load_secs = t0.elapsed().as_secs_f64();
    let gen = Arc::new(OpGen::new(wl.clone(), records, a.f("--theta", 0.99), vsize));
    let done = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let deadline = if secs > 0 { Some(Instant::now() + Duration::from_secs(secs)) } else { None };
    let t1 = Instant::now();
    let timeline: Arc<std::sync::Mutex<BTreeMap<u64, (u64, u64, u64)>>> = Arc::new(std::sync::Mutex::new(BTreeMap::new())); // sec -> (ok, errors, max_us)
    let want_timeline = a.flag("--timeline");
    let mut hs = vec![];
    for t in 0..nclients {
        let (gen, done, errors, timeline) = (gen.clone(), done.clone(), errors.clone(), timeline.clone());
        let c = make_be(&backend, &etcd_eps, &router).await;
        hs.push(tokio::spawn(async move {
            let mut rng = ChaCha8Rng::seed_from_u64(t as u64 + 99);
            let mut h: BTreeMap<&'static str, Hist> = BTreeMap::new();
            loop {
                if let Some(d) = deadline {
                    if Instant::now() >= d {
                        break;
                    }
                } else if done.fetch_add(1, Ordering::Relaxed) >= ops {
                    break;
                }
                let op = gen.next(&mut rng);
                let kind = op.kind();
                let s = Instant::now();
                let r = match op {
                    Op::Read(k) => c.get(&k, rc).await,
                    Op::Update(k, v) | Op::Insert(k, v) => c.put(&k, &v).await,
                    Op::Scan(k, n) => c.scan(&k, n).await,
                    Op::Rmw(k, v) => match c.get(&k, rc).await {
                        Ok(_) => c.put(&k, &v).await,
                        Err(e) => Err(e),
                    },
                };
                if r.is_err() {
                    errors.fetch_add(1, Ordering::Relaxed);
                } else {
                    h.entry(kind).or_default().record_us(s.elapsed().as_micros() as u64);
                }
                if want_timeline {
                    let sec = t1.elapsed().as_secs();
                    let mut tl = timeline.lock().unwrap();
                    let e = tl.entry(sec).or_insert((0, 0, 0));
                    if r.is_err() {
                        e.1 += 1
                    } else {
                        e.0 += 1
                    }
                    e.2 = e.2.max(s.elapsed().as_micros() as u64);
                }
            }
            h
        }));
    }
    let mut merged: BTreeMap<&'static str, Hist> = BTreeMap::new();
    for h in hs {
        for (k, v) in h.await.unwrap() {
            merged.entry(k).or_default().merge(&v);
        }
    }
    let run_secs = t1.elapsed().as_secs_f64();
    let mut all = Hist::new();
    let mut by = serde_json::Map::new();
    for (k, h) in &merged {
        all.merge(h);
        by.insert(k.to_string(), h.to_json());
    }
    let body = json!({
        "bench": "cluster-ycsb",
        "config": { "workload": wl.name.to_string(), "records": records, "clients": nclients, "value_size": vsize, "read_mode": a.s("--read-mode", "linearizable"), "backend": a.s("--backend", "rkv"), "pd": a.get("--pd"), "seeds": seeds, "cluster_label": a.s("--cluster-label", "unlabeled") },
        "results": { "load_secs": load_secs, "run_secs": run_secs, "ok_ops": all.count, "errors": errors.load(Ordering::Relaxed), "ops_per_sec": all.count as f64 / run_secs, "latency_all": all.to_json(), "latency_by_op": by, "client_retries": router.retries.load(Ordering::Relaxed),
            "timeline": timeline.lock().unwrap().iter().map(|(s, (ok, er, mx))| json!({"sec": s, "ok": ok, "errors": er, "max_latency_us": mx})).collect::<Vec<_>>() },
    });
    let labels = json!({ "scope": a.s("--scope-label", "localhost multi-process cluster (all nodes on one machine, shared CPU/disk, no network latency)"), "faults": a.s("--fault-label", "none"), "clients": "closed loop; no coordinated-omission correction" });
    println!("{}", write_result(&a.s("--out", "bench/results"), &a.s("--id", "R1"), body, labels).unwrap());
}

async fn run_txn_bank(a: &Args) {
    use kv_client::txn_client::TxnError;
    let (pd, seeds) = clients(a);
    let pd = pd.expect("--pd required");
    let iso = if a.s("--isolation", "snapshot") == "serializable" { kv_client::IsolationLevel::Serializable } else { kv_client::IsolationLevel::Snapshot };
    let tc = kv_client::TxnClient::with_isolation(pd, seeds, iso);
    let accounts = a.u("--accounts", 1000);
    let nclients = a.u("--clients", 16) as usize;
    let theta = a.f("--theta", 0.5);
    let keys_per = a.u("--keys-per-txn", 4) as usize;
    let secs = a.u("--secs", 0);
    let ops = a.u("--ops", 2000);
    let initial = 1000i64;
    let key = |i: u64| format!("bank{i:08}").into_bytes();
    // load balances in batches
    let t0 = Instant::now();
    for chunk in (0..accounts).collect::<Vec<_>>().chunks(100) {
        let mut t = tc.begin().await.unwrap();
        for i in chunk {
            t.put(&key(*i), initial.to_string().as_bytes());
        }
        t.commit().await.expect("load");
    }
    let load_secs = t0.elapsed().as_secs_f64();
    let zipf = Arc::new(ycsb::dist::Zipf::new(accounts, theta));
    let (commits, conflicts, aborted, other) =
        (Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)));
    let done = Arc::new(AtomicU64::new(0));
    let deadline = if secs > 0 { Some(Instant::now() + Duration::from_secs(secs)) } else { None };
    let t1 = Instant::now();
    let mut hs = vec![];
    for c in 0..nclients {
        let (tc, zipf, commits, conflicts, aborted, other, done) =
            (tc.clone(), zipf.clone(), commits.clone(), conflicts.clone(), aborted.clone(), other.clone(), done.clone());
        hs.push(tokio::spawn(async move {
            let mut rng = ChaCha8Rng::seed_from_u64(c as u64 + 5);
            let mut h = Hist::new();
            loop {
                if let Some(d) = deadline {
                    if Instant::now() >= d {
                        break;
                    }
                } else if done.fetch_add(1, Ordering::Relaxed) >= ops {
                    break;
                }
                let mut ids: Vec<u64> = vec![];
                while ids.len() < keys_per {
                    let i = zipf.next_scrambled(&mut rng);
                    if !ids.contains(&i) {
                        ids.push(i);
                    }
                }
                let s = Instant::now();
                let res: Result<(), TxnError> = async {
                    let mut t = tc.begin().await?;
                    let mut bal = vec![];
                    for i in &ids {
                        let v = t.get(&key(*i)).await?.expect("account exists");
                        bal.push(String::from_utf8(v).unwrap().parse::<i64>().unwrap());
                    }
                    // move 1 from the first account to each of the others: total is conserved
                    let n = ids.len() as i64 - 1;
                    t.put(&key(ids[0]), (bal[0] - n).to_string().as_bytes());
                    for (j, i) in ids.iter().enumerate().skip(1) {
                        t.put(&key(*i), (bal[j] + 1).to_string().as_bytes());
                    }
                    t.commit().await.map(|_| ())
                }
                .await;
                match res {
                    Ok(()) => {
                        commits.fetch_add(1, Ordering::Relaxed);
                        h.record_us(s.elapsed().as_micros() as u64);
                    }
                    Err(TxnError::Conflict { .. }) => {
                        conflicts.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(TxnError::Aborted(_)) => {
                        aborted.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        other.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            h
        }));
    }
    let mut lat = Hist::new();
    for h in hs {
        lat.merge(&h.await.unwrap());
    }
    let run_secs = t1.elapsed().as_secs_f64();
    tokio::time::sleep(Duration::from_millis(1500)).await; // let async secondary commits land
                                                           // audit: total must be conserved
    let mut total = 0i64;
    let mut audit_ok = false;
    for _ in 0..5 {
        let mut t = tc.begin().await.unwrap();
        let mut sum = 0i64;
        let mut ok = true;
        for i in 0..accounts {
            match t.get(&key(i)).await {
                Ok(Some(v)) => sum += String::from_utf8(v).unwrap().parse::<i64>().unwrap(),
                _ => ok = false,
            }
        }
        if ok {
            total = sum;
            audit_ok = true;
            break;
        }
    }
    let attempts = commits.load(Ordering::Relaxed) + conflicts.load(Ordering::Relaxed) + aborted.load(Ordering::Relaxed) + other.load(Ordering::Relaxed);
    let body = json!({
        "bench": "txn-bank",
        "config": { "accounts": accounts, "clients": nclients, "theta": theta, "keys_per_txn": keys_per, "isolation": a.s("--isolation", "snapshot") },
        "results": {
            "load_secs": load_secs, "run_secs": run_secs, "commits": commits.load(Ordering::Relaxed), "write_conflicts": conflicts.load(Ordering::Relaxed),
            "aborted": aborted.load(Ordering::Relaxed), "other_errors": other.load(Ordering::Relaxed),
            "abort_rate": if attempts > 0 { 1.0 - commits.load(Ordering::Relaxed) as f64 / attempts as f64 } else { 0.0 },
            "commits_per_sec": commits.load(Ordering::Relaxed) as f64 / run_secs, "commit_latency": lat.to_json(),
            "audit_total": total, "audit_expected": accounts as i64 * initial, "invariant_ok": audit_ok && total == accounts as i64 * initial,
        },
    });
    let labels = json!({ "scope": a.s("--scope-label", "localhost multi-process cluster"), "faults": a.s("--fault-label", "none") });
    let out = write_result(&a.s("--out", "bench/results"), &a.s("--id", "D2-bank"), body, labels).unwrap();
    println!("{out}");
    if !(audit_ok && total == accounts as i64 * initial) {
        eprintln!("BANK INVARIANT VIOLATED: total {total}");
        std::process::exit(1);
    }
}

async fn run_txn_skew(a: &Args) {
    let (pd, seeds) = clients(a);
    let pd = pd.expect("--pd required");
    let pairs = a.u("--pairs", 200);
    let mut results = serde_json::Map::new();
    for (name, iso) in [("snapshot", kv_client::IsolationLevel::Snapshot), ("serializable", kv_client::IsolationLevel::Serializable)] {
        let tc = kv_client::TxnClient::with_isolation(pd.clone(), seeds.clone(), iso);
        let tag = format!("{}-{}", name, std::process::id());
        let ka = |i: u64| format!("doc/{tag}/{i:06}/a").into_bytes();
        let kb = |i: u64| format!("doc/{tag}/{i:06}/b").into_bytes();
        let mut t = tc.begin().await.unwrap();
        for i in 0..pairs {
            t.put(&ka(i), b"1");
            t.put(&kb(i), b"1");
        }
        t.commit().await.unwrap();
        // for each pair, two concurrent txns: each checks the other doctor is on call, then goes off call
        let mut hs = vec![];
        for i in 0..pairs {
            for side in 0..2u8 {
                let tc = tc.clone();
                let (a_, b_) = (ka(i), kb(i));
                hs.push(tokio::spawn(async move {
                    for _ in 0..5 {
                        let Ok(mut t) = tc.begin().await else { continue };
                        let (Ok(Some(x)), Ok(Some(y))) = (t.get(&a_).await, t.get(&b_).await) else { continue };
                        if x == b"1" && y == b"1" {
                            t.put(if side == 0 { &a_ } else { &b_ }, b"0");
                            if t.commit().await.is_ok() {
                                return;
                            }
                        } else {
                            return;
                        }
                    }
                }));
            }
        }
        for h in hs {
            let _ = h.await;
        }
        tokio::time::sleep(Duration::from_millis(1000)).await;
        let mut t = tc.begin().await.unwrap();
        let mut skew = 0u64;
        for i in 0..pairs {
            let x = t.get(&ka(i)).await.unwrap();
            let y = t.get(&kb(i)).await.unwrap();
            if x.as_deref() == Some(b"0") && y.as_deref() == Some(b"0") {
                skew += 1;
            }
        }
        results.insert(name.to_string(), json!({ "pairs": pairs, "write_skew_violations": skew }));
    }
    let body = json!({ "bench": "txn-write-skew", "config": { "pairs": pairs }, "results": Value::Object(results) });
    let labels = json!({ "scope": "localhost multi-process cluster", "expectation": "snapshot isolation: skew possible (expected); serializable mode: must be 0 for point reads" });
    println!("{}", write_result(&a.s("--out", "bench/results"), &a.s("--id", "D2-skew"), body, labels).unwrap());
}

async fn run_lock_recovery(a: &Args) {
    use rkv_proto::{Mutation, PrewriteReq};
    let (pd, seeds) = clients(a);
    let pd = pd.expect("--pd required");
    let trials = a.u("--trials", 20);
    let ttl = a.u("--ttl-ms", 500);
    let tc = kv_client::TxnClient::new(pd.clone(), seeds);
    let mut hist = Hist::new();
    let mut failures = 0;
    for i in 0..trials {
        let key = format!("lockrec/{}/{i}", std::process::id()).into_bytes();
        let start_ts = pd.tso_fresh().await.unwrap();
        // "coordinator crash": prewrite (primary == this key) and never commit
        let (k2, ts2) = (key.clone(), start_ts);
        tc.router
            .exec(&key, 0, 0, |mut c, ctx| {
                let req = PrewriteReq {
                    ctx: Some(ctx),
                    mutations: vec![Mutation { op: 1, key: k2.clone(), value: b"orphan".to_vec() }],
                    primary: k2.clone(),
                    start_ts: ts2,
                    ttl_ms: ttl,
                    read_keys: vec![],
                    now_ms: 0,
                };
                async move { c.prewrite(req).await }
            })
            .await
            .expect("prewrite");
        // a reader now has to resolve the abandoned lock
        let t0 = Instant::now();
        let mut t = tc.begin().await.unwrap();
        match t.get(&key).await {
            Ok(None) => hist.record_us(t0.elapsed().as_micros() as u64),
            _ => failures += 1,
        }
    }
    let body = json!({ "bench": "txn-lock-recovery", "config": { "trials": trials, "lock_ttl_ms": ttl }, "results": { "trials": trials, "failures": failures, "time_to_unblocked_read": hist.to_json() } });
    let labels = json!({ "scope": "localhost multi-process cluster", "faults": "simulated coordinator crash (client abandons txn after prewrite); the cluster itself is healthy" });
    println!("{}", write_result(&a.s("--out", "bench/results"), &a.s("--id", "D2-lock-recovery"), body, labels).unwrap());
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let sub = args.get(1).cloned().unwrap_or_default();
    let a = Args(args[2.min(args.len())..].to_vec());
    let rt = || tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
    match sub.as_str() {
        "engine" => run_engine(&a),
        "recovery" => run_recovery(&a),
        "s2-sim" => run_s2_sim(&a),
        "s2-writer" => run_s2_writer(&a),
        "s2-driver" => run_s2_driver(&a),
        "cluster" => rt().block_on(run_cluster(&a)),
        "txn-bank" => rt().block_on(run_txn_bank(&a)),
        "txn-skew" => rt().block_on(run_txn_skew(&a)),
        "lock-recovery" => rt().block_on(run_lock_recovery(&a)),
        _ => {
            eprintln!("usage: rkv-bench <engine|s2-sim|s2-driver|cluster|txn-bank|txn-skew|lock-recovery> [--id NAME] [--out DIR] ...");
            std::process::exit(2);
        }
    }
}
