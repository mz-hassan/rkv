//! Model-based tests: the engine must behave like a BTreeMap, including across
//! simulated power loss + recovery (MemFs drops/tears unsynced data).
use kv_engine::*;
use proptest::prelude::*;
use std::collections::BTreeMap;

fn small_opts(sync: SyncPolicy, style: CompactionStyle) -> Options {
    Options {
        memtable_size: 1500,
        block_size: 128,
        l0_compaction_trigger: 2,
        level_base_bytes: 3000,
        target_file_size: 1500,
        max_levels: 4,
        cache_bytes: 1 << 16,
        sync,
        background: false,
        compaction: style,
        tier_min_merge: 2,
        ..Options::default()
    }
}

#[derive(Debug, Clone)]
enum Op {
    Put(u8, u16),
    Del(u8),
    Get(u8),
    Scan(u8, u8),
    Flush,
    Compact,
    Crash(u64),
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        6 => (0u8..40, any::<u16>()).prop_map(|(k, v)| Op::Put(k, v)),
        2 => (0u8..40).prop_map(Op::Del),
        3 => (0u8..40).prop_map(Op::Get),
        1 => (0u8..40, 0u8..40).prop_map(|(a, b)| Op::Scan(a, b)),
        1 => Just(Op::Flush),
        1 => Just(Op::Compact),
        1 => any::<u64>().prop_map(Op::Crash),
    ]
}

fn key(k: u8) -> Vec<u8> {
    format!("key{k:03}").into_bytes()
}
fn val(v: u16) -> Vec<u8> {
    // varied sizes to trigger flushes
    let n = 20 + (v as usize % 60);
    format!("{v:05}").repeat(n / 5 + 1).into_bytes()
}

fn run(ops: Vec<Op>, style: CompactionStyle) {
    let fs = MemFs::new();
    let vfs = fs.as_vfs();
    let mut db = Db::open(small_opts(SyncPolicy::Group, style), vfs.clone(), "/db").unwrap();
    let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    for op in ops {
        match op {
            Op::Put(k, v) => {
                db.put(&key(k), &val(v)).unwrap();
                model.insert(key(k), val(v));
            }
            Op::Del(k) => {
                db.delete(&key(k)).unwrap();
                model.remove(&key(k));
            }
            Op::Get(k) => assert_eq!(db.get(&key(k)).unwrap(), model.get(&key(k)).cloned()),
            Op::Scan(a, b) => {
                let (a, b) = (key(a.min(b)), key(a.max(b)));
                let got = db.scan(&a, &b, 0).unwrap();
                let want: Vec<_> = model.range(a.clone()..b.clone()).map(|(k, v)| (k.clone(), v.clone())).collect();
                if got != want {
                    let gk: Vec<String> = got.iter().map(|(k, _)| String::from_utf8_lossy(k).into_owned()).collect();
                    let wk: Vec<String> = want.iter().map(|(k, _)| String::from_utf8_lossy(k).into_owned()).collect();
                    let val_diff: Vec<String> = got
                        .iter()
                        .filter(|(k, v)| want.iter().any(|(k2, v2)| k2 == k && v2 != v))
                        .map(|(k, _)| String::from_utf8_lossy(k).into_owned())
                        .collect();
                    panic!(
                        "scan [{:?},{:?}) mismatch\n  engine keys: {gk:?}\n  model keys:  {wk:?}\n  keys with different VALUES: {val_diff:?}",
                        String::from_utf8_lossy(&a),
                        String::from_utf8_lossy(&b)
                    );
                }
            }
            Op::Flush => db.flush().unwrap(),
            Op::Compact => db.compact_until_idle().unwrap(),
            Op::Crash(seed) => {
                db.close();
                drop(db);
                fs.crash(seed);
                db = Db::open(small_opts(SyncPolicy::Group, style), vfs.clone(), "/db").unwrap();
                // synced writes (policy Group) must all survive
                let got = db.scan(b"", b"", 0).unwrap();
                let want: Vec<_> = model.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                assert_eq!(got, want, "state after crash recovery differs from model");
            }
        }
    }
    let got = db.scan(b"", b"", 0).unwrap();
    let want: Vec<_> = model.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    assert_eq!(got, want);
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 300, ..ProptestConfig::default() })]
    #[test]
    fn leveled_matches_model(ops in proptest::collection::vec(op_strategy(), 1..200)) {
        run(ops, CompactionStyle::Leveled);
    }
    #[test]
    fn tiered_matches_model(ops in proptest::collection::vec(op_strategy(), 1..200)) {
        run(ops, CompactionStyle::SizeTiered);
    }
}

/// With SyncPolicy::Never, a crash may lose a suffix of writes but the result
/// must equal the model after SOME prefix (no holes, no corruption).
#[test]
fn never_policy_recovers_a_prefix() {
    for seed in 0..60u64 {
        let fs = MemFs::new();
        let vfs = fs.as_vfs();
        let db = Db::open(small_opts(SyncPolicy::Never, CompactionStyle::Leveled), vfs.clone(), "/db").unwrap();
        let mut prefixes: Vec<BTreeMap<Vec<u8>, Vec<u8>>> = vec![BTreeMap::new()];
        let mut m = BTreeMap::new();
        for i in 0..300u32 {
            let k = key((i * 7 % 40) as u8);
            if i % 5 == 4 {
                db.delete(&k).unwrap();
                m.remove(&k);
            } else {
                db.put(&k, &val(i as u16)).unwrap();
                m.insert(k, val(i as u16));
            }
            prefixes.push(m.clone());
        }
        db.close();
        drop(db);
        fs.crash(seed);
        let db = Db::open(small_opts(SyncPolicy::Never, CompactionStyle::Leveled), vfs, "/db").unwrap();
        let got: BTreeMap<_, _> = db.scan(b"", b"", 0).unwrap().into_iter().collect();
        assert!(prefixes.contains(&got), "seed {seed}: recovered state is not a prefix of history");
    }
}

#[test]
fn mvcc_snapshot_reads() {
    let fs = MemFs::new();
    let mut o = small_opts(SyncPolicy::Group, CompactionStyle::Leveled);
    o.memtable_size = 1 << 20;
    let db = Db::open(o, fs.as_vfs(), "/db").unwrap();
    db.set_gc_watermark(0); // keep all versions
    let mut b = WriteBatch::new();
    b.put_at(b"k", b"v10", 10);
    db.write(b).unwrap();
    let mut b = WriteBatch::new();
    b.put_at(b"k", b"v20", 20);
    db.write(b).unwrap();
    let mut b = WriteBatch::new();
    b.delete_at(b"k", 30);
    db.write(b).unwrap();
    db.flush().unwrap();
    assert_eq!(db.get_at(b"k", 5).unwrap(), None);
    assert_eq!(db.get_at(b"k", 10).unwrap().unwrap(), b"v10");
    assert_eq!(db.get_at(b"k", 25).unwrap().unwrap(), b"v20");
    assert_eq!(db.get_at(b"k", 99).unwrap(), None);
    assert_eq!(db.scan_at(b"", b"", 15, 0).unwrap().len(), 1);
}

#[test]
fn gc_drops_old_versions_and_tombstones() {
    let fs = MemFs::new();
    let db = Db::open(small_opts(SyncPolicy::Group, CompactionStyle::Leveled), fs.as_vfs(), "/db").unwrap();
    for round in 0..20u16 {
        for k in 0..30u8 {
            db.put(&key(k), &val(round)).unwrap();
        }
    }
    for k in 0..30u8 {
        db.delete(&key(k)).unwrap();
    }
    db.compact_until_idle().unwrap();
    let s = db.stats();
    assert!(db.scan(b"", b"", 0).unwrap().is_empty());
    // everything was overwritten then deleted: space must be tiny after full compaction
    assert!(s.space_bytes < 20_000, "space {}", s.space_bytes);
}

#[test]
fn fsync_failure_makes_db_refuse_writes() {
    let fs = MemFs::new();
    let db = Db::open(small_opts(SyncPolicy::Group, CompactionStyle::Leveled), fs.as_vfs(), "/db").unwrap();
    db.put(b"a", b"1").unwrap();
    fs.fail_next_syncs(1);
    assert!(db.put(b"b", b"2").is_err());
    assert!(db.put(b"c", b"3").is_err(), "must stay failed after fsync error");
}

#[test]
fn snapshot_export_import_roundtrip() {
    let fs = MemFs::new();
    let db = Db::open(small_opts(SyncPolicy::Group, CompactionStyle::Leveled), fs.as_vfs(), "/a").unwrap();
    for i in 0..500u32 {
        db.put(format!("k{i:04}").as_bytes(), format!("v{i}").as_bytes()).unwrap();
    }
    db.delete(b"k0100").unwrap();
    let chunks = db.export_snapshot(2000).unwrap();
    assert!(chunks.len() > 1);
    let db2 = Db::import_snapshot(small_opts(SyncPolicy::Group, CompactionStyle::Leveled), fs.as_vfs(), "/b", &chunks).unwrap();
    assert_eq!(db.scan(b"", b"", 0).unwrap(), db2.scan(b"", b"", 0).unwrap());
    assert_eq!(db2.get(b"k0100").unwrap(), None);
}

/// Guard against the model tests silently not exercising deep levels.
#[test]
fn model_options_actually_exercise_compaction() {
    for style in [CompactionStyle::Leveled, CompactionStyle::SizeTiered] {
        let fs = MemFs::new();
        let db = Db::open(small_opts(SyncPolicy::Group, style), fs.as_vfs(), "/db").unwrap();
        for i in 0..400u16 {
            db.put(&key((i % 40) as u8), &val(i)).unwrap();
        }
        let m = db.metrics();
        use std::sync::atomic::Ordering::Relaxed;
        assert!(m.flushes.load(Relaxed) > 5, "{style:?} flushes");
        assert!(m.compactions.load(Relaxed) > 2, "{style:?} compactions");
        if style == CompactionStyle::Leveled {
            assert!(db.stats().level_files[1..].iter().sum::<usize>() > 0);
        }
    }
}
