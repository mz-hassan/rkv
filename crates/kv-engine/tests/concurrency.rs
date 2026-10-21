use kv_engine::*;
use std::sync::Arc;

#[test]
fn concurrent_writers_with_background_compaction() {
    let fs = MemFs::new();
    let opts = Options {
        memtable_size: 64 << 10,
        l0_compaction_trigger: 3,
        level_base_bytes: 256 << 10,
        target_file_size: 128 << 10,
        background: true,
        ..Options::default()
    };
    let db = Arc::new(Db::open(opts, fs.as_vfs(), "/db").unwrap());
    let threads: Vec<_> = (0..8)
        .map(|t| {
            let db = db.clone();
            std::thread::spawn(move || {
                for i in 0..3000u32 {
                    db.put(format!("t{t}-k{i:05}").as_bytes(), format!("val-{t}-{i}").repeat(4).as_bytes()).unwrap();
                    if i % 7 == 0 {
                        let _ = db.get(format!("t{t}-k{:05}", i / 2).as_bytes()).unwrap();
                    }
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    db.compact_until_idle().unwrap();
    for t in 0..8 {
        for i in (0..3000u32).step_by(97) {
            assert_eq!(db.get(format!("t{t}-k{i:05}").as_bytes()).unwrap().unwrap(), format!("val-{t}-{i}").repeat(4).as_bytes());
        }
    }
    assert_eq!(db.scan(b"", b"", 0).unwrap().len(), 8 * 3000);
    let m = db.metrics();
    use std::sync::atomic::Ordering::Relaxed;
    assert!(m.write_groups.load(Relaxed) <= m.write_batches.load(Relaxed));
    assert!(m.compactions.load(Relaxed) > 0);
}

#[test]
fn real_filesystem_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().to_str().unwrap().to_string();
    let opts = Options { memtable_size: 32 << 10, background: false, ..Options::default() };
    {
        let db = Db::open(opts.clone(), OsFs::new_arc(false), &p).unwrap();
        for i in 0..2000u32 {
            db.put(format!("k{i:05}").as_bytes(), b"value-value-value").unwrap();
        }
    }
    let db = Db::open(opts, OsFs::new_arc(false), &p).unwrap();
    assert_eq!(db.scan(b"", b"", 0).unwrap().len(), 2000);
    assert_eq!(db.get(b"k01999").unwrap().unwrap(), b"value-value-value");
}
