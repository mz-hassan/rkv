//! Short scans copy only a bounded prefix of each memtable (a full copy made YCSB-E O(memtable) per scan). The result
//! must be identical to an unbounded scan, including keys with many versions and deletes.
use kv_engine::*;

#[test]
fn limited_scan_matches_unbounded_scan() {
    let fs = MemFs::new();
    // a big memtable so everything stays in memory
    let opts = Options { memtable_size: 64 << 20, background: false, ..Options::default() };
    let db = Db::open(opts, fs.as_vfs(), "/db").unwrap();
    for round in 0..5u32 {
        for i in 0..2000u32 {
            db.put(format!("k{i:05}").as_bytes(), format!("v{round}-{i}").as_bytes()).unwrap();
            // 5 versions per key
        }
    }
    for i in (0..2000u32).step_by(3) {
        db.delete(format!("k{i:05}").as_bytes()).unwrap();
    }
    let all = db.scan(b"", b"", 0).unwrap();
    assert_eq!(all.len(), 2000 - 667);
    for start in [0u32, 1, 2, 500, 1999] {
        let s = format!("k{start:05}");
        let full: Vec<_> = all.iter().filter(|(k, _)| k.as_slice() >= s.as_bytes()).cloned().collect();
        for limit in [1usize, 2, 10, 100, 700] {
            let got = db.scan(s.as_bytes(), b"", limit).unwrap();
            let want: Vec<_> = full.iter().take(limit).cloned().collect();
            assert_eq!(got, want, "start={s} limit={limit}");
        }
    }
}
