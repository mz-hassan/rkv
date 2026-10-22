#![no_main]
//! The WAL reader must never panic or loop on arbitrary bytes; it returns a valid prefix.
use kv_engine::vfs::{MemFs, Vfs};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let fs = MemFs::new();
    let v = fs.as_vfs();
    let f = v.create("/w").unwrap();
    f.append(data).unwrap();
    let (recs, valid) = kv_engine::wal::read_all(&*f).unwrap();
    assert!(valid as usize <= data.len());
    let total: usize = recs.iter().map(|r| r.len() + 8).sum();
    assert_eq!(total as u64, valid);
});
