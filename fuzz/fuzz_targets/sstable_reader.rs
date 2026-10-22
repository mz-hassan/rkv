#![no_main]
//! Opening and iterating a corrupt SSTable must fail cleanly (Err / truncated iteration), never panic.
use kv_engine::sstable::SstReader;
use kv_engine::vfs::{MemFs, Vfs};
use libfuzzer_sys::fuzz_target;
use std::sync::Arc;

fuzz_target!(|data: &[u8]| {
    let fs = MemFs::new();
    let v = fs.as_vfs();
    let f = v.create("/t.sst").unwrap();
    f.append(data).unwrap();
    if let Ok(r) = SstReader::open(1, f, None) {
        let r = Arc::new(r);
        let _ = r.get(b"key", u64::MAX);
        let mut n = 0;
        for _e in r.iter(b"", b"") {
            n += 1;
            if n > 100_000 {
                break;
            }
        }
    }
});
