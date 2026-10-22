//! M1 micro-benchmarks. Run: cargo bench -p kv-engine   (results land in target/criterion; not part of bench/results)
use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use kv_engine::bloom::Bloom;
use kv_engine::key::{Entry, KIND_PUT};
use kv_engine::memtable::Memtable;
use kv_engine::{Db, MemFs, Options, SyncPolicy};

fn skiplist(c: &mut Criterion) {
    let mut g = c.benchmark_group("memtable");
    g.throughput(Throughput::Elements(1));
    g.bench_function("insert_random", |b| {
        let m = Memtable::new(1);
        let mut i = 0u64;
        b.iter(|| {
            i = i.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            m.insert(Entry::new(&i.to_be_bytes(), 1, KIND_PUT, b"0123456789abcdef"));
        })
    });
    let m = Memtable::new(2);
    for i in 0..100_000u64 {
        m.insert(Entry::new(&i.to_be_bytes(), 1, KIND_PUT, b"v"));
    }
    g.bench_function("get_hit_100k", |b| {
        let mut i = 0u64;
        b.iter(|| {
            i = (i + 7919) % 100_000;
            m.get(&i.to_be_bytes(), u64::MAX)
        })
    });
    g.finish();
}

fn bloom(c: &mut Criterion) {
    let keys: Vec<Vec<u8>> = (0..100_000u32).map(|i| i.to_be_bytes().to_vec()).collect();
    let f = Bloom::build(&keys, 10);
    c.bench_function("bloom_lookup", |b| {
        let mut i = 0u32;
        b.iter(|| {
            i = i.wrapping_add(1);
            Bloom::may_contain(&f, &i.to_be_bytes())
        })
    });
}

fn wal_policies(c: &mut Criterion) {
    // MemFs: measures CPU cost of the write path per policy, not disk latency (see S1 for real disks)
    let mut g = c.benchmark_group("write_path_memfs");
    for (name, sync) in [("group", SyncPolicy::Group), ("never", SyncPolicy::Never)] {
        g.bench_function(name, |b| {
            b.iter_batched(
                || Db::open(Options { sync, background: false, ..Options::default() }, MemFs::new().as_vfs(), "/db").unwrap(),
                |db| {
                    for i in 0..1000u32 {
                        db.put(&i.to_be_bytes(), b"value-value-value").unwrap();
                    }
                },
                BatchSize::LargeInput,
            )
        });
    }
    g.finish();
}

criterion_group!(benches, skiplist, bloom, wal_policies);
criterion_main!(benches);
