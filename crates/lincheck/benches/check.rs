use criterion::{criterion_group, criterion_main, Criterion};
use lincheck::{check, Op, OpKind, OpResult};

fn history(n: usize) -> Vec<Op> {
    // sequentially consistent writes/reads on one key with overlapping intervals
    let mut ops = vec![];
    let mut cur: Option<Vec<u8>> = None;
    for i in 0..n {
        let t = (i as u64) * 10;
        if i % 2 == 0 {
            let v = vec![(i % 251) as u8];
            cur = Some(v.clone());
            ops.push(Op { id: i as u64, client: 0, key: b"k".to_vec(), kind: OpKind::Write(v), invoke: t, ret: Some(t + 12), result: Some(OpResult::WriteOk) });
        } else {
            ops.push(Op {
                id: i as u64,
                client: 1,
                key: b"k".to_vec(),
                kind: OpKind::Read,
                invoke: t + 1,
                ret: Some(t + 6),
                result: Some(OpResult::ReadOk(cur.clone())),
            });
        }
    }
    ops
}

fn bench(c: &mut Criterion) {
    let h = history(200);
    c.bench_function("lincheck_200_ops_one_key", |b| b.iter(|| check(&h, 10_000_000)));
}
criterion_group!(benches, bench);
criterion_main!(benches);
