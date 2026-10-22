//! Raft state-machine throughput: a 3-node in-memory cluster driven synchronously.
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use raft::{Config, InitialState, Membership, Message, Raft, Role};

fn cluster() -> Vec<Raft> {
    (1..=3u64).map(|id| Raft::new(Config::new(id), Membership::new([1, 2, 3]), InitialState::default())).collect()
}

/// Deliver everything until quiescent (zero-latency network, instant persistence).
fn pump(nodes: &mut [Raft]) {
    loop {
        let mut msgs: Vec<Message> = vec![];
        for n in nodes.iter_mut() {
            while n.has_ready() {
                let rd = n.ready();
                msgs.extend(rd.messages.iter().cloned());
                n.advance(&rd);
            }
        }
        if msgs.is_empty() {
            return;
        }
        for m in msgs {
            nodes[(m.to - 1) as usize].step(m);
        }
    }
}

fn propose_commit(c: &mut Criterion) {
    let mut nodes = cluster();
    for _ in 0..40 {
        for n in nodes.iter_mut() {
            n.tick();
        }
        pump(&mut nodes);
    }
    let l = nodes.iter().position(|n| n.role() == Role::Leader).expect("leader");
    let mut g = c.benchmark_group("raft");
    g.throughput(Throughput::Elements(100));
    g.bench_function("propose_100_and_commit_3_nodes", |b| {
        b.iter(|| {
            for _ in 0..100 {
                nodes[l].propose(vec![0u8; 128]).unwrap();
            }
            pump(&mut nodes);
        })
    });
    g.finish();
}

criterion_group!(benches, propose_commit);
criterion_main!(benches);
