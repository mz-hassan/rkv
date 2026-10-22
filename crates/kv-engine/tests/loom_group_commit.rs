//! loom model of the engine's write-group (leader/follower) protocol from `db.rs`:
//! writers enqueue; the writer at the head of an idle queue becomes leader, takes the whole
//! group, "persists" it, publishes each result and wakes everyone. Properties checked under
//! every interleaving: each writer completes exactly once, results are never lost, and two
//! leaders are never active at the same time.
//! Run: RUSTFLAGS="--cfg loom" cargo test -p kv-engine --test loom_group_commit --release
#![cfg(loom)]
use loom::sync::atomic::{AtomicUsize, Ordering};
use loom::sync::{Arc, Condvar, Mutex};
use loom::thread;
use std::collections::VecDeque;

struct Wq {
    q: VecDeque<usize>,
    active: bool,
}

struct Shared {
    wq: Mutex<Wq>,
    cv: Condvar,
    results: Vec<Mutex<Option<u32>>>,
    leaders_now: AtomicUsize,
    max_leaders: AtomicUsize,
    applied: Mutex<Vec<usize>>,
}

fn write(s: &Arc<Shared>, me: usize) -> u32 {
    let mut q = s.wq.lock().unwrap();
    q.q.push_back(me);
    loop {
        if let Some(r) = s.results[me].lock().unwrap().take() {
            return r;
        }
        let leader = !q.active && q.q.front() == Some(&me);
        if leader {
            q.active = true;
            let group: Vec<usize> = q.q.drain(..).collect();
            drop(q);
            let n = s.leaders_now.fetch_add(1, Ordering::SeqCst) + 1;
            s.max_leaders.fetch_max(n, Ordering::SeqCst);
            s.applied.lock().unwrap().extend(group.iter().copied()); // the "WAL append"
            for g in &group {
                *s.results[*g].lock().unwrap() = Some(100 + *g as u32);
            }
            s.leaders_now.fetch_sub(1, Ordering::SeqCst);
            let mut q2 = s.wq.lock().unwrap();
            q2.active = false;
            drop(q2);
            s.cv.notify_all();
            return s.results[me].lock().unwrap().take().unwrap();
        }
        q = s.cv.wait(q).unwrap();
    }
}

#[test]
fn group_commit_protocol_is_safe_and_live() {
    loom::model(|| {
        let n = 3;
        let s = Arc::new(Shared {
            wq: Mutex::new(Wq { q: VecDeque::new(), active: false }),
            cv: Condvar::new(),
            results: (0..n).map(|_| Mutex::new(None)).collect(),
            leaders_now: AtomicUsize::new(0),
            max_leaders: AtomicUsize::new(0),
            applied: Mutex::new(vec![]),
        });
        let hs: Vec<_> = (0..n)
            .map(|i| {
                let s = s.clone();
                thread::spawn(move || {
                    let r = write(&s, i);
                    assert_eq!(r, 100 + i as u32);
                })
            })
            .collect();
        for h in hs {
            h.join().unwrap();
        }
        assert_eq!(s.max_leaders.load(Ordering::SeqCst), 1, "two leaders ran concurrently");
        let mut applied = s.applied.lock().unwrap().clone();
        applied.sort();
        assert_eq!(applied, vec![0, 1, 2], "every writer applied exactly once");
    });
}
