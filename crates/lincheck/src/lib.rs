//! Linearizability checker: Wing & Gong search with Lowe's memoization
//! (cache of (set of linearized ops, model state)), partitioned by key.
//!
//! Model: a map of independent registers with Write / Read / CAS. Operations
//! whose outcome is unknown (client timed out) are *pending*: they never
//! return, and may or may not have taken effect, at any time after invocation.
use std::collections::{HashMap, HashSet};

pub type Value = Vec<u8>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OpKind {
    Write(Value),
    Read,
    /// compare-and-swap: expected (None = key absent), new value
    Cas(Option<Value>, Value),
    Delete,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OpResult {
    WriteOk,
    ReadOk(Option<Value>),
    CasOk(bool),
}

#[derive(Clone, Debug)]
pub struct Op {
    pub id: u64,
    pub client: u64,
    pub key: Vec<u8>,
    pub kind: OpKind,
    pub invoke: u64,
    /// None = never returned (pending / unknown outcome)
    pub ret: Option<u64>,
    pub result: Option<OpResult>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    Linearizable,
    /// Not linearizable: the key whose sub-history cannot be linearized.
    Violation { key: Vec<u8> },
    /// Search budget exhausted for this key: neither proven nor refuted.
    Inconclusive { key: Vec<u8> },
}

#[derive(Debug, Clone)]
pub struct CheckStats {
    pub keys: usize,
    pub ops: usize,
    pub states_explored: u64,
}

type State = Option<Value>;

/// Apply op to state. Some(new_state) if consistent with the recorded result.
fn step(state: &State, op: &Op) -> Vec<State> {
    match (&op.kind, &op.result) {
        (OpKind::Write(v), Some(OpResult::WriteOk)) => vec![Some(v.clone())],
        (OpKind::Delete, Some(OpResult::WriteOk)) => vec![None],
        (OpKind::Read, Some(OpResult::ReadOk(r))) => {
            if r == state {
                vec![state.clone()]
            } else {
                vec![]
            }
        }
        (OpKind::Cas(exp, new), Some(OpResult::CasOk(true))) => {
            if exp == state {
                vec![Some(new.clone())]
            } else {
                vec![]
            }
        }
        (OpKind::Cas(exp, _), Some(OpResult::CasOk(false))) => {
            if exp != state {
                vec![state.clone()]
            } else {
                vec![]
            }
        }
        // pending: may have taken effect (or not: handled by the search skipping it)
        (OpKind::Write(v), None) => vec![Some(v.clone())],
        (OpKind::Delete, None) => vec![None],
        (OpKind::Cas(exp, new), None) => {
            if exp == state {
                vec![Some(new.clone())]
            } else {
                vec![]
            }
        }
        (OpKind::Read, None) => vec![state.clone()],
        _ => vec![], // mismatched kind/result: invalid history
    }
}

struct Search<'a> {
    ops: &'a [Op],
    order: Vec<usize>, // indices sorted by invoke time
    budget: u64,
    explored: u64,
    memo: HashSet<(Vec<u64>, u32)>,
    intern: HashMap<State, u32>,
    states: Vec<State>,
}

impl<'a> Search<'a> {
    fn state_id(&mut self, s: &State) -> u32 {
        if let Some(i) = self.intern.get(s) {
            return *i;
        }
        let id = self.states.len() as u32;
        self.states.push(s.clone());
        self.intern.insert(s.clone(), id);
        id
    }

    /// returns Some(true)=linearizable, Some(false)=no, None=budget exhausted
    fn dfs(&mut self, done: &mut Vec<u64>, remaining_completed: usize, state: State) -> Option<bool> {
        if remaining_completed == 0 {
            return Some(true);
        }
        self.explored += 1;
        if self.explored > self.budget {
            return None;
        }
        let sid = self.state_id(&state);
        if !self.memo.insert((done.clone(), sid)) {
            return Some(false); // already explored this configuration and it failed
        }
        // earliest return time among unlinearized completed ops bounds who may go next
        let mut min_ret = u64::MAX;
        for &i in &self.order {
            if done[i / 64] >> (i % 64) & 1 == 0 {
                if let Some(r) = self.ops[i].ret {
                    min_ret = min_ret.min(r);
                }
            }
        }
        let order = self.order.clone();
        for i in order {
            if done[i / 64] >> (i % 64) & 1 == 1 {
                continue;
            }
            let op = &self.ops[i];
            if op.invoke > min_ret {
                break; // sorted by invoke: nobody later can be minimal
            }
            for ns in step(&state, op) {
                done[i / 64] |= 1 << (i % 64);
                let rem = remaining_completed - if op.ret.is_some() { 1 } else { 0 };
                match self.dfs(done, rem, ns) {
                    Some(true) => return Some(true),
                    None => {
                        done[i / 64] &= !(1 << (i % 64));
                        return None;
                    }
                    Some(false) => {}
                }
                done[i / 64] &= !(1 << (i % 64));
            }
        }
        Some(false)
    }
}

fn check_key(ops: &[Op], budget: u64, explored: &mut u64) -> Option<bool> {
    // Reads with unknown outcome have no effect: drop them.
    let ops: Vec<Op> = ops.iter().filter(|o| !(o.kind == OpKind::Read && o.ret.is_none())).cloned().collect();
    let mut order: Vec<usize> = (0..ops.len()).collect();
    order.sort_by_key(|&i| (ops[i].invoke, ops[i].id));
    let completed = ops.iter().filter(|o| o.ret.is_some()).count();
    let mut s = Search { ops: &ops, order, budget, explored: 0, memo: HashSet::new(), intern: HashMap::new(), states: vec![] };
    let mut done = vec![0u64; ops.len().div_ceil(64).max(1)];
    let r = s.dfs(&mut done, completed, None);
    *explored += s.explored;
    r
}

/// Check a whole history. `budget` bounds search nodes per key.
pub fn check(history: &[Op], budget: u64) -> (Verdict, CheckStats) {
    let mut by_key: HashMap<&[u8], Vec<Op>> = HashMap::new();
    for o in history {
        by_key.entry(&o.key).or_default().push(o.clone());
    }
    let mut stats = CheckStats { keys: by_key.len(), ops: history.len(), states_explored: 0 };
    let mut keys: Vec<_> = by_key.keys().copied().collect();
    keys.sort();
    let mut inconclusive = None;
    for k in keys {
        match check_key(&by_key[k], budget, &mut stats.states_explored) {
            Some(true) => {}
            Some(false) => return (Verdict::Violation { key: k.to_vec() }, stats),
            None => inconclusive = Some(k.to_vec()),
        }
    }
    match inconclusive {
        Some(key) => (Verdict::Inconclusive { key }, stats),
        None => (Verdict::Linearizable, stats),
    }
}

/// Thread-safe-ish helper for building histories.
#[derive(Default)]
pub struct Recorder {
    pub ops: Vec<Op>,
    next_id: u64,
}

impl Recorder {
    pub fn invoke(&mut self, client: u64, key: &[u8], kind: OpKind, t: u64) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        self.ops.push(Op { id, client, key: key.to_vec(), kind, invoke: t, ret: None, result: None });
        id
    }
    pub fn complete(&mut self, id: u64, t: u64, result: OpResult) {
        let op = &mut self.ops[id as usize];
        if op.ret.is_none() {
            op.ret = Some(t);
            op.result = Some(result);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{Rng, SeedableRng};
    use rand_chacha::ChaCha8Rng;

    fn v(x: u8) -> Value {
        vec![x]
    }
    fn op(id: u64, kind: OpKind, inv: u64, ret: Option<u64>, res: Option<OpResult>) -> Op {
        Op { id, client: id, key: b"k".to_vec(), kind, invoke: inv, ret, result: res }
    }

    #[test]
    fn sequential_ok_and_stale_read_rejected() {
        let h = vec![
            op(0, OpKind::Write(v(1)), 0, Some(1), Some(OpResult::WriteOk)),
            op(1, OpKind::Read, 2, Some(3), Some(OpResult::ReadOk(Some(v(1))))),
        ];
        assert_eq!(check(&h, 1_000_000).0, Verdict::Linearizable);
        let h = vec![
            op(0, OpKind::Write(v(1)), 0, Some(1), Some(OpResult::WriteOk)),
            op(1, OpKind::Write(v(2)), 2, Some(3), Some(OpResult::WriteOk)),
            op(2, OpKind::Read, 4, Some(5), Some(OpResult::ReadOk(Some(v(1))))),
        ];
        assert!(matches!(check(&h, 1_000_000).0, Verdict::Violation { .. }));
    }

    #[test]
    fn concurrent_ops_allow_either_order() {
        let h = vec![
            op(0, OpKind::Write(v(1)), 0, Some(10), Some(OpResult::WriteOk)),
            op(1, OpKind::Read, 5, Some(6), Some(OpResult::ReadOk(None))),
            op(2, OpKind::Read, 5, Some(7), Some(OpResult::ReadOk(Some(v(1))))),
        ];
        assert_eq!(check(&h, 1_000_000).0, Verdict::Linearizable);
    }

    #[test]
    fn pending_write_may_or_may_not_apply() {
        // write never returned; a later read sees it: fine. A later read not seeing it: fine too.
        for seen in [Some(v(9)), None] {
            let h = vec![
                op(0, OpKind::Write(v(9)), 0, None, None),
                op(1, OpKind::Read, 5, Some(6), Some(OpResult::ReadOk(seen))),
            ];
            assert_eq!(check(&h, 1_000_000).0, Verdict::Linearizable);
        }
        // but it cannot appear, vanish, and reappear
        let h = vec![
            op(0, OpKind::Write(v(9)), 0, None, None),
            op(1, OpKind::Read, 5, Some(6), Some(OpResult::ReadOk(Some(v(9))))),
            op(2, OpKind::Read, 7, Some(8), Some(OpResult::ReadOk(None))),
        ];
        assert!(matches!(check(&h, 1_000_000).0, Verdict::Violation { .. }));
    }

    #[test]
    fn cas_semantics() {
        let h = vec![
            op(0, OpKind::Write(v(1)), 0, Some(1), Some(OpResult::WriteOk)),
            op(1, OpKind::Cas(Some(v(1)), v(2)), 2, Some(3), Some(OpResult::CasOk(true))),
            op(2, OpKind::Cas(Some(v(1)), v(3)), 4, Some(5), Some(OpResult::CasOk(false))),
            op(3, OpKind::Read, 6, Some(7), Some(OpResult::ReadOk(Some(v(2))))),
        ];
        assert_eq!(check(&h, 1_000_000).0, Verdict::Linearizable);
        let mut bad = h.clone();
        bad[2].result = Some(OpResult::CasOk(true)); // two successful CAS from the same expected value
        assert!(matches!(check(&bad, 1_000_000).0, Verdict::Violation { .. }));
    }

    /// Generate a history from a real sequential register with a hidden
    /// linearization order, then widen the intervals so they overlap.
    fn gen_linearizable(seed: u64, n: usize) -> Vec<Op> {
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        let mut state: Option<Value> = None;
        let mut ops = vec![];
        for i in 0..n {
            let t = (i as u64) * 10 + 5; // linearization point
            let (kind, res) = match rng.gen_range(0..4) {
                0 | 1 => {
                    let x = v(rng.gen_range(0..5));
                    state = Some(x.clone());
                    (OpKind::Write(x), OpResult::WriteOk)
                }
                2 => (OpKind::Read, OpResult::ReadOk(state.clone())),
                _ => {
                    let exp = if rng.gen_bool(0.5) { state.clone() } else { Some(v(rng.gen_range(0..5))) };
                    let new = v(rng.gen_range(0..5));
                    let ok = exp == state;
                    if ok {
                        state = Some(new.clone());
                    }
                    (OpKind::Cas(exp, new), OpResult::CasOk(ok))
                }
            };
            let inv = t.saturating_sub(rng.gen_range(0..40));
            let ret = t + rng.gen_range(1..40);
            ops.push(op(i as u64, kind, inv, Some(ret), Some(res)));
        }
        ops
    }

    #[test]
    fn accepts_random_linearizable_histories() {
        for seed in 0..200 {
            let h = gen_linearizable(seed, 40);
            let (v, _) = check(&h, 5_000_000);
            assert_eq!(v, Verdict::Linearizable, "seed {seed}");
        }
    }

    #[test]
    fn mutation_testing_rejects_injected_bugs() {
        // Mutations that always break linearizability: a read returning a value no one wrote.
        let mut caught = 0;
        let mut total = 0;
        for seed in 0..200 {
            let mut h = gen_linearizable(seed, 40);
            let Some(ri) = h.iter().position(|o| o.kind == OpKind::Read) else { continue };
            h[ri].result = Some(OpResult::ReadOk(Some(v(200)))); // never written
            total += 1;
            if matches!(check(&h, 5_000_000).0, Verdict::Violation { .. }) {
                caught += 1;
            }
        }
        assert_eq!(caught, total, "checker missed a phantom read");
    }

    #[test]
    fn mutation_stale_read_after_acked_write() {
        // Real-time order violation: write acked, later (non-overlapping) read returns older value.
        let mut caught = 0;
        let mut total = 0;
        for seed in 0..200 {
            let mut rng = ChaCha8Rng::seed_from_u64(seed);
            let mut h = vec![];
            let mut t = 0;
            let mut id = 0;
            let mut last = None;
            let mut prev = None;
            for _ in 0..10 {
                let x = v(rng.gen_range(0..250));
                h.push(op(id, OpKind::Write(x.clone()), t, Some(t + 2), Some(OpResult::WriteOk)));
                id += 1;
                t += 3;
                prev = last.clone();
                last = Some(x);
            }
            if prev == last {
                continue;
            }
            // read after all writes returns the second-to-last value
            h.push(op(id, OpKind::Read, t, Some(t + 1), Some(OpResult::ReadOk(prev))));
            total += 1;
            if matches!(check(&h, 5_000_000).0, Verdict::Violation { .. }) {
                caught += 1;
            }
        }
        assert!(total > 100);
        assert_eq!(caught, total);
    }

    #[test]
    fn mutation_lost_acked_write_and_double_apply() {
        // lost write: acked write, then read sees None
        let h = vec![
            op(0, OpKind::Write(v(1)), 0, Some(1), Some(OpResult::WriteOk)),
            op(1, OpKind::Read, 2, Some(3), Some(OpResult::ReadOk(None))),
        ];
        assert!(matches!(check(&h, 1000).0, Verdict::Violation { .. }));
        // double apply: a CAS-increment style op observed applying twice
        let h = vec![
            op(0, OpKind::Write(v(0)), 0, Some(1), Some(OpResult::WriteOk)),
            op(1, OpKind::Cas(Some(v(0)), v(1)), 2, Some(3), Some(OpResult::CasOk(true))),
            op(2, OpKind::Cas(Some(v(0)), v(1)), 4, Some(5), Some(OpResult::CasOk(true))),
        ];
        assert!(matches!(check(&h, 1000).0, Verdict::Violation { .. }));
    }

    #[test]
    fn budget_exhaustion_reports_inconclusive() {
        let h = gen_linearizable(7, 40);
        let (v, _) = check(&h, 2);
        assert!(matches!(v, Verdict::Inconclusive { .. } | Verdict::Linearizable));
    }
}
