//! Percolator-style transaction primitives, executed deterministically inside the
//! replicated state machine (so conflict checks are atomic with the writes).
//!
//! Commit point = writing the PRIMARY key's write record. Secondaries are committed
//! afterwards (asynchronously); any reader that finds a secondary lock resolves it by
//! consulting the primary's fate (`check_txn_status`).
use crate::keys::*;
use raft::codec::{R, W};

pub trait TxnStore {
    fn get(&self, engine_key: &[u8]) -> Option<Vec<u8>>;
    /// Entries with start <= key < end in ascending order, at most `limit`.
    fn scan(&self, start: &[u8], end: &[u8], limit: usize) -> Vec<(Vec<u8>, Vec<u8>)>;
}

/// Collected writes of one command; applied atomically by the state machine.
#[derive(Default, Debug, Clone)]
pub struct Batch {
    pub ops: Vec<(Vec<u8>, Option<Vec<u8>>)>,
}
impl Batch {
    pub fn put(&mut self, k: Vec<u8>, v: Vec<u8>) {
        self.ops.push((k, Some(v)));
    }
    pub fn del(&mut self, k: Vec<u8>) {
        self.ops.push((k, None));
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MutOp {
    Put = 1,
    Delete = 2,
    /// lock only (used for read-validation / pessimistic read locks)
    Lock = 3,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Lock {
    pub primary: Vec<u8>,
    pub start_ts: u64,
    pub ttl_ms: u64,
    pub op: u8,
}

impl Lock {
    pub fn encode(&self) -> Vec<u8> {
        let mut w = W::default();
        w.bytes(&self.primary);
        w.u64(self.start_ts);
        w.u64(self.ttl_ms);
        w.u8(self.op);
        w.0
    }
    pub fn decode(b: &[u8]) -> Option<Lock> {
        let mut r = R(b);
        Some(Lock { primary: r.bytes()?, start_ts: r.u64()?, ttl_ms: r.u64()?, op: r.u8()? })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteKind {
    Put = 1,
    Delete = 2,
    Rollback = 3,
    Lock = 4,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WriteRec {
    pub kind: WriteKind,
    pub start_ts: u64,
}

impl WriteRec {
    pub fn encode(&self) -> Vec<u8> {
        let mut v = vec![self.kind as u8];
        v.extend_from_slice(&self.start_ts.to_le_bytes());
        v
    }
    pub fn decode(b: &[u8]) -> Option<WriteRec> {
        let kind = match b.first()? {
            1 => WriteKind::Put,
            2 => WriteKind::Delete,
            3 => WriteKind::Rollback,
            4 => WriteKind::Lock,
            _ => return None,
        };
        Some(WriteRec { kind, start_ts: u64::from_le_bytes(b.get(1..9)?.try_into().ok()?) })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KeyError {
    Locked {
        key: Vec<u8>,
        lock: Lock,
    },
    /// someone committed `key` at `conflict_ts` >= our start_ts (or we were rolled back)
    WriteConflict {
        key: Vec<u8>,
        conflict_ts: u64,
    },
    /// commit found neither our lock nor a commit record: the txn was rolled back
    TxnNotFound {
        key: Vec<u8>,
    },
    /// rollback requested but the transaction already committed
    AlreadyCommitted {
        key: Vec<u8>,
        commit_ts: u64,
    },
}

pub fn get_lock<S: TxnStore>(s: &S, key: &[u8]) -> Option<Lock> {
    s.get(&lock_key(key)).and_then(|v| Lock::decode(&v))
}

/// Write records of `key`, newest commit first.
pub fn write_recs<S: TxnStore>(s: &S, key: &[u8], limit: usize) -> Vec<(u64, WriteRec)> {
    let (lo, _) = (cf_key(CF_WRITE, key), ());
    let hi = cf_key_end(CF_WRITE, key);
    s.scan(&lo, &hi, limit)
        .into_iter()
        .filter_map(|(k, v)| {
            let (uk, cts) = key_ts(&k)?;
            if uk != key {
                return None;
            }
            Some((cts, WriteRec::decode(&v)?))
        })
        .collect()
}

/// Find the record written by transaction `start_ts` (commit or rollback).
fn find_txn_rec<S: TxnStore>(s: &S, key: &[u8], start_ts: u64) -> Option<(u64, WriteRec)> {
    // records with commit_ts >= start_ts, newest first; stop once commit_ts < start_ts
    for (cts, rec) in write_recs(s, key, usize::MAX) {
        if cts < start_ts {
            break;
        }
        if rec.start_ts == start_ts {
            return Some((cts, rec));
        }
    }
    None
}

/// Snapshot read at `ts`. Err(lock) if a lock that could affect the result exists.
pub fn read<S: TxnStore>(s: &S, key: &[u8], ts: u64) -> Result<Option<Vec<u8>>, Lock> {
    if let Some(l) = get_lock(s, key) {
        if l.start_ts <= ts && l.op != MutOp::Lock as u8 {
            return Err(l);
        }
    }
    for (cts, rec) in write_recs(s, key, usize::MAX) {
        if cts > ts {
            continue;
        }
        match rec.kind {
            WriteKind::Put => return Ok(s.get(&versioned_key(CF_DATA, key, rec.start_ts))),
            WriteKind::Delete => return Ok(None),
            WriteKind::Rollback | WriteKind::Lock => continue,
        }
    }
    Ok(None)
}

pub fn prewrite<S: TxnStore>(s: &S, b: &mut Batch, key: &[u8], op: MutOp, value: &[u8], primary: &[u8], start_ts: u64, ttl_ms: u64) -> Result<(), KeyError> {
    if let Some(l) = get_lock(s, key) {
        if l.start_ts == start_ts {
            return Ok(()); // idempotent retry
        }
        return Err(KeyError::Locked { key: key.to_vec(), lock: l });
    }
    // conflict only with COMMITTED writes at/after our snapshot; another txn's rollback
    // tombstone or a lock-only record is not a conflict
    for (cts, rec) in write_recs(s, key, usize::MAX) {
        if cts < start_ts {
            break;
        }
        if matches!(rec.kind, WriteKind::Put | WriteKind::Delete) {
            return Err(KeyError::WriteConflict { key: key.to_vec(), conflict_ts: cts });
        }
    }
    if let Some((cts, rec)) = find_txn_rec(s, key, start_ts) {
        if rec.kind == WriteKind::Rollback {
            return Err(KeyError::WriteConflict { key: key.to_vec(), conflict_ts: cts });
        }
    }
    b.put(lock_key(key), Lock { primary: primary.to_vec(), start_ts, ttl_ms, op: op as u8 }.encode());
    if op == MutOp::Put {
        b.put(versioned_key(CF_DATA, key, start_ts), value.to_vec());
    }
    Ok(())
}

/// Serializable-mode validation: the key we read must not have been committed to
/// since our snapshot, and must not be locked by another transaction.
pub fn validate_read<S: TxnStore>(s: &S, key: &[u8], start_ts: u64) -> Result<(), KeyError> {
    if let Some(l) = get_lock(s, key) {
        if l.start_ts != start_ts {
            return Err(KeyError::Locked { key: key.to_vec(), lock: l });
        }
    }
    for (cts, rec) in write_recs(s, key, 4) {
        if cts > start_ts && matches!(rec.kind, WriteKind::Put | WriteKind::Delete) {
            return Err(KeyError::WriteConflict { key: key.to_vec(), conflict_ts: cts });
        }
        if cts <= start_ts {
            break;
        }
    }
    Ok(())
}

pub fn commit<S: TxnStore>(s: &S, b: &mut Batch, key: &[u8], start_ts: u64, commit_ts: u64) -> Result<(), KeyError> {
    match get_lock(s, key) {
        Some(l) if l.start_ts == start_ts => {
            let kind = match l.op {
                1 => WriteKind::Put,
                2 => WriteKind::Delete,
                _ => WriteKind::Lock,
            };
            b.put(versioned_key(CF_WRITE, key, commit_ts), WriteRec { kind, start_ts }.encode());
            b.del(lock_key(key));
            Ok(())
        }
        _ => match find_txn_rec(s, key, start_ts) {
            Some((_, rec)) if rec.kind != WriteKind::Rollback => Ok(()), // already committed: idempotent
            _ => Err(KeyError::TxnNotFound { key: key.to_vec() }),
        },
    }
}

pub fn rollback<S: TxnStore>(s: &S, b: &mut Batch, key: &[u8], start_ts: u64) -> Result<(), KeyError> {
    if let Some(l) = get_lock(s, key) {
        if l.start_ts == start_ts {
            b.del(lock_key(key));
            if l.op == MutOp::Put as u8 {
                b.del(versioned_key(CF_DATA, key, start_ts));
            }
            // tombstone so a delayed prewrite of this txn cannot succeed later
            b.put(versioned_key(CF_WRITE, key, start_ts), WriteRec { kind: WriteKind::Rollback, start_ts }.encode());
            return Ok(());
        }
    }
    match find_txn_rec(s, key, start_ts) {
        Some((cts, rec)) if rec.kind != WriteKind::Rollback => Err(KeyError::AlreadyCommitted { key: key.to_vec(), commit_ts: cts }),
        Some(_) => Ok(()),
        None => {
            b.put(versioned_key(CF_WRITE, key, start_ts), WriteRec { kind: WriteKind::Rollback, start_ts }.encode());
            Ok(())
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TxnStatus {
    /// primary lock still alive; remaining ttl
    Locked {
        ttl_ms: u64,
    },
    Committed {
        commit_ts: u64,
    },
    RolledBack,
    NotExist,
}

/// TSO timestamps carry physical milliseconds in the high bits.
pub const LOGICAL_BITS: u32 = 18;
pub fn physical_ms(ts: u64) -> u64 {
    ts >> LOGICAL_BITS
}

pub fn check_txn_status<S: TxnStore>(s: &S, b: &mut Batch, primary: &[u8], start_ts: u64, now_ms: u64, rollback_if_not_exist: bool) -> TxnStatus {
    if let Some(l) = get_lock(s, primary) {
        if l.start_ts == start_ts {
            let expires = physical_ms(start_ts) + l.ttl_ms;
            if now_ms >= expires {
                let _ = rollback(s, b, primary, start_ts);
                return TxnStatus::RolledBack;
            }
            return TxnStatus::Locked { ttl_ms: expires - now_ms };
        }
    }
    match find_txn_rec(s, primary, start_ts) {
        Some((cts, rec)) if rec.kind != WriteKind::Rollback => TxnStatus::Committed { commit_ts: cts },
        Some(_) => TxnStatus::RolledBack,
        None if rollback_if_not_exist => {
            b.put(versioned_key(CF_WRITE, primary, start_ts), WriteRec { kind: WriteKind::Rollback, start_ts }.encode());
            TxnStatus::RolledBack
        }
        None => TxnStatus::NotExist,
    }
}

/// Delete versions no longer visible to any reader at or above `safe_point`.
pub fn gc_key<S: TxnStore>(s: &S, b: &mut Batch, key: &[u8], safe_point: u64) {
    let recs = write_recs(s, key, usize::MAX);
    let mut kept_visible = false;
    for (cts, rec) in recs {
        if cts > safe_point {
            continue;
        }
        match rec.kind {
            // the newest committed value at the safe point must stay; rollback/lock markers above it are only markers
            WriteKind::Put if !kept_visible => {
                kept_visible = true;
                continue;
            }
            // a delete as newest version hides everything older, so the tombstone and the older versions can all go
            WriteKind::Delete => kept_visible = true,
            WriteKind::Put | WriteKind::Rollback | WriteKind::Lock => {}
        }
        b.del(versioned_key(CF_WRITE, key, cts));
        if rec.kind == WriteKind::Put {
            b.del(versioned_key(CF_DATA, key, rec.start_ts));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::collections::BTreeMap;

    #[derive(Default)]
    struct Mem(RefCell<BTreeMap<Vec<u8>, Vec<u8>>>);
    impl TxnStore for Mem {
        fn get(&self, k: &[u8]) -> Option<Vec<u8>> {
            self.0.borrow().get(k).cloned()
        }
        fn scan(&self, a: &[u8], b: &[u8], limit: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
            self.0.borrow().range(a.to_vec()..b.to_vec()).take(limit).map(|(k, v)| (k.clone(), v.clone())).collect()
        }
    }
    impl Mem {
        fn apply(&self, b: Batch) {
            let mut m = self.0.borrow_mut();
            for (k, v) in b.ops {
                match v {
                    Some(v) => m.insert(k, v),
                    None => m.remove(&k),
                };
            }
        }
    }
    fn do_txn(s: &Mem, keys: &[(&[u8], &[u8])], start: u64, commit_ts: u64) -> Result<(), KeyError> {
        let primary = keys[0].0;
        for (k, v) in keys {
            let mut b = Batch::default();
            prewrite(s, &mut b, k, MutOp::Put, v, primary, start, 3000)?;
            s.apply(b);
        }
        for (k, _) in keys {
            let mut b = Batch::default();
            commit(s, &mut b, k, start, commit_ts)?;
            s.apply(b);
        }
        Ok(())
    }

    #[test]
    fn snapshot_reads_see_committed_versions() {
        let s = Mem::default();
        do_txn(&s, &[(b"a", b"1")], 10, 11).unwrap();
        do_txn(&s, &[(b"a", b"2")], 20, 21).unwrap();
        assert_eq!(read(&s, b"a", 5).unwrap(), None);
        assert_eq!(read(&s, b"a", 15).unwrap().unwrap(), b"1");
        assert_eq!(read(&s, b"a", 25).unwrap().unwrap(), b"2");
    }

    #[test]
    fn write_write_conflict_first_committer_wins() {
        let s = Mem::default();
        // T1 and T2 start at the same snapshot; T1 commits first
        do_txn(&s, &[(b"x", b"t1")], 10, 15).unwrap();
        let mut b = Batch::default();
        let r = prewrite(&s, &mut b, b"x", MutOp::Put, b"t2", b"x", 12, 3000);
        assert!(matches!(r, Err(KeyError::WriteConflict { conflict_ts: 15, .. })));
    }

    #[test]
    fn reader_blocked_by_lock_then_resolved_through_primary() {
        let s = Mem::default();
        // txn prewrites a (primary) and b, commits only primary then "crashes"
        for (k, v) in [(b"a", b"A"), (b"b", b"B")] {
            let mut bt = Batch::default();
            prewrite(&s, &mut bt, k, MutOp::Put, v, b"a", 10, 3000).unwrap();
            s.apply(bt);
        }
        let mut bt = Batch::default();
        commit(&s, &mut bt, b"a", 10, 11).unwrap();
        s.apply(bt);
        // reader at ts 20 sees b locked
        let lock = read(&s, b"b", 20).unwrap_err();
        assert_eq!(lock.primary, b"a");
        // resolve: ask primary
        let mut bt = Batch::default();
        let st = check_txn_status(&s, &mut bt, b"a", 10, 0, false);
        assert_eq!(st, TxnStatus::Committed { commit_ts: 11 });
        let mut bt = Batch::default();
        commit(&s, &mut bt, b"b", 10, 11).unwrap();
        s.apply(bt);
        assert_eq!(read(&s, b"b", 20).unwrap().unwrap(), b"B");
    }

    #[test]
    fn expired_primary_lock_is_rolled_back_and_blocks_late_prewrite() {
        let s = Mem::default();
        let start = 1000u64 << LOGICAL_BITS;
        let mut bt = Batch::default();
        prewrite(&s, &mut bt, b"p", MutOp::Put, b"v", b"p", start, 100).unwrap();
        s.apply(bt);
        let mut bt = Batch::default();
        assert_eq!(check_txn_status(&s, &mut bt, b"p", start, 1050, false), TxnStatus::Locked { ttl_ms: 50 });
        let mut bt = Batch::default();
        assert_eq!(check_txn_status(&s, &mut bt, b"p", start, 1200, false), TxnStatus::RolledBack);
        s.apply(bt);
        assert!(read(&s, b"p", start + 5).unwrap().is_none());
        // delayed duplicate prewrite of the dead txn must fail
        let mut bt = Batch::default();
        assert!(prewrite(&s, &mut bt, b"p", MutOp::Put, b"v", b"p", start, 100).is_err());
        // commit after rollback must fail
        let mut bt = Batch::default();
        assert!(matches!(commit(&s, &mut bt, b"p", start, start + 9), Err(KeyError::TxnNotFound { .. })));
    }

    #[test]
    fn other_txns_rollback_tombstone_is_not_a_conflict() {
        let s = Mem::default();
        let mut b = Batch::default();
        rollback(&s, &mut b, b"k", 50).unwrap(); // some other txn (start 50) rolled back here
        s.apply(b);
        let mut b = Batch::default();
        assert!(prewrite(&s, &mut b, b"k", MutOp::Put, b"v", b"k", 40, 1000).is_ok());
    }

    #[test]
    fn commit_and_rollback_are_idempotent() {
        let s = Mem::default();
        do_txn(&s, &[(b"k", b"v")], 10, 12).unwrap();
        let mut bt = Batch::default();
        assert!(commit(&s, &mut bt, b"k", 10, 12).is_ok());
        let mut bt = Batch::default();
        assert!(matches!(rollback(&s, &mut bt, b"k", 10), Err(KeyError::AlreadyCommitted { commit_ts: 12, .. })));
    }

    #[test]
    fn serializable_validation_detects_write_skew() {
        // classic write skew: both read {x,y}, T1 writes x, T2 writes y.
        let s = Mem::default();
        do_txn(&s, &[(b"x", b"1")], 1, 2).unwrap();
        do_txn(&s, &[(b"y", b"1")], 3, 4).unwrap();
        // T1 (start 10) and T2 (start 10) both read x and y at 10. T1 commits x at 20.
        do_txn(&s, &[(b"x", b"0")], 10, 20).unwrap();
        // T2 validates its reads at prewrite: x changed after its snapshot
        assert!(matches!(validate_read(&s, b"x", 11), Err(KeyError::WriteConflict { .. })));
        assert!(validate_read(&s, b"y", 11).is_ok());
    }

    #[test]
    fn gc_keeps_visible_version() {
        let s = Mem::default();
        for (st, cm, v) in [(10u64, 11u64, b"1"), (20, 21, b"2"), (30, 31, b"3")] {
            do_txn(&s, &[(b"g", v)], st, cm).unwrap();
        }
        let mut b = Batch::default();
        gc_key(&s, &mut b, b"g", 25);
        s.apply(b);
        assert_eq!(read(&s, b"g", 25).unwrap().unwrap(), b"2");
        assert_eq!(read(&s, b"g", 40).unwrap().unwrap(), b"3");
        assert_eq!(write_recs(&s, b"g", 10).len(), 2);
    }

    /// BUG-021: a rollback record newer than the safe point's visible version used to count as "the visible version",
    /// so GC deleted the real committed value below it.
    #[test]
    fn gc_keeps_value_below_rollback_marker() {
        let s = Mem::default();
        do_txn(&s, &[(b"r", b"1000")], 10, 11).unwrap();
        let mut b = Batch::default();
        rollback(&s, &mut b, b"r", 20).unwrap();
        s.apply(b);
        let mut b = Batch::default();
        gc_key(&s, &mut b, b"r", 30);
        s.apply(b);
        assert_eq!(read(&s, b"r", 40).unwrap().unwrap(), b"1000");
    }
}
