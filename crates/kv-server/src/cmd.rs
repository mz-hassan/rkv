//! Commands replicated through Raft and their results. Everything the state
//! machine needs is inside the command (no wall-clock reads at apply time), so
//! all replicas compute identical results.
use raft::codec::{R, W};
use txn::percolator::{KeyError, Lock, MutOp};

#[derive(Clone, Debug, PartialEq)]
pub struct Mut {
    pub op: MutOp,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Cmd {
    RawPut { key: Vec<u8>, value: Vec<u8>, expire_ms: u64, client: u64, seq: u64 },
    RawDelete { key: Vec<u8>, client: u64, seq: u64 },
    RawCas { key: Vec<u8>, expect: Option<Vec<u8>>, new: Vec<u8>, client: u64, seq: u64 },
    Prewrite { muts: Vec<Mut>, primary: Vec<u8>, start_ts: u64, ttl_ms: u64, read_keys: Vec<Vec<u8>> },
    Commit { keys: Vec<Vec<u8>>, start_ts: u64, commit_ts: u64 },
    Rollback { keys: Vec<Vec<u8>>, start_ts: u64 },
    CheckTxnStatus { primary: Vec<u8>, start_ts: u64, now_ms: u64, rollback_if_not_exist: bool },
    ResolveLock { keys: Vec<Vec<u8>>, start_ts: u64, commit_ts: u64 },
    Split { split_key: Vec<u8>, new_range_id: u64 },
    SweepExpired { now_ms: u64, limit: u32 },
    Gc { safe_point: u64, limit: u32 },
}

#[derive(Clone, Debug, PartialEq)]
pub enum Resp {
    Ok,
    Cas(bool),
    KeyErrors(Vec<KeyError>),
    TxnStatus { action: u32, commit_ts: u64, ttl_ms: u64 },
    /// key not (or no longer) in this range: the client must refresh its route
    RangeMoved,
    Failed(String),
}

fn put_keys(w: &mut W, ks: &[Vec<u8>]) {
    w.u64(ks.len() as u64);
    for k in ks {
        w.bytes(k);
    }
}
fn get_keys(r: &mut R) -> Option<Vec<Vec<u8>>> {
    let n = r.u64()? as usize;
    if n > 1 << 24 {
        return None;
    }
    let mut v = Vec::with_capacity(n.min(4096));
    for _ in 0..n {
        v.push(r.bytes()?);
    }
    Some(v)
}

impl Cmd {
    pub fn encode(&self) -> Vec<u8> {
        let mut w = W::default();
        match self {
            Cmd::RawPut { key, value, expire_ms, client, seq } => {
                w.u8(1);
                w.bytes(key);
                w.bytes(value);
                w.u64(*expire_ms);
                w.u64(*client);
                w.u64(*seq);
            }
            Cmd::RawDelete { key, client, seq } => {
                w.u8(2);
                w.bytes(key);
                w.u64(*client);
                w.u64(*seq);
            }
            Cmd::RawCas { key, expect, new, client, seq } => {
                w.u8(3);
                w.bytes(key);
                w.bool(expect.is_some());
                w.bytes(expect.as_deref().unwrap_or(&[]));
                w.bytes(new);
                w.u64(*client);
                w.u64(*seq);
            }
            Cmd::Prewrite { muts, primary, start_ts, ttl_ms, read_keys } => {
                w.u8(4);
                w.u64(muts.len() as u64);
                for m in muts {
                    w.u8(m.op as u8);
                    w.bytes(&m.key);
                    w.bytes(&m.value);
                }
                w.bytes(primary);
                w.u64(*start_ts);
                w.u64(*ttl_ms);
                put_keys(&mut w, read_keys);
            }
            Cmd::Commit { keys, start_ts, commit_ts } => {
                w.u8(5);
                put_keys(&mut w, keys);
                w.u64(*start_ts);
                w.u64(*commit_ts);
            }
            Cmd::Rollback { keys, start_ts } => {
                w.u8(6);
                put_keys(&mut w, keys);
                w.u64(*start_ts);
            }
            Cmd::CheckTxnStatus { primary, start_ts, now_ms, rollback_if_not_exist } => {
                w.u8(7);
                w.bytes(primary);
                w.u64(*start_ts);
                w.u64(*now_ms);
                w.bool(*rollback_if_not_exist);
            }
            Cmd::ResolveLock { keys, start_ts, commit_ts } => {
                w.u8(8);
                put_keys(&mut w, keys);
                w.u64(*start_ts);
                w.u64(*commit_ts);
            }
            Cmd::Split { split_key, new_range_id } => {
                w.u8(9);
                w.bytes(split_key);
                w.u64(*new_range_id);
            }
            Cmd::SweepExpired { now_ms, limit } => {
                w.u8(10);
                w.u64(*now_ms);
                w.u64(*limit as u64);
            }
            Cmd::Gc { safe_point, limit } => {
                w.u8(11);
                w.u64(*safe_point);
                w.u64(*limit as u64);
            }
        }
        w.0
    }

    pub fn decode(b: &[u8]) -> Option<Cmd> {
        let mut r = R(b);
        Some(match r.u8()? {
            1 => Cmd::RawPut { key: r.bytes()?, value: r.bytes()?, expire_ms: r.u64()?, client: r.u64()?, seq: r.u64()? },
            2 => Cmd::RawDelete { key: r.bytes()?, client: r.u64()?, seq: r.u64()? },
            3 => {
                let key = r.bytes()?;
                let has = r.bool()?;
                let e = r.bytes()?;
                Cmd::RawCas { key, expect: if has { Some(e) } else { None }, new: r.bytes()?, client: r.u64()?, seq: r.u64()? }
            }
            4 => {
                let n = r.u64()? as usize;
                if n > 1 << 24 {
                    return None;
                }
                let mut muts = vec![];
                for _ in 0..n {
                    let op = match r.u8()? {
                        1 => MutOp::Put,
                        2 => MutOp::Delete,
                        3 => MutOp::Lock,
                        _ => return None,
                    };
                    muts.push(Mut { op, key: r.bytes()?, value: r.bytes()? });
                }
                Cmd::Prewrite { muts, primary: r.bytes()?, start_ts: r.u64()?, ttl_ms: r.u64()?, read_keys: get_keys(&mut r)? }
            }
            5 => Cmd::Commit { keys: get_keys(&mut r)?, start_ts: r.u64()?, commit_ts: r.u64()? },
            6 => Cmd::Rollback { keys: get_keys(&mut r)?, start_ts: r.u64()? },
            7 => Cmd::CheckTxnStatus { primary: r.bytes()?, start_ts: r.u64()?, now_ms: r.u64()?, rollback_if_not_exist: r.bool()? },
            8 => Cmd::ResolveLock { keys: get_keys(&mut r)?, start_ts: r.u64()?, commit_ts: r.u64()? },
            9 => Cmd::Split { split_key: r.bytes()?, new_range_id: r.u64()? },
            10 => Cmd::SweepExpired { now_ms: r.u64()?, limit: r.u64()? as u32 },
            11 => Cmd::Gc { safe_point: r.u64()?, limit: r.u64()? as u32 },
            _ => return None,
        })
    }

    /// Keys this command touches (for range-ownership checks), or None for range-level commands.
    pub fn keys(&self) -> Vec<&[u8]> {
        match self {
            Cmd::RawPut { key, .. } | Cmd::RawDelete { key, .. } | Cmd::RawCas { key, .. } => vec![key],
            Cmd::Prewrite { muts, read_keys, .. } => muts.iter().map(|m| m.key.as_slice()).chain(read_keys.iter().map(|k| k.as_slice())).collect(),
            Cmd::Commit { keys, .. } | Cmd::Rollback { keys, .. } | Cmd::ResolveLock { keys, .. } => keys.iter().map(|k| k.as_slice()).collect(),
            Cmd::CheckTxnStatus { primary, .. } => vec![primary],
            _ => vec![],
        }
    }
}

fn put_key_error(w: &mut W, e: &KeyError) {
    match e {
        KeyError::Locked { key, lock } => {
            w.u8(1);
            w.bytes(key);
            w.bytes(&lock.encode());
        }
        KeyError::WriteConflict { key, conflict_ts } => {
            w.u8(2);
            w.bytes(key);
            w.u64(*conflict_ts);
        }
        KeyError::TxnNotFound { key } => {
            w.u8(3);
            w.bytes(key);
        }
        KeyError::AlreadyCommitted { key, commit_ts } => {
            w.u8(4);
            w.bytes(key);
            w.u64(*commit_ts);
        }
    }
}
fn get_key_error(r: &mut R) -> Option<KeyError> {
    Some(match r.u8()? {
        1 => KeyError::Locked { key: r.bytes()?, lock: Lock::decode(&r.bytes()?)? },
        2 => KeyError::WriteConflict { key: r.bytes()?, conflict_ts: r.u64()? },
        3 => KeyError::TxnNotFound { key: r.bytes()? },
        4 => KeyError::AlreadyCommitted { key: r.bytes()?, commit_ts: r.u64()? },
        _ => return None,
    })
}

impl Resp {
    pub fn encode(&self) -> Vec<u8> {
        let mut w = W::default();
        match self {
            Resp::Ok => w.u8(0),
            Resp::Cas(b) => {
                w.u8(1);
                w.bool(*b)
            }
            Resp::KeyErrors(es) => {
                w.u8(2);
                w.u64(es.len() as u64);
                for e in es {
                    put_key_error(&mut w, e);
                }
            }
            Resp::TxnStatus { action, commit_ts, ttl_ms } => {
                w.u8(3);
                w.u64(*action as u64);
                w.u64(*commit_ts);
                w.u64(*ttl_ms);
            }
            Resp::RangeMoved => w.u8(4),
            Resp::Failed(s) => {
                w.u8(5);
                w.bytes(s.as_bytes())
            }
        }
        w.0
    }
    pub fn decode(b: &[u8]) -> Option<Resp> {
        let mut r = R(b);
        Some(match r.u8()? {
            0 => Resp::Ok,
            1 => Resp::Cas(r.bool()?),
            2 => {
                let n = r.u64()? as usize;
                let mut v = vec![];
                for _ in 0..n.min(1 << 20) {
                    v.push(get_key_error(&mut r)?);
                }
                Resp::KeyErrors(v)
            }
            3 => Resp::TxnStatus { action: r.u64()? as u32, commit_ts: r.u64()?, ttl_ms: r.u64()? },
            4 => Resp::RangeMoved,
            5 => Resp::Failed(String::from_utf8_lossy(&r.bytes()?).into_owned()),
            _ => return None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cmd_roundtrip() {
        let cmds = vec![
            Cmd::RawPut { key: b"k".to_vec(), value: b"v".to_vec(), expire_ms: 5, client: 1, seq: 2 },
            Cmd::RawCas { key: b"k".to_vec(), expect: None, new: b"n".to_vec(), client: 1, seq: 3 },
            Cmd::Prewrite { muts: vec![Mut { op: MutOp::Put, key: b"a".to_vec(), value: b"1".to_vec() }], primary: b"a".to_vec(), start_ts: 9, ttl_ms: 3000, read_keys: vec![b"r".to_vec()] },
            Cmd::Commit { keys: vec![b"a".to_vec(), b"b".to_vec()], start_ts: 9, commit_ts: 10 },
            Cmd::Split { split_key: b"m".to_vec(), new_range_id: 77 },
            Cmd::Gc { safe_point: 5, limit: 100 },
        ];
        for c in cmds {
            assert_eq!(Cmd::decode(&c.encode()).unwrap(), c);
        }
        let r = Resp::KeyErrors(vec![KeyError::WriteConflict { key: b"x".to_vec(), conflict_ts: 3 }]);
        assert_eq!(Resp::decode(&r.encode()).unwrap(), r);
    }
}
