//! The replicated state machine used by the simulator: a KV map with
//! exactly-once client sessions (request dedup).
use raft::codec::{R, W};
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Cmd {
    Put { client: u64, seq: u64, key: Vec<u8>, val: Vec<u8> },
    Del { client: u64, seq: u64, key: Vec<u8> },
    Cas { client: u64, seq: u64, key: Vec<u8>, exp: Option<Vec<u8>>, new: Vec<u8> },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CmdResult {
    Ok,
    Cas(bool),
}

impl Cmd {
    pub fn client(&self) -> (u64, u64) {
        match self {
            Cmd::Put { client, seq, .. } | Cmd::Del { client, seq, .. } | Cmd::Cas { client, seq, .. } => (*client, *seq),
        }
    }
    pub fn encode(&self) -> Vec<u8> {
        let mut w = W::default();
        match self {
            Cmd::Put { client, seq, key, val } => {
                w.u8(1);
                w.u64(*client);
                w.u64(*seq);
                w.bytes(key);
                w.bytes(val);
            }
            Cmd::Del { client, seq, key } => {
                w.u8(2);
                w.u64(*client);
                w.u64(*seq);
                w.bytes(key);
            }
            Cmd::Cas { client, seq, key, exp, new } => {
                w.u8(3);
                w.u64(*client);
                w.u64(*seq);
                w.bytes(key);
                w.bool(exp.is_some());
                w.bytes(exp.as_deref().unwrap_or(&[]));
                w.bytes(new);
            }
        }
        w.0
    }
    pub fn decode(b: &[u8]) -> Option<Cmd> {
        let mut r = R(b);
        Some(match r.u8()? {
            1 => Cmd::Put { client: r.u64()?, seq: r.u64()?, key: r.bytes()?, val: r.bytes()? },
            2 => Cmd::Del { client: r.u64()?, seq: r.u64()?, key: r.bytes()? },
            3 => {
                let (client, seq, key) = (r.u64()?, r.u64()?, r.bytes()?);
                let has = r.bool()?;
                let e = r.bytes()?;
                Cmd::Cas { client, seq, key, exp: if has { Some(e) } else { None }, new: r.bytes()? }
            }
            _ => return None,
        })
    }
}

#[derive(Clone, Default)]
pub struct Sm {
    pub kv: BTreeMap<Vec<u8>, Vec<u8>>,
    sessions: BTreeMap<u64, (u64, CmdResult)>,
    /// MUTATION (test the checker): disable request dedup
    pub no_dedup: bool,
}

impl Sm {
    pub fn apply(&mut self, cmd: &Cmd) -> CmdResult {
        let (client, seq) = cmd.client();
        if !self.no_dedup {
            if let Some((last, res)) = self.sessions.get(&client) {
                if seq <= *last {
                    return *res; // duplicate: return the cached outcome, do not re-apply
                }
            }
        }
        let res = match cmd {
            Cmd::Put { key, val, .. } => {
                self.kv.insert(key.clone(), val.clone());
                CmdResult::Ok
            }
            Cmd::Del { key, .. } => {
                self.kv.remove(key);
                CmdResult::Ok
            }
            Cmd::Cas { key, exp, new, .. } => {
                if self.kv.get(key) == exp.as_ref() {
                    self.kv.insert(key.clone(), new.clone());
                    CmdResult::Cas(true)
                } else {
                    CmdResult::Cas(false)
                }
            }
        };
        self.sessions.insert(client, (seq, res));
        res
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut w = W::default();
        w.u64(self.kv.len() as u64);
        for (k, v) in &self.kv {
            w.bytes(k);
            w.bytes(v);
        }
        w.u64(self.sessions.len() as u64);
        for (c, (s, r)) in &self.sessions {
            w.u64(*c);
            w.u64(*s);
            match r {
                CmdResult::Ok => w.u8(0),
                CmdResult::Cas(b) => w.u8(if *b { 2 } else { 1 }),
            }
        }
        w.0
    }

    pub fn decode(b: &[u8]) -> Option<Sm> {
        let mut r = R(b);
        let mut sm = Sm::default();
        for _ in 0..r.u64()? {
            let k = r.bytes()?;
            sm.kv.insert(k, r.bytes()?);
        }
        for _ in 0..r.u64()? {
            let c = r.u64()?;
            let s = r.u64()?;
            let res = match r.u8()? {
                0 => CmdResult::Ok,
                1 => CmdResult::Cas(false),
                2 => CmdResult::Cas(true),
                _ => return None,
            };
            sm.sessions.insert(c, (s, res));
        }
        Some(sm)
    }
}
