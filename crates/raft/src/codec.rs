//! Hand-rolled binary codec for Raft messages and entries (no serde dependency).
//! Decoders never panic on malformed input: they return `None`.
use crate::types::*;
use std::collections::BTreeSet;

#[derive(Default)]
pub struct W(pub Vec<u8>);
impl W {
    pub fn u8(&mut self, v: u8) {
        self.0.push(v)
    }
    pub fn bool(&mut self, v: bool) {
        self.0.push(v as u8)
    }
    pub fn u64(&mut self, v: u64) {
        self.0.extend_from_slice(&v.to_le_bytes())
    }
    pub fn bytes(&mut self, b: &[u8]) {
        self.0.extend_from_slice(&(b.len() as u32).to_le_bytes());
        self.0.extend_from_slice(b);
    }
    pub fn set(&mut self, s: &BTreeSet<NodeId>) {
        self.u64(s.len() as u64);
        for n in s {
            self.u64(*n);
        }
    }
}

pub struct R<'a>(pub &'a [u8]);
impl<'a> R<'a> {
    pub fn u8(&mut self) -> Option<u8> {
        let v = *self.0.first()?;
        self.0 = &self.0[1..];
        Some(v)
    }
    pub fn bool(&mut self) -> Option<bool> {
        match self.u8()? {
            0 => Some(false),
            1 => Some(true),
            _ => None,
        }
    }
    pub fn u64(&mut self) -> Option<u64> {
        let v = u64::from_le_bytes(self.0.get(..8)?.try_into().ok()?);
        self.0 = &self.0[8..];
        Some(v)
    }
    pub fn bytes(&mut self) -> Option<Vec<u8>> {
        let n = u32::from_le_bytes(self.0.get(..4)?.try_into().ok()?) as usize;
        let v = self.0.get(4..4 + n)?.to_vec();
        self.0 = &self.0[4 + n..];
        Some(v)
    }
    pub fn set(&mut self) -> Option<BTreeSet<NodeId>> {
        let n = self.u64()? as usize;
        if n > 1 << 16 {
            return None;
        }
        let mut s = BTreeSet::new();
        for _ in 0..n {
            s.insert(self.u64()?);
        }
        Some(s)
    }
    pub fn done(&self) -> bool {
        self.0.is_empty()
    }
}

pub fn put_membership(w: &mut W, m: &Membership) {
    w.set(&m.voters);
    w.set(&m.outgoing);
    w.set(&m.learners);
}
pub fn get_membership(r: &mut R) -> Option<Membership> {
    Some(Membership { voters: r.set()?, outgoing: r.set()?, learners: r.set()? })
}

pub fn put_entry(w: &mut W, e: &Entry) {
    w.u64(e.term);
    w.u64(e.index);
    match &e.data {
        EntryData::Noop => w.u8(0),
        EntryData::Normal(d) => {
            w.u8(1);
            w.bytes(d)
        }
        EntryData::ConfEnterJoint { voters, learners } => {
            w.u8(2);
            w.set(voters);
            w.set(learners)
        }
        EntryData::ConfLeaveJoint => w.u8(3),
        EntryData::ConfSet(m) => {
            w.u8(4);
            put_membership(w, m)
        }
    }
}
pub fn get_entry(r: &mut R) -> Option<Entry> {
    let term = r.u64()?;
    let index = r.u64()?;
    let data = match r.u8()? {
        0 => EntryData::Noop,
        1 => EntryData::Normal(r.bytes()?),
        2 => EntryData::ConfEnterJoint { voters: r.set()?, learners: r.set()? },
        3 => EntryData::ConfLeaveJoint,
        4 => EntryData::ConfSet(get_membership(r)?),
        _ => return None,
    };
    Some(Entry { term, index, data })
}

pub fn put_snapshot(w: &mut W, s: &Snapshot) {
    w.u64(s.meta.index);
    w.u64(s.meta.term);
    put_membership(w, &s.meta.membership);
    w.bytes(&s.data);
}
pub fn get_snapshot(r: &mut R) -> Option<Snapshot> {
    let index = r.u64()?;
    let term = r.u64()?;
    let membership = get_membership(r)?;
    let data = r.bytes()?;
    Some(Snapshot { meta: SnapshotMeta { index, term, membership }, data })
}

pub fn encode_message(m: &Message) -> Vec<u8> {
    let mut w = W::default();
    w.u64(m.from);
    w.u64(m.to);
    w.u64(m.term);
    match &m.body {
        MsgBody::RequestVote { pre, last_index, last_term, transfer } => {
            w.u8(0);
            w.bool(*pre);
            w.u64(*last_index);
            w.u64(*last_term);
            w.bool(*transfer);
        }
        MsgBody::VoteResp { pre, granted } => {
            w.u8(1);
            w.bool(*pre);
            w.bool(*granted);
        }
        MsgBody::Append { prev_index, prev_term, entries, commit } => {
            w.u8(2);
            w.u64(*prev_index);
            w.u64(*prev_term);
            w.u64(*commit);
            w.u64(entries.len() as u64);
            for e in entries {
                put_entry(&mut w, e);
            }
        }
        MsgBody::AppendResp { success, index, hint } => {
            w.u8(3);
            w.bool(*success);
            w.u64(*index);
            w.u64(*hint);
        }
        MsgBody::Heartbeat { commit, read_seq } => {
            w.u8(4);
            w.u64(*commit);
            w.u64(*read_seq);
        }
        MsgBody::HeartbeatResp { read_seq } => {
            w.u8(5);
            w.u64(*read_seq);
        }
        MsgBody::Snapshot(s) => {
            w.u8(6);
            put_snapshot(&mut w, s);
        }
        MsgBody::TimeoutNow => w.u8(7),
    }
    w.0
}

pub fn decode_message(b: &[u8]) -> Option<Message> {
    let mut r = R(b);
    let (from, to, term) = (r.u64()?, r.u64()?, r.u64()?);
    let body = match r.u8()? {
        0 => MsgBody::RequestVote { pre: r.bool()?, last_index: r.u64()?, last_term: r.u64()?, transfer: r.bool()? },
        1 => MsgBody::VoteResp { pre: r.bool()?, granted: r.bool()? },
        2 => {
            let prev_index = r.u64()?;
            let prev_term = r.u64()?;
            let commit = r.u64()?;
            let n = r.u64()? as usize;
            if n > 1 << 20 {
                return None;
            }
            let mut entries = Vec::with_capacity(n.min(1024));
            for _ in 0..n {
                entries.push(get_entry(&mut r)?);
            }
            MsgBody::Append { prev_index, prev_term, entries, commit }
        }
        3 => MsgBody::AppendResp { success: r.bool()?, index: r.u64()?, hint: r.u64()? },
        4 => MsgBody::Heartbeat { commit: r.u64()?, read_seq: r.u64()? },
        5 => MsgBody::HeartbeatResp { read_seq: r.u64()? },
        6 => MsgBody::Snapshot(get_snapshot(&mut r)?),
        7 => MsgBody::TimeoutNow,
        _ => return None,
    };
    if !r.done() {
        return None;
    }
    Some(Message { from, to, term, body })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn roundtrip_and_garbage() {
        let m = Message {
            from: 1,
            to: 2,
            term: 5,
            body: MsgBody::Append {
                prev_index: 3,
                prev_term: 4,
                commit: 2,
                entries: vec![
                    Entry { term: 5, index: 4, data: EntryData::Normal(b"x".to_vec()) },
                    Entry { term: 5, index: 5, data: EntryData::ConfSet(Membership::new([1, 2, 3])) },
                ],
            },
        };
        assert_eq!(decode_message(&encode_message(&m)).unwrap(), m);
        let enc = encode_message(&m);
        for i in 0..enc.len() {
            let _ = decode_message(&enc[..i]); // must not panic
        }
        assert!(decode_message(&[0xff; 64]).is_none());
    }
}
