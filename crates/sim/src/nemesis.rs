//! Fault schedules. Generated from a seed up front as an explicit list so a
//! failing run can be minimized by deleting actions.
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

#[derive(Clone, Debug, PartialEq)]
pub enum Action {
    /// disconnect the listed groups from one another
    Partition(Vec<Vec<u64>>),
    /// block traffic from `.0` to `.1` only
    OneWay(u64, u64),
    Isolate(u64),
    Heal,
    Crash(u64),
    Restart(u64),
    SetDrop(f64),
    SetDup(f64),
    SetDelay(u64, u64),
    /// set a node's clock rate (1.0 = nominal)
    Drift(u64, f64),
    TransferLeader,
    /// make the next N fsyncs of a node fail (the node fail-stops)
    FailSync(u64),
    ConfChange {
        voters: Vec<u64>,
        learners: Vec<u64>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Profile {
    Calm,
    Stormy,
    PartitionHeavy,
    CrashHeavy,
    Membership,
    Skew,
}

impl Profile {
    pub fn parse(s: &str) -> Option<Profile> {
        Some(match s {
            "calm" => Profile::Calm,
            "stormy" => Profile::Stormy,
            "partition" => Profile::PartitionHeavy,
            "crash" => Profile::CrashHeavy,
            "membership" => Profile::Membership,
            "skew" => Profile::Skew,
            _ => return None,
        })
    }
    pub fn name(&self) -> &'static str {
        match self {
            Profile::Calm => "calm",
            Profile::Stormy => "stormy",
            Profile::PartitionHeavy => "partition",
            Profile::CrashHeavy => "crash",
            Profile::Membership => "membership",
            Profile::Skew => "skew",
        }
    }
}

pub fn generate(seed: u64, profile: Profile, duration_ms: u64, nodes: u64) -> Vec<(u64, Action)> {
    let mut rng = ChaCha8Rng::seed_from_u64(seed ^ 0xA5A5_5A5A_1234_5678);
    let mut out = vec![];
    if profile == Profile::Calm {
        return out;
    }
    let mut t = 500u64;
    let ids: Vec<u64> = (1..=nodes).collect();
    while t < duration_ms {
        let kind = rng.gen_range(0..100);
        let node = ids[rng.gen_range(0..ids.len())];
        let a = match profile {
            Profile::Stormy | Profile::Membership | Profile::Skew => match kind {
                0..=19 => {
                    let mut v = ids.clone();
                    for i in (1..v.len()).rev() {
                        v.swap(i, rng.gen_range(0..=i));
                    }
                    let k = rng.gen_range(1..v.len());
                    Action::Partition(vec![v[..k].to_vec(), v[k..].to_vec()])
                }
                20..=29 => Action::Isolate(node),
                30..=44 => Action::Heal,
                45..=54 => {
                    let down = rng.gen_range(100..2500);
                    out.push((t + down, Action::Restart(node)));
                    Action::Crash(node)
                }
                55..=61 => Action::SetDrop(rng.gen_range(0.0..0.25)),
                62..=66 => Action::SetDup(rng.gen_range(0.0..0.15)),
                67..=71 => Action::SetDelay(1, rng.gen_range(2..60)),
                72..=76 => {
                    let r = if profile == Profile::Skew { rng.gen_range(0.4..2.5) } else { rng.gen_range(0.9..1.1) };
                    Action::Drift(node, r)
                }
                77..=84 => Action::TransferLeader,
                85..=88 => Action::FailSync(node),
                _ if profile == Profile::Membership => {
                    let mut v = ids.clone();
                    for i in (1..v.len()).rev() {
                        v.swap(i, rng.gen_range(0..=i));
                    }
                    let nv = rng.gen_range(1..=v.len().min(5));
                    let nl = rng.gen_range(0..=(v.len() - nv));
                    Action::ConfChange { voters: v[..nv].to_vec(), learners: v[nv..nv + nl].to_vec() }
                }
                _ => Action::Heal,
            },
            Profile::PartitionHeavy => match kind {
                0..=39 => {
                    let mut v = ids.clone();
                    for i in (1..v.len()).rev() {
                        v.swap(i, rng.gen_range(0..=i));
                    }
                    let k = rng.gen_range(1..v.len());
                    Action::Partition(vec![v[..k].to_vec(), v[k..].to_vec()])
                }
                40..=59 => Action::Isolate(node),
                60..=74 => {
                    let b = ids[rng.gen_range(0..ids.len())];
                    Action::OneWay(node, b)
                }
                _ => Action::Heal,
            },
            Profile::CrashHeavy => match kind {
                0..=59 => {
                    let down = rng.gen_range(50..1500);
                    out.push((t + down, Action::Restart(node)));
                    Action::Crash(node)
                }
                60..=74 => Action::FailSync(node),
                75..=84 => Action::Heal,
                _ => Action::TransferLeader,
            },
            Profile::Calm => unreachable!(),
        };
        out.push((t, a));
        t += rng.gen_range(150..1500);
    }
    out.sort_by_key(|(t, _)| *t);
    out
}
