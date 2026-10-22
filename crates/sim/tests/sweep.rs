//! A small deterministic sweep that runs under `cargo test` (the big sweeps are `make sim`),
//! plus checks that the harness can actually fail: injected bugs must be caught.
use lincheck::Verdict;
use sim::*;

fn caught(r: &SimResult) -> bool {
    !r.safety_violations.is_empty() || matches!(r.lin, Verdict::Violation { .. })
}

#[test]
fn correct_code_is_clean_on_every_profile() {
    for profile in [Profile::Calm, Profile::Stormy, Profile::PartitionHeavy, Profile::CrashHeavy, Profile::Membership, Profile::Skew] {
        for seed in 0..12 {
            let mut cfg = SimConfig::new(seed, profile);
            cfg.duration_ms = 8_000;
            let r = run_seed(cfg);
            assert!(r.ok(), "profile {profile:?} seed {seed}: {:?} {:?}", r.safety_violations, r.lin);
            assert!(r.liveness_ok, "profile {profile:?} seed {seed}: no operation completed after the faults healed");
        }
    }
}

fn catches(bug: Bug, profile: Profile, voters: u64, seeds: std::ops::Range<u64>) -> bool {
    seeds.into_iter().any(|seed| {
        let mut cfg = SimConfig::new(seed, profile);
        cfg.duration_ms = 10_000;
        cfg.bug = Some(bug);
        cfg.initial_voters = voters;
        caught(&run_seed(cfg))
    })
}

#[test]
fn injected_application_bugs_are_caught() {
    assert!(catches(Bug::StaleReads, Profile::Stormy, 3, 0..10), "stale follower reads must break linearizability");
    assert!(catches(Bug::AckEarly, Profile::Stormy, 3, 0..10), "acking before commit must lose acknowledged writes");
    assert!(catches(Bug::NoDedup, Profile::Stormy, 3, 0..80), "missing request dedup must double-apply a retried write");
}

#[test]
fn injected_consensus_bugs_are_caught() {
    assert!(catches(Bug::NoPersistVote, Profile::CrashHeavy, 3, 0..120), "not persisting votes must allow two leaders in a term");
    assert!(catches(Bug::Raft(raft::RaftBug::VoteIgnoresLog), Profile::Stormy, 3, 0..40), "ignoring the log up-to-date check must violate leader completeness");
    assert!(catches(Bug::Raft(raft::RaftBug::CommitMinority), Profile::PartitionHeavy, 5, 0..10), "committing on a minority must be detected (needs 5 voters)");
}
