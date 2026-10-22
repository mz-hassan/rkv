//! Seed sweep runner:
//!   rkv-sim --seeds 0..1000 --profile stormy [--duration 20000] [--jobs 8] [--out DIR] [--bug stale-reads]
use lincheck::Verdict;
use sim::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

fn arg(args: &[String], name: &str) -> Option<String> {
    args.iter().position(|a| a == name).and_then(|i| args.get(i + 1).cloned())
}

fn fails(r: &SimResult) -> bool {
    !r.safety_violations.is_empty() || matches!(r.lin, Verdict::Violation { .. })
}

/// Greedy delta-debugging of the fault schedule and duration.
fn minimize(mut cfg: SimConfig) -> SimConfig {
    let base = run_seed(cfg.clone());
    if !fails(&base) {
        return cfg;
    }
    cfg.schedule = Some(base.schedule.clone());
    loop {
        let mut changed = false;
        let sched = cfg.schedule.clone().unwrap();
        for i in (0..sched.len()).rev() {
            let mut s2 = cfg.schedule.clone().unwrap();
            if i >= s2.len() {
                continue;
            }
            s2.remove(i);
            let mut c2 = cfg.clone();
            c2.schedule = Some(s2);
            if fails(&run_seed(c2.clone())) {
                cfg = c2;
                changed = true;
            }
        }
        for d in [cfg.duration_ms / 2, cfg.duration_ms * 3 / 4] {
            if d < 1000 {
                continue;
            }
            let mut c2 = cfg.clone();
            c2.duration_ms = d;
            if fails(&run_seed(c2.clone())) {
                cfg = c2;
                changed = true;
                break;
            }
        }
        if !changed {
            return cfg;
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let range = arg(&args, "--seeds").unwrap_or_else(|| "0..100".into());
    let (a, b) = range.split_once("..").expect("--seeds A..B");
    let (lo, hi): (u64, u64) = (a.parse().unwrap(), b.parse().unwrap());
    let profile = Profile::parse(&arg(&args, "--profile").unwrap_or_else(|| "stormy".into())).expect("bad profile");
    let duration: u64 = arg(&args, "--duration").map(|s| s.parse().unwrap()).unwrap_or(20_000);
    let jobs: usize = arg(&args, "--jobs").map(|s| s.parse().unwrap()).unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4));
    let bug = arg(&args, "--bug").map(|s| match s.as_str() {
        "stale-reads" => Bug::StaleReads,
        "no-dedup" => Bug::NoDedup,
        "ack-early" => Bug::AckEarly,
        "no-persist-vote" => Bug::NoPersistVote,
        "vote-ignores-log" => Bug::Raft(raft::RaftBug::VoteIgnoresLog),
        "commit-minority" => Bug::Raft(raft::RaftBug::CommitMinority),
        "commit-old-term" => Bug::Raft(raft::RaftBug::CommitOldTerm),
        "skip-conflict-check" => Bug::Raft(raft::RaftBug::SkipConflictCheck),
        _ => panic!("unknown bug"),
    });
    let out_dir = arg(&args, "--out");
    let do_min = args.iter().any(|a| a == "--minimize");
    let voters: u64 = arg(&args, "--voters").map(|s| s.parse().unwrap()).unwrap_or(3);

    let next = Arc::new(AtomicU64::new(lo));
    let failures: Arc<Mutex<Vec<(u64, Vec<String>, String)>>> = Arc::new(Mutex::new(vec![]));
    let totals = Arc::new(Mutex::new(SimStats::default()));
    let live_fail = Arc::new(AtomicU64::new(0));
    let inconclusive = Arc::new(AtomicU64::new(0));
    let ran = Arc::new(AtomicU64::new(0));
    let virtual_ms = Arc::new(AtomicU64::new(0));
    let t0 = std::time::Instant::now();
    let mut hs = vec![];
    for _ in 0..jobs {
        let (next, failures, totals, live_fail, inconclusive, ran, virtual_ms) = (next.clone(), failures.clone(), totals.clone(), live_fail.clone(), inconclusive.clone(), ran.clone(), virtual_ms.clone());
        hs.push(std::thread::spawn(move || loop {
            let seed = next.fetch_add(1, Ordering::SeqCst);
            if seed >= hi {
                break;
            }
            let mut cfg = SimConfig::new(seed, profile);
            cfg.duration_ms = duration;
            cfg.bug = bug;
            cfg.initial_voters = voters;
            let r = run_seed(cfg.clone());
            ran.fetch_add(1, Ordering::Relaxed);
            virtual_ms.fetch_add(duration + 6000, Ordering::Relaxed);
            if !r.liveness_ok {
                live_fail.fetch_add(1, Ordering::Relaxed);
            }
            if matches!(r.lin, Verdict::Inconclusive { .. }) {
                inconclusive.fetch_add(1, Ordering::Relaxed);
            }
            {
                let mut t = totals.lock().unwrap();
                let s = &r.stats;
                t.events += s.events;
                t.msgs_sent += s.msgs_sent;
                t.msgs_dropped += s.msgs_dropped;
                t.leaders_elected += s.leaders_elected;
                t.max_term = t.max_term.max(s.max_term);
                t.crashes += s.crashes;
                t.restarts += s.restarts;
                t.snapshots_installed += s.snapshots_installed;
                t.snapshots_taken += s.snapshots_taken;
                t.ops_total += s.ops_total;
                t.ops_completed += s.ops_completed;
                t.ops_pending += s.ops_pending;
                t.committed_entries += s.committed_entries;
                t.invariant_checks += s.invariant_checks;
                t.conf_changes += s.conf_changes;
            }
            if fails(&r) {
                let mut desc = format!("{:?}", r.lin);
                if do_min {
                    let m = minimize(cfg);
                    desc = format!("{desc}; minimized to {} actions, {} ms: {:?}", m.schedule.as_ref().map(|s| s.len()).unwrap_or(0), m.duration_ms, m.schedule);
                }
                failures.lock().unwrap().push((seed, r.safety_violations.clone(), desc));
            }
        }));
    }
    for h in hs {
        h.join().unwrap();
    }
    let wall = t0.elapsed().as_secs_f64();
    let fl = failures.lock().unwrap();
    let t = totals.lock().unwrap();
    let ran = ran.load(Ordering::Relaxed);
    let vh = virtual_ms.load(Ordering::Relaxed) as f64 / 3_600_000.0;
    println!("profile={} seeds={}..{} ran={} failed={} liveness_failures={} lin_inconclusive={}", profile.name(), lo, hi, ran, fl.len(), live_fail.load(Ordering::Relaxed), inconclusive.load(Ordering::Relaxed));
    println!("simulated_hours={:.2} wall_s={:.1} ops={} completed={} pending={} elections={} max_term={} crashes={} snapshots_installed={} snapshots_taken={} conf_changes={} invariant_checks={} committed_entries={}",
        vh, wall, t.ops_total, t.ops_completed, t.ops_pending, t.leaders_elected, t.max_term, t.crashes, t.snapshots_installed, t.snapshots_taken, t.conf_changes, t.invariant_checks, t.committed_entries);
    for (seed, v, d) in fl.iter().take(10) {
        println!("FAIL seed={seed} violations={v:?} lin={d}");
    }
    if let Some(dir) = out_dir {
        std::fs::create_dir_all(&dir).unwrap();
        let ts = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
        let j = format!(
            "{{\"id\":\"sim-{}\",\"timestamp\":{},\"profile\":\"{}\",\"seed_lo\":{},\"seed_hi\":{},\"seeds_run\":{},\"failures\":{},\"liveness_failures\":{},\"lin_inconclusive\":{},\"simulated_hours\":{:.3},\"wall_seconds\":{:.1},\"ops_total\":{},\"ops_completed\":{},\"elections\":{},\"crashes\":{},\"snapshots_installed\":{},\"conf_changes\":{},\"invariant_checks\":{},\"bug_injected\":{},\"honesty\":\"deterministic simulation; simulated faults (network, torn-write power loss, fsync failure, clock drift); same raft crate as production\"}}",
            profile.name(), ts, profile.name(), lo, hi, ran, fl.len(), live_fail.load(Ordering::Relaxed), inconclusive.load(Ordering::Relaxed), vh, wall, t.ops_total, t.ops_completed, t.leaders_elected, t.crashes, t.snapshots_installed, t.conf_changes, t.invariant_checks,
            bug.map(|b| format!("\"{:?}\"", b)).unwrap_or("null".into())
        );
        std::fs::write(format!("{dir}/sim-{}-{ts}.json", profile.name()), j).unwrap();
    }
    if !fl.is_empty() && bug.is_none() {
        std::process::exit(1);
    }
}
