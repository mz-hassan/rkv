//! Replays every saved failing seed (crates/sim/regressions/*.json) and requires a clean run.
use sim::*;

fn field(s: &str, name: &str) -> Option<String> {
    let i = s.find(&format!("\"{name}\""))?;
    let rest = &s[i + name.len() + 2..];
    let rest = rest.trim_start_matches([':', ' ']);
    let end = rest.find([',', '}']).unwrap_or(rest.len());
    Some(rest[..end].trim().trim_matches('"').to_string())
}

#[test]
fn saved_failing_seeds_stay_fixed() {
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/regressions");
    let mut replayed = 0;
    for e in std::fs::read_dir(dir).unwrap().flatten() {
        let p = e.path();
        if p.extension().map(|x| x == "json") != Some(true) {
            continue;
        }
        let s = std::fs::read_to_string(&p).unwrap();
        let seed: u64 = field(&s, "seed").unwrap().parse().unwrap();
        let profile = Profile::parse(&field(&s, "profile").unwrap()).unwrap();
        let mut cfg = SimConfig::new(seed, profile);
        cfg.duration_ms = field(&s, "duration_ms").unwrap().parse().unwrap();
        cfg.initial_voters = field(&s, "voters").map(|v| v.parse().unwrap()).unwrap_or(3);
        let r = run_seed(cfg);
        assert!(r.ok(), "regression {} failed again: {:?} {:?}", p.display(), r.safety_violations, r.lin);
        replayed += 1;
    }
    eprintln!("replayed {replayed} regression seeds");
}
