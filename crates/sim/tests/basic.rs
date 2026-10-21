use sim::*;

#[test]
fn calm_cluster_makes_progress_and_is_linearizable() {
    let mut cfg = SimConfig::new(1, Profile::Calm);
    cfg.duration_ms = 5000;
    let r = run_seed(cfg);
    eprintln!("{:#?}", r.stats);
    assert!(r.safety_violations.is_empty(), "{:?}", r.safety_violations);
    assert_eq!(r.lin, lincheck::Verdict::Linearizable);
    assert!(r.stats.ops_completed > 100, "only {} ops", r.stats.ops_completed);
    assert!(r.stats.leaders_elected >= 1);
    assert!(r.liveness_ok);
}
