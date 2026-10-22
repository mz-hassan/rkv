#![allow(clippy::result_large_err, clippy::type_complexity, clippy::too_many_arguments)]
//! rkv-chaos: Jepsen-style fault injection against REAL rkv-server/rkv-pd processes on localhost.
//!   rkv-chaos register  --nemesis kill-leader|pause|partition-half|ring|skew|skew-lease|rolling|mixed [--secs 30 --clients 8 --keys 4 --seed 1 --stores 3]
//!   rkv-chaos bank      [--secs 30 --accounts 20 --clients 8 --seed 1 --pd-nodes 1]       (transactions + faults, conservation audits)
//!   rkv-chaos failover  --fault kill|pause|partition [--trials 100 --election-tick 10 --tick-ms 20 --stores 3 --no-prevote --no-check-quorum]
//!   rkv-chaos catchup   [--mb 64 --snapshot-threshold 2000]                               (R2: lagging replica catch-up time)
//!   rkv-chaos cluster-crash [--iterations 10]                                              (S2 cluster: kill -9 ALL nodes, acked writes must survive)
//! Binaries come from RKV_BIN_DIR (default ./target/release). Results: bench/results/<id>/<ts>/result.json
mod cluster;
use ::kv_client::{PdClient, RawClient, TxnClient};
use cluster::*;
use lincheck::{OpKind, OpResult, Recorder, Verdict};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use serde_json::json;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use ycsb::hist::Hist;
use ycsb::report::write_result;

struct Args(Vec<String>);
impl Args {
    fn get(&self, n: &str) -> Option<String> {
        self.0.iter().position(|x| x == n).and_then(|i| self.0.get(i + 1).cloned())
    }
    fn s(&self, n: &str, d: &str) -> String {
        self.get(n).unwrap_or_else(|| d.into())
    }
    fn u(&self, n: &str, d: u64) -> u64 {
        self.get(n).map(|v| v.parse().unwrap()).unwrap_or(d)
    }
    fn flag(&self, n: &str) -> bool {
        self.0.iter().any(|x| x == n)
    }
}

fn now_us(t0: &Instant) -> u64 {
    t0.elapsed().as_micros() as u64
}

// ------------------------------------------------------------------ nemesis

#[derive(Default, Clone)]
struct FaultCounts(Arc<Mutex<BTreeMap<String, u64>>>);
impl FaultCounts {
    fn bump(&self, k: &str) {
        *self.0.lock().unwrap().entry(k.to_string()).or_default() += 1;
    }
}

async fn nemesis(cl: Arc<tokio::sync::Mutex<Cluster>>, kind: String, stop: Arc<AtomicBool>, seed: u64, counts: FaultCounts) {
    let mut rng = ChaCha8Rng::seed_from_u64(seed ^ 0xC4A05);
    while !stop.load(Ordering::Relaxed) {
        tokio::time::sleep(Duration::from_millis(rng.gen_range(1500..4000))).await;
        if stop.load(Ordering::Relaxed) {
            break;
        }
        let pick = if kind == "mixed" { ["kill-leader", "pause", "partition-half", "ring", "rolling"][rng.gen_range(0..5)].to_string() } else { kind.clone() };
        let mut c = cl.lock().await;
        let n = c.stores.len();
        match pick.as_str() {
            "kill-leader" => {
                let victim = c.leader_of(0).await.unwrap_or(rng.gen_range(0..n));
                c.kill9_store(victim);
                counts.bump("kill-9");
                drop(c);
                tokio::time::sleep(Duration::from_millis(rng.gen_range(500..2500))).await;
                cl.lock().await.start_store(victim);
            }
            "pause" => {
                let victim = if rng.gen_bool(0.6) { c.leader_of(0).await.unwrap_or(0) } else { rng.gen_range(0..n) };
                c.pause_store(victim);
                counts.bump("sigstop");
                drop(c);
                tokio::time::sleep(Duration::from_millis(rng.gen_range(800..4000))).await;
                cl.lock().await.resume_store(victim);
            }
            "partition-half" => {
                let ids: Vec<u64> = c.stores.iter().map(|s| s.id).collect();
                let k = rng.gen_range(1..=n / 2);
                let mut shuffled = ids.clone();
                for i in (1..shuffled.len()).rev() {
                    shuffled.swap(i, rng.gen_range(0..=i));
                }
                let (minor, major) = shuffled.split_at(k);
                c.partition(&[minor.to_vec(), major.to_vec()]).await;
                counts.bump("partition");
                drop(c);
                tokio::time::sleep(Duration::from_millis(rng.gen_range(1500..5000))).await;
                cl.lock().await.heal().await;
            }
            "ring" => {
                // each node can only talk to its two ring neighbours (overlapping majorities)
                let ids: Vec<u64> = c.stores.iter().map(|s| s.id).collect();
                for (i, s) in ids.iter().enumerate() {
                    let near = [ids[(i + 1) % n], ids[(i + n - 1) % n]];
                    let blocked: Vec<u64> = ids.iter().copied().filter(|x| x != s && !near.contains(x)).collect();
                    c.set_faults(i, &blocked, 0, 0, 100).await;
                }
                counts.bump("ring-partition");
                drop(c);
                tokio::time::sleep(Duration::from_millis(rng.gen_range(1500..5000))).await;
                cl.lock().await.heal().await;
            }
            "skew" | "skew-lease" => {
                let victim = rng.gen_range(0..n);
                let rate = if rng.gen_bool(0.5) { rng.gen_range(30..70) } else { rng.gen_range(150..400) };
                c.set_faults(victim, &[], 0, 0, rate).await;
                counts.bump("clock-skew");
                drop(c);
                tokio::time::sleep(Duration::from_millis(rng.gen_range(1500..4000))).await;
                let c = cl.lock().await;
                c.set_faults(victim, &[], 0, 0, 100).await;
            }
            "rolling" => {
                for i in 0..n {
                    c.kill9_store(i);
                    counts.bump("kill-9");
                    tokio::time::sleep(Duration::from_millis(700)).await;
                    c.start_store(i);
                    tokio::time::sleep(Duration::from_millis(2500)).await;
                }
            }
            _ => {}
        }
    }
}

// ------------------------------------------------------------------ register workload (linearizability)

async fn run_register(a: &Args) -> i32 {
    let nem = a.s("--nemesis", "mixed");
    let secs = a.u("--secs", 30);
    let nclients = a.u("--clients", 8) as usize;
    let nkeys = a.u("--keys", 4) as usize;
    let seed = a.u("--seed", 1);
    let stores = a.u("--stores", 3);
    let mut extra = vec![];
    if nem == "skew-lease" {
        extra.push("--lease-reads".to_string());
    }
    let root = a.s("--root", &format!("/tmp/rkv-chaos-{}-{}", nem, std::process::id()));
    let mut c = Cluster::new(
        &root,
        ClusterOpts { stores, extra_server_args: extra, election_tick: a.u("--election-tick", 10), tick_ms: a.u("--tick-ms", 20), ..Default::default() },
    );
    c.start_all().await;
    if !c.wait_ready(30).await {
        eprintln!("cluster did not become ready; logs in {root}");
        return 2;
    }
    let seeds = c.store_addrs();
    let cl = Arc::new(tokio::sync::Mutex::new(c));
    let rec = Arc::new(Mutex::new(Recorder::default()));
    let t0 = Instant::now();
    let stop = Arc::new(AtomicBool::new(false));
    let counts = FaultCounts::default();
    let nem_task = tokio::spawn(nemesis(cl.clone(), nem.clone(), stop.clone(), seed, counts.clone()));
    let value_ctr = Arc::new(AtomicU64::new(1));
    let mut hs = vec![];
    for ci in 0..nclients {
        let (seeds, rec, stop, ctr) = (seeds.clone(), rec.clone(), stop.clone(), value_ctr.clone());
        hs.push(tokio::spawn(async move {
            let client = RawClient::new(None, seeds);
            let mut rng = ChaCha8Rng::seed_from_u64(seed * 1000 + ci as u64);
            let mut last_seen: BTreeMap<usize, Option<Vec<u8>>> = BTreeMap::new();
            while !stop.load(Ordering::Relaxed) {
                let ki = rng.gen_range(0..nkeys);
                let key = format!("reg{ki}").into_bytes();
                let r = rng.gen_range(0..100);
                let (kind, id) = if r < 40 {
                    let v = ctr.fetch_add(1, Ordering::SeqCst).to_be_bytes().to_vec();
                    let k = OpKind::Write(v);
                    let id = rec.lock().unwrap().invoke(ci as u64, &key, k.clone(), now_us(&t0));
                    (k, id)
                } else if r < 85 {
                    let k = OpKind::Read;
                    let id = rec.lock().unwrap().invoke(ci as u64, &key, k.clone(), now_us(&t0));
                    (k, id)
                } else {
                    let exp = last_seen.get(&ki).cloned().unwrap_or(None);
                    let nv = ctr.fetch_add(1, Ordering::SeqCst).to_be_bytes().to_vec();
                    let k = OpKind::Cas(exp, nv);
                    let id = rec.lock().unwrap().invoke(ci as u64, &key, k.clone(), now_us(&t0));
                    (k, id)
                };
                let res = tokio::time::timeout(Duration::from_secs(8), async {
                    match &kind {
                        OpKind::Write(v) => client.put(&key, v).await.map(|_| OpResult::WriteOk),
                        OpKind::Read => client.get(&key).await.map(OpResult::ReadOk),
                        OpKind::Cas(e, n) => client.cas(&key, e.as_deref(), n).await.map(OpResult::CasOk),
                        OpKind::Delete => unreachable!(),
                    }
                })
                .await;
                if let Ok(Ok(result)) = res {
                    match (&kind, &result) {
                        (_, OpResult::ReadOk(v)) => {
                            last_seen.insert(ki, v.clone());
                        }
                        (OpKind::Write(v), _) => {
                            last_seen.insert(ki, Some(v.clone()));
                        }
                        (OpKind::Cas(_, n), OpResult::CasOk(true)) => {
                            last_seen.insert(ki, Some(n.clone()));
                        }
                        _ => {}
                    }
                    rec.lock().unwrap().complete(id, now_us(&t0), result);
                } else {
                    // outcome unknown: leave the operation pending forever (it may or may not have applied)
                    last_seen.remove(&ki);
                }
                tokio::time::sleep(Duration::from_millis(rng.gen_range(1..20))).await;
            }
        }));
    }
    tokio::time::sleep(Duration::from_secs(secs)).await;
    stop.store(true, Ordering::Relaxed);
    for h in hs {
        let _ = h.await;
    }
    let _ = nem_task.await;
    // heal everything, restart dead nodes, then read every key back (detects lost acknowledged writes)
    {
        let mut c = cl.lock().await;
        c.heal().await;
        for i in 0..c.stores.len() {
            if c.stores[i].paused {
                c.resume_store(i);
            }
            if !c.is_up(i) {
                c.start_store(i);
            }
        }
        let ready = c.wait_ready(30).await;
        if !ready {
            eprintln!("warning: cluster not ready after healing");
        }
    }
    let final_client = RawClient::new(None, seeds.clone());
    for ki in 0..nkeys {
        let key = format!("reg{ki}").into_bytes();
        let id = rec.lock().unwrap().invoke(9999, &key, OpKind::Read, now_us(&t0));
        if let Ok(Ok(v)) = tokio::time::timeout(Duration::from_secs(20), final_client.get(&key)).await {
            rec.lock().unwrap().complete(id, now_us(&t0), OpResult::ReadOk(v));
        }
    }
    let history = rec.lock().unwrap().ops.clone();
    let completed = history.iter().filter(|o| o.ret.is_some()).count();
    let (verdict, stats) = lincheck::check(&history, 3_000_000);
    let (vname, violation) = match &verdict {
        Verdict::Linearizable => ("linearizable", false),
        Verdict::Violation { .. } => ("VIOLATION", true),
        Verdict::Inconclusive { .. } => ("inconclusive (search budget)", false),
    };
    let expected_violation_possible = nem == "skew-lease";
    let id = a.s("--id", &format!("J-{nem}"));
    let body = json!({
        "bench": "chaos-register", "config": { "nemesis": nem, "secs": secs, "clients": nclients, "keys": nkeys, "seed": seed, "stores": stores, "election_tick": a.u("--election-tick", 10), "tick_ms": a.u("--tick-ms", 20) },
        "results": { "ops": history.len(), "completed": completed, "pending_unknown": history.len() - completed, "faults": counts.0.lock().unwrap().clone(), "verdict": vname, "violation": violation, "lin_states_explored": stats.states_explored, "violation_expected_possible": expected_violation_possible },
    });
    let labels = json!({ "scope": "localhost multi-process cluster; real processes and sockets", "faults": "REAL kill -9 / SIGSTOP; partitions, drops and clock-rate skew injected at the Raft transport layer (not kernel-level netem/iptables)", "checker": "lincheck (Wing-Gong/Lowe)" });
    let p = write_result(&a.s("--out", "bench/results"), &id, body, labels).unwrap();
    println!("{p}\nverdict: {vname} ({} ops, {} completed, faults {:?})", history.len(), completed, counts.0.lock().unwrap());
    if violation && !expected_violation_possible {
        3
    } else {
        0
    }
}

// ------------------------------------------------------------------ bank under faults

async fn run_bank(a: &Args) -> i32 {
    let secs = a.u("--secs", 30);
    let accounts = a.u("--accounts", 20);
    let nclients = a.u("--clients", 8) as usize;
    let seed = a.u("--seed", 1);
    let nem = a.s("--nemesis", "mixed");
    let root = a.s("--root", &format!("/tmp/rkv-chaos-bank-{}", std::process::id()));
    let mut c =
        Cluster::new(&root, ClusterOpts { stores: a.u("--stores", 3), pd_nodes: a.u("--pd-nodes", 1), election_tick: 10, tick_ms: 20, ..Default::default() });
    c.start_all().await;
    if !c.wait_ready(60).await {
        eprintln!("cluster did not become ready; logs in {root}");
        return 2;
    }
    let pd = Arc::new(PdClient::new(c.pd_addrs()));
    let iso = if a.s("--isolation", "snapshot") == "serializable" { ::kv_client::IsolationLevel::Serializable } else { ::kv_client::IsolationLevel::Snapshot };
    let tc = TxnClient::with_isolation(pd.clone(), vec![], iso);
    let key = |i: u64| format!("acct{i:04}").into_bytes();
    for chunk in (0..accounts).collect::<Vec<_>>().chunks(50) {
        let mut t = tc.begin().await.unwrap();
        for i in chunk {
            t.put(&key(*i), b"100");
        }
        t.commit().await.expect("load");
    }
    let expected = accounts as i64 * 100;
    let cl = Arc::new(tokio::sync::Mutex::new(c));
    let stop = Arc::new(AtomicBool::new(false));
    let counts = FaultCounts::default();
    let nem_task = tokio::spawn(nemesis(cl.clone(), nem.clone(), stop.clone(), seed, counts.clone()));
    let (commits, errs, bad_audits, audits) =
        (Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)));
    let mut hs = vec![];
    for ci in 0..nclients {
        let (tc, stop, commits, errs, bad, audits) = (tc.clone(), stop.clone(), commits.clone(), errs.clone(), bad_audits.clone(), audits.clone());
        hs.push(tokio::spawn(async move {
            let mut rng = ChaCha8Rng::seed_from_u64(seed * 77 + ci as u64);
            while !stop.load(Ordering::Relaxed) {
                if ci == 0 && rng.gen_bool(0.2) {
                    // audit: a snapshot read of everything must sum to the constant total
                    let r = tokio::time::timeout(Duration::from_secs(10), async {
                        let mut t = tc.begin().await.ok()?;
                        let mut s = 0i64;
                        for i in 0..accounts {
                            s += String::from_utf8(t.get(&key(i)).await.ok()??).ok()?.parse::<i64>().ok()?;
                        }
                        Some(s)
                    })
                    .await;
                    if let Ok(Some(sum)) = r {
                        audits.fetch_add(1, Ordering::Relaxed);
                        if sum != expected {
                            bad.fetch_add(1, Ordering::Relaxed);
                            eprintln!("AUDIT FAILED: total {sum} != {expected}");
                        }
                    }
                    continue;
                }
                let (x, y) = (rng.gen_range(0..accounts), rng.gen_range(0..accounts));
                if x == y {
                    continue;
                }
                let r = tokio::time::timeout(Duration::from_secs(10), async {
                    let mut t = tc.begin().await.ok()?;
                    let bx: i64 = String::from_utf8(t.get(&key(x)).await.ok()??).ok()?.parse().ok()?;
                    let by: i64 = String::from_utf8(t.get(&key(y)).await.ok()??).ok()?.parse().ok()?;
                    t.put(&key(x), (bx - 7).to_string().as_bytes());
                    t.put(&key(y), (by + 7).to_string().as_bytes());
                    t.commit().await.ok()
                })
                .await;
                match r {
                    Ok(Some(_)) => commits.fetch_add(1, Ordering::Relaxed),
                    _ => errs.fetch_add(1, Ordering::Relaxed),
                };
            }
        }));
    }
    tokio::time::sleep(Duration::from_secs(secs)).await;
    stop.store(true, Ordering::Relaxed);
    for h in hs {
        let _ = h.await;
    }
    let _ = nem_task.await;
    {
        let mut c = cl.lock().await;
        c.heal().await;
        for i in 0..c.stores.len() {
            if c.stores[i].paused {
                c.resume_store(i);
            }
            if !c.is_up(i) {
                c.start_store(i);
            }
        }
        c.wait_ready(60).await;
    }
    tokio::time::sleep(Duration::from_secs(2)).await; // let lock resolution settle
    let mut final_total = None;
    for _ in 0..10 {
        if let Ok(mut t) = tc.begin().await {
            let mut s = 0i64;
            let mut ok = true;
            for i in 0..accounts {
                match t.get(&key(i)).await {
                    Ok(Some(v)) => s += String::from_utf8(v).unwrap().parse::<i64>().unwrap(),
                    _ => ok = false,
                }
            }
            if ok {
                final_total = Some(s);
                break;
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let violated = bad_audits.load(Ordering::Relaxed) > 0 || final_total != Some(expected);
    let body = json!({
        "bench": "chaos-bank", "config": { "secs": secs, "accounts": accounts, "clients": nclients, "nemesis": nem, "seed": seed, "isolation": a.s("--isolation", "snapshot") },
        "results": { "committed_transfers": commits.load(Ordering::Relaxed), "failed_or_unknown": errs.load(Ordering::Relaxed), "audits_ok_or_bad": audits.load(Ordering::Relaxed), "bad_audits": bad_audits.load(Ordering::Relaxed), "final_total": final_total, "expected_total": expected, "invariant_ok": !violated, "faults": counts.0.lock().unwrap().clone() },
    });
    let labels = json!({ "scope": "localhost multi-process cluster with PD", "faults": "REAL kill -9 / SIGSTOP; transport-layer partitions" });
    println!("{}", write_result(&a.s("--out", "bench/results"), &a.s("--id", "J-bank"), body, labels).unwrap());
    if violated {
        eprintln!("BANK INVARIANT VIOLATED");
        3
    } else {
        0
    }
}

// ------------------------------------------------------------------ R2: failover windows

async fn run_failover(a: &Args) -> i32 {
    let fault = a.s("--fault", "kill");
    let trials = a.u("--trials", 20);
    let mut extra = vec![];
    if a.flag("--no-prevote") {
        extra.push("--no-prevote".to_string());
    }
    if a.flag("--no-check-quorum") {
        extra.push("--no-check-quorum".to_string());
    }
    let (et, tm) = (a.u("--election-tick", 10), a.u("--tick-ms", 20));
    let root = a.s("--root", &format!("/tmp/rkv-chaos-failover-{}", std::process::id()));
    let mut c = Cluster::new(&root, ClusterOpts { stores: a.u("--stores", 3), extra_server_args: extra, election_tick: et, tick_ms: tm, ..Default::default() });
    c.start_all().await;
    if !c.wait_ready(30).await {
        eprintln!("cluster not ready; logs in {root}");
        return 2;
    }
    let client = RawClient::new(None, c.store_addrs());
    let mut windows = Hist::new();
    let mut raw_ms = vec![];
    let mut failed = 0;
    for trial in 0..trials {
        // healthy baseline
        let mut ok = false;
        for _ in 0..100 {
            if client.put(b"probe", b"x").await.is_ok() {
                ok = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        if !ok {
            failed += 1;
            continue;
        }
        let Some(leader) = c.leader_of(0).await else { continue };
        let t0 = Instant::now();
        match fault.as_str() {
            "kill" => c.kill9_store(leader),
            "pause" => c.pause_store(leader),
            _ => {
                let lid = c.stores[leader].id;
                let others: Vec<u64> = c.stores.iter().map(|s| s.id).filter(|i| *i != lid).collect();
                c.partition(&[vec![lid], others]).await;
            }
        }
        // client-observed unavailability: first successful write after the fault
        let mut window = None;
        while t0.elapsed() < Duration::from_secs(30) {
            if tokio::time::timeout(Duration::from_millis(1500), client.put(b"probe", format!("t{trial}").as_bytes())).await.map(|r| r.is_ok()).unwrap_or(false)
            {
                window = Some(t0.elapsed());
                break;
            }
        }
        match window {
            Some(w) => {
                windows.record_us(w.as_micros() as u64);
                raw_ms.push(w.as_millis() as u64);
            }
            None => failed += 1,
        }
        // restore
        match fault.as_str() {
            "kill" => c.start_store(leader),
            "pause" => c.resume_store(leader),
            _ => c.heal().await,
        }
        c.wait_ready(30).await;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    raw_ms.sort();
    let pct =
        |p: f64| raw_ms.get(((raw_ms.len() as f64 * p / 100.0).ceil() as usize).saturating_sub(1).min(raw_ms.len().saturating_sub(1))).copied().unwrap_or(0);
    let body = json!({
        "bench": "failover-window",
        "config": { "fault": fault, "trials": trials, "election_tick": et, "tick_ms": tm, "election_timeout_ms_range": [et * tm, 2 * et * tm], "prevote": !a.flag("--no-prevote"), "check_quorum": !a.flag("--no-check-quorum"), "stores": a.u("--stores", 3) },
        "results": { "completed_trials": raw_ms.len(), "failed_trials": failed, "unavailability_ms": { "p50": pct(50.0), "p95": pct(95.0), "p99": pct(99.0), "max": raw_ms.last().copied().unwrap_or(0), "mean": windows.mean_us() / 1000.0 }, "samples_ms": raw_ms },
    });
    let labels = json!({ "scope": "localhost multi-process cluster", "faults": match fault.as_str() { "kill" => "REAL kill -9 of the leader", "pause" => "REAL SIGSTOP of the leader", _ => "leader isolated by transport-layer partition" }, "metric": "client-observed time from fault injection to first successful linearizable write (includes client leader re-discovery)" });
    println!("{}", write_result(&a.s("--out", "bench/results"), &a.s("--id", &format!("R2-{fault}")), body, labels).unwrap());
    0
}

// ------------------------------------------------------------------ R2: snapshot catch-up

async fn run_catchup(a: &Args) -> i32 {
    let mb = a.u("--mb", 64);
    let st = a.u("--snapshot-threshold", 2000).to_string();
    let root = a.s("--root", &format!("/tmp/rkv-chaos-catchup-{}", std::process::id()));
    let mut c = Cluster::new(&root, ClusterOpts { stores: 3, extra_server_args: vec!["--snapshot-threshold".into(), st], ..Default::default() });
    c.start_all().await;
    if !c.wait_ready(30).await {
        return 2;
    }
    let seeds = c.store_addrs();
    let client = Arc::new(RawClient::new(None, seeds));
    let victim = (c.leader_of(0).await.unwrap_or(0) + 1) % 3;
    c.kill9_store(victim);
    let value = vec![b'v'; 1024];
    let n = mb * 1024;
    let t_load = Instant::now();
    let mut hs = vec![];
    let ctr = Arc::new(AtomicU64::new(0));
    for _ in 0..16 {
        let (client, ctr, value) = (client.clone(), ctr.clone(), value.clone());
        hs.push(tokio::spawn(async move {
            loop {
                let i = ctr.fetch_add(1, Ordering::Relaxed);
                if i >= n {
                    break;
                }
                let _ = client.put(format!("blob{i:09}").as_bytes(), &value).await;
            }
        }));
    }
    for h in hs {
        let _ = h.await;
    }
    let load_secs = t_load.elapsed().as_secs_f64();
    let leader = c.leader_of(0).await.unwrap_or(0);
    let target = c.statuses().await.into_iter().find(|(i, _, _)| *i == leader).and_then(|(_, _, r)| r.first().map(|x| x.commit)).unwrap_or(0);
    let t0 = Instant::now();
    c.start_store(victim);
    let mut caught = None;
    while t0.elapsed() < Duration::from_secs(300) {
        for (i, _, rs) in c.statuses().await {
            if i == victim && rs.first().map(|r| r.applied >= target).unwrap_or(false) {
                caught = Some(t0.elapsed());
            }
        }
        if caught.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let body = json!({
        "bench": "snapshot-catchup", "config": { "data_mb": mb, "value_bytes": 1024, "snapshot_threshold_entries": a.u("--snapshot-threshold", 2000) },
        "results": { "load_secs": load_secs, "target_applied_index": target, "catchup_secs": caught.map(|d| d.as_secs_f64()), "caught_up": caught.is_some() },
    });
    let labels = json!({ "scope": "localhost multi-process cluster", "faults": "REAL kill -9 of one follower, restart after the leader compacted its log (forces snapshot transfer)", "caveat": "snapshot data is shipped as one in-memory message; practical for lab-scale state only" });
    println!("{}", write_result(&a.s("--out", "bench/results"), &a.s("--id", "R2-catchup"), body, labels).unwrap());
    0
}

// ------------------------------------------------------------------ S2 (cluster): kill -9 everything

async fn run_cluster_crash(a: &Args) -> i32 {
    let iters = a.u("--iterations", 5);
    let root = a.s("--root", &format!("/tmp/rkv-chaos-ccrash-{}", std::process::id()));
    let mut c = Cluster::new(&root, ClusterOpts { stores: 3, ..Default::default() });
    c.start_all().await;
    if !c.wait_ready(30).await {
        return 2;
    }
    let seeds = c.store_addrs();
    let mut acked: Vec<u64> = vec![];
    let mut next = 0u64;
    let mut lost = 0u64;
    let mut rng = ChaCha8Rng::seed_from_u64(a.u("--seed", 3));
    for _ in 0..iters {
        let client = RawClient::new(None, seeds.clone());
        let t0 = Instant::now();
        let kill_after = Duration::from_millis(rng.gen_range(500..2500));
        while t0.elapsed() < kill_after {
            let k = format!("dur{next:08}");
            if tokio::time::timeout(Duration::from_secs(3), client.put(k.as_bytes(), k.as_bytes())).await.map(|r| r.is_ok()).unwrap_or(false) {
                acked.push(next);
            }
            next += 1;
        }
        for i in 0..c.stores.len() {
            c.kill9_store(i); // whole-cluster crash
        }
        for i in 0..c.stores.len() {
            c.start_store(i);
        }
        c.wait_ready(60).await;
        let reader = RawClient::new(None, seeds.clone());
        for i in &acked {
            let k = format!("dur{i:08}");
            match tokio::time::timeout(Duration::from_secs(10), reader.get(k.as_bytes())).await {
                Ok(Ok(Some(v))) if v == k.as_bytes() => {}
                _ => lost += 1,
            }
        }
    }
    let body = json!({ "bench": "cluster-crash-durability", "config": { "iterations": iters, "nodes": 3 }, "results": { "acked_writes": acked.len(), "lost_acked_writes": lost } });
    let labels = json!({ "scope": "localhost multi-process cluster", "faults": "REAL kill -9 of ALL nodes simultaneously (process crash; OS page cache survives, so not a power-loss test)" });
    println!("{}", write_result(&a.s("--out", "bench/results"), &a.s("--id", "S2-cluster"), body, labels).unwrap());
    if lost > 0 {
        3
    } else {
        0
    }
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let sub = args.get(1).cloned().unwrap_or_default();
    let a = Args(args.get(2..).unwrap_or(&[]).to_vec());
    let code = match sub.as_str() {
        "register" => run_register(&a).await,
        "bank" => run_bank(&a).await,
        "failover" => run_failover(&a).await,
        "catchup" => run_catchup(&a).await,
        "cluster-crash" => run_cluster_crash(&a).await,
        _ => {
            eprintln!("usage: rkv-chaos <register|bank|failover|catchup|cluster-crash> ...");
            2
        }
    };
    std::process::exit(code);
}
