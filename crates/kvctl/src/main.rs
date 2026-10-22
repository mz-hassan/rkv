#![allow(clippy::result_large_err, clippy::type_complexity, clippy::too_many_arguments)]
//! kvctl: command line client and cluster admin.
//!
//!   kvctl [--pd HOST:PORT[,..]] [--seeds HOST:PORT[,..]] <command> ...
//!   put KEY VALUE [--ttl-ms N] | get KEY [--stale-ms N] | del KEY | cas KEY EXPECT|- NEW | scan START END [LIMIT]
//!   txn put K V [K V ...]             one multi-key transaction (may span ranges)
//!   txn get K [K ...]                 snapshot read of several keys
//!   watch START END                   stream change events
//!   status                            ranges, leaders, replica progress for every store
//!   members RANGE --voters 1,2,3 [--learners 4]
//!   transfer-leader RANGE STORE
//!   split RANGE [KEY]                 split a range (omitted key = median)
//!   lease acquire NAME OWNER TTL_MS | lease holder NAME | lease renew|release NAME OWNER TOKEN TTL_MS
//!   merge LEFT_RANGE                  merge a range with its right neighbour (replica sets must be identical)
//!   presplit KEY [KEY ...]            split at each key (finds the range that contains it)
//!   tso | pd-config KEY VALUE | pd-ranges
//!   faults STORE_ADDR [--block 1,2] [--drop-pct N] [--delay-ms N] [--clock-pct N]
use ::kv_client::raw::ReadConsistency;
use ::kv_client::{PdClient, RawClient, TxnClient};
use rkv_proto::admin_client::AdminClient;
use rkv_proto::kv_client::KvClient;
use rkv_proto::*;
use std::sync::Arc;

fn opt(a: &mut Vec<String>, name: &str) -> Option<String> {
    let i = a.iter().position(|x| x == name)?;
    a.remove(i);
    if i < a.len() {
        Some(a.remove(i))
    } else {
        None
    }
}

fn csv(s: Option<String>) -> Vec<String> {
    s.map(|s| s.split(',').filter(|x| !x.is_empty()).map(|x| x.to_string()).collect()).unwrap_or_default()
}

fn show(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

#[tokio::main]
async fn main() {
    let mut a: Vec<String> = std::env::args().skip(1).collect();
    let pd_addrs = csv(opt(&mut a, "--pd"));
    let seeds = csv(opt(&mut a, "--seeds"));
    let pd = if pd_addrs.is_empty() { None } else { Some(Arc::new(PdClient::new(pd_addrs.clone()))) };
    if a.is_empty() {
        eprintln!(
            "usage: kvctl [--pd ADDR] [--seeds ADDR,..] put|get|del|cas|scan|txn|watch|status|members|transfer-leader|split|tso|pd-config|pd-ranges|faults ..."
        );
        std::process::exit(2);
    }
    let cmd = a.remove(0);
    let need = |n: usize, a: &Vec<String>| {
        if a.len() < n {
            eprintln!("missing arguments for command");
            std::process::exit(2);
        }
    };
    let raw = || RawClient::new(pd.clone(), seeds.clone());
    let res: Result<(), String> = async {
        match cmd.as_str() {
            "put" => {
                let ttl: u64 = opt(&mut a, "--ttl-ms").map(|v| v.parse().unwrap()).unwrap_or(0);
                need(2, &a);
                raw().put_ttl(a[0].as_bytes(), a[1].as_bytes(), ttl).await.map_err(|e| e.to_string())?;
                println!("OK");
            }
            "get" => {
                let stale = opt(&mut a, "--stale-ms").map(|v| v.parse().unwrap());
                need(1, &a);
                let rc = stale.map(|ms| ReadConsistency::Stale { max_staleness_ms: ms }).unwrap_or(ReadConsistency::Linearizable);
                match raw().get_with(a[0].as_bytes(), rc).await.map_err(|e| e.to_string())? {
                    Some(v) => println!("{}", show(&v)),
                    None => println!("(not found)"),
                }
            }
            "del" => {
                need(1, &a);
                raw().delete(a[0].as_bytes()).await.map_err(|e| e.to_string())?;
                println!("OK");
            }
            "cas" => {
                need(3, &a);
                let exp = if a[1] == "-" { None } else { Some(a[1].as_bytes().to_vec()) };
                let ok = raw().cas(a[0].as_bytes(), exp.as_deref(), a[2].as_bytes()).await.map_err(|e| e.to_string())?;
                println!("{}", if ok { "swapped" } else { "not swapped" });
            }
            "scan" => {
                need(2, &a);
                let limit = a.get(2).map(|v| v.parse().unwrap()).unwrap_or(0);
                for (k, v) in raw().scan(a[0].as_bytes(), a[1].as_bytes(), limit).await.map_err(|e| e.to_string())? {
                    println!("{} = {}", show(&k), show(&v));
                }
            }
            "txn" => {
                let pd = pd.clone().ok_or("txn needs --pd")?;
                let tc = TxnClient::new(pd, seeds.clone());
                need(1, &a);
                let sub = a.remove(0);
                let mut t = tc.begin().await.map_err(|e| e.to_string())?;
                if sub == "put" {
                    for kv in a.chunks(2) {
                        t.put(kv[0].as_bytes(), kv.get(1).ok_or("odd number of arguments")?.as_bytes());
                    }
                    let ts = t.commit().await.map_err(|e| e.to_string())?;
                    println!("committed at ts {ts}");
                } else {
                    for k in &a {
                        match t.get(k.as_bytes()).await.map_err(|e| e.to_string())? {
                            Some(v) => println!("{k} = {}", show(&v)),
                            None => println!("{k} = (not found)"),
                        }
                    }
                }
            }
            "watch" => {
                need(2, &a);
                let c = raw();
                let info = c.router.locate(a[0].as_bytes()).await.map_err(|e| e.to_string())?;
                let store = info.peers.iter().find(|p| p.store_id == info.leader_store).or(info.peers.first()).ok_or("no peers")?;
                let mut kv = KvClient::new(::kv_client::pd::channel(&store.addr)?);
                let ctx = Context { range_id: info.id, version: 0, read_mode: 0, max_staleness_ms: 0 };
                let mut s = kv
                    .watch(WatchReq { ctx: Some(ctx), start: a[0].as_bytes().to_vec(), end: a[1].as_bytes().to_vec() })
                    .await
                    .map_err(|e| e.to_string())?
                    .into_inner();
                while let Some(ev) = s.message().await.map_err(|e| e.to_string())? {
                    println!("[{}] {} {} {}", ev.index, if ev.deleted { "DEL" } else { "PUT" }, show(&ev.key), show(&ev.value));
                }
            }
            "status" => {
                let mut addrs = seeds.clone();
                if let Some(pd) = &pd {
                    for s in pd.all_ranges().await?.stores {
                        addrs.push(s.addr);
                    }
                }
                for addr in addrs {
                    match AdminClient::new(::kv_client::pd::channel(&addr)?).status(Empty {}).await {
                        Ok(r) => {
                            let r = r.into_inner();
                            println!("store {} @ {}", r.store_id, r.addr);
                            for rs in r.ranges {
                                let i = rs.info.unwrap();
                                println!(
                                    "  range {:<14} [{:?}, {:?}) v{} {} term={} commit={} applied={} bytes={} peers={:?}",
                                    i.id,
                                    show(&i.start),
                                    show(&i.end),
                                    i.version,
                                    if rs.is_leader { "LEADER" } else { "follower" },
                                    rs.term,
                                    rs.commit,
                                    rs.applied,
                                    rs.approx_bytes,
                                    i.peers.iter().map(|p| format!("{}{}", p.store_id, if p.learner { "L" } else { "" })).collect::<Vec<_>>()
                                );
                            }
                        }
                        Err(e) => println!("store @ {addr}: unreachable ({})", e.message()),
                    }
                }
            }
            "members" => {
                let range: u64 = a.first().ok_or("range id")?.parse().unwrap();
                let voters: Vec<u64> = csv(opt(&mut a, "--voters")).iter().map(|x| x.parse().unwrap()).collect();
                let learners: Vec<u64> = csv(opt(&mut a, "--learners")).iter().map(|x| x.parse().unwrap()).collect();
                for addr in all_store_addrs(&pd, &seeds).await? {
                    let r = AdminClient::new(::kv_client::pd::channel(&addr)?)
                        .change_membership(MembershipReq { range_id: range, voters: voters.clone(), learners: learners.clone() })
                        .await
                        .map_err(|e| e.to_string())?
                        .into_inner();
                    if r.error.is_none() {
                        println!("{}", r.msg);
                        return Ok(());
                    }
                }
                return Err("no store accepted the change (is the range leader reachable?)".into());
            }
            "transfer-leader" => {
                need(2, &a);
                let (range, to): (u64, u64) = (a[0].parse().unwrap(), a[1].parse().unwrap());
                for addr in all_store_addrs(&pd, &seeds).await? {
                    let r = AdminClient::new(::kv_client::pd::channel(&addr)?)
                        .transfer_leader(TransferLeaderReq { range_id: range, to_store: to })
                        .await
                        .map_err(|e| e.to_string())?
                        .into_inner();
                    if r.error.is_none() {
                        println!("{}", r.msg);
                        return Ok(());
                    }
                }
                return Err("no leader accepted the transfer".into());
            }
            "split" => {
                need(1, &a);
                let range: u64 = a[0].parse().unwrap();
                let key = a.get(1).map(|k| k.as_bytes().to_vec()).unwrap_or_default();
                for addr in all_store_addrs(&pd, &seeds).await? {
                    let r = AdminClient::new(::kv_client::pd::channel(&addr)?)
                        .split_range(SplitReq { range_id: range, split_key: key.clone() })
                        .await
                        .map_err(|e| e.to_string())?
                        .into_inner();
                    if r.error.is_none() {
                        println!("{}", r.msg);
                        return Ok(());
                    }
                }
                return Err("split failed on every store (is the key valid / leader reachable?)".into());
            }
            "lease" => {
                let lc = ::kv_client::LeaseClient::new(std::sync::Arc::new(raw()));
                need(2, &a);
                let sub = a.remove(0);
                match sub.as_str() {
                    "acquire" => {
                        need(3, &a);
                        match lc.acquire(&a[0], &a[1], a[2].parse().unwrap()).await.map_err(|e| e.to_string())? {
                            Some(l) => println!("acquired {} as {} fencing token {}", l.name, l.owner, l.token),
                            None => println!("held by someone else"),
                        }
                    }
                    "holder" => match lc.holder(&a[0]).await.map_err(|e| e.to_string())? {
                        Some((o, t)) => println!("{o} (token {t})"),
                        None => println!("free"),
                    },
                    "renew" | "release" => {
                        need(4, &a);
                        let l = ::kv_client::Lease { name: a[0].clone(), owner: a[1].clone(), token: a[2].parse().unwrap(), ttl_ms: a[3].parse().unwrap() };
                        let ok = if sub == "renew" { lc.renew(&l).await } else { lc.release(&l).await }.map_err(|e| e.to_string())?;
                        println!("{}", if ok { "ok" } else { "lease no longer held" });
                    }
                    _ => return Err("lease acquire|holder|renew|release".into()),
                }
            }
            "merge" => {
                need(1, &a);
                let left: u64 = a[0].parse().unwrap();
                let addrs = all_store_addrs(&pd, &seeds).await?;
                // find the right neighbour from any store's view
                let mut infos = vec![];
                for addr in &addrs {
                    if let Ok(r) = AdminClient::new(::kv_client::pd::channel(addr)?).status(Empty {}).await {
                        infos.extend(r.into_inner().ranges.into_iter().filter_map(|x| x.info));
                    }
                }
                let l = infos.iter().find(|i| i.id == left).ok_or("unknown left range")?.clone();
                let right = infos.iter().find(|i| i.start == l.end && !l.end.is_empty()).ok_or("left range has no right neighbour")?.clone();
                let call = |phase: u32, fi: u64, re: Vec<u8>| {
                    let addrs = addrs.clone();
                    async move {
                        let mut last = String::from("no stores");
                        for _attempt in 0..20 {
                            for addr in &addrs {
                                let ch = ::kv_client::pd::channel(addr).map_err(|e| e.to_string())?;
                                match AdminClient::new(ch)
                                    .merge_range(MergeReq { left_id: left, right_id: right.id, phase, final_index: fi, right_end: re.clone() })
                                    .await
                                {
                                    Ok(r) => {
                                        let r = r.into_inner();
                                        match r.error {
                                            None => return Ok::<_, String>(r),
                                            Some(e) => last = format!("{} (code {})", e.msg, e.code),
                                        }
                                    }
                                    Err(e) => last = e.message().to_string(),
                                }
                            }
                            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                        }
                        Err(last)
                    }
                };
                let p1 = call(1, 0, vec![]).await.map_err(|e| format!("phase 1 (freeze right range) failed: {e}"))?;
                match call(2, p1.final_index, p1.right_end.clone()).await {
                    Ok(_) => println!("merged range {} into {}", right.id, left),
                    Err(e) => {
                        let _ = call(3, 0, vec![]).await;
                        return Err(format!("phase 2 failed ({e}); merge aborted and the right range unfrozen"));
                    }
                }
            }
            "presplit" => {
                let c = raw();
                for k in &a {
                    let info = c.router.locate(k.as_bytes()).await.map_err(|e| e.to_string())?;
                    if info.start == k.as_bytes() {
                        continue; // already a boundary
                    }
                    let mut done = false;
                    // a freshly split child may not have elected a leader yet: retry for a few seconds
                    for _attempt in 0..20 {
                        for addr in all_store_addrs(&pd, &seeds).await? {
                            let r = AdminClient::new(::kv_client::pd::channel(&addr)?)
                                .split_range(SplitReq { range_id: info.id, split_key: k.as_bytes().to_vec() })
                                .await
                                .map_err(|e| e.to_string())?
                                .into_inner();
                            if r.error.is_none() {
                                done = true;
                                break;
                            }
                        }
                        if done {
                            break;
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                        c.router.refresh_all().await.map_err(|e| e.to_string())?;
                    }
                    if !done {
                        return Err(format!("could not split at {k}"));
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(400)).await; // let the child group elect
                    c.router.refresh_all().await.map_err(|e| e.to_string())?;
                }
                println!("presplit done");
            }
            "tso" => println!("{}", pd.clone().ok_or("needs --pd")?.tso_fresh().await?),
            "pd-config" => {
                need(2, &a);
                let r = pd.clone().ok_or("needs --pd")?.set_config(&a[0], &a[1]).await?;
                println!("{}", r.msg);
            }
            "pd-ranges" => {
                let r = pd.clone().ok_or("needs --pd")?.all_ranges().await?;
                for s in r.stores {
                    println!("store {} {} alive={} ranges={} leaders={}", s.id, s.addr, s.alive, s.range_count, s.leader_count);
                }
                for i in r.ranges {
                    println!(
                        "range {} [{:?}, {:?}) v{} leader={} peers={:?}",
                        i.id,
                        show(&i.start),
                        show(&i.end),
                        i.version,
                        i.leader_store,
                        i.peers.iter().map(|p| format!("{}{}", p.store_id, if p.learner { "L" } else { "" })).collect::<Vec<_>>()
                    );
                }
            }
            "faults" => {
                let block: Vec<u64> = csv(opt(&mut a, "--block")).iter().map(|x| x.parse().unwrap()).collect();
                let drop_pct: u32 = opt(&mut a, "--drop-pct").map(|v| v.parse().unwrap()).unwrap_or(0);
                let delay_ms: u32 = opt(&mut a, "--delay-ms").map(|v| v.parse().unwrap()).unwrap_or(0);
                let clock: u32 = opt(&mut a, "--clock-pct").map(|v| v.parse().unwrap()).unwrap_or(100);
                need(1, &a);
                let r = AdminClient::new(::kv_client::pd::channel(&a[0])?)
                    .set_faults(FaultsReq { blocked_stores: block, drop_pct, delay_ms, clock_rate_pct: clock })
                    .await
                    .map_err(|e| e.to_string())?
                    .into_inner();
                println!("{}", r.msg);
            }
            other => return Err(format!("unknown command {other}")),
        }
        Ok(())
    }
    .await;
    if let Err(e) = res {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

async fn all_store_addrs(pd: &Option<Arc<PdClient>>, seeds: &[String]) -> Result<Vec<String>, String> {
    let mut v: Vec<String> = seeds.to_vec();
    if let Some(pd) = pd {
        v.extend(pd.all_ranges().await?.stores.into_iter().map(|s| s.addr));
    }
    if v.is_empty() {
        return Err("give --pd or --seeds".into());
    }
    Ok(v)
}
