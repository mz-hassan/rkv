//! Minimal HTTP endpoint: /healthz, /readyz, /metrics (Prometheus text format).
use crate::store::Store;
use raft::Role;
use std::fmt::Write;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

pub async fn serve(store: Arc<Store>, addr: String) {
    let Ok(l) = TcpListener::bind(&addr).await else {
        tracing::error!("cannot bind metrics address {addr}");
        return;
    };
    loop {
        let Ok((mut sock, _)) = l.accept().await else { continue };
        let store = store.clone();
        tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            let n = sock.read(&mut buf).await.unwrap_or(0);
            let req = String::from_utf8_lossy(&buf[..n]).to_string();
            let path = req.split_whitespace().nth(1).unwrap_or("/").to_string();
            let (code, body) = match path.as_str() {
                "/healthz" => ("200 OK", "ok\n".to_string()),
                "/readyz" => {
                    let mut ready = false;
                    for r in store.all_replicas() {
                        if let Some(st) = r.handle.status().await {
                            if st.leader.is_some() {
                                ready = true;
                            }
                        }
                    }
                    if ready {
                        ("200 OK", "ready\n".to_string())
                    } else {
                        ("503 Service Unavailable", "no leader known\n".to_string())
                    }
                }
                "/metrics" => ("200 OK", metrics(&store).await),
                _ => ("404 Not Found", "not found\n".to_string()),
            };
            let resp =
                format!("HTTP/1.1 {code}\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
            let _ = sock.write_all(resp.as_bytes()).await;
        });
    }
}

async fn metrics(s: &Store) -> String {
    let mut o = String::new();
    let sid = s.cfg.store_id;
    let _ = writeln!(o, "# TYPE rkv_requests_total counter\nrkv_requests_total{{store=\"{sid}\"}} {}", s.metrics.requests.load(Ordering::Relaxed));
    let _ = writeln!(o, "# TYPE rkv_proposals_total counter\nrkv_proposals_total{{store=\"{sid}\"}} {}", s.metrics.proposals.load(Ordering::Relaxed));
    let _ = writeln!(o, "# TYPE rkv_raft_msgs_sent_total counter\nrkv_raft_msgs_sent_total{{store=\"{sid}\"}} {}", s.transport.sent.load(Ordering::Relaxed));
    let _ = writeln!(
        o,
        "# TYPE rkv_raft_msgs_dropped_total counter\nrkv_raft_msgs_dropped_total{{store=\"{sid}\"}} {}",
        s.transport.dropped.load(Ordering::Relaxed)
    );
    let m = s.db.metrics();
    let g = |a: &std::sync::atomic::AtomicU64| a.load(Ordering::Relaxed);
    let _ = writeln!(o, "rkv_engine_user_bytes_total{{store=\"{sid}\"}} {}", g(&m.user_bytes));
    let _ = writeln!(o, "rkv_engine_wal_bytes_total{{store=\"{sid}\"}} {}", g(&m.wal_bytes));
    let _ = writeln!(o, "rkv_engine_flush_bytes_total{{store=\"{sid}\"}} {}", g(&m.flush_bytes));
    let _ = writeln!(o, "rkv_engine_compaction_write_bytes_total{{store=\"{sid}\"}} {}", g(&m.compact_write));
    let _ = writeln!(o, "rkv_engine_stall_micros_total{{store=\"{sid}\"}} {}", g(&m.stall_micros));
    let st = s.db.stats();
    let _ = writeln!(o, "rkv_engine_block_cache_hit_ratio{{store=\"{sid}\"}} {:.4}", st.cache_hit_ratio);
    let _ = writeln!(o, "rkv_engine_write_amplification{{store=\"{sid}\"}} {:.3}", st.write_amp);
    for (l, n) in st.level_files.iter().enumerate() {
        let _ = writeln!(o, "rkv_engine_level_files{{store=\"{sid}\",level=\"{l}\"}} {n}");
    }
    for r in s.all_replicas() {
        if let Some(t) = r.handle.status().await {
            let rid = r.id;
            let _ = writeln!(o, "rkv_range_is_leader{{store=\"{sid}\",range=\"{rid}\"}} {}", (t.role == Role::Leader) as u8);
            let _ = writeln!(o, "rkv_range_term{{store=\"{sid}\",range=\"{rid}\"}} {}", t.term);
            let _ = writeln!(o, "rkv_range_commit_index{{store=\"{sid}\",range=\"{rid}\"}} {}", t.commit);
            let _ = writeln!(o, "rkv_range_applied_index{{store=\"{sid}\",range=\"{rid}\"}} {}", t.applied);
            let _ = writeln!(o, "rkv_range_approx_bytes{{store=\"{sid}\",range=\"{rid}\"}} {}", r.sm.approx_bytes.load(Ordering::Relaxed));
            let _ = writeln!(o, "rkv_range_writes_total{{store=\"{sid}\",range=\"{rid}\"}} {}", r.sm.writes.load(Ordering::Relaxed));
        }
    }
    o
}
