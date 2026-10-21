//! gRPC transport for Raft messages: one coalescing sender task per peer store.
use parking_lot::{Mutex, RwLock};
use raft::Message;
use raft_driver::Transport;
use rkv_proto::raft_transport_client::RaftTransportClient;
use rkv_proto::{RaftBatch, RaftMsg};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tonic::transport::Endpoint;

pub type AddrBook = Arc<RwLock<HashMap<u64, String>>>;

pub struct GrpcTransport {
    self_store: u64,
    addrs: AddrBook,
    senders: Mutex<HashMap<u64, mpsc::Sender<RaftMsg>>>,
    pub dropped: Arc<AtomicU64>,
    pub sent: Arc<AtomicU64>,
}

impl GrpcTransport {
    pub fn new(self_store: u64, addrs: AddrBook) -> Arc<GrpcTransport> {
        Arc::new(GrpcTransport { self_store, addrs, senders: Mutex::new(HashMap::new()), dropped: Arc::new(AtomicU64::new(0)), sent: Arc::new(AtomicU64::new(0)) })
    }

    fn sender(&self, to: u64) -> Option<mpsc::Sender<RaftMsg>> {
        let mut s = self.senders.lock();
        if let Some(tx) = s.get(&to) {
            if !tx.is_closed() {
                return Some(tx.clone());
            }
        }
        let addr = self.addrs.read().get(&to).cloned()?;
        let (tx, mut rx) = mpsc::channel::<RaftMsg>(8192);
        let (dropped, sent, from) = (self.dropped.clone(), self.sent.clone(), self.self_store);
        let addrs = self.addrs.clone();
        tokio::spawn(async move {
            let mut cur_addr = addr;
            let mut client: Option<RaftTransportClient<tonic::transport::Channel>> = None;
            while let Some(first) = rx.recv().await {
                let mut batch = vec![first];
                while batch.len() < 256 {
                    match rx.try_recv() {
                        Ok(m) => batch.push(m),
                        Err(_) => break,
                    }
                }
                if let Some(a) = addrs.read().get(&to).cloned() {
                    if a != cur_addr {
                        cur_addr = a;
                        client = None;
                    }
                }
                if client.is_none() {
                    let ep = Endpoint::from_shared(format!("http://{cur_addr}")).unwrap().connect_timeout(Duration::from_millis(500)).timeout(Duration::from_secs(2)).tcp_nodelay(true);
                    client = Some(RaftTransportClient::new(ep.connect_lazy()).max_decoding_message_size(256 << 20).max_encoding_message_size(256 << 20));
                }
                let n = batch.len() as u64;
                let mut req = RaftBatch { msgs: batch };
                for m in req.msgs.iter_mut() {
                    m.from_store = from;
                }
                match client.as_mut().unwrap().send(req).await {
                    Ok(_) => {
                        sent.fetch_add(n, Ordering::Relaxed);
                    }
                    Err(_) => {
                        dropped.fetch_add(n, Ordering::Relaxed);
                        client = None; // reconnect next time; Raft retransmits
                    }
                }
            }
        });
        s.insert(to, tx.clone());
        Some(tx)
    }
}

impl Transport for GrpcTransport {
    fn send(&self, group: u64, msg: Message) {
        let to = msg.to;
        let Some(tx) = self.sender(to) else {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        };
        let data = raft::codec::encode_message(&msg);
        if tx.try_send(RaftMsg { range_id: group, data, from_store: self.self_store }).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}
