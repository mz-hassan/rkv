pub mod cmd;
pub mod http;
pub mod service;
pub mod sm;
pub mod store;
pub mod transport;

use rkv_proto::admin_server::AdminServer;
use rkv_proto::kv_server::KvServer;
use rkv_proto::raft_transport_server::RaftTransportServer;
use std::net::SocketAddr;
use std::sync::Arc;

pub use store::{Store, StoreConfig};

/// Serve all gRPC services of a store on `addr` until the future is dropped.
pub async fn serve(store: Arc<Store>, addr: SocketAddr) -> Result<(), tonic::transport::Error> {
    let max = 256 << 20;
    tonic::transport::Server::builder()
        .tcp_nodelay(true)
        .add_service(KvServer::new(service::KvSvc(store.clone())).max_decoding_message_size(max).max_encoding_message_size(max))
        .add_service(RaftTransportServer::new(service::RaftSvc(store.clone())).max_decoding_message_size(max))
        .add_service(AdminServer::new(service::AdminSvc(store)))
        .serve(addr)
        .await
}
