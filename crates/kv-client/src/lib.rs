//! Client library: PD client, routing cache with leader discovery and retries,
//! raw client with exactly-once sessions, and the Percolator transaction client.
pub mod pd;
pub mod router;
pub mod raw;
pub mod txn_client;

pub use pd::PdClient;
pub use raw::RawClient;
pub use router::{ClientError, Router};
pub use txn_client::{IsolationLevel, TxnClient, Txn};

use rkv_proto::Error;

pub const ERR_NOT_LEADER: u32 = 1;
pub const ERR_RANGE_MOVED: u32 = 2;
pub const ERR_BUSY: u32 = 3;
pub const ERR_NO_RANGE: u32 = 4;
pub const ERR_UNKNOWN_OUTCOME: u32 = 5;
pub const ERR_OTHER: u32 = 6;

pub fn err(code: u32, msg: &str) -> Error {
    Error { code, msg: msg.to_string(), leader: None, ranges: vec![] }
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64
}
