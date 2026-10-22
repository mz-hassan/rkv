#![no_main]
//! Protobuf request decoders (client-facing API) must reject garbage without panicking.
use libfuzzer_sys::fuzz_target;
use prost::Message;
use rkv_proto::*;

fuzz_target!(|data: &[u8]| {
    let _ = GetReq::decode(data);
    let _ = PutReq::decode(data);
    let _ = CasReq::decode(data);
    let _ = ScanReq::decode(data);
    let _ = PrewriteReq::decode(data);
    let _ = CommitReq::decode(data);
    let _ = RaftBatch::decode(data);
    let _ = HeartbeatReq::decode(data);
});
