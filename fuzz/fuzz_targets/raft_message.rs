#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Some(m) = raft::codec::decode_message(data) {
        let enc = raft::codec::encode_message(&m);
        assert_eq!(raft::codec::decode_message(&enc), Some(m));
    }
});
