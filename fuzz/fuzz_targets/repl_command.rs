#![no_main]
use kv_server::cmd::{Cmd, Resp};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Some(c) = Cmd::decode(data) {
        assert_eq!(Cmd::decode(&c.encode()), Some(c));
    }
    if let Some(r) = Resp::decode(data) {
        assert_eq!(Resp::decode(&r.encode()), Some(r));
    }
});
