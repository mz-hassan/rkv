#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(edits) = kv_engine::version::decode_edits(data) {
        // anything that decodes must re-encode to something that decodes to the same edits
        let again = kv_engine::version::decode_edits(&kv_engine::version::encode_edits(&edits)).unwrap();
        assert_eq!(edits, again);
    }
});
