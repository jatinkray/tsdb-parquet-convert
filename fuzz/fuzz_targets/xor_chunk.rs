#![no_main]

use libfuzzer_sys::fuzz_target;
use std::path::Path;

fuzz_target!(|data: &[u8]| {
    let _ = tsdb_reader::chunks::decode_xor(Path::new("fuzz"), data);
});
