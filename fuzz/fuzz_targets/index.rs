#![no_main]

use libfuzzer_sys::fuzz_target;
use std::path::Path;

fuzz_target!(|data: &[u8]| {
    let _ = tsdb_reader::Index::parse(Path::new("fuzz"), data);
});
