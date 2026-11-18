#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::path::PathBuf;
use tsdb_reader::{Block, StaleMarkerPolicy};

#[test]
fn native_histogram_chunks_decode_and_are_reported_as_unrepresentable() {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/native_hist");
    let block = Block::open(&dir).unwrap();
    let mut hist_errors = 0usize;
    let mut floats = 0usize;
    for series in block.series() {
        match block.read_series(series, i64::MIN, i64::MAX, StaleMarkerPolicy::Keep) {
            Ok(samples) => floats += samples.len(),
            Err(error) => {
                let text = error.to_string();
                assert!(
                    text.contains("native histogram"),
                    "unexpected error: {text}"
                );
                hist_errors += 1;
            }
        }
    }
    assert!(hist_errors >= 2, "fixture should contain histogram series");
    assert!(floats > 0, "fixture should also contain float series");
}
