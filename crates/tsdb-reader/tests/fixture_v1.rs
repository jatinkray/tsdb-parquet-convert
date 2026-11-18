#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::path::PathBuf;
use tsdb_reader::{Block, StaleMarkerPolicy};

#[test]
fn reads_real_index_v1_block_and_chunks() {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/index_format_v1");
    let block = Block::open(&dir).unwrap();
    assert_eq!(block.meta.ulid, "01DXXFZDYD1MQW6079WK0K6EDQ");
    assert_eq!(block.series().len(), 102);
    assert_eq!(block.series()[0].chunks.len(), 1);

    let mut total = 0usize;
    for series in block.series() {
        let samples = block
            .read_series(series, i64::MIN, i64::MAX, StaleMarkerPolicy::Keep)
            .unwrap();
        assert_eq!(samples.len(), 1, "fixture has one sample per series");
        total += samples.len();
    }
    assert_eq!(total, 102);
}

#[test]
fn resolves_v1_absolute_symbol_offsets() {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/index_format_v1");
    let block = Block::open(&dir).unwrap();
    let first = &block.series()[0];
    // V1 series carry absolute file offsets as series IDs and symbol refs.
    assert_eq!(first.id, 323);
    assert_eq!(first.labels.get("bar").map(String::as_str), Some("0"));
}
