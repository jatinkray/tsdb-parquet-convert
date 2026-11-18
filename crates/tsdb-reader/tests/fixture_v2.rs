#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::path::PathBuf;
use tsdb_reader::{Block, StaleMarkerPolicy};

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/testblock")
}

#[test]
fn reads_real_index_v2_block() {
    let block = Block::open(&fixture()).unwrap();
    assert_eq!(block.index.version(), 2);
    assert_eq!(block.meta.stats.num_series, 35_354);
    assert_eq!(block.series().len(), 35_354);
    assert!(block.series()[0].metric_name().is_some());

    let mut total_samples = 0usize;
    let mut total_chunks = 0usize;
    for series in block.series() {
        total_chunks += series.chunks.len();
    }
    assert_eq!(total_chunks, 37_020);

    // Decode one full series and assert timestamps are strictly increasing.
    let samples = block
        .read_series(
            &block.series()[0],
            i64::MIN,
            i64::MAX,
            StaleMarkerPolicy::Keep,
        )
        .unwrap();
    assert!(!samples.is_empty());
    assert!(samples
        .windows(2)
        .all(|pair| pair[0].timestamp_ms <= pair[1].timestamp_ms));
    total_samples += samples.len();
    assert!(total_samples > 0);
}

#[test]
fn scans_a_limited_time_window() {
    let mut options = tsdb_reader::SourceOptions::new(fixture());
    options.min_time_ms = Some(1_650_005_000_000);
    options.max_time_ms = Some(1_650_006_000_000);
    let source = tsdb_reader::Source::open(options).unwrap();
    let mut series = 0usize;
    let mut samples = 0u64;
    let stats = source
        .scan(|record| {
            series += 1;
            samples += record.samples.len() as u64;
            assert!(record.metric_name().is_some());
            Ok(())
        })
        .unwrap();
    assert_eq!(stats.series, series);
    assert_eq!(stats.samples, samples);
    assert!(samples > 0);
}
