#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::path::{Path, PathBuf};
use tsdb_reader::{DownsamplePolicy, ExternalLabels, Source, SourceOptions, StaleMarkerPolicy};

fn copy_block(src: &Path, dst: &Path, ulid: &str, resolution: i64) {
    std::fs::create_dir_all(dst.join("chunks")).unwrap();
    std::fs::copy(src.join("index"), dst.join("index")).unwrap();
    std::fs::copy(src.join("chunks/000001"), dst.join("chunks/000001")).unwrap();
    let original: serde_json::Value =
        serde_json::from_slice(&std::fs::read(src.join("meta.json")).unwrap()).unwrap();
    let mut meta = original.clone();
    meta["ulid"] = serde_json::Value::String(ulid.to_string());
    if resolution > 0 {
        meta["thanos"] = serde_json::json!({
            "source": "compactor",
            "external_labels": {"cluster": "prod", "replica": "a"},
            "downsample": {"resolution": resolution}
        });
    } else {
        meta["thanos"] = serde_json::json!({
            "external_labels": {"cluster": "prod"}
        });
    }
    std::fs::write(dst.join("meta.json"), serde_json::to_vec(&meta).unwrap()).unwrap();
}

fn options(root: &Path, downsampled: DownsamplePolicy) -> SourceOptions {
    let mut options = SourceOptions::new(root);
    options.downsampled = downsampled;
    options.stale_markers = StaleMarkerPolicy::Keep;
    options
}

#[test]
fn selects_raw_and_downsampled_blocks_by_policy() {
    let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/testblock");
    let temp = tempfile::tempdir().unwrap();
    copy_block(
        &fixture,
        &temp.path().join("01ARZ3NDEKTSV4RRFFQ69G5FAV"),
        "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        0,
    );
    copy_block(
        &fixture,
        &temp.path().join("01ARZ3NDEKTSV4RRFFQ69G5FAW"),
        "01ARZ3NDEKTSV4RRFFQ69G5FAW",
        300_000,
    );

    let raw = Source::open(options(temp.path(), DownsamplePolicy::Skip)).unwrap();
    assert_eq!(raw.block_count(), 1);
    assert_eq!(raw.block_infos()[0].resolution, 0);

    let downsampled = Source::open(options(temp.path(), DownsamplePolicy::Only)).unwrap();
    assert_eq!(downsampled.block_count(), 1);
    assert_eq!(downsampled.block_infos()[0].resolution, 300_000);

    let include = Source::open(options(temp.path(), DownsamplePolicy::Include)).unwrap();
    assert_eq!(include.block_count(), 2);
}

#[test]
fn external_labels_are_merged_or_ignored() {
    let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/testblock");
    let temp = tempfile::tempdir().unwrap();
    copy_block(
        &fixture,
        &temp.path().join("01ARZ3NDEKTSV4RRFFQ69G5FAV"),
        "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        0,
    );

    let mut with_labels = options(temp.path(), DownsamplePolicy::Skip);
    with_labels.external_labels = ExternalLabels::Include;
    let source = Source::open(with_labels).unwrap();
    let mut saw_cluster = false;
    source
        .scan(|record| {
            saw_cluster |= record.labels.get("cluster") == Some(&"prod".to_string());
            Ok(())
        })
        .unwrap();
    assert!(saw_cluster);

    let mut without_labels = options(temp.path(), DownsamplePolicy::Skip);
    without_labels.external_labels = ExternalLabels::Ignore;
    let source = Source::open(without_labels).unwrap();
    source
        .scan(|record| {
            assert!(!record.labels.contains_key("cluster"));
            Ok(())
        })
        .unwrap();
}

#[test]
fn replica_dedup_merges_identical_timestamp_streams() {
    let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/testblock");
    let temp = tempfile::tempdir().unwrap();
    for ulid in ["01ARZ3NDEKTSV4RRFFQ69G5FAV", "01ARZ3NDEKTSV4RRFFQ69G5FAW"] {
        copy_block(&fixture, &temp.path().join(ulid), ulid, 0);
    }

    let mut without = options(temp.path(), DownsamplePolicy::Skip);
    without.external_labels = ExternalLabels::Ignore;
    let source = Source::open(without).unwrap();
    let mut samples = 0u64;
    source
        .scan(|record| {
            samples += record.samples.len() as u64;
            Ok(())
        })
        .unwrap();

    let mut with = options(temp.path(), DownsamplePolicy::Skip);
    with.external_labels = ExternalLabels::Ignore;
    with.dedup_replica_labels = vec!["replica".to_string()];
    let source = Source::open(with).unwrap();
    let mut deduped = 0u64;
    source
        .scan(|record| {
            deduped += record.samples.len() as u64;
            Ok(())
        })
        .unwrap();

    assert_eq!(
        deduped * 2,
        samples,
        "dedup should collapse the two replicas"
    );
}
