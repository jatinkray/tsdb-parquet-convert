#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use parqtel_sink::{
    write_metric_block, BlockSpec, DataPoint, LabelSet, MetricKind, MetricValue, SeriesData,
    WriterConfig,
};
use std::collections::BTreeSet;

fn labels(pairs: &[(&str, &str)]) -> LabelSet {
    LabelSet::try_from_iter(
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect::<Vec<_>>(),
    )
    .unwrap()
}

#[test]
fn writes_parqtel_block_schema_and_series_footer() {
    let tmp = tempfile::tempdir().unwrap();
    let points = vec![
        DataPoint::new(
            1_700_000_000_000_000_000,
            MetricValue::Double(1.5),
            labels(&[("job", "api"), ("instance", "a")]),
        )
        .unwrap(),
        DataPoint::new(
            1_700_000_060_000_000_000,
            MetricValue::Double(2.5),
            labels(&[("job", "api"), ("instance", "a")]),
        )
        .unwrap(),
    ];
    let block = write_metric_block(
        tmp.path(),
        &BlockSpec {
            source_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".into(),
            window_start_ns: 1_700_000_000_000_000_000,
            window_end_ns: 1_700_007_200_000_000_000,
            part: 0,
        },
        vec![SeriesData {
            metric_name: "http_requests_total".into(),
            kind: MetricKind::Sum,
            resource: LabelSet::default(),
            points,
        }],
        &WriterConfig::default(),
    )
    .unwrap();

    assert!(block.filename.starts_with("m_l0_"));
    assert!(block.filename.ends_with(".parquet"));
    assert_eq!(block.metadata.row_count, 2);
    assert!(block.metadata.metric_names.contains("http_requests_total"));
    assert!(block.metadata.label_names.contains("job"));

    let file = std::fs::File::open(tmp.path().join(&block.filename)).unwrap();
    let reader =
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let metadata = reader.metadata();
    let schema = metadata.file_metadata().schema_descr();
    let names: BTreeSet<&str> = schema.columns().iter().map(|c| c.name()).collect();
    assert!(
        !names.contains("labels"),
        "new blocks use the footer series table"
    );
    assert!(names.contains("timestamp_ns"));
    assert!(names.contains("value_float"));
    assert!(names.contains("value_complex"));
    let kv = metadata.file_metadata().key_value_metadata().unwrap();
    assert!(kv.iter().any(|item| item.key == "parqtel.series_index"));
    assert_eq!(metadata.num_row_groups(), 1);

    let batch = reader.build().unwrap().next().unwrap().unwrap();
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.schema().field(0).name(), "timestamp_ns");
}
