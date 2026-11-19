#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use parqtel_sink::{
    write_metric_block, BlockSpec, DataPoint, LabelSet, MetricKind, MetricValue, SeriesData,
    WriterConfig,
};
use parquet::basic::Compression;
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
            resource: labels(&[("service.name", "checkout")]),
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

    let row_group = metadata.row_group(0);
    assert_eq!(row_group.num_rows(), 2);
    let mut bloom_columns = BTreeSet::new();
    for column in row_group.columns() {
        assert!(matches!(column.compression(), Compression::ZSTD(_)));
        assert!(column.statistics().is_some(), "page statistics missing");
        if column.bloom_filter_offset().is_some() {
            bloom_columns.insert(column.column_path().string());
        }
    }
    // Upstream Parqtel calls the global `set_bloom_filter_fpp`, which
    // implicitly enables bloom filters for every column even though the
    // per-column settings only name metric_name/service_name. Match upstream
    // exactly rather than silently changing the block layout.
    assert!(bloom_columns.contains("metric_name"));
    assert!(bloom_columns.contains("service_name"));

    let batch = reader.build().unwrap().next().unwrap().unwrap();
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.schema().field(0).name(), "timestamp_ns");
    assert!(matches!(
        batch.schema().field(1).data_type(),
        arrow::datatypes::DataType::Dictionary(_, _)
    ));
}

#[test]
fn groups_sorted_metrics_into_exact_row_groups() {
    let tmp = tempfile::tempdir().unwrap();
    let point = |metric: &str, ts: i64, value: f64| SeriesData {
        metric_name: metric.into(),
        kind: MetricKind::Gauge,
        resource: LabelSet::default(),
        points: vec![
            DataPoint::new(ts, MetricValue::Double(value), labels(&[("job", "api")])).unwrap(),
        ],
    };
    let block = write_metric_block(
        tmp.path(),
        &BlockSpec {
            source_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".into(),
            window_start_ns: 1,
            window_end_ns: 2,
            part: 0,
        },
        vec![
            point("z_metric", 2_000_000_000, 2.0),
            point("a_metric", 1_000_000_000, 1.0),
        ],
        &WriterConfig {
            row_group_size: 1,
            ..WriterConfig::default()
        },
    )
    .unwrap();

    let file = std::fs::File::open(tmp.path().join(&block.filename)).unwrap();
    let metadata = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .metadata()
        .clone();
    assert_eq!(metadata.num_row_groups(), 2);
    let expected = ["a_metric", "z_metric"];
    for (index, name) in expected.iter().enumerate() {
        let column = metadata.row_group(index).column(1);
        let stats = column.statistics().unwrap();
        assert_eq!(stats.min_bytes_opt(), stats.max_bytes_opt());
        let _ = name;
        assert_eq!(
            stats
                .min_bytes_opt()
                .map(|bytes| String::from_utf8_lossy(bytes).to_string()),
            Some((*name).to_string())
        );
    }
}
