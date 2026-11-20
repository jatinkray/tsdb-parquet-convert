//! Writes a schema-driven file and reads it back through the Parquet Arrow reader.

use std::collections::BTreeMap;
use std::error::Error;
use std::fs::File;
use std::str::FromStr;

use arrow_array::{
    Array, ArrayRef, Decimal128Array, Float64Array, Int64Array, StringArray,
    TimestampMillisecondArray,
};
use arrow_schema::{DataType, TimeUnit};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet_sink::{block_filename, BlockWriter, SchemaSpec, SeriesInput, WriteOptions};

const SPEC: &str = r#"
name: tsdb_labelled
columns:
  - name: timestamp
    type: timestamp_ms
    source: timestamp
    timezone: UTC
  - name: metric
    type: utf8
    source: metric_name
  - name: job
    type: utf8
    source: label:job
    nullable: true
  - name: region
    type: utf8
    source: "const:eu-north-1"
  - name: value
    type: float64
    source: value
  - name: series
    type: int64
    source: series_index
  - name: cost
    type: decimal128
    source: value
    precision: 12
    scale: 2
"#;

fn labels(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
        .collect()
}

fn read_back(path: &std::path::Path) -> Result<arrow_array::RecordBatch, Box<dyn Error>> {
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let schema = builder.schema().clone();
    assert_eq!(schema.fields().len(), 7);
    assert_eq!(
        schema.field(0).data_type(),
        &DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
    );
    assert!(schema
        .field(1)
        .metadata()
        .contains_key("tsdb2parqtel.source"));
    let mut batches = Vec::new();
    for batch in builder.build()? {
        batches.push(batch?);
    }
    assert_eq!(batches.len(), 1);
    let batch = batches.remove(0);
    Ok(batch)
}

fn column(batch: &arrow_array::RecordBatch, index: usize) -> &ArrayRef {
    batch.column(index)
}

#[test]
fn writes_declared_columns_and_nulls() -> Result<(), Box<dyn Error>> {
    let spec = SchemaSpec::from_str(SPEC)?;
    let mut writer = BlockWriter::new(spec, WriteOptions::default())?;
    let first = labels(&[("__name__", "up"), ("job", "api")]);
    let second = labels(&[("__name__", "up"), ("instance", "a:9090")]);
    writer.push_series(SeriesInput {
        block_ulid: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        series_fingerprint: 11,
        series_index: 0,
        metric_name: Some("up"),
        labels: &first,
        samples: &[
            (1_700_000_000_000_000_000, 1.5),
            (1_700_000_500_000_000_000, 2.5),
        ],
    })?;
    writer.push_series(SeriesInput {
        block_ulid: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        series_fingerprint: 12,
        series_index: 1,
        metric_name: Some("up"),
        labels: &second,
        samples: &[(1_700_001_000_000_000_000, 0.25)],
    })?;
    assert_eq!(writer.rows(), 3);

    let dir = tempfile::tempdir()?;
    let path = dir.path().join(block_filename(0, 7_200_000_000_000, 0));
    let info = writer.write(&path)?;
    assert_eq!(info.rows, 3);
    assert!(info.bytes > 0);
    assert_eq!(info.filename, "data_0_7200000000000_00000.parquet");

    let batch = read_back(&path)?;
    assert_eq!(batch.num_rows(), 3);

    let timestamps = column(&batch, 0)
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .ok_or("timestamp column type")?;
    assert_eq!(timestamps.value(0), 1_700_000_000_000);
    assert_eq!(timestamps.value(2), 1_700_001_000_000);

    let metrics = column(&batch, 1)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or("metric column type")?;
    assert_eq!(metrics.value(0), "up");

    let jobs = column(&batch, 2)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or("job column type")?;
    assert_eq!(jobs.value(0), "api");
    assert!(jobs.is_null(2), "series without a job label must be NULL");

    let regions = column(&batch, 3)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or("region column type")?;
    assert_eq!(regions.value(1), "eu-north-1");

    let values = column(&batch, 4)
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or("value column type")?;
    assert_eq!(values.value(0), 1.5);
    assert_eq!(values.value(1), 2.5);

    let series = column(&batch, 5)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or("series column type")?;
    assert_eq!(series.values(), &[0, 0, 1]);

    let costs = column(&batch, 6)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or("decimal column type")?;
    assert_eq!(costs.value(0), 150);
    assert_eq!(costs.value(1), 250);
    assert_eq!(costs.value(2), 25);

    Ok(())
}

#[test]
fn rejects_missing_value_for_non_nullable_label() -> Result<(), Box<dyn Error>> {
    let spec = SchemaSpec::from_str(
        "columns:\n  - name: ts\n    type: timestamp_ms\n    source: timestamp\n  - name: job\n    type: utf8\n    source: label:job\n",
    )?;
    let mut writer = BlockWriter::new(spec, WriteOptions::default())?;
    let labels = labels(&[("__name__", "up")]);
    let error = writer
        .push_series(SeriesInput {
            block_ulid: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            series_fingerprint: 99,
            series_index: 0,
            metric_name: Some("up"),
            labels: &labels,
            samples: &[(1_700_000_000_000_000_000, 1.0)],
        })
        .err()
        .ok_or("expected an error for the missing label")?;
    let message = error.to_string();
    assert!(message.contains("`job`"), "{message}");
    assert!(message.contains("99"), "{message}");
    assert!(message.contains("not nullable"), "{message}");
    assert_eq!(writer.rows(), 0);
    Ok(())
}

#[test]
fn rejects_unparseable_numeric_label() -> Result<(), Box<dyn Error>> {
    let spec = SchemaSpec::from_str(
        "columns:\n  - name: ts\n    type: timestamp_ms\n    source: timestamp\n  - name: le\n    type: float64\n    source: label:le\n",
    )?;
    let mut writer = BlockWriter::new(spec, WriteOptions::default())?;
    let labels = labels(&[("__name__", "http_requests_bucket"), ("le", "half")]);
    let error = writer
        .push_series(SeriesInput {
            block_ulid: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            series_fingerprint: 5,
            series_index: 0,
            metric_name: Some("http_requests_bucket"),
            labels: &labels,
            samples: &[(1_700_000_000_000_000_000, 3.0)],
        })
        .err()
        .ok_or("expected an error for `half`")?;
    assert!(error.to_string().contains("not a number"), "{error}");
    Ok(())
}

#[test]
fn schema_is_embedded_in_file_metadata() -> Result<(), Box<dyn Error>> {
    let spec = SchemaSpec::from_str(SPEC)?;
    let mut writer = BlockWriter::new(spec, WriteOptions::default())?;
    let labels = labels(&[("__name__", "up")]);
    writer.push_series(SeriesInput {
        block_ulid: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        series_fingerprint: 1,
        series_index: 0,
        metric_name: Some("up"),
        labels: &labels,
        samples: &[(1_700_000_000_000_000_000, 1.0)],
    })?;
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("data.parquet");
    writer.write(&path)?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&path)?)?;
    let metadata = builder.metadata();
    let file_metadata = metadata.file_metadata();
    let key_values = file_metadata
        .key_value_metadata()
        .ok_or("missing metadata")?;
    let schema_json = key_values
        .iter()
        .find(|entry| entry.key == "tsdb2parqtel.schema")
        .map(|entry| entry.value.clone().unwrap_or_default())
        .ok_or("missing embedded schema")?;
    let parsed: SchemaSpec = serde_json::from_str(&schema_json)?;
    assert_eq!(parsed.name.as_deref(), Some("tsdb_labelled"));
    assert_eq!(parsed.columns.len(), 7);
    Ok(())
}

#[test]
fn refuses_empty_file() -> Result<(), Box<dyn Error>> {
    let spec = SchemaSpec::from_str(SPEC)?;
    let writer = BlockWriter::new(spec, WriteOptions::default())?;
    let dir = tempfile::tempdir()?;
    assert!(writer.write(&dir.path().join("empty.parquet")).is_err());
    Ok(())
}
