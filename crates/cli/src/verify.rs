//! Per-series reconciliation between a TSDB source and Parqtel blocks.

use crate::labels::LabelMapper;
use anyhow::{Context, Result};
use parqtel_core::storage::SeriesTable;
use serde::Serialize;
use std::collections::BTreeMap;
use std::path::Path;
use tsdb_reader::Source;

#[derive(Debug, Clone)]
pub struct VerifyOptions {
    pub source: tsdb_reader::SourceOptions,
    pub label_mapper: LabelMapper,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct Digest {
    pub samples: u64,
    pub first_ts_ns: Option<i64>,
    pub last_ts_ns: Option<i64>,
    pub hash: u64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct VerifyReport {
    pub source_series: usize,
    pub output_series: usize,
    pub source_samples: u64,
    pub output_samples: u64,
    pub missing: Vec<String>,
    pub extra: Vec<String>,
    pub mismatched: Vec<Mismatch>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Mismatch {
    pub series: String,
    pub source: Digest,
    pub output: Digest,
}

impl VerifyReport {
    pub fn ok(&self) -> bool {
        self.missing.is_empty() && self.extra.is_empty() && self.mismatched.is_empty()
    }
}

pub fn verify(options: &VerifyOptions, output_dir: &Path) -> Result<VerifyReport> {
    let source = Source::open(options.source.clone())?;
    let mut source_digests: BTreeMap<String, Digest> = BTreeMap::new();
    source.scan(|record| {
        let Some(metric) = record.metric_name().map(str::to_string) else {
            return Ok(());
        };
        let mapped = options
            .label_mapper
            .map(&record.labels)
            .map_err(|error| tsdb_reader::Error::Source(error.to_string()))?;
        let key = series_key(&metric, mapped.labels.json_str());
        let digest = source_digests.entry(key).or_default();
        for sample in record.samples {
            let timestamp_ns = sample.timestamp_ms.checked_mul(1_000_000).ok_or(
                tsdb_reader::Error::TimestampOutOfRange {
                    timestamp_ms: sample.timestamp_ms,
                },
            )?;
            add_sample(digest, timestamp_ns, sample.value.to_bits());
        }
        Ok(())
    })?;

    let mut output_digests: BTreeMap<String, Digest> = BTreeMap::new();
    read_parquet_dir(output_dir, &mut output_digests)?;

    let mut report = VerifyReport {
        source_series: source_digests.len(),
        output_series: output_digests.len(),
        source_samples: source_digests.values().map(|digest| digest.samples).sum(),
        output_samples: output_digests.values().map(|digest| digest.samples).sum(),
        ..VerifyReport::default()
    };
    for (key, source) in &source_digests {
        match output_digests.get(key) {
            None => report.missing.push(key.clone()),
            Some(output)
                if output.samples != source.samples
                    || output.first_ts_ns != source.first_ts_ns
                    || output.last_ts_ns != source.last_ts_ns
                    || output.hash != source.hash =>
            {
                report.mismatched.push(Mismatch {
                    series: key.clone(),
                    source: source.clone(),
                    output: output.clone(),
                });
            }
            Some(_) => {}
        }
    }
    for key in output_digests.keys() {
        if !source_digests.contains_key(key) {
            report.extra.push(key.clone());
        }
    }
    Ok(report)
}

fn read_parquet_dir(dir: &Path, out: &mut BTreeMap<String, Digest>) -> Result<()> {
    let mut files = Vec::new();
    for entry in walkdir::WalkDir::new(dir).max_depth(2).into_iter() {
        let entry = entry?;
        if entry.file_type().is_file()
            && entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "parquet")
        {
            files.push(entry.path().to_path_buf());
        }
    }
    files.sort();
    for path in files {
        read_parquet_block(&path, out)
            .with_context(|| format!("reading output block {}", path.display()))?;
    }
    Ok(())
}

fn read_parquet_block(path: &Path, out: &mut BTreeMap<String, Digest>) -> Result<()> {
    let file = std::fs::File::open(path)?;
    let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)?;
    let table_text = builder
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .and_then(|items| items.iter().find(|item| item.key == "parqtel.series_index"))
        .and_then(|item| item.value.as_deref())
        .context("output block has no parqtel.series_index footer")?;
    let table = SeriesTable::from_footer_value(table_text)
        .map_err(|e| anyhow::anyhow!("parsing series index: {e}"))?;
    let mut starts = Vec::new();
    for run_index in 0..table.metric_run_count() {
        let run = table
            .metric_run(run_index)
            .context("invalid metric run in series index")?;
        for offset in 0..run.series_count {
            let entry = table
                .series_entry(run.first_series + offset)
                .map_err(|e| anyhow::anyhow!("series entry: {e}"))?;
            let key = series_key(run.metric_name, entry.labels.json_str());
            starts.push((entry.start, entry.start + u64::from(entry.rows), key));
        }
    }
    starts.sort_by_key(|(start, _, _)| *start);

    let reader = builder.build()?;
    let mut row = 0u64;
    let mut cursor = 0usize;
    for batch in reader {
        let batch = batch?;
        let ts = batch
            .column_by_name("timestamp_ns")
            .context("output block has no timestamp_ns")?
            .as_any()
            .downcast_ref::<arrow_array::TimestampNanosecondArray>()
            .context("timestamp_ns is not Timestamp(ns)")?;
        let vf = batch
            .column_by_name("value_float")
            .context("output block has no value_float")?
            .as_any()
            .downcast_ref::<arrow_array::Float64Array>()
            .context("value_float is not Float64")?;
        for row_in_batch in 0..batch.num_rows() {
            while cursor < starts.len() && row >= starts[cursor].1 {
                cursor += 1;
            }
            let Some((_, _, key)) = starts.get(cursor) else {
                anyhow::bail!("row {row} is not covered by the series index");
            };
            let digest = out.entry(key.clone()).or_default();
            let value = vf.value(row_in_batch);
            add_sample(digest, ts.value(row_in_batch), value.to_bits());
            row += 1;
        }
    }
    Ok(())
}

/// Verifies a live Parqtel instance by asking one instant query per source
/// sample. This is deliberately slow but exact: a range query would interpolate
/// or advance to its step and could hide a timestamp mismatch.
pub fn verify_live(options: &VerifyOptions, target: &str) -> Result<VerifyReport> {
    let source = Source::open(options.source.clone())?;
    let endpoint = if target.ends_with("/api/v1/query") {
        target.to_string()
    } else {
        format!("{}/api/v1/query", target.trim_end_matches('/'))
    };
    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?;
    let mut source_digests: BTreeMap<String, Digest> = BTreeMap::new();
    let mut live_digests: BTreeMap<String, Digest> = BTreeMap::new();
    source.scan(|record| {
        let Some(metric) = record.metric_name().map(str::to_string) else {
            return Ok(());
        };
        let mapped = options
            .label_mapper
            .map(&record.labels)
            .map_err(|error| tsdb_reader::Error::Source(error.to_string()))?;
        let key = series_key(&metric, mapped.labels.json_str());
        let selector = build_selector(&metric, &record.labels);
        let source_digest = source_digests.entry(key.clone()).or_default();
        let live_digest = live_digests.entry(key).or_default();
        for sample in record.samples {
            let timestamp_ns = sample.timestamp_ms.checked_mul(1_000_000).ok_or(
                tsdb_reader::Error::TimestampOutOfRange {
                    timestamp_ms: sample.timestamp_ms,
                },
            )?;
            add_sample(source_digest, timestamp_ns, sample.value.to_bits());
            let time = format!("{:.3}", sample.timestamp_ms as f64 / 1000.0);
            let response = client
                .get(&endpoint)
                .query(&[("query", selector.as_str()), ("time", time.as_str())])
                .send()
                .and_then(reqwest::blocking::Response::error_for_status)
                .map_err(|error| {
                    tsdb_reader::Error::Source(format!("querying {endpoint}: {error}"))
                })?;
            let body: serde_json::Value = response.json().map_err(|error| {
                tsdb_reader::Error::Source(format!("decoding query response: {error}"))
            })?;
            let value = body
                .get("data")
                .and_then(|data| data.get("result"))
                .and_then(|result| result.as_array())
                .and_then(|result| result.first())
                .and_then(|series| series.get("value"))
                .and_then(|value| value.get(1))
                .and_then(|value| value.as_str())
                .and_then(|value| value.parse::<f64>().ok());
            if let Some(value) = value {
                add_sample(live_digest, timestamp_ns, value.to_bits());
            }
        }
        Ok(())
    })?;

    let mut report = VerifyReport {
        source_series: source_digests.len(),
        output_series: live_digests.len(),
        source_samples: source_digests.values().map(|digest| digest.samples).sum(),
        output_samples: live_digests.values().map(|digest| digest.samples).sum(),
        ..VerifyReport::default()
    };
    for (key, source) in &source_digests {
        match live_digests.get(key) {
            None => report.missing.push(key.clone()),
            Some(live) if live.hash != source.hash || live.samples != source.samples => {
                report.mismatched.push(Mismatch {
                    series: key.clone(),
                    source: source.clone(),
                    output: live.clone(),
                });
            }
            Some(_) => {}
        }
    }
    for key in live_digests.keys() {
        if !source_digests.contains_key(key) {
            report.extra.push(key.clone());
        }
    }
    Ok(report)
}

fn build_selector(metric: &str, labels: &tsdb_reader::Labels) -> String {
    let mut out = String::from(metric);
    let mut first = true;
    for (key, value) in labels {
        if key == "__name__" {
            continue;
        }
        if first {
            out.push('{');
            first = false;
        } else {
            out.push(',');
        }
        out.push_str(key);
        out.push_str("=\"");
        for ch in value.chars() {
            if ch == '\\' || ch == '"' {
                out.push('\\');
            }
            out.push(ch);
        }
        out.push('"');
    }
    if !first {
        out.push('}');
    }
    out
}

fn series_key(metric: &str, labels_json: &str) -> String {
    format!("{metric}{labels_json}")
}

fn add_sample(digest: &mut Digest, timestamp_ns: i64, value_bits: u64) {
    use std::hash::Hasher;
    let mut hasher = xxhash_rust::xxh3::Xxh3::new();
    hasher.write_u64(digest.hash);
    hasher.write_i64(timestamp_ns);
    hasher.write_u64(value_bits);
    digest.hash = hasher.finish();
    digest.samples += 1;
    digest.first_ts_ns.get_or_insert(timestamp_ns);
    digest.last_ts_ns = Some(timestamp_ns);
}
