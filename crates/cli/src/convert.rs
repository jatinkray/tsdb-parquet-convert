//! Streaming source → Parqtel block conversion.

use crate::labels::LabelMapper;
use crate::metadata::Metadata;
use anyhow::{Context, Result};
use parqtel_sink::{
    write_metric_block, write_sidecar_fragment, BlockFile, BlockSpec, DataPoint, MetricValue,
    SeriesData, WriterConfig,
};
use parquet_sink::{BlockWriter, FileInfo, SchemaSpec, SeriesInput, WriteOptions};
use rayon::prelude::*;
use std::collections::BTreeMap;
use std::path::PathBuf;
use tsdb_reader::{Labels, ScanStats, SeriesRecord, Source, SourceOptions};

#[derive(Debug, Clone)]
pub struct ConvertOptions {
    pub source: SourceOptions,
    pub out_dir: PathBuf,
    pub block_duration_secs: u64,
    pub max_rows_per_block: usize,
    pub writer: WriterConfig,
    pub label_mapper: LabelMapper,
    pub metadata: Metadata,
    pub write_sidecar: bool,
    /// When set, rows are written through `parquet-sink` for this schema and
    /// the Parqtel block writer is not used.
    pub schema: Option<SchemaSpec>,
    /// Settings for the schema-driven writer.
    pub parquet: WriteOptions,
    pub dry_run: bool,
    pub target_retention_days: Option<u64>,
    pub retention_refuse: bool,
    pub progress: bool,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct WindowPlan {
    pub block_ulid: String,
    pub start_ns: i64,
    pub end_ns: i64,
    pub rows: usize,
    pub parts: u32,
}

#[derive(Debug, Clone, Default)]
pub struct ConvertReport {
    pub stats: ScanStats,
    pub blocks: Vec<BlockFile>,
    /// Files written by the schema-driven writer.
    pub files: Vec<FileInfo>,
    pub windows: Vec<WindowPlan>,
    pub total_rows: usize,
    pub preview: Vec<String>,
    pub sidecar: Option<PathBuf>,
    pub retention_warning: Option<String>,
    pub retention_refused: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct WindowKey {
    block_ulid: String,
    start_ns: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SeriesKey {
    metric: String,
    fingerprint: u64,
}

#[derive(Debug)]
struct SeriesAccum {
    metric_name: String,
    labels: Labels,
    /// `(timestamp_ns, value)` in source order.
    samples: Vec<(i64, f64)>,
}

#[derive(Debug, Default)]
struct WindowState {
    start_ns: i64,
    end_ns: i64,
    rows: usize,
    series: BTreeMap<SeriesKey, SeriesAccum>,
}

pub fn convert(options: &ConvertOptions) -> Result<ConvertReport> {
    if options.block_duration_secs == 0 {
        anyhow::bail!("block-duration-secs must be greater than 0");
    }
    if options.max_rows_per_block == 0 {
        anyhow::bail!("max-rows-per-block must be greater than 0");
    }
    if options.writer.row_group_size > options.max_rows_per_block {
        anyhow::bail!(
            "row-group-size ({}) must not exceed max-rows-per-block ({})",
            options.writer.row_group_size,
            options.max_rows_per_block
        );
    }
    let window_ns = i64::try_from(options.block_duration_secs)
        .ok()
        .and_then(|seconds| seconds.checked_mul(1_000_000_000))
        .context("block-duration-secs is too large")?;
    let source = Source::open(options.source.clone())?;
    let (min_ms, max_ms) = source
        .time_bounds_ms()
        .context("source selection has no samples in range")?;
    if !options.dry_run {
        std::fs::create_dir_all(&options.out_dir)
            .with_context(|| format!("creating output directory {}", options.out_dir.display()))?;
    }

    let window_ms = i64::try_from(options.block_duration_secs)
        .ok()
        .and_then(|seconds| seconds.checked_mul(1_000))
        .context("block-duration-secs is too large")?;
    let first_window_ms = min_ms.div_euclid(window_ms) * window_ms;
    let mut ranges = Vec::new();
    let mut window_start_ms = first_window_ms;
    while window_start_ms <= max_ms {
        let window_end_ms = window_start_ms
            .checked_add(window_ms)
            .context("output window end overflow")?;
        ranges.push((window_start_ms, window_end_ms.min(max_ms.saturating_add(1))));
        window_start_ms = window_end_ms;
    }

    let mut report = ConvertReport {
        stats: ScanStats {
            blocks: source.block_count(),
            ..ScanStats::default()
        },
        ..ConvertReport::default()
    };
    let parallel = ranges.len() > 1 && rayon::current_num_threads() > 1;
    if parallel {
        let parts: Vec<ConvertReport> = ranges
            .par_iter()
            .map(|(start, end)| convert_one_window(options, &source, window_ns, *start, *end))
            .collect::<Result<Vec<_>>>()?;
        for part in parts {
            merge_report(&mut report, part);
        }
    } else {
        for (start, end) in ranges {
            let part = convert_one_window(options, &source, window_ns, start, end)?;
            merge_report(&mut report, part);
        }
    }
    report.windows.sort_by(|a, b| {
        (a.start_ns, &a.block_ulid, a.parts).cmp(&(b.start_ns, &b.block_ulid, b.parts))
    });
    report.blocks.sort_by(|a, b| a.filename.cmp(&b.filename));
    report.preview.sort();
    report.preview.truncate(10);
    report.stats.blocks = source.block_count();

    if options.write_sidecar && !report.blocks.is_empty() {
        let metadata: Vec<_> = report
            .blocks
            .iter()
            .map(|block| block.metadata.clone())
            .collect();
        report.sidecar = Some(write_sidecar_fragment(&options.out_dir, &metadata)?);
    }
    if let Some(days) = options
        .target_retention_days
        .filter(|_| options.schema.is_none())
    {
        if let Some(oldest) = report.windows.iter().map(|window| window.start_ns).min() {
            let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(i64::MAX);
            let cutoff = now_ns.saturating_sub(
                i64::try_from(days)
                    .unwrap_or(i64::MAX)
                    .saturating_mul(86_400_000_000_000),
            );
            if oldest < cutoff {
                report.retention_warning = Some(format!(
                    "oldest imported window starts at {oldest} ns, before Parqtel's {days}-day retention cutoff; raise `storage.retention_days` in the target config"
                ));
                report.retention_refused = options.retention_refuse;
            }
        }
    }
    Ok(report)
}

fn convert_one_window(
    options: &ConvertOptions,
    source: &Source,
    window_ns: i64,
    start_ms: i64,
    end_ms: i64,
) -> Result<ConvertReport> {
    let mut windows: BTreeMap<WindowKey, WindowState> = BTreeMap::new();
    let mut next_parts: BTreeMap<WindowKey, u32> = BTreeMap::new();
    let mut report = ConvertReport::default();
    report.stats = source.scan_range(start_ms, end_ms, |record| {
        append_record(
            options,
            window_ns,
            record,
            &mut windows,
            &mut next_parts,
            &mut report,
        )
        .map_err(|error| tsdb_reader::Error::Source(error.to_string()))
    })?;
    let keys: Vec<WindowKey> = windows.keys().cloned().collect();
    for key in keys {
        flush_window(options, &key, &mut windows, &mut next_parts, &mut report)?;
    }
    if options.progress {
        tracing::info!(
            start_ms,
            end_ms,
            rows = report.total_rows,
            blocks = report.blocks.len(),
            "window converted"
        );
    }
    Ok(report)
}

fn merge_report(total: &mut ConvertReport, mut part: ConvertReport) {
    merge_stats(&mut total.stats, part.stats);
    total.blocks.append(&mut part.blocks);
    total.files.append(&mut part.files);
    total.windows.append(&mut part.windows);
    total.total_rows += part.total_rows;
    total.preview.append(&mut part.preview);
}

fn merge_stats(total: &mut ScanStats, window: ScanStats) {
    total.series += window.series;
    total.samples += window.samples;
    total.skipped_blocks += window.skipped_blocks;
    total.skipped_series += window.skipped_series;
    total.stale_dropped += window.stale_dropped;
    total.deleted_samples += window.deleted_samples;
    total.errors.extend(window.errors);
}

fn append_record(
    options: &ConvertOptions,
    window_ns: i64,
    record: SeriesRecord,
    windows: &mut BTreeMap<WindowKey, WindowState>,
    next_parts: &mut BTreeMap<WindowKey, u32>,
    report: &mut ConvertReport,
) -> Result<()> {
    let Some(metric_name) = record.metric_name().map(str::to_string) else {
        return Ok(());
    };
    let fingerprint = fingerprint(&record.labels);
    let key = SeriesKey {
        metric: metric_name.clone(),
        fingerprint,
    };
    if report.preview.len() < 10 {
        let mapped = options.label_mapper.map(&record.labels)?;
        report.preview.push(format!(
            "{}{{{}}}",
            metric_name,
            mapped
                .labels
                .iter()
                .map(|(key, value)| format!("{key}={value:?}"))
                .collect::<Vec<_>>()
                .join(",")
        ));
    }

    for sample in record.samples {
        let timestamp_ns = sample
            .timestamp_ms
            .checked_mul(1_000_000)
            .context("timestamp does not fit nanoseconds")?;
        if timestamp_ns <= 0 {
            continue;
        }
        let start_ns = timestamp_ns.div_euclid(window_ns) * window_ns;
        let window_key = WindowKey {
            block_ulid: record.block_ulid.clone(),
            start_ns,
        };
        let state = windows
            .entry(window_key.clone())
            .or_insert_with(|| WindowState {
                start_ns,
                end_ns: start_ns.saturating_add(window_ns),
                rows: 0,
                series: BTreeMap::new(),
            });
        state
            .series
            .entry(key.clone())
            .or_insert_with(|| SeriesAccum {
                metric_name: metric_name.clone(),
                labels: record.labels.clone(),
                samples: Vec::new(),
            })
            .samples
            .push((timestamp_ns, sample.value));
        state.rows += 1;
        report.total_rows += 1;
        if state.rows >= options.max_rows_per_block {
            flush_window(options, &window_key, windows, next_parts, report)?;
        }
    }
    Ok(())
}

fn flush_window(
    options: &ConvertOptions,
    key: &WindowKey,
    windows: &mut BTreeMap<WindowKey, WindowState>,
    next_parts: &mut BTreeMap<WindowKey, u32>,
    report: &mut ConvertReport,
) -> Result<()> {
    let Some(state) = windows.remove(key) else {
        return Ok(());
    };
    if state.series.is_empty() {
        return Ok(());
    }
    let part = next_parts.entry(key.clone()).or_insert(0);
    let plan = WindowPlan {
        block_ulid: key.block_ulid.clone(),
        start_ns: state.start_ns,
        end_ns: state.end_ns,
        rows: state.rows,
        parts: *part,
    };
    report.windows.push(plan);

    if let Some(schema) = &options.schema {
        let mut writer = BlockWriter::new(schema.clone(), options.parquet.clone())?;
        for (index, series) in state.series.into_iter().enumerate() {
            writer.push_series(SeriesInput {
                block_ulid: &key.block_ulid,
                series_fingerprint: series.0.fingerprint,
                series_index: index as u64,
                metric_name: Some(series.1.metric_name.as_str()),
                labels: &series.1.labels,
                samples: &series.1.samples,
            })?;
        }
        let filename = parquet_sink::block_filename(state.start_ns, state.end_ns, *part);
        if !options.dry_run {
            let file = writer.write(&options.out_dir.join(&filename))?;
            report.files.push(file);
        }
    } else {
        let series: Vec<SeriesData> = state
            .series
            .into_values()
            .map(|series| -> Result<SeriesData> {
                let mapped = options.label_mapper.map(&series.labels)?;
                let mut points = Vec::with_capacity(series.samples.len());
                for (timestamp_ns, value) in series.samples {
                    let point = DataPoint::new(
                        timestamp_ns,
                        MetricValue::Double(value),
                        mapped.labels.clone(),
                    )
                    .map_err(|e| anyhow::anyhow!("building data point: {e}"))?;
                    points.push(point);
                }
                Ok(SeriesData {
                    kind: options.metadata.kind_for(&series.metric_name),
                    metric_name: series.metric_name,
                    resource: mapped.resource,
                    points,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        if !options.dry_run {
            let block = write_metric_block(
                &options.out_dir,
                &BlockSpec {
                    source_id: key.block_ulid.clone(),
                    window_start_ns: state.start_ns,
                    window_end_ns: state.end_ns,
                    part: *part,
                },
                series,
                &options.writer,
            )?;
            report.blocks.push(block);
        }
    }
    *part = part.saturating_add(1);
    Ok(())
}

/// Stable series identity over the label set; independent of label mapping.
fn fingerprint(labels: &Labels) -> u64 {
    let mut hasher = xxhash_rust::xxh3::Xxh3::new();
    for (key, value) in labels {
        hasher.update(key.as_bytes());
        hasher.update(&[0xff]);
        hasher.update(value.as_bytes());
        hasher.update(&[0xfe]);
    }
    hasher.digest()
}
