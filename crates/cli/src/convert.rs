//! Streaming source → Parqtel block conversion.

use crate::destination::Destination;
use crate::labels::LabelMapper;
use crate::metadata::Metadata;
use crate::state::{schema_hash, State};
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
    /// Source as typed on the command line, recorded in the manifest.
    pub source_label: String,
    pub out_dir: PathBuf,
    pub destination: Destination,
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
    /// Incremental-run state: blocks recorded here are skipped next time.
    pub state: Option<PathBuf>,
    /// Bucket width in seconds; `None` keeps every sample.
    pub rollup_seconds: Option<u64>,
    /// Which aggregate a rollup row holds.
    pub rollup_func: RollupFunc,
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
    /// Where `manifest.json` was written, when the run produced one.
    pub manifest: Option<String>,
    /// Source blocks skipped because the state file already covers them.
    pub blocks_skipped: usize,
    /// Rows actually written (differs from `total_rows` under `--rollup`).
    pub rows_written: usize,
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
    /// Per-bucket aggregates, only filled when `--rollup` is set.
    buckets: BTreeMap<i64, Bucket>,
}

/// Running aggregate for one rollup bucket.
#[derive(Debug, Clone, Copy)]
struct Bucket {
    count: u64,
    sum: f64,
    min: f64,
    max: f64,
    last_ts: i64,
    last: f64,
}

impl Bucket {
    fn first(ts: i64, value: f64) -> Self {
        Self {
            count: 1,
            sum: value,
            min: value,
            max: value,
            last_ts: ts,
            last: value,
        }
    }

    fn add(&mut self, ts: i64, value: f64) {
        self.count += 1;
        self.sum += value;
        if value < self.min {
            self.min = value;
        }
        if value > self.max {
            self.max = value;
        }
        if ts >= self.last_ts {
            self.last_ts = ts;
            self.last = value;
        }
    }

    fn value(&self, function: RollupFunc) -> f64 {
        match function {
            RollupFunc::Avg => self.sum / self.count as f64,
            RollupFunc::Sum => self.sum,
            RollupFunc::Min => self.min,
            RollupFunc::Max => self.max,
            RollupFunc::Last => self.last,
            RollupFunc::Count => self.count as f64,
        }
    }
}

/// Which aggregate a rollup row holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RollupFunc {
    Avg,
    Sum,
    Min,
    Max,
    Last,
    Count,
}

impl RollupFunc {
    pub fn parse(value: &str) -> Result<Self> {
        Ok(match value {
            "avg" => Self::Avg,
            "sum" => Self::Sum,
            "min" => Self::Min,
            "max" => Self::Max,
            "last" => Self::Last,
            "count" => Self::Count,
            other => anyhow::bail!(
                "unknown rollup function `{other}`; use avg, sum, min, max, last or count"
            ),
        })
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Avg => "avg",
            Self::Sum => "sum",
            Self::Min => "min",
            Self::Max => "max",
            Self::Last => "last",
            Self::Count => "count",
        }
    }
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
    let mut source = Source::open(options.source.clone())?;
    let mut incremental: Option<State> = None;
    if let Some(state_path) = options.state.as_deref() {
        let spec = options
            .schema
            .as_ref()
            .context("--state needs a schema so runs write one layout")?;
        let hash = schema_hash(spec)?;
        let mut state = State::load(state_path, &hash)?;
        let infos = source.block_infos();
        let mut keep = std::collections::BTreeSet::new();
        let mut skipped = 0usize;
        let mut pending: Vec<(String, i64)> = Vec::new();
        for info in &infos {
            if state.covers(&info.meta.ulid, info.meta.max_time_ms) {
                skipped += 1;
            } else {
                keep.insert(info.meta.ulid.clone());
                pending.push((info.meta.ulid.clone(), info.meta.max_time_ms));
            }
        }
        tracing::info!(
            blocks = infos.len(),
            skipped,
            remaining = keep.len(),
            "incremental run"
        );
        source = source.retain_blocks(&keep);
        if keep.is_empty() {
            return Ok(ConvertReport {
                blocks_skipped: skipped,
                ..ConvertReport::default()
            });
        }
        for (ulid, max_time_ms) in pending {
            state.record(&ulid, max_time_ms);
        }
        incremental = Some(state);
    }
    // Remote destinations are assembled locally and uploaded per file, so the
    // writer always has a real directory to fsync and rename in.
    // Held for the whole run: dropping it would delete staged files before
    // they are uploaded.
    let _staging: Option<tempfile::TempDir>;
    let mut options = options.clone();
    if options.destination.is_remote() {
        let dir = tempfile::TempDir::new().context("creating the upload staging directory")?;
        options.out_dir = dir.path().to_path_buf();
        _staging = Some(dir);
    } else {
        _staging = None;
    }
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
            .map(|(start, end)| convert_one_window(&options, &source, window_ns, *start, *end))
            .collect::<Result<Vec<_>>>()?;
        for part in parts {
            merge_report(&mut report, part);
        }
    } else {
        for (start, end) in ranges {
            let part = convert_one_window(&options, &source, window_ns, start, end)?;
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
    if options.schema.is_some() && !options.dry_run && !report.files.is_empty() {
        let manifest = build_manifest(&options, &report)?;
        let body = serde_json::to_string_pretty(&manifest)?;
        let run_id = manifest.run_id.clone();
        options.destination.write_text("manifest.json", &body)?;
        options
            .destination
            .write_text(&format!("manifests/{run_id}.json"), &body)?;
        report.manifest = Some(format!("{}/manifest.json", options.destination.describe()));
    }
    if let (Some(state), Some(state_path), false) = (
        incremental.as_mut(),
        options.state.as_deref(),
        options.dry_run,
    ) {
        state.save(state_path)?;
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
    total.rows_written += part.rows_written;
    total.blocks_skipped += part.blocks_skipped;
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

    let bucket_ns = match options.rollup_seconds {
        Some(seconds) => Some(
            i64::try_from(seconds)
                .ok()
                .and_then(|value| value.checked_mul(1_000_000_000))
                .context("rollup duration is too large")?,
        ),
        None => None,
    };
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
        let accumulator = state
            .series
            .entry(key.clone())
            .or_insert_with(|| SeriesAccum {
                metric_name: metric_name.clone(),
                labels: record.labels.clone(),
                samples: Vec::new(),
                buckets: BTreeMap::new(),
            });
        match bucket_ns {
            Some(bucket_ns) => {
                let start = timestamp_ns.div_euclid(bucket_ns) * bucket_ns;
                match accumulator.buckets.get_mut(&start) {
                    Some(bucket) => bucket.add(timestamp_ns, sample.value),
                    None => {
                        accumulator
                            .buckets
                            .insert(start, Bucket::first(timestamp_ns, sample.value));
                    }
                }
            }
            None => accumulator.samples.push((timestamp_ns, sample.value)),
        }
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
            let rolled = rolled_samples(&series.1, options.rollup_func);
            let samples = rolled.as_deref().unwrap_or(&series.1.samples);
            report.rows_written += samples.len();
            writer.push_series(SeriesInput {
                block_ulid: &key.block_ulid,
                series_fingerprint: series.0.fingerprint,
                series_index: index as u64,
                metric_name: Some(series.1.metric_name.as_str()),
                labels: &series.1.labels,
                samples,
            })?;
        }
        let filename = parquet_sink::block_filename(state.start_ns, state.end_ns, *part);
        if !options.dry_run {
            let file = writer.write(&options.out_dir.join(&filename))?;
            options.destination.publish(&file.path, &file.filename)?;
            report.files.push(file);
        }
    } else {
        let series: Vec<SeriesData> = state
            .series
            .into_values()
            .map(|series| -> Result<SeriesData> {
                let mapped = options.label_mapper.map(&series.labels)?;
                let rolled = rolled_samples(&series, options.rollup_func);
                let samples = rolled.as_deref().unwrap_or(&series.samples);
                let mut points = Vec::with_capacity(samples.len());
                for &(timestamp_ns, value) in samples {
                    let point = DataPoint::new(
                        timestamp_ns,
                        MetricValue::Double(value),
                        mapped.labels.clone(),
                    )
                    .map_err(|e| anyhow::anyhow!("building data point: {e}"))?;
                    points.push(point);
                }
                report.rows_written += points.len();
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

/// Bucket aggregates as `(bucket_start_ns, aggregate)` when a rollup ran;
/// `None` keeps the raw samples.
fn rolled_samples(series: &SeriesAccum, function: RollupFunc) -> Option<Vec<(i64, f64)>> {
    if series.buckets.is_empty() {
        return None;
    }
    Some(
        series
            .buckets
            .iter()
            .map(|(start, bucket)| (*start, bucket.value(function)))
            .collect(),
    )
}

/// Machine-readable record of one conversion run, written next to the data.
#[derive(Debug, serde::Serialize)]
pub struct Manifest {
    pub tool: String,
    pub run_id: String,
    pub created_at: String,
    pub source: String,
    pub destination: String,
    pub schema: ManifestSchema,
    pub rollup: Option<ManifestRollup>,
    pub totals: ManifestTotals,
    pub windows: Vec<WindowPlan>,
    pub files: Vec<ManifestFile>,
}

#[derive(Debug, serde::Serialize)]
pub struct ManifestSchema {
    pub name: Option<String>,
    /// xxHash3 of the canonical schema JSON; changes when any column changes.
    pub hash: String,
    pub columns: usize,
}

#[derive(Debug, serde::Serialize)]
pub struct ManifestRollup {
    pub seconds: u64,
    pub function: &'static str,
}

#[derive(Debug, serde::Serialize)]
pub struct ManifestTotals {
    pub windows: usize,
    pub files: usize,
    pub rows: usize,
    pub bytes: u64,
}

#[derive(Debug, serde::Serialize)]
pub struct ManifestFile {
    pub name: String,
    pub rows: usize,
    pub bytes: u64,
}

fn build_manifest(options: &ConvertOptions, report: &ConvertReport) -> Result<Manifest> {
    let spec = options
        .schema
        .as_ref()
        .context("a manifest needs a schema")?;
    let canonical = serde_json::to_string(spec)?;
    let hash = xxhash_rust::xxh3::xxh3_64(canonical.as_bytes());
    let now = chrono::Utc::now();
    Ok(Manifest {
        tool: format!("tsdb2parqtel {}", env!("CARGO_PKG_VERSION")),
        run_id: format!("{}-{:016x}", now.format("%Y%m%dT%H%M%SZ"), hash),
        created_at: now.to_rfc3339(),
        source: options.source_label.clone(),
        destination: options.destination.describe(),
        schema: ManifestSchema {
            name: spec.name.clone(),
            hash: format!("{hash:016x}"),
            columns: spec.columns.len(),
        },
        rollup: options.rollup_seconds.map(|seconds| ManifestRollup {
            seconds,
            function: options.rollup_func.as_str(),
        }),
        totals: ManifestTotals {
            windows: report.windows.len(),
            files: report.files.len(),
            rows: report.rows_written,
            bytes: report.files.iter().map(|file| file.bytes).sum(),
        },
        windows: report.windows.clone(),
        files: report
            .files
            .iter()
            .map(|file| ManifestFile {
                name: file.filename.clone(),
                rows: file.rows,
                bytes: file.bytes,
            })
            .collect(),
    })
}

/// Stable series identity over the label set; independent of label mapping.
///
/// `verify --schema` recomputes the same value so schema-mode output can be
/// reconciled by `series_fingerprint` without carrying labels.
pub(crate) fn fingerprint(labels: &Labels) -> u64 {
    let mut hasher = xxhash_rust::xxh3::Xxh3::new();
    for (key, value) in labels {
        hasher.update(key.as_bytes());
        hasher.update(&[0xff]);
        hasher.update(value.as_bytes());
        hasher.update(&[0xfe]);
    }
    hasher.digest()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Aggregates `(timestamp, value)` samples into buckets of `bucket_ns`,
    /// mirroring what `append_record` does.
    fn accum(samples: &[(i64, f64)], bucket_ns: i64) -> SeriesAccum {
        let mut series = SeriesAccum {
            metric_name: "m".to_string(),
            labels: Labels::new(),
            samples: Vec::new(),
            buckets: BTreeMap::new(),
        };
        for (ts, value) in samples {
            let start = ts.div_euclid(bucket_ns) * bucket_ns;
            match series.buckets.get_mut(&start) {
                Some(bucket) => bucket.add(*ts, *value),
                None => {
                    series.buckets.insert(start, Bucket::first(*ts, *value));
                }
            }
        }
        series
    }

    #[test]
    fn bucket_aggregates_match_the_definition() {
        let series = accum(&[(0, 1.0), (1, 3.0), (2, 5.0)], 100);
        let roll = rolled_samples(&series, RollupFunc::Avg).unwrap_or_default();
        let expected = [
            (RollupFunc::Avg, 3.0),
            (RollupFunc::Sum, 9.0),
            (RollupFunc::Min, 1.0),
            (RollupFunc::Max, 5.0),
            (RollupFunc::Last, 5.0),
            (RollupFunc::Count, 3.0),
        ];
        for (function, value) in expected {
            let rolled = rolled_samples(&series, function).unwrap_or_default();
            assert_eq!(rolled, vec![(0, value)], "{function:?}");
        }
        assert_eq!(roll.len(), 1);
    }

    #[test]
    fn last_uses_the_latest_timestamp_not_insertion_order() {
        let mut bucket = Bucket::first(10, 1.0);
        bucket.add(5, 9.0);
        bucket.add(20, 2.0);
        assert_eq!(bucket.value(RollupFunc::Last), 2.0);
    }

    #[test]
    fn rollup_rows_are_sorted_by_bucket_start() {
        let series = accum(&[(10, 1.0), (0, 2.0), (5, 3.0)], 5);
        let rolled = rolled_samples(&series, RollupFunc::Avg).unwrap_or_default();
        assert_eq!(rolled, vec![(0, 2.0), (5, 3.0), (10, 1.0)]);
    }

    #[test]
    fn no_buckets_means_raw_samples_are_written() {
        let series = SeriesAccum {
            metric_name: "m".to_string(),
            labels: Labels::new(),
            samples: vec![(1, 2.0)],
            buckets: BTreeMap::new(),
        };
        assert!(rolled_samples(&series, RollupFunc::Avg).is_none());
    }
}
