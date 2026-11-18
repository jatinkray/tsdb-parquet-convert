//! Streaming source → Parqtel block conversion.

use crate::labels::{LabelMapper, MappedLabels};
use crate::metadata::Metadata;
use anyhow::{Context, Result};
use parqtel_sink::{
    write_metric_block, write_sidecar_fragment, BlockFile, BlockSpec, DataPoint, LabelSet,
    MetricKind, MetricValue, SeriesData, WriterConfig,
};
use std::collections::BTreeMap;
use std::path::PathBuf;
use tsdb_reader::{ScanStats, SeriesRecord, Source, SourceOptions};

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
    pub dry_run: bool,
    pub target_retention_days: Option<u64>,
    pub retention_refuse: bool,
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
    kind: MetricKind,
    resource_json: String,
    fingerprint: u64,
}

#[derive(Debug)]
struct SeriesAccum {
    metric_name: String,
    kind: MetricKind,
    resource: LabelSet,
    points: Vec<DataPoint>,
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
    let mut windows: BTreeMap<WindowKey, WindowState> = BTreeMap::new();
    let mut next_parts: BTreeMap<WindowKey, u32> = BTreeMap::new();
    let mut report = ConvertReport::default();
    if !options.dry_run {
        std::fs::create_dir_all(&options.out_dir)
            .with_context(|| format!("creating output directory {}", options.out_dir.display()))?;
    }

    let stats = source.scan(|record| {
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
    report.stats = stats;

    let keys: Vec<WindowKey> = windows.keys().cloned().collect();
    for key in keys {
        flush_window(options, &key, &mut windows, &mut next_parts, &mut report)?;
    }

    if options.write_sidecar && !report.blocks.is_empty() {
        let metadata: Vec<_> = report
            .blocks
            .iter()
            .map(|block| block.metadata.clone())
            .collect();
        report.sidecar = Some(write_sidecar_fragment(&options.out_dir, &metadata)?);
    }
    if let Some(days) = options.target_retention_days {
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
    let kind = options.metadata.kind_for(&metric_name);
    let MappedLabels { labels, resource } = options.label_mapper.map(&record.labels)?;
    let resource_json = resource.json_str().to_string();
    let key = SeriesKey {
        metric: metric_name.clone(),
        kind,
        resource_json,
        fingerprint: labels.fingerprint(),
    };
    if report.preview.len() < 10 {
        report.preview.push(format!(
            "{}{{{}}}",
            metric_name,
            labels
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
        let point = DataPoint::new(
            timestamp_ns,
            MetricValue::Double(sample.value),
            labels.clone(),
        )
        .map_err(|e| anyhow::anyhow!("building data point: {e}"))?;
        state
            .series
            .entry(key.clone())
            .or_insert_with(|| SeriesAccum {
                metric_name: metric_name.clone(),
                kind,
                resource: resource.clone(),
                points: Vec::new(),
            })
            .points
            .push(point);
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

    let series: Vec<SeriesData> = state
        .series
        .into_values()
        .map(|series| SeriesData {
            metric_name: series.metric_name,
            kind: series.kind,
            resource: series.resource,
            points: series.points,
        })
        .collect();
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
    *part = part.saturating_add(1);
    Ok(())
}
