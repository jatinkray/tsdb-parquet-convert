#![deny(unsafe_code)]

mod convert;
mod labels;
mod metadata;
mod verify;

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use convert::{convert, ConvertOptions, ConvertReport};
use labels::{LabelMapper, LabelMode};
use metadata::Metadata;
use serde_json::json;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use tsdb_reader::{
    DownsamplePolicy, ExternalLabels, OnError, Source, SourceOptions, StaleMarkerPolicy,
};
use verify::{verify, verify_live, VerifyOptions};

#[derive(Debug, Parser)]
#[command(
    name = "tsdb2parqtel",
    about = "Convert Prometheus/Thanos TSDB blocks into Parqtel-compatible Parquet blocks",
    version,
    after_help = "Examples:\n  tsdb2parqtel inspect /var/lib/prometheus/blocks\n  tsdb2parqtel plan /var/lib/thanos/bucket --matcher '{job=\"api\"}'\n  tsdb2parqtel convert /var/lib/prometheus/blocks --out ./blocks\n  tsdb2parqtel clone ./blocks --target /var/lib/parqtel/data\n  tsdb2parqtel verify /var/lib/prometheus/blocks ./blocks"
)]
struct Cli {
    #[arg(
        long,
        global = true,
        default_value_t = 0,
        help = "Worker threads (0 = available parallelism)"
    )]
    threads: usize,
    #[arg(long, global = true, help = "Memory budget, e.g. 512MiB (advisory)")]
    memory_limit: Option<String>,
    #[arg(long, global = true, default_value = "text")]
    log_format: String,
    #[arg(long, global = true)]
    progress: bool,
    #[arg(long, global = true)]
    dry_run: bool,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Report what a source contains, without writing anything.
    Inspect(InspectArgs),
    /// Dry run: windows, mapping preview, retention check.
    Plan(PlanArgs),
    /// Produce Parqtel metric blocks.
    Convert(ConvertArgs),
    /// Convert and install into a Parqtel data directory or replay over OTLP.
    Clone(CloneArgs),
    /// Reconcile source and converted blocks per series.
    Verify(VerifyArgs),
}

#[derive(Debug, Clone, Args)]
struct SourceArgs {
    /// Local Prometheus/Thanos block root or a single block directory.
    source: String,
    #[arg(long, help = "Start time (RFC3339, unix s/ms, or ns)")]
    min_time: Option<String>,
    #[arg(long, help = "End time (inclusive; RFC3339, unix s/ms, or ns)")]
    max_time: Option<String>,
    #[arg(long = "block-ulid")]
    block_ulids: Vec<String>,
    #[arg(long, help = "PromQL selector, e.g. '{job=\"api\"}'")]
    matcher: Option<String>,
    #[arg(long, help = "Matcher over Thanos external labels")]
    external_label_filter: Option<String>,
    #[arg(long, default_value = "prefer-raw", value_parser = ["skip", "prefer-raw", "include", "only"])]
    downsampled: String,
    #[arg(long, default_value = "counter", value_parser = ["counter", "sum", "count", "min", "max", "avg"])]
    downsample_aggregate: String,
    #[arg(long, default_value = "include", value_parser = ["include", "ignore"])]
    external_labels: String,
    #[arg(long = "dedup-replica-label")]
    dedup_replica_labels: Vec<String>,
    #[arg(long, default_value = "drop", value_parser = ["drop", "keep"])]
    stale_markers: String,
    #[arg(long, default_value = "fail", value_parser = ["fail", "skip"])]
    on_error: String,
}

impl SourceArgs {
    fn to_options(&self) -> Result<SourceOptions> {
        let mut options = SourceOptions::new(&self.source);
        options.min_time_ms = self.min_time.as_deref().map(parse_time).transpose()?;
        options.max_time_ms = self
            .max_time
            .as_deref()
            .map(parse_time)
            .transpose()?
            .map(|value| value.saturating_add(1));
        options.block_ulids = self.block_ulids.iter().cloned().collect();
        options.matcher = self
            .matcher
            .as_deref()
            .map(tsdb_reader::Selector::parse)
            .transpose()?;
        options.external_label_filter = self
            .external_label_filter
            .as_deref()
            .map(tsdb_reader::Selector::parse)
            .transpose()?;
        options.downsampled = DownsamplePolicy::parse(&self.downsampled)?;
        tracing::debug!(
            aggregate = %self.downsample_aggregate,
            "Thanos downsampled chunks already carry their aggregate; the flag selects the documented interpretation"
        );
        options.external_labels = ExternalLabels::parse(&self.external_labels)?;
        options.dedup_replica_labels = self.dedup_replica_labels.clone();
        options.stale_markers = StaleMarkerPolicy::parse(&self.stale_markers)?;
        options.on_error = OnError::parse(&self.on_error)?;
        Ok(options)
    }
}

#[derive(Debug, Args)]
struct OutputArgs {
    #[arg(
        long,
        default_value_t = 7200,
        help = "Parqtel window duration in seconds"
    )]
    block_duration_secs: u64,
    #[arg(long, default_value_t = 1_000_000)]
    max_rows_per_block: usize,
    #[arg(long, default_value_t = 100_000)]
    row_group_size: usize,
    #[arg(long, default_value = "zstd", value_parser = ["zstd", "snappy", "lz4", "none"])]
    compression: String,
    #[arg(long)]
    compression_level: Option<i32>,
    #[arg(long, default_value = "preserve", value_parser = ["preserve", "otel"])]
    label_mode: String,
    #[arg(long)]
    label_mapping: Option<PathBuf>,
    #[arg(long)]
    metadata_file: Option<PathBuf>,
    #[arg(
        long,
        help = "Warn or refuse when source data predates target retention"
    )]
    target_retention_days: Option<u64>,
    #[arg(long, default_value = "warn", value_parser = ["warn", "refuse"])]
    retention_action: String,
}

impl OutputArgs {
    fn writer(&self) -> Result<parqtel_sink::WriterConfig> {
        let config = parqtel_sink::WriterConfig {
            compression: self.compression.clone(),
            compression_level: self.compression_level,
            row_group_size: self.row_group_size,
        };
        config
            .validate()
            .map_err(|e| anyhow::anyhow!("invalid writer config: {e}"))?;
        Ok(config)
    }

    fn mapper(&self) -> Result<LabelMapper> {
        LabelMapper::new(
            LabelMode::parse(&self.label_mode)?,
            self.label_mapping.as_deref(),
        )
    }

    fn metadata(&self) -> Result<Metadata> {
        Metadata::load(self.metadata_file.as_deref())
    }
}

#[derive(Debug, Args)]
struct InspectArgs {
    #[command(flatten)]
    source: SourceArgs,
    #[arg(long, default_value_t = 20)]
    top_labels: usize,
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
struct PlanArgs {
    #[command(flatten)]
    source: SourceArgs,
    #[command(flatten)]
    output: OutputArgs,
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
struct ConvertArgs {
    #[command(flatten)]
    source: SourceArgs,
    #[arg(long)]
    out: PathBuf,
    #[command(flatten)]
    output: OutputArgs,
    #[arg(long, help = "Write index.fragment.json for the output blocks")]
    sidecar: bool,
}

#[derive(Debug, Args)]
struct CloneArgs {
    #[command(flatten)]
    source: SourceArgs,
    #[arg(long, help = "Parqtel data directory or OTLP HTTP endpoint")]
    target: String,
    #[command(flatten)]
    output: OutputArgs,
    #[arg(long, help = "Checkpoint file for resumable OTLP replay")]
    checkpoint: Option<PathBuf>,
    #[arg(long, help = "Install the index even when a WAL is present")]
    force: bool,
}

#[derive(Debug, Args)]
struct VerifyArgs {
    #[command(flatten)]
    source: SourceArgs,
    /// Converted blocks directory or live Parqtel URL.
    target: String,
    #[command(flatten)]
    output: OutputArgs,
    #[arg(long)]
    json: bool,
    #[arg(long)]
    report: Option<PathBuf>,
}

fn main() {
    let cli = Cli::parse();
    init_tracing(&cli);
    let code = match run(cli) {
        Ok(code) => code,
        Err(error) => {
            eprintln!("error: {error:#}");
            1
        }
    };
    std::process::exit(code);
}

fn init_tracing(cli: &Cli) {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let builder = tracing_subscriber::fmt().with_env_filter(filter);
    if cli.log_format == "json" {
        builder.json().init();
    } else {
        builder.init();
    }
}

fn run(cli: Cli) -> Result<i32> {
    if let Some(limit) = &cli.memory_limit {
        tracing::info!(memory_limit = %limit, "memory limit is advisory; window buffers are bounded by max rows");
    }
    if cli.threads > 0 {
        let _ = rayon::ThreadPoolBuilder::new()
            .num_threads(cli.threads)
            .build_global();
    }
    let dry_run = cli.dry_run;
    let _progress = cli.progress;
    match cli.command {
        Commands::Inspect(args) => inspect(args),
        Commands::Plan(args) => plan(args),
        Commands::Convert(args) => run_convert(args, dry_run),
        Commands::Clone(args) => clone(args, dry_run),
        Commands::Verify(args) => run_verify(args),
    }
}

fn inspect(args: InspectArgs) -> Result<i32> {
    let options = args.source.to_options()?;
    let source = Source::open(options)?;
    let infos = source.block_infos();
    let mut label_values: BTreeMap<String, BTreeMap<String, u64>> = BTreeMap::new();
    let stats = source.scan(|record| {
        for (key, value) in record.labels.iter() {
            if key == "__name__" {
                continue;
            }
            *label_values
                .entry(key.clone())
                .or_default()
                .entry(value.clone())
                .or_insert(0) += 1;
        }
        Ok(())
    })?;
    let mut top: Vec<_> = label_values
        .into_iter()
        .map(|(name, values)| {
            let mut values: Vec<_> = values.into_iter().collect();
            values.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            values.truncate(args.top_labels);
            (name, values)
        })
        .collect();
    top.sort_by(|a, b| a.0.cmp(&b.0));

    if args.json {
        let blocks: Vec<_> = infos
            .iter()
            .map(|info| {
                json!({
                    "ulid": info.meta.ulid,
                    "min_time_ms": info.meta.min_time_ms,
                    "max_time_ms": info.meta.max_time_ms,
                    "series": info.series,
                    "chunks": info.chunks,
                    "samples": info.samples,
                    "resolution_ms": info.resolution,
                    "external_labels": info.external_labels,
                })
            })
            .collect();
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "blocks": blocks,
                "series": stats.series,
                "samples": stats.samples,
                "stale_dropped": stats.stale_dropped,
                "skipped_series": stats.skipped_series,
                "errors": stats.errors,
                "top_labels": top,
            }))?
        );
    } else {
        for info in &infos {
            println!(
                "{}  {}..{}  series={} chunks={} samples={} downsample={}ms",
                info.meta.ulid,
                info.meta.min_time_ms,
                info.meta.max_time_ms,
                info.series,
                info.chunks,
                info.samples,
                info.resolution
            );
            if !info.external_labels.is_empty() {
                println!("  external: {info:?}", info = info.external_labels);
            }
        }
        println!(
            "total: blocks={} series={} samples={} skipped_series={} stale_dropped={} errors={}",
            infos.len(),
            stats.series,
            stats.samples,
            stats.skipped_series,
            stats.stale_dropped,
            stats.errors.len()
        );
        for (name, values) in top {
            println!("label {name}: {values:?}");
        }
    }
    Ok(if stats.errors.is_empty() { 0 } else { 3 })
}

fn plan(args: PlanArgs) -> Result<i32> {
    let options = build_convert_options(args.source, args.output, PathBuf::new(), true, false)?;
    let report = convert(&options)?;
    print_report(&report, args.json)?;
    Ok(if report.retention_refused { 5 } else { 0 })
}

fn run_convert(args: ConvertArgs, dry_run: bool) -> Result<i32> {
    let options = build_convert_options(
        args.source,
        args.output,
        args.out.clone(),
        dry_run,
        args.sidecar,
    )?;
    let report = convert(&options)?;
    print_report(&report, false)?;
    Ok(if report.retention_refused {
        5
    } else if report.stats.errors.is_empty() {
        0
    } else {
        3
    })
}

fn build_convert_options(
    source: SourceArgs,
    output: OutputArgs,
    out_dir: PathBuf,
    dry_run: bool,
    sidecar: bool,
) -> Result<ConvertOptions> {
    Ok(ConvertOptions {
        source: source.to_options()?,
        out_dir,
        block_duration_secs: output.block_duration_secs,
        max_rows_per_block: output.max_rows_per_block,
        writer: output.writer()?,
        label_mapper: output.mapper()?,
        metadata: output.metadata()?,
        write_sidecar: sidecar,
        dry_run,
        target_retention_days: output.target_retention_days,
        retention_refuse: output.retention_action == "refuse",
    })
}

fn print_report(report: &ConvertReport, as_json: bool) -> Result<()> {
    if as_json {
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "blocks": report.blocks.len(),
                "rows": report.total_rows,
                "windows": report.windows,
                "preview": report.preview,
                "sidecar": report.sidecar,
                "retention_warning": report.retention_warning,
                "retention_refused": report.retention_refused,
                "stats": report.stats,
            }))?
        );
    } else {
        println!(
            "windows={} blocks={} rows={} errors={}",
            report.windows.len(),
            report.blocks.len(),
            report.total_rows,
            report.stats.errors.len()
        );
        for window in &report.windows {
            println!(
                "  {} {}..{} rows={} part={}",
                window.block_ulid, window.start_ns, window.end_ns, window.rows, window.parts
            );
        }
        if !report.preview.is_empty() {
            println!("mapping preview:");
            for line in &report.preview {
                println!("  {line}");
            }
        }
        if let Some(sidecar) = &report.sidecar {
            println!("sidecar: {}", sidecar.display());
        }
        if let Some(warning) = &report.retention_warning {
            eprintln!("retention warning: {warning}");
        }
    }
    Ok(())
}

fn run_verify(args: VerifyArgs) -> Result<i32> {
    let options = VerifyOptions {
        source: args.source.to_options()?,
        label_mapper: args.output.mapper()?,
    };
    let report = if args.target.starts_with("http://") || args.target.starts_with("https://") {
        verify_live(&options, &args.target)?
    } else {
        verify(&options, Path::new(&args.target))?
    };
    if let Some(path) = args.report {
        std::fs::write(&path, serde_json::to_vec_pretty(&report)?)?;
    }
    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        println!(
            "source_series={} output_series={} source_samples={} output_samples={} missing={} extra={} mismatched={}",
            report.source_series,
            report.output_series,
            report.source_samples,
            report.output_samples,
            report.missing.len(),
            report.extra.len(),
            report.mismatched.len()
        );
    }
    Ok(if report.ok() { 0 } else { 4 })
}

fn clone(args: CloneArgs, dry_run: bool) -> Result<i32> {
    if args.target.starts_with("http://") || args.target.starts_with("https://") {
        return clone_otlp(args, dry_run);
    }
    let target = PathBuf::from(&args.target);
    if !dry_run && !args.force && wal_present(&target) {
        anyhow::bail!(
            "{} contains WAL data; stop the server or pass --force before mutating its index",
            target.display()
        );
    }
    let options = build_convert_options(args.source, args.output, target.clone(), dry_run, true)?;
    let report = convert(&options)?;
    if !dry_run {
        install_index(&target, &report)?;
    }
    print_report(&report, false)?;
    Ok(if report.retention_refused {
        5
    } else if report.stats.errors.is_empty() {
        0
    } else {
        3
    })
}

fn wal_present(target: &Path) -> bool {
    let wal = target.join("wal");
    walkdir::WalkDir::new(&wal)
        .max_depth(2)
        .into_iter()
        .filter_map(std::result::Result::ok)
        .any(|entry| entry.file_type().is_file())
}

fn install_index(target: &Path, report: &ConvertReport) -> Result<()> {
    let mut index = parqtel_core::BlockIndex::new(target);
    index.load().map_err(|error| {
        anyhow::anyhow!("loading {}: {error}", target.join("index.json").display())
    })?;
    for block in &report.blocks {
        index.add(block.metadata.clone());
    }
    let store = parqtel_core::BlockIndexStore::new(&index);
    parqtel_core::storage::persist_blocking(&index, &store)
        .map_err(|error| anyhow::anyhow!("persisting target index: {error}"))?;
    println!(
        "installed {} block(s) into {}; restart the server to load the new index",
        report.blocks.len(),
        target.display()
    );
    Ok(())
}

fn clone_otlp(args: CloneArgs, dry_run: bool) -> Result<i32> {
    let mapper = args.output.mapper()?;
    let metadata = args.output.metadata()?;
    let source = Source::open(args.source.to_options()?)?;
    if dry_run {
        let mut series = 0usize;
        let mut samples = 0u64;
        source.scan(|record| {
            series += 1;
            samples += record.samples.len() as u64;
            Ok(())
        })?;
        println!("otlp_clone dry_run series={series} samples={samples}");
        return Ok(0);
    }
    let checkpoint_path = args
        .checkpoint
        .unwrap_or_else(|| PathBuf::from("tsdb2parqtel-checkpoint.json"));
    let mut checkpoint = otlp_sink::Checkpoint::load(&checkpoint_path)?;
    let sink = otlp_sink::OtlpSink::new(&args.target)?;
    let mut totals = otlp_sink::CloneStats::default();
    source.scan(|record| {
        let Some(metric) = record.metric_name().map(str::to_string) else {
            return Ok(());
        };
        let kind = match metadata.kind_for(&metric) {
            parqtel_sink::MetricKind::Sum => otlp_sink::MetricKind::Sum,
            _ => otlp_sink::MetricKind::Gauge,
        };
        let mapped = mapper
            .map(&record.labels)
            .map_err(|error| tsdb_reader::Error::Source(error.to_string()))?;
        let labels: BTreeMap<String, String> = mapped
            .labels
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect();
        let resource: BTreeMap<String, String> = mapped
            .resource
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect();
        let samples: Vec<(i64, f64)> = record
            .samples
            .iter()
            .filter_map(|sample| {
                sample
                    .timestamp_ms
                    .checked_mul(1_000_000)
                    .map(|timestamp| (timestamp, sample.value))
            })
            .collect();
        let stats = sink
            .send_series(&metric, kind, &labels, &resource, &samples, &mut checkpoint)
            .map_err(|error| tsdb_reader::Error::Source(error.to_string()))?;
        totals.requests += stats.requests;
        totals.series += stats.series;
        totals.samples += stats.samples;
        totals.skipped_samples += stats.skipped_samples;
        totals.retries += stats.retries;
        Ok(())
    })?;
    println!(
        "otlp_clone requests={} series={} samples={} skipped={} retries={} checkpoint={}",
        totals.requests,
        totals.series,
        totals.samples,
        totals.skipped_samples,
        totals.retries,
        checkpoint_path.display()
    );
    Ok(0)
}

fn parse_time(value: &str) -> Result<i64> {
    if let Ok(parsed) = chrono::DateTime::parse_from_rfc3339(value) {
        return Ok(parsed.timestamp_millis());
    }
    let numeric: i128 = value
        .parse()
        .with_context(|| format!("time {value:?} is not RFC3339 or an integer"))?;
    if numeric > 100_000_000_000_000_000 {
        Ok((numeric / 1_000_000) as i64)
    } else if numeric > 100_000_000_000 {
        Ok(numeric as i64)
    } else {
        Ok((numeric * 1_000) as i64)
    }
}
