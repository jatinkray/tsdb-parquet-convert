#![deny(unsafe_code)]

mod compact;
mod convert;
mod ddl;
mod destination;
mod labels;
mod metadata;
mod presets;
mod state;
mod verify;

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use convert::{convert, ConvertOptions, ConvertReport, RollupFunc};
use destination::Destination;
use labels::{LabelMapper, LabelMode};
use metadata::Metadata;
use serde_json::json;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use tsdb_reader::{
    DownsamplePolicy, ExternalLabels, OnError, Source, SourceOptions, StaleMarkerPolicy,
};
use verify::{verify, verify_live, verify_schema, VerifyOptions};

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
    /// Produce Parqtel metric blocks, or Parquet for --schema.
    Convert(ConvertArgs),
    /// Print a starter schema file for a source (one column per label).
    Schema(SchemaArgs),
    /// List the built-in schemas accepted by `--preset`.
    Presets,
    /// Print query-engine DDL (or a view) for schema-mode output.
    EmitDdl(EmitDdlArgs),
    /// Merge small schema-mode Parquet files into larger ones.
    Compact(CompactArgs),
    /// Export a source to a data lake: convert, optional rollup/compaction,
    /// manifest, cost estimate and optional verification.
    Migrate(MigrateArgs),
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

    fn parquet_options(&self) -> parquet_sink::WriteOptions {
        parquet_sink::WriteOptions {
            compression: self.compression.clone(),
            compression_level: self.compression_level,
            row_group_size: self.row_group_size,
        }
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
    /// Output directory or object-store URL (`s3://`, `gs://`, `az://`).
    #[arg(long)]
    out: String,
    #[command(flatten)]
    output: OutputArgs,
    #[arg(long, help = "Write index.fragment.json for the output blocks")]
    sidecar: bool,
    #[arg(
        long,
        value_name = "FILE",
        help = "Write Parquet for this schema file (YAML/JSON) instead of Parqtel blocks"
    )]
    schema: Option<PathBuf>,
    #[arg(
        long,
        value_name = "NAME",
        conflicts_with = "schema",
        help = "Use a built-in schema instead of a file (see `tsdb2parqtel presets`)"
    )]
    preset: Option<String>,
    /// Incremental state file: skip source blocks a previous run already wrote.
    #[arg(long, value_name = "FILE")]
    state: Option<PathBuf>,
    /// Roll samples up per series (e.g. `30s`, `5m`, `1h`), one row per bucket.
    #[arg(long, value_name = "DURATION", value_parser = parse_duration)]
    rollup: Option<u64>,
    /// Aggregate held by a rollup row.
    #[arg(long, value_name = "FUNC", default_value = "avg",
          value_parser = ["avg", "sum", "min", "max", "last", "count"])]
    rollup_func: String,
}

#[derive(Debug, Args)]
struct MigrateArgs {
    #[command(flatten)]
    source: SourceArgs,
    /// Output directory or object-store URL.
    #[arg(long)]
    out: String,
    #[command(flatten)]
    output: OutputArgs,
    #[arg(long, value_name = "FILE")]
    schema: Option<PathBuf>,
    #[arg(long, value_name = "NAME", conflicts_with = "schema")]
    preset: Option<String>,
    /// Roll samples up per series (e.g. `5m`), one row per bucket.
    #[arg(long, value_name = "DURATION", value_parser = parse_duration)]
    rollup: Option<u64>,
    #[arg(long, value_name = "FUNC", default_value = "avg",
          value_parser = ["avg", "sum", "min", "max", "last", "count"])]
    rollup_func: String,
    /// Incremental state file for repeated runs.
    #[arg(long, value_name = "FILE")]
    state: Option<PathBuf>,
    /// Compact the written files into larger ones (local output only).
    #[arg(long)]
    compact: bool,
    /// Rows per compacted file.
    #[arg(long, default_value_t = 2_000_000)]
    rows_per_file: usize,
    /// Reconcile the written files against the source before finishing.
    #[arg(long)]
    verify: bool,
}

#[derive(Debug, Args)]
struct CompactArgs {
    /// Directory holding the files to merge.
    input: PathBuf,
    /// Directory for the merged files.
    #[arg(long)]
    out: PathBuf,
    #[arg(long, value_name = "FILE")]
    schema: Option<PathBuf>,
    #[arg(long, value_name = "NAME", conflicts_with = "schema")]
    preset: Option<String>,
    /// Rows per output file.
    #[arg(long, default_value_t = 2_000_000)]
    rows_per_file: usize,
    #[arg(long, default_value = "zstd", value_parser = ["zstd", "snappy", "lz4", "none"])]
    compression: String,
    #[arg(long, default_value_t = 100_000)]
    row_group_size: usize,
}

#[derive(Debug, Args)]
struct EmitDdlArgs {
    /// Schema the files were written with.
    #[arg(long, value_name = "FILE")]
    schema: Option<PathBuf>,
    /// Built-in schema the files were written with.
    #[arg(long, value_name = "NAME", conflicts_with = "schema")]
    preset: Option<String>,
    /// Target engine.
    #[arg(long, value_parser = ["duckdb", "athena", "bigquery"])]
    target: String,
    /// Table or view name.
    #[arg(long, default_value = "metrics")]
    table: String,
    /// Data location: a local directory for DuckDB, `s3://…` for Athena,
    /// `gs://…` for BigQuery.
    #[arg(long, default_value = "")]
    location: String,
    /// Write to this file instead of stdout.
    #[arg(long)]
    out: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct SchemaArgs {
    #[command(flatten)]
    source: SourceArgs,
    /// Write the schema here; stdout when omitted.
    #[arg(long)]
    out: Option<PathBuf>,
    /// Schema name recorded in the file and the Parquet metadata.
    #[arg(long, default_value = "tsdb_generic")]
    name: String,
    /// Emit only timestamp, metric_name and value.
    #[arg(long)]
    no_labels: bool,
    /// Keep this many label columns, highest series count first (0 = all).
    #[arg(long, default_value_t = 0)]
    max_labels: usize,
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
    #[arg(long, default_value = "json", value_parser = ["json", "protobuf"])]
    otlp_protocol: String,
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
    /// Verify schema-mode Parquet output against this schema file.
    #[arg(long, value_name = "FILE")]
    schema: Option<PathBuf>,
    /// Verify against a built-in schema (see `tsdb2parqtel presets`).
    #[arg(long, value_name = "NAME", conflicts_with = "schema")]
    preset: Option<String>,
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
    // Logs go to stderr so stdout stays machine-readable: manifests, JSON
    // reports and `emit-ddl` output can be piped straight into `jq`.
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr);
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
        Commands::Plan(args) => plan(args, cli.progress),
        Commands::Convert(args) => run_convert(args, dry_run, cli.progress),
        Commands::Schema(args) => generate_schema(args),
        Commands::Presets => {
            println!("{}", presets::listing());
            Ok(0)
        }
        Commands::EmitDdl(args) => emit_ddl(args),
        Commands::Compact(args) => run_compact(args),
        Commands::Migrate(args) => run_migrate(args),
        Commands::Clone(args) => clone(args, dry_run, cli.progress),
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
                "estimated_output_bytes": stats.samples,
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
            "total: blocks={} series={} samples={} estimated_output_bytes={} skipped_series={} stale_dropped={} errors={}",
            infos.len(),
            stats.series,
            stats.samples,
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

fn plan(args: PlanArgs, progress: bool) -> Result<i32> {
    let source_label = args.source.source.clone();
    let options = build_convert_options(
        args.source,
        args.output,
        OutputTarget {
            destination: Destination::Local(PathBuf::new()),
            source_label,
            dry_run: true,
            sidecar: false,
            state: None,
            rollup: None,
            rollup_func: RollupFunc::Avg,
        },
        None,
        progress,
    )?;
    let report = convert(&options)?;
    print_report(&report, args.json)?;
    Ok(if report.retention_refused { 5 } else { 0 })
}

fn run_convert(args: ConvertArgs, dry_run: bool, progress: bool) -> Result<i32> {
    let destination = Destination::parse(&args.out)?;
    let source_label = args.source.source.clone();
    let options = build_convert_options(
        args.source,
        args.output,
        OutputTarget {
            destination,
            source_label,
            dry_run,
            sidecar: args.sidecar,
            state: args.state,
            rollup: args.rollup,
            rollup_func: RollupFunc::parse(&args.rollup_func)?,
        },
        resolve_schema(args.schema, args.preset)?,
        progress,
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

fn run_migrate(args: MigrateArgs) -> Result<i32> {
    let spec = resolve_schema(args.schema, args.preset)?
        .context("migrate writes schema-mode Parquet; pass --schema or --preset")?;
    let destination = Destination::parse(&args.out)?;
    if args.verify && args.rollup.is_some() {
        anyhow::bail!(
            "--verify compares every source sample; it cannot reconcile rolled-up output. Run `verify --preset …` on a raw export, or aggregate the source with your query engine"
        );
    }
    if args.compact && destination.is_remote() {
        anyhow::bail!(
            "--compact reads files back; run it on the local copy after download for object-store output"
        );
    }
    let rollup_func = RollupFunc::parse(&args.rollup_func)?;
    let source_label = args.source.source.clone();
    let options = build_convert_options(
        args.source,
        args.output,
        OutputTarget {
            destination: destination.clone(),
            source_label,
            dry_run: false,
            sidecar: false,
            state: args.state.clone(),
            rollup: args.rollup,
            rollup_func,
        },
        Some(spec.clone()),
        false,
    )?;
    let report = convert(&options)?;

    let mut compacted = None;
    let mut data_dir = match &destination {
        Destination::Local(path) => Some(path.clone()),
        Destination::Remote { .. } => None,
    };
    if args.compact {
        let Some(input) = data_dir.clone() else {
            anyhow::bail!("--compact needs a local output directory");
        };
        let out = input.join("compacted");
        let compact_report =
            compact::compact(&input, &out, &spec, &options.parquet, args.rows_per_file)?;
        compacted = Some(compact_report);
        data_dir = Some(out);
    }

    let mut verification = None;
    if args.verify {
        let Some(dir) = data_dir.clone() else {
            anyhow::bail!("--verify needs a local output directory");
        };
        let verify_options = VerifyOptions {
            source: options.source.clone(),
            label_mapper: LabelMapper::new(LabelMode::Preserve, None)?,
        };
        let verified = verify_schema(&verify_options, &spec, &dir)?;
        verification = Some(verified);
    }

    let bytes = report.files.iter().map(|file| file.bytes).sum::<u64>();
    let summary = json!({
        "rows_scanned": report.total_rows,
        "rows_written": report.rows_written,
        "files": report.files.len(),
        "bytes": bytes,
        "cost_per_month": {
            "s3_standard_usd": cost(bytes, 0.023),
            "s3_glacier_instant_usd": cost(bytes, 0.004),
        },
        "manifest": report.manifest,
        "compacted": compacted,
        "verify": verification,
    });
    println!("{}", serde_json::to_string_pretty(&summary)?);

    let ok = verification.as_ref().is_none_or(|report| report.ok());
    Ok(if ok { 0 } else { 4 })
}

/// Monthly storage cost for `bytes` at `usd_per_gb` (list price, four decimals
/// so small exports do not round to zero).
fn cost(bytes: u64, usd_per_gb: f64) -> f64 {
    (bytes as f64 / 1_073_741_824.0 * usd_per_gb * 10_000.0).round() / 10_000.0
}

fn run_compact(args: CompactArgs) -> Result<i32> {
    let spec = resolve_schema(args.schema, args.preset)?
        .context("compact needs --schema or --preset so the layout is known")?;
    let options = parquet_sink::WriteOptions {
        compression: args.compression,
        compression_level: None,
        row_group_size: args.row_group_size,
    };
    let report = compact::compact(&args.input, &args.out, &spec, &options, args.rows_per_file)?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(0)
}

fn emit_ddl(args: EmitDdlArgs) -> Result<i32> {
    let spec =
        resolve_schema(args.schema, args.preset)?.context("emit-ddl needs --schema or --preset")?;
    let target = ddl::Target::parse(&args.target)?;
    let text = ddl::render(&spec, target, &args.table, &args.location)?;
    match args.out {
        Some(path) => {
            std::fs::write(&path, text).with_context(|| format!("writing {}", path.display()))?;
            println!("wrote {}", path.display());
        }
        None => print!("{text}"),
    }
    Ok(0)
}

fn generate_schema(args: SchemaArgs) -> Result<i32> {
    let source = Source::open(args.source.to_options()?)?;
    let labels = source.label_names();
    let spec = starter_schema(&args.name, &labels, args.no_labels, args.max_labels)?;
    let text = serde_yaml::to_string(&spec)?;
    match args.out {
        Some(path) => {
            std::fs::write(&path, text)
                .with_context(|| format!("writing schema {}", path.display()))?;
            println!("wrote {}", path.display());
        }
        None => print!("{text}"),
    }
    Ok(0)
}

/// Builds the starter schema: fixed timestamp/metric/value columns plus one
/// nullable utf8 column per label, ordered by how many series carry the label.
fn starter_schema(
    name: &str,
    labels: &BTreeMap<String, u64>,
    no_labels: bool,
    max_labels: usize,
) -> Result<parquet_sink::SchemaSpec> {
    let mut columns = vec![
        parquet_sink::ColumnSpec {
            name: "timestamp".to_string(),
            column_type: parquet_sink::ColumnType::TimestampMs,
            source: "timestamp".to_string(),
            nullable: false,
            precision: None,
            scale: None,
            timezone: None,
            description: Some("sample timestamp, milliseconds since the Unix epoch".to_string()),
        },
        parquet_sink::ColumnSpec {
            name: "metric".to_string(),
            column_type: parquet_sink::ColumnType::Utf8,
            source: "metric_name".to_string(),
            nullable: false,
            precision: None,
            scale: None,
            timezone: None,
            description: Some("`__name__` of the series".to_string()),
        },
        parquet_sink::ColumnSpec {
            name: "value".to_string(),
            column_type: parquet_sink::ColumnType::Float64,
            source: "value".to_string(),
            nullable: false,
            precision: None,
            scale: None,
            timezone: None,
            description: None,
        },
    ];
    if !no_labels {
        let mut ordered: Vec<(&String, &u64)> = labels
            .iter()
            .filter(|(key, _)| key.as_str() != "__name__")
            .collect();
        ordered.sort_by(|(left_name, left_count), (right_name, right_count)| {
            right_count
                .cmp(left_count)
                .then_with(|| left_name.cmp(right_name))
        });
        if max_labels > 0 {
            ordered.truncate(max_labels);
        }
        let mut used: std::collections::BTreeSet<String> =
            columns.iter().map(|column| column.name.clone()).collect();
        for (key, _) in ordered {
            let column_name = unique_column_name(key, &mut used);
            columns.push(parquet_sink::ColumnSpec {
                name: column_name,
                column_type: parquet_sink::ColumnType::Utf8,
                source: format!("label:{key}"),
                nullable: true,
                precision: None,
                scale: None,
                timezone: None,
                description: Some(format!("label `{key}`")),
            });
        }
    }
    let spec = parquet_sink::SchemaSpec {
        version: parquet_sink::CURRENT_VERSION,
        name: Some(name.to_string()),
        description: Some(
            "starter schema generated by `tsdb2parqtel schema`; edit the columns before converting"
                .to_string(),
        ),
        columns,
    };
    spec.validate()
        .map_err(|error| anyhow::anyhow!("{error}"))?;
    Ok(spec)
}

/// Parquet readers treat `.` as a nested-field separator, so label keys become
/// snake_case identifiers; collisions get a numeric suffix.
fn unique_column_name(key: &str, used: &mut std::collections::BTreeSet<String>) -> String {
    let mut base: String = key
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect();
    if base.is_empty() || base.starts_with(|character: char| character.is_ascii_digit()) {
        base.insert_str(0, "label_");
    }
    let mut candidate = base.clone();
    let mut suffix = 2;
    while !used.insert(candidate.clone()) {
        candidate = format!("{base}_{suffix}");
        suffix += 1;
    }
    candidate
}

/// Everything about where a run writes, bundled so the option builder stays
/// readable.
struct OutputTarget {
    destination: Destination,
    source_label: String,
    dry_run: bool,
    sidecar: bool,
    state: Option<PathBuf>,
    rollup: Option<u64>,
    rollup_func: RollupFunc,
}

/// A schema either comes from a file or from the built-in presets; `clap`
/// rejects both at once.
fn resolve_schema(
    schema: Option<PathBuf>,
    preset: Option<String>,
) -> Result<Option<parquet_sink::SchemaSpec>> {
    match (schema, preset) {
        (Some(path), None) => Ok(Some(
            parquet_sink::SchemaSpec::from_path(&path)
                .map_err(|error| anyhow::anyhow!("{error}"))
                .with_context(|| format!("loading schema {}", path.display()))?,
        )),
        (None, Some(name)) => Ok(Some(presets::load(&name)?)),
        (None, None) => Ok(None),
        (Some(_), Some(_)) => anyhow::bail!("--schema and --preset cannot be combined"),
    }
}

fn build_convert_options(
    source: SourceArgs,
    output: OutputArgs,
    target: OutputTarget,
    schema: Option<parquet_sink::SchemaSpec>,
    progress: bool,
) -> Result<ConvertOptions> {
    let OutputTarget {
        destination,
        source_label,
        dry_run,
        sidecar,
        state,
        rollup,
        rollup_func,
    } = target;
    if rollup == Some(0) {
        anyhow::bail!("--rollup must be at least one second");
    }
    if state.is_some() && schema.is_none() {
        anyhow::bail!("--state needs --schema or --preset so every run writes the same layout");
    }
    if destination.is_remote() && schema.is_none() {
        anyhow::bail!(
            "object-store output is for schema-mode Parquet; Parqtel blocks are a local directory format — add --schema or --preset"
        );
    }
    if destination.is_remote() && sidecar {
        anyhow::bail!("--sidecar writes a local index fragment and cannot target an object store");
    }
    if schema.is_some() {
        if sidecar {
            anyhow::bail!(
                "--sidecar writes a Parqtel index fragment and cannot be combined with --schema"
            );
        }
        if output.label_mode != "preserve" {
            anyhow::bail!(
                "--label-mode {} rewrites Parqtel correlation columns; --schema reads the raw series labels, so leave label-mode at `preserve`",
                output.label_mode
            );
        }
        for (flag, given) in [
            ("--label-mapping", output.label_mapping.is_some()),
            ("--metadata-file", output.metadata_file.is_some()),
            (
                "--target-retention-days",
                output.target_retention_days.is_some(),
            ),
        ] {
            if given {
                anyhow::bail!(
                    "{flag} only affects Parqtel blocks; remove it when using --schema so nothing is silently ignored"
                );
            }
        }
    }
    Ok(ConvertOptions {
        source: source.to_options()?,
        source_label,
        out_dir: match &destination {
            Destination::Local(path) => path.clone(),
            Destination::Remote { .. } => PathBuf::new(),
        },
        destination,
        block_duration_secs: output.block_duration_secs,
        max_rows_per_block: output.max_rows_per_block,
        writer: output.writer()?,
        label_mapper: output.mapper()?,
        metadata: output.metadata()?,
        write_sidecar: sidecar,
        schema,
        parquet: output.parquet_options(),
        state,
        rollup_seconds: rollup,
        rollup_func,
        dry_run,
        target_retention_days: output.target_retention_days,
        retention_refuse: output.retention_action == "refuse",
        progress,
    })
}

fn print_report(report: &ConvertReport, as_json: bool) -> Result<()> {
    if as_json {
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "blocks": report.blocks.len(),
                "files": report.files.len(),
                "rows": report.total_rows,
                "rows_written": report.rows_written,
                "windows": report.windows,
                "preview": report.preview,
                "sidecar": report.sidecar,
                "manifest": report.manifest,
                "blocks_skipped": report.blocks_skipped,
                "retention_warning": report.retention_warning,
                "retention_refused": report.retention_refused,
                "stats": report.stats,
            }))?
        );
    } else {
        println!(
            "windows={} blocks={} files={} rows={} rows_written={} skipped_blocks={} errors={}",
            report.windows.len(),
            report.blocks.len(),
            report.files.len(),
            report.total_rows,
            report.rows_written,
            report.blocks_skipped,
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
        if let Some(manifest) = &report.manifest {
            println!("manifest: {manifest}");
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
    let live = args.target.starts_with("http://") || args.target.starts_with("https://");
    let schema_mode = args.schema.is_some() || args.preset.is_some();
    let report = if schema_mode {
        if live {
            anyhow::bail!(
                "--schema verifies Parquet files; a live URL is reconciled by the Parqtel path"
            );
        }
        let spec = resolve_schema(args.schema, args.preset)?
            .context("a schema or preset is required for schema-mode verification")?;
        verify_schema(&options, &spec, Path::new(&args.target))?
    } else if live {
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
            "source_series={} output_series={} source_samples={} output_samples={} nan_payloads_normalized={} missing={} extra={} mismatched={}",
            report.source_series,
            report.output_series,
            report.source_samples,
            report.output_samples,
            report.nan_payloads_normalized,
            report.missing.len(),
            report.extra.len(),
            report.mismatched.len()
        );
    }
    Ok(if report.ok() { 0 } else { 4 })
}

fn clone(args: CloneArgs, dry_run: bool, progress: bool) -> Result<i32> {
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
    let mut output = args.output;
    if output.target_retention_days.is_none() {
        output.target_retention_days = read_target_retention(&target);
    }
    if !dry_run {
        std::fs::create_dir_all(&target)
            .with_context(|| format!("creating target directory {}", target.display()))?;
    }
    let staging = if dry_run {
        None
    } else {
        Some(
            tempfile::Builder::new()
                .prefix(".tsdb2parqtel-")
                .tempdir_in(&target)
                .with_context(|| format!("creating staging directory in {}", target.display()))?,
        )
    };
    let out_dir = staging
        .as_ref()
        .map_or_else(|| target.clone(), |dir| dir.path().to_path_buf());
    let source_label = args.source.source.clone();
    let options = build_convert_options(
        args.source,
        output,
        OutputTarget {
            destination: Destination::Local(out_dir.clone()),
            source_label,
            dry_run,
            sidecar: false,
            state: None,
            rollup: None,
            rollup_func: RollupFunc::Avg,
        },
        None,
        progress,
    )?;
    let mut report = convert(&options)?;
    if let Some(staging) = staging {
        for block in &mut report.blocks {
            let final_path = target.join(&block.filename);
            std::fs::rename(staging.path().join(&block.filename), &final_path)
                .with_context(|| format!("installing block {}", final_path.display()))?;
            block.metadata.path = final_path;
        }
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

fn read_target_retention(target: &Path) -> Option<u64> {
    let mut candidates = vec![
        target.join("parqtel.toml"),
        target.join("config/default.toml"),
        target
            .parent()
            .map(|parent| parent.join("parqtel.toml"))
            .unwrap_or_default(),
    ];
    if let Some(config) = std::env::var_os("PARQTEL_CONFIG") {
        candidates.push(PathBuf::from(config));
    }
    for path in candidates {
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        let Ok(value) = text.parse::<toml::Value>() else {
            continue;
        };
        if let Some(days) = value
            .get("storage")
            .and_then(|storage| storage.get("retention_days"))
            .and_then(toml::Value::as_integer)
            .and_then(|days| u64::try_from(days).ok())
        {
            tracing::info!(path = %path.display(), retention_days = days, "using target retention");
            return Some(days);
        }
    }
    None
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
    let protocol = otlp_sink::Protocol::parse(&args.otlp_protocol)?;
    let sink = otlp_sink::OtlpSink::with_protocol(&args.target, protocol)?;
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

/// Parses `30s`, `5m`, `1h` (or a bare number of seconds) into seconds.
fn parse_duration(value: &str) -> Result<u64, String> {
    let trimmed = value.trim();
    let (number, multiplier) = match trimmed.chars().last() {
        Some('s') => (&trimmed[..trimmed.len() - 1], 1),
        Some('m') => (&trimmed[..trimmed.len() - 1], 60),
        Some('h') => (&trimmed[..trimmed.len() - 1], 3_600),
        Some(_) => (trimmed, 1),
        None => return Err("duration is empty".to_string()),
    };
    let amount: u64 = number
        .trim()
        .parse()
        .map_err(|_| format!("`{value}` is not a duration such as 30s, 5m or 1h"))?;
    amount
        .checked_mul(multiplier)
        .filter(|seconds| *seconds > 0)
        .ok_or_else(|| format!("`{value}` is not a positive duration"))
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

#[cfg(test)]
mod tests {
    use super::*;

    fn counts(pairs: &[(&str, u64)]) -> BTreeMap<String, u64> {
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_string(), *value))
            .collect()
    }

    #[test]
    fn starter_schema_orders_labels_by_series_count() -> Result<()> {
        let spec = starter_schema(
            "test",
            &counts(&[("job", 10), ("instance", 30), ("__name__", 99)]),
            false,
            0,
        )?;
        let names: Vec<&str> = spec
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect();
        assert_eq!(
            names,
            vec!["timestamp", "metric", "value", "instance", "job"]
        );
        assert!(spec.columns[3].nullable);
        assert_eq!(spec.columns[3].source, "label:instance");
        Ok(())
    }

    #[test]
    fn starter_schema_sanitizes_label_keys() -> Result<()> {
        let spec = starter_schema(
            "test",
            &counts(&[
                ("k8s.pod.name", 5),
                ("k8s.pod_name", 4),
                ("0weird", 3),
                ("dash-name", 2),
            ]),
            false,
            0,
        )?;
        let names: Vec<&str> = spec
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect();
        assert_eq!(
            names,
            vec![
                "timestamp",
                "metric",
                "value",
                "k8s_pod_name",
                "k8s_pod_name_2",
                "label_0weird",
                "dash_name"
            ]
        );
        assert_eq!(spec.columns[3].source, "label:k8s.pod.name");
        assert_eq!(spec.columns[4].source, "label:k8s.pod_name");
        Ok(())
    }

    #[test]
    fn starter_schema_honours_flags() -> Result<()> {
        let labels = counts(&[("job", 1)]);
        let no_labels = starter_schema("test", &labels, true, 0)?;
        assert_eq!(no_labels.columns.len(), 3);
        let limited = starter_schema("test", &labels, false, 1)?;
        assert_eq!(limited.columns.len(), 4);
        Ok(())
    }
}
