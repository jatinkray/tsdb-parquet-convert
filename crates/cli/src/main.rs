use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(
    name = "tsdb2parqtel",
    about = "Convert Prometheus/Thanos TSDB blocks into Parqtel-compatible Parquet blocks",
    version
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Inspect a Prometheus/Thanos block source.
    #[command(about = "Report what a source contains, without writing anything")]
    Inspect {
        /// Source path or bucket prefix.
        source: String,
    },
    /// Plan a conversion without writing anything.
    #[command(about = "Dry run: windows, mapping preview, retention check")]
    Plan {
        /// Source path or bucket prefix.
        source: String,
    },
    /// Produce Parqtel metric blocks.
    #[command(about = "Write Parqtel metric blocks into a directory")]
    Convert {
        /// Source path or bucket prefix.
        source: String,
        /// Output directory for the Parquet blocks.
        #[arg(long)]
        out: String,
    },
    /// Convert and install / ingest in one pass, with checkpoint/resume.
    #[command(about = "Convert and install / ingest, with checkpoint/resume")]
    Clone {
        /// Source path or bucket prefix.
        source: String,
        /// Target data dir or OTLP endpoint.
        #[arg(long)]
        target: String,
    },
    /// Reconcile source and converted blocks.
    #[command(about = "Compare source blocks with converted / cloned blocks")]
    Verify {
        /// Source path or bucket prefix.
        source: String,
        /// Converted blocks dir or live Parqtel endpoint.
        target: String,
    },
}

fn main() {
    let cli = Cli::parse();
    match cli.command {
        Commands::Inspect { source } => run_inspect(&source),
        Commands::Plan { source } => run_plan(&source),
        Commands::Convert { source, out } => run_convert(&source, &out),
        Commands::Clone { source, target } => run_clone(&source, &target),
        Commands::Verify { source, target } => run_verify(&source, &target),
    }
}

fn run_inspect(source: &str) {
    eprintln!("inspect is not implemented yet: source {source}");
    std::process::exit(1);
}

fn run_plan(source: &str) {
    eprintln!("plan is not implemented yet: source {source}");
    std::process::exit(1);
}

fn run_convert(source: &str, out: &str) {
    eprintln!("convert is not implemented yet: source {source}, out {out}");
    std::process::exit(1);
}

fn run_clone(source: &str, target: &str) {
    eprintln!("clone is not implemented yet: source {source}, target {target}");
    std::process::exit(1);
}

fn run_verify(source: &str, target: &str) {
    eprintln!("verify is not implemented yet: source {source}, target {target}");
    std::process::exit(1);
}
