use clap::Parser;
use etp_lib::ops;
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "etp-csv",
    about = "Generate CSV index from indexed database",
    version = concat!(env!("CARGO_PKG_VERSION"), " (", env!("GIT_HASH"), ")")
)]
struct Cli {
    /// Root directory to scan
    directory: PathBuf,

    /// CSV output path
    #[arg(short, long)]
    output: Option<PathBuf>,

    /// Database path
    #[arg(long)]
    db: Option<PathBuf>,

    /// Directory names to exclude from output
    #[arg(short, long)]
    exclude: Vec<String>,

    /// Filter output to files matching this regex pattern
    #[arg(long)]
    find: Option<String>,

    /// Case-insensitive --find matching
    #[arg(short = 'i', long = "insensitive")]
    insensitive: bool,

    /// Show hidden files (names starting with '.')
    #[arg(short, long)]
    all: bool,

    /// Scan the directory before generating CSV (default: read existing DB)
    #[arg(long, default_value_t = false)]
    scan: bool,

    /// Do not scan, read existing DB (default)
    #[arg(long, default_value_t = false)]
    no_scan: bool,

    /// Include NAS/OS system files in output (e.g. @eaDir, .etp.db)
    #[arg(long, default_value_t = false)]
    include_system_files: bool,

    /// Hide NAS/OS system files from output (default)
    #[arg(long, default_value_t = false)]
    no_include_system_files: bool,

    /// Print diagnostic info on stderr
    #[arg(short, long)]
    verbose: bool,

    /// Write Chrome Trace profiling data to a file
    #[cfg(feature = "profiling")]
    #[arg(long)]
    profile: bool,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let cli = Cli::parse();

    #[cfg(feature = "profiling")]
    let _profiling_guard = if cli.profile {
        Some(etp_lib::profiling::init_profiling(
            &etp_lib::profiling::trace_path("etp-csv"),
        ))
    } else {
        None
    };

    let output = cli
        .output
        .unwrap_or_else(|| cli.directory.join("index.csv"));

    let ctx = ops::open_and_resolve_scan(
        &cli.directory,
        cli.db,
        cli.scan,
        cli.no_scan,
        &cli.exclude,
        cli.verbose,
    )
    .await;

    let filter = ops::FilterConfig::from_flags(
        cli.include_system_files,
        cli.no_include_system_files,
        cli.all,
    );

    if let Some(ref find_pattern) = cli.find {
        let pattern = ops::compile_pattern(find_pattern, cli.insensitive);
        let matches =
            ops::collect_find_matches(&ctx.pool, ctx.scan_id, &pattern, &cli.exclude, &filter)
                .await;
        let output_str = output.to_string_lossy();
        ops::write_find_csv(&matches, &output_str).unwrap_or_else(|e| {
            eprintln!("error writing CSV: {}", e);
            std::process::exit(1);
        });
    } else {
        ops::write_csv_from_db(
            &ctx.pool,
            ctx.scan_id,
            &output,
            &cli.exclude,
            &filter,
            cli.verbose,
        )
        .await;
    }

    etp_lib::db::close_db(ctx.pool).await;

    #[cfg(feature = "profiling")]
    if let Some(guard) = _profiling_guard {
        guard.finish();
    }
}
