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

    /// Scan the directory before generating CSV (default: read existing DB)
    #[arg(long, default_value_t = false)]
    scan: bool,

    /// Hidden backward-compat alias (no-scan is already the default)
    #[arg(long, hide = true, default_value_t = false)]
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

    ops::validate_directory(&cli.directory);

    let output = cli
        .output
        .unwrap_or_else(|| cli.directory.join("index.csv"));
    let db_path = cli.db.unwrap_or_else(|| cli.directory.join(".etp.db"));

    // When not scanning, bail early if no DB exists to avoid creating an empty one.
    if !cli.scan && !db_path.exists() {
        eprintln!(
            "error: no previous scan exists for this directory; run etp-scan first, or pass --scan"
        );
        std::process::exit(ops::EXIT_NO_SCAN);
    }

    let pool = etp_lib::db::open_db(&db_path, cli.verbose)
        .await
        .unwrap_or_else(|e| {
            eprintln!("error opening database: {}", e);
            std::process::exit(1);
        });

    let canon = cli
        .directory
        .canonicalize()
        .unwrap_or(cli.directory.clone());
    let run_type = canon.to_string_lossy();

    let scan_id = if cli.scan {
        ops::run_scan_to_db(&cli.directory, &pool, &run_type, &cli.exclude, cli.verbose).await
    } else {
        ops::resolve_latest_scan_id(&pool, &run_type, cli.verbose).await
    };

    let include_system = ops::resolve_bool_pair(
        cli.include_system_files,
        cli.no_include_system_files,
        "include-system-files",
        false,
    );
    let filter = ops::FilterConfig::new(include_system);

    if let Some(ref find_pattern) = cli.find {
        let pattern = ops::compile_pattern(find_pattern, cli.insensitive);
        let matches =
            ops::collect_find_matches(&pool, scan_id, &pattern, &cli.exclude, &filter).await;
        let output_str = output.to_string_lossy();
        ops::write_find_csv(&matches, &output_str).unwrap_or_else(|e| {
            eprintln!("error writing CSV: {}", e);
            std::process::exit(1);
        });
    } else {
        ops::write_csv_from_db(&pool, scan_id, &output, &cli.exclude, &filter, cli.verbose).await;
    }

    etp_lib::db::close_db(pool).await;

    #[cfg(feature = "profiling")]
    if let Some(guard) = _profiling_guard {
        guard.finish();
    }
}
