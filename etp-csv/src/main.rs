use clap::Parser;
use etp_lib::ops;
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "etp-csv",
    about = "Incremental filesystem scanner with CSV output",
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
    #[arg(short, long, default_values_t = [String::from("@eaDir")])]
    exclude: Vec<String>,

    /// Filter output to files matching this regex pattern
    #[arg(long)]
    find: Option<String>,

    /// Case-insensitive --find matching
    #[arg(short = 'i', long = "insensitive")]
    insensitive: bool,

    /// Skip scanning, use existing DB data
    #[arg(long, hide = true)]
    no_scan: bool,

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

    let scan_id = if cli.no_scan {
        if cli.verbose {
            eprintln!("--no-scan: skipping scan, using cached data");
        }
        match etp_lib::db::dao::latest_scan_id(&pool, &run_type).await {
            Ok(Some(id)) => id,
            Ok(None) => {
                eprintln!(
                    "error: --no-scan specified but no previous scan exists for this directory"
                );
                std::process::exit(1);
            }
            Err(e) => {
                eprintln!("error querying database: {}", e);
                std::process::exit(1);
            }
        }
    } else {
        ops::run_scan_to_db(&cli.directory, &pool, &run_type, &cli.exclude, cli.verbose).await
    };

    if let Some(ref find_pattern) = cli.find {
        let pattern = ops::compile_pattern(find_pattern, cli.insensitive);
        let matches = ops::collect_find_matches(&pool, scan_id, &pattern, &cli.exclude).await;
        let output_str = output.to_string_lossy();
        ops::write_find_csv(&matches, &output_str).unwrap_or_else(|e| {
            eprintln!("error writing CSV: {}", e);
            std::process::exit(1);
        });
    } else {
        ops::write_csv_from_db(&pool, scan_id, &output, &cli.exclude, cli.verbose).await;
    }

    etp_lib::db::close_db(pool).await;

    #[cfg(feature = "profiling")]
    if let Some(guard) = _profiling_guard {
        guard.finish();
    }
}
