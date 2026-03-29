use clap::Parser;
use etp_lib::ops;
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "etp-scan",
    about = "Scan a directory and update the database",
    version = concat!(env!("CARGO_PKG_VERSION"), " (", env!("GIT_HASH"), ")")
)]
struct Cli {
    /// Directory to scan
    directory: PathBuf,

    /// Database path (defaults to <directory>/.etp.db)
    #[arg(long)]
    db: Option<PathBuf>,

    /// Directory names to exclude from scan
    #[arg(short, long)]
    exclude: Vec<String>,

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
            &etp_lib::profiling::trace_path("etp-scan"),
        ))
    } else {
        None
    };

    let config = etp_lib::config::RuntimeConfig::load_or_default();

    let (directory, db_path) = match ops::resolve_nickname(&cli.directory, &config) {
        Some((root, db)) => (root, db),
        None => {
            let db = cli.db.unwrap_or_else(|| cli.directory.join(".etp.db"));
            (cli.directory.clone(), db)
        }
    };

    ops::validate_directory(&directory);

    let pool = etp_lib::db::open_db(&db_path, cli.verbose)
        .await
        .unwrap_or_else(|e| {
            eprintln!("error opening database: {e}");
            std::process::exit(1);
        });

    let canon = directory.canonicalize().unwrap_or(directory);
    let run_type = canon.to_string_lossy();

    let scan_id = ops::run_scan_to_db(
        &canon,
        &pool,
        &run_type,
        &cli.exclude,
        cli.verbose,
        config.cas_dir.as_deref(),
    )
    .await;

    if cli.verbose {
        eprintln!("scan complete, scan_id = {scan_id}");
    }

    etp_lib::db::close_db(pool).await;

    #[cfg(feature = "profiling")]
    if let Some(guard) = _profiling_guard {
        guard.finish();
    }
}
