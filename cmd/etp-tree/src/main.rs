use clap::Parser;
use etp_lib::ops;
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "etp-tree",
    about = "Display directory tree from indexed database",
    version = concat!(env!("CARGO_PKG_VERSION"), " (", env!("GIT_HASH"), ")")
)]
struct Cli {
    /// Root directory to display
    directory: PathBuf,

    /// Database path
    #[arg(long)]
    db: Option<PathBuf>,

    /// Directory names to exclude from output
    #[arg(short, long)]
    exclude: Vec<String>,

    /// Print names as-is (no character escaping)
    #[arg(short = 'N', long = "no-escape")]
    no_escape: bool,

    /// Glob pattern to exclude from output (repeatable)
    #[arg(short = 'I', long = "ignore")]
    ignore: Vec<String>,

    /// Show hidden files (names starting with '.')
    #[arg(short, long)]
    all: bool,

    /// Filter output to files matching this regex pattern
    #[arg(long)]
    find: Option<String>,

    /// Case-insensitive --find matching
    #[arg(short = 'i', long = "insensitive")]
    insensitive: bool,

    /// Scan the directory before displaying (default: read existing DB)
    #[arg(long, default_value_t = false)]
    scan: bool,

    /// Skip scanning, use existing DB data (default behavior, kept for backward compat)
    #[arg(long, hide = true, default_value_t = false)]
    no_scan: bool,

    /// Print size summary after tree output
    #[arg(long)]
    du: bool,

    /// With --du, also print per-subdirectory sizes
    #[arg(long)]
    du_subs: bool,

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
            &etp_lib::profiling::trace_path("etp-tree"),
        ))
    } else {
        None
    };

    if cli.verbose {
        eprintln!("root is {}", cli.directory.display());
    }
    ops::validate_directory(&cli.directory);

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

    let scan_id = if cli.scan {
        ops::run_scan_to_db(&cli.directory, &pool, &run_type, &cli.exclude, cli.verbose).await
    } else {
        if cli.verbose && !cli.no_scan {
            eprintln!("using existing database (pass --scan to rescan)");
        }
        match etp_lib::db::dao::latest_scan_id(&pool, &run_type).await {
            Ok(Some(id)) => id,
            Ok(None) => {
                eprintln!(
                    "error: no previous scan exists for this directory; run etp-scan first, or pass --scan"
                );
                std::process::exit(ops::EXIT_NO_SCAN);
            }
            Err(e) => {
                eprintln!("error querying database: {}", e);
                std::process::exit(1);
            }
        }
    };

    if let Some(ref find_pattern) = cli.find {
        let pattern = ops::compile_pattern(find_pattern, cli.insensitive);
        let matches = ops::collect_find_matches(&pool, scan_id, &pattern, &cli.exclude).await;
        ops::render_find_tree(&matches, &cli.directory, "-").unwrap_or_else(|e| {
            eprintln!("error rendering tree: {}", e);
            std::process::exit(1);
        });
    } else {
        // Combine exclude and ignore into patterns for tree rendering
        let mut all_ignore = cli.ignore.clone();
        all_ignore.extend(cli.exclude.iter().cloned());

        ops::render_tree_from_db(
            &pool,
            scan_id,
            &cli.directory,
            &all_ignore,
            cli.no_escape,
            cli.all,
        )
        .await
        .unwrap_or_else(|e| {
            eprintln!("error rendering tree: {}", e);
            std::process::exit(1);
        });
    }

    if cli.du {
        ops::render_du(&pool, scan_id, cli.du_subs).await;
    }

    etp_lib::db::close_db(pool).await;

    #[cfg(feature = "profiling")]
    if let Some(guard) = _profiling_guard {
        guard.finish();
    }
}
