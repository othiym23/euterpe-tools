use clap::Parser;
use etp_lib::ops;
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "etp-find",
    about = "Search indexed files by regex pattern",
    version = concat!(env!("CARGO_PKG_VERSION"), " (", env!("GIT_HASH"), ")")
)]
struct Cli {
    /// Regex pattern to match against full file paths
    pattern: String,

    /// Root directory to search (omit to search all scans in --db)
    #[arg(short = 'R', long = "root")]
    directory: Option<PathBuf>,

    /// Write matching files as a tree to file (use - for stdout)
    #[arg(long)]
    tree: Option<String>,

    /// Write matching files as CSV to file (use - for stdout)
    #[arg(long)]
    csv: Option<String>,

    /// Print total size summary of matched files
    #[arg(long)]
    size: bool,

    /// Database path (required when no directory is given)
    #[arg(long)]
    db: Option<PathBuf>,

    /// Directory names to exclude from scan
    #[arg(short, long)]
    exclude: Vec<String>,

    /// Case-insensitive pattern matching
    #[arg(short = 'i', long = "insensitive")]
    insensitive: bool,

    /// Skip scanning, use existing DB data
    #[arg(long, hide = true)]
    no_scan: bool,

    /// Show hidden files (names starting with '.')
    #[arg(short, long)]
    all: bool,

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
            &etp_lib::profiling::trace_path("etp-find"),
        ))
    } else {
        None
    };

    let config = etp_lib::config::RuntimeConfig::load_or_default();

    let pattern = ops::compile_pattern(&cli.pattern, cli.insensitive);

    // Resolve nicknames on -R/--root and/or --db.
    let (directory, explicit_db) = if let Some(ref dir) = cli.directory {
        match ops::resolve_nickname(dir, &config) {
            Some((root, db_path)) => (Some(root), Some(db_path)),
            None => (Some(dir.clone()), cli.db.clone()),
        }
    } else {
        (None, cli.db.clone())
    };

    let resolved_db = explicit_db.map(|db| ops::resolve_db_path(&db, &config));

    // When no directory is given, --db is required and we search all scans.
    let db_path = match (&directory, &resolved_db) {
        (Some(dir), Some(db)) => {
            ops::validate_directory(dir);
            db.clone()
        }
        (Some(dir), None) => {
            ops::validate_directory(dir);
            dir.join(".etp.db")
        }
        (None, Some(db)) => db.clone(),
        (None, None) => {
            eprintln!("error: --db is required when no directory is given");
            std::process::exit(1);
        }
    };

    // Check before open_db, which creates the file if missing.
    let db_existed = db_path.exists();

    let pool = etp_lib::db::open_db(&db_path, cli.verbose)
        .await
        .unwrap_or_else(|e| {
            eprintln!("error opening database: {}", e);
            std::process::exit(1);
        });

    // Resolve scan_id when a directory is given; None means search all scans.
    let scan_id: Option<i64> = if let Some(ref dir) = directory {
        let canon = dir.canonicalize().unwrap_or(dir.clone());
        let run_type = canon.to_string_lossy();

        let skip_scan = cli.no_scan || db_existed;

        let id = if skip_scan {
            if cli.verbose {
                if cli.no_scan {
                    eprintln!("--no-scan: skipping scan, using cached data");
                } else {
                    eprintln!("database exists, skipping scan");
                }
            }
            match etp_lib::db::dao::latest_scan_id(&pool, &run_type).await {
                Ok(Some(id)) => id,
                Ok(None) => {
                    eprintln!(
                        "error: no previous scan exists for this directory in {}; run etp-scan first",
                        db_path.display()
                    );
                    std::process::exit(ops::EXIT_NO_SCAN);
                }
                Err(e) => {
                    eprintln!("error querying database: {}", e);
                    std::process::exit(1);
                }
            }
        } else {
            ops::run_scan_to_db(
                dir,
                &pool,
                &run_type,
                &cli.exclude,
                cli.verbose,
                config.cas_dir.as_deref(),
            )
            .await
        };
        Some(id)
    } else {
        if cli.verbose {
            eprintln!("no directory given, searching all scans");
        }
        None
    };

    let filter = ops::FilterConfig::from_config(
        &config,
        cli.include_system_files,
        cli.no_include_system_files,
        false,
        cli.all,
    );

    // Determine if any output goes to stdout via "-"
    let stdout_tree = cli.tree.as_deref() == Some("-");
    let stdout_csv = cli.csv.as_deref() == Some("-");
    let needs_collect = cli.tree.is_some() || cli.csv.is_some();

    if needs_collect {
        // Collect all matches
        let matches = match scan_id {
            Some(id) => ops::collect_find_matches(&pool, id, &pattern, &cli.exclude, &filter).await,
            None => ops::collect_find_all_matches(&pool, &pattern, &cli.exclude, &filter).await,
        };
        let count = matches.len();
        let total_size: u64 = matches.iter().map(|m| m.size).sum();

        // Print paths to stdout unless tree or csv go to stdout
        if !stdout_tree && !stdout_csv {
            for m in &matches {
                println!("{}", m.full_path);
            }
        }

        // Write tree output (requires a root directory for the tree)
        if let Some(ref tree_path) = cli.tree {
            if let Some(ref dir) = directory {
                ops::render_find_tree(&matches, dir, tree_path).unwrap_or_else(|e| {
                    eprintln!("error rendering tree: {}", e);
                    std::process::exit(1);
                });
            } else {
                eprintln!("error: --tree requires --root <directory>");
                std::process::exit(1);
            }
        }

        // Write CSV output
        if let Some(ref csv_path) = cli.csv {
            ops::write_find_csv(&matches, csv_path).unwrap_or_else(|e| {
                eprintln!("error writing CSV: {}", e);
                std::process::exit(1);
            });
        }

        if cli.size {
            println!("\n{} matches, {}", count, ops::format_size(total_size));
        }
    } else {
        // Stream matches to stdout
        let (count, total_size) = match scan_id {
            Some(id) => ops::stream_find_matches(&pool, id, &pattern, &cli.exclude, &filter).await,
            None => ops::stream_find_all_matches(&pool, &pattern, &cli.exclude, &filter).await,
        };

        if cli.size {
            println!("\n{} matches, {}", count, ops::format_size(total_size));
        }
    }

    etp_lib::db::close_db(pool).await;

    #[cfg(feature = "profiling")]
    if let Some(guard) = _profiling_guard {
        guard.finish();
    }
}
