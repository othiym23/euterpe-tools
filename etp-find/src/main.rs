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
    /// Root directory to search
    directory: PathBuf,

    /// Regex pattern to match against full file paths
    pattern: String,

    /// Write matching files as a tree to file (use - for stdout)
    #[arg(long)]
    tree: Option<String>,

    /// Write matching files as CSV to file (use - for stdout)
    #[arg(long)]
    csv: Option<String>,

    /// Print total size summary of matched files
    #[arg(long)]
    size: bool,

    /// Database path
    #[arg(long)]
    db: Option<PathBuf>,

    /// Directory names to exclude from scan
    #[arg(short, long, default_values_t = [String::from("@eaDir")])]
    exclude: Vec<String>,

    /// Skip scanning, use existing DB data
    #[arg(long, hide = true)]
    no_scan: bool,

    /// Print diagnostic info on stderr
    #[arg(short, long)]
    verbose: bool,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let cli = Cli::parse();

    ops::validate_directory(&cli.directory);

    let pattern = match regex::Regex::new(&cli.pattern) {
        Ok(re) => re,
        Err(e) => {
            eprintln!("error: invalid regex '{}': {}", cli.pattern, e);
            std::process::exit(1);
        }
    };

    let db_path = cli.db.unwrap_or_else(|| cli.directory.join(".etp.db"));

    // Check before open_db, which creates the file if missing.
    let db_existed = db_path.exists();

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

    // Skip scanning if DB already existed (etp-find is read-heavy; scan only
    // when no data is available yet). --no-scan forces skip even for new DBs.
    let skip_scan = cli.no_scan || db_existed;

    let scan_id = if skip_scan {
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
                    "error: no previous scan exists for this directory in {}",
                    db_path.display()
                );
                std::process::exit(1);
            }
            Err(e) => {
                eprintln!("error querying database: {}", e);
                std::process::exit(1);
            }
        }
    } else {
        ops::run_scan_to_db(&cli.directory, &pool, &run_type, cli.verbose).await
    };

    // Determine if any output goes to stdout via "-"
    let stdout_tree = cli.tree.as_deref() == Some("-");
    let stdout_csv = cli.csv.as_deref() == Some("-");
    let needs_collect = cli.tree.is_some() || cli.csv.is_some();

    if needs_collect {
        // Collect all matches
        let matches = ops::collect_find_matches(&pool, scan_id, &pattern).await;
        let count = matches.len();
        let total_size: u64 = matches.iter().map(|m| m.size).sum();

        // Print paths to stdout unless tree or csv go to stdout
        if !stdout_tree && !stdout_csv {
            for m in &matches {
                println!("{}", m.full_path);
            }
        }

        // Write tree output
        if let Some(ref tree_path) = cli.tree {
            ops::render_find_tree(&matches, &cli.directory, tree_path);
        }

        // Write CSV output
        if let Some(ref csv_path) = cli.csv {
            ops::write_find_csv(&matches, csv_path);
        }

        if cli.size {
            println!("\n{} matches, {}", count, ops::format_size(total_size));
        }
    } else {
        // Stream matches to stdout
        let (count, total_size) = ops::stream_find_matches(&pool, scan_id, &pattern).await;

        if cli.size {
            println!("\n{} matches, {}", count, ops::format_size(total_size));
        }
    }

    etp_lib::db::close_db(pool).await;
}
