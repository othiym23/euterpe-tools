use clap::Parser;
use etp_lib::ops;
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "etp-tree",
    about = "Incremental filesystem scanner with tree output",
    version = concat!(env!("CARGO_PKG_VERSION"), " (", env!("GIT_HASH"), ")")
)]
struct Cli {
    /// Root directory to display
    directory: PathBuf,

    /// Database path
    #[arg(long)]
    db: Option<PathBuf>,

    /// Directory names to exclude from output
    #[arg(short, long, default_values_t = [String::from("@eaDir")])]
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

    /// Skip scanning, use existing DB data
    #[arg(long, hide = true)]
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
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let cli = Cli::parse();

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
        ops::run_scan_to_db(&cli.directory, &pool, &run_type, cli.verbose).await
    };

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
    .await;

    if cli.du {
        ops::render_du(&pool, scan_id, cli.du_subs).await;
    }

    etp_lib::db::close_db(pool).await;
}
