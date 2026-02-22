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

    /// State file path (deprecated, ignored)
    #[arg(short, long, hide = true)]
    state: Option<PathBuf>,

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

    /// Print scan info on stderr
    #[arg(short, long)]
    verbose: bool,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let cli = Cli::parse();

    if cli.state.is_some() {
        eprintln!("warning: --state is deprecated and ignored, using database");
    }

    if cli.verbose {
        eprintln!("root is {}", cli.directory.display());
    }
    ops::validate_directory(&cli.directory);

    let db_path = cli.db.unwrap_or_else(|| cli.directory.join(".etp.db"));

    let pool = etp_lib::db::open_db(&db_path).await.unwrap_or_else(|e| {
        eprintln!("error opening database: {}", e);
        std::process::exit(1);
    });

    let canon = cli
        .directory
        .canonicalize()
        .unwrap_or(cli.directory.clone());
    let run_type = canon.to_string_lossy();

    let scan_id = ops::run_scan_to_db(&cli.directory, &pool, &run_type, cli.verbose).await;

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
}
