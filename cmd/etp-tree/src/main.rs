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

    /// Do not scan, read existing DB (default)
    #[arg(long, default_value_t = false)]
    no_scan: bool,

    /// Include NAS/OS system files in output (e.g. @eaDir, .etp.db)
    #[arg(long, default_value_t = false)]
    include_system_files: bool,

    /// Hide NAS/OS system files from output (default)
    #[arg(long, default_value_t = false)]
    no_include_system_files: bool,

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

    let config = etp_lib::config::RuntimeConfig::load_or_default();

    let (directory, db) = match ops::resolve_nickname(&cli.directory, &config) {
        Some((root, db_path)) => (root, Some(db_path)),
        None => (cli.directory.clone(), cli.db),
    };

    if cli.verbose {
        eprintln!("root is {}", directory.display());
    }

    let ctx = ops::open_and_resolve_scan(
        &directory,
        db,
        cli.scan,
        cli.no_scan,
        &cli.exclude,
        cli.verbose,
        config.cas_dir.as_deref(),
    )
    .await;

    let filter = ops::FilterConfig::from_config(
        &config,
        cli.include_system_files,
        cli.no_include_system_files,
        false,
        cli.all,
    );

    if let Some(ref find_pattern) = cli.find {
        let pattern = ops::compile_pattern(find_pattern, cli.insensitive);
        let matches =
            ops::collect_find_matches(&ctx.pool, ctx.scan_id, &pattern, &cli.exclude, &filter)
                .await;
        ops::render_find_tree(&matches, &ctx.directory, "-").unwrap_or_else(|e| {
            eprintln!("error rendering tree: {}", e);
            std::process::exit(1);
        });
    } else {
        let mut all_ignore = cli.ignore.clone();
        all_ignore.extend(cli.exclude.iter().cloned());

        ops::render_tree_from_db(
            &ctx.pool,
            ctx.scan_id,
            &ctx.directory,
            &all_ignore,
            &filter,
            cli.no_escape,
        )
        .await
        .unwrap_or_else(|e| {
            eprintln!("error rendering tree: {}", e);
            std::process::exit(1);
        });
    }

    if cli.du {
        ops::render_du(&ctx.pool, ctx.scan_id, cli.du_subs).await;
    }

    etp_lib::db::close_db(ctx.pool).await;

    #[cfg(feature = "profiling")]
    if let Some(guard) = _profiling_guard {
        guard.finish();
    }
}
