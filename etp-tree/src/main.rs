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

    /// State file path for incremental scanning
    #[arg(short, long)]
    state: Option<PathBuf>,

    /// Directory names to exclude from scanning
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

fn main() {
    let cli = Cli::parse();

    if cli.verbose {
        eprintln!("root is {}", cli.directory.display());
    }
    ops::validate_directory(&cli.directory);

    let state_path = ops::resolve_state_path(cli.state, &cli.directory);
    if cli.verbose {
        eprintln!("state_path is {}", state_path.display());
    }

    let mut scan_state = ops::load_state(&state_path, cli.verbose);
    ops::run_scan(&cli.directory, &mut scan_state, &cli.exclude, cli.verbose);
    ops::save_state(&scan_state, &state_path, cli.verbose);
    ops::render_tree(
        &scan_state,
        &cli.directory,
        &cli.ignore,
        cli.no_escape,
        cli.all,
    );
}
