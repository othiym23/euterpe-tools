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

    /// State file path for incremental scanning
    #[arg(short, long)]
    state: Option<PathBuf>,

    /// Directory names to exclude from scanning
    #[arg(short, long, default_values_t = [String::from("@eaDir")])]
    exclude: Vec<String>,

    /// Print cache hit/miss info
    #[arg(short, long)]
    verbose: bool,
}

fn main() {
    let cli = Cli::parse();

    ops::validate_directory(&cli.directory);

    let output = cli
        .output
        .unwrap_or_else(|| cli.directory.join("index.csv"));
    let state_path = ops::resolve_state_path(cli.state, &cli.directory);

    let mut scan_state = ops::load_state(&state_path, cli.verbose);
    ops::run_scan(&cli.directory, &mut scan_state, &cli.exclude, cli.verbose);
    ops::write_csv(&scan_state, &output, cli.verbose);
    ops::save_state(&scan_state, &state_path, cli.verbose);
}
