use crate::scanner;
use crate::state::{LoadOutcome, ScanState};
use crate::{csv_writer, tree};
use std::path::{Path, PathBuf};
use std::process;

/// Verify that a path is a directory, exiting with an error if not.
pub fn validate_directory(root: &Path) {
    if !root.is_dir() {
        eprintln!("error: {} is not a directory", root.display());
        process::exit(1);
    }
}

/// Resolve the state file path, defaulting to `<root>/.fsscan.state`.
pub fn resolve_state_path(state: Option<PathBuf>, root: &Path) -> PathBuf {
    state.unwrap_or_else(|| root.join(".fsscan.state"))
}

/// Load scan state from disk, handling all three outcomes.
/// Exits the process on unrecoverable errors.
pub fn load_state(path: &Path, verbose: bool) -> ScanState {
    match ScanState::load(path) {
        LoadOutcome::Loaded(s) => {
            if verbose {
                eprintln!("loaded state from {}", path.display());
            }
            s
        }
        LoadOutcome::NotFound => {
            if verbose {
                eprintln!("no previous state, starting fresh");
            }
            ScanState::default()
        }
        LoadOutcome::Invalid(reason) => {
            eprintln!("warning: {}: {}, rescanning", path.display(), reason);
            ScanState::default()
        }
    }
}

/// Run the scanner and log stats. Exits on error.
pub fn run_scan(root: &Path, state: &mut ScanState, exclude: &[String], verbose: bool) {
    match scanner::scan(root, state, exclude, verbose) {
        Ok(stats) => {
            if verbose {
                eprintln!(
                    "dirs: {} cached, {} scanned, {} removed",
                    stats.dirs_cached, stats.dirs_scanned, stats.dirs_removed
                );
            }
        }
        Err(e) => {
            eprintln!("error scanning: {}", e);
            process::exit(1);
        }
    }
}

/// Parse glob ignore patterns, warning on and discarding invalid ones.
pub fn parse_ignore_patterns(patterns: &[String]) -> Vec<glob::Pattern> {
    patterns
        .iter()
        .filter_map(|p| match glob::Pattern::new(p) {
            Ok(pat) => Some(pat),
            Err(e) => {
                eprintln!("warning: invalid glob pattern '{}': {}, discarding", p, e);
                None
            }
        })
        .collect()
}

/// Save scan state to disk. Exits on error.
pub fn save_state(state: &ScanState, path: &Path, verbose: bool) {
    if let Err(e) = state.save(path) {
        eprintln!("error saving state: {}", e);
        process::exit(1);
    }
    if verbose {
        eprintln!("saved state to {}", path.display());
    }
}

/// Write CSV output from scan state. Exits on error.
pub fn write_csv(state: &ScanState, output: &Path, verbose: bool) {
    if let Err(e) = csv_writer::write_csv(state, output) {
        eprintln!("error writing CSV: {}", e);
        process::exit(1);
    }
    if verbose {
        eprintln!("wrote {}", output.display());
    }
}

/// Render tree output from scan state, printing summary line.
pub fn render_tree(
    state: &ScanState,
    root: &Path,
    ignore: &[String],
    no_escape: bool,
    show_hidden: bool,
) {
    let patterns = parse_ignore_patterns(ignore);
    let (dir_count, file_count) = tree::render_tree(state, root, &patterns, no_escape, show_hidden);
    println!("\n{} directories, {} files", dir_count, file_count);
}
