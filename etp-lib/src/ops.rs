use crate::db::dao;
use crate::scanner;
use crate::{csv_writer, tree};
use sqlx::SqlitePool;
use std::io::Write;
use std::path::Path;
use std::process;

/// Compile a regex pattern, optionally case-insensitive. Exits on invalid pattern.
pub fn compile_pattern(pattern: &str, case_insensitive: bool) -> regex::Regex {
    regex::RegexBuilder::new(pattern)
        .case_insensitive(case_insensitive)
        .build()
        .unwrap_or_else(|e| {
            eprintln!("error: invalid regex '{}': {}", pattern, e);
            process::exit(1);
        })
}

/// Verify that a path is a directory, exiting with an error if not.
pub fn validate_directory(root: &Path) {
    if !root.is_dir() {
        eprintln!("error: {} is not a directory", root.display());
        process::exit(1);
    }
}

/// Check whether a directory path contains any excluded directory name as a
/// path component.
pub fn is_excluded_path(dir_path: &str, exclude: &[String]) -> bool {
    if exclude.is_empty() {
        return false;
    }
    std::path::Path::new(dir_path)
        .components()
        .any(|c| exclude.iter().any(|ex| c.as_os_str() == ex.as_str()))
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

/// Run the DB-backed scanner and log stats. Returns scan_id. Exits on error.
#[cfg_attr(
    feature = "profiling",
    tracing::instrument(name = "run_scan_to_db", skip_all)
)]
pub async fn run_scan_to_db(
    root: &Path,
    pool: &SqlitePool,
    run_type: &str,
    exclude: &[String],
    verbose: bool,
) -> i64 {
    match scanner::scan_to_db(root, pool, run_type, exclude, verbose).await {
        Ok((scan_id, stats)) => {
            if verbose {
                eprintln!(
                    "scan complete in {}ms: {} cached, {} scanned, {} removed",
                    stats.elapsed_ms, stats.dirs_cached, stats.dirs_scanned, stats.dirs_removed
                );
            }
            scan_id
        }
        Err(e) => {
            eprintln!("error scanning: {}", e);
            process::exit(1);
        }
    }
}

/// Write CSV output from database. Exits on error.
#[cfg_attr(
    feature = "profiling",
    tracing::instrument(name = "write_csv_from_db", skip_all)
)]
pub async fn write_csv_from_db(
    pool: &SqlitePool,
    scan_id: i64,
    output: &Path,
    exclude: &[String],
    verbose: bool,
) {
    if let Err(e) = csv_writer::write_csv_from_db(pool, scan_id, output, exclude, verbose).await {
        eprintln!("error writing CSV: {}", e);
        process::exit(1);
    }
    if verbose {
        eprintln!("wrote {}", output.display());
    }
}

/// Render tree output from database, printing summary line.
#[cfg_attr(
    feature = "profiling",
    tracing::instrument(name = "render_tree_from_db", skip_all)
)]
pub async fn render_tree_from_db(
    pool: &SqlitePool,
    scan_id: i64,
    root: &Path,
    ignore: &[String],
    no_escape: bool,
    show_hidden: bool,
) -> std::io::Result<()> {
    let patterns = parse_ignore_patterns(ignore);
    let (dir_count, file_count) =
        tree::render_tree_from_db(pool, scan_id, root, &patterns, no_escape, show_hidden).await?;
    println!("\n{} directories, {} files", dir_count, file_count);
    Ok(())
}

/// Format a byte count as a human-readable string with two significant digits.
pub fn format_size(bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = 1024.0 * 1024.0;
    const GIB: f64 = 1024.0 * 1024.0 * 1024.0;
    const TIB: f64 = 1024.0 * 1024.0 * 1024.0 * 1024.0;

    let b = bytes as f64;
    if b >= TIB {
        format!("{:.2} TiB", b / TIB)
    } else if b >= GIB {
        format!("{:.2} GiB", b / GIB)
    } else if b >= MIB {
        format!("{:.2} MiB", b / MIB)
    } else if b >= KIB {
        format!("{:.2} KiB", b / KIB)
    } else {
        format!("{} B", bytes)
    }
}

/// Render size summary (du replacement). Exits on error.
#[cfg_attr(
    feature = "profiling",
    tracing::instrument(name = "render_du", skip_all)
)]
pub async fn render_du(pool: &SqlitePool, scan_id: i64, show_subs: bool) {
    let total = dao::subtree_size(pool, scan_id, "")
        .await
        .unwrap_or_else(|e| {
            eprintln!("error querying size: {}", e);
            process::exit(1);
        });
    println!("Size: {} (root)", format_size(total));

    if show_subs {
        let subs = dao::immediate_subdirectory_sizes(pool, scan_id)
            .await
            .unwrap_or_else(|e| {
                eprintln!("error querying subdirectory sizes: {}", e);
                process::exit(1);
            });
        for (name, size) in &subs {
            println!("  {}  {}", format_size(*size), name);
        }
    }
}

/// Stream files from DB, printing matching paths immediately. Returns (count, total_size).
#[cfg_attr(
    feature = "profiling",
    tracing::instrument(name = "stream_find_matches", skip_all)
)]
pub async fn stream_find_matches(
    pool: &SqlitePool,
    scan_id: i64,
    pattern: &regex::Regex,
    exclude: &[String],
) -> (usize, u64) {
    use crate::finder;
    use std::future::poll_fn;
    use std::pin::Pin;

    let mut stream = dao::stream_files(pool, scan_id);
    let mut count = 0;
    let mut total_size = 0u64;

    while let Some(result) = poll_fn(|cx| {
        use futures_core::Stream;
        Pin::new(&mut stream).poll_next(cx)
    })
    .await
    {
        match result {
            Ok(record) => {
                if is_excluded_path(&record.dir_path, exclude) {
                    continue;
                }
                if let Some(m) = finder::matches_pattern(&record, pattern) {
                    println!("{}", m.full_path);
                    total_size += m.size;
                    count += 1;
                }
            }
            Err(e) => {
                eprintln!("error reading from database: {}", e);
                process::exit(1);
            }
        }
    }

    (count, total_size)
}

/// Collect all matching files into a Vec. Returns the matches.
#[cfg_attr(
    feature = "profiling",
    tracing::instrument(name = "collect_find_matches", skip_all)
)]
pub async fn collect_find_matches(
    pool: &SqlitePool,
    scan_id: i64,
    pattern: &regex::Regex,
    exclude: &[String],
) -> Vec<crate::finder::FindMatch> {
    use crate::finder;

    let files = dao::list_files(pool, scan_id).await.unwrap_or_else(|e| {
        eprintln!("error reading from database: {}", e);
        process::exit(1);
    });

    files
        .iter()
        .filter(|record| !is_excluded_path(&record.dir_path, exclude))
        .filter_map(|record| finder::matches_pattern(record, pattern))
        .collect()
}

/// Stream files from all scans in DB, printing matching paths. Returns (count, total_size).
#[cfg_attr(
    feature = "profiling",
    tracing::instrument(name = "stream_find_all_matches", skip_all)
)]
pub async fn stream_find_all_matches(
    pool: &SqlitePool,
    pattern: &regex::Regex,
    exclude: &[String],
) -> (usize, u64) {
    use crate::finder;
    use std::future::poll_fn;
    use std::pin::Pin;

    let mut stream = dao::stream_all_files(pool);
    let mut count = 0;
    let mut total_size = 0u64;

    while let Some(result) = poll_fn(|cx| {
        use futures_core::Stream;
        Pin::new(&mut stream).poll_next(cx)
    })
    .await
    {
        match result {
            Ok(record) => {
                if is_excluded_path(&record.dir_path, exclude) {
                    continue;
                }
                if let Some(m) = finder::matches_pattern(&record, pattern) {
                    println!("{}", m.full_path);
                    total_size += m.size;
                    count += 1;
                }
            }
            Err(e) => {
                eprintln!("error reading from database: {}", e);
                process::exit(1);
            }
        }
    }

    (count, total_size)
}

/// Collect all matching files across all scans into a Vec.
#[cfg_attr(
    feature = "profiling",
    tracing::instrument(name = "collect_find_all_matches", skip_all)
)]
pub async fn collect_find_all_matches(
    pool: &SqlitePool,
    pattern: &regex::Regex,
    exclude: &[String],
) -> Vec<crate::finder::FindMatch> {
    use crate::finder;

    let files = dao::list_all_files(pool).await.unwrap_or_else(|e| {
        eprintln!("error reading from database: {}", e);
        process::exit(1);
    });

    files
        .iter()
        .filter(|record| !is_excluded_path(&record.dir_path, exclude))
        .filter_map(|record| finder::matches_pattern(record, pattern))
        .collect()
}

/// Write matched files as CSV to a file path, or stdout when `output == "-"`.
#[cfg_attr(
    feature = "profiling",
    tracing::instrument(name = "write_find_csv", skip_all)
)]
pub fn write_find_csv(matches: &[crate::finder::FindMatch], output: &str) -> std::io::Result<()> {
    let writer: Box<dyn std::io::Write> = if output == "-" {
        Box::new(std::io::stdout().lock())
    } else {
        Box::new(std::fs::File::create(output)?)
    };

    let mut wtr = csv::Writer::from_writer(writer);
    wtr.write_record(["path", "size", "ctime", "mtime"])
        .map_err(std::io::Error::other)?;

    for m in matches {
        wtr.write_record([
            &m.full_path,
            &m.size.to_string(),
            &m.ctime.to_string(),
            &m.mtime.to_string(),
        ])
        .map_err(std::io::Error::other)?;
    }

    wtr.flush().map_err(std::io::Error::other)?;
    Ok(())
}

/// Render matched files as a tree to a file path, or stdout when `output == "-"`.
#[cfg_attr(
    feature = "profiling",
    tracing::instrument(name = "render_find_tree", skip_all)
)]
pub fn render_find_tree(
    matches: &[crate::finder::FindMatch],
    root: &Path,
    output: &str,
) -> std::io::Result<()> {
    let mut writer: Box<dyn std::io::Write> = if output == "-" {
        Box::new(std::io::stdout().lock())
    } else {
        Box::new(std::fs::File::create(output)?)
    };

    let (dir_count, file_count) = tree::render_tree_from_paths(matches, root, &mut writer)?;
    writeln!(writer, "\n{} directories, {} files", dir_count, file_count)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_size_bytes() {
        assert_eq!(format_size(0), "0 B");
        assert_eq!(format_size(512), "512 B");
        assert_eq!(format_size(1023), "1023 B");
    }

    #[test]
    fn format_size_kib() {
        assert_eq!(format_size(1024), "1.00 KiB");
        assert_eq!(format_size(1536), "1.50 KiB");
    }

    #[test]
    fn format_size_mib() {
        assert_eq!(format_size(1024 * 1024), "1.00 MiB");
        assert_eq!(format_size(500 * 1024 * 1024), "500.00 MiB");
    }

    #[test]
    fn format_size_gib() {
        assert_eq!(format_size(1024 * 1024 * 1024), "1.00 GiB");
        assert_eq!(format_size(2_500_000_000), "2.33 GiB");
    }

    #[test]
    fn format_size_tib() {
        assert_eq!(format_size(1024 * 1024 * 1024 * 1024), "1.00 TiB");
    }

    #[test]
    fn is_excluded_path_matches_component() {
        let exclude = vec!["@eaDir".to_string()];
        assert!(is_excluded_path("/data/@eaDir", &exclude));
    }

    #[test]
    fn is_excluded_path_matches_nested() {
        let exclude = vec!["@eaDir".to_string()];
        assert!(is_excluded_path("/data/sub/@eaDir/thumbs", &exclude));
    }

    #[test]
    fn is_excluded_path_no_match() {
        let exclude = vec!["@eaDir".to_string()];
        assert!(!is_excluded_path("/data/sub", &exclude));
    }

    #[test]
    fn is_excluded_path_empty_exclude() {
        assert!(!is_excluded_path("/data/@eaDir", &[]));
    }
}
