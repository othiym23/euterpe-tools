use crate::db::dao;
use crate::{cas, metadata, scanner};
use crate::{csv_writer, tree};
use anyhow::{Context, bail};
use sqlx::SqlitePool;
use std::collections::HashSet;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Instant;

/// Create an ICU collator for locale-aware Unicode sorting (Quaternary
/// strength with shifted alternate handling for consistent punctuation ordering).
pub fn make_collator() -> std::io::Result<icu_collator::CollatorBorrowed<'static>> {
    use icu_collator::options::{AlternateHandling, CollatorOptions, Strength};
    let mut options = CollatorOptions::default();
    options.strength = Some(Strength::Quaternary);
    options.alternate_handling = Some(AlternateHandling::Shifted);
    icu_collator::CollatorBorrowed::try_new(Default::default(), options)
        .map_err(|e| std::io::Error::other(format!("collator initialization failed: {e}")))
}

/// Exit code for "no scan exists" — recoverable by running etp-scan first.
pub const EXIT_NO_SCAN: i32 = 2;

/// Error indicating no scan exists for a directory. Binary crates check for
/// this via `anyhow::Error::downcast_ref::<NoScanExists>()` to set exit code 2.
///
/// **Do not wrap this error with `.context()`** — that would make the downcast
/// fail silently, causing the binary to exit with code 1 instead of code 2.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct NoScanExists(pub String);

/// NAS/OS system files: always scanned, counted in du, hidden from display
/// unless `--include-system-files` is passed.
pub const DEFAULT_SYSTEM_PATTERNS: &[&str] = &[
    "@eaDir",
    "@eaStream",
    "@tmp",
    "@SynoResource",
    "@SynoEAStream",
    "#recycle",
    ".SynologyWorkingDirectory",
    ".etp.db",
    ".etp.db-wal",
    ".etp.db-shm",
];

/// Default user excludes: hidden from display AND excluded from size calculations.
/// Uses glob patterns matched against file/directory names. Dotfile hiding is
/// handled separately by the --all/-A flag, not by user excludes.
pub const DEFAULT_USER_EXCLUDES: &[&str] = &[];

/// Resolve a `--flag` / `--no-flag` boolean pair. If both are passed, prints a
/// warning and returns the default value.
pub fn resolve_bool_pair(flag: bool, no_flag: bool, flag_name: &str, default: bool) -> bool {
    if flag && no_flag {
        eprintln!(
            "warning: both --{flag_name} and --no-{flag_name} passed; using default ({})",
            if default {
                flag_name.to_string()
            } else {
                format!("no-{flag_name}")
            }
        );
        default
    } else if flag {
        true
    } else if no_flag {
        false
    } else {
        default
    }
}

/// Compile a regex pattern, optionally case-insensitive.
pub fn compile_pattern(pattern: &str, case_insensitive: bool) -> anyhow::Result<regex::Regex> {
    regex::RegexBuilder::new(pattern)
        .case_insensitive(case_insensitive)
        .build()
        .map_err(|e| anyhow::anyhow!("invalid regex '{}': {}", pattern, e))
}

/// Verify that a path is a directory.
pub fn validate_directory(root: &Path) -> anyhow::Result<()> {
    if !root.is_dir() {
        bail!("{} is not a directory", root.display());
    }
    Ok(())
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

/// Check whether a name matches any system file pattern (exact match).
pub fn is_system_name(name: &str, system_patterns: &HashSet<String>) -> bool {
    system_patterns.contains(name)
}

/// Check whether a directory path or filename matches any system file pattern.
/// Checks each path component and the filename itself.
///
/// Note: dir_path is the full absolute path (e.g. `/volume1/music/sub`), so
/// all components are checked including the root. This is safe because system
/// patterns use distinctive prefixes (`@`, `#`, `.etp.`, `.Synology`) that
/// don't collide with normal filesystem path components. If config.kdl system
/// patterns are extended with generic names, they could false-positive on root
/// path components — but this is a configuration error caught by testing, not
/// an architectural limitation.
pub fn is_system_path(
    dir_path: &str,
    filename: Option<&str>,
    system_patterns: &HashSet<String>,
) -> bool {
    if system_patterns.is_empty() {
        return false;
    }
    if std::path::Path::new(dir_path).components().any(|c| {
        c.as_os_str()
            .to_str()
            .is_some_and(|s| system_patterns.contains(s))
    }) {
        return true;
    }
    if let Some(name) = filename {
        return is_system_name(name, system_patterns);
    }
    false
}

/// Check whether a name matches any user exclude glob pattern.
pub fn is_user_excluded_name(name: &str, patterns: &[glob::Pattern]) -> bool {
    patterns.iter().any(|p| p.matches(name))
}

pub fn default_system_patterns() -> HashSet<String> {
    DEFAULT_SYSTEM_PATTERNS
        .iter()
        .map(|s| s.to_string())
        .collect()
}

/// Build default user exclude patterns as compiled globs.
pub fn default_user_exclude_patterns() -> Vec<glob::Pattern> {
    DEFAULT_USER_EXCLUDES
        .iter()
        .filter_map(|p| glob::Pattern::new(p).ok())
        .collect()
}

/// Bundled filtering options for display-time filtering.
pub struct FilterConfig {
    pub system_patterns: HashSet<String>,
    pub user_excludes: Vec<glob::Pattern>,
    pub include_system_files: bool,
    pub show_hidden: bool,
}

impl FilterConfig {
    /// Create with defaults: system files hidden, dotfiles hidden, default patterns.
    pub fn new(include_system_files: bool) -> Self {
        Self {
            system_patterns: default_system_patterns(),
            user_excludes: default_user_exclude_patterns(),
            include_system_files,
            show_hidden: false,
        }
    }

    /// Create from CLI flags and RuntimeConfig patterns.
    pub fn from_config(
        config: &crate::config::RuntimeConfig,
        include_system_flag: bool,
        no_include_system_flag: bool,
        include_default: bool,
        show_hidden: bool,
    ) -> Self {
        let include = resolve_bool_pair(
            include_system_flag,
            no_include_system_flag,
            "include-system-files",
            include_default,
        );
        Self {
            system_patterns: config.system_patterns.clone(),
            user_excludes: parse_ignore_patterns(&config.user_excludes),
            include_system_files: include,
            show_hidden,
        }
    }

    /// Check whether a name (file or directory) should be shown.
    ///
    /// This checks a single name in isolation — it cannot check dir_path
    /// context. A file inside `@eaDir` won't be caught unless the `@eaDir`
    /// directory itself was filtered first.
    ///
    /// **Contract**: only use this in tree rendering's `merge_entries`, which
    /// filters entries at each directory level during top-down traversal.
    /// System directories are removed before their children are visited, so
    /// files inside system directories are never reached. CSV and find output
    /// use `should_show()` instead, which checks the full dir_path.
    pub fn should_show_name(&self, name: &str) -> bool {
        let is_system = is_system_name(name, &self.system_patterns);
        if !self.include_system_files && is_system {
            return false;
        }
        // Dotfiles hidden unless --all is passed; system files are exempt
        if !self.show_hidden && !is_system && name.starts_with('.') {
            return false;
        }
        // System files are exempt from user excludes (they're managed separately)
        if !is_system && is_user_excluded_name(name, &self.user_excludes) {
            return false;
        }
        true
    }

    /// Check whether a file record should be shown. System patterns are checked
    /// against all dir_path components and the filename. User excludes are only
    /// checked against the filename (not dir_path components, which include the
    /// absolute root and may contain unrelated dot-dirs like macOS tempdir paths).
    pub fn should_show(&self, dir_path: &str, filename: &str) -> bool {
        let is_system = is_system_path(dir_path, Some(filename), &self.system_patterns);
        if !self.include_system_files && is_system {
            return false;
        }
        // Dotfiles hidden unless --all is passed; system files are exempt
        if !self.show_hidden && !is_system && filename.starts_with('.') {
            return false;
        }
        // System files are exempt from user excludes (they're managed separately)
        if !is_system && is_user_excluded_name(filename, &self.user_excludes) {
            return false;
        }
        true
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

/// Try to resolve a directory argument as a database nickname. If the path
/// doesn't exist as a directory, look it up in the config's database entries.
/// Returns `(directory, db_path)` if found, or `None` if not a nickname.
pub fn resolve_nickname(
    directory: &Path,
    config: &crate::config::RuntimeConfig,
) -> Option<(PathBuf, PathBuf)> {
    if directory.is_dir() {
        return None;
    }
    let name = directory.to_string_lossy();
    config.resolve_database(&name).map(|entry| {
        eprintln!(
            "using database nickname \"{name}\": root={}, db={}",
            entry.root.display(),
            entry.db.display()
        );
        (entry.root.clone(), entry.db.clone())
    })
}

/// Resolve a `--db` argument that could be a file path or a database nickname.
pub fn resolve_db_path(
    db_arg: &Path,
    config: &crate::config::RuntimeConfig,
) -> anyhow::Result<PathBuf> {
    if db_arg.exists() {
        return Ok(db_arg.to_path_buf());
    }
    if let Some((_, db)) = resolve_nickname(db_arg, config) {
        return Ok(db);
    }
    bail!(
        "database not found: {}\nprovide a path to an existing database, or a nickname from config.kdl",
        db_arg.display()
    );
}

/// Resolve `--db` with fallback to `default-database` from config.
/// Accepts a path, a nickname, or falls back to the configured default.
pub fn resolve_db_or_default(
    db_arg: Option<&Path>,
    config: &crate::config::RuntimeConfig,
) -> anyhow::Result<PathBuf> {
    if let Some(db) = db_arg {
        return resolve_db_path(db, config);
    }
    if let Some(ref default_name) = config.default_database {
        if let Some(entry) = config.resolve_database(default_name) {
            eprintln!(
                "using default database \"{default_name}\": db={}",
                entry.db.display()
            );
            return Ok(entry.db.clone());
        }
        // Validated at config load time, but guard against it anyway
        bail!(
            "--db is required (default-database \"{}\" not found)",
            default_name
        );
    }
    bail!("--db is required");
}

/// Result of opening a database and resolving a scan for a directory.
pub struct ScanContext {
    pub pool: SqlitePool,
    pub scan_id: i64,
    pub directory: PathBuf,
}

/// CLI-derived options for opening a database and resolving a scan.
pub struct ScanOptions<'a> {
    pub directory: &'a Path,
    pub db: Option<PathBuf>,
    pub scan: bool,
    pub no_scan: bool,
    pub exclude: &'a [String],
    pub verbose: bool,
}

/// Open the database for a directory and resolve the scan_id.
///
/// Handles the common setup sequence shared by etp-tree and etp-csv:
/// 1. Resolve `--db` path (defaults to `<directory>/.etp.db`)
/// 2. Resolve `--[no-]scan` via `resolve_bool_pair`
/// 3. Guard against creating an empty DB when not scanning
/// 4. Open the database
/// 5. Canonicalize the directory and resolve the scan_id
pub async fn open_and_resolve_scan(
    opts: ScanOptions<'_>,
    config: &crate::config::RuntimeConfig,
) -> anyhow::Result<ScanContext> {
    let cas_dir = config.cas_dir.as_deref();
    validate_directory(opts.directory)?;

    let db_path = opts.db.unwrap_or_else(|| opts.directory.join(".etp.db"));
    let do_scan = resolve_bool_pair(opts.scan, opts.no_scan, "scan", false);

    if !do_scan && !db_path.exists() {
        return Err(NoScanExists(
            "no previous scan exists for this directory; run etp-scan first, or pass --scan".into(),
        )
        .into());
    }

    let pool = crate::db::open_db(&db_path, opts.verbose)
        .await
        .context("opening database")?;

    let canon = opts
        .directory
        .canonicalize()
        .unwrap_or(opts.directory.to_path_buf());
    let run_type = canon.to_string_lossy();

    let scan_id = if do_scan {
        run_scan_to_db(
            opts.directory,
            &pool,
            &run_type,
            opts.exclude,
            opts.verbose,
            cas_dir,
        )
        .await?
    } else {
        resolve_latest_scan_id(&pool, &run_type, opts.verbose).await?
    };

    Ok(ScanContext {
        pool,
        scan_id,
        directory: opts.directory.to_path_buf(),
    })
}

/// Look up the latest scan_id for a directory.
pub async fn resolve_latest_scan_id(
    pool: &SqlitePool,
    run_type: &str,
    verbose: bool,
) -> anyhow::Result<i64> {
    if verbose {
        eprintln!("using existing database (pass --scan to rescan)");
    }
    match dao::latest_scan_id(pool, run_type)
        .await
        .context("querying database")?
    {
        Some(id) => Ok(id),
        None => Err(NoScanExists(
            "no previous scan exists for this directory; run etp-scan first, or pass --scan".into(),
        )
        .into()),
    }
}

/// Run the DB-backed scanner and log stats. Returns scan_id.
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
    cas_dir: Option<&Path>,
) -> anyhow::Result<i64> {
    let (scan_id, stats) = scanner::scan_to_db(root, pool, run_type, exclude, verbose, cas_dir)
        .await
        .context("scanning failed")?;
    if verbose {
        eprintln!(
            "scan complete in {}ms: {} cached, {} scanned, {} removed",
            stats.elapsed_ms, stats.dirs_cached, stats.dirs_scanned, stats.dirs_removed
        );
    }
    Ok(scan_id)
}

/// Write CSV output from database.
#[cfg_attr(
    feature = "profiling",
    tracing::instrument(name = "write_csv_from_db", skip_all)
)]
pub async fn write_csv_from_db(
    pool: &SqlitePool,
    scan_id: i64,
    output: &Path,
    exclude: &[String],
    filter: &FilterConfig,
    verbose: bool,
) -> anyhow::Result<()> {
    csv_writer::write_csv_from_db(pool, scan_id, output, exclude, filter)
        .await
        .context("writing CSV")?;
    if verbose {
        eprintln!("wrote {}", output.display());
    }
    Ok(())
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
    filter: &FilterConfig,
    no_escape: bool,
) -> anyhow::Result<()> {
    let patterns = parse_ignore_patterns(ignore);
    let (dir_count, file_count) =
        tree::render_tree_from_db(pool, scan_id, root, &patterns, filter, no_escape)
            .await
            .context("rendering tree")?;
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

/// Render size summary (du replacement).
#[cfg_attr(
    feature = "profiling",
    tracing::instrument(name = "render_du", skip_all)
)]
pub async fn render_du(pool: &SqlitePool, scan_id: i64, show_subs: bool) -> anyhow::Result<()> {
    let total = dao::subtree_size(pool, scan_id, "")
        .await
        .context("querying size")?;
    println!("Size: {} (root)", format_size(total));

    if show_subs {
        let subs = dao::immediate_subdirectory_sizes(pool, scan_id)
            .await
            .context("querying subdirectory sizes")?;
        for (name, size) in &subs {
            println!("  {}  {}", format_size(*size), name);
        }
    }
    Ok(())
}

/// Stream files from DB, printing matching paths immediately. Returns (count, total_size).
///
/// Pass `scan_id: Some(id)` to search a single scan, or `None` to search all scans.
pub async fn stream_find_matches(
    pool: &SqlitePool,
    scan_id: Option<i64>,
    pattern: &regex::Regex,
    exclude: &[String],
    filter: &FilterConfig,
) -> anyhow::Result<(usize, u64)> {
    use crate::finder;
    use futures_util::StreamExt;

    let mut stream: std::pin::Pin<
        Box<dyn futures_core::Stream<Item = Result<dao::FileRecord, sqlx::Error>> + Send + '_>,
    > = match scan_id {
        Some(id) => dao::stream_files(pool, id),
        None => dao::stream_all_files(pool),
    };

    let mut count = 0;
    let mut total_size = 0u64;

    while let Some(result) = stream.next().await {
        let record = result.context("reading from database")?;
        if is_excluded_path(&record.dir_path, exclude) {
            continue;
        }
        if !filter.should_show(&record.dir_path, &record.filename) {
            continue;
        }
        if let Some(m) = finder::matches_pattern(&record, pattern) {
            println!("{}", m.full_path);
            total_size += m.size;
            count += 1;
        }
    }

    Ok((count, total_size))
}

/// Collect all matching files into a Vec. Returns the matches.
///
/// Pass `scan_id: Some(id)` to search a single scan, or `None` to search all scans.
pub async fn collect_find_matches(
    pool: &SqlitePool,
    scan_id: Option<i64>,
    pattern: &regex::Regex,
    exclude: &[String],
    filter: &FilterConfig,
) -> anyhow::Result<Vec<crate::finder::FindMatch>> {
    use crate::finder;

    let files = match scan_id {
        Some(id) => dao::list_files(pool, id).await,
        None => dao::list_all_files(pool).await,
    }
    .context("reading from database")?;

    Ok(files
        .iter()
        .filter(|record| !is_excluded_path(&record.dir_path, exclude))
        .filter(|record| filter.should_show(&record.dir_path, &record.filename))
        .filter_map(|record| finder::matches_pattern(record, pattern))
        .collect())
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

    #[test]
    fn user_exclude_glob_matching() {
        let pat = glob::Pattern::new("*.bak").unwrap();
        assert!(is_user_excluded_name("file.bak", &[pat.clone()]));
        assert!(!is_user_excluded_name("file.txt", &[pat]));
    }

    #[test]
    fn system_name_matching() {
        let sys = default_system_patterns();
        assert!(is_system_name("@eaDir", &sys));
        assert!(is_system_name(".etp.db", &sys));
        assert!(!is_system_name("music", &sys));
    }

    #[test]
    fn filter_config_hides_dotfiles_by_default() {
        let filter = FilterConfig::new(false);
        assert!(filter.should_show("/data/sub", "song.mp3"));
        assert!(!filter.should_show("/data/sub", ".hidden"));
        assert!(!filter.should_show("/data/sub", ".DS_Store"));
    }

    #[test]
    fn filter_config_show_hidden_reveals_dotfiles() {
        let mut filter = FilterConfig::new(false);
        filter.show_hidden = true;
        assert!(filter.should_show("/data/sub", ".hidden"));
        assert!(filter.should_show("/data/sub", ".DS_Store"));
    }

    #[test]
    fn filter_config_hides_system_files_by_default() {
        let filter = FilterConfig::new(false);
        assert!(!filter.should_show("/data/@eaDir", "thumb.jpg"));
        assert!(!filter.should_show("/data/sub", ".etp.db"));
    }

    #[test]
    fn filter_config_include_system_files() {
        let filter = FilterConfig::new(true);
        assert!(filter.should_show("/data/@eaDir", "thumb.jpg"));
        assert!(filter.should_show("/data/sub", ".etp.db"));
        // Dotfiles still hidden (show_hidden defaults to false)
        assert!(!filter.should_show("/data/sub", ".hidden"));
    }

    #[test]
    fn filter_config_system_files_exempt_from_dotfile_hiding() {
        // .etp.db starts with '.' but is a system file, not a dotfile
        let filter = FilterConfig::new(true);
        assert!(filter.should_show("/data/sub", ".etp.db"));
        assert!(filter.should_show("/data/sub", ".SynologyWorkingDirectory"));
    }
    #[test]
    fn resolve_nickname_returns_none_for_real_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let config = crate::config::RuntimeConfig::defaults();
        assert!(resolve_nickname(tmp.path(), &config).is_none());
    }

    #[test]
    fn resolve_nickname_finds_configured_database() {
        let config = crate::config::parse_runtime_config(
            r#"
database "music" {
    root "/volume1/music"
    db "/data/music.db"
}
"#,
        )
        .unwrap();

        let result = resolve_nickname(std::path::Path::new("music"), &config);
        assert!(result.is_some());
        let (root, db) = result.unwrap();
        assert_eq!(root, std::path::Path::new("/volume1/music"));
        assert_eq!(db, std::path::Path::new("/data/music.db"));
    }

    #[test]
    fn resolve_nickname_returns_none_for_unknown_name() {
        let config = crate::config::parse_runtime_config(
            r#"
database "music" {
    root "/volume1/music"
    db "/data/music.db"
}
"#,
        )
        .unwrap();
        assert!(resolve_nickname(std::path::Path::new("videos"), &config).is_none());
    }

    #[test]
    fn filter_config_from_config_uses_config_patterns() {
        let config = crate::config::parse_runtime_config(
            r#"
system-files {
    pattern "@custom"
}
user-excludes {
    pattern "*.tmp"
}
"#,
        )
        .unwrap();

        let filter = FilterConfig::from_config(&config, false, false, false, false);
        assert!(filter.should_show("/data/sub", "song.mp3"));
        assert!(!filter.should_show("/data/@custom", "file.txt"));
        assert!(!filter.should_show("/data/sub", "file.tmp"));
        // @eaDir is NOT filtered because config overrides defaults
        assert!(filter.should_show("/data/@eaDir", "thumb.jpg"));
    }
}

/// Stats returned by a metadata scan.
pub struct MetadataScanStats {
    pub files_scanned: usize,
    pub files_skipped: usize,
    pub errors: usize,
    pub elapsed_ms: u128,
}

/// Scan audio files in the database and extract metadata using lofty.
/// Files are processed in directory order for sequential I/O on spinning disks.
/// Errors on individual files are logged and skipped.
#[cfg_attr(
    feature = "profiling",
    tracing::instrument(name = "run_metadata_scan", skip_all)
)]
pub async fn run_metadata_scan(
    pool: &SqlitePool,
    scan_id: i64,
    force: bool,
    verbose: bool,
    cas_dir: Option<&Path>,
) -> anyhow::Result<MetadataScanStats> {
    let start = Instant::now();
    let mut stats = MetadataScanStats {
        files_scanned: 0,
        files_skipped: 0,
        errors: 0,
        elapsed_ms: 0,
    };

    let files = dao::files_needing_metadata_scan(pool, scan_id, metadata::MEDIA_EXTENSIONS, force)
        .await
        .context("querying files for metadata scan")?;

    if verbose {
        eprintln!("metadata scan: {} file(s) to process", files.len());
    }

    for record in &files {
        match process_media_file(pool, record, verbose, cas_dir).await {
            Ok(()) => stats.files_scanned += 1,
            Err(e) => {
                if verbose {
                    eprintln!("  warning: {}: {e}", record.filename);
                }
                stats.errors += 1;
            }
        }
    }

    stats.elapsed_ms = start.elapsed().as_millis();
    Ok(stats)
}

/// Extract and persist metadata for a single media file.
async fn process_media_file(
    pool: &SqlitePool,
    record: &dao::MediaFileRecord,
    verbose: bool,
    cas_dir: Option<&Path>,
) -> anyhow::Result<()> {
    let full_path = if record.dir_path.is_empty() {
        PathBuf::from(&record.root_path).join(&record.filename)
    } else {
        PathBuf::from(&record.root_path)
            .join(&record.dir_path)
            .join(&record.filename)
    };

    if verbose {
        eprintln!("  reading: {}", full_path.display());
    }

    let file_meta = metadata::read_metadata(&full_path)
        .with_context(|| format!("reading metadata from {}", full_path.display()))?;

    let all_tags: Vec<(String, String)> = file_meta
        .properties
        .iter()
        .chain(file_meta.tags.iter())
        .map(|(key, val)| (key.clone(), val.to_string()))
        .collect();

    dao::replace_file_metadata(pool, record.file_id, &all_tags)
        .await
        .context("replacing file metadata")?;

    if !file_meta.images.is_empty() {
        let mut image_inputs = Vec::new();
        for img in &file_meta.images {
            match cas::store_blob(&img.data, cas_dir) {
                Ok((hash, size)) => {
                    image_inputs.push(dao::EmbeddedImageInput {
                        image_type: img.image_type.clone(),
                        mime_type: img.mime_type.clone(),
                        blob_hash: hash,
                        blob_size: size,
                        width: img.width.map(|w| w as i64),
                        height: img.height.map(|h| h as i64),
                    });
                }
                Err(e) => {
                    if verbose {
                        eprintln!("  warning: failed to store image blob: {e}");
                    }
                }
            }
        }
        if let Ok(orphan_hashes) =
            dao::replace_embedded_images(pool, record.file_id, &image_inputs).await
        {
            for hash in &orphan_hashes {
                let _ = cas::remove_blob(hash, cas_dir);
            }
        }
    }

    if let Some(cue) = &file_meta.cue_sheet {
        let _ = dao::upsert_cue_sheet(pool, record.file_id, "embedded", cue).await;
    }

    let cue_path = full_path.with_extension("cue");
    if cue_path.is_file() {
        match std::fs::read_to_string(&cue_path) {
            Ok(content) => {
                let _ = dao::upsert_cue_sheet(pool, record.file_id, "standalone", &content).await;
            }
            Err(e) => {
                if verbose {
                    eprintln!("  warning: failed to read {}: {e}", cue_path.display());
                }
            }
        }
    }

    // Compute content hash for move tracking and deduplication
    let content_hash = cas::hash_file(&full_path);

    dao::mark_metadata_scanned(pool, record.file_id, content_hash.as_deref())
        .await
        .context("marking metadata scanned")?;

    Ok(())
}

/// Read metadata from a single audio file (no database). Returns JSON.
pub fn read_file_metadata(path: &Path) -> anyhow::Result<serde_json::Value> {
    let meta = metadata::read_metadata(path)
        .with_context(|| format!("reading metadata from {}", path.display()))?;
    let mut json = serde_json::to_value(&meta).context("serializing metadata")?;

    // Add file path and replace image data with sizes
    if let Some(obj) = json.as_object_mut() {
        obj.insert(
            "file".into(),
            serde_json::Value::String(path.display().to_string()),
        );
        // Images: add byte sizes (raw data not serialized due to #[serde(skip)])
        if let Some(serde_json::Value::Array(images)) = obj.get_mut("images") {
            for (i, img_val) in images.iter_mut().enumerate() {
                if let Some(img_obj) = img_val.as_object_mut() {
                    img_obj.insert(
                        "size".into(),
                        serde_json::Value::Number(meta.images[i].data.len().into()),
                    );
                }
            }
        }

        // Check for standalone .cue file
        let cue_path = path.with_extension("cue");
        if cue_path.is_file()
            && let Ok(content) = std::fs::read_to_string(&cue_path)
        {
            obj.insert(
                "standalone_cue_sheet".into(),
                serde_json::Value::String(content),
            );
        }
    }

    Ok(json)
}

/// Remove CAS blobs that are not referenced by any database record.
/// Returns the number of blobs removed.
pub async fn gc_orphan_blobs(
    pool: &SqlitePool,
    verbose: bool,
    cas_dir: Option<&Path>,
) -> anyhow::Result<usize> {
    let referenced = dao::referenced_blob_hashes(pool)
        .await
        .context("querying referenced blobs")?;

    let on_disk = cas::list_blob_hashes(cas_dir).context("listing CAS blobs")?;

    let mut removed = 0;
    for hash in &on_disk {
        if !referenced.contains(hash) {
            if verbose {
                eprintln!("  removing orphan blob: {hash}");
            }
            let _ = cas::remove_blob(hash, cas_dir);
            removed += 1;
        }
    }
    Ok(removed)
}
