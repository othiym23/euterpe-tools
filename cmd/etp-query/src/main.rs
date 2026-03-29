use clap::{Parser, Subcommand};
use etp_lib::{db, ops};
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "etp-query",
    about = "Query indexed files and metadata",
    version = concat!(env!("CARGO_PKG_VERSION"), " (", env!("GIT_HASH"), ")")
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Database path
    #[arg(long, global = true)]
    db: Option<PathBuf>,

    /// Include NAS/OS system files in results (default for most subcommands)
    #[arg(long, global = true, default_value_t = false)]
    include_system_files: bool,

    /// Hide NAS/OS system files from results
    #[arg(long, global = true, default_value_t = false)]
    no_include_system_files: bool,

    /// Print diagnostic info on stderr
    #[arg(short, long, global = true)]
    verbose: bool,
}

#[derive(Subcommand)]
enum Commands {
    /// List files in a directory
    Files {
        /// Directory path (relative to scan root, or absolute)
        directory: Option<String>,
    },
    /// Show all metadata tags for a file
    Tags {
        /// File path
        file: String,
    },
    /// Find files by metadata tag
    Find {
        /// Tag name to search (e.g. "track_artist", "genre")
        #[arg(long)]
        tag: String,
        /// Value to match (supports SQL LIKE wildcards: % for any, _ for one char)
        #[arg(long)]
        value: String,
    },
    /// Show collection statistics
    Stats,
    /// Show total size of a directory subtree
    Size {
        /// Directory path (relative to scan root)
        directory: Option<String>,
    },
    /// Execute a custom WHERE clause against the file/metadata tables
    Sql {
        /// SQL WHERE clause (e.g. "f.size > 10000000")
        #[arg(name = "WHERE")]
        where_clause: String,
    },
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let cli = Cli::parse();

    let config = etp_lib::config::load_runtime_config().unwrap_or_else(|e| {
        eprintln!("warning: failed to load config: {e}");
        etp_lib::config::RuntimeConfig::defaults()
    });

    // Resolve --db: accept a path, a nickname, or fall back to default-database.
    let db_path = if let Some(ref db_arg) = cli.db {
        if db_arg.exists() {
            db_arg.clone()
        } else if let Some((_, db)) = ops::resolve_nickname(db_arg, &config) {
            db
        } else {
            // Not a nickname and doesn't exist — try it as a path anyway
            // (open_db will create it or fail)
            db_arg.clone()
        }
    } else if let Some(ref default_name) = config.default_database {
        match config.resolve_database(default_name) {
            Some(entry) => {
                eprintln!(
                    "using default database \"{default_name}\": db={}",
                    entry.db.display()
                );
                entry.db.clone()
            }
            None => {
                eprintln!(
                    "error: --db is required (default-database \"{default_name}\" not found)"
                );
                std::process::exit(1);
            }
        }
    } else {
        eprintln!("error: --db is required");
        std::process::exit(1);
    };

    let pool = db::open_db(&db_path, cli.verbose)
        .await
        .unwrap_or_else(|e| {
            eprintln!("error opening database: {e}");
            std::process::exit(1);
        });

    // Find the latest scan (or the only one)
    let scan_id = match db::dao::latest_any_scan_id(&pool).await {
        Ok(Some(id)) => id,
        Ok(None) => {
            eprintln!("error: no scans found in database; run etp-scan first");
            std::process::exit(ops::EXIT_NO_SCAN);
        }
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
    };

    // etp-query defaults to including system files (it's a low-level command),
    // except stats which excludes them (they skew statistics). show_hidden is
    // always true — etp-query doesn't hide dotfiles.
    // See docs/adrs/2026-03-28-03-query-system-file-defaults.md.
    let include_default = !matches!(cli.command, Commands::Stats);
    let filter = ops::FilterConfig::from_config(
        &config,
        cli.include_system_files,
        cli.no_include_system_files,
        include_default,
        true,
    );

    match cli.command {
        Commands::Files { directory } => {
            let prefix = directory.as_deref().unwrap_or("");
            let files = db::dao::list_files_in_directory(&pool, scan_id, prefix)
                .await
                .unwrap_or_else(|e| {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                });
            for f in &files {
                if filter.should_show(&f.dir_path, &f.filename) {
                    println!("{}/{}", f.dir_path, f.filename);
                }
            }
        }
        Commands::Tags { file } => {
            let file_id = db::dao::find_file_id_by_path_suffix(&pool, scan_id, &file)
                .await
                .unwrap_or_else(|e| {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                });

            match file_id {
                Some(id) => {
                    let tags = db::dao::get_file_metadata(&pool, id)
                        .await
                        .unwrap_or_else(|e| {
                            eprintln!("error: {e}");
                            std::process::exit(1);
                        });
                    if tags.is_empty() {
                        eprintln!("no metadata found for {file}");
                    } else {
                        let max_key_len = tags.iter().map(|t| t.tag_name.len()).max().unwrap_or(0);
                        for t in &tags {
                            println!("{:>width$}: {}", t.tag_name, t.value, width = max_key_len);
                        }
                    }
                }
                None => {
                    eprintln!("error: file not found in database: {file}");
                    std::process::exit(1);
                }
            }
        }
        Commands::Find { tag, value } => {
            let pattern = if value.contains('%') || value.contains('_') {
                value
            } else {
                format!("%{value}%")
            };
            let results = db::dao::find_files_by_tag(&pool, Some(scan_id), &tag, &pattern)
                .await
                .unwrap_or_else(|e| {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                });
            let mut count = 0;
            for (path, val) in &results {
                // find_files_by_tag returns full paths; extract filename for filtering
                let filename = std::path::Path::new(path)
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("");
                let dir = std::path::Path::new(path)
                    .parent()
                    .and_then(|p| p.to_str())
                    .unwrap_or("");
                if filter.should_show(dir, filename) {
                    println!("{path}  [{val}]");
                    count += 1;
                }
            }
            if cli.verbose {
                eprintln!("{count} match(es)");
            }
        }
        Commands::Stats => {
            // Stats uses the filter to exclude system files by default,
            // since their counts and sizes would badly skew the statistics.
            // Streams rows to avoid loading all FileRecords into memory.
            use std::future::poll_fn;
            use std::pin::Pin;

            let mut stream = db::dao::stream_files(&pool, scan_id);
            let mut file_count: usize = 0;
            let mut total: u64 = 0;
            let mut ext_counts: std::collections::HashMap<String, usize> =
                std::collections::HashMap::new();

            while let Some(result) = poll_fn(|cx| {
                use futures_core::Stream;
                Pin::new(&mut stream).poll_next(cx)
            })
            .await
            {
                match result {
                    Ok(record) => {
                        if !filter.should_show(&record.dir_path, &record.filename) {
                            continue;
                        }
                        file_count += 1;
                        total += record.size;
                        let ext = record
                            .filename
                            .rfind('.')
                            .map(|i| record.filename[i + 1..].to_lowercase())
                            .unwrap_or_default();
                        if !ext.is_empty() {
                            *ext_counts.entry(ext).or_default() += 1;
                        }
                    }
                    Err(e) => {
                        eprintln!("error reading from database: {e}");
                        std::process::exit(1);
                    }
                }
            }

            println!("Files: {file_count}");
            println!("Total size: {}", ops::format_size(total));

            if !ext_counts.is_empty() {
                let mut sorted: Vec<_> = ext_counts.into_iter().collect();
                sorted.sort_by(|a, b| b.1.cmp(&a.1));
                println!("\nBy extension:");
                for (ext, count) in &sorted {
                    println!("  .{ext}: {count}");
                }
            }
        }
        Commands::Size { directory } => {
            // Size always includes system files — they're real disk usage.
            let prefix = directory.as_deref().unwrap_or("");
            let size = db::dao::subtree_size(&pool, scan_id, prefix)
                .await
                .unwrap_or(0);
            println!("{}", ops::format_size(size));
        }
        Commands::Sql { where_clause } => {
            // SQL passthrough — no display-time filtering applied.
            let results = db::dao::query_files_where(&pool, &where_clause)
                .await
                .unwrap_or_else(|e| {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                });
            for path in &results {
                println!("{path}");
            }
            if cli.verbose {
                eprintln!("{} match(es)", results.len());
            }
        }
    }

    db::close_db(pool).await;
}
