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

    let db_path = cli.db.unwrap_or_else(|| {
        eprintln!("error: --db is required");
        std::process::exit(1);
    });

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
            eprintln!("error: no scans found in database");
            std::process::exit(1);
        }
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
    };

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
                println!("{}/{}", f.dir_path, f.filename);
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
            // Wrap value in % for substring matching if no wildcards present
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
            for (path, val) in &results {
                println!("{path}  [{val}]");
            }
            if cli.verbose {
                eprintln!("{} match(es)", results.len());
            }
        }
        Commands::Stats => {
            let total = db::dao::total_size(&pool, scan_id).await.unwrap_or(0);
            let file_count = db::dao::count_files(&pool, scan_id).await.unwrap_or(0);
            let extensions = db::dao::count_files_by_extension(&pool, Some(scan_id))
                .await
                .unwrap_or_default();

            println!("Files: {file_count}");
            println!("Total size: {}", ops::format_size(total));
            if !extensions.is_empty() {
                println!("\nBy extension:");
                for (ext, count) in &extensions {
                    println!("  .{ext}: {count}");
                }
            }
        }
        Commands::Size { directory } => {
            let prefix = directory.as_deref().unwrap_or("");
            let size = db::dao::subtree_size(&pool, scan_id, prefix)
                .await
                .unwrap_or(0);
            println!("{}", ops::format_size(size));
        }
        Commands::Sql { where_clause } => {
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
