use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use etp_lib::{db, ops};
use std::path::PathBuf;
use std::process;

#[derive(Clone, ValueEnum)]
enum StatsFormat {
    Text,
    Csv,
    Json,
    Kdl,
}

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
    Stats {
        /// Output format
        #[arg(long, default_value = "text")]
        format: StatsFormat,
    },
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

async fn run(cli: Cli) -> Result<()> {
    let config = etp_lib::config::RuntimeConfig::load_or_default();

    let db_path = ops::resolve_db_or_default(cli.db.as_deref(), &config)?;

    let pool = db::open_db(&db_path, cli.verbose)
        .await
        .context("opening database")?;

    let scan_id = match db::dao::latest_any_scan_id(&pool).await? {
        Some(id) => id,
        None => {
            return Err(
                ops::NoScanExists("no scans found in database; run etp-scan first".into()).into(),
            );
        }
    };

    // etp-query defaults to including system files (it's a low-level command),
    // except stats which excludes them (they skew statistics). show_hidden is
    // always true — etp-query doesn't hide dotfiles.
    // See docs/adrs/2026-03-28-03-query-system-file-defaults.md.
    let include_default = !matches!(cli.command, Commands::Stats { .. });
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
            let files = db::dao::list_files_in_directory(&pool, scan_id, prefix).await?;
            for f in &files {
                if filter.should_show(&f.dir_path, &f.filename) {
                    println!("{}/{}", f.dir_path, f.filename);
                }
            }
        }
        Commands::Tags { file } => {
            let file_id = db::dao::find_file_id_by_path_suffix(&pool, scan_id, &file).await?;

            match file_id {
                Some(id) => {
                    let tags = db::dao::get_file_metadata(&pool, id).await?;
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
                    anyhow::bail!("file not found in database: {file}");
                }
            }
        }
        Commands::Find { tag, value } => {
            let pattern = if value.contains('%') || value.contains('_') {
                value
            } else {
                format!("%{value}%")
            };
            let results = db::dao::find_files_by_tag(&pool, Some(scan_id), &tag, &pattern).await?;
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
        Commands::Stats { format } => {
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
                let record = result.context("reading from database")?;
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

            let mut sorted: Vec<_> = ext_counts.into_iter().collect();
            sorted.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

            match format {
                StatsFormat::Json => {
                    let extensions: serde_json::Map<String, serde_json::Value> = sorted
                        .iter()
                        .map(|(ext, count)| {
                            (ext.clone(), serde_json::Value::Number((*count).into()))
                        })
                        .collect();
                    let obj = serde_json::json!({
                        "files": file_count,
                        "total_size": total,
                        "total_size_human": ops::format_size(total),
                        "extensions": extensions,
                    });
                    println!("{}", serde_json::to_string_pretty(&obj).unwrap());
                }
                StatsFormat::Csv => {
                    println!("extension,count");
                    for (ext, count) in &sorted {
                        println!(".{ext},{count}");
                    }
                }
                StatsFormat::Text => {
                    println!("Files: {file_count}");
                    println!("Total size: {}", ops::format_size(total));

                    if !sorted.is_empty() {
                        // +1 for the leading dot
                        let max_width = sorted.iter().map(|(e, _)| e.len() + 1).max().unwrap_or(0);
                        println!("\nBy extension:");
                        for (ext, count) in &sorted {
                            let label = format!(".{ext}");
                            println!("  {label:>max_width$}: {count}");
                        }
                    }
                }
                StatsFormat::Kdl => {
                    println!("stats {{");
                    println!("    files {file_count}");
                    println!("    total-size {total}");
                    println!("    total-size-human \"{}\"", ops::format_size(total));
                    if !sorted.is_empty() {
                        println!("    extensions {{");
                        for (ext, count) in &sorted {
                            println!("        ext \".{ext}\" {count}");
                        }
                        println!("    }}");
                    }
                    println!("}}");
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
            let results = db::dao::query_files_where(&pool, &where_clause).await?;
            for path in &results {
                println!("{path}");
            }
            if cli.verbose {
                eprintln!("{} match(es)", results.len());
            }
        }
    }

    db::close_db(pool).await;
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let cli = Cli::parse();

    if let Err(e) = run(cli).await {
        if e.downcast_ref::<ops::NoScanExists>().is_some() {
            eprintln!("error: {e}");
            process::exit(ops::EXIT_NO_SCAN);
        }
        eprintln!("error: {e:#}");
        process::exit(1);
    }
}
