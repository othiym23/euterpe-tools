use clap::{Parser, Subcommand};
use etp_lib::ops;
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "etp-meta",
    about = "Read and manage audio file metadata",
    version = concat!(env!("CARGO_PKG_VERSION"), " (", env!("GIT_HASH"), ")")
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Print diagnostic info on stderr
    #[arg(short, long, global = true)]
    verbose: bool,

    /// Write Chrome Trace profiling data to a file
    #[cfg(feature = "profiling")]
    #[arg(long, global = true)]
    profile: bool,
}

#[derive(Subcommand)]
enum Commands {
    /// Scan audio files and store metadata in the database
    Scan {
        /// Root directory (must already be scanned with etp-csv or similar)
        #[arg(short = 'R', long = "root")]
        directory: Option<PathBuf>,

        /// Database path
        #[arg(long)]
        db: Option<PathBuf>,

        /// Directory names to exclude from scan
        #[arg(short, long, default_values_t = [String::from("@eaDir")])]
        exclude: Vec<String>,

        /// Force re-scan all files, ignoring mtime cache
        #[arg(long)]
        force: bool,
    },
    /// Read metadata from a single audio file (no database needed)
    Read {
        /// Path to audio file
        file: PathBuf,

        /// Include embedded image details
        #[arg(long)]
        images: bool,
    },
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let cli = Cli::parse();

    #[cfg(feature = "profiling")]
    let _profiling_guard = if cli.profile {
        Some(etp_lib::profiling::init_profiling(
            &etp_lib::profiling::trace_path("etp-meta"),
        ))
    } else {
        None
    };

    match cli.command {
        Commands::Scan {
            directory,
            db,
            exclude,
            force,
        } => {
            let db_path = match (&directory, &db) {
                (Some(dir), Some(db)) => {
                    ops::validate_directory(dir);
                    db.clone()
                }
                (Some(dir), None) => {
                    ops::validate_directory(dir);
                    dir.join(".etp.db")
                }
                (None, Some(db)) => db.clone(),
                (None, None) => {
                    eprintln!("error: --root or --db is required");
                    std::process::exit(1);
                }
            };

            let is_new = !db_path.exists();

            let pool = etp_lib::db::open_db(&db_path, cli.verbose)
                .await
                .unwrap_or_else(|e| {
                    eprintln!("error opening database: {e}");
                    std::process::exit(1);
                });

            // Find or create a scan for this directory
            let scan_id = if let Some(ref dir) = directory {
                let canon = dir.canonicalize().unwrap_or(dir.clone());
                let run_type = canon.to_string_lossy();

                if is_new || force {
                    ops::run_scan_to_db(dir, &pool, &run_type, &exclude, cli.verbose).await
                } else {
                    match etp_lib::db::dao::latest_scan_id(&pool, &run_type).await {
                        Ok(Some(id)) => id,
                        Ok(None) => {
                            ops::run_scan_to_db(dir, &pool, &run_type, &exclude, cli.verbose).await
                        }
                        Err(e) => {
                            eprintln!("error querying database: {e}");
                            std::process::exit(1);
                        }
                    }
                }
            } else {
                match etp_lib::db::dao::latest_any_scan_id(&pool).await {
                    Ok(Some(id)) => id,
                    _ => {
                        eprintln!("error: no scans found in database");
                        std::process::exit(1);
                    }
                }
            };

            let stats = ops::run_metadata_scan(&pool, scan_id, force, cli.verbose).await;

            eprintln!(
                "metadata scan complete: {} scanned, {} errors in {}ms",
                stats.files_scanned, stats.errors, stats.elapsed_ms
            );

            etp_lib::db::close_db(pool).await;
        }
        Commands::Read { file, images } => {
            if !file.is_file() {
                eprintln!("error: {} is not a file", file.display());
                std::process::exit(1);
            }

            match ops::read_file_metadata(&file) {
                Ok(mut json) => {
                    if !images && let Some(obj) = json.as_object_mut() {
                        obj.remove("images");
                    }
                    println!("{}", serde_json::to_string_pretty(&json).unwrap());
                }
                Err(e) => {
                    eprintln!("error reading {}: {e}", file.display());
                    std::process::exit(1);
                }
            }
        }
    }

    #[cfg(feature = "profiling")]
    if let Some(guard) = _profiling_guard {
        guard.finish();
    }
}
