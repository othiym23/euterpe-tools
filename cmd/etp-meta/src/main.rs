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
    /// Display CUE sheet info with MusicBrainz disc ID
    Cue {
        /// Path to .cue file
        file: PathBuf,

        /// Audio file(s) — one per FILE block in the CUE sheet (needed for disc
        /// ID and track durations). For single-image CUE sheets, pass one file.
        #[arg(long = "audio-file")]
        audio_files: Vec<PathBuf>,

        /// Output format
        #[arg(long, default_value = "summary", value_parser = ["summary", "cuetools", "eac"])]
        format: String,
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
        Commands::Cue {
            file,
            audio_files,
            format,
        } => {
            let content = std::fs::read_to_string(&file).unwrap_or_else(|e| {
                eprintln!("error: {}: {e}", file.display());
                std::process::exit(1);
            });
            let sheet = etp_cue::parse_cue_sheet(&content).unwrap_or_else(|e| {
                eprintln!("error parsing {}: {e}", file.display());
                std::process::exit(1);
            });

            // Get per-file durations in sectors from audio files
            let mut file_durations: Vec<u64> = Vec::new();
            for path in &audio_files {
                match etp_lib::metadata::read_metadata(path) {
                    Ok(meta) => {
                        let sectors = meta
                            .properties
                            .iter()
                            .find(|(k, _)| k == "audio_duration_ms")
                            .and_then(|(_, v)| v.as_u64())
                            .map(etp_cue::milliseconds_to_sectors);
                        match sectors {
                            Some(s) => file_durations.push(s),
                            None => {
                                eprintln!("warning: no duration found in {}", path.display());
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("warning: {}: {e}", path.display());
                    }
                }
            }
            if !audio_files.is_empty() && file_durations.len() != audio_files.len() {
                eprintln!(
                    "warning: got {} duration(s) for {} audio file(s); \
                     disc ID and track durations may be wrong",
                    file_durations.len(),
                    audio_files.len()
                );
            }

            let disc_id = if !file_durations.is_empty() {
                Some(etp_cue::compute_disc_id(&sheet, &file_durations))
            } else {
                None
            };

            match format.as_str() {
                "summary" => {
                    print!(
                        "{}",
                        etp_cue::format_album_summary(&sheet, &file_durations, disc_id.as_deref(),)
                    );
                }
                "cuetools" => {
                    print!("{}", etp_cue::format_cuetools_toc(&sheet, &file_durations));
                }
                "eac" => {
                    print!("{}", etp_cue::format_eac_toc(&sheet, &file_durations));
                }
                _ => unreachable!(),
            }
        }
    }

    #[cfg(feature = "profiling")]
    if let Some(guard) = _profiling_guard {
        guard.finish();
    }
}
