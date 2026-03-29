use clap::{Parser, Subcommand};
use etp_lib::{cas, ops};
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "etp-cas",
    about = "Content-addressable blob storage operations",
    version = concat!(env!("CARGO_PKG_VERSION"), " (", env!("GIT_HASH"), ")")
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Store a file in the CAS and print its BLAKE3 hash
    Store {
        /// Path to file to store
        file: PathBuf,
    },
    /// Retrieve a blob by hash
    Get {
        /// BLAKE3 hash of the blob
        hash: String,

        /// Write to file instead of stdout
        #[arg(short, long)]
        output: Option<PathBuf>,
    },
    /// Remove blobs not referenced by any database
    Gc {
        /// Database path (required)
        #[arg(long)]
        db: PathBuf,

        /// Print diagnostic info on stderr
        #[arg(short, long)]
        verbose: bool,
    },
    /// List all blob hashes in the CAS
    List,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let cli = Cli::parse();

    let config = etp_lib::config::RuntimeConfig::load_or_default();

    match cli.command {
        Commands::Store { file } => {
            let data = std::fs::read(&file).unwrap_or_else(|e| {
                eprintln!("error: {}: {e}", file.display());
                std::process::exit(1);
            });
            let (hash, size) =
                cas::store_blob(&data, config.cas_dir.as_deref()).unwrap_or_else(|e| {
                    eprintln!("error: failed to store blob: {e}");
                    std::process::exit(1);
                });
            println!("{hash}  {size}");
        }
        Commands::Get { hash, output } => {
            let data = cas::get_blob(&hash, config.cas_dir.as_deref()).unwrap_or_else(|e| {
                eprintln!("error: {hash}: {e}");
                std::process::exit(1);
            });
            match output {
                Some(path) => {
                    std::fs::write(&path, &data).unwrap_or_else(|e| {
                        eprintln!("error: {}: {e}", path.display());
                        std::process::exit(1);
                    });
                }
                None => {
                    use std::io::Write;
                    std::io::stdout().write_all(&data).unwrap_or_else(|e| {
                        eprintln!("error: {e}");
                        std::process::exit(1);
                    });
                }
            }
        }
        Commands::Gc { db, verbose } => {
            let pool = etp_lib::db::open_db(&db, verbose)
                .await
                .unwrap_or_else(|e| {
                    eprintln!("error opening database: {e}");
                    std::process::exit(1);
                });
            let removed = ops::gc_orphan_blobs(&pool, verbose, config.cas_dir.as_deref()).await;
            etp_lib::db::close_db(pool).await;
            if removed > 0 || verbose {
                eprintln!("removed {removed} orphan blob(s)");
            }
        }
        Commands::List => {
            let hashes = cas::list_blob_hashes(config.cas_dir.as_deref()).unwrap_or_else(|e| {
                eprintln!("error: {e}");
                std::process::exit(1);
            });
            for hash in &hashes {
                println!("{hash}");
            }
        }
    }
}
