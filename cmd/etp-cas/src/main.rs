use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use etp_lib::{cas, ops};
use std::path::PathBuf;
use std::process;

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

async fn run(cli: Cli) -> Result<()> {
    let config = etp_lib::config::RuntimeConfig::load_or_default();

    match cli.command {
        Commands::Store { file } => {
            let data = std::fs::read(&file).with_context(|| format!("{}", file.display()))?;
            let (hash, size) =
                cas::store_blob(&data, config.cas_dir.as_deref()).context("storing blob")?;
            println!("{hash}  {size}");
        }
        Commands::Get { hash, output } => {
            let data =
                cas::get_blob(&hash, config.cas_dir.as_deref()).with_context(|| hash.clone())?;
            match output {
                Some(path) => {
                    std::fs::write(&path, &data).with_context(|| format!("{}", path.display()))?;
                }
                None => {
                    use std::io::Write;
                    std::io::stdout()
                        .write_all(&data)
                        .context("writing to stdout")?;
                }
            }
        }
        Commands::Gc { db, verbose } => {
            let pool = etp_lib::db::open_db(&db, verbose)
                .await
                .context("opening database")?;
            let removed = ops::gc_orphan_blobs(&pool, verbose, config.cas_dir.as_deref()).await?;
            etp_lib::db::close_db(pool).await;
            if removed > 0 || verbose {
                eprintln!("removed {removed} orphan blob(s)");
            }
        }
        Commands::List => {
            let hashes = cas::list_blob_hashes(config.cas_dir.as_deref())?;
            for hash in &hashes {
                println!("{hash}");
            }
        }
    }

    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let cli = Cli::parse();

    if let Err(e) = run(cli).await {
        eprintln!("error: {e:#}");
        process::exit(1);
    }
}
