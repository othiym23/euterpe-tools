use crate::cas;
use crate::db::dao::{self, FileInput};
use sqlx::SqlitePool;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::time::Instant;
use walkdir::WalkDir;

pub struct ScanStats {
    pub dirs_cached: usize,
    pub dirs_scanned: usize,
    pub dirs_removed: usize,
    pub elapsed_ms: u128,
}

#[cfg_attr(
    feature = "profiling",
    tracing::instrument(name = "scan_to_db", skip_all)
)]
pub async fn scan_to_db(
    root: &Path,
    pool: &SqlitePool,
    run_type: &str,
    exclude: &[String],
    verbose: bool,
) -> io::Result<(i64, ScanStats)> {
    let start = Instant::now();

    if verbose {
        eprintln!("starting scan: {}", root.display());
    }

    let root_str = root.to_string_lossy();
    let scan_id = dao::upsert_scan(pool, run_type, &root_str)
        .await
        .map_err(io::Error::other)?;

    let mut stats = ScanStats {
        dirs_cached: 0,
        dirs_scanned: 0,
        dirs_removed: 0,
        elapsed_ms: 0,
    };
    let mut seen_paths = HashSet::new();

    let mut orphan_hashes: Vec<String> = Vec::new();

    // Bulk-load all cached mtimes in one query instead of per-directory SELECTs.
    let cached_mtimes: HashMap<String, i64> = dao::all_directory_mtimes(pool, scan_id)
        .await
        .map_err(io::Error::other)?;

    // Walk without sorting — order doesn't matter for scanning (output reads
    // from DB with its own sort). Skipping sort avoids buffering + extra
    // syscalls per directory. filter_entry skips excluded directories so
    // walkdir never descends into them (e.g. Synology @eaDir).
    let exclude_set: HashSet<&str> = exclude.iter().map(|s| s.as_str()).collect();
    let walker = WalkDir::new(root).into_iter().filter_entry(|e| {
        if e.file_type().is_dir()
            && let Some(name) = e.file_name().to_str()
        {
            return !exclude_set.contains(name);
        }
        true
    });

    let mut pending: Vec<DirUpdate> = Vec::new();
    const BATCH_SIZE: usize = 256;

    for entry in walker {
        let entry = entry.map_err(io::Error::other)?;
        if !entry.file_type().is_dir() {
            continue;
        }

        let dir_path = entry.path().to_path_buf();
        let relative = dir_path
            .strip_prefix(root)
            .map_err(|e| io::Error::other(format!("path not under root: {}", e)))?
            .to_string_lossy()
            .into_owned();
        seen_paths.insert(relative.clone());

        let dir_meta = fs::metadata(&dir_path)?;
        let dir_mtime = dir_meta.mtime();
        let dir_size = dir_meta.size();

        if cached_mtimes.get(&relative) == Some(&dir_mtime) {
            stats.dirs_cached += 1;
            #[cfg(feature = "profiling")]
            if (stats.dirs_scanned + stats.dirs_cached).is_multiple_of(1000) {
                tracing::info!(
                    scanned = stats.dirs_scanned,
                    cached = stats.dirs_cached,
                    "scan_progress"
                );
                crate::profiling::sample_proc_metrics("scan_progress");
            }
            if verbose {
                eprintln!("directory unchanged, skipping: {}", dir_path.display());
            }
            continue;
        }

        stats.dirs_scanned += 1;

        #[cfg(feature = "profiling")]
        if (stats.dirs_scanned + stats.dirs_cached).is_multiple_of(1000) {
            tracing::info!(
                scanned = stats.dirs_scanned,
                cached = stats.dirs_cached,
                "scan_progress"
            );
            crate::profiling::sample_proc_metrics("scan_progress");
        }

        let files = scan_directory(&dir_path)?;
        if verbose {
            eprintln!("scanning: {} ({} files)", dir_path.display(), files.len());
        }

        pending.push(DirUpdate {
            relative,
            mtime: dir_mtime,
            size: dir_size,
            files,
        });

        if pending.len() >= BATCH_SIZE {
            let hashes = flush_pending(pool, scan_id, &mut pending)
                .await
                .map_err(io::Error::other)?;
            orphan_hashes.extend(hashes);
        }
    }

    // Flush any remaining directories
    if !pending.is_empty() {
        let hashes = flush_pending(pool, scan_id, &mut pending)
            .await
            .map_err(io::Error::other)?;
        orphan_hashes.extend(hashes);
    }

    // If nothing was scanned, every directory matched its cached mtime —
    // the DB is already in sync and no directories can be stale.
    let (removed, stale_orphans) = if stats.dirs_scanned > 0 {
        dao::remove_stale_directories(pool, scan_id, &seen_paths)
            .await
            .map_err(io::Error::other)?
    } else {
        (0, Vec::new())
    };
    stats.dirs_removed = removed;
    orphan_hashes.extend(stale_orphans);

    // Clean up any CAS blobs orphaned by file removals
    for hash in &orphan_hashes {
        let _ = cas::remove_blob(hash);
    }

    dao::finish_scan(pool, scan_id)
        .await
        .map_err(io::Error::other)?;

    stats.elapsed_ms = start.elapsed().as_millis();

    Ok((scan_id, stats))
}

/// Flush a batch of pending directory updates in a single transaction.
#[cfg_attr(feature = "profiling", tracing::instrument(name = "flush_pending", skip_all, fields(batch_size = pending.len())))]
/// Returns hashes of any orphaned CAS blobs from file removals.
async fn flush_pending(
    pool: &SqlitePool,
    scan_id: i64,
    pending: &mut Vec<DirUpdate>,
) -> Result<Vec<String>, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let mut orphan_hashes = Vec::new();

    for update in pending.drain(..) {
        let dir_id = {
            let result = sqlx::query(
                "INSERT INTO directories (scan_id, path, mtime, size)
                 VALUES (?, ?, ?, ?)
                 ON CONFLICT(scan_id, path) DO UPDATE SET mtime = excluded.mtime, size = excluded.size
                 RETURNING id",
            )
            .bind(scan_id)
            .bind(&update.relative)
            .bind(update.mtime)
            .bind(update.size as i64)
            .fetch_one(&mut *tx)
            .await?;
            sqlx::Row::get::<i64, _>(&result, 0)
        };

        let mut hashes = dao::replace_files_on(&mut tx, dir_id, &update.files).await?;
        orphan_hashes.append(&mut hashes);
    }

    tx.commit().await?;
    Ok(orphan_hashes)
}

/// Local struct for batching directory updates in scan_to_db.
struct DirUpdate {
    relative: String,
    mtime: i64,
    size: u64,
    files: Vec<dao::FileInput>,
}

fn scan_directory(dir: &Path) -> io::Result<Vec<FileInput>> {
    let mut files = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let ft = entry.file_type()?;
        if !ft.is_file() {
            continue;
        }
        let meta = entry.metadata()?;
        files.push(FileInput {
            filename: entry.file_name().to_string_lossy().into_owned(),
            size: meta.size(),
            ctime: meta.ctime(),
            mtime: meta.mtime(),
        });
    }
    files.sort_by(|a, b| a.filename.cmp(&b.filename));
    Ok(files)
}
