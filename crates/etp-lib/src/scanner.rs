use crate::cas;
use crate::db::dao::{self, FileInput, RemovedFile};
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
    let mut all_removed: Vec<RemovedFile> = Vec::new();

    // Bulk-load all cached mtimes in one query instead of per-directory SELECTs.
    let cached_mtimes: HashMap<String, i64> = dao::all_directory_mtimes(pool, scan_id)
        .await
        .map_err(io::Error::other)?;

    // Walk without sorting — order doesn't matter for scanning (output reads
    // from DB with its own sort). Skipping sort avoids buffering + extra
    // syscalls per directory. filter_entry skips excluded directories so
    // walkdir never descends into them (e.g. Synology @eaDir).
    let mut pending: Vec<DirUpdate> = Vec::new();
    const BATCH_SIZE: usize = 64;

    for entry in WalkDir::new(root)
        .sort_by(|_, _| std::cmp::Ordering::Equal)
        .into_iter()
        .filter_entry(|e| {
            !exclude.iter().any(|ex| {
                e.file_name()
                    .to_str()
                    .is_some_and(|name| name == ex.as_str())
            })
        })
    {
        let entry = entry.map_err(io::Error::other)?;
        if !entry.file_type().is_dir() {
            continue;
        }
        let dir_path = entry.path();
        let relative = dir_path
            .strip_prefix(root)
            .unwrap_or(dir_path)
            .to_string_lossy()
            .to_string();

        seen_paths.insert(relative.clone());

        let meta = fs::metadata(dir_path)?;
        let dir_mtime = meta.mtime();

        if let Some(&cached_mtime) = cached_mtimes.get(&relative)
            && cached_mtime == dir_mtime
        {
            stats.dirs_cached += 1;
            continue;
        }
        stats.dirs_scanned += 1;

        #[cfg(feature = "profiling")]
        let _dir_span = tracing::info_span!("scan_directory", path = %relative).entered();

        if verbose {
            let file_count = fs::read_dir(dir_path)?.count();
            let display_path = if relative.is_empty() { "." } else { &relative };
            eprintln!("scanning: {display_path} ({file_count} files)");
        }

        let mut files = Vec::new();
        let mut dir_size: u64 = 0;

        #[cfg(feature = "profiling")]
        let _readdir_span = tracing::info_span!("readdir_and_stat").entered();

        for child in fs::read_dir(dir_path)? {
            let child = child?;
            if child.file_type()?.is_file() {
                let child_meta = child.metadata()?;
                dir_size += child_meta.len();
                files.push(FileInput {
                    filename: child.file_name().to_string_lossy().to_string(),
                    size: child_meta.len(),
                    ctime: child_meta.ctime(),
                    mtime: child_meta.mtime(),
                });
            }
        }

        #[cfg(feature = "profiling")]
        drop(_readdir_span);

        pending.push(DirUpdate {
            relative,
            mtime: dir_mtime,
            size: dir_size,
            files,
        });

        if pending.len() >= BATCH_SIZE {
            let removed = flush_pending(pool, scan_id, &mut pending)
                .await
                .map_err(io::Error::other)?;
            all_removed.extend(removed);
        }
    }

    // Flush any remaining directories
    if !pending.is_empty() {
        let removed = flush_pending(pool, scan_id, &mut pending)
            .await
            .map_err(io::Error::other)?;
        all_removed.extend(removed);
    }

    // If nothing was scanned, every directory matched its cached mtime —
    // the DB is already in sync and no directories can be stale.
    let (dir_removed, stale_orphans) = if stats.dirs_scanned > 0 {
        dao::remove_stale_directories(pool, scan_id, &seen_paths)
            .await
            .map_err(io::Error::other)?
    } else {
        (0, Vec::new())
    };
    stats.dirs_removed = dir_removed;

    // Move-tracking: match removed files against newly appeared files by
    // size, then verify with BLAKE3 hash. Matched files get their dir_id
    // and filename updated; unmatched files are deleted.
    let orphan_hashes = reconcile_moves(pool, root, &mut all_removed, verbose)
        .await
        .map_err(io::Error::other)?;

    // Clean up CAS blobs orphaned by unmatched deletions + stale dirs
    for hash in orphan_hashes.iter().chain(stale_orphans.iter()) {
        let _ = cas::remove_blob(hash);
    }

    dao::finish_scan(pool, scan_id)
        .await
        .map_err(io::Error::other)?;

    stats.elapsed_ms = start.elapsed().as_millis();

    Ok((scan_id, stats))
}

/// Flush a batch of pending directory updates in a single transaction.
/// Returns removed files for move-tracking reconciliation.
#[cfg_attr(feature = "profiling", tracing::instrument(name = "flush_pending", skip_all, fields(batch_size = pending.len())))]
async fn flush_pending(
    pool: &SqlitePool,
    scan_id: i64,
    pending: &mut Vec<DirUpdate>,
) -> Result<Vec<RemovedFile>, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let mut removed = Vec::new();

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

        let sync = dao::replace_files_on(&mut tx, dir_id, &update.files).await?;
        removed.extend(sync.removed_files);
    }

    tx.commit().await?;
    Ok(removed)
}

/// Match removed files against newly appeared files to detect moves/renames.
///
/// Uses a two-phase approach: first match by size, then verify with BLAKE3
/// hash. When the old file is gone (expected for a move), accepts a size-only
/// match if there's exactly one removed file AND one candidate at that size.
///
/// The entire reconciliation runs in a single transaction for atomicity.
async fn reconcile_moves(
    pool: &SqlitePool,
    root: &Path,
    removed: &mut [RemovedFile],
    verbose: bool,
) -> Result<Vec<String>, sqlx::Error> {
    if removed.is_empty() {
        return Ok(Vec::new());
    }

    // Build a size → removed-files index
    let mut by_size: HashMap<u64, Vec<usize>> = HashMap::new();
    for (i, rf) in removed.iter().enumerate() {
        by_size.entry(rf.size).or_default().push(i);
    }

    let sizes: Vec<u64> = by_size.keys().copied().collect();
    if sizes.is_empty() {
        let mut tx = pool.begin().await?;
        let orphans = dao::delete_unmatched_files(&mut tx, removed).await?;
        tx.commit().await?;
        return Ok(orphans);
    }

    // Collect removed file IDs to exclude from candidates
    let removed_ids: HashSet<i64> = removed.iter().map(|rf| rf.file_id).collect();

    // Query files that match sizes of removed files. We filter out removed
    // file IDs to avoid matching a file against itself. We also exclude files
    // that already have metadata (they existed before this scan).
    let placeholders: String = sizes.iter().map(|_| "?").collect::<Vec<_>>().join(",");
    let id_excludes: String = removed_ids
        .iter()
        .map(|_| "?")
        .collect::<Vec<_>>()
        .join(",");
    let query = format!(
        "SELECT f.id, d.path, f.filename, f.size, f.dir_id, f.mtime, f.ctime
         FROM files f
         JOIN directories d ON f.dir_id = d.id
         WHERE f.metadata_scanned_at IS NULL
           AND f.id NOT IN ({id_excludes})
           AND f.size IN ({placeholders})"
    );
    let mut q = sqlx::query_as::<_, (i64, String, String, i64, i64, i64, i64)>(&query);
    for &id in &removed_ids {
        q = q.bind(id);
    }
    for &s in &sizes {
        q = q.bind(s as i64);
    }
    let candidates: Vec<(i64, String, String, i64, i64, i64, i64)> = q.fetch_all(pool).await?;

    // Pre-fetch directory paths for removed files (must happen before we
    // start the transaction, since pool has max_connections=1)
    let mut dir_paths: HashMap<i64, String> = HashMap::new();
    for rf in removed.iter() {
        if let std::collections::hash_map::Entry::Vacant(e) = dir_paths.entry(rf.dir_id) {
            let row: Option<(String,)> =
                sqlx::query_as("SELECT path FROM directories WHERE id = ?")
                    .bind(rf.dir_id)
                    .fetch_optional(pool)
                    .await?;
            if let Some((path,)) = row {
                e.insert(path);
            }
        }
    }

    // Build a size → candidates index to check uniqueness
    let mut candidates_by_size: HashMap<u64, Vec<usize>> = HashMap::new();
    for (i, (_, _, _, size, _, _, _)) in candidates.iter().enumerate() {
        candidates_by_size.entry(*size as u64).or_default().push(i);
    }

    let mut matched_removed: HashSet<usize> = HashSet::new();
    let mut matched_new: HashSet<i64> = HashSet::new();
    let mut tx = pool.begin().await?;

    for (new_id, dir_path, new_filename, new_size, new_dir_id, new_mtime, new_ctime) in &candidates
    {
        if matched_new.contains(new_id) {
            continue;
        }
        let size = *new_size as u64;
        let Some(removed_indices) = by_size.get(&size) else {
            continue;
        };

        // Build the full path of the new file for hashing
        let new_path = if dir_path.is_empty() {
            root.join(new_filename)
        } else {
            root.join(dir_path).join(new_filename)
        };
        let new_hash = match hash_file(&new_path) {
            Some(h) => h,
            None => continue,
        };

        for &idx in removed_indices {
            if matched_removed.contains(&idx) {
                continue;
            }
            let rf = &removed[idx];

            let old_path = match dir_paths.get(&rf.dir_id) {
                Some(dp) if dp.is_empty() => root.join(&rf.filename),
                Some(dp) => root.join(dp).join(&rf.filename),
                None => continue,
            };
            let old_hash = match hash_file(&old_path) {
                Some(h) => h,
                None => {
                    // Old file is gone (expected for a move). Accept a size-only
                    // match only when both sides are unique at this size.
                    let candidate_count = candidates_by_size.get(&size).map_or(0, |v| {
                        v.iter()
                            .filter(|&&ci2| !matched_new.contains(&candidates[ci2].0))
                            .count()
                    });
                    let removed_count = removed_indices
                        .iter()
                        .filter(|&&ri| !matched_removed.contains(&ri))
                        .count();

                    if candidate_count == 1 && removed_count == 1 {
                        if verbose {
                            eprintln!(
                                "  move detected (size match): {} -> {}",
                                rf.filename, new_filename
                            );
                        }
                        apply_move(
                            &mut tx,
                            rf.file_id,
                            *new_id,
                            *new_dir_id,
                            new_filename,
                            *new_mtime,
                            *new_ctime,
                        )
                        .await?;
                        matched_removed.insert(idx);
                        matched_new.insert(*new_id);
                        break;
                    }
                    continue;
                }
            };

            if old_hash == new_hash {
                if verbose {
                    eprintln!(
                        "  move detected (hash match): {} -> {}",
                        rf.filename, new_filename
                    );
                }
                apply_move(
                    &mut tx,
                    rf.file_id,
                    *new_id,
                    *new_dir_id,
                    new_filename,
                    *new_mtime,
                    *new_ctime,
                )
                .await?;
                matched_removed.insert(idx);
                matched_new.insert(*new_id);
                break;
            }
        }
    }

    // Delete unmatched removed files
    let unmatched: Vec<RemovedFile> = removed
        .iter()
        .enumerate()
        .filter(|(i, _)| !matched_removed.contains(i))
        .map(|(_, rf)| rf.clone())
        .collect();

    let orphans = dao::delete_unmatched_files(&mut tx, &unmatched).await?;
    tx.commit().await?;
    Ok(orphans)
}

/// Apply a file move: update the old record's location and stat info,
/// then delete the newly-inserted duplicate.
#[allow(clippy::explicit_auto_deref)]
async fn apply_move(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    old_file_id: i64,
    new_file_id: i64,
    new_dir_id: i64,
    new_filename: &str,
    new_mtime: i64,
    new_ctime: i64,
) -> Result<(), sqlx::Error> {
    // Delete the newly-inserted duplicate first to free the UNIQUE(dir_id, filename)
    // constraint, then update the old record to the new location.
    dao::delete_file_dependents(&mut **tx, new_file_id).await?;
    sqlx::query("DELETE FROM files WHERE id = ?")
        .bind(new_file_id)
        .execute(&mut **tx)
        .await?;
    sqlx::query("UPDATE files SET dir_id = ?, filename = ?, mtime = ?, ctime = ? WHERE id = ?")
        .bind(new_dir_id)
        .bind(new_filename)
        .bind(new_mtime)
        .bind(new_ctime)
        .bind(old_file_id)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// BLAKE3 hash of a file using streaming I/O (constant memory).
/// Returns None if the file can't be read.
fn hash_file(path: &Path) -> Option<String> {
    let file = fs::File::open(path).ok()?;
    let mut reader = io::BufReader::new(file);
    let mut hasher = blake3::Hasher::new();
    hasher.update_reader(&mut reader).ok()?;
    Some(hasher.finalize().to_hex().to_string())
}

/// Local struct for batching directory updates in scan_to_db.
struct DirUpdate {
    relative: String,
    mtime: i64,
    size: u64,
    files: Vec<dao::FileInput>,
}
