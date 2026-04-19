use crate::cas;
use crate::db::dao::{self, FileInput, RemovedFile};
use sqlx::SqlitePool;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::time::{Duration, Instant};
use walkdir::WalkDir;

/// Verbose progress cadence for long phases (walk, reconcile match loop).
const PROGRESS_EVERY: Duration = Duration::from_secs(30);

/// Log a completed phase when `verbose` is true. `detail` may be empty.
fn log_phase(verbose: bool, name: &str, elapsed: Duration, detail: &str) {
    if !verbose {
        return;
    }
    if detail.is_empty() {
        eprintln!("phase: {name} done in {:.2}s", elapsed.as_secs_f64());
    } else {
        eprintln!(
            "phase: {name} done in {:.2}s — {detail}",
            elapsed.as_secs_f64()
        );
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ScanError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
    #[error("walkdir error: {0}")]
    Walk(#[from] walkdir::Error),
}

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
    cas_dir: Option<&Path>,
) -> Result<(i64, ScanStats), ScanError> {
    let start = Instant::now();

    if verbose {
        eprintln!("starting scan: {}", root.display());
    }

    let root_str = root.to_string_lossy();
    let scan_id = dao::upsert_scan(pool, run_type, &root_str).await?;

    let mut stats = ScanStats {
        dirs_cached: 0,
        dirs_scanned: 0,
        dirs_removed: 0,
        elapsed_ms: 0,
    };
    let mut seen_paths = HashSet::new();
    let mut all_removed: Vec<RemovedFile> = Vec::new();

    // Bulk-load all cached mtimes in one query instead of per-directory SELECTs.
    let phase = Instant::now();
    let cached_mtimes: HashMap<String, i64> = dao::all_directory_mtimes(pool, scan_id).await?;
    log_phase(
        verbose,
        "all_directory_mtimes",
        phase.elapsed(),
        &format!("{} entries", cached_mtimes.len()),
    );

    // Walk without sorting — order doesn't matter for scanning (output reads
    // from DB with its own sort). Skipping sort avoids buffering + extra
    // syscalls per directory. filter_entry skips excluded directories so
    // walkdir never descends into them.
    let mut pending: Vec<DirUpdate> = Vec::new();
    const BATCH_SIZE: usize = 64;
    let walk_start = Instant::now();
    let mut last_progress = Instant::now();

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
        let entry = entry?;
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

        if verbose {
            let display_path = if relative.is_empty() { "." } else { &relative };
            eprintln!("scanning: {display_path} ({} files)", files.len());
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
            let removed = flush_pending(pool, scan_id, &mut pending).await?;
            all_removed.extend(removed);
        }

        if verbose && last_progress.elapsed() >= PROGRESS_EVERY {
            eprintln!(
                "progress: walking at {:.1}s — dirs_scanned={}, dirs_cached={}, seen_paths={}, removed_accum={}",
                walk_start.elapsed().as_secs_f64(),
                stats.dirs_scanned,
                stats.dirs_cached,
                seen_paths.len(),
                all_removed.len(),
            );
            last_progress = Instant::now();
        }
    }

    // Flush any remaining directories
    if !pending.is_empty() {
        let phase = Instant::now();
        let removed = flush_pending(pool, scan_id, &mut pending).await?;
        all_removed.extend(removed);
        log_phase(verbose, "final flush_pending", phase.elapsed(), "");
    }

    log_phase(
        verbose,
        "walk",
        walk_start.elapsed(),
        &format!(
            "dirs_scanned={}, dirs_cached={}, removed_files_accum={}, seen_paths={}",
            stats.dirs_scanned,
            stats.dirs_cached,
            all_removed.len(),
            seen_paths.len(),
        ),
    );

    // If nothing was scanned, every directory matched its cached mtime —
    // the DB is already in sync and no directories can be stale.
    let phase = Instant::now();
    let (dir_removed, stale_orphans) = if stats.dirs_scanned > 0 {
        dao::remove_stale_directories(pool, scan_id, &seen_paths).await?
    } else {
        (0, Vec::new())
    };
    stats.dirs_removed = dir_removed;
    log_phase(
        verbose,
        "remove_stale_directories",
        phase.elapsed(),
        &format!(
            "dir_removed={dir_removed}, stale_orphans={}",
            stale_orphans.len()
        ),
    );

    // Move-tracking: match removed files against newly appeared files by
    // size, then verify with BLAKE3 hash. Matched files get their dir_id
    // and filename updated; unmatched files are deleted.
    let phase = Instant::now();
    let orphan_hashes = reconcile_moves(pool, root, &mut all_removed, verbose).await?;
    log_phase(
        verbose,
        "reconcile_moves",
        phase.elapsed(),
        &format!("orphan_hashes={}", orphan_hashes.len()),
    );

    // Clean up CAS blobs orphaned by unmatched deletions + stale dirs
    let phase = Instant::now();
    let mut blobs_removed = 0usize;
    for hash in orphan_hashes.iter().chain(stale_orphans.iter()) {
        if cas::remove_blob(hash, cas_dir).is_ok() {
            blobs_removed += 1;
        }
    }
    log_phase(
        verbose,
        "cas blob cleanup",
        phase.elapsed(),
        &format!("blobs_removed={blobs_removed}"),
    );

    let phase = Instant::now();
    dao::finish_scan(pool, scan_id).await?;
    log_phase(verbose, "finish_scan", phase.elapsed(), "");

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

    if verbose {
        eprintln!("  reconcile: starting with {} removed files", removed.len());
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

    if verbose {
        eprintln!(
            "  reconcile: {} unique sizes, {} removed_ids — building candidate query",
            sizes.len(),
            removed_ids.len()
        );
    }

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
    let phase = Instant::now();
    let candidates: Vec<(i64, String, String, i64, i64, i64, i64)> = q.fetch_all(pool).await?;
    if verbose {
        eprintln!(
            "  reconcile: candidate query done in {:.2}s — {} candidates",
            phase.elapsed().as_secs_f64(),
            candidates.len()
        );
    }

    // Pre-fetch directory paths for removed files in one query (must happen
    // before we start the transaction, since pool has max_connections=1)
    let unique_dir_ids: HashSet<i64> = removed.iter().map(|rf| rf.dir_id).collect();
    let phase = Instant::now();
    let dir_paths: HashMap<i64, String> = if unique_dir_ids.is_empty() {
        HashMap::new()
    } else {
        let placeholders: String = unique_dir_ids
            .iter()
            .map(|_| "?")
            .collect::<Vec<_>>()
            .join(",");
        let query = format!("SELECT id, path FROM directories WHERE id IN ({placeholders})");
        let mut q = sqlx::query_as::<_, (i64, String)>(&query);
        for &id in &unique_dir_ids {
            q = q.bind(id);
        }
        q.fetch_all(pool).await?.into_iter().collect()
    };
    if verbose {
        eprintln!(
            "  reconcile: dir_paths query done in {:.2}s — {} dirs",
            phase.elapsed().as_secs_f64(),
            dir_paths.len()
        );
    }

    // Build a size → candidates index to check uniqueness
    let mut candidates_by_size: HashMap<u64, Vec<usize>> = HashMap::new();
    for (i, (_, _, _, size, _, _, _)) in candidates.iter().enumerate() {
        candidates_by_size.entry(*size as u64).or_default().push(i);
    }

    let mut matched_removed: HashSet<usize> = HashSet::new();
    let mut matched_new: HashSet<i64> = HashSet::new();
    let match_start = Instant::now();
    let mut match_last_progress = Instant::now();
    let mut hashes_computed = 0usize;
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
        let new_hash = match cas::hash_file(&new_path) {
            Some(h) => h,
            None => continue,
        };
        hashes_computed += 1;

        if verbose && match_last_progress.elapsed() >= PROGRESS_EVERY {
            eprintln!(
                "  reconcile: matching at {:.1}s — matched={}, hashes_computed={}",
                match_start.elapsed().as_secs_f64(),
                matched_new.len(),
                hashes_computed
            );
            match_last_progress = Instant::now();
        }

        for &idx in removed_indices {
            if matched_removed.contains(&idx) {
                continue;
            }
            let rf = &removed[idx];

            // Use stored content hash if available (from prior metadata scan),
            // otherwise read and hash the old file from disk.
            let old_hash = if let Some(ref stored) = rf.content_hash {
                Some(stored.clone())
            } else {
                let old_path = match dir_paths.get(&rf.dir_id) {
                    Some(dp) if dp.is_empty() => root.join(&rf.filename),
                    Some(dp) => root.join(dp).join(&rf.filename),
                    None => {
                        continue;
                    }
                };
                cas::hash_file(&old_path)
            };
            let old_hash = match old_hash {
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

    if verbose {
        eprintln!(
            "  reconcile: match loop done in {:.2}s — matched={}, hashes_computed={}",
            match_start.elapsed().as_secs_f64(),
            matched_new.len(),
            hashes_computed
        );
    }

    // Delete unmatched removed files
    let unmatched: Vec<RemovedFile> = removed
        .iter()
        .enumerate()
        .filter(|(i, _)| !matched_removed.contains(i))
        .map(|(_, rf)| rf.clone())
        .collect();

    let phase = Instant::now();
    let orphans = dao::delete_unmatched_files(&mut tx, &unmatched).await?;
    tx.commit().await?;
    if verbose {
        eprintln!(
            "  reconcile: delete_unmatched + commit done in {:.2}s — unmatched={}, orphans={}",
            phase.elapsed().as_secs_f64(),
            unmatched.len(),
            orphans.len()
        );
    }
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

/// Local struct for batching directory updates in scan_to_db.
struct DirUpdate {
    relative: String,
    mtime: i64,
    size: u64,
    files: Vec<dao::FileInput>,
}
