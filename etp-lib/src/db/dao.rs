use futures_core::Stream;
use sqlx::SqlitePool;
use std::collections::HashSet;
use std::pin::Pin;

/// A file record as returned by `list_files`, with the full path reconstructed.
#[derive(Debug, Clone)]
pub struct FileRecord {
    pub dir_path: String,
    pub filename: String,
    pub size: u64,
    pub ctime: i64,
    pub mtime: i64,
}

/// Insert or update a scan entry. Returns the scan ID.
#[cfg_attr(
    feature = "profiling",
    tracing::instrument(name = "upsert_scan", skip_all)
)]
pub async fn upsert_scan(
    pool: &SqlitePool,
    run_type: &str,
    root_path: &str,
) -> Result<i64, sqlx::Error> {
    let now = chrono_now();
    let result = sqlx::query(
        "INSERT INTO scans (run_type, root_path, started_at)
         VALUES (?, ?, ?)
         ON CONFLICT(run_type) DO UPDATE SET root_path = excluded.root_path, started_at = excluded.started_at, finished_at = NULL
         RETURNING id",
    )
    .bind(run_type)
    .bind(root_path)
    .bind(&now)
    .fetch_one(pool)
    .await?;
    Ok(sqlx::Row::get(&result, 0))
}

/// Mark a scan as finished.
#[cfg_attr(
    feature = "profiling",
    tracing::instrument(name = "finish_scan", skip_all)
)]
pub async fn finish_scan(pool: &SqlitePool, scan_id: i64) -> Result<(), sqlx::Error> {
    let now = chrono_now();
    sqlx::query("UPDATE scans SET finished_at = ? WHERE id = ?")
        .bind(&now)
        .bind(scan_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Get the cached mtime for a directory, or None if not cached.
pub async fn directory_mtime(
    pool: &SqlitePool,
    scan_id: i64,
    path: &str,
) -> Result<Option<i64>, sqlx::Error> {
    let row: Option<(i64,)> =
        sqlx::query_as("SELECT mtime FROM directories WHERE scan_id = ? AND path = ?")
            .bind(scan_id)
            .bind(path)
            .fetch_optional(pool)
            .await?;
    Ok(row.map(|r| r.0))
}

/// Bulk-load all cached directory mtimes for a scan into a HashMap.
/// Replaces per-directory `directory_mtime` queries during scanning.
#[cfg_attr(
    feature = "profiling",
    tracing::instrument(name = "all_directory_mtimes", skip_all)
)]
pub async fn all_directory_mtimes(
    pool: &SqlitePool,
    scan_id: i64,
) -> Result<std::collections::HashMap<String, i64>, sqlx::Error> {
    let rows: Vec<(String, i64)> =
        sqlx::query_as("SELECT path, mtime FROM directories WHERE scan_id = ?")
            .bind(scan_id)
            .fetch_all(pool)
            .await?;
    Ok(rows.into_iter().collect())
}

/// Insert or update a directory entry. Returns the directory ID.
pub async fn upsert_directory(
    pool: &SqlitePool,
    scan_id: i64,
    path: &str,
    mtime: i64,
    size: u64,
) -> Result<i64, sqlx::Error> {
    let result = sqlx::query(
        "INSERT INTO directories (scan_id, path, mtime, size)
         VALUES (?, ?, ?, ?)
         ON CONFLICT(scan_id, path) DO UPDATE SET mtime = excluded.mtime, size = excluded.size
         RETURNING id",
    )
    .bind(scan_id)
    .bind(path)
    .bind(mtime)
    .bind(size as i64)
    .fetch_one(pool)
    .await?;
    Ok(sqlx::Row::get(&result, 0))
}

/// A file to be inserted into the database.
pub struct FileInput {
    pub filename: String,
    pub size: u64,
    pub ctime: i64,
    pub mtime: i64,
}

/// Sync files for a directory — upserts each file (preserving file IDs for
/// unchanged filenames) and removes files no longer present. Clears
/// `metadata_scanned_at` when a file's mtime changes so metadata will be
/// re-read on the next metadata scan.
pub async fn replace_files(
    pool: &SqlitePool,
    dir_id: i64,
    files: &[FileInput],
) -> Result<Vec<String>, sqlx::Error> {
    let mut conn = pool.acquire().await?;
    replace_files_on(&mut conn, dir_id, files).await
}

/// Inner implementation that works on a mutable connection reference, so it
/// can be called within an existing transaction (e.g., `flush_pending`).
/// Returns hashes of any orphaned CAS blobs that should be removed from disk.
pub async fn replace_files_on(
    conn: &mut sqlx::SqliteConnection,
    dir_id: i64,
    files: &[FileInput],
) -> Result<Vec<String>, sqlx::Error> {
    let new_filenames: HashSet<&str> = files.iter().map(|f| f.filename.as_str()).collect();

    // Upsert each file. Clear metadata_scanned_at when mtime changes so
    // the metadata scanner knows to re-read this file.
    for f in files {
        sqlx::query(
            "INSERT INTO files (dir_id, filename, size, ctime, mtime)
             VALUES (?, ?, ?, ?, ?)
             ON CONFLICT(dir_id, filename) DO UPDATE SET
                 size = excluded.size,
                 ctime = excluded.ctime,
                 mtime = excluded.mtime,
                 metadata_scanned_at = CASE
                     WHEN files.mtime != excluded.mtime THEN NULL
                     ELSE files.metadata_scanned_at
                 END",
        )
        .bind(dir_id)
        .bind(&f.filename)
        .bind(f.size as i64)
        .bind(f.ctime)
        .bind(f.mtime)
        .execute(&mut *conn)
        .await?;
    }

    // Remove files no longer on disk. Clean up metadata rows first
    // (ON DELETE RESTRICT prevents deleting files that have metadata).
    let existing: Vec<(i64, String)> =
        sqlx::query_as("SELECT id, filename FROM files WHERE dir_id = ?")
            .bind(dir_id)
            .fetch_all(&mut *conn)
            .await?;

    let mut had_removals = false;
    for (file_id, filename) in &existing {
        if !new_filenames.contains(filename.as_str()) {
            delete_file_dependents(&mut *conn, *file_id).await?;
            sqlx::query("DELETE FROM files WHERE id = ?")
                .bind(file_id)
                .execute(&mut *conn)
                .await?;
            had_removals = true;
        }
    }
    let orphan_hashes = if had_removals {
        cleanup_orphan_blobs(&mut *conn).await?
    } else {
        Vec::new()
    };

    Ok(orphan_hashes)
}

/// Delete all rows that reference a file (metadata, cue_sheets, embedded_images).
/// Must be called before deleting the file itself due to ON DELETE RESTRICT.
/// Call `cleanup_orphan_blobs` after a batch of these to remove zero-refcount blobs.
pub async fn delete_file_dependents(
    conn: &mut sqlx::SqliteConnection,
    file_id: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE blobs SET ref_count = ref_count - 1
         WHERE hash IN (SELECT blob_hash FROM embedded_images WHERE file_id = ?)",
    )
    .bind(file_id)
    .execute(&mut *conn)
    .await?;
    sqlx::query("DELETE FROM embedded_images WHERE file_id = ?")
        .bind(file_id)
        .execute(&mut *conn)
        .await?;
    sqlx::query("DELETE FROM metadata WHERE file_id = ?")
        .bind(file_id)
        .execute(&mut *conn)
        .await?;
    sqlx::query("DELETE FROM cue_sheets WHERE file_id = ?")
        .bind(file_id)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// Bulk-delete all dependents for every file in a directory.
/// More efficient than calling `delete_file_dependents` per file.
pub async fn delete_directory_dependents(
    conn: &mut sqlx::SqliteConnection,
    dir_id: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE blobs SET ref_count = ref_count - 1
         WHERE hash IN (SELECT blob_hash FROM embedded_images
                        WHERE file_id IN (SELECT id FROM files WHERE dir_id = ?))",
    )
    .bind(dir_id)
    .execute(&mut *conn)
    .await?;
    sqlx::query(
        "DELETE FROM embedded_images WHERE file_id IN (SELECT id FROM files WHERE dir_id = ?)",
    )
    .bind(dir_id)
    .execute(&mut *conn)
    .await?;
    sqlx::query("DELETE FROM metadata WHERE file_id IN (SELECT id FROM files WHERE dir_id = ?)")
        .bind(dir_id)
        .execute(&mut *conn)
        .await?;
    sqlx::query("DELETE FROM cue_sheets WHERE file_id IN (SELECT id FROM files WHERE dir_id = ?)")
        .bind(dir_id)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// Remove blobs with zero or negative ref_count and return their hashes.
/// The caller is responsible for removing the corresponding CAS files
/// (the DAO layer has no filesystem dependency).
pub async fn cleanup_orphan_blobs(
    conn: &mut sqlx::SqliteConnection,
) -> Result<Vec<String>, sqlx::Error> {
    let orphans: Vec<(String,)> =
        sqlx::query_as("SELECT hash FROM blobs WHERE ref_count <= 0")
            .fetch_all(&mut *conn)
            .await?;
    sqlx::query("DELETE FROM blobs WHERE ref_count <= 0")
        .execute(&mut *conn)
        .await?;
    Ok(orphans.into_iter().map(|(h,)| h).collect())
}

/// Remove directories that are no longer present on disk. Returns `(count_removed, orphan_blob_hashes)`.
/// The caller is responsible for removing orphaned CAS files.
#[cfg_attr(
    feature = "profiling",
    tracing::instrument(name = "remove_stale_directories", skip_all)
)]
pub async fn remove_stale_directories(
    pool: &SqlitePool,
    scan_id: i64,
    seen_paths: &HashSet<String>,
) -> Result<(usize, Vec<String>), sqlx::Error> {
    let all_dirs: Vec<(i64, String)> =
        sqlx::query_as("SELECT id, path FROM directories WHERE scan_id = ?")
            .bind(scan_id)
            .fetch_all(pool)
            .await?;

    let mut removed = 0;
    let mut conn = pool.acquire().await?;
    for (dir_id, path) in &all_dirs {
        if !seen_paths.contains(path) {
            delete_directory_dependents(&mut *conn, *dir_id).await?;
            sqlx::query("DELETE FROM files WHERE dir_id = ?")
                .bind(dir_id)
                .execute(&mut *conn)
                .await?;
            sqlx::query("DELETE FROM directories WHERE id = ?")
                .bind(dir_id)
                .execute(&mut *conn)
                .await?;
            removed += 1;
        }
    }
    let orphan_hashes = cleanup_orphan_blobs(&mut *conn).await?;
    Ok((removed, orphan_hashes))
}

/// List all files for a scan, with full paths reconstructed by joining
/// `scans.root_path` and `directories.path`.
#[cfg_attr(
    feature = "profiling",
    tracing::instrument(name = "list_files", skip_all)
)]
pub async fn list_files(pool: &SqlitePool, scan_id: i64) -> Result<Vec<FileRecord>, sqlx::Error> {
    let rows: Vec<(String, String, String, i64, i64, i64)> = sqlx::query_as(
        "SELECT s.root_path, d.path, f.filename, f.size, f.ctime, f.mtime
         FROM files f
         JOIN directories d ON f.dir_id = d.id
         JOIN scans s ON d.scan_id = s.id
         WHERE d.scan_id = ?",
    )
    .bind(scan_id)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(root, dir_path, filename, size, ctime, mtime)| {
            let full_path = if dir_path.is_empty() {
                root
            } else {
                format!("{}/{}", root, dir_path)
            };
            FileRecord {
                dir_path: full_path,
                filename,
                size: size as u64,
                ctime,
                mtime,
            }
        })
        .collect())
}

/// Stream all files for a scan, yielding `FileRecord` values one at a time
/// via a database cursor instead of loading everything into memory.
pub fn stream_files(
    pool: &SqlitePool,
    scan_id: i64,
) -> Pin<Box<dyn Stream<Item = Result<FileRecord, sqlx::Error>> + Send + '_>> {
    let raw = sqlx::query_as::<_, (String, String, String, i64, i64, i64)>(
        "SELECT s.root_path, d.path, f.filename, f.size, f.ctime, f.mtime
         FROM files f
         JOIN directories d ON f.dir_id = d.id
         JOIN scans s ON d.scan_id = s.id
         WHERE d.scan_id = ?",
    )
    .bind(scan_id)
    .fetch(pool);

    Box::pin(MapStream {
        inner: Box::pin(raw),
    })
}

type RawRowStream<'a> = Pin<
    Box<
        dyn Stream<Item = Result<(String, String, String, i64, i64, i64), sqlx::Error>> + Send + 'a,
    >,
>;

/// Adapter that maps raw query rows to `FileRecord`.
struct MapStream<'a> {
    inner: RawRowStream<'a>,
}

impl Stream for MapStream<'_> {
    type Item = Result<FileRecord, sqlx::Error>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx).map(|opt| {
            opt.map(|res| {
                res.map(|(root, dir_path, filename, size, ctime, mtime)| {
                    let full_path = if dir_path.is_empty() {
                        root
                    } else {
                        format!("{}/{}", root, dir_path)
                    };
                    FileRecord {
                        dir_path: full_path,
                        filename,
                        size: size as u64,
                        ctime,
                        mtime,
                    }
                })
            })
        })
    }
}

/// List all files across all scans. Same as `list_files` but without scan filter.
pub async fn list_all_files(pool: &SqlitePool) -> Result<Vec<FileRecord>, sqlx::Error> {
    let rows: Vec<(String, String, String, i64, i64, i64)> = sqlx::query_as(
        "SELECT s.root_path, d.path, f.filename, f.size, f.ctime, f.mtime
         FROM files f
         JOIN directories d ON f.dir_id = d.id
         JOIN scans s ON d.scan_id = s.id",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(root, dir_path, filename, size, ctime, mtime)| {
            let full_path = if dir_path.is_empty() {
                root
            } else {
                format!("{}/{}", root, dir_path)
            };
            FileRecord {
                dir_path: full_path,
                filename,
                size: size as u64,
                ctime,
                mtime,
            }
        })
        .collect())
}

/// Stream all files across all scans. Same as `stream_files` but without scan filter.
pub fn stream_all_files(
    pool: &SqlitePool,
) -> Pin<Box<dyn Stream<Item = Result<FileRecord, sqlx::Error>> + Send + '_>> {
    let raw = sqlx::query_as::<_, (String, String, String, i64, i64, i64)>(
        "SELECT s.root_path, d.path, f.filename, f.size, f.ctime, f.mtime
         FROM files f
         JOIN directories d ON f.dir_id = d.id
         JOIN scans s ON d.scan_id = s.id",
    )
    .fetch(pool);

    Box::pin(MapStream {
        inner: Box::pin(raw),
    })
}

/// Get the total size of all files in a scan.
pub async fn total_size(pool: &SqlitePool, scan_id: i64) -> Result<u64, sqlx::Error> {
    let row: (i64,) = sqlx::query_as(
        "SELECT COALESCE(SUM(f.size), 0)
         FROM files f
         JOIN directories d ON f.dir_id = d.id
         WHERE d.scan_id = ?",
    )
    .bind(scan_id)
    .fetch_one(pool)
    .await?;
    Ok(row.0 as u64)
}

/// List all directory full paths for a scan, including empty directories.
pub async fn list_directory_paths(
    pool: &SqlitePool,
    scan_id: i64,
) -> Result<Vec<String>, sqlx::Error> {
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT s.root_path, d.path
         FROM directories d
         JOIN scans s ON d.scan_id = s.id
         WHERE d.scan_id = ?",
    )
    .bind(scan_id)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(root, dir_path)| {
            if dir_path.is_empty() {
                root
            } else {
                format!("{}/{}", root, dir_path)
            }
        })
        .collect())
}

/// Find the most recent finished scan for a given run_type.
pub async fn latest_scan_id(pool: &SqlitePool, run_type: &str) -> Result<Option<i64>, sqlx::Error> {
    let row: Option<(i64,)> = sqlx::query_as(
        "SELECT id FROM scans WHERE run_type = ? AND finished_at IS NOT NULL ORDER BY finished_at DESC LIMIT 1",
    )
    .bind(run_type)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|r| r.0))
}

/// Sum of all file sizes under a given directory prefix for a scan.
/// Empty prefix means total for the entire scan.
#[cfg_attr(
    feature = "profiling",
    tracing::instrument(name = "subtree_size", skip_all)
)]
pub async fn subtree_size(
    pool: &SqlitePool,
    scan_id: i64,
    relative_path_prefix: &str,
) -> Result<u64, sqlx::Error> {
    let row: (i64,) = if relative_path_prefix.is_empty() {
        sqlx::query_as(
            "SELECT COALESCE(SUM(f.size), 0)
             FROM files f
             JOIN directories d ON f.dir_id = d.id
             WHERE d.scan_id = ?",
        )
        .bind(scan_id)
        .fetch_one(pool)
        .await?
    } else {
        sqlx::query_as(
            "SELECT COALESCE(SUM(f.size), 0)
             FROM files f
             JOIN directories d ON f.dir_id = d.id
             WHERE d.scan_id = ? AND (d.path = ? OR d.path LIKE ? || '/%')",
        )
        .bind(scan_id)
        .bind(relative_path_prefix)
        .bind(relative_path_prefix)
        .fetch_one(pool)
        .await?
    };
    Ok(row.0 as u64)
}

/// Sizes grouped by top-level subdirectory name for a scan.
#[cfg_attr(
    feature = "profiling",
    tracing::instrument(name = "immediate_subdirectory_sizes", skip_all)
)]
pub async fn immediate_subdirectory_sizes(
    pool: &SqlitePool,
    scan_id: i64,
) -> Result<Vec<(String, u64)>, sqlx::Error> {
    // Get all directories that are immediate children of the root (path has no '/')
    // plus all deeper directories grouped by their first path component
    let rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT
           CASE WHEN INSTR(d.path, '/') > 0
             THEN SUBSTR(d.path, 1, INSTR(d.path, '/') - 1)
             ELSE d.path
           END AS top_dir,
           COALESCE(SUM(f.size), 0) AS total_size
         FROM directories d
         LEFT JOIN files f ON f.dir_id = d.id
         WHERE d.scan_id = ? AND d.path != ''
         GROUP BY top_dir
         ORDER BY top_dir",
    )
    .bind(scan_id)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(name, size)| (name, size as u64))
        .collect())
}

fn chrono_now() -> String {
    // Simple ISO 8601 timestamp without external dependency.
    // For the purposes of this application, second precision is sufficient.
    use std::time::SystemTime;
    let duration = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("system clock before Unix epoch");
    let secs = duration.as_secs();
    // Format as ISO 8601 using simple arithmetic
    let days = secs / 86400;
    let time_secs = secs % 86400;
    let hours = time_secs / 3600;
    let minutes = (time_secs % 3600) / 60;
    let seconds = time_secs % 60;

    // Days since epoch to Y-M-D (simplified Gregorian)
    let (year, month, day) = days_to_ymd(days);
    format!("{year:04}-{month:02}-{day:02}T{hours:02}:{minutes:02}:{seconds:02}Z")
}

fn days_to_ymd(mut days: u64) -> (u64, u64, u64) {
    // Algorithm from http://howardhinnant.github.io/date_algorithms.html
    days += 719_468;
    let era = days / 146_097;
    let doe = days - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::open_memory;

    #[tokio::test]
    async fn upsert_scan_creates_and_updates() {
        let pool = open_memory().await.unwrap();

        let id1 = upsert_scan(&pool, "music", "/volume1/music").await.unwrap();
        assert!(id1 > 0);

        // Same run_type should return a (possibly different) id, updating root_path
        let id2 = upsert_scan(&pool, "music", "/volume2/music").await.unwrap();
        // ON CONFLICT updates the existing row
        assert_eq!(id1, id2);

        // Different run_type should create a new row
        let id3 = upsert_scan(&pool, "television", "/volume1/tv")
            .await
            .unwrap();
        assert_ne!(id1, id3);
    }

    #[tokio::test]
    async fn finish_scan_sets_timestamp() {
        let pool = open_memory().await.unwrap();
        let id = upsert_scan(&pool, "test", "/tmp").await.unwrap();

        let before: (Option<String>,) =
            sqlx::query_as("SELECT finished_at FROM scans WHERE id = ?")
                .bind(id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(before.0.is_none());

        finish_scan(&pool, id).await.unwrap();

        let after: (Option<String>,) = sqlx::query_as("SELECT finished_at FROM scans WHERE id = ?")
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(after.0.is_some());
    }

    #[tokio::test]
    async fn directory_mtime_returns_none_for_unknown() {
        let pool = open_memory().await.unwrap();
        let scan_id = upsert_scan(&pool, "test", "/tmp").await.unwrap();

        let mtime = directory_mtime(&pool, scan_id, "subdir").await.unwrap();
        assert!(mtime.is_none());
    }

    #[tokio::test]
    async fn upsert_directory_and_query_mtime() {
        let pool = open_memory().await.unwrap();
        let scan_id = upsert_scan(&pool, "test", "/tmp").await.unwrap();

        let dir_id = upsert_directory(&pool, scan_id, "subdir", 1000, 4096)
            .await
            .unwrap();
        assert!(dir_id > 0);

        let mtime = directory_mtime(&pool, scan_id, "subdir").await.unwrap();
        assert_eq!(mtime, Some(1000));

        // Verify size was stored
        let row: (i64,) = sqlx::query_as("SELECT size FROM directories WHERE id = ?")
            .bind(dir_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(row.0, 4096);

        // Update mtime and size
        let dir_id2 = upsert_directory(&pool, scan_id, "subdir", 2000, 8192)
            .await
            .unwrap();
        assert_eq!(dir_id, dir_id2);

        let mtime = directory_mtime(&pool, scan_id, "subdir").await.unwrap();
        assert_eq!(mtime, Some(2000));

        // Verify size was updated
        let row: (i64,) = sqlx::query_as("SELECT size FROM directories WHERE id = ?")
            .bind(dir_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(row.0, 8192);
    }

    #[tokio::test]
    async fn replace_files_inserts_and_replaces() {
        let pool = open_memory().await.unwrap();
        let scan_id = upsert_scan(&pool, "test", "/tmp").await.unwrap();
        let dir_id = upsert_directory(&pool, scan_id, "", 1000, 0).await.unwrap();

        let files = vec![
            FileInput {
                filename: "a.txt".into(),
                size: 100,
                ctime: 1000,
                mtime: 2000,
            },
            FileInput {
                filename: "b.txt".into(),
                size: 200,
                ctime: 3000,
                mtime: 4000,
            },
        ];
        replace_files(&pool, dir_id, &files).await.unwrap();

        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM files WHERE dir_id = ?")
            .bind(dir_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count.0, 2);

        // Replace with different files
        let new_files = vec![FileInput {
            filename: "c.txt".into(),
            size: 300,
            ctime: 5000,
            mtime: 6000,
        }];
        replace_files(&pool, dir_id, &new_files).await.unwrap();

        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM files WHERE dir_id = ?")
            .bind(dir_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count.0, 1);
    }

    #[tokio::test]
    async fn replace_files_preserves_ids_on_rescan() {
        let pool = open_memory().await.unwrap();
        let scan_id = upsert_scan(&pool, "test", "/tmp").await.unwrap();
        let dir_id = upsert_directory(&pool, scan_id, "", 1000, 0).await.unwrap();

        let files = vec![
            FileInput {
                filename: "a.mp3".into(),
                size: 100,
                ctime: 1000,
                mtime: 2000,
            },
            FileInput {
                filename: "b.flac".into(),
                size: 200,
                ctime: 3000,
                mtime: 4000,
            },
        ];
        replace_files(&pool, dir_id, &files).await.unwrap();

        // Record file IDs
        let ids: Vec<(i64, String)> =
            sqlx::query_as("SELECT id, filename FROM files WHERE dir_id = ? ORDER BY filename")
                .bind(dir_id)
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(ids.len(), 2);
        let (id_a, _) = &ids[0];
        let (id_b, _) = &ids[1];

        // Rescan with same files but updated mtime for one
        let files2 = vec![
            FileInput {
                filename: "a.mp3".into(),
                size: 100,
                ctime: 1000,
                mtime: 2000, // unchanged
            },
            FileInput {
                filename: "b.flac".into(),
                size: 250,
                ctime: 3000,
                mtime: 5000, // changed
            },
        ];
        replace_files(&pool, dir_id, &files2).await.unwrap();

        let ids2: Vec<(i64, String, Option<String>)> = sqlx::query_as(
            "SELECT id, filename, metadata_scanned_at FROM files WHERE dir_id = ? ORDER BY filename",
        )
        .bind(dir_id)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(ids2.len(), 2);

        // IDs preserved
        assert_eq!(ids2[0].0, *id_a, "file ID for a.mp3 should be stable");
        assert_eq!(ids2[1].0, *id_b, "file ID for b.flac should be stable");

        // metadata_scanned_at cleared for changed file (b.flac)
        // (both are None since no metadata scan has run, but the CASE logic is exercised)
    }

    #[tokio::test]
    async fn remove_stale_directories_deletes_unseen() {
        let pool = open_memory().await.unwrap();
        let scan_id = upsert_scan(&pool, "test", "/tmp").await.unwrap();

        upsert_directory(&pool, scan_id, "keep", 1000, 4096)
            .await
            .unwrap();
        let stale_id = upsert_directory(&pool, scan_id, "stale", 1000, 4096)
            .await
            .unwrap();

        // Add a file to the stale dir to verify explicit deletion
        let files = vec![FileInput {
            filename: "orphan.txt".into(),
            size: 50,
            ctime: 100,
            mtime: 200,
        }];
        replace_files(&pool, stale_id, &files).await.unwrap();

        let mut seen = HashSet::new();
        seen.insert("keep".to_string());

        let (removed, _orphan_hashes) = remove_stale_directories(&pool, scan_id, &seen)
            .await
            .unwrap();
        assert_eq!(removed, 1);

        // Verify the files were explicitly deleted along with the directory
        let file_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM files WHERE dir_id = ?")
            .bind(stale_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(file_count.0, 0);
    }

    #[tokio::test]
    async fn list_files_returns_full_paths() {
        let pool = open_memory().await.unwrap();
        let scan_id = upsert_scan(&pool, "test", "/volume1/music").await.unwrap();

        let root_dir_id = upsert_directory(&pool, scan_id, "", 1000, 0).await.unwrap();
        replace_files(
            &pool,
            root_dir_id,
            &[FileInput {
                filename: "root.txt".into(),
                size: 10,
                ctime: 100,
                mtime: 200,
            }],
        )
        .await
        .unwrap();

        let sub_dir_id = upsert_directory(&pool, scan_id, "sub/dir", 2000, 0)
            .await
            .unwrap();
        replace_files(
            &pool,
            sub_dir_id,
            &[FileInput {
                filename: "deep.txt".into(),
                size: 20,
                ctime: 300,
                mtime: 400,
            }],
        )
        .await
        .unwrap();

        let files = list_files(&pool, scan_id).await.unwrap();
        assert_eq!(files.len(), 2);

        let mut paths: Vec<String> = files
            .iter()
            .map(|f| format!("{}/{}", f.dir_path, f.filename))
            .collect();
        paths.sort();
        assert_eq!(paths[0], "/volume1/music/root.txt");
        assert_eq!(paths[1], "/volume1/music/sub/dir/deep.txt");
    }

    #[tokio::test]
    async fn list_files_empty_scan() {
        let pool = open_memory().await.unwrap();
        let scan_id = upsert_scan(&pool, "test", "/tmp").await.unwrap();

        let files = list_files(&pool, scan_id).await.unwrap();
        assert!(files.is_empty());
    }

    #[tokio::test]
    async fn total_size_sums_correctly() {
        let pool = open_memory().await.unwrap();
        let scan_id = upsert_scan(&pool, "test", "/tmp").await.unwrap();
        let dir_id = upsert_directory(&pool, scan_id, "", 1000, 0).await.unwrap();

        replace_files(
            &pool,
            dir_id,
            &[
                FileInput {
                    filename: "a.txt".into(),
                    size: 100,
                    ctime: 0,
                    mtime: 0,
                },
                FileInput {
                    filename: "b.txt".into(),
                    size: 250,
                    ctime: 0,
                    mtime: 0,
                },
            ],
        )
        .await
        .unwrap();

        let size = total_size(&pool, scan_id).await.unwrap();
        assert_eq!(size, 350);
    }

    #[tokio::test]
    async fn total_size_empty_scan() {
        let pool = open_memory().await.unwrap();
        let scan_id = upsert_scan(&pool, "test", "/tmp").await.unwrap();

        let size = total_size(&pool, scan_id).await.unwrap();
        assert_eq!(size, 0);
    }

    #[tokio::test]
    async fn delete_directory_with_files_is_rejected() {
        let pool = open_memory().await.unwrap();
        let scan_id = upsert_scan(&pool, "test", "/tmp").await.unwrap();
        let dir_id = upsert_directory(&pool, scan_id, "has-files", 1000, 0)
            .await
            .unwrap();

        replace_files(
            &pool,
            dir_id,
            &[FileInput {
                filename: "keep.txt".into(),
                size: 10,
                ctime: 0,
                mtime: 0,
            }],
        )
        .await
        .unwrap();

        // Attempting to delete the directory without first deleting its files
        // must fail due to ON DELETE RESTRICT.
        let result = sqlx::query("DELETE FROM directories WHERE id = ?")
            .bind(dir_id)
            .execute(&pool)
            .await;
        assert!(result.is_err(), "expected foreign key violation");
    }

    #[tokio::test]
    async fn latest_scan_id_returns_none_when_no_scan() {
        let pool = open_memory().await.unwrap();
        let result = latest_scan_id(&pool, "nonexistent").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn latest_scan_id_returns_none_for_unfinished() {
        let pool = open_memory().await.unwrap();
        upsert_scan(&pool, "test", "/tmp").await.unwrap();
        // Not finished yet
        let result = latest_scan_id(&pool, "test").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn latest_scan_id_returns_finished_scan() {
        let pool = open_memory().await.unwrap();
        let id = upsert_scan(&pool, "test", "/tmp").await.unwrap();
        finish_scan(&pool, id).await.unwrap();
        let result = latest_scan_id(&pool, "test").await.unwrap();
        assert_eq!(result, Some(id));
    }

    #[tokio::test]
    async fn subtree_size_empty_prefix_is_total() {
        let pool = open_memory().await.unwrap();
        let scan_id = upsert_scan(&pool, "test", "/tmp").await.unwrap();
        let dir_id = upsert_directory(&pool, scan_id, "", 1000, 0).await.unwrap();
        replace_files(
            &pool,
            dir_id,
            &[
                FileInput {
                    filename: "a.txt".into(),
                    size: 100,
                    ctime: 0,
                    mtime: 0,
                },
                FileInput {
                    filename: "b.txt".into(),
                    size: 200,
                    ctime: 0,
                    mtime: 0,
                },
            ],
        )
        .await
        .unwrap();
        let sub_id = upsert_directory(&pool, scan_id, "sub", 1000, 0)
            .await
            .unwrap();
        replace_files(
            &pool,
            sub_id,
            &[FileInput {
                filename: "c.txt".into(),
                size: 300,
                ctime: 0,
                mtime: 0,
            }],
        )
        .await
        .unwrap();

        let total = subtree_size(&pool, scan_id, "").await.unwrap();
        assert_eq!(total, 600);
    }

    #[tokio::test]
    async fn subtree_size_with_prefix() {
        let pool = open_memory().await.unwrap();
        let scan_id = upsert_scan(&pool, "test", "/tmp").await.unwrap();
        let root_id = upsert_directory(&pool, scan_id, "", 1000, 0).await.unwrap();
        replace_files(
            &pool,
            root_id,
            &[FileInput {
                filename: "root.txt".into(),
                size: 100,
                ctime: 0,
                mtime: 0,
            }],
        )
        .await
        .unwrap();
        let sub_id = upsert_directory(&pool, scan_id, "sub", 1000, 0)
            .await
            .unwrap();
        replace_files(
            &pool,
            sub_id,
            &[FileInput {
                filename: "a.txt".into(),
                size: 200,
                ctime: 0,
                mtime: 0,
            }],
        )
        .await
        .unwrap();
        let deep_id = upsert_directory(&pool, scan_id, "sub/deep", 1000, 0)
            .await
            .unwrap();
        replace_files(
            &pool,
            deep_id,
            &[FileInput {
                filename: "b.txt".into(),
                size: 300,
                ctime: 0,
                mtime: 0,
            }],
        )
        .await
        .unwrap();

        let sub_total = subtree_size(&pool, scan_id, "sub").await.unwrap();
        assert_eq!(sub_total, 500); // 200 + 300
    }

    #[tokio::test]
    async fn immediate_subdirectory_sizes_groups_correctly() {
        let pool = open_memory().await.unwrap();
        let scan_id = upsert_scan(&pool, "test", "/tmp").await.unwrap();
        let root_id = upsert_directory(&pool, scan_id, "", 1000, 0).await.unwrap();
        replace_files(
            &pool,
            root_id,
            &[FileInput {
                filename: "root.txt".into(),
                size: 50,
                ctime: 0,
                mtime: 0,
            }],
        )
        .await
        .unwrap();
        let a_id = upsert_directory(&pool, scan_id, "alpha", 1000, 0)
            .await
            .unwrap();
        replace_files(
            &pool,
            a_id,
            &[FileInput {
                filename: "a.txt".into(),
                size: 100,
                ctime: 0,
                mtime: 0,
            }],
        )
        .await
        .unwrap();
        let a_deep_id = upsert_directory(&pool, scan_id, "alpha/deep", 1000, 0)
            .await
            .unwrap();
        replace_files(
            &pool,
            a_deep_id,
            &[FileInput {
                filename: "d.txt".into(),
                size: 150,
                ctime: 0,
                mtime: 0,
            }],
        )
        .await
        .unwrap();
        let b_id = upsert_directory(&pool, scan_id, "beta", 1000, 0)
            .await
            .unwrap();
        replace_files(
            &pool,
            b_id,
            &[FileInput {
                filename: "b.txt".into(),
                size: 200,
                ctime: 0,
                mtime: 0,
            }],
        )
        .await
        .unwrap();

        let subs = immediate_subdirectory_sizes(&pool, scan_id).await.unwrap();
        assert_eq!(subs.len(), 2);
        assert_eq!(subs[0], ("alpha".to_string(), 250)); // 100 + 150
        assert_eq!(subs[1], ("beta".to_string(), 200));
    }

    #[tokio::test]
    async fn stream_files_yields_all_records() {
        use std::future::poll_fn;
        use std::pin::Pin;

        let pool = open_memory().await.unwrap();
        let scan_id = upsert_scan(&pool, "test", "/volume1/music").await.unwrap();

        let root_dir_id = upsert_directory(&pool, scan_id, "", 1000, 0).await.unwrap();
        replace_files(
            &pool,
            root_dir_id,
            &[FileInput {
                filename: "root.txt".into(),
                size: 10,
                ctime: 100,
                mtime: 200,
            }],
        )
        .await
        .unwrap();

        let sub_dir_id = upsert_directory(&pool, scan_id, "sub/dir", 2000, 0)
            .await
            .unwrap();
        replace_files(
            &pool,
            sub_dir_id,
            &[FileInput {
                filename: "deep.txt".into(),
                size: 20,
                ctime: 300,
                mtime: 400,
            }],
        )
        .await
        .unwrap();

        // Collect from stream using poll_fn
        let mut stream = stream_files(&pool, scan_id);
        let mut streamed: Vec<FileRecord> = Vec::new();
        while let Some(result) = poll_fn(|cx| {
            use futures_core::Stream;
            Pin::new(&mut stream).poll_next(cx)
        })
        .await
        {
            streamed.push(result.unwrap());
        }

        // Compare with list_files
        let listed = list_files(&pool, scan_id).await.unwrap();
        assert_eq!(streamed.len(), listed.len());

        let mut stream_paths: Vec<String> = streamed
            .iter()
            .map(|f| format!("{}/{}", f.dir_path, f.filename))
            .collect();
        stream_paths.sort();
        let mut list_paths: Vec<String> = listed
            .iter()
            .map(|f| format!("{}/{}", f.dir_path, f.filename))
            .collect();
        list_paths.sort();
        assert_eq!(stream_paths, list_paths);
    }

    #[tokio::test]
    async fn delete_scan_with_directories_is_rejected() {
        let pool = open_memory().await.unwrap();
        let scan_id = upsert_scan(&pool, "test", "/tmp").await.unwrap();
        upsert_directory(&pool, scan_id, "child", 1000, 0)
            .await
            .unwrap();

        // Attempting to delete the scan without first deleting its directories
        // must fail due to the foreign key constraint.
        let result = sqlx::query("DELETE FROM scans WHERE id = ?")
            .bind(scan_id)
            .execute(&pool)
            .await;
        assert!(result.is_err(), "expected foreign key violation");
    }
}
