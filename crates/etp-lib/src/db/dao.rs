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

/// A file that disappeared from a directory during a scan. Held for
/// move-tracking reconciliation before being deleted.
#[derive(Debug, Clone)]
pub struct RemovedFile {
    pub file_id: i64,
    pub dir_id: i64,
    pub filename: String,
    pub size: u64,
    pub mtime: i64,
    pub content_hash: Option<String>,
}

/// Result of syncing files in a directory.
/// Result of syncing files in a directory.
pub struct SyncResult {
    /// Files that disappeared from this directory. Hold these for move-tracking
    /// reconciliation before deleting.
    pub removed_files: Vec<RemovedFile>,
}

/// Sync files for a directory — upserts each file (preserving file IDs for
/// unchanged filenames). Returns removed files for move-tracking instead of
/// deleting them immediately.
pub async fn replace_files(
    pool: &SqlitePool,
    dir_id: i64,
    files: &[FileInput],
) -> Result<SyncResult, sqlx::Error> {
    let mut conn = pool.acquire().await?;
    replace_files_on(&mut conn, dir_id, files).await
}

/// Inner implementation that works on a mutable connection reference, so it
/// can be called within an existing transaction (e.g., `flush_pending`).
pub async fn replace_files_on(
    conn: &mut sqlx::SqliteConnection,
    dir_id: i64,
    files: &[FileInput],
) -> Result<SyncResult, sqlx::Error> {
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
                 END,
                 content_hash = CASE
                     WHEN files.mtime != excluded.mtime THEN NULL
                     ELSE files.content_hash
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

    // Collect files no longer on disk as candidates for move-tracking.
    let existing: Vec<(i64, String, i64, i64, Option<String>)> = sqlx::query_as(
        "SELECT id, filename, size, mtime, content_hash FROM files WHERE dir_id = ?",
    )
    .bind(dir_id)
    .fetch_all(&mut *conn)
    .await?;

    let mut removed_files = Vec::new();
    for (file_id, filename, size, mtime, content_hash) in existing {
        if !new_filenames.contains(filename.as_str()) {
            removed_files.push(RemovedFile {
                file_id,
                dir_id,
                filename,
                size: size as u64,
                mtime,
                content_hash,
            });
        }
    }

    Ok(SyncResult { removed_files })
}

/// Move a file to a new directory and/or filename, preserving its ID and
/// all dependent metadata.
pub async fn move_file(
    conn: &mut sqlx::SqliteConnection,
    file_id: i64,
    new_dir_id: i64,
    new_filename: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE files SET dir_id = ?, filename = ? WHERE id = ?")
        .bind(new_dir_id)
        .bind(new_filename)
        .bind(file_id)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// Delete removed files that were not matched by move-tracking.
/// Returns orphaned CAS blob hashes.
pub async fn delete_unmatched_files(
    conn: &mut sqlx::SqliteConnection,
    removed: &[RemovedFile],
) -> Result<Vec<String>, sqlx::Error> {
    for rf in removed {
        delete_file_dependents(&mut *conn, rf.file_id).await?;
        sqlx::query("DELETE FROM files WHERE id = ?")
            .bind(rf.file_id)
            .execute(&mut *conn)
            .await?;
    }
    if !removed.is_empty() {
        cleanup_orphan_blobs(&mut *conn).await
    } else {
        Ok(Vec::new())
    }
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
    let orphans: Vec<(String,)> = sqlx::query_as("SELECT hash FROM blobs WHERE ref_count <= 0")
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
    let mut tx = pool.begin().await?;
    for (dir_id, path) in &all_dirs {
        if !seen_paths.contains(path) {
            delete_directory_dependents(&mut tx, *dir_id).await?;
            sqlx::query("DELETE FROM files WHERE dir_id = ?")
                .bind(dir_id)
                .execute(&mut *tx)
                .await?;
            sqlx::query("DELETE FROM directories WHERE id = ?")
                .bind(dir_id)
                .execute(&mut *tx)
                .await?;
            removed += 1;
        }
    }
    let orphan_hashes = cleanup_orphan_blobs(&mut tx).await?;
    tx.commit().await?;
    Ok((removed, orphan_hashes))
}

/// List all files for a scan, with full paths reconstructed by joining
/// `scans.root_path` and `directories.path`.
#[cfg_attr(
    feature = "profiling",
    tracing::instrument(name = "list_files", skip_all)
)]
pub async fn list_files(pool: &SqlitePool, scan_id: i64) -> Result<Vec<FileRecord>, sqlx::Error> {
    let rows: Vec<RawRow> = sqlx::query_as(
        "SELECT s.root_path, d.path, f.filename, f.size, f.ctime, f.mtime
         FROM files f
         JOIN directories d ON f.dir_id = d.id
         JOIN scans s ON d.scan_id = s.id
         WHERE d.scan_id = ?",
    )
    .bind(scan_id)
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter().map(row_to_file_record).collect())
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

type RawRow = (String, String, String, i64, i64, i64);
type RawRowStream<'a> = Pin<Box<dyn Stream<Item = Result<RawRow, sqlx::Error>> + Send + 'a>>;

fn row_to_file_record(row: RawRow) -> FileRecord {
    let (root, dir_path, filename, size, ctime, mtime) = row;
    let full_path = if dir_path.is_empty() {
        root
    } else {
        format!("{root}/{dir_path}")
    };
    FileRecord {
        dir_path: full_path,
        filename,
        size: size as u64,
        ctime,
        mtime,
    }
}

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
        self.inner
            .as_mut()
            .poll_next(cx)
            .map(|opt| opt.map(|res| res.map(row_to_file_record)))
    }
}

/// List all files across all scans. Same as `list_files` but without scan filter.
pub async fn list_all_files(pool: &SqlitePool) -> Result<Vec<FileRecord>, sqlx::Error> {
    let rows: Vec<RawRow> = sqlx::query_as(
        "SELECT s.root_path, d.path, f.filename, f.size, f.ctime, f.mtime
         FROM files f
         JOIN directories d ON f.dir_id = d.id
         JOIN scans s ON d.scan_id = s.id",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter().map(row_to_file_record).collect())
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

// Query variants differ on two axes — scan-id filter and system-file filter —
// so there are four `&'static str` consts. The streaming API needs a static
// pointer without per-call string allocation. Every variant pushes the user
// match into SQLite via the REGEXP UDF registered by `with_regexp()`;
// callers handle case folding by prefixing `(?i)` as needed.
//
// The `*_NO_SYS` variants add `AND NOT (<full_path>) REGEXP ?` so system
// files (@eaDir, .etp.db, etc.) are discarded at the B-tree layer rather
// than crossing into Rust. SQLite short-circuits AND chains, so rows that
// don't match the user pattern never hit the system regex.
const FIND_REGEXP_ALL: &str = "SELECT s.root_path, d.path, f.filename, f.size, f.ctime, f.mtime
     FROM files f
     JOIN directories d ON f.dir_id = d.id
     JOIN scans s ON d.scan_id = s.id
     WHERE (s.root_path || CASE WHEN d.path = '' THEN '' ELSE '/' || d.path END || '/' || f.filename) REGEXP ?";
const FIND_REGEXP_SCAN: &str = "SELECT s.root_path, d.path, f.filename, f.size, f.ctime, f.mtime
     FROM files f
     JOIN directories d ON f.dir_id = d.id
     JOIN scans s ON d.scan_id = s.id
     WHERE d.scan_id = ?
       AND (s.root_path || CASE WHEN d.path = '' THEN '' ELSE '/' || d.path END || '/' || f.filename) REGEXP ?";
const FIND_REGEXP_ALL_NO_SYS: &str = "SELECT s.root_path, d.path, f.filename, f.size, f.ctime, f.mtime
     FROM files f
     JOIN directories d ON f.dir_id = d.id
     JOIN scans s ON d.scan_id = s.id
     WHERE (s.root_path || CASE WHEN d.path = '' THEN '' ELSE '/' || d.path END || '/' || f.filename) REGEXP ?
       AND NOT ((s.root_path || CASE WHEN d.path = '' THEN '' ELSE '/' || d.path END || '/' || f.filename) REGEXP ?)";
const FIND_REGEXP_SCAN_NO_SYS: &str = "SELECT s.root_path, d.path, f.filename, f.size, f.ctime, f.mtime
     FROM files f
     JOIN directories d ON f.dir_id = d.id
     JOIN scans s ON d.scan_id = s.id
     WHERE d.scan_id = ?
       AND (s.root_path || CASE WHEN d.path = '' THEN '' ELSE '/' || d.path END || '/' || f.filename) REGEXP ?
       AND NOT ((s.root_path || CASE WHEN d.path = '' THEN '' ELSE '/' || d.path END || '/' || f.filename) REGEXP ?)";

fn find_query_str(has_scan_id: bool, has_system_re: bool) -> &'static str {
    match (has_scan_id, has_system_re) {
        (true, true) => FIND_REGEXP_SCAN_NO_SYS,
        (true, false) => FIND_REGEXP_SCAN,
        (false, true) => FIND_REGEXP_ALL_NO_SYS,
        (false, false) => FIND_REGEXP_ALL,
    }
}

/// Build the SQL query + bind the user pattern, scan id, and system regex
/// for the `list_files_matching` / `stream_files_matching` call path. Bind
/// values are owned so the returned query is not tied to caller lifetimes —
/// important for `stream_files_matching`, which must outlive its inputs.
fn bind_find_query(
    scan_id: Option<i64>,
    pattern: &str,
    exclude_system_re: Option<&str>,
) -> sqlx::query::QueryAs<'static, sqlx::Sqlite, RawRow, sqlx::sqlite::SqliteArguments<'static>> {
    let query = find_query_str(scan_id.is_some(), exclude_system_re.is_some());
    let mut q = sqlx::query_as::<_, RawRow>(query);
    if let Some(id) = scan_id {
        q = q.bind(id);
    }
    q = q.bind(pattern.to_string());
    if let Some(sys) = exclude_system_re {
        q = q.bind(sys.to_string());
    }
    q
}

/// List files whose reconstructed full path matches `pattern` (a regex).
///
/// - `scan_id: Some(id)` searches a single scan; `None` searches all scans.
/// - `exclude_system_re: Some(r)` adds `AND NOT path REGEXP r` to the WHERE
///   clause, so system files are filtered out at the SQL layer.
/// - Case sensitivity is controlled by the pattern itself — callers that
///   want case-insensitive matching should prefix `(?i)`.
pub async fn list_files_matching(
    pool: &SqlitePool,
    scan_id: Option<i64>,
    pattern: &str,
    exclude_system_re: Option<&str>,
) -> Result<Vec<FileRecord>, sqlx::Error> {
    let rows = bind_find_query(scan_id, pattern, exclude_system_re)
        .fetch_all(pool)
        .await?;
    Ok(rows.into_iter().map(row_to_file_record).collect())
}

/// Stream files whose reconstructed full path matches `pattern` (a regex).
/// See [`list_files_matching`] for parameter notes.
pub fn stream_files_matching<'a>(
    pool: &'a SqlitePool,
    scan_id: Option<i64>,
    pattern: &str,
    exclude_system_re: Option<&str>,
) -> Pin<Box<dyn Stream<Item = Result<FileRecord, sqlx::Error>> + Send + 'a>> {
    let raw = bind_find_query(scan_id, pattern, exclude_system_re).fetch(pool);
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

/// Return the ID of the most recently finished scan, regardless of run_type.
pub async fn latest_any_scan_id(pool: &SqlitePool) -> Result<Option<i64>, sqlx::Error> {
    let row: Option<(i64,)> = sqlx::query_as(
        "SELECT id FROM scans WHERE finished_at IS NOT NULL ORDER BY finished_at DESC LIMIT 1",
    )
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

// --- Metadata DAO functions ---

/// A file record for metadata scanning, with its database ID.
#[derive(Debug, Clone)]
pub struct MediaFileRecord {
    pub file_id: i64,
    pub root_path: String,
    pub dir_path: String,
    pub filename: String,
    pub mtime: i64,
}

/// A tag name/value pair from the metadata table.
#[derive(Debug, Clone)]
pub struct MetadataRecord {
    pub tag_name: String,
    pub value: String,
}

/// Input for inserting an embedded image reference.
pub struct EmbeddedImageInput {
    pub image_type: String,
    pub mime_type: String,
    pub blob_hash: String,
    pub blob_size: u64,
    pub width: Option<i64>,
    pub height: Option<i64>,
}

/// Query files that need a metadata scan: either never scanned or stale
/// (mtime changed since last scan). Filtered by extension, ordered by
/// directory path then filename for sequential I/O on spinning disks.
pub async fn files_needing_metadata_scan(
    pool: &SqlitePool,
    scan_id: i64,
    extensions: &[&str],
    force: bool,
) -> Result<Vec<MediaFileRecord>, sqlx::Error> {
    let like_clauses: Vec<String> = extensions
        .iter()
        .map(|_| "lower(f.filename) LIKE ?".to_string())
        .collect();
    let ext_filter = like_clauses.join(" OR ");
    let staleness_filter = if force {
        "1 = 1".to_string()
    } else {
        "(f.metadata_scanned_at IS NULL OR f.metadata_scanned_at < datetime(f.mtime, 'unixepoch'))"
            .to_string()
    };
    let query = format!(
        "SELECT f.id, s.root_path, d.path, f.filename, f.mtime
         FROM files f
         JOIN directories d ON f.dir_id = d.id
         JOIN scans s ON d.scan_id = s.id
         WHERE d.scan_id = ?
           AND {staleness_filter}
           AND ({ext_filter})
         ORDER BY d.path, f.filename"
    );

    let mut q = sqlx::query_as::<_, (i64, String, String, String, i64)>(&query).bind(scan_id);
    for ext in extensions {
        q = q.bind(format!("%.{ext}"));
    }

    let rows = q.fetch_all(pool).await?;
    Ok(rows
        .into_iter()
        .map(
            |(file_id, root_path, dir_path, filename, mtime)| MediaFileRecord {
                file_id,
                root_path,
                dir_path,
                filename,
                mtime,
            },
        )
        .collect())
}

/// Replace all metadata tags for a file. Deletes existing tags and inserts
/// the new set in a single transaction.
pub async fn replace_file_metadata(
    pool: &SqlitePool,
    file_id: i64,
    tags: &[(String, String)],
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    sqlx::query("DELETE FROM metadata WHERE file_id = ?")
        .bind(file_id)
        .execute(&mut *tx)
        .await?;
    for (tag_name, value) in tags {
        sqlx::query("INSERT INTO metadata (file_id, tag_name, value) VALUES (?, ?, ?)")
            .bind(file_id)
            .bind(tag_name)
            .bind(value)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Insert or increment ref_count for a blob.
pub async fn upsert_blob(pool: &SqlitePool, hash: &str, size: u64) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO blobs (hash, size, ref_count) VALUES (?, ?, 1)
         ON CONFLICT(hash) DO UPDATE SET ref_count = ref_count + 1",
    )
    .bind(hash)
    .bind(size as i64)
    .execute(pool)
    .await?;
    Ok(())
}

/// Replace all embedded image references for a file. Handles blob ref_count
/// bookkeeping: decrements old refs, deletes old rows, inserts new rows,
/// upserts new blobs. Returns hashes of any orphaned blobs.
pub async fn replace_embedded_images(
    pool: &SqlitePool,
    file_id: i64,
    images: &[EmbeddedImageInput],
) -> Result<Vec<String>, sqlx::Error> {
    let mut tx = pool.begin().await?;

    // Decrement old blob refs
    sqlx::query(
        "UPDATE blobs SET ref_count = ref_count - 1
         WHERE hash IN (SELECT blob_hash FROM embedded_images WHERE file_id = ?)",
    )
    .bind(file_id)
    .execute(&mut *tx)
    .await?;

    // Delete old image rows
    sqlx::query("DELETE FROM embedded_images WHERE file_id = ?")
        .bind(file_id)
        .execute(&mut *tx)
        .await?;

    // Insert new images and upsert blobs
    for img in images {
        sqlx::query(
            "INSERT INTO blobs (hash, size, ref_count) VALUES (?, ?, 1)
             ON CONFLICT(hash) DO UPDATE SET ref_count = ref_count + 1",
        )
        .bind(&img.blob_hash)
        .bind(img.blob_size as i64)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            "INSERT INTO embedded_images (file_id, image_type, mime_type, blob_hash, width, height)
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(file_id)
        .bind(&img.image_type)
        .bind(&img.mime_type)
        .bind(&img.blob_hash)
        .bind(img.width)
        .bind(img.height)
        .execute(&mut *tx)
        .await?;
    }

    let orphans = cleanup_orphan_blobs(&mut tx).await?;
    tx.commit().await?;
    Ok(orphans)
}

/// Insert or update a cue sheet for a file.
pub async fn upsert_cue_sheet(
    pool: &SqlitePool,
    file_id: i64,
    source: &str,
    content: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO cue_sheets (file_id, source, content) VALUES (?, ?, ?)
         ON CONFLICT(file_id, source) DO UPDATE SET content = excluded.content",
    )
    .bind(file_id)
    .bind(source)
    .bind(content)
    .execute(pool)
    .await?;
    Ok(())
}

/// Mark a file's metadata as scanned at the current time.
pub async fn mark_metadata_scanned(
    pool: &SqlitePool,
    file_id: i64,
    content_hash: Option<&str>,
) -> Result<(), sqlx::Error> {
    let now = chrono_now();
    sqlx::query("UPDATE files SET metadata_scanned_at = ?, content_hash = ? WHERE id = ?")
        .bind(&now)
        .bind(content_hash)
        .bind(file_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Get all metadata tags for a file.
pub async fn get_file_metadata(
    pool: &SqlitePool,
    file_id: i64,
) -> Result<Vec<MetadataRecord>, sqlx::Error> {
    let rows: Vec<(String, String)> =
        sqlx::query_as("SELECT tag_name, value FROM metadata WHERE file_id = ? ORDER BY tag_name")
            .bind(file_id)
            .fetch_all(pool)
            .await?;
    Ok(rows
        .into_iter()
        .map(|(tag_name, value)| MetadataRecord { tag_name, value })
        .collect())
}

/// Look up a file's database ID by scan, directory path, and filename.
pub async fn get_file_id_by_path(
    pool: &SqlitePool,
    scan_id: i64,
    dir_path: &str,
    filename: &str,
) -> Result<Option<i64>, sqlx::Error> {
    let row: Option<(i64,)> = sqlx::query_as(
        "SELECT f.id FROM files f
         JOIN directories d ON f.dir_id = d.id
         WHERE d.scan_id = ? AND d.path = ? AND f.filename = ?",
    )
    .bind(scan_id)
    .bind(dir_path)
    .bind(filename)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(id,)| id))
}

/// Return the set of all blob hashes referenced by the database.
pub async fn referenced_blob_hashes(pool: &SqlitePool) -> Result<HashSet<String>, sqlx::Error> {
    let rows: Vec<(String,)> = sqlx::query_as("SELECT hash FROM blobs")
        .fetch_all(pool)
        .await?;
    Ok(rows.into_iter().map(|(h,)| h).collect())
}

/// SQL expression that reconstructs the full file path from root_path, dir_path,
/// and filename. Used in multiple query functions.
const FULL_PATH_SQL: &str =
    "s.root_path || CASE WHEN d.path = '' THEN '' ELSE '/' || d.path END || '/' || f.filename";

/// Count of files in a scan.
pub async fn count_files(pool: &SqlitePool, scan_id: i64) -> Result<i64, sqlx::Error> {
    let row: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM files f
         JOIN directories d ON f.dir_id = d.id
         WHERE d.scan_id = ?",
    )
    .bind(scan_id)
    .fetch_one(pool)
    .await?;
    Ok(row.0)
}

/// List files filtered by a directory path prefix. The prefix is matched
/// against the full reconstructed path. Pass an empty string to list all files.
pub async fn list_files_in_directory(
    pool: &SqlitePool,
    scan_id: i64,
    dir_prefix: &str,
) -> Result<Vec<FileRecord>, sqlx::Error> {
    let pattern = format!("{dir_prefix}%");
    let query = format!(
        "SELECT s.root_path, d.path, f.filename, f.size, f.ctime, f.mtime
         FROM files f
         JOIN directories d ON f.dir_id = d.id
         JOIN scans s ON d.scan_id = s.id
         WHERE d.scan_id = ? AND {FULL_PATH_SQL} LIKE ?"
    );
    let rows: Vec<(String, String, String, i64, i64, i64)> = sqlx::query_as(&query)
        .bind(scan_id)
        .bind(&pattern)
        .fetch_all(pool)
        .await?;
    Ok(rows
        .into_iter()
        .map(|(root, dir_path, filename, size, ctime, mtime)| {
            let full_path = if dir_path.is_empty() {
                root
            } else {
                format!("{root}/{dir_path}")
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

/// Look up a file's database ID by matching against the full reconstructed path.
/// The `path_suffix` is matched against `root_path/dir_path/filename` using
/// a trailing match, so both absolute and relative paths work.
pub async fn find_file_id_by_path_suffix(
    pool: &SqlitePool,
    scan_id: i64,
    path_suffix: &str,
) -> Result<Option<i64>, sqlx::Error> {
    let pattern = format!("%{path_suffix}");
    let query = format!(
        "SELECT f.id FROM files f
         JOIN directories d ON f.dir_id = d.id
         JOIN scans s ON d.scan_id = s.id
         WHERE d.scan_id = ?
           AND ({FULL_PATH_SQL}) LIKE ?
         LIMIT 1"
    );
    let row: Option<(i64,)> = sqlx::query_as(&query)
        .bind(scan_id)
        .bind(&pattern)
        .fetch_optional(pool)
        .await?;
    Ok(row.map(|(id,)| id))
}

// --- Query interface DAO functions ---

/// Find files whose metadata matches a tag name and value pattern.
/// `value_pattern` uses SQL LIKE syntax (% for wildcard).
pub async fn find_files_by_tag(
    pool: &SqlitePool,
    scan_id: Option<i64>,
    tag_name: &str,
    value_pattern: &str,
) -> Result<Vec<(String, String)>, sqlx::Error> {
    let (query, needs_scan_id) = if scan_id.is_some() {
        (
            format!(
                "SELECT {FULL_PATH_SQL}, m.value
                 FROM metadata m
                 JOIN files f ON m.file_id = f.id
                 JOIN directories d ON f.dir_id = d.id
                 JOIN scans s ON d.scan_id = s.id
                 WHERE d.scan_id = ? AND m.tag_name = ? AND m.value LIKE ?
                 ORDER BY d.path, f.filename"
            ),
            true,
        )
    } else {
        (
            format!(
                "SELECT {FULL_PATH_SQL}, m.value
                 FROM metadata m
                 JOIN files f ON m.file_id = f.id
                 JOIN directories d ON f.dir_id = d.id
                 JOIN scans s ON d.scan_id = s.id
                 WHERE m.tag_name = ? AND m.value LIKE ?
                 ORDER BY d.path, f.filename"
            ),
            false,
        )
    };

    let mut q = sqlx::query_as::<_, (String, String)>(&query);
    if needs_scan_id {
        q = q.bind(scan_id.unwrap());
    }
    q = q.bind(tag_name).bind(value_pattern);
    q.fetch_all(pool).await
}

/// Count files grouped by extension (the part after the last dot).
/// Extension extraction is done in Rust because SQLite lacks a
/// "last index of" function for reliable multi-dot filename handling.
pub async fn count_files_by_extension(
    pool: &SqlitePool,
    scan_id: Option<i64>,
) -> Result<Vec<(String, i64)>, sqlx::Error> {
    let query = if scan_id.is_some() {
        "SELECT f.filename FROM files f
         JOIN directories d ON f.dir_id = d.id
         WHERE d.scan_id = ?"
    } else {
        "SELECT f.filename FROM files f"
    };

    let mut q = sqlx::query_as::<_, (String,)>(query);
    if let Some(id) = scan_id {
        q = q.bind(id);
    }
    let rows = q.fetch_all(pool).await?;

    let mut counts: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
    for (filename,) in &rows {
        if let Some(dot_pos) = filename.rfind('.') {
            let ext = filename[dot_pos + 1..].to_ascii_lowercase();
            if !ext.is_empty() {
                *counts.entry(ext).or_default() += 1;
            }
        }
    }

    let mut result: Vec<(String, i64)> = counts.into_iter().collect();
    result.sort_by_key(|r| std::cmp::Reverse(r.1));
    Ok(result)
}

/// Execute a custom WHERE clause against files+metadata.
/// Returns matching file paths. The WHERE clause is appended to a base query
/// that joins files, directories, scans, and metadata.
///
/// WARNING: The caller must sanitize the WHERE clause. This function does NOT
/// parameterize the clause — it is appended directly to SQL.
pub async fn query_files_where(
    pool: &SqlitePool,
    where_clause: &str,
) -> Result<Vec<String>, sqlx::Error> {
    let query = format!(
        "SELECT DISTINCT {FULL_PATH_SQL}
         FROM files f
         JOIN directories d ON f.dir_id = d.id
         JOIN scans s ON d.scan_id = s.id
         LEFT JOIN metadata m ON m.file_id = f.id
         WHERE {where_clause}
         ORDER BY d.path, f.filename"
    );
    let rows: Vec<(String,)> = sqlx::query_as(&query).fetch_all(pool).await?;
    Ok(rows.into_iter().map(|(p,)| p).collect())
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

        // Replace with different files — removed files are returned, not deleted
        let new_files = vec![FileInput {
            filename: "c.txt".into(),
            size: 300,
            ctime: 5000,
            mtime: 6000,
        }];
        let sync = replace_files(&pool, dir_id, &new_files).await.unwrap();
        assert_eq!(sync.removed_files.len(), 2); // a.txt and b.txt removed

        // Files still exist until we explicitly delete unmatched ones
        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM files WHERE dir_id = ?")
            .bind(dir_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count.0, 3); // c.txt + a.txt + b.txt still in DB

        // Delete unmatched
        {
            let mut conn = pool.acquire().await.unwrap();
            delete_unmatched_files(&mut conn, &sync.removed_files)
                .await
                .unwrap();
        }

        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM files WHERE dir_id = ?")
            .bind(dir_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count.0, 1); // only c.txt remains
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

    // --- Metadata DAO tests ---

    #[tokio::test]
    async fn replace_file_metadata_roundtrips() {
        let pool = open_memory().await.unwrap();
        let scan_id = upsert_scan(&pool, "test", "/tmp").await.unwrap();
        let dir_id = upsert_directory(&pool, scan_id, "", 1000, 0).await.unwrap();
        replace_files(
            &pool,
            dir_id,
            &[FileInput {
                filename: "song.mp3".into(),
                size: 1000,
                ctime: 100,
                mtime: 200,
            }],
        )
        .await
        .unwrap();

        let file_id = get_file_id_by_path(&pool, scan_id, "", "song.mp3")
            .await
            .unwrap()
            .unwrap();

        let tags = vec![
            ("track_title".into(), "\"Test Song\"".into()),
            ("track_artist".into(), "\"Test Artist\"".into()),
        ];
        replace_file_metadata(&pool, file_id, &tags).await.unwrap();

        let result = get_file_metadata(&pool, file_id).await.unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].tag_name, "track_artist");
        assert_eq!(result[1].tag_name, "track_title");
    }

    #[tokio::test]
    async fn replace_file_metadata_overwrites() {
        let pool = open_memory().await.unwrap();
        let scan_id = upsert_scan(&pool, "test", "/tmp").await.unwrap();
        let dir_id = upsert_directory(&pool, scan_id, "", 1000, 0).await.unwrap();
        replace_files(
            &pool,
            dir_id,
            &[FileInput {
                filename: "song.flac".into(),
                size: 5000,
                ctime: 100,
                mtime: 200,
            }],
        )
        .await
        .unwrap();
        let file_id = get_file_id_by_path(&pool, scan_id, "", "song.flac")
            .await
            .unwrap()
            .unwrap();

        // First set
        replace_file_metadata(&pool, file_id, &[("genre".into(), "\"Rock\"".into())])
            .await
            .unwrap();

        // Overwrite
        replace_file_metadata(&pool, file_id, &[("genre".into(), "\"Jazz\"".into())])
            .await
            .unwrap();

        let result = get_file_metadata(&pool, file_id).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].value, "\"Jazz\"");
    }

    #[tokio::test]
    async fn upsert_blob_increments_ref_count() {
        let pool = open_memory().await.unwrap();
        upsert_blob(&pool, "abc123", 100).await.unwrap();
        upsert_blob(&pool, "abc123", 100).await.unwrap();

        let row: (i64,) = sqlx::query_as("SELECT ref_count FROM blobs WHERE hash = 'abc123'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(row.0, 2);
    }

    #[tokio::test]
    async fn mark_metadata_scanned_sets_timestamp() {
        let pool = open_memory().await.unwrap();
        let scan_id = upsert_scan(&pool, "test", "/tmp").await.unwrap();
        let dir_id = upsert_directory(&pool, scan_id, "", 1000, 0).await.unwrap();
        replace_files(
            &pool,
            dir_id,
            &[FileInput {
                filename: "x.mp3".into(),
                size: 100,
                ctime: 100,
                mtime: 200,
            }],
        )
        .await
        .unwrap();
        let file_id = get_file_id_by_path(&pool, scan_id, "", "x.mp3")
            .await
            .unwrap()
            .unwrap();

        // Initially null
        let row: (Option<String>,) =
            sqlx::query_as("SELECT metadata_scanned_at FROM files WHERE id = ?")
                .bind(file_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(row.0.is_none());

        mark_metadata_scanned(&pool, file_id, Some("abc123hash"))
            .await
            .unwrap();

        let row: (Option<String>,) =
            sqlx::query_as("SELECT metadata_scanned_at FROM files WHERE id = ?")
                .bind(file_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(row.0.is_some());

        // Verify content_hash was stored
        let hash_row: (Option<String>,) =
            sqlx::query_as("SELECT content_hash FROM files WHERE id = ?")
                .bind(file_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(hash_row.0.as_deref(), Some("abc123hash"));
    }

    #[tokio::test]
    async fn delete_file_dependents_cleans_all_tables() {
        let pool = open_memory().await.unwrap();
        let scan_id = upsert_scan(&pool, "test", "/tmp").await.unwrap();
        let dir_id = upsert_directory(&pool, scan_id, "", 1000, 0).await.unwrap();
        replace_files(
            &pool,
            dir_id,
            &[FileInput {
                filename: "a.flac".into(),
                size: 1000,
                ctime: 100,
                mtime: 200,
            }],
        )
        .await
        .unwrap();
        let file_id = get_file_id_by_path(&pool, scan_id, "", "a.flac")
            .await
            .unwrap()
            .unwrap();

        // Add metadata, image, cue sheet
        replace_file_metadata(&pool, file_id, &[("genre".into(), "\"Pop\"".into())])
            .await
            .unwrap();
        upsert_blob(&pool, "imgblob1", 500).await.unwrap();
        sqlx::query(
            "INSERT INTO embedded_images (file_id, image_type, mime_type, blob_hash)
             VALUES (?, 'front_cover', 'image/jpeg', 'imgblob1')",
        )
        .bind(file_id)
        .execute(&pool)
        .await
        .unwrap();
        upsert_cue_sheet(&pool, file_id, "embedded", "FILE data")
            .await
            .unwrap();

        // Delete dependents
        {
            let mut conn = pool.acquire().await.unwrap();
            delete_file_dependents(&mut *conn, file_id).await.unwrap();
        }

        // Verify all cleaned up
        let meta_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM metadata WHERE file_id = ?")
            .bind(file_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(meta_count.0, 0);

        let img_count: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM embedded_images WHERE file_id = ?")
                .bind(file_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(img_count.0, 0);

        let cue_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM cue_sheets WHERE file_id = ?")
            .bind(file_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(cue_count.0, 0);

        // File itself should still exist (dependents only)
        let file_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM files WHERE id = ?")
            .bind(file_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(file_count.0, 1);
    }

    #[tokio::test]
    async fn files_needing_metadata_scan_filters_by_extension() {
        let pool = open_memory().await.unwrap();
        let scan_id = upsert_scan(&pool, "test", "/tmp").await.unwrap();
        let dir_id = upsert_directory(&pool, scan_id, "", 1000, 0).await.unwrap();
        replace_files(
            &pool,
            dir_id,
            &[
                FileInput {
                    filename: "song.mp3".into(),
                    size: 100,
                    ctime: 100,
                    mtime: 200,
                },
                FileInput {
                    filename: "video.mkv".into(),
                    size: 500,
                    ctime: 100,
                    mtime: 200,
                },
                FileInput {
                    filename: "track.flac".into(),
                    size: 300,
                    ctime: 100,
                    mtime: 200,
                },
            ],
        )
        .await
        .unwrap();

        let results = files_needing_metadata_scan(&pool, scan_id, &["mp3", "flac"], false)
            .await
            .unwrap();
        let names: Vec<&str> = results.iter().map(|r| r.filename.as_str()).collect();
        assert_eq!(names.len(), 2);
        assert!(names.contains(&"song.mp3"));
        assert!(names.contains(&"track.flac"));
        assert!(!names.contains(&"video.mkv"));
    }

    #[tokio::test]
    async fn files_needing_metadata_scan_skips_scanned() {
        let pool = open_memory().await.unwrap();
        let scan_id = upsert_scan(&pool, "test", "/tmp").await.unwrap();
        let dir_id = upsert_directory(&pool, scan_id, "", 1000, 0).await.unwrap();
        replace_files(
            &pool,
            dir_id,
            &[FileInput {
                filename: "done.mp3".into(),
                size: 100,
                ctime: 100,
                mtime: 200,
            }],
        )
        .await
        .unwrap();
        let file_id = get_file_id_by_path(&pool, scan_id, "", "done.mp3")
            .await
            .unwrap()
            .unwrap();

        // Mark as scanned
        mark_metadata_scanned(&pool, file_id, None).await.unwrap();

        let results = files_needing_metadata_scan(&pool, scan_id, &["mp3"], false)
            .await
            .unwrap();
        assert!(results.is_empty(), "scanned file should be skipped");
    }
}
