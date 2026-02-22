use sqlx::SqlitePool;
use std::collections::HashSet;

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

/// Replace all files in a directory — deletes existing files for the dir_id,
/// then inserts the new set.
pub async fn replace_files(
    pool: &SqlitePool,
    dir_id: i64,
    files: &[FileInput],
) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM files WHERE dir_id = ?")
        .bind(dir_id)
        .execute(pool)
        .await?;

    for f in files {
        sqlx::query(
            "INSERT INTO files (dir_id, filename, size, ctime, mtime) VALUES (?, ?, ?, ?, ?)",
        )
        .bind(dir_id)
        .bind(&f.filename)
        .bind(f.size as i64)
        .bind(f.ctime)
        .bind(f.mtime)
        .execute(pool)
        .await?;
    }
    Ok(())
}

/// Remove directories that are no longer present on disk. Returns the count removed.
pub async fn remove_stale_directories(
    pool: &SqlitePool,
    scan_id: i64,
    seen_paths: &HashSet<String>,
) -> Result<usize, sqlx::Error> {
    let all_dirs: Vec<(i64, String)> =
        sqlx::query_as("SELECT id, path FROM directories WHERE scan_id = ?")
            .bind(scan_id)
            .fetch_all(pool)
            .await?;

    let mut removed = 0;
    for (dir_id, path) in &all_dirs {
        if !seen_paths.contains(path) {
            sqlx::query("DELETE FROM files WHERE dir_id = ?")
                .bind(dir_id)
                .execute(pool)
                .await?;
            sqlx::query("DELETE FROM directories WHERE id = ?")
                .bind(dir_id)
                .execute(pool)
                .await?;
            removed += 1;
        }
    }
    Ok(removed)
}

/// List all files for a scan, with full paths reconstructed by joining
/// `scans.root_path` and `directories.path`.
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

fn chrono_now() -> String {
    // Simple ISO 8601 timestamp without external dependency.
    // For the purposes of this application, second precision is sufficient.
    use std::time::SystemTime;
    let duration = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();
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

        let removed = remove_stale_directories(&pool, scan_id, &seen)
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
