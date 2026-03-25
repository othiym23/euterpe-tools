use crate::db::dao::{self, FileRecord};
use crate::ops;
use icu_collator::CollatorBorrowed;
use icu_collator::options::{AlternateHandling, CollatorOptions, Strength};
use sqlx::SqlitePool;
use std::collections::HashMap;
use std::io;
use std::path::Path;

#[cfg_attr(
    feature = "profiling",
    tracing::instrument(name = "csv_write_csv_from_db", skip_all)
)]
pub async fn write_csv_from_db(
    pool: &SqlitePool,
    scan_id: i64,
    output: &Path,
    exclude: &[String],
    verbose: bool,
) -> io::Result<()> {
    let all_files = dao::list_files(pool, scan_id)
        .await
        .map_err(io::Error::other)?;

    // Filter out files whose dir_path contains any excluded directory name
    let total_before = all_files.len();
    let mut excluded_dirs: std::collections::HashSet<String> = std::collections::HashSet::new();
    let files: Vec<FileRecord> = if exclude.is_empty() {
        all_files
    } else {
        all_files
            .into_iter()
            .filter(|f| {
                let dominated = ops::is_excluded_path(&f.dir_path, exclude);
                if dominated && verbose {
                    excluded_dirs.insert(f.dir_path.clone());
                }
                !dominated
            })
            .collect()
    };
    if verbose && !excluded_dirs.is_empty() {
        let excluded_count = total_before - files.len();
        let mut dirs: Vec<&str> = excluded_dirs.iter().map(|s| s.as_str()).collect();
        dirs.sort();
        for dir in &dirs {
            eprintln!("excluding directory from CSV output: {}", dir);
        }
        eprintln!(
            "excluded {} files from {} directories",
            excluded_count,
            dirs.len()
        );
    }

    // Group by dir_path
    let mut by_dir: HashMap<String, Vec<&FileRecord>> = HashMap::new();
    for f in &files {
        by_dir.entry(f.dir_path.clone()).or_default().push(f);
    }

    let mut options = CollatorOptions::default();
    options.strength = Some(Strength::Quaternary);
    options.alternate_handling = Some(AlternateHandling::Shifted);
    let collator = CollatorBorrowed::try_new(Default::default(), options)
        .map_err(|e| io::Error::other(format!("collator initialization failed: {e}")))?;

    let mut dirs: Vec<String> = by_dir.keys().cloned().collect();
    dirs.sort_by(|a, b| collator.compare(a, b));

    let file = std::fs::File::create(output)?;
    let mut wtr = csv::Writer::from_writer(file);
    wtr.write_record(["path", "size", "ctime", "mtime"])
        .map_err(io::Error::other)?;

    for dir in &dirs {
        let Some(dir_files) = by_dir.get_mut(dir.as_str()) else {
            continue;
        };
        dir_files.sort_by(|a, b| collator.compare(&a.filename, &b.filename));
        for f in dir_files {
            let path = Path::new(dir).join(&f.filename);
            wtr.write_record([
                path.to_string_lossy().as_ref(),
                &f.size.to_string(),
                &f.ctime.to_string(),
                &f.mtime.to_string(),
            ])
            .map_err(io::Error::other)?;
        }
    }

    wtr.flush().map_err(io::Error::other)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;
    use crate::db::dao::FileInput;

    fn read_csv(path: &Path) -> String {
        std::fs::read_to_string(path).unwrap()
    }

    #[tokio::test]
    async fn empty_scan_produces_header_only() {
        let pool = db::open_memory().await.unwrap();
        let scan_id = dao::upsert_scan(&pool, "test", "/tmp").await.unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let csv_path = tmp.path().join("out.csv");
        write_csv_from_db(&pool, scan_id, &csv_path, &[], false)
            .await
            .unwrap();

        assert_eq!(read_csv(&csv_path), "path,size,ctime,mtime\n");
    }

    #[tokio::test]
    async fn scan_with_entries_produces_correct_csv() {
        let pool = db::open_memory().await.unwrap();
        let scan_id = dao::upsert_scan(&pool, "test", "/data").await.unwrap();
        let dir_id = dao::upsert_directory(&pool, scan_id, "", 100, 4096)
            .await
            .unwrap();
        dao::replace_files(
            &pool,
            dir_id,
            &[FileInput {
                filename: "file.txt".into(),
                size: 42,
                ctime: 1000,
                mtime: 2000,
            }],
        )
        .await
        .unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let csv_path = tmp.path().join("out.csv");
        write_csv_from_db(&pool, scan_id, &csv_path, &[], false)
            .await
            .unwrap();

        let content = read_csv(&csv_path);
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0], "path,size,ctime,mtime");
        assert_eq!(lines[1], "/data/file.txt,42,1000,2000");
    }

    #[tokio::test]
    async fn directories_sorted_by_collation() {
        let pool = db::open_memory().await.unwrap();
        let scan_id = dao::upsert_scan(&pool, "test", "/root").await.unwrap();

        for name in &["z_dir", "a_dir", "m_dir"] {
            let dir_id = dao::upsert_directory(&pool, scan_id, name, 100, 4096)
                .await
                .unwrap();
            dao::replace_files(
                &pool,
                dir_id,
                &[FileInput {
                    filename: "f.txt".into(),
                    size: 1,
                    ctime: 0,
                    mtime: 0,
                }],
            )
            .await
            .unwrap();
        }

        let tmp = tempfile::tempdir().unwrap();
        let csv_path = tmp.path().join("out.csv");
        write_csv_from_db(&pool, scan_id, &csv_path, &[], false)
            .await
            .unwrap();

        let content = read_csv(&csv_path);
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 4);
        assert!(lines[1].starts_with("/root/a_dir/"));
        assert!(lines[2].starts_with("/root/m_dir/"));
        assert!(lines[3].starts_with("/root/z_dir/"));
    }

    #[tokio::test]
    async fn files_sorted_by_collation() {
        let pool = db::open_memory().await.unwrap();
        let scan_id = dao::upsert_scan(&pool, "test", "/dir").await.unwrap();
        let dir_id = dao::upsert_directory(&pool, scan_id, "", 100, 4096)
            .await
            .unwrap();
        dao::replace_files(
            &pool,
            dir_id,
            &[
                FileInput {
                    filename: "second.txt".into(),
                    size: 1,
                    ctime: 0,
                    mtime: 0,
                },
                FileInput {
                    filename: "first.txt".into(),
                    size: 2,
                    ctime: 0,
                    mtime: 0,
                },
                FileInput {
                    filename: "third.txt".into(),
                    size: 3,
                    ctime: 0,
                    mtime: 0,
                },
            ],
        )
        .await
        .unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let csv_path = tmp.path().join("out.csv");
        write_csv_from_db(&pool, scan_id, &csv_path, &[], false)
            .await
            .unwrap();

        let content = read_csv(&csv_path);
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 4);
        assert!(lines[1].contains("first.txt"));
        assert!(lines[2].contains("second.txt"));
        assert!(lines[3].contains("third.txt"));
    }
}
