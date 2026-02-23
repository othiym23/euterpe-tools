use crate::db::dao::{self, FileInput};
use sqlx::SqlitePool;
use std::collections::HashSet;
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

pub async fn scan_to_db(
    root: &Path,
    pool: &SqlitePool,
    run_type: &str,
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

    let walker = WalkDir::new(root).sort_by_file_name();

    for entry in walker {
        let entry = entry.map_err(io::Error::other)?;
        if !entry.file_type().is_dir() {
            continue;
        }

        let dir_path = entry.path().to_path_buf();
        let relative = dir_path
            .strip_prefix(root)
            .unwrap()
            .to_string_lossy()
            .into_owned();
        seen_paths.insert(relative.clone());

        let dir_meta = fs::metadata(&dir_path)?;
        let dir_mtime = dir_meta.mtime();
        let dir_size = dir_meta.size();

        let cached_mtime = dao::directory_mtime(pool, scan_id, &relative)
            .await
            .map_err(io::Error::other)?;

        if cached_mtime == Some(dir_mtime) {
            stats.dirs_cached += 1;
            if verbose {
                eprintln!("directory unchanged, skipping: {}", dir_path.display());
            }
            continue;
        }

        stats.dirs_scanned += 1;

        let files = scan_directory(&dir_path)?;
        if verbose {
            eprintln!("scanning: {} ({} files)", dir_path.display(), files.len());
        }

        let dir_id = dao::upsert_directory(pool, scan_id, &relative, dir_mtime, dir_size)
            .await
            .map_err(io::Error::other)?;

        dao::replace_files(pool, dir_id, &files)
            .await
            .map_err(io::Error::other)?;
    }

    let removed = dao::remove_stale_directories(pool, scan_id, &seen_paths)
        .await
        .map_err(io::Error::other)?;
    stats.dirs_removed = removed;

    dao::finish_scan(pool, scan_id)
        .await
        .map_err(io::Error::other)?;

    stats.elapsed_ms = start.elapsed().as_millis();

    Ok((scan_id, stats))
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
