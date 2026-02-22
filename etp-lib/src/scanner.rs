use crate::state::{DirEntry, FileEntry, ScanState};
use std::fs;
use std::io;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use walkdir::WalkDir;

pub struct ScanStats {
    pub dirs_cached: usize,
    pub dirs_scanned: usize,
    pub dirs_removed: usize,
}

pub fn scan(
    root: &Path,
    state: &mut ScanState,
    exclude: &[String],
    verbose: bool,
) -> io::Result<ScanStats> {
    let mut stats = ScanStats {
        dirs_cached: 0,
        dirs_scanned: 0,
        dirs_removed: 0,
    };

    let mut seen_dirs = std::collections::HashSet::new();

    let walker = WalkDir::new(root)
        .sort_by_file_name()
        .into_iter()
        .filter_entry(|e| {
            if e.file_type().is_dir()
                && let Some(name) = e.path().file_name()
            {
                return !exclude
                    .iter()
                    .any(|ex| ex == name.to_string_lossy().as_ref());
            }
            true
        });

    for entry in walker {
        let entry = entry.map_err(io::Error::other)?;
        if !entry.file_type().is_dir() {
            continue;
        }

        let dir_path = entry.path().to_path_buf();
        let dir_key = dir_path.to_string_lossy().into_owned();
        seen_dirs.insert(dir_key.clone());

        let dir_meta = fs::metadata(&dir_path)?;
        let dir_mtime = dir_meta.mtime();

        if let Some(cached) = state.dirs.get(&dir_key)
            && cached.dir_mtime == dir_mtime
        {
            stats.dirs_cached += 1;
            if verbose {
                eprintln!("cache hit: {}", dir_path.display());
            }
            continue;
        }

        stats.dirs_scanned += 1;
        if verbose {
            eprintln!("scanning: {}", dir_path.display());
        }

        let files = scan_directory(&dir_path)?;
        state.dirs.insert(dir_key, DirEntry { dir_mtime, files });
    }

    // Remove directories that no longer exist
    let to_remove: Vec<_> = state
        .dirs
        .keys()
        .filter(|k| !seen_dirs.contains(k.as_str()))
        .cloned()
        .collect();
    stats.dirs_removed = to_remove.len();
    for k in &to_remove {
        if verbose {
            eprintln!("removed: {k}");
        }
        state.dirs.remove(k);
    }

    Ok(stats)
}

fn scan_directory(dir: &Path) -> io::Result<Vec<FileEntry>> {
    let mut files = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let ft = entry.file_type()?;
        if !ft.is_file() {
            continue;
        }
        let meta = entry.metadata()?;
        files.push(FileEntry {
            filename: entry.file_name().to_string_lossy().into_owned(),
            size: meta.size(),
            ctime: meta.ctime(),
            mtime: meta.mtime(),
        });
    }
    files.sort_by(|a, b| a.filename.cmp(&b.filename));
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_tree(dir: &std::path::Path) {
        // dir/
        //   a.txt
        //   sub/
        //     b.txt
        //     deeper/
        //       c.txt
        fs::write(dir.join("a.txt"), "hello").unwrap();
        fs::create_dir_all(dir.join("sub/deeper")).unwrap();
        fs::write(dir.join("sub/b.txt"), "world").unwrap();
        fs::write(dir.join("sub/deeper/c.txt"), "deep").unwrap();
    }

    /// Helper to build a state key from a path (matching what scan() does).
    fn key(path: &Path) -> String {
        path.to_string_lossy().into_owned()
    }

    #[test]
    fn fresh_scan_all_dirs_scanned() {
        let tmp = tempfile::tempdir().unwrap();
        make_tree(tmp.path());

        let mut state = ScanState::default();
        let stats = scan(tmp.path(), &mut state, &[], false).unwrap();

        // root, sub, sub/deeper = 3 dirs scanned
        assert_eq!(stats.dirs_scanned, 3);
        assert_eq!(stats.dirs_cached, 0);
        assert_eq!(stats.dirs_removed, 0);
        assert_eq!(state.dirs.len(), 3);
    }

    #[test]
    fn second_scan_no_changes_all_cached() {
        let tmp = tempfile::tempdir().unwrap();
        make_tree(tmp.path());

        let mut state = ScanState::default();
        scan(tmp.path(), &mut state, &[], false).unwrap();

        let stats = scan(tmp.path(), &mut state, &[], false).unwrap();
        assert_eq!(stats.dirs_scanned, 0);
        assert_eq!(stats.dirs_cached, 3);
        assert_eq!(stats.dirs_removed, 0);
    }

    #[test]
    fn modified_dir_is_rescanned() {
        let tmp = tempfile::tempdir().unwrap();
        make_tree(tmp.path());

        let mut state = ScanState::default();
        scan(tmp.path(), &mut state, &[], false).unwrap();

        // Simulate sub/ having changed by setting its cached mtime to a stale value
        let sub_key = key(&tmp.path().join("sub"));
        state.dirs.get_mut(&sub_key).unwrap().dir_mtime -= 1;

        let stats = scan(tmp.path(), &mut state, &[], false).unwrap();
        // Only sub/ should be rescanned (its cached mtime doesn't match)
        assert_eq!(stats.dirs_scanned, 1);
        assert_eq!(stats.dirs_cached, 2);
        assert_eq!(stats.dirs_removed, 0);
    }

    #[test]
    fn remove_subdir_shows_removed() {
        let tmp = tempfile::tempdir().unwrap();
        make_tree(tmp.path());

        let mut state = ScanState::default();
        scan(tmp.path(), &mut state, &[], false).unwrap();
        assert!(
            state
                .dirs
                .contains_key(&key(&tmp.path().join("sub/deeper")))
        );

        // Remove sub/deeper/
        fs::remove_dir_all(tmp.path().join("sub/deeper")).unwrap();

        let stats = scan(tmp.path(), &mut state, &[], false).unwrap();
        assert_eq!(stats.dirs_removed, 1);
        assert!(
            !state
                .dirs
                .contains_key(&key(&tmp.path().join("sub/deeper")))
        );
    }

    #[test]
    fn exclude_list_skips_directories() {
        let tmp = tempfile::tempdir().unwrap();
        make_tree(tmp.path());
        // Add an excluded dir
        fs::create_dir_all(tmp.path().join("@eaDir")).unwrap();
        fs::write(tmp.path().join("@eaDir/junk.txt"), "junk").unwrap();

        let mut state = ScanState::default();
        let exclude = vec!["@eaDir".to_string()];
        let stats = scan(tmp.path(), &mut state, &exclude, false).unwrap();

        assert!(!state.dirs.contains_key(&key(&tmp.path().join("@eaDir"))));
        // 3 dirs: root, sub, sub/deeper (not @eaDir)
        assert_eq!(stats.dirs_scanned, 3);
    }

    #[test]
    fn files_sorted_by_filename() {
        let tmp = tempfile::tempdir().unwrap();
        // Create files in reverse alphabetical order
        fs::write(tmp.path().join("z.txt"), "z").unwrap();
        fs::write(tmp.path().join("m.txt"), "m").unwrap();
        fs::write(tmp.path().join("a.txt"), "a").unwrap();

        let mut state = ScanState::default();
        scan(tmp.path(), &mut state, &[], false).unwrap();

        let entry = &state.dirs[&key(tmp.path())];
        let names: Vec<&str> = entry.files.iter().map(|f| f.filename.as_str()).collect();
        assert_eq!(names, vec!["a.txt", "m.txt", "z.txt"]);
    }

    #[test]
    fn empty_directory_produces_empty_files() {
        let tmp = tempfile::tempdir().unwrap();

        let mut state = ScanState::default();
        scan(tmp.path(), &mut state, &[], false).unwrap();

        let entry = &state.dirs[&key(tmp.path())];
        assert!(entry.files.is_empty());
    }
}
