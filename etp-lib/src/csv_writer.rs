use crate::state::ScanState;
use icu_collator::CollatorBorrowed;
use icu_collator::options::{AlternateHandling, CollatorOptions, Strength};
use std::io;
use std::path::Path;

pub fn write_csv(state: &ScanState, output: &Path) -> io::Result<()> {
    let file = std::fs::File::create(output)?;
    let mut wtr = csv::Writer::from_writer(file);

    let mut options = CollatorOptions::default();
    options.strength = Some(Strength::Quaternary);
    options.alternate_handling = Some(AlternateHandling::Shifted);
    let collator = CollatorBorrowed::try_new(Default::default(), options).unwrap();

    wtr.write_record(["path", "size", "ctime", "mtime"])
        .map_err(io::Error::other)?;

    // Sort everything collated for very stable output
    let mut dirs: Vec<_> = state.dirs.keys().collect();
    dirs.sort_by(|a, b| collator.compare(a, b));

    for dir in dirs {
        let entry = &state.dirs[dir];
        let mut files = entry.files.clone();
        files.sort_by(|a, b| collator.compare(a.filename.as_str(), b.filename.as_str()));
        for file in files {
            let path = Path::new(dir).join(&file.filename);
            wtr.write_record([
                path.to_string_lossy().as_ref(),
                &file.size.to_string(),
                &file.ctime.to_string(),
                &file.mtime.to_string(),
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
    use crate::state::{DirEntry, FileEntry};

    fn read_csv(path: &Path) -> String {
        std::fs::read_to_string(path).unwrap()
    }

    #[test]
    fn empty_state_produces_header_only() {
        let tmp = tempfile::tempdir().unwrap();
        let csv_path = tmp.path().join("out.csv");

        let state = ScanState::default();
        write_csv(&state, &csv_path).unwrap();

        let content = read_csv(&csv_path);
        assert_eq!(content, "path,size,ctime,mtime\n");
    }

    #[test]
    fn state_with_entries_produces_correct_csv() {
        let tmp = tempfile::tempdir().unwrap();
        let csv_path = tmp.path().join("out.csv");

        let mut state = ScanState::default();
        state.dirs.insert(
            "/data".into(),
            DirEntry {
                dir_mtime: 100,
                files: vec![FileEntry {
                    filename: "file.txt".into(),
                    size: 42,
                    ctime: 1000,
                    mtime: 2000,
                }],
            },
        );
        write_csv(&state, &csv_path).unwrap();

        let content = read_csv(&csv_path);
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0], "path,size,ctime,mtime");
        assert_eq!(lines[1], "/data/file.txt,42,1000,2000");
    }

    #[test]
    fn directories_sorted_lexicographically() {
        let tmp = tempfile::tempdir().unwrap();
        let csv_path = tmp.path().join("out.csv");

        let mut state = ScanState::default();
        // Insert in reverse order
        for name in &["/z_dir", "/a_dir", "/m_dir"] {
            state.dirs.insert(
                (*name).into(),
                DirEntry {
                    dir_mtime: 100,
                    files: vec![FileEntry {
                        filename: "f.txt".into(),
                        size: 1,
                        ctime: 0,
                        mtime: 0,
                    }],
                },
            );
        }
        write_csv(&state, &csv_path).unwrap();

        let content = read_csv(&csv_path);
        let lines: Vec<&str> = content.lines().collect();
        // Header + 3 entries
        assert_eq!(lines.len(), 4);
        assert!(lines[1].starts_with("/a_dir/"));
        assert!(lines[2].starts_with("/m_dir/"));
        assert!(lines[3].starts_with("/z_dir/"));
    }

    #[test]
    fn files_appear_in_collated_order() {
        let tmp = tempfile::tempdir().unwrap();
        let csv_path = tmp.path().join("out.csv");

        let mut state = ScanState::default();
        state.dirs.insert(
            "/dir".into(),
            DirEntry {
                dir_mtime: 100,
                files: vec![
                    FileEntry {
                        filename: "second.txt".into(),
                        size: 1,
                        ctime: 0,
                        mtime: 0,
                    },
                    FileEntry {
                        filename: "first.txt".into(),
                        size: 2,
                        ctime: 0,
                        mtime: 0,
                    },
                    FileEntry {
                        filename: "third.txt".into(),
                        size: 3,
                        ctime: 0,
                        mtime: 0,
                    },
                ],
            },
        );
        write_csv(&state, &csv_path).unwrap();

        let content = read_csv(&csv_path);
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 4);
        assert!(lines[1].contains("first.txt"));
        assert!(lines[2].contains("second.txt"));
        assert!(lines[3].contains("third.txt"));
    }
}
