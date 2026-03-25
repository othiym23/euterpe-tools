use crate::db::dao::FileRecord;
use regex::Regex;

/// A file that matched a regex pattern, with its full path pre-computed.
#[derive(Debug, Clone)]
pub struct FindMatch {
    pub full_path: String,
    pub size: u64,
    pub ctime: i64,
    pub mtime: i64,
}

/// Test whether a file record matches the pattern, returning a `FindMatch` if so.
pub fn matches_pattern(record: &FileRecord, pattern: &Regex) -> Option<FindMatch> {
    let full_path = format!("{}/{}", record.dir_path, record.filename);
    if pattern.is_match(&full_path) {
        Some(FindMatch {
            full_path,
            size: record.size,
            ctime: record.ctime,
            mtime: record.mtime,
        })
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_record(dir_path: &str, filename: &str, size: u64) -> FileRecord {
        FileRecord {
            dir_path: dir_path.to_string(),
            filename: filename.to_string(),
            size,
            ctime: 1000,
            mtime: 2000,
        }
    }

    #[test]
    fn matches_filename() {
        let re = Regex::new(r"\.txt$").unwrap();
        let record = make_record("/data/sub", "notes.txt", 100);
        let m = matches_pattern(&record, &re).unwrap();
        assert_eq!(m.full_path, "/data/sub/notes.txt");
        assert_eq!(m.size, 100);
    }

    #[test]
    fn no_match() {
        let re = Regex::new(r"\.txt$").unwrap();
        let record = make_record("/data/sub", "image.png", 200);
        assert!(matches_pattern(&record, &re).is_none());
    }

    #[test]
    fn matches_root_level_file() {
        let re = Regex::new(r"readme").unwrap();
        let record = make_record("/volume1/music", "readme.md", 50);
        let m = matches_pattern(&record, &re).unwrap();
        assert_eq!(m.full_path, "/volume1/music/readme.md");
    }

    #[test]
    fn matches_unicode_filename() {
        let re = Regex::new(r"カタカナ").unwrap();
        let record = make_record("/data", "カタカナ.flac", 1000);
        let m = matches_pattern(&record, &re).unwrap();
        assert_eq!(m.full_path, "/data/カタカナ.flac");
    }

    #[test]
    fn matches_path_component() {
        let re = Regex::new(r"/sub/").unwrap();
        let record = make_record("/data/sub", "file.txt", 100);
        let m = matches_pattern(&record, &re).unwrap();
        assert_eq!(m.full_path, "/data/sub/file.txt");
    }
}
