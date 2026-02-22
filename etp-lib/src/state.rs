use rkyv::{Archive, Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

/// Magic bytes identifying an fsscan state file.
const MAGIC: &[u8; 4] = b"FSSN";
/// Current state file format version (rkyv 0.8 + brotli compression).
const VERSION: u8 = 2;
/// Size of the header: 4 bytes magic + 1 byte version.
const HEADER_SIZE: usize = 5;

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct FileEntry {
    pub filename: String,
    pub size: u64,
    pub ctime: i64,
    pub mtime: i64,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct DirEntry {
    pub dir_mtime: i64,
    pub files: Vec<FileEntry>,
}

#[derive(Debug, Default, Archive, Serialize, Deserialize)]
pub struct ScanState {
    pub dirs: HashMap<String, DirEntry>,
}

/// Result of attempting to load a state file.
#[derive(Debug)]
pub enum LoadOutcome {
    /// Successfully loaded state.
    Loaded(ScanState),
    /// File doesn't exist — fresh start.
    NotFound,
    /// File exists but is corrupt, wrong version, or unreadable format.
    Invalid(String),
}

impl ScanState {
    pub fn load(path: &Path) -> LoadOutcome {
        let data = match fs::read(path) {
            Ok(d) => d,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return LoadOutcome::NotFound,
            Err(e) => return LoadOutcome::Invalid(format!("read error: {e}")),
        };

        if data.len() < HEADER_SIZE {
            return LoadOutcome::Invalid("truncated state file".into());
        }

        if &data[..4] != MAGIC {
            return LoadOutcome::Invalid("not a state file (wrong magic)".into());
        }

        let version = data[4];
        let payload = &data[HEADER_SIZE..];

        // Decompress if version 2 (brotli), pass through if version 1 (raw rkyv).
        let rkyv_bytes: Vec<u8> = match version {
            1 => payload.to_vec(),
            2 => {
                let mut decompressed = Vec::new();
                if let Err(e) = brotli::BrotliDecompress(&mut &payload[..], &mut decompressed) {
                    return LoadOutcome::Invalid(format!("decompression error: {e}"));
                }
                decompressed
            }
            _ => return LoadOutcome::Invalid(format!("unsupported version {version}")),
        };

        // Copy into an aligned buffer — rkyv requires alignment and our header
        // (and compression) shift the payload off the allocation's alignment boundary.
        let mut aligned = rkyv::util::AlignedVec::<16>::new();
        aligned.extend_from_slice(&rkyv_bytes);

        match rkyv::from_bytes::<ScanState, rkyv::rancor::Error>(&aligned) {
            Ok(state) => LoadOutcome::Loaded(state),
            Err(e) => LoadOutcome::Invalid(format!("corrupt data: {e}")),
        }
    }

    pub fn save(&self, path: &Path) -> io::Result<()> {
        let rkyv_data = rkyv::to_bytes::<rkyv::rancor::Error>(self).map_err(io::Error::other)?;

        // Brotli compress: quality 5 for fast compression, lgwin 22 (4 MB window).
        let mut compressed = Vec::new();
        {
            let mut encoder = brotli::CompressorWriter::new(&mut compressed, 4096, 5, 22);
            encoder.write_all(&rkyv_data)?;
        }

        let mut data = Vec::with_capacity(HEADER_SIZE + compressed.len());
        data.extend_from_slice(MAGIC);
        data.push(VERSION);
        data.extend_from_slice(&compressed);

        // Write to a hidden temp file then rename for atomicity — a crash
        // mid-write leaves the old state file intact rather than corrupted.
        let tmp_path = tmp_path_for(path);
        fs::write(&tmp_path, data)?;
        fs::rename(&tmp_path, path).inspect_err(|_| {
            let _ = fs::remove_file(&tmp_path);
        })
    }
}

/// Build a hidden sibling path for atomic writes: `/dir/.fsscan.state` → `/dir/.fsscan.state.tmp`,
/// `/dir/my.state` → `/dir/.my.state.tmp`.
fn tmp_path_for(path: &Path) -> PathBuf {
    let file_name = path.file_name().unwrap_or_default().to_string_lossy();
    let hidden_name = if file_name.starts_with('.') {
        format!("{file_name}.tmp")
    } else {
        format!(".{file_name}.tmp")
    };
    match path.parent() {
        Some(dir) => dir.join(hidden_name),
        None => PathBuf::from(hidden_name),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_populated_state() {
        let dir = tempfile::tempdir().unwrap();
        let state_path = dir.path().join("test.state");

        let mut state = ScanState::default();
        state.dirs.insert(
            "/some/dir".into(),
            DirEntry {
                dir_mtime: 1234567890,
                files: vec![
                    FileEntry {
                        filename: "a.txt".into(),
                        size: 100,
                        ctime: 1000,
                        mtime: 2000,
                    },
                    FileEntry {
                        filename: "b.txt".into(),
                        size: 200,
                        ctime: 3000,
                        mtime: 4000,
                    },
                ],
            },
        );

        state.save(&state_path).unwrap();
        let loaded = match ScanState::load(&state_path) {
            LoadOutcome::Loaded(s) => s,
            other => panic!("expected Loaded, got {:?}", other),
        };

        assert_eq!(loaded.dirs.len(), 1);
        let entry = &loaded.dirs["/some/dir"];
        assert_eq!(entry.dir_mtime, 1234567890);
        assert_eq!(entry.files.len(), 2);
        assert_eq!(entry.files[0].filename, "a.txt");
        assert_eq!(entry.files[0].size, 100);
        assert_eq!(entry.files[1].filename, "b.txt");
        assert_eq!(entry.files[1].mtime, 4000);
    }

    #[test]
    fn round_trip_empty_state() {
        let dir = tempfile::tempdir().unwrap();
        let state_path = dir.path().join("empty.state");

        let state = ScanState::default();
        state.save(&state_path).unwrap();
        let loaded = match ScanState::load(&state_path) {
            LoadOutcome::Loaded(s) => s,
            other => panic!("expected Loaded, got {:?}", other),
        };
        assert!(loaded.dirs.is_empty());
    }

    #[test]
    fn load_nonexistent_returns_not_found() {
        match ScanState::load(Path::new("/nonexistent/path/state.bin")) {
            LoadOutcome::NotFound => {}
            other => panic!("expected NotFound, got {:?}", other),
        }
    }

    #[test]
    fn load_garbage_returns_invalid() {
        let dir = tempfile::tempdir().unwrap();
        let state_path = dir.path().join("garbage.state");
        fs::write(&state_path, b"not valid data at all!!!!!!!!!").unwrap();

        match ScanState::load(&state_path) {
            LoadOutcome::Invalid(msg) => assert!(msg.contains("not a state file"), "{msg}"),
            other => panic!("expected Invalid, got {:?}", other),
        }
    }

    #[test]
    fn save_to_valid_path_succeeds() {
        let dir = tempfile::tempdir().unwrap();
        let state_path = dir.path().join("ok.state");
        let state = ScanState::default();
        assert!(state.save(&state_path).is_ok());
        assert!(state_path.exists());
    }

    #[test]
    fn load_empty_file() {
        let dir = tempfile::tempdir().unwrap();
        let state_path = dir.path().join("empty.state");
        fs::write(&state_path, b"").unwrap();

        match ScanState::load(&state_path) {
            LoadOutcome::Invalid(msg) => assert!(msg.contains("truncated"), "{msg}"),
            other => panic!("expected Invalid, got {:?}", other),
        }
    }

    #[test]
    fn load_truncated_file() {
        let dir = tempfile::tempdir().unwrap();
        let state_path = dir.path().join("short.state");
        fs::write(&state_path, b"FS").unwrap();

        match ScanState::load(&state_path) {
            LoadOutcome::Invalid(msg) => assert!(msg.contains("truncated"), "{msg}"),
            other => panic!("expected Invalid, got {:?}", other),
        }
    }

    #[test]
    fn load_wrong_magic() {
        let dir = tempfile::tempdir().unwrap();
        let state_path = dir.path().join("bad_magic.state");
        let mut data = vec![b'X', b'X', b'X', b'X', VERSION];
        data.extend_from_slice(&[0u8; 32]);
        fs::write(&state_path, data).unwrap();

        match ScanState::load(&state_path) {
            LoadOutcome::Invalid(msg) => assert!(msg.contains("not a state file"), "{msg}"),
            other => panic!("expected Invalid, got {:?}", other),
        }
    }

    #[test]
    fn load_wrong_version() {
        let dir = tempfile::tempdir().unwrap();
        let state_path = dir.path().join("bad_version.state");
        let mut data = Vec::from(*MAGIC);
        data.push(99); // unsupported version
        data.extend_from_slice(&[0u8; 32]);
        fs::write(&state_path, data).unwrap();

        match ScanState::load(&state_path) {
            LoadOutcome::Invalid(msg) => assert!(msg.contains("unsupported version 99"), "{msg}"),
            other => panic!("expected Invalid, got {:?}", other),
        }
    }

    #[test]
    fn load_corrupt_compressed_data() {
        let dir = tempfile::tempdir().unwrap();
        let state_path = dir.path().join("corrupt.state");
        let mut data = Vec::from(*MAGIC);
        data.push(VERSION); // version 2: expects brotli-compressed data
        data.extend_from_slice(b"this is not valid brotli data!!");
        fs::write(&state_path, data).unwrap();

        match ScanState::load(&state_path) {
            LoadOutcome::Invalid(msg) => {
                assert!(
                    msg.contains("decompression error") || msg.contains("corrupt data"),
                    "{msg}"
                );
            }
            other => panic!("expected Invalid, got {:?}", other),
        }
    }

    #[test]
    fn load_corrupt_rkyv_data_v1() {
        let dir = tempfile::tempdir().unwrap();
        let state_path = dir.path().join("corrupt_v1.state");
        let mut data = Vec::from(*MAGIC);
        data.push(1); // version 1: raw rkyv, no compression
        data.extend_from_slice(b"this is not valid rkyv data!!");
        fs::write(&state_path, data).unwrap();

        match ScanState::load(&state_path) {
            LoadOutcome::Invalid(msg) => assert!(msg.contains("corrupt data"), "{msg}"),
            other => panic!("expected Invalid, got {:?}", other),
        }
    }

    #[test]
    fn load_old_bincode_state() {
        let dir = tempfile::tempdir().unwrap();
        let state_path = dir.path().join("old.state");
        // Bincode starts with length-prefixed data, not "FSSN"
        fs::write(&state_path, b"\x00\x00\x00\x00\x00\x00\x00\x00some bincode").unwrap();

        match ScanState::load(&state_path) {
            LoadOutcome::Invalid(msg) => assert!(msg.contains("not a state file"), "{msg}"),
            other => panic!("expected Invalid, got {:?}", other),
        }
    }

    #[test]
    fn save_overwrites_corrupt_file() {
        let dir = tempfile::tempdir().unwrap();
        let state_path = dir.path().join("overwrite.state");

        // Write garbage
        fs::write(&state_path, b"garbage garbage garbage").unwrap();

        // Save valid state over it
        let mut state = ScanState::default();
        state.dirs.insert(
            "/test".into(),
            DirEntry {
                dir_mtime: 42,
                files: vec![],
            },
        );
        state.save(&state_path).unwrap();

        // Load should succeed
        let loaded = match ScanState::load(&state_path) {
            LoadOutcome::Loaded(s) => s,
            other => panic!("expected Loaded, got {:?}", other),
        };
        assert_eq!(loaded.dirs.len(), 1);
        assert_eq!(loaded.dirs["/test"].dir_mtime, 42);
    }

    #[test]
    fn round_trip_large_state() {
        let dir = tempfile::tempdir().unwrap();
        let state_path = dir.path().join("large.state");

        let mut state = ScanState::default();
        for i in 0..500 {
            let files: Vec<FileEntry> = (0..20)
                .map(|j| FileEntry {
                    filename: format!("file_{j}.txt"),
                    size: (i * 20 + j) as u64,
                    ctime: 1000 + j as i64,
                    mtime: 2000 + j as i64,
                })
                .collect();
            state.dirs.insert(
                format!("/dir_{i}/sub"),
                DirEntry {
                    dir_mtime: 1000000 + i as i64,
                    files,
                },
            );
        }

        state.save(&state_path).unwrap();
        let loaded = match ScanState::load(&state_path) {
            LoadOutcome::Loaded(s) => s,
            other => panic!("expected Loaded, got {:?}", other),
        };

        assert_eq!(loaded.dirs.len(), 500);
        let entry = &loaded.dirs["/dir_0/sub"];
        assert_eq!(entry.files.len(), 20);
        assert_eq!(entry.files[0].filename, "file_0.txt");
    }

    #[test]
    fn load_version_1_uncompressed() {
        let dir = tempfile::tempdir().unwrap();
        let state_path = dir.path().join("backpat-v1.state");

        let mut state = ScanState::default();
        state.dirs.insert(
            "/test".into(),
            DirEntry {
                dir_mtime: 42,
                files: vec![],
            },
        );

        let mut rawstate = vec![];
        // MAGIC
        rawstate.write_all("FSSN".as_bytes()).unwrap();
        // version
        rawstate.write_all(&[1]).unwrap();
        // state
        rawstate
            .write_all(&rkyv::to_bytes::<rkyv::rancor::Error>(&state).unwrap())
            .unwrap();

        fs::write(&state_path, rawstate).unwrap();
        let loaded = match ScanState::load(&state_path) {
            LoadOutcome::Loaded(s) => s,
            other => panic!("expected Loaded, got {:?}", other),
        };
        assert_eq!(loaded.dirs.len(), 1);
        assert_eq!(loaded.dirs["/test"].dir_mtime, 42);
    }

    #[test]
    fn compressed_smaller_than_uncompressed() {
        let dir = tempfile::tempdir().unwrap();
        let state_path = dir.path().join("compressed.state");

        // Build a large state with repetitive paths (highly compressible).
        let mut state = ScanState::default();
        for i in 0..500 {
            let files: Vec<FileEntry> = (0..20)
                .map(|j| FileEntry {
                    filename: format!("file_{j}.txt"),
                    size: (i * 20 + j) as u64,
                    ctime: 1000 + j as i64,
                    mtime: 2000 + j as i64,
                })
                .collect();
            state.dirs.insert(
                format!("/long/path/prefix/dir_{i}/sub"),
                DirEntry {
                    dir_mtime: 1000000 + i as i64,
                    files,
                },
            );
        }

        let rkyv_size = rkyv::to_bytes::<rkyv::rancor::Error>(&state).unwrap().len();
        state.save(&state_path).unwrap();
        let file_size = fs::metadata(&state_path).unwrap().len() as usize;

        // The file includes the 5-byte header, so subtract that for a fair comparison.
        let compressed_size = file_size - HEADER_SIZE;
        assert!(
            compressed_size < rkyv_size,
            "compressed ({compressed_size}) should be smaller than raw rkyv ({rkyv_size})"
        );
    }
}
