use crate::paths;
use std::fs;
use std::io;

/// Store blob data in the CAS. Returns `(hash, size)`.
///
/// Writes to a temp file then renames for atomicity (safe on Btrfs).
/// No-op if a blob with the same hash already exists.
pub fn store_blob(data: &[u8]) -> io::Result<(String, u64)> {
    let hash = blake3::hash(data).to_hex().to_string();
    let size = data.len() as u64;
    let path = paths::cas_blob_path(&hash).map_err(io::Error::other)?;

    if path.exists() {
        return Ok((hash, size));
    }

    let parent = path.parent().unwrap();
    fs::create_dir_all(parent)?;

    let tmp_path = parent.join(format!(".tmp.{hash}"));
    fs::write(&tmp_path, data)?;
    fs::rename(&tmp_path, &path)?;

    Ok((hash, size))
}

/// Read a blob by its hash.
pub fn get_blob(hash: &str) -> io::Result<Vec<u8>> {
    let path = paths::cas_blob_path(hash).map_err(io::Error::other)?;
    fs::read(path)
}

/// Remove a blob by its hash.
pub fn remove_blob(hash: &str) -> io::Result<()> {
    let path = paths::cas_blob_path(hash).map_err(io::Error::other)?;
    if path.exists() {
        fs::remove_file(path)?;
    }
    Ok(())
}

/// BLAKE3 hash of a file using streaming I/O (constant memory).
/// Returns None if the file can't be read.
pub fn hash_file(path: &std::path::Path) -> Option<String> {
    let file = fs::File::open(path).ok()?;
    let mut reader = io::BufReader::new(file);
    let mut hasher = blake3::Hasher::new();
    hasher.update_reader(&mut reader).ok()?;
    Some(hasher.finalize().to_hex().to_string())
}

/// List all blob hashes present on disk in the CAS directory.
pub fn list_blob_hashes() -> io::Result<Vec<String>> {
    let cas = paths::cas_dir().map_err(io::Error::other)?;
    let mut hashes = Vec::new();
    if !cas.exists() {
        return Ok(hashes);
    }
    for prefix_entry in fs::read_dir(&cas)? {
        let prefix_entry = prefix_entry?;
        if !prefix_entry.file_type()?.is_dir() {
            continue;
        }
        for blob_entry in fs::read_dir(prefix_entry.path())? {
            let blob_entry = blob_entry?;
            if blob_entry.file_type()?.is_file()
                && let Some(name) = blob_entry.file_name().to_str()
                && !name.starts_with(".tmp.")
            {
                hashes.push(name.to_string());
            }
        }
    }
    Ok(hashes)
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_hash_deterministic() {
        let hash1 = blake3::hash(b"test data").to_hex().to_string();
        let hash2 = blake3::hash(b"test data").to_hex().to_string();
        assert_eq!(hash1, hash2);
        assert_ne!(hash1, blake3::hash(b"other data").to_hex().to_string());
    }

    #[test]
    fn test_hash_file_matches_in_memory() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let data = b"streaming hash test content";
        std::fs::write(tmp.path(), data).unwrap();

        let file_hash = super::hash_file(tmp.path()).unwrap();
        let mem_hash = blake3::hash(data).to_hex().to_string();
        assert_eq!(file_hash, mem_hash);
    }

    #[test]
    fn test_hash_file_nonexistent_returns_none() {
        assert!(super::hash_file(std::path::Path::new("/nonexistent/file")).is_none());
    }
}
