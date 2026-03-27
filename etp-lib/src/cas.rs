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

#[cfg(test)]
mod tests {
    #[test]
    fn test_hash_deterministic() {
        let hash1 = blake3::hash(b"test data").to_hex().to_string();
        let hash2 = blake3::hash(b"test data").to_hex().to_string();
        assert_eq!(hash1, hash2);
        assert_ne!(hash1, blake3::hash(b"other data").to_hex().to_string());
    }
}
