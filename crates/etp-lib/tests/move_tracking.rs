use etp_lib::db;
use etp_lib::scanner;
use std::fs;

/// Helper to scan a directory and return the scan_id.
async fn scan(dir: &std::path::Path, pool: &sqlx::SqlitePool) -> i64 {
    // Sleep briefly to ensure directory mtime changes are detectable
    // (filesystem mtime resolution may be 1 second).
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    let run_type = dir.to_string_lossy();
    let (scan_id, _stats) = scanner::scan_to_db(dir, pool, &run_type, &[".etp.db".into()], false)
        .await
        .unwrap();
    scan_id
}

/// Create a pool with the DB outside the scan root.
async fn open_test_db() -> (tempfile::TempDir, sqlx::SqlitePool) {
    let db_dir = tempfile::tempdir().unwrap();
    let db_path = db_dir.path().join("test.db");
    let pool = db::open_db(&db_path, false).await.unwrap();
    (db_dir, pool)
}

/// Get a file's database ID by filename suffix.
async fn file_id(pool: &sqlx::SqlitePool, scan_id: i64, name: &str) -> i64 {
    db::dao::find_file_id_by_path_suffix(pool, scan_id, name)
        .await
        .unwrap()
        .unwrap_or_else(|| panic!("file not found: {name}"))
}

#[tokio::test]
async fn move_between_directories_preserves_id() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();

    // Create initial structure: dir_a/song.mp3
    fs::create_dir_all(root.join("dir_a")).unwrap();
    fs::write(root.join("dir_a/song.mp3"), "fake audio content here").unwrap();

    let (_db_dir, pool) = open_test_db().await;

    // First scan
    let scan_id = scan(root, &pool).await;
    let original_id = file_id(&pool, scan_id, "song.mp3").await;

    // Move the file to dir_b
    fs::create_dir_all(root.join("dir_b")).unwrap();
    fs::rename(root.join("dir_a/song.mp3"), root.join("dir_b/song.mp3")).unwrap();

    // Rescan — should detect the move
    let scan_id = scan(root, &pool).await;
    let new_id = file_id(&pool, scan_id, "song.mp3").await;

    // The file ID should be preserved
    assert_eq!(
        original_id, new_id,
        "file ID should be preserved after move"
    );

    db::close_db(pool).await;
}

#[tokio::test]
async fn rename_in_place_preserves_id() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();

    fs::write(root.join("old_name.flac"), "some flac data here!!").unwrap();

    let (_db_dir, pool) = open_test_db().await;

    let scan_id = scan(root, &pool).await;
    let original_id = file_id(&pool, scan_id, "old_name.flac").await;

    // Rename in the same directory
    fs::rename(root.join("old_name.flac"), root.join("new_name.flac")).unwrap();

    let scan_id = scan(root, &pool).await;
    let new_id = file_id(&pool, scan_id, "new_name.flac").await;

    assert_eq!(
        original_id, new_id,
        "file ID should be preserved after rename"
    );

    db::close_db(pool).await;
}

#[tokio::test]
async fn true_deletion_removes_file() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();

    fs::write(root.join("doomed.txt"), "goodbye").unwrap();

    let (_db_dir, pool) = open_test_db().await;

    let scan_id = scan(root, &pool).await;
    assert!(
        db::dao::find_file_id_by_path_suffix(&pool, scan_id, "doomed.txt")
            .await
            .unwrap()
            .is_some()
    );

    // Delete the file
    fs::remove_file(root.join("doomed.txt")).unwrap();

    let scan_id = scan(root, &pool).await;
    assert!(
        db::dao::find_file_id_by_path_suffix(&pool, scan_id, "doomed.txt")
            .await
            .unwrap()
            .is_none(),
        "deleted file should be gone from DB"
    );

    db::close_db(pool).await;
}

#[tokio::test]
async fn same_size_different_content_no_false_match() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();

    // Two files with identical size but different content
    fs::create_dir_all(root.join("dir_a")).unwrap();
    fs::create_dir_all(root.join("dir_b")).unwrap();
    fs::write(root.join("dir_a/file1.txt"), "AAAAAAAAAA").unwrap(); // 10 bytes
    fs::write(root.join("dir_b/file2.txt"), "BBBBBBBBBB").unwrap(); // 10 bytes

    let (_db_dir, pool) = open_test_db().await;

    let scan_id = scan(root, &pool).await;
    let id1 = file_id(&pool, scan_id, "file1.txt").await;
    let id2 = file_id(&pool, scan_id, "file2.txt").await;
    assert_ne!(id1, id2);

    // Delete file1, add file3 with same size but different content
    fs::remove_file(root.join("dir_a/file1.txt")).unwrap();
    fs::write(root.join("dir_a/file3.txt"), "CCCCCCCCCC").unwrap(); // 10 bytes, different content

    let scan_id = scan(root, &pool).await;

    // file2 should still have its original ID (not stolen by move matching)
    let id2_after = file_id(&pool, scan_id, "file2.txt").await;
    assert_eq!(id2, id2_after, "file2 ID should be unchanged");

    // file1 should be gone
    assert!(
        db::dao::find_file_id_by_path_suffix(&pool, scan_id, "file1.txt")
            .await
            .unwrap()
            .is_none()
    );

    db::close_db(pool).await;
}

#[tokio::test]
async fn no_moves_all_deleted() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();

    fs::write(root.join("a.txt"), "aaa").unwrap();
    fs::write(root.join("b.txt"), "bbb").unwrap();

    let (_db_dir, pool) = open_test_db().await;

    scan(root, &pool).await;

    // Delete both, add completely new files
    fs::remove_file(root.join("a.txt")).unwrap();
    fs::remove_file(root.join("b.txt")).unwrap();
    fs::write(root.join("c.txt"), "completely different content").unwrap();

    let scan_id = scan(root, &pool).await;

    // Only c.txt should exist
    let count = db::dao::count_files(&pool, scan_id).await.unwrap();
    assert_eq!(count, 1);

    assert!(
        db::dao::find_file_id_by_path_suffix(&pool, scan_id, "c.txt")
            .await
            .unwrap()
            .is_some()
    );

    db::close_db(pool).await;
}

#[tokio::test]
async fn move_with_stored_hash_skips_file_io() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();

    fs::write(root.join("song.flac"), "audio content for hashing").unwrap();

    let (_db_dir, pool) = open_test_db().await;

    let scan_id = scan(root, &pool).await;
    let original_id = file_id(&pool, scan_id, "song.flac").await;

    // Simulate a metadata scan by storing a content hash
    let hash = blake3::hash(b"audio content for hashing")
        .to_hex()
        .to_string();
    db::dao::mark_metadata_scanned(&pool, original_id, Some(&hash))
        .await
        .unwrap();

    // Move the file to a subdirectory
    fs::create_dir_all(root.join("subdir")).unwrap();
    fs::rename(root.join("song.flac"), root.join("subdir/song.flac")).unwrap();

    // Rescan — should detect move using stored hash (old file is gone)
    let scan_id = scan(root, &pool).await;
    let new_id = file_id(&pool, scan_id, "song.flac").await;

    assert_eq!(
        original_id, new_id,
        "file ID should be preserved using stored hash"
    );

    db::close_db(pool).await;
}
