use etp_lib::config::RuntimeConfig;
use etp_lib::db;
use etp_lib::ops;
use std::fs;

/// open_and_resolve_scan with --scan creates the DB and returns a valid scan_id.
#[tokio::test]
async fn scan_mode_creates_db_and_returns_scan_id() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("fixture");
    fs::create_dir(&root).unwrap();
    fs::write(root.join("a.txt"), "hello").unwrap();

    // Use a DB path outside the scan root to avoid indexing the DB itself
    let db_path = tmp.path().join("test.db");

    let defaults = RuntimeConfig::defaults();
    let ctx = ops::open_and_resolve_scan(
        &root,
        Some(db_path),
        true,  // --scan
        false, // --no-scan
        &[],
        false,
        &defaults,
    )
    .await
    .unwrap();

    assert!(ctx.scan_id > 0, "scan_id should be positive");
    assert_eq!(ctx.directory, root);

    let files = db::dao::list_files(&ctx.pool, ctx.scan_id).await.unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].filename, "a.txt");

    db::close_db(ctx.pool).await;
}

/// open_and_resolve_scan with --no-scan reads existing DB.
#[tokio::test]
async fn no_scan_mode_reads_existing_db() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("fixture");
    fs::create_dir(&root).unwrap();
    fs::write(root.join("a.txt"), "hello").unwrap();

    let db_path = tmp.path().join("test.db");

    // First: scan to create the DB
    let ctx1 = ops::open_and_resolve_scan(
        &root,
        Some(db_path.clone()),
        true,
        false,
        &[],
        false,
        &RuntimeConfig::defaults(),
    )
    .await
    .unwrap();
    let first_scan_id = ctx1.scan_id;
    db::close_db(ctx1.pool).await;

    // Second: read without scanning
    let ctx2 = ops::open_and_resolve_scan(
        &root,
        Some(db_path),
        false,
        false,
        &[],
        false,
        &RuntimeConfig::defaults(),
    )
    .await
    .unwrap();
    assert_eq!(
        ctx2.scan_id, first_scan_id,
        "should return the same scan_id from existing DB"
    );

    db::close_db(ctx2.pool).await;
}

/// open_and_resolve_scan uses custom --db path when provided.
#[tokio::test]
async fn custom_db_path() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("fixture");
    fs::create_dir(&root).unwrap();
    fs::write(root.join("a.txt"), "hello").unwrap();

    let custom_db = tmp.path().join("custom.db");

    let defaults = RuntimeConfig::defaults();
    let ctx = ops::open_and_resolve_scan(
        &root,
        Some(custom_db.clone()),
        true,
        false,
        &[],
        false,
        &defaults,
    )
    .await
    .unwrap();

    assert!(custom_db.exists(), "DB should be at custom path");
    assert!(
        !root.join(".etp.db").exists(),
        "default DB path should not exist"
    );

    db::close_db(ctx.pool).await;
}

/// open_and_resolve_scan with --scan respects --exclude.
#[tokio::test]
async fn scan_respects_exclude() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("fixture");
    fs::create_dir_all(root.join("keep")).unwrap();
    fs::create_dir_all(root.join("skip")).unwrap();
    fs::write(root.join("keep/a.txt"), "keep").unwrap();
    fs::write(root.join("skip/b.txt"), "skip").unwrap();

    let db_path = tmp.path().join("test.db");
    let exclude = vec!["skip".to_string()];
    let ctx = ops::open_and_resolve_scan(
        &root,
        Some(db_path),
        true,
        false,
        &exclude,
        false,
        &RuntimeConfig::defaults(),
    )
    .await
    .unwrap();

    let files = db::dao::list_files(&ctx.pool, ctx.scan_id).await.unwrap();
    assert_eq!(files.len(), 1, "excluded directory should be skipped");
    assert_eq!(files[0].filename, "a.txt");

    db::close_db(ctx.pool).await;
}

/// open_and_resolve_scan returns the directory it was given.
#[tokio::test]
async fn directory_is_preserved() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("fixture");
    fs::create_dir(&root).unwrap();

    let db_path = tmp.path().join("test.db");
    let ctx = ops::open_and_resolve_scan(
        &root,
        Some(db_path),
        true,
        false,
        &[],
        false,
        &RuntimeConfig::defaults(),
    )
    .await
    .unwrap();
    assert_eq!(ctx.directory, root);

    db::close_db(ctx.pool).await;
}
