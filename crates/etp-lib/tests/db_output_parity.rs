use etp_lib::csv_writer;
use etp_lib::db;
use etp_lib::ops;
use etp_lib::scanner;
use std::fs;

fn make_fixture(dir: &std::path::Path) {
    // dir/
    //   a.txt (5 bytes)
    //   sub/
    //     b.txt (5 bytes)
    //     deeper/
    //       c.txt (4 bytes)
    fs::write(dir.join("a.txt"), "hello").unwrap();
    fs::create_dir_all(dir.join("sub/deeper")).unwrap();
    fs::write(dir.join("sub/b.txt"), "world").unwrap();
    fs::write(dir.join("sub/deeper/c.txt"), "deep").unwrap();
}

#[tokio::test]
async fn csv_output_from_db_is_correct() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("fixture");
    fs::create_dir(&root).unwrap();
    make_fixture(&root);

    let pool = db::open_memory().await.unwrap();
    let run_type = root.to_string_lossy();
    let (scan_id, _stats) = scanner::scan_to_db(&root, &pool, &run_type, &[], false, None)
        .await
        .unwrap();

    let csv_path = tmp.path().join("out.csv");
    csv_writer::write_csv_from_db(
        &pool,
        scan_id,
        &csv_path,
        &[],
        &ops::FilterConfig::new(true),
    )
    .await
    .unwrap();

    let content = fs::read_to_string(&csv_path).unwrap();
    let lines: Vec<&str> = content.lines().collect();
    // header + 3 files
    assert_eq!(lines.len(), 4);
    assert_eq!(lines[0], "path,size,ctime,mtime");
    // Files should be present with correct paths
    assert!(lines.iter().any(|l| l.contains("/a.txt,")));
    assert!(lines.iter().any(|l| l.contains("/sub/b.txt,")));
    assert!(lines.iter().any(|l| l.contains("/sub/deeper/c.txt,")));
}

#[tokio::test]
async fn csv_output_with_exclude_filters_correctly() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("fixture");
    fs::create_dir(&root).unwrap();
    make_fixture(&root);

    // Add an @eaDir directory
    fs::create_dir(root.join("@eaDir")).unwrap();
    fs::write(root.join("@eaDir/junk.txt"), "junk").unwrap();

    let pool = db::open_memory().await.unwrap();
    let run_type = root.to_string_lossy();
    let exclude = vec!["@eaDir".to_string()];
    let (scan_id, _stats) = scanner::scan_to_db(&root, &pool, &run_type, &exclude, false, None)
        .await
        .unwrap();

    let csv_path = tmp.path().join("out.csv");
    csv_writer::write_csv_from_db(
        &pool,
        scan_id,
        &csv_path,
        &exclude,
        &ops::FilterConfig::new(true),
    )
    .await
    .unwrap();

    let content = fs::read_to_string(&csv_path).unwrap();
    // Should not contain @eaDir files
    assert!(!content.contains("@eaDir"));
    // Should still contain the other 3 files
    let lines: Vec<&str> = content.lines().collect();
    assert_eq!(lines.len(), 4); // header + 3 files
}

#[tokio::test]
async fn scan_to_db_excludes_directories() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("fixture");
    fs::create_dir(&root).unwrap();
    make_fixture(&root);

    // Add excluded dir
    fs::create_dir(root.join("@eaDir")).unwrap();
    fs::write(root.join("@eaDir/junk.txt"), "junk").unwrap();

    let pool = db::open_memory().await.unwrap();
    let run_type = root.to_string_lossy();
    let exclude = vec!["@eaDir".to_string()];
    let (scan_id, stats) = scanner::scan_to_db(&root, &pool, &run_type, &exclude, false, None)
        .await
        .unwrap();

    // @eaDir should be excluded from scan
    // root, sub, sub/deeper = 3 dirs
    assert_eq!(stats.dirs_scanned, 3);

    let dir_paths = db::dao::list_directory_paths(&pool, scan_id).await.unwrap();
    assert_eq!(dir_paths.len(), 3);

    let files = db::dao::list_files(&pool, scan_id).await.unwrap();
    // a.txt, b.txt, c.txt = 3 files (no junk.txt)
    assert_eq!(files.len(), 3);
}

#[tokio::test]
async fn scan_to_db_without_exclude_captures_all() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("fixture");
    fs::create_dir(&root).unwrap();
    make_fixture(&root);

    // Add @eaDir directory
    fs::create_dir(root.join("@eaDir")).unwrap();
    fs::write(root.join("@eaDir/junk.txt"), "junk").unwrap();

    let pool = db::open_memory().await.unwrap();
    let run_type = root.to_string_lossy();
    let (scan_id, stats) = scanner::scan_to_db(&root, &pool, &run_type, &[], false, None)
        .await
        .unwrap();

    // No exclude — all directories captured
    // root, sub, sub/deeper, @eaDir = 4 dirs
    assert_eq!(stats.dirs_scanned, 4);

    let files = db::dao::list_files(&pool, scan_id).await.unwrap();
    assert_eq!(files.len(), 4);
}
