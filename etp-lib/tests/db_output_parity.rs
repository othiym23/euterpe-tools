use etp_lib::csv_writer;
use etp_lib::db;
use etp_lib::scanner;
use etp_lib::state::ScanState;
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
async fn csv_output_is_byte_identical() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("fixture");
    fs::create_dir(&root).unwrap();
    make_fixture(&root);

    // Old path: scan → ScanState → write_csv
    let old_csv = tmp.path().join("old.csv");
    let mut state = ScanState::default();
    scanner::scan(&root, &mut state, &[], false).unwrap();
    csv_writer::write_csv(&state, &old_csv).unwrap();

    // New path: scan_to_db → write_csv_from_db
    let new_csv = tmp.path().join("new.csv");
    let pool = db::open_memory().await.unwrap();
    let run_type = root.to_string_lossy();
    let (scan_id, _stats) = scanner::scan_to_db(&root, &pool, &run_type, false)
        .await
        .unwrap();
    csv_writer::write_csv_from_db(&pool, scan_id, &new_csv, &[])
        .await
        .unwrap();

    let old_content = fs::read_to_string(&old_csv).unwrap();
    let new_content = fs::read_to_string(&new_csv).unwrap();
    assert_eq!(
        old_content, new_content,
        "CSV output must be byte-identical between old and new path"
    );
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

    // Old path with exclude
    let old_csv = tmp.path().join("old.csv");
    let mut state = ScanState::default();
    let exclude = vec!["@eaDir".to_string()];
    scanner::scan(&root, &mut state, &exclude, false).unwrap();
    csv_writer::write_csv(&state, &old_csv).unwrap();

    // New path: scan everything, exclude in CSV writer
    let new_csv = tmp.path().join("new.csv");
    let pool = db::open_memory().await.unwrap();
    let run_type = root.to_string_lossy();
    let (scan_id, _stats) = scanner::scan_to_db(&root, &pool, &run_type, false)
        .await
        .unwrap();
    csv_writer::write_csv_from_db(&pool, scan_id, &new_csv, &exclude)
        .await
        .unwrap();

    let old_content = fs::read_to_string(&old_csv).unwrap();
    let new_content = fs::read_to_string(&new_csv).unwrap();
    assert_eq!(
        old_content, new_content,
        "CSV with exclude must match between old and new path"
    );
}

#[tokio::test]
async fn scan_to_db_captures_all_directories() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("fixture");
    fs::create_dir(&root).unwrap();
    make_fixture(&root);

    // Add excluded dir
    fs::create_dir(root.join("@eaDir")).unwrap();
    fs::write(root.join("@eaDir/junk.txt"), "junk").unwrap();

    let pool = db::open_memory().await.unwrap();
    let run_type = root.to_string_lossy();
    let (scan_id, stats) = scanner::scan_to_db(&root, &pool, &run_type, false)
        .await
        .unwrap();

    // scan_to_db should capture ALL directories, including @eaDir
    // root, sub, sub/deeper, @eaDir = 4 dirs
    assert_eq!(stats.dirs_scanned, 4);

    let dir_paths = db::dao::list_directory_paths(&pool, scan_id).await.unwrap();
    assert_eq!(dir_paths.len(), 4);

    let files = db::dao::list_files(&pool, scan_id).await.unwrap();
    // a.txt, b.txt, c.txt, junk.txt = 4 files
    assert_eq!(files.len(), 4);
}
