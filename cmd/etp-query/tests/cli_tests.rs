use std::path::Path;

/// Generate the stats fixture database if it doesn't exist.
/// Called before trycmd tests so snapshot tests have data to query.
fn ensure_fixture_db() {
    let db_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/cmd/stats-fixture.db");
    if db_path.exists() {
        return;
    }

    // Build a minimal tokio runtime to run async DB setup.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        use etp_lib::db;
        use etp_lib::db::dao::{self, FileInput};

        let pool = db::open_db(&db_path, false).await.unwrap();

        let scan_id = dao::upsert_scan(&pool, "test", "/data/music")
            .await
            .unwrap();
        let root_dir = dao::upsert_directory(&pool, scan_id, "", 100, 4096)
            .await
            .unwrap();
        dao::replace_files(
            &pool,
            root_dir,
            &[
                FileInput {
                    filename: "album.flac".into(),
                    size: 30_000_000,
                    ctime: 100,
                    mtime: 200,
                },
                FileInput {
                    filename: "track.flac".into(),
                    size: 25_000_000,
                    ctime: 100,
                    mtime: 200,
                },
                FileInput {
                    filename: "song.mp3".into(),
                    size: 5_000_000,
                    ctime: 100,
                    mtime: 200,
                },
                FileInput {
                    filename: "cover.jpg".into(),
                    size: 500_000,
                    ctime: 100,
                    mtime: 200,
                },
                FileInput {
                    filename: "notes.txt".into(),
                    size: 1_000,
                    ctime: 100,
                    mtime: 200,
                },
            ],
        )
        .await
        .unwrap();

        let sub_dir = dao::upsert_directory(&pool, scan_id, "sub", 100, 4096)
            .await
            .unwrap();
        dao::replace_files(
            &pool,
            sub_dir,
            &[FileInput {
                filename: "bonus.mp3".into(),
                size: 4_000_000,
                ctime: 100,
                mtime: 200,
            }],
        )
        .await
        .unwrap();

        dao::finish_scan(&pool, scan_id).await.unwrap();
        db::close_db(pool).await;
    });
}

#[test]
fn cli_tests() {
    ensure_fixture_db();
    trycmd::TestCases::new().case("tests/cmd/*.toml");
}
