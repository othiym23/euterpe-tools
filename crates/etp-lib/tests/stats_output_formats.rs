use etp_lib::db;
use etp_lib::db::dao::{self, FileInput};
use etp_lib::ops::{self, FilterConfig};
use std::collections::HashMap;

/// Helper: create a scan with known files for stats testing.
async fn setup_stats_db(pool: &sqlx::SqlitePool) -> i64 {
    let scan_id = dao::upsert_scan(pool, "test", "/data").await.unwrap();
    let dir_id = dao::upsert_directory(pool, scan_id, "", 100, 4096)
        .await
        .unwrap();
    dao::replace_files(
        pool,
        dir_id,
        &[
            FileInput {
                filename: "song.mp3".into(),
                size: 5_000_000,
                ctime: 100,
                mtime: 200,
            },
            FileInput {
                filename: "track.flac".into(),
                size: 30_000_000,
                ctime: 100,
                mtime: 200,
            },
            FileInput {
                filename: "another.flac".into(),
                size: 25_000_000,
                ctime: 100,
                mtime: 200,
            },
            FileInput {
                filename: "cover.jpg".into(),
                size: 500_000,
                ctime: 100,
                mtime: 200,
            },
        ],
    )
    .await
    .unwrap();
    dao::finish_scan(pool, scan_id).await.unwrap();
    scan_id
}

/// Collect stats by streaming (same logic as etp-query stats).
async fn collect_stats(
    pool: &sqlx::SqlitePool,
    scan_id: i64,
    filter: &FilterConfig,
) -> (usize, u64, Vec<(String, usize)>) {
    use std::future::poll_fn;
    use std::pin::Pin;

    let mut stream = db::dao::stream_files(pool, scan_id);
    let mut file_count: usize = 0;
    let mut total: u64 = 0;
    let mut ext_counts: HashMap<String, usize> = HashMap::new();

    while let Some(result) = poll_fn(|cx| {
        use futures_core::Stream;
        Pin::new(&mut stream).poll_next(cx)
    })
    .await
    {
        let record = result.unwrap();
        if !filter.should_show(&record.dir_path, &record.filename) {
            continue;
        }
        file_count += 1;
        total += record.size;
        let ext = record
            .filename
            .rfind('.')
            .map(|i| record.filename[i + 1..].to_lowercase())
            .unwrap_or_default();
        if !ext.is_empty() {
            *ext_counts.entry(ext).or_default() += 1;
        }
    }

    let mut sorted: Vec<_> = ext_counts.into_iter().collect();
    sorted.sort_by_key(|b| std::cmp::Reverse(b.1));
    (file_count, total, sorted)
}

#[tokio::test]
async fn stats_text_alignment() {
    let pool = db::open_memory().await.unwrap();
    let scan_id = setup_stats_db(&pool).await;
    let filter = FilterConfig::new(true);
    let (file_count, _total, sorted) = collect_stats(&pool, scan_id, &filter).await;

    let max_width = sorted.iter().map(|(e, _)| e.len() + 1).max().unwrap_or(0);
    let mut lines = Vec::new();
    for (ext, count) in &sorted {
        let label = format!(".{ext}");
        lines.push(format!("  {label:>max_width$}: {count}"));
    }

    assert_eq!(file_count, 4);
    assert!(lines.iter().any(|l| l.contains(".flac: 2")));
    assert!(lines.iter().any(|l| l.contains(".mp3: 1")));
    assert!(lines.iter().any(|l| l.contains(".jpg: 1")));

    // All colons at the same column
    let colon_positions: Vec<usize> = lines.iter().map(|l| l.find(':').unwrap()).collect();
    assert!(
        colon_positions.windows(2).all(|w| w[0] == w[1]),
        "colons should be aligned: {lines:?}"
    );
}

#[tokio::test]
async fn stats_json_roundtrips() {
    let pool = db::open_memory().await.unwrap();
    let scan_id = setup_stats_db(&pool).await;
    let filter = FilterConfig::new(true);
    let (file_count, total, sorted) = collect_stats(&pool, scan_id, &filter).await;

    let extensions: serde_json::Map<String, serde_json::Value> = sorted
        .iter()
        .map(|(ext, count)| (ext.clone(), serde_json::Value::Number((*count).into())))
        .collect();
    let obj = serde_json::json!({
        "files": file_count,
        "total_size": total,
        "total_size_human": ops::format_size(total),
        "extensions": extensions,
    });
    let output = serde_json::to_string_pretty(&obj).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();

    assert_eq!(parsed["files"], 4);
    assert_eq!(parsed["total_size"], 60_500_000u64);
    assert_eq!(parsed["extensions"]["flac"], 2);
    assert_eq!(parsed["extensions"]["mp3"], 1);
    assert_eq!(parsed["extensions"]["jpg"], 1);
    assert!(parsed["total_size_human"].as_str().unwrap().contains("MiB"));
}

#[tokio::test]
async fn stats_csv_has_header_and_rows() {
    let pool = db::open_memory().await.unwrap();
    let scan_id = setup_stats_db(&pool).await;
    let filter = FilterConfig::new(true);
    let (_file_count, _total, sorted) = collect_stats(&pool, scan_id, &filter).await;

    let mut output = String::from("extension,count\n");
    for (ext, count) in &sorted {
        output.push_str(&format!(".{ext},{count}\n"));
    }

    let lines: Vec<&str> = output.lines().collect();
    assert_eq!(lines[0], "extension,count");
    assert!(lines.contains(&".flac,2"));
    assert!(lines.contains(&".mp3,1"));
    assert!(lines.contains(&".jpg,1"));
}

#[tokio::test]
async fn stats_kdl_structure() {
    let pool = db::open_memory().await.unwrap();
    let scan_id = setup_stats_db(&pool).await;
    let filter = FilterConfig::new(true);
    let (file_count, total, sorted) = collect_stats(&pool, scan_id, &filter).await;

    let mut output = String::new();
    output.push_str("stats {\n");
    output.push_str(&format!("    files {file_count}\n"));
    output.push_str(&format!("    total-size {total}\n"));
    output.push_str(&format!(
        "    total-size-human \"{}\"\n",
        ops::format_size(total)
    ));
    if !sorted.is_empty() {
        output.push_str("    extensions {\n");
        for (ext, count) in &sorted {
            output.push_str(&format!("        ext \".{ext}\" {count}\n"));
        }
        output.push_str("    }\n");
    }
    output.push_str("}\n");

    assert!(output.contains("files 4"));
    assert!(output.contains("total-size 60500000"));
    assert!(output.contains("total-size-human \""));
    assert!(output.contains("MiB\""));
    assert!(output.contains("ext \".flac\" 2"));
    assert!(output.contains("ext \".mp3\" 1"));
    assert!(output.contains("ext \".jpg\" 1"));
    assert!(output.starts_with("stats {"));
    assert!(output.ends_with("}\n"));
}
