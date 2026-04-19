//! Behavior tests for `etp-find`'s SQL-pushdown pattern matching.
//!
//! # Dispatch
//!
//! Every search runs through SQLite's REGEXP UDF (registered by
//! `with_regexp()`), backed by the Rust `regex` crate. `-i` is expressed as a
//! `(?i)` prefix on the pattern so case folding happens inside the regex
//! engine. There is no separate LIKE path.
//!
//! | `-i`     | SQL op                   | example pattern sent to SQLite |
//! | -------- | ------------------------ | ------------------------------ |
//! | off      | `… REGEXP ?`             | `swans`                        |
//! | **on**   | `… REGEXP ?` with `(?i)` | `(?i)swans`                    |
//!
//! # Case-sensitivity matrix
//!
//! With `-i` off, the pattern is case-sensitive. With `-i` on, the Rust
//! `regex` crate applies Unicode *simple* case folding (1:1 mappings only,
//! e.g. `Å`↔`å`, `Σ`↔`σ`, `ẞ`↔`ß`). The `ß`↔`SS` 1:2 mapping is *full* case
//! folding, which Rust's regex does not implement.
//!
//! |                      | ASCII case | non-ASCII 1:1 | `ß`↔`SS` (1:2) |
//! | -------------------- | ---------- | ------------- | --------------- |
//! | REGEXP, no `-i`      | strict     | strict        | no              |
//! | REGEXP with `(?i)`   | folded     | folded        | **no**          |
//!
//! Each row below has at least one test that pins its cell.
//!
//! Notes:
//! - NFKC folding (stripping diacritics) is not applied — `björk` and `bjork`
//!   are distinct under `-i`.
//! - LIKE wildcards (`%`, `_`) have no special meaning here since we never
//!   call LIKE. A pattern like `50%` is a valid regex that matches literal
//!   `50%` (`%` is not a regex metacharacter).

use etp_lib::db;
use etp_lib::ops;
use etp_lib::scanner;
use std::fs;

fn make_fixture(dir: &std::path::Path) {
    // ASCII case variants
    fs::write(dir.join("Swans - The Seer.flac"), b"x").unwrap();
    fs::write(dir.join("swans-demo.mp3"), b"x").unwrap();
    fs::write(dir.join("SWANS_live.mp3"), b"x").unwrap();
    // Non-ASCII letter (diacritic) — case fold only, no diacritic strip
    fs::write(dir.join("Björk.flac"), b"x").unwrap();
    fs::write(dir.join("bjork.flac"), b"x").unwrap();
    // German sharp-S fold: ß ↔ SS (1:2, not applied)
    fs::write(dir.join("Weiße Nächte.flac"), b"x").unwrap();
    fs::write(dir.join("WEISSE NACHTE.flac"), b"x").unwrap();
    // Special chars that happen not to be regex metacharacters
    fs::write(dir.join("50% off.txt"), b"x").unwrap();
    fs::write(dir.join("notes_2026.md"), b"x").unwrap();
    fs::create_dir_all(dir.join("sub")).unwrap();
    fs::write(dir.join("sub/track01-swans.flac"), b"x").unwrap();
    // System-file shapes used by the SQL-layer filter tests.
    fs::create_dir_all(dir.join("@eaDir/SYNOLOGY")).unwrap();
    fs::write(dir.join("@eaDir/SYNOLOGY/Swans@eadir-thumb.jpg"), b"x").unwrap();
    fs::write(dir.join("@eaDir/SYNOLOGY/swans-eaDir-inside.flac"), b"x").unwrap();
    fs::write(dir.join("sub/.etp.db"), b"x").unwrap();
}

async fn scanned_pool() -> (sqlx::SqlitePool, i64, tempfile::TempDir) {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("fixture");
    fs::create_dir(&root).unwrap();
    make_fixture(&root);

    let pool = db::open_memory().await.unwrap();
    let run_type = root.to_string_lossy();
    let (scan_id, _stats) = scanner::scan_to_db(&root, &pool, &run_type, &[], false, None)
        .await
        .unwrap();
    (pool, scan_id, tmp)
}

async fn find(pool: &sqlx::SqlitePool, scan_id: i64, pat: &str, i: bool) -> Vec<String> {
    // include_system_files=true — keeps system paths visible for the case-
    // sensitivity tests that don't care about the system filter.
    let filter = ops::FilterConfig::new(true);
    ops::collect_find_matches(pool, Some(scan_id), pat, i, &[], &filter)
        .await
        .unwrap()
        .into_iter()
        .map(|m| m.full_path.rsplit('/').next().unwrap().to_string())
        .collect()
}

async fn find_with_filter(
    pool: &sqlx::SqlitePool,
    scan_id: i64,
    pat: &str,
    i: bool,
    include_system: bool,
) -> Vec<String> {
    let filter = ops::FilterConfig::new(include_system);
    ops::collect_find_matches(pool, Some(scan_id), pat, i, &[], &filter)
        .await
        .unwrap()
        .into_iter()
        .map(|m| m.full_path.rsplit('/').next().unwrap().to_string())
        .collect()
}

// ─── REGEXP no -i: strict ASCII, non-ASCII, and ß ──────────────────────────

#[tokio::test]
async fn without_i_ascii_case_strict() {
    let (pool, scan_id, _tmp) = scanned_pool().await;
    let names = find(&pool, scan_id, "swans", false).await;
    assert!(names.iter().any(|n| n == "swans-demo.mp3"), "{names:?}");
    assert!(names.iter().any(|n| n == "track01-swans.flac"), "{names:?}");
    assert!(
        !names.iter().any(|n| n == "Swans - The Seer.flac"),
        "mixed-case must NOT match: {names:?}"
    );
    assert!(
        !names.iter().any(|n| n == "SWANS_live.mp3"),
        "uppercase must NOT match: {names:?}"
    );
}

#[tokio::test]
async fn without_i_regex_metachars_still_case_strict() {
    let (pool, scan_id, _tmp) = scanned_pool().await;
    let names = find(&pool, scan_id, "sw.*s", false).await;
    assert!(names.iter().any(|n| n == "swans-demo.mp3"));
    assert!(names.iter().any(|n| n == "track01-swans.flac"));
    assert!(
        !names.iter().any(|n| n == "Swans - The Seer.flac"),
        "Upper-S must not match without -i: {names:?}"
    );
}

#[tokio::test]
async fn without_i_non_ascii_case_strict() {
    let (pool, scan_id, _tmp) = scanned_pool().await;
    let names = find(&pool, scan_id, "björk", false).await;
    assert!(
        !names.iter().any(|n| n == "Björk.flac"),
        "non-ASCII uppercase must NOT match: {names:?}"
    );
    assert!(
        !names.iter().any(|n| n == "bjork.flac"),
        "diacritic-stripped variant must NOT match: {names:?}"
    );
    assert!(names.is_empty(), "no exact-case match expected: {names:?}");
}

#[tokio::test]
async fn without_i_sharp_s_not_folded() {
    let (pool, scan_id, _tmp) = scanned_pool().await;
    let names = find(&pool, scan_id, "weiße", false).await;
    assert!(
        !names.iter().any(|n| n == "WEISSE NACHTE.flac"),
        "ß must not fold to SS without -i: {names:?}"
    );
    assert!(names.is_empty(), "{names:?}");
}

// ─── REGEXP with -i: Unicode simple case folding ───────────────────────────

#[tokio::test]
async fn with_i_ascii_case_folded() {
    let (pool, scan_id, _tmp) = scanned_pool().await;
    let names = find(&pool, scan_id, "swans", true).await;
    assert!(names.iter().any(|n| n == "swans-demo.mp3"));
    assert!(names.iter().any(|n| n == "Swans - The Seer.flac"));
    assert!(names.iter().any(|n| n == "SWANS_live.mp3"));
    assert!(names.iter().any(|n| n == "track01-swans.flac"));
}

#[tokio::test]
async fn with_i_non_ascii_case_folded() {
    let (pool, scan_id, _tmp) = scanned_pool().await;
    let names = find(&pool, scan_id, "björk", true).await;
    assert!(names.iter().any(|n| n == "Björk.flac"), "{names:?}");
    assert!(
        !names.iter().any(|n| n == "bjork.flac"),
        "Unicode case fold does not strip diacritics: {names:?}"
    );
}

#[tokio::test]
async fn with_i_sharp_s_is_not_ss_folded() {
    let (pool, scan_id, _tmp) = scanned_pool().await;
    // `s`↔`S` is 1:1 so `weisse` matches `WEISSE`, but ß↔SS is a 1:2 full-fold
    // mapping that Rust's regex doesn't implement.
    let names = find(&pool, scan_id, "weisse", true).await;
    assert!(
        names.iter().any(|n| n == "WEISSE NACHTE.flac"),
        "ASCII case fold must apply: {names:?}"
    );
    assert!(
        !names.iter().any(|n| n == "Weiße Nächte.flac"),
        "ß ↔ SS is a 1:2 full-fold that is NOT applied: {names:?}"
    );
}

#[tokio::test]
async fn with_i_sharp_s_pattern_matches_exact() {
    let (pool, scan_id, _tmp) = scanned_pool().await;
    let names = find(&pool, scan_id, "weiße", true).await;
    assert!(
        names.iter().any(|n| n == "Weiße Nächte.flac"),
        "exact ß match with ASCII case fold on the rest: {names:?}"
    );
    assert!(
        !names.iter().any(|n| n == "WEISSE NACHTE.flac"),
        "ß in pattern should not fold to match SS in filename: {names:?}"
    );
}

// ─── Regex metacharacters are interpreted as regex ──────────────────────────

#[tokio::test]
async fn percent_is_literal_in_regex() {
    // `%` is not a regex metacharacter, so `50%` matches literal "50%".
    let (pool, scan_id, _tmp) = scanned_pool().await;
    let names = find(&pool, scan_id, "50%", false).await;
    assert_eq!(names, vec!["50% off.txt".to_string()], "{names:?}");
}

#[tokio::test]
async fn underscore_is_literal_in_regex() {
    // `_` is not a regex metacharacter either.
    let (pool, scan_id, _tmp) = scanned_pool().await;
    let names = find(&pool, scan_id, "_2026", false).await;
    assert_eq!(names, vec!["notes_2026.md".to_string()], "{names:?}");
}

// ─── SQL-layer system-file exclusion ────────────────────────────────────────

#[tokio::test]
async fn system_filter_default_hides_eadir_children() {
    // Default filter (include_system=false) must drop anything inside @eaDir,
    // even when the pattern matches its contents.
    let (pool, scan_id, _tmp) = scanned_pool().await;
    let names = find_with_filter(&pool, scan_id, "(?i)swans", true, false).await;
    // Should surface the real Swans files...
    assert!(names.iter().any(|n| n == "swans-demo.mp3"));
    assert!(names.iter().any(|n| n == "Swans - The Seer.flac"));
    assert!(names.iter().any(|n| n == "track01-swans.flac"));
    // ...but not @eaDir descendants, even though they contain "swans".
    assert!(
        !names.iter().any(|n| n == "Swans@eadir-thumb.jpg"),
        "file inside @eaDir must be hidden: {names:?}"
    );
    assert!(
        !names.iter().any(|n| n == "swans-eaDir-inside.flac"),
        "file inside @eaDir must be hidden: {names:?}"
    );
}

#[tokio::test]
async fn system_filter_include_shows_eadir_children() {
    // With include_system_files=true the SQL filter is bypassed entirely and
    // @eaDir contents appear in the results.
    let (pool, scan_id, _tmp) = scanned_pool().await;
    let names = find_with_filter(&pool, scan_id, "(?i)swans", true, true).await;
    assert!(
        names.iter().any(|n| n == "Swans@eadir-thumb.jpg"),
        "{names:?}"
    );
    assert!(
        names.iter().any(|n| n == "swans-eaDir-inside.flac"),
        "{names:?}"
    );
}

#[tokio::test]
async fn system_filter_hides_etp_db_filename() {
    // `.etp.db` is a system-pattern filename. It must be excluded by default.
    let (pool, scan_id, _tmp) = scanned_pool().await;
    let default_names = find_with_filter(&pool, scan_id, "etp", false, false).await;
    assert!(
        !default_names.iter().any(|n| n == ".etp.db"),
        "default filter must hide .etp.db: {default_names:?}"
    );

    let include_names = find_with_filter(&pool, scan_id, "etp", false, true).await;
    assert!(
        include_names.iter().any(|n| n == ".etp.db"),
        "include_system must surface .etp.db: {include_names:?}"
    );
}
