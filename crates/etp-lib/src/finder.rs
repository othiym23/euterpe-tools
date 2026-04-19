/// A file that matched a regex pattern, with its full path pre-computed.
///
/// Produced by `ops::collect_find_matches` after the SQL layer has filtered
/// down to matching rows. The regex evaluation itself runs inside SQLite via
/// the REGEXP UDF registered by `db::open_db` / `db::open_memory` — see the
/// "Search (etp-find)" section of `docs/DESIGN_NOTES.md`.
#[derive(Debug, Clone)]
pub struct FindMatch {
    pub full_path: String,
    pub size: u64,
    pub ctime: i64,
    pub mtime: i64,
}
