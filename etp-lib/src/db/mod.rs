pub mod dao;

use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use std::path::Path;
use std::str::FromStr;

/// Open (or create) a SQLite database at the given path and run migrations.
pub async fn open_db(path: &Path, verbose: bool) -> Result<SqlitePool, sqlx::Error> {
    let is_new = !path.exists();
    if verbose {
        if is_new {
            eprintln!("creating new database: {}", path.display());
        } else {
            eprintln!("using database: {}", path.display());
        }
    }

    let url = format!("sqlite:{}?mode=rwc", path.display());
    let options = SqliteConnectOptions::from_str(&url)?
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
        .foreign_keys(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await?;
    migrate(&pool, verbose).await?;
    Ok(pool)
}

/// Checkpoint the WAL and close the database pool cleanly.
///
/// This removes the `-wal` and `-shm` files, leaving only the single `.db` file.
pub async fn close_db(pool: SqlitePool) {
    sqlx::raw_sql("PRAGMA wal_checkpoint(TRUNCATE)")
        .execute(&pool)
        .await
        .ok();
    pool.close().await;
}

/// Open an in-memory SQLite database for testing.
pub async fn open_memory() -> Result<SqlitePool, sqlx::Error> {
    let options = SqliteConnectOptions::from_str("sqlite::memory:")?
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
        .foreign_keys(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await?;
    migrate(&pool, false).await?;
    Ok(pool)
}

async fn migrate(pool: &SqlitePool, verbose: bool) -> Result<(), sqlx::Error> {
    let migrator = sqlx::migrate!();

    if verbose {
        // Check which migrations still need to be applied
        let applied: Vec<(i64,)> =
            sqlx::query_as("SELECT version FROM _sqlx_migrations ORDER BY version")
                .fetch_all(pool)
                .await
                .unwrap_or_default();
        let applied_versions: std::collections::HashSet<i64> =
            applied.into_iter().map(|r| r.0).collect();

        let pending: Vec<_> = migrator
            .iter()
            .filter(|m| !applied_versions.contains(&m.version))
            .collect();

        if !pending.is_empty() {
            eprintln!("applying {} database migration(s):", pending.len());
            for m in &pending {
                eprintln!("  {}", m.description);
            }
        }
    }

    migrator.run(pool).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn open_memory_creates_tables() {
        let pool = open_memory().await.unwrap();
        // Verify tables exist by querying sqlite_master
        let tables: Vec<(String,)> =
            sqlx::query_as("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
                .fetch_all(&pool)
                .await
                .unwrap();
        let names: Vec<&str> = tables.iter().map(|t| t.0.as_str()).collect();
        assert!(names.contains(&"scans"));
        assert!(names.contains(&"directories"));
        assert!(names.contains(&"files"));
    }

    #[tokio::test]
    async fn open_memory_twice_is_idempotent() {
        let pool = open_memory().await.unwrap();
        // Running migrate again should not fail — sqlx tracks applied migrations
        migrate(&pool, false).await.unwrap();

        let tables: Vec<(String,)> =
            sqlx::query_as("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
                .fetch_all(&pool)
                .await
                .unwrap();
        assert!(!tables.is_empty());
    }

    #[tokio::test]
    async fn reopening_existing_db_does_not_fail() {
        // Regression test: opening a DB that already has all migrations applied
        // must not fail with "duplicate column name" or similar errors.
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("test.db");

        // First open — creates and migrates
        {
            let pool = open_db(&db_path, false).await.unwrap();
            // Insert some data to prove it's functional
            sqlx::raw_sql("INSERT INTO scans (run_type, root_path, started_at) VALUES ('test', '/tmp', '2026-01-01T00:00:00Z')")
                .execute(&pool)
                .await
                .unwrap();
            close_db(pool).await;
        }

        // Second open — must not fail on migrations
        {
            let pool = open_db(&db_path, false).await.unwrap();
            // Verify previous data survived
            let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM scans")
                .fetch_one(&pool)
                .await
                .unwrap();
            assert_eq!(count.0, 1);
            close_db(pool).await;
        }
    }
}
