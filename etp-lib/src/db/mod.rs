pub mod dao;

use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use std::path::Path;
use std::str::FromStr;

/// The clean schema SQL, used to create new databases in a single transaction.
const CLEAN_SCHEMA: &str = include_str!("../../schema.sql");

/// Open (or create) a SQLite database at the given path and run migrations.
///
/// For new databases, the clean schema is applied directly (single transaction)
/// and all migrations are recorded as applied. For existing databases, pending
/// migrations are applied incrementally.
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
        .synchronous(sqlx::sqlite::SqliteSynchronous::Normal)
        .foreign_keys(true)
        .pragma("cache_size", "-64000") // 64 MiB page cache (negative = KiB)
        .pragma("temp_store", "MEMORY"); // temp tables/indexes in memory
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await?;

    if is_new {
        init_from_schema(&pool, verbose).await?;
    } else {
        migrate(&pool, verbose).await?;
    }
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

/// Create a new database from the clean schema and record all migrations as
/// applied so that future `migrate!()` calls are no-ops.
async fn init_from_schema(pool: &SqlitePool, verbose: bool) -> Result<(), sqlx::Error> {
    if verbose {
        eprintln!("initializing from clean schema");
    }

    let mut tx = pool.begin().await?;

    // Apply the full schema in one shot
    sqlx::raw_sql(CLEAN_SCHEMA).execute(&mut *tx).await?;

    // Record each migration as already applied so incremental migration
    // works correctly if the database is opened again later.
    //
    // The _sqlx_migrations DDL matches sqlx 0.8's internal schema. The
    // clean_schema_matches_migrations test catches any drift on upgrade.
    let migrator = sqlx::migrate!();
    sqlx::raw_sql(
        "CREATE TABLE IF NOT EXISTS _sqlx_migrations (
            version BIGINT PRIMARY KEY,
            description TEXT NOT NULL,
            installed_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            success BOOLEAN NOT NULL,
            checksum BLOB NOT NULL,
            execution_time BIGINT NOT NULL
        )",
    )
    .execute(&mut *tx)
    .await?;

    for m in migrator.iter() {
        sqlx::query(
            "INSERT INTO _sqlx_migrations (version, description, success, checksum, execution_time)
             VALUES (?, ?, TRUE, ?, 0)",
        )
        .bind(m.version)
        .bind(&*m.description)
        .bind(&*m.checksum)
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;
    Ok(())
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

    /// Query PRAGMA table_info for a table and return a sorted, normalized
    /// representation: Vec of (column_name, type, notnull, default, pk).
    async fn table_columns(pool: &SqlitePool, table: &str) -> Vec<String> {
        let rows: Vec<(i64, String, String, i64, Option<String>, i64)> =
            sqlx::query_as(&format!("PRAGMA table_info('{table}')"))
                .fetch_all(pool)
                .await
                .unwrap();
        rows.into_iter()
            .map(|(_, name, typ, notnull, dflt, pk)| {
                format!(
                    "{name} {typ} notnull={notnull} default={} pk={pk}",
                    dflt.unwrap_or_else(|| "NULL".into())
                )
            })
            .collect()
    }

    #[tokio::test]
    async fn clean_schema_matches_migrations() {
        // Create a DB via migrations (the existing path)
        let migrated = open_memory().await.unwrap();

        // Create a DB via clean schema
        let options = SqliteConnectOptions::from_str("sqlite::memory:")
            .unwrap()
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            .foreign_keys(true);
        let clean = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .unwrap();
        init_from_schema(&clean, false).await.unwrap();

        // Compare tables present in each DB
        let table_query = "SELECT name FROM sqlite_master \
                           WHERE type = 'table' \
                           AND name NOT LIKE '_sqlx%' \
                           AND name NOT LIKE 'sqlite_%' \
                           ORDER BY name";
        let migrated_tables: Vec<(String,)> = sqlx::query_as(table_query)
            .fetch_all(&migrated)
            .await
            .unwrap();
        let clean_tables: Vec<(String,)> =
            sqlx::query_as(table_query).fetch_all(&clean).await.unwrap();
        assert_eq!(
            migrated_tables, clean_tables,
            "different tables in migrated vs clean schema"
        );

        // Compare columns in each table via PRAGMA table_info (immune to
        // whitespace differences in sqlite_master.sql from ALTER TABLE)
        for (table,) in &migrated_tables {
            let migrated_cols = table_columns(&migrated, table).await;
            let clean_cols = table_columns(&clean, table).await;
            assert_eq!(
                migrated_cols, clean_cols,
                "column mismatch in table {table}"
            );
        }

        // Compare indexes
        let index_query = "SELECT name, tbl_name, sql FROM sqlite_master \
                           WHERE type = 'index' \
                           AND name NOT LIKE 'sqlite_%' \
                           AND sql IS NOT NULL \
                           ORDER BY name";
        let migrated_indexes: Vec<(String, String, String)> = sqlx::query_as(index_query)
            .fetch_all(&migrated)
            .await
            .unwrap();
        let clean_indexes: Vec<(String, String, String)> =
            sqlx::query_as(index_query).fetch_all(&clean).await.unwrap();
        assert_eq!(
            migrated_indexes, clean_indexes,
            "index mismatch between migrated and clean schema"
        );

        // Verify migrations are recorded in the clean-schema DB
        let migration_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM _sqlx_migrations")
            .fetch_one(&clean)
            .await
            .unwrap();
        let migrator = sqlx::migrate!();
        assert_eq!(migration_count.0, migrator.iter().count() as i64);
    }
}
