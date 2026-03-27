# Clean Schema Alongside Migrations

- **Status**: Accepted
- **Date**: 2026-03-27

## Context

The project uses sqlx's `migrate!()` macro for incremental database schema
changes. As the number of migrations grows, new databases must replay every
historical migration (including ALTERs, backfills, and index additions) even
though the end result is always the same final schema. This has several
downsides:

1. **Clarity**: The current schema is spread across multiple migration files.
   Understanding the full table structure requires mentally replaying all
   migrations in order.
2. **Performance**: Each migration is a separate transaction with its own fsync.
   On spinning disks (NAS deployment), creating a new database becomes
   measurably slower as migrations accumulate.
3. **SQLite ALTER limitations**: SQLite's ALTER TABLE support is limited. Some
   schema changes that are trivial in the clean schema (e.g., adding NOT NULL
   columns with defaults) require multi-step workarounds in migrations.

## Decision

Maintain a clean schema file (`etp-lib/schema.sql`) that represents the current
database schema as a single, readable SQL file. This file is the canonical
reference for the current state of the database.

When creating a **new** database:

1. Execute `schema.sql` directly (single transaction, one fsync).
2. Record all known migrations as applied in sqlx's `_sqlx_migrations` table so
   that future `migrate!()` calls are no-ops.

When opening an **existing** database:

1. Run `migrate!()` as before — sqlx applies only pending migrations.

The clean schema must be kept in sync with the migrations. Whenever a new
migration is added, the developer must also update `schema.sql` to reflect the
post-migration state. CI can verify this by creating a database via each path
and comparing the resulting schemas.

## Consequences

- **Positive**: New database creation is a single DDL transaction. The full
  schema is readable in one file. SQLite ALTER workarounds are only needed in
  migrations for existing databases.
- **Positive**: Existing databases continue to upgrade incrementally via
  migrations — no data loss or rebuild required.
- **Negative**: Two representations of the schema must be kept in sync. A desync
  means new databases and migrated databases have different schemas.
- **Mitigation**: A test compares the schema produced by both paths (clean
  schema vs. sequential migrations) and fails if they diverge.
