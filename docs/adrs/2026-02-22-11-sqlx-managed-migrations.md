# Use sqlx managed migrations

Date: 2026-02-22

## Status

Accepted

## Context

The initial database implementation (SP1.2) ran migration SQL files manually via
`sqlx::raw_sql()` on every database open. This worked for
`CREATE TABLE IF NOT EXISTS` statements, which are inherently idempotent.
However, when SP1.3 added migration 002
(`ALTER TABLE directories ADD COLUMN size`), reopening an existing database
failed with "duplicate column name: size" because `ALTER TABLE ADD COLUMN` is
not idempotent and there was no tracking of which migrations had already been
applied.

## Decision

Use sqlx's built-in `migrate!()` macro and `Migrator`, which:

- Embeds migration SQL files at compile time from `etp-lib/migrations/`
- Creates a `_sqlx_migrations` tracking table in the database
- Records each migration's version, description, and checksum after it runs
- Only applies migrations that have not yet been recorded

Migration files are moved from `etp-lib/src/db/migrations/` to
`etp-lib/migrations/` (sqlx's default location relative to
`CARGO_MANIFEST_DIR`). The `"migrate"` feature is enabled on the sqlx
dependency.

## Consequences

- Database opens are idempotent regardless of migration content — `ALTER TABLE`,
  `CREATE INDEX`, and other non-idempotent DDL statements are safe to use.
- Adding a new migration is a simple matter of adding a new numbered SQL file to
  `etp-lib/migrations/`.
- The `_sqlx_migrations` table is added to every database. This is a standard
  pattern and does not affect application queries.
- Modifying a migration file after it has been applied to a database will cause
  a checksum mismatch error on next open. Migrations should be treated as
  immutable once merged.
