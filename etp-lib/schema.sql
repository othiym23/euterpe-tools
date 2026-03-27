-- Clean schema for euterpe-tools database.
--
-- This file is the canonical reference for the current database schema.
-- It is used to create new databases in a single transaction, bypassing
-- the incremental migration path.
--
-- IMPORTANT: This file must be kept in sync with the migrations in
-- migrations/. A test verifies that both paths produce identical schemas.

CREATE TABLE IF NOT EXISTS scans (
    id          INTEGER PRIMARY KEY,
    run_type    TEXT NOT NULL UNIQUE,
    root_path   TEXT NOT NULL,
    started_at  TEXT NOT NULL,
    finished_at TEXT
);

CREATE TABLE IF NOT EXISTS directories (
    id       INTEGER PRIMARY KEY,
    scan_id  INTEGER NOT NULL REFERENCES scans(id) ON DELETE RESTRICT,
    path     TEXT NOT NULL,
    mtime    INTEGER NOT NULL,
    size     INTEGER NOT NULL DEFAULT 0,
    UNIQUE(scan_id, path)
);

CREATE TABLE IF NOT EXISTS files (
    id                  INTEGER PRIMARY KEY,
    dir_id              INTEGER NOT NULL REFERENCES directories(id) ON DELETE RESTRICT,
    filename            TEXT NOT NULL,
    size                INTEGER NOT NULL,
    ctime               INTEGER NOT NULL,
    mtime               INTEGER NOT NULL,
    metadata_scanned_at TEXT,
    UNIQUE(dir_id, filename)
);

-- Covering index for the list_files JOIN: lets SQLite satisfy
-- SELECT f.filename, f.size, f.ctime, f.mtime ... WHERE f.dir_id = ?
-- entirely from the index without touching the main table.
CREATE INDEX IF NOT EXISTS idx_files_dir_covering
    ON files(dir_id, filename, size, ctime, mtime);

-- Index for finding files needing metadata scan.
CREATE INDEX IF NOT EXISTS idx_files_metadata_scan
    ON files(mtime, metadata_scanned_at);

-- Content-addressable blob storage for embedded images.
CREATE TABLE IF NOT EXISTS blobs (
    hash      TEXT PRIMARY KEY,
    size      INTEGER NOT NULL,
    ref_count INTEGER NOT NULL DEFAULT 1
);

-- Tag metadata, one row per (file, tag_name) pair.
-- Multi-value tags are stored as JSON arrays.
CREATE TABLE IF NOT EXISTS metadata (
    id        INTEGER PRIMARY KEY,
    file_id   INTEGER NOT NULL REFERENCES files(id) ON DELETE RESTRICT,
    tag_name  TEXT NOT NULL,
    value     TEXT NOT NULL,
    UNIQUE(file_id, tag_name)
);

-- Cue sheets (embedded in audio files or standalone companion files).
CREATE TABLE IF NOT EXISTS cue_sheets (
    id       INTEGER PRIMARY KEY,
    file_id  INTEGER NOT NULL REFERENCES files(id) ON DELETE RESTRICT,
    source   TEXT NOT NULL,
    content  TEXT NOT NULL,
    UNIQUE(file_id, source)
);

-- Embedded image references. Actual image data stored in CAS (blobs table
-- tracks hashes and ref counts; filesystem stores the bytes).
CREATE TABLE IF NOT EXISTS embedded_images (
    id         INTEGER PRIMARY KEY,
    file_id    INTEGER NOT NULL REFERENCES files(id) ON DELETE RESTRICT,
    image_type TEXT NOT NULL,
    mime_type  TEXT NOT NULL,
    blob_hash  TEXT NOT NULL REFERENCES blobs(hash) ON DELETE RESTRICT,
    width      INTEGER,
    height     INTEGER,
    UNIQUE(file_id, image_type)
);
