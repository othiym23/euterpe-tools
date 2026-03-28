-- Track when metadata was last scanned for incremental re-scan.
ALTER TABLE files ADD COLUMN metadata_scanned_at TEXT;

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

-- Index for finding files needing metadata scan.
CREATE INDEX IF NOT EXISTS idx_files_metadata_scan
    ON files(mtime, metadata_scanned_at);
