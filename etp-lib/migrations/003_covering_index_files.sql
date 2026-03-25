-- Covering index for the list_files JOIN: lets SQLite satisfy
-- SELECT f.filename, f.size, f.ctime, f.mtime ... WHERE f.dir_id = ?
-- entirely from the index without touching the main table.
CREATE INDEX IF NOT EXISTS idx_files_dir_covering
    ON files(dir_id, filename, size, ctime, mtime);
