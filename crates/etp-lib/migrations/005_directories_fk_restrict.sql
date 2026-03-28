-- Add ON DELETE RESTRICT to directories.scan_id foreign key.
-- SQLite doesn't support ALTER TABLE to modify constraints, so we must
-- recreate the table. FK checks are disabled at the connection level
-- during migration (see db/mod.rs) since files.dir_id references
-- directories(id).

CREATE TABLE directories_new (
    id       INTEGER PRIMARY KEY,
    scan_id  INTEGER NOT NULL REFERENCES scans(id) ON DELETE RESTRICT,
    path     TEXT NOT NULL,
    mtime    INTEGER NOT NULL,
    size     INTEGER NOT NULL DEFAULT 0,
    UNIQUE(scan_id, path)
);

INSERT INTO directories_new SELECT * FROM directories;
DROP TABLE directories;
ALTER TABLE directories_new RENAME TO directories;
