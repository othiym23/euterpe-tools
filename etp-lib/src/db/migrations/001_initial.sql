CREATE TABLE IF NOT EXISTS scans (
    id          INTEGER PRIMARY KEY,
    run_type    TEXT NOT NULL UNIQUE,
    root_path   TEXT NOT NULL,
    started_at  TEXT NOT NULL,
    finished_at TEXT
);

CREATE TABLE IF NOT EXISTS directories (
    id       INTEGER PRIMARY KEY,
    scan_id  INTEGER NOT NULL REFERENCES scans(id),
    path     TEXT NOT NULL,
    mtime    INTEGER NOT NULL,
    UNIQUE(scan_id, path)
);

CREATE TABLE IF NOT EXISTS files (
    id       INTEGER PRIMARY KEY,
    dir_id   INTEGER NOT NULL REFERENCES directories(id) ON DELETE RESTRICT,
    filename TEXT NOT NULL,
    size     INTEGER NOT NULL,
    ctime    INTEGER NOT NULL,
    mtime    INTEGER NOT NULL,
    UNIQUE(dir_id, filename)
);
