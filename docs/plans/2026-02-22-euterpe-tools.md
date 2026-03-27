# euterpe-tools: Large-Scale Media Metadata Management

## Context

The existing `caching-scanners` project provides incremental filesystem scanning
with CSV and tree output, deployed to a Synology NAS (Btrfs, spinning disks,
RAID 6 with two parity disks, DSM 7.3). It works well for its current scope, but
the next step is a queryable metadata database that can drive quality-checking,
bulk tag normalization, and large-scale media management across a 220K+ file
collection.

This plan transforms `caching-scanners` into `euterpe-tools` — a toolkit of
composable plumbing commands (Rust) with Python porcelain, centered on a SQLite
database that replaces the current rkyv state files. Eventually the database
will be split off into `euterpe-db`, and `euterpe-tools` will be dedicated to
the file management and things that involve maintaining the physical files in
the music collection.

## Current State (as of 2026-02-21)

The binary consolidation is complete (commit `70599bb`). The project has:

- **One binary**: `dir-tree-scanner` with `csv` and `tree` subcommands
- **Library modules**: `ops.rs` (shared operations), `scanner.rs`, `state.rs`,
  `csv_writer.rs`, `tree.rs`
- **7 trycmd snapshot tests** + unit tests across modules
- **Version 0.0.4**, trycmd 1.0, brotli 8, clap 4.5.60
- **Branch protection** on `main` — all changes via feature branch + PR
- CSV uses byte-order sorting; tree uses ICU4X collation

## Decisions (confirmed)

| Area            | Choice                                     | Rationale                                                   |
| --------------- | ------------------------------------------ | ----------------------------------------------------------- |
| Database driver | sqlx (async, SQLite + PostgreSQL)          | Eventual PG migration; tokio likely needed anyway           |
| Migrations      | sqlx built-in                              | Single tool, works for both SQLite and PG                   |
| Audio metadata  | lofty (primary) + TagLib FFI (DSF/WMA/MKA) | lofty covers 95%+ formats; TagLib for gaps + safe writes    |
| Scripting       | mlua + LuaJIT (vendored)                   | Embedded in Rust for metadata transforms                    |
| CAS             | Filesystem, BLAKE3 hashing                 | No DB bloat; safe on Btrfs; write blob before reference     |
| XDG paths       | etcetera + fallback helper                 | Native-first on macOS, XDG fallback; strict XDG on Linux    |
| Config format   | KDL with knuffel crate                     | Better nesting for growing config complexity; replaces TOML |
| Porcelain       | Python                                     | Orchestration, workflow composition, interactive use        |
| Plumbing        | Rust binaries over shared `etp-lib`        | Like libgit + git plumbing commands                         |

## Workspace Structure

```txt
euterpe-tools/                      # Cargo workspace root (same repo, preserved history)
├── Cargo.toml                      # workspace manifest
├── etp-lib/                        # Library crate — ALL shared functionality
│   └── src/
│       ├── lib.rs                  # Re-exports all modules
│       ├── scanner.rs              # Incremental filesystem scanner (existing)
│       ├── state.rs                # rkyv state (kept during SP1 transition only)
│       ├── csv_writer.rs           # CSV output (existing)
│       ├── tree.rs                 # Tree rendering with ICU4X collation (existing)
│       ├── ops.rs                  # Shared CLI operations (existing, evolves)
│       ├── paths.rs                # XDG/native path resolution (etcetera)
│       ├── config.rs               # KDL config loading (knuffel)
│       ├── db/
│       │   ├── mod.rs              # Connection factory, re-exports
│       │   ├── dao.rs              # Concrete query functions (data mapper pattern)
│       │   ├── import.rs           # One-time rkyv state → DB migration
│       │   └── migrations/         # sqlx migration SQL files
│       ├── metadata/
│       │   ├── mod.rs
│       │   ├── reader.rs           # lofty-first, TagLib fallback
│       │   ├── writer.rs           # Safe metadata writing (SP3)
│       │   ├── formats.rs          # Extension → format mapping
│       │   ├── taglib_ffi.rs       # TagLib C FFI (feature-gated)
│       │   ├── images.rs           # Embedded image extraction → CAS
│       │   ├── cue.rs              # Cue sheet parsing
│       │   └── safety.rs           # Pre-write validation (SP3)
│       ├── cas/
│       │   └── mod.rs              # store_blob, get_blob, gc
│       └── scripting/
│           ├── mod.rs              # Lua VM setup
│           ├── api.rs              # Lua ↔ Rust bridge
│           └── runner.rs           # Batch execution, change coalescing
├── etp-csv/                        # Plumbing: scan → DB → CSV
│   └── src/main.rs
├── etp-tree/                       # Plumbing: scan → DB → tree
│   └── src/main.rs
├── etp-meta/                       # Plumbing: metadata scan/read/write/check
│   └── src/main.rs
├── etp-query/                      # Plumbing: ad-hoc DB queries
│   └── src/main.rs
├── etp-cas/                        # Plumbing: CAS blob operations
│   └── src/main.rs
├── etp/                            # Python porcelain
│   ├── etp                         # Entry point, Git-style subcommand dispatch
│   └── etp-catalog                 # Evolved from catalog-nas.py
├── conf/
│   ├── catalog.kdl                 # User config (KDL format, moved from scripts/)
│   └── catalog.default             # Template config
├── scripts/
│   └── catalog-nas.sh              # Legacy reference
├── docs/plans/
└── tests/                          # trycmd snapshot tests (workspace root)
```

XDG paths (app name: `euterpe-tools`):

- Config: `$XDG_CONFIG_HOME/euterpe-tools/config.kdl` or
  `~/Library/Application Support/net.aoaioxxysz.etp/config.kdl`
- Data: `$XDG_DATA_HOME/euterpe-tools/metadata.sqlite` or
  `~/Library/Application Support/net.aoaioxxysz.etp/`
- CAS: `$XDG_DATA_HOME/euterpe-tools/assets/{ab}/{abcdef...}`

## Config Format (KDL)

Replaces `catalog.toml`. Uses the `knuffel` crate for deserialization into Rust
structs via `#[derive(Decode)]`.

```kdl
global {
    scanner "$HOME/bin/etp-csv"
    tree "$HOME/bin/etp-tree"
    home-base "/volume1/data/downloads/(music)"
    trees-path "{home-base}/catalogs/trees"
    csvs-path "{trees-path}/csv"
    state-path "{trees-path}/state"
}

scan "music" {
    mode "subs"
    disk "/volume1/music"
    desc "euterpe music (NAS volume)"
    header "Synology NAS //music"
}

scan "television" {
    mode "df"
    disk "/volume1/data/video/Television"
    desc "euterpe television (NAS directory)"
    header "Synology NAS //data/video/Television share"
}

// Disabled scans use slashdash to comment out the entire node
/- scan "laptop-music" {
    mode "used"
    disk "/Users/ogd/Downloads/music"
    desc "laptop music directory"
    header "local music processing directory"
}
```

The `enabled` boolean field is replaced by KDL's slashdash (`/-`) comment
syntax, which comments out an entire node. This is more idiomatic KDL.

## Database Schema

The `run_type` column in `scans` is the partition key — it maps to the scan name
argument (e.g., `scan "music"` in config). The file path + run type combination
has a uniqueness constraint. Surrogate keys are used throughout for join
performance.

**Relative paths**: `directories.path` stores paths relative to
`scans.root_path`. This deduplicates the common prefix across all rows, makes
libraries relocatable (update one row in `scans` to move a collection), and has
no impact on CSV or tree output — the full path is reconstructed by joining
`root_path` + `path` at render time.

```sql
-- SP1: filesystem scanning
CREATE TABLE scans (
    id          INTEGER PRIMARY KEY,
    run_type    TEXT NOT NULL UNIQUE,  -- partition key, e.g. "music", "television"
    root_path   TEXT NOT NULL,         -- absolute path to scan root
    started_at  TEXT NOT NULL,         -- ISO 8601
    finished_at TEXT
);

CREATE TABLE directories (
    id       INTEGER PRIMARY KEY,
    scan_id  INTEGER NOT NULL REFERENCES scans(id) ON DELETE RESTRICT,
    path     TEXT NOT NULL,            -- relative to scans.root_path
    mtime    INTEGER NOT NULL,
    size     INTEGER NOT NULL DEFAULT 0,
    UNIQUE(scan_id, path)
);

CREATE TABLE files (
    id                  INTEGER PRIMARY KEY,
    dir_id              INTEGER NOT NULL REFERENCES directories(id) ON DELETE RESTRICT,
    filename            TEXT NOT NULL,
    size                INTEGER NOT NULL,
    ctime               INTEGER NOT NULL,
    mtime               INTEGER NOT NULL,
    metadata_scanned_at TEXT,          -- NULL = needs scan; cleared when mtime changes
    UNIQUE(dir_id, filename)
);

-- SP2: metadata (all FKs use ON DELETE RESTRICT per ADR; see also
-- 2026-03-27-03-upsert-file-sync.md for the orphan cleanup design)
CREATE TABLE blobs (
    hash      TEXT PRIMARY KEY,        -- BLAKE3 hex
    size      INTEGER NOT NULL,
    ref_count INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE metadata (
    id        INTEGER PRIMARY KEY,
    file_id   INTEGER NOT NULL REFERENCES files(id) ON DELETE RESTRICT,
    tag_name  TEXT NOT NULL,            -- normalized lowercase_snake_case
    value     TEXT NOT NULL,            -- JSON: scalar or array for multi-value
    UNIQUE(file_id, tag_name)
);

CREATE TABLE cue_sheets (
    id       INTEGER PRIMARY KEY,
    file_id  INTEGER NOT NULL REFERENCES files(id) ON DELETE RESTRICT,
    source   TEXT NOT NULL,             -- 'embedded' or 'standalone'
    content  TEXT NOT NULL,
    UNIQUE(file_id, source)
);

CREATE TABLE embedded_images (
    id         INTEGER PRIMARY KEY,
    file_id    INTEGER NOT NULL REFERENCES files(id) ON DELETE RESTRICT,
    image_type TEXT NOT NULL,            -- 'front_cover', 'back_cover', etc.
    mime_type  TEXT NOT NULL,
    blob_hash  TEXT NOT NULL REFERENCES blobs(hash) ON DELETE RESTRICT,
    width      INTEGER,
    height     INTEGER,
    UNIQUE(file_id, image_type)
);
```

Portability: all types map directly to PostgreSQL (TEXT, BIGINT, INTEGER). No
SQLite-specific syntax. Timestamps as ISO 8601 text for human-facing values;
Unix epoch integers for filesystem-derived values (ctime, mtime).

---

## Subproject 1: SQL Database Foundation

### SP1.1: Workspace Restructuring

Convert single-crate project to Cargo workspace. Move library code into
`etp-lib/`, split `dir-tree-scanner` into `etp-csv/` and `etp-tree/` plumbing
binaries.

**Files moved** (preserving content):

- `src/lib.rs` → `etp-lib/src/lib.rs`
- `src/scanner.rs` → `etp-lib/src/scanner.rs`
- `src/state.rs` → `etp-lib/src/state.rs`
- `src/csv_writer.rs` → `etp-lib/src/csv_writer.rs`
- `src/tree.rs` → `etp-lib/src/tree.rs`
- `src/ops.rs` → `etp-lib/src/ops.rs`

**Files created**:

- Root `Cargo.toml` — workspace manifest
- `etp-lib/Cargo.toml` — library deps (clap, rkyv, csv, glob, walkdir, brotli,
  icu_collator)
- `etp-csv/Cargo.toml` + `etp-csv/src/main.rs` — CSV plumbing (extracts
  `run_csv` from `dir_tree_scanner.rs`)
- `etp-tree/Cargo.toml` + `etp-tree/src/main.rs` — tree plumbing (extracts
  `run_tree` from `dir_tree_scanner.rs`)

**Files removed**:

- `src/bin/dir_tree_scanner.rs` — split into `etp-csv` and `etp-tree`
- `build.rs` — version embedding moves to workspace level

**Done when**: `cargo test --workspace` passes all existing unit + 7 trycmd
tests (updated to reference `etp-csv` and `etp-tree` binary names). Both
binaries produce identical output to current `dir-tree-scanner csv` and
`dir-tree-scanner tree`.

### SP1.2: SQLite Database Layer

Add sqlx + tokio to `etp-lib`. Create `db/` module with migrations and DAO.

**Files created**:

- `etp-lib/src/db/mod.rs` — connection factory, `open_db(path)` helper
- `etp-lib/src/db/dao.rs` — concrete query functions (see below)
- `etp-lib/src/db/migrations/001_initial.sql` — `scans`, `directories`, `files`
- `etp-lib/src/paths.rs` — XDG/native path resolution (etcetera)
- `etp-lib/src/config.rs` — KDL config loading (knuffel)

**New dependencies** in `etp-lib/Cargo.toml`:

- `sqlx` (features: `runtime-tokio`, `sqlite`)
- `tokio` (features: `rt`, `macros`) — single-threaded, not `rt-multi-thread`
- `etcetera`
- `knuffel` — KDL config deserialization

**Key DAO functions** — the shared query path that both CSV and tree use:

```rust
// Write path (scanner)
pub async fn upsert_scan(pool, run_type, root_path) -> Result<i64>
pub async fn upsert_directory(pool, scan_id, path, mtime) -> Result<i64>
pub async fn replace_files(pool, dir_id, files) -> Result<()>
pub async fn remove_stale_directories(pool, scan_id, seen) -> Result<usize>
pub async fn directory_mtime(pool, scan_id, path) -> Result<Option<i64>>

// Read path (SHARED — both etp-csv and etp-tree call these)
pub async fn list_files(pool, scan_id) -> Result<Vec<FileRecord>>
pub async fn total_size(pool, scan_id) -> Result<u64>
```

`list_files` returns `Vec<FileRecord>` where
`FileRecord { dir_path, filename, size, ctime, mtime }`. The `dir_path` is the
**full path** (root_path + relative path joined in the query or DAO layer), so
downstream consumers don't need to know about the relative storage. Both
`csv_writer` and `tree` receive this same data and apply their own sorting in
Rust (byte-order for CSV, ICU4X for tree) to guarantee byte-identical output.

**Done when**: DAO functions work against in-memory SQLite. Unit tests for all
DAO operations. `paths.rs` resolves XDG/native correctly. KDL config loads and
deserializes.

### SP1.3: DB-Backed Output (byte-identical gate)

Rewire `etp-csv` and `etp-tree` to: scan → write to DB → query DB → produce
output.

**Files modified**:

- `etp-lib/src/scanner.rs` — add
  `scan_to_db(root, pool, run_type, exclude, verbose)` alongside existing
  `scan()`
- `etp-lib/src/csv_writer.rs` — add `write_csv_from_db(pool, scan_id, output)`
  that calls `dao::list_files` then applies byte-order sorting identically to
  current `write_csv`
- `etp-lib/src/tree.rs` — add
  `render_tree_from_db(pool, scan_id, root, patterns, no_escape, show_hidden)`
  that calls `dao::list_files` then renders with ICU4X collation identically
- `etp-csv/src/main.rs` — use tokio runtime, call `scan_to_db` →
  `write_csv_from_db`
- `etp-tree/src/main.rs` — same pattern

**Hard requirement**: existing trycmd snapshot tests pass with zero changes to
expected output. This is the regression gate. Additionally, a new integration
test scans the same fixture with both old (ScanState) and new (DB) paths and
diffs the output.

**Done when**: all 7 trycmd tests pass unchanged. New integration diff test
passes.

### SP1.4: State Migration and Cleanup

**Files created**:

- `etp-lib/src/db/import.rs` —
  `import_state_file(pool, state_path, run_type, root)` reads rkyv state,
  inserts into DB

**Files modified**:

- `etp-lib/src/ops.rs` — replace `load_state`/`save_state` with
  `open_db`/`ensure_scan`
- Binary `main.rs` files — remove `--state` args, add `--db` (defaults to XDG
  path)

**Files removed** (after import tool verified):

- `etp-lib/src/state.rs` — move behind a `migration` feature flag, then remove
  entirely
- rkyv, brotli dependencies (behind feature flag, then removed)

**Done when**: `etp-csv` and `etp-tree` no longer reference state files. Import
tool migrates existing `.fsscan.state` data into the DB. `total_size` query
replaces `du -sm` calls.

### SP1.5: Python Porcelain

**Files created**:

- `etp/etp` — Python entry point. Discovers `etp-*` binaries on `$PATH`,
  dispatches `etp <cmd> ...` → `etp-<cmd> ...`
- `etp/etp-catalog` — evolved from `scripts/catalog-nas.py`. Calls `etp csv` and
  `etp tree` per configured scan. Uses `etp query size` instead of `du -sm`.
  Keeps `df -PH` calls (cheap syscall). Reads KDL config.
- `conf/catalog.kdl` — config converted from TOML to KDL
- `conf/catalog.default` — template config in KDL

**Files modified**:

- `justfile` — deploy recipe updated for new layout and binary names

**Done when**: `etp csv`, `etp tree`, `etp catalog --dry-run` all work. Deploy
to NAS succeeds. pytest tests adapted from `test_catalog.py`.

---

## Subproject 2: Metadata Annotation

### SP2.1: Metadata Reading (lofty)

Read audio metadata using `lofty` crate. Supported formats: MP3, FLAC, OGG,
Opus, WAV, M4A/AAC, APE. Tag names normalized to `lowercase_snake_case`.
Multi-value frames combined into JSON arrays, preserving order within same-type
frames. Other frame types: order not preserved.

Add `etp-meta` binary: `etp meta scan <dir>`, `etp meta read <file>`.

Incremental strategy: compare `files.mtime` against last metadata scan. Only
re-read files whose mtime changed. Process files in directory order for
sequential I/O on spinning disks.

Migration: `etp-lib/src/db/migrations/002_metadata.sql` adds `metadata`,
`cue_sheets`, `blobs`, `embedded_images` tables.

**Done when**: `etp meta scan` reads and stores tags for all lofty-supported
formats. `etp meta read` dumps tags for a single file. Incremental re-scan skips
unchanged files.

### SP2.2: TagLib FFI (feature-gated)

FFI bindings to TagLib C API for DSF, WMA, MKA. Feature-gated: `etp-lib` gets a
`taglib` cargo feature. Dispatch in `reader.rs`: try lofty first, fall back to
TagLib for unrecognized formats.

Static linking concern: TagLib is C++. May need vendored build for musl, or
accept dynamic linking on the NAS for this feature. lofty-only path covers 95%+
of formats and always works statically.

**Done when**: DSF, WMA, MKA metadata read and stored. Feature gate documented.

### SP2.3: CAS CLI (reduced scope)

**Note**: The CAS library (`cas.rs`) and embedded image extraction were
implemented in SP2.1. The `blobs` and `embedded_images` tables exist, images are
extracted during metadata scan, and `gc_orphan_blobs` handles cleanup. What
remains is the `etp-cas` CLI binary for manual blob operations.

Add `etp-cas` binary: `etp cas store`, `etp cas get`, `etp cas gc`.

**Done when**: CLI binary provides user-facing CAS operations.

### SP2.4: Cue Sheet Parsing (reduced scope)

**Note**: Embedded cue sheet detection and storage were implemented in SP2.1.
The `cue_sheets` table exists and embedded FLAC CUESHEET vorbis comments are
extracted during metadata scan. What remains is standalone `.cue` file detection
and deeper content parsing (track indices, etc.).

Parse standalone `.cue` files (alongside audio). Optionally parse cue sheet
content for track-level metadata.

**Done when**: standalone cue sheets detected and stored.

### SP2.5: Query Interface

`etp-query` binary (or `etp query` via porcelain):

```bash
etp query files <directory>                 # list files
etp query tags <file>                       # show all tags
etp query find --tag artist --value "X"     # find by tag
etp query stats                             # counts by format, total size
etp query size <directory>                  # SUM(size), replaces du -sm
etp query sql "WHERE ..."                   # sanitized WHERE pass-through
```

The `sql` subcommand sanitizes and passes a WHERE clause to the SQL engine,
providing a raw query escape hatch.

**Done when**: all query subcommands work. `etp query size` replaces `du -sm` in
`etp-catalog`.

### SP2.6: File-Move Tracking

When a file is moved or renamed, the current UPSERT design treats it as a
deletion from the old location and an insertion at the new one. The old
`files.id` (and all associated metadata, images, cue sheets) is lost, forcing a
full metadata re-read of the "new" file.

File-move tracking detects these moves during a filesystem scan and updates the
file's `dir_id` and/or `filename` instead of deleting and re-creating the row.
This preserves the `files.id` and all dependent metadata.

**Detection strategy**: When files disappear from one directory and appear in
another during the same scan, match them by content fingerprint. Candidates:

- **Size + mtime**: fast (no I/O beyond stat), but not unique — multiple files
  can share size and mtime.
- **Partial content hash**: hash the first N bytes. Requires reading the file
  but avoids a full-file hash.
- **BLAKE3 hash**: most reliable, but requires reading the entire file. Could be
  computed lazily only for files that match on size.

A two-pass approach works well: (1) collect all disappeared files and their
sizes, (2) when inserting a new file whose size matches a disappeared file,
compute hashes on both and match. This avoids hashing files that weren't moved.

**Implementation**: Modify `replace_files_on` to return removed files (with
their IDs and sizes) instead of immediately deleting them. A post-scan
reconciliation pass matches removed files against newly appeared files across
all directories in the same scan. Matched files get an UPDATE to their `dir_id`
and `filename`; unmatched files are deleted with dependent cleanup.

**Done when**: moving a file between directories preserves its `files.id` and
all metadata. A file renamed in place (same directory, different name) is also
tracked.

---

## Subproject 3: Large-Scale Metadata Management

### SP3.1: Metadata Write Path

Safe writes using lofty (+ TagLib for gap formats). Safety invariants:

1. Read current tags first (verify file is parseable)
2. Write to temp file, then rename (atomic on Btrfs)
3. Re-read after write to verify tags persisted
4. Update DB only after verified write
5. Never write to a file that failed to parse on read

Coalesced updates: collect ALL changes for a file across all sources (scripts,
CSV/spreadsheet imports), apply in a single write. Each file touched at most
once.

Plex compatibility: use standard tag field names (ARTIST, ALBUM, ALBUMARTIST,
etc.) that Plex's scanner expects. Test by writing tags and reading back in
strict mode.

**Done when**: `etp meta write <file> --tag artist --value "X"` safely writes.
Batch coalesced writes work. Files never corrupted.

### SP3.2: Lua Scripting Runtime

mlua + LuaJIT embedded in Rust. Scripts receive file metadata, return tag
changes. Batch runner collects changes across all scripts per file, coalesces
into single write.

```lua
function process(file)
    local genre = file:tag("genre")
    if genre == "Electronica" then
        file:set_tag("genre", "Electronic")
    end
end
```

Lua API: `file:tag(name)`, `file:set_tag(name, value)`, `file.path`,
`file.format`, `etp.run(cmd, args)` (for calling external binaries).

**Done when**: Lua scripts read/modify metadata. Batch processing with coalesced
writes works. Script errors don't cause partial writes.

### SP3.3: Quality Checking

`etp meta check` subcommand: validates metadata against rules.

Checks: missing required tags, inconsistent album metadata, missing cover art,
oversized images (> configurable threshold), encoding issues (non-UTF-8),
duplicate files by content hash, missing MusicBrainz GUIDs.

Output: one issue per line, machine-parseable structured format.

**Done when**: `etp meta check <dir>` reports issues. Each check has a test.

### SP3.4: Declarative Transforms

Support for tabular (CSV/spreadsheet) metadata updates alongside Lua scripts.
Example: export genres to CSV, edit in spreadsheet, reimport. The tool reads the
CSV, diffs against current DB state, and applies changes through the same
coalesced write path.

External binary callouts via `etp.run()` in Lua scripts for MusicBrainz/AcoustID
lookups, image optimization (ffmpeg/ImageMagick), etc.

**Done when**: CSV import + Lua scripts + external tool callouts all feed into
the same coalesced write pipeline.

---

## Phase Dependencies

```txt
SP1.1 Workspace Restructure
 └→ SP1.2 SQLite Layer + KDL Config
     └→ SP1.3 DB-Backed Output (BYTE-IDENTICAL GATE)
         └→ SP1.4 State Cleanup
             └→ SP1.5 Python Porcelain
                 └→ SP2.1 Metadata Reading (lofty)
                     ├→ SP2.2 TagLib FFI (parallel)
                     ├→ SP2.3 CAS CLI (parallel, reduced scope)
                     ├→ SP2.4 Cue Sheets (parallel, reduced scope)
                     ├→ SP2.5 Query Interface (incremental)
                     └→ SP2.6 File-Move Tracking (parallel)
                         └→ SP3.1 Write Path
                             ├→ SP3.2 Lua Scripting
                             │   └→ SP3.4 Declarative Transforms
                             └→ SP3.3 Quality Checking
```

## Cross-Cutting

**tokio**: single-threaded runtime (`rt`, not `rt-multi-thread`). Sequential
disk I/O is intentional — minimizes seek on spinning disks.

**Error handling**: `anyhow` in binaries, `thiserror` in `etp-lib`. Scanner
errors log and continue; metadata write errors abort the file but continue the
batch.

**Static linking**: all binaries static for musl except TagLib feature (may
require dynamic). LuaJIT vendored via mlua `vendored` feature.

**Testing**: every requirement has at least one test. Unit tests in each module.
trycmd snapshot tests for CLI behavior. Integration tests for cross-module paths
(scan → DB → output).

**Git workflow**: branch protection on `main`. All changes via feature branch +
PR.

## Verification (SP1)

After each SP1 phase, run:

```bash
cargo test --workspace                                          # all unit + snapshot tests
cargo build --workspace --release                               # verify compilation
# After SP1.3:
diff <(dir-tree-scanner csv fixture) <(etp-csv fixture)         # byte-identical CSV
diff <(dir-tree-scanner tree fixture) <(etp-tree fixture)       # byte-identical tree
```

## Critical Files (existing, to be modified/moved)

- `src/scanner.rs` — core scanning logic, add `scan_to_db` variant
- `src/csv_writer.rs` — add `write_csv_from_db`, preserve byte-order sorting
- `src/tree.rs` — add `render_tree_from_db`, preserve ICU4X collation
- `src/state.rs` — kept during transition for import tool, then removed
- `src/ops.rs` — `load_state`/`save_state` replaced with `open_db`/`ensure_scan`
- `src/bin/dir_tree_scanner.rs` — split into `etp-csv` and `etp-tree`
- `Cargo.toml` — becomes workspace manifest
- `scripts/catalog-nas.py` — evolves into `etp/etp-catalog`
- `scripts/catalog.toml` — converted to KDL, moves to `conf/catalog.kdl`
