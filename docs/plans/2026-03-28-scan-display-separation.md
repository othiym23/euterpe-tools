# Scan/Display Separation, Two-Tier Filtering, Smart Porcelain

## Context

The current scanner excludes `@eaDir` (and similar NAS system directories) at
scan time via `WalkDir::filter_entry()`, so they never enter the database. This
corrupts `--du` size calculations — those directories contain real files whose
sizes should count toward disk usage. The fix: scan everything, filter at
display time.

This also bundles three related improvements: inverting `--no-scan` to be the
default (with `etp-scan` as the explicit scan step), adding smart porcelain
dispatch that auto-scans when needed, and adding a config file for database
nicknames and configurable filter patterns.

## Phase 1: Scan Everything, Default to No-Scan

**Goal**: Scanner indexes all files (including system files and `.etp.db*`).
etp-tree/etp-csv default to reading from an existing DB rather than scanning.

### etp-scan (`cmd/etp-scan/src/main.rs`)

- Change `--exclude` default from `["@eaDir"]` to `[]` (empty)
- Scanner indexes everything, including `.etp.db*` files (they have real sizes
  and will be filtered as system files at display time)

### Scanner (`crates/etp-lib/src/scanner.rs`)

- Remove the comment about `@eaDir` on line 57-58
- No longer skip `.etp.db*` — these are treated as system files in Phase 2

### etp-tree (`cmd/etp-tree/src/main.rs`)

- Change `--exclude` default from `["@eaDir"]` to `[]`
- Use clap's `--[no-]scan` boolean pair: `--scan` enables scanning, `--no-scan`
  disables it. Default: no scan. clap 4 supports this via two args with
  `overrides_with` or via custom logic. Keep `--no-scan` working for catalog.py
  backward compat.
- When no scan exists and `--scan` not passed, exit with code 2 (not 1)
- Define `EXIT_NO_SCAN: i32 = 2` constant in `crates/etp-lib/src/ops.rs`

### etp-csv (`cmd/etp-csv/src/main.rs`)

- Same changes as etp-tree: empty default excludes, `--[no-]scan`, exit code 2

### etp-find (`cmd/etp-find/src/main.rs`)

- Change `--exclude` default from `["@eaDir"]` to `[]`

## Phase 2: Two-Tier Display Filtering

**Goal**: Two configurable pattern lists control what's shown in output.

### Filtering model

| Category                                      | Scanned? | In size/du? | Displayed? | Override flag                 |
| --------------------------------------------- | -------- | ----------- | ---------- | ----------------------------- |
| System files (`@eaDir`, `.etp.db*`, etc.)     | Yes      | Yes         | No         | `--[no-]include-system-files` |
| User excludes (`--exclude`, `--ignore`, `.*`) | Yes      | No          | No         | (remove from exclude list)    |
| All other files                               | Yes      | Yes         | Yes        | —                             |

### Default system file patterns (`ops.rs`)

```rust
pub const DEFAULT_SYSTEM_PATTERNS: &[&str] = &[
    "@eaDir", "@eaStream", "@tmp", "@SynoResource",
    "@SynoEAStream", "#recycle", ".SynologyWorkingDirectory",
    ".etp.db", ".etp.db-wal", ".etp.db-shm",
];
```

### Default user exclude patterns (`ops.rs`)

```rust
pub const DEFAULT_USER_EXCLUDES: &[&str] = &[
    ".*",        // dotfiles (glob pattern)
    ".DS_Store", // macOS (redundant with .* but explicit for clarity)
];
```

User excludes use glob pattern matching (same as `--ignore` in etp-tree, via the
`glob` crate's `Pattern::matches()`). This matches against filenames and
directory name components.

### `--[no-]include-system-files`

Boolean pair flag, default false (system files hidden). Clap implementation same
as `--[no-]scan`.

### Filtering approach

Post-query in Rust using `is_excluded_path()` (already exists) extended to
support glob patterns. Two separate filter passes:

1. System files: filter from display, but NOT from size aggregation
2. User excludes: filter from both display AND size aggregation

### Changes

**`crates/etp-lib/src/ops.rs`**:

- Add `DEFAULT_SYSTEM_PATTERNS` and `DEFAULT_USER_EXCLUDES` constants
- Add `is_system_path()` (matches path components against system patterns)
- Add `is_user_excluded()` (matches path components and filenames against glob
  patterns)
- Update `render_tree_from_db` signature: add `system_patterns`,
  `user_excludes`, `include_system_files`
- Update `collect_find_matches` / `stream_find_matches`: add system filtering
- `render_du`: does NOT filter system files (they count toward size). User
  excludes applied to du would need a separate code path — defer this.

**`crates/etp-lib/src/tree.rs`**:

- Add `system_patterns` to `TreeContext`
- In `merge_entries`, filter entries matching system patterns (unless
  `include_system_files` is set)
- System file filtering is separate from the existing `--ignore` glob filtering

**`crates/etp-lib/src/csv_writer.rs`**:

- Add system file filtering to `write_csv_from_db` (same shape as existing
  exclude filtering)

**Rust commands** (etp-tree, etp-csv, etp-find, etp-query):

- Add `--[no-]include-system-files` flag (default false)
- Load system and user exclude patterns from config file if it exists, fall back
  to `DEFAULT_SYSTEM_PATTERNS` / `DEFAULT_USER_EXCLUDES`
- Pass through to library functions

## Phase 3: Runtime Config + etp-init

**Goal**: `config.kdl` provides database nicknames, configurable system/exclude
patterns, and a default database. `etp-init` creates it.

### Config file location

`paths::config_file()` already returns `{config_dir}/config.kdl`. Separate from
`catalog.kdl` (orchestration config). `config.kdl` is runtime defaults,
`catalog.kdl` drives batch catalog generation.

### KDL schema

```kdl
// Default database nickname — used when no --db is specified
// and no .etp.db exists in the target directory.
// default-database "music"

// System files: NAS/OS byproducts. Always scanned, counted in
// disk usage, but hidden from listings unless --include-system-files
// is passed.
system-files {
    pattern "@eaDir"
    pattern "@eaStream"
    pattern "@tmp"
    pattern "@SynoResource"
    pattern "@SynoEAStream"
    pattern "#recycle"
    pattern ".SynologyWorkingDirectory"
    pattern ".etp.db"
    pattern ".etp.db-wal"
    pattern ".etp.db-shm"
}

// User excludes: hidden from listings AND excluded from size
// calculations. Uses glob patterns matched against file/directory
// names.
user-excludes {
    pattern ".*"
}

// Database nicknames: map short names to root + db path pairs.
// Use `etp tree music` instead of `etp tree /volume1/music --db /path/to/db`.
// database "music" {
//     root "/volume1/music"
//     db "/path/to/music.db"
// }
```

### etp-init command (`cmd/etp-init/`)

New Rust plumbing command:

```
etp-init [--force]
```

- Creates `{config_dir}/config.kdl` with the commented template above
- Errors if file already exists (unless `--force`)
- Prints the path it wrote to
- Add to workspace `Cargo.toml` and dispatcher's `BUILTIN_COMMANDS`

### RuntimeConfig (`crates/etp-lib/src/config.rs`)

```rust
pub struct RuntimeConfig {
    pub default_database: Option<String>,
    pub system_patterns: Vec<String>,
    pub user_excludes: Vec<String>,
    pub databases: Vec<DatabaseEntry>,
}

pub struct DatabaseEntry {
    pub name: String,
    pub root: PathBuf,
    pub db: PathBuf,
}
```

- `load_runtime_config()` → reads from `paths::config_file()`, falls back to
  defaults if file doesn't exist
- `resolve_database(name: &str) -> Option<&DatabaseEntry>`

### Rust commands (etp-tree, etp-csv, etp-find, etp-query, etp-scan)

- On startup, load runtime config
- If the `directory` argument doesn't exist as a filesystem path, try resolving
  it as a database nickname
- If nickname resolves, use `root` as directory and `db` as database path
- System patterns from config override `DEFAULT_SYSTEM_PATTERNS`
- User excludes from config are merged with CLI `--exclude` values

## Phase 4: Smart Porcelain Dispatch

**Goal**: `etp tree <dir>` and `etp csv <dir>` auto-scan when no DB exists.
Nicknames work end-to-end.

### Exit code convention

- Exit 1: general error
- Exit 2: no scan exists for this directory (recoverable by scanning first)

Defined as `pub const EXIT_NO_SCAN: i32 = 2` in `ops.rs` (from Phase 1).

### Dispatcher changes (`cmd/etp/etp_commands/dispatcher.py`)

Replace `os.execv` with `subprocess.run` for orchestrated commands:

```python
ORCHESTRATED = {"tree", "csv", "find", "query"}

def main():
    ...
    if cmd in ORCHESTRATED:
        result = subprocess.run([exe] + sys.argv[2:])
        if result.returncode == 2:
            directory, db = extract_target(sys.argv[2:])
            scan_exe = find_binary("etp-scan")
            scan_cmd = [scan_exe, directory]
            if db:
                scan_cmd += ["--db", db]
            subprocess.run(scan_cmd, check=True)
            result = subprocess.run([exe] + sys.argv[2:])
        return result.returncode
    else:
        os.execv(exe, [exe] + sys.argv[2:])
```

Commands that don't benefit from orchestration (`scan`, `meta`, `cas`, `init`)
keep `os.execv` for zero overhead.

### Argument extraction

Lightweight parser that scans argv for the first non-flag positional argument
and `--db <path>`. Does not need full arg parsing — just enough to construct the
`etp-scan` invocation.

## Phase 5: Catalog Update

**Goal**: catalog.py scans first, then generates tree + CSV in parallel.

### Changes (`cmd/etp/etp_commands/catalog.py`)

Restructure `run_scan()`:

1. **Scan phase**: `etp-scan <disk> --db <db_file>` (no excludes)
2. **Output phase**: Run `etp-tree` and `etp-csv` concurrently (both read-only)
   - Use `concurrent.futures.ThreadPoolExecutor(max_workers=2)` or two `Popen`
   - Both commands default to no-scan now, no flag needed
   - Remove `--no-scan` from the etp-csv invocation (it's the default)
3. Remove `@eaDir` from any exclude arguments
4. Report scan time separately from tree+CSV generation time

## Phase Order

1 → 2 → 3 → 4 → 5

Phases 1-2 are tightly coupled (need system files in DB before filtering them).
Phase 3 is needed before Phase 4 (nicknames need config). Phase 5 can follow any
time after Phase 1.

Use feature branch `rei/sp3.0/scan-display-separation` with sub-branches per
phase merging into it.

## Files changed (by phase)

### Phase 1

| File                            | Change                                     |
| ------------------------------- | ------------------------------------------ |
| `cmd/etp-scan/src/main.rs`      | Empty default excludes                     |
| `crates/etp-lib/src/scanner.rs` | Remove @eaDir comment                      |
| `crates/etp-lib/src/ops.rs`     | Add `EXIT_NO_SCAN` constant                |
| `cmd/etp-tree/src/main.rs`      | `--[no-]scan`, invert default, exit code 2 |
| `cmd/etp-csv/src/main.rs`       | Same as etp-tree                           |
| `cmd/etp-find/src/main.rs`      | Empty default excludes                     |

### Phase 2

| File                               | Change                                                            |
| ---------------------------------- | ----------------------------------------------------------------- |
| `crates/etp-lib/src/ops.rs`        | `DEFAULT_SYSTEM_PATTERNS`, `DEFAULT_USER_EXCLUDES`, filtering fns |
| `crates/etp-lib/src/tree.rs`       | System file filtering in TreeContext                              |
| `crates/etp-lib/src/csv_writer.rs` | System file filtering                                             |
| `cmd/etp-tree/src/main.rs`         | `--[no-]include-system-files` flag                                |
| `cmd/etp-csv/src/main.rs`          | `--[no-]include-system-files` flag                                |
| `cmd/etp-find/src/main.rs`         | `--[no-]include-system-files` flag                                |
| `cmd/etp-query/src/main.rs`        | `--[no-]include-system-files` flag                                |

### Phase 3

| File                                 | Change                                   |
| ------------------------------------ | ---------------------------------------- |
| `crates/etp-lib/src/config.rs`       | `RuntimeConfig`, `load_runtime_config()` |
| `cmd/etp-init/Cargo.toml`            | New crate                                |
| `cmd/etp-init/src/main.rs`           | Config file generator                    |
| `Cargo.toml`                         | Add etp-init to workspace                |
| `cmd/etp/etp_commands/dispatcher.py` | Add `init` to BUILTIN_COMMANDS           |
| All Rust commands                    | Load config, resolve nicknames           |
| `conf/config.kdl`                    | Example/default config file              |

### Phase 4

| File                                 | Change                                    |
| ------------------------------------ | ----------------------------------------- |
| `cmd/etp/etp_commands/dispatcher.py` | subprocess.run, auto-scan, arg extraction |

### Phase 5

| File                              | Change                        |
| --------------------------------- | ----------------------------- |
| `cmd/etp/etp_commands/catalog.py` | Scan-first, parallel tree+CSV |

## Verification

```bash
# After each phase:
just check && just test

# Phase 1: verify scanning indexes @eaDir and .etp.db
etp-scan /path/to/dir && etp-query --db /path/to/.etp.db find --tag path --value @eaDir

# Phase 2: verify system files hidden but counted in du
etp-tree /path/to/dir --du                    # @eaDir hidden, sizes include it
etp-tree /path/to/dir --include-system-files  # @eaDir visible
etp-tree /path/to/dir --no-include-system-files  # explicit hide (default)

# Phase 3: verify etp-init and nicknames
etp-init && cat ~/.config/euterpe-tools/config.kdl
etp-scan music && etp-tree music

# Phase 4: verify auto-scan
rm /path/to/.etp.db && etp tree /path/to/dir  # should auto-scan then show tree
etp csv /path/to/dir  # also auto-scans if needed

# Phase 5: verify catalog
etp catalog --dry-run
```
