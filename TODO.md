# TODO.md – extra stuff I came up with along the way

This file is a place to take notes on additional plans (feature requests,
functionality changes, bug fixes) for existing plans. The items are broken out
by project or subproject, with dependencies between tasks indicated by nesting.
Tasks should be marked as done when they are incorporated into the plan, and
then removed once it has been verified that they have been completely
implemented.

## Backlog (not assigned to a subproject)

- [x] etp-scan: extract the scanning portion into its own plumbing command
- [x] scan/display separation and two-tier filtering
      (docs/plans/2026-03-28-scan-display-separation.md)
  - [x] Phase 1: scan everything, default to `--no-scan`
    - [x] remove `@eaDir` default exclude from etp-scan, etp-tree, etp-csv,
          etp-find (also etp-meta)
    - [x] add `--[no-]scan` boolean pair to etp-tree and etp-csv (default: no
          scan)
    - [x] exit code 2 when no scan exists (for porcelain auto-scan)
  - [x] Phase 2: two-tier display filtering
    - [x] system file patterns (hidden from display, counted in du)
    - [x] user exclude patterns (hidden from display AND du)
    - [x] `--[no-]include-system-files` flag on etp-tree, etp-csv, etp-find,
          etp-query
    - [x] configurable default patterns for both lists
    - [x] `is_system_path` checks all absolute path components — documented as
          safe due to distinctive pattern prefixes; configuration error, not
          architectural limitation
    - [x] `should_show_name` vs `should_show` contract — documented: only use
          `should_show_name` in tree rendering's top-down traversal where
          directories are filtered before descending
  - [x] Phase 3: runtime config + etp-init
    - [x] `config.kdl` with system patterns, user excludes, database nicknames,
          and CAS directory path
    - [x] `etp-init` plumbing command to generate commented config template
    - [x] database nickname resolution in all Rust commands
    - [x] wire up `default-database` from config.kdl — used as fallback when no
          `--db` is specified and no `.etp.db` exists in the target directory
  - [x] Phase 4: smart porcelain dispatch
    - [x] replace os.execv with subprocess.run for orchestrated commands
    - [x] auto-scan on exit code 2 for `etp tree` and `etp csv`
    - [x] argument extraction for directory and --db from argv
  - [x] Phase 5: catalog.py update
    - [x] run etp-scan first, then tree + CSV in parallel
    - [x] remove @eaDir from scan excludes
- [ ] `etp-completions` plumbing command for shell completions (fish, bash, zsh)
      via `eval (etp completions --fish)` etc. Use clap's built-in
      `clap_complete` for the plumbing commands and generate porcelain
      completions (with database nicknames from config.kdl) for the dispatcher.
- [x] create README with description of all porcelain commands and with
      installation instructions
- [ ] write a utility to truncate media files for various formats to just
      include the metadata blocks and enough frame data to be a valid media file
      for that encoding. Useful for gathering test cases for metadata tag
      reading and updating.
- [ ] write a function to fingerprint the metadata blocks without reading the
      whole media file
- [x] store BLAKE3 content hash in the files table during metadata scan to
      simplify move tracking (eliminates I/O-heavy hashing during reconciliation
      and enables content-based deduplication detection)

## Incremental Background Metadata Scanning

- [ ] `etp meta scan --limit N` to process only N unscanned files per invocation
      (default ~1000), enabling batched background ingestion
- [ ] Run niced (`nice -n 19 ionice -c3`) via cron/systemd timer on NAS
- [ ] Porcelain support: `etp catalog --meta-scan` runs batched metadata scans
      across all configured databases

## SP 3.1: Metadata Write Path

- [ ] safe writes via lofty: read → write to temp → rename → re-read → update DB
- [ ] coalesced updates: collect all changes per file, apply in a single write
- [ ] Plex compatibility: use standard tag field names
- [ ] `etp meta write <file> --tag artist --value "X"` CLI

## SP 3.1b: MusicBrainz Read-Through Cache

- [ ] local cache with high/infinite TTL, refresh-on-request
- [ ] batch-first: prefer few large API requests over many small ones
- [ ] rate limiting (1 req/sec authenticated)
- [ ] disc ID → release lookup
- [ ] verify MB metadata is current (for quality checks)

## SP 3.2: Lua Scripting Runtime

- [ ] mlua + LuaJIT embedded in Rust
- [ ] Lua API: file:tag(), file:set_tag(), file.path, file.format, etp.run()
- [ ] batch runner with coalesced writes
- [ ] script errors don't cause partial writes

## SP 3.3: Quality Checking

- [ ] `etp meta check` subcommand
- [ ] missing required tags, inconsistent album metadata
- [ ] missing cover art, oversized images
- [ ] encoding issues (non-UTF-8)
- [ ] duplicate files by content hash
- [ ] missing MusicBrainz GUIDs
- [ ] verify MusicBrainz metadata is current (uses SP3.1b cache)

## SP 3.4: Declarative Transforms

- [ ] CSV/spreadsheet metadata import with diff against DB state
- [ ] external binary callouts via etp.run() in Lua
- [ ] everything feeds into the coalesced write pipeline

## Config Bootstrapping

- [ ] `etp init` should bootstrap `config.kdl` from `catalog.kdl` if it exists:
      generate database nicknames from catalog scan blocks (name → root/db
      mapping), set first scan as default-database
- [ ] Cross-reference databases in `config.kdl` with scans in `catalog.kdl` to
      validate that nicknames point to real catalog entries

## Code Quality Improvements

- [x] `process::exit` in etp-lib functions → return `Result`; binaries use
      anyhow with `?` propagation (ADR 2026-03-28-05)
- [x] Collapse `stream_find_matches` / `collect_find_matches` (4 → 2
      parameterized by `scan_id: Option<i64>`)
- [x] Add `futures-util` for `StreamExt::next()` (replaces `poll_fn`
      boilerplate)
- [x] RAII profiling guard with `Drop` + `maybe_init_profiling` helper
- [x] `ScanOptions` struct for `open_and_resolve_scan` parameters
- [ ] Move single-caller ops.rs functions (`gc_orphan_blobs`,
      `read_file_metadata`) closer to their respective command crates
- [ ] CSV writer: stream from DB with SQL-side sorting instead of loading all
      records into memory (biggest memory improvement for large scans)
- [ ] `resolve_cas_dir`: resolve once per operation and pass `&Path` directly
      instead of `Option<&Path>` per CAS call
- [x] `reconcile_moves`: batch dir_paths pre-fetch into one `WHERE id IN (...)`
      query
- [x] `system_patterns` as `HashSet<String>` for O(1) lookup

## Anime Destination Cache (etp-scan SQLite)

- [ ] Use the etp-scan SQLite database for the anime destination directory
      (`/volume1/video/anime`, 82K files, 1543 dirs) instead of walking:
  - [ ] `scan_dest_ids` → SQL query for `anidb.id`/`tvdb.id` files
  - [ ] `scan_dest_directory` (DestScan) → SQL query instead of `iterdir`
  - [ ] Auto-run `etp-scan` if DB is stale (mtime > threshold)
  - [ ] Fallback to `os.walk` if etp-scan binary or DB unavailable
- [ ] QA subcommand: `etp anime qa --missing-ids` to enumerate destination
      series directories lacking `anidb.id` / `tvdb.id` files
- [ ] Aggressive title matching: use AniDB/TVDB alternate names from cached
      metadata to increase download → Sonarr → destination matching percentage

## Terminal UI Improvements

- [ ] Investigate [Rich](https://github.com/Textualize/rich) for terminal
      rendering — color depth detection/mapping, tables, progress bars, styled
      text. Could replace the manual ANSI plumbing in `colorize.py` and improve
      the triage/series interactive workflow.
- [ ] Evaluate [Textual](https://github.com/Textualize/textual) (TUI framework
      from the same team as Rich) if full interactive screens are needed (e.g.,
      manifest editing with live preview instead of shelling out to Vim).
- [ ] Consider [click](https://github.com/pallets/click) or
      [Typer](https://github.com/fastapi/typer) for CLI argument parsing with
      built-in color/help formatting (Typer is built on click + Rich).
- [ ] Look at [tqdm](https://github.com/tqdm/tqdm) for progress bars during
      long-running operations (metadata scanning, file copying).
- [ ] [Prompt Toolkit](https://github.com/prompt-toolkit/python-prompt-toolkit)
      for advanced interactive prompts (autocomplete, multi-select, fuzzy
      matching) — could improve the ID selection flow in triage/series.

## Post-SP3: Memory Profiling

- [ ] Profile peak memory of all commands and subcommands against a large
      sample: scanning + metadata reading against a real directory (200K+
      files), querying against the resulting database. Identify any O(n) memory
      usage that should be streaming. Candidates: `list_files` callers, tree
      rendering data structures, metadata scan file list, CSV output grouping.
