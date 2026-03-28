# TODO.md – extra stuff I came up with along the way

This file is a place to take notes on additional plans (feature requests,
functionality changes, bug fixes) for existing plans. The items are broken out
by project or subproject, with dependencies between tasks indicated by nesting.
Tasks should be marked as done when they are incorporated into the plan, and
then removed once it has been verified that they have been completely
implemented.

## Backlog (not assigned to a subproject)

- [x] etp-scan: extract the scanning portion into its own plumbing command
- [ ] scan/display separation and two-tier filtering
      (docs/plans/2026-03-28-scan-display-separation.md)
  - [ ] Phase 1: scan everything, default to `--no-scan`
    - [ ] remove `@eaDir` default exclude from etp-scan, etp-tree, etp-csv,
          etp-find
    - [ ] add `--[no-]scan` boolean pair to etp-tree and etp-csv (default: no
          scan)
    - [ ] exit code 2 when no scan exists (for porcelain auto-scan)
  - [ ] Phase 2: two-tier display filtering
    - [ ] system file patterns (hidden from display, counted in du)
    - [ ] user exclude patterns (hidden from display AND du)
    - [ ] `--[no-]include-system-files` flag on etp-tree, etp-csv, etp-find,
          etp-query
    - [ ] configurable default patterns for both lists
  - [ ] Phase 3: runtime config + etp-init
    - [ ] `config.kdl` with system patterns, user excludes, database nicknames
    - [ ] `etp-init` plumbing command to generate commented config template
    - [ ] database nickname resolution in all Rust commands
  - [ ] Phase 4: smart porcelain dispatch
    - [ ] replace os.execv with subprocess.run for orchestrated commands
    - [ ] auto-scan on exit code 2 for `etp tree` and `etp csv`
    - [ ] argument extraction for directory and --db from argv
  - [ ] Phase 5: catalog.py update
    - [ ] run etp-scan first, then tree + CSV in parallel
    - [ ] remove @eaDir from scan excludes
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
