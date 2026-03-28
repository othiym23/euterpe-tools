# TODO.md – extra stuff I came up with along the way

This file is a place to take notes on additional plans (feature requests,
functionality changes, bug fixes) for existing plans. The items are broken out
by project or subproject, with dependencies between tasks indicated by nesting.
Tasks should be marked as done when they are incorporated into the plan, and
then removed once it has been verified that they have been completely
implemented.

## Backlog (not assigned to a subproject)

- [ ] etp-scan: extract the scanning portion into its own plumbing command and
      refactor etp-csv/etp-tree to use `--no-scan` by default with scanning
      managed by porcelain or a separate etp-scan invocation
- [ ] create README with description of all porcelain commands and with
      installation instructions
- [ ] write a utility to truncate media files for various formats to just
      include the metadata blocks and enough frame data to be a valid media file
      for that encoding. Useful for gathering test cases for metadata tag
      reading and updating.
- [ ] write a function to fingerprint the metadata blocks without reading the
      whole media file
- [ ] store BLAKE3 content hash in the files table during metadata scan to
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
