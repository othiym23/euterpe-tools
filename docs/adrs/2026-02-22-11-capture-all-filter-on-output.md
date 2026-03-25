# Capture everything, filter on output

Date: 2026-02-22

## Status

Superseded by
[2026-03-24-01-scan-time-exclude-filtering](2026-03-24-01-scan-time-exclude-filtering.md)
— `--exclude` now filters at scan time as well as output time.

## Context

The original scanner accepted an `exclude` list that skipped directories during
the walk. This meant excluded directories were never recorded, making it
impossible to compute accurate directory and subtree sizes across the full tree.
Different output formats (CSV, tree) may also need different filtering rules
(ignore patterns, hidden file visibility, directory name exclusions), and
applying them at scan time couples the scanner to each output format.

## Decision

The DB-backed scanner (`scan_to_db`) captures ALL paths with no exclusion
parameter. Filtering moves entirely to the output layer:

- CSV writer accepts an `exclude` list and skips files in matching directories
- Tree renderer accepts ignore patterns and a `show_hidden` flag, applied at
  render time
- Directory name exclusions (e.g., `@eaDir`) are output filters, not scan
  filters

## Consequences

- The database contains a complete picture of the filesystem, enabling accurate
  size calculations (directory + subtree sizes) regardless of output filters.
- Scan time increases slightly for trees with many excluded directories (e.g.,
  Synology `@eaDir`), but these directories are typically small.
- Different output formats can apply independent filtering without rescanning.
- The old `scan()` function retains its `exclude` parameter for backward
  compatibility until removed in SP1.4.
