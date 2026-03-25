# Scan-Time Exclude Filtering

## Status

Accepted — supersedes
[2026-02-22-11-capture-all-filter-on-output](2026-02-22-11-capture-all-filter-on-output.md).

## Context

The earlier decision to capture everything at scan time and filter only on
output (ADR 2026-02-22-11) assumed excluded directories like Synology `@eaDir`
were "typically small." In practice, `@eaDir` contains large thumbnail and
metadata trees that dominate scan time on RAID6 spinning disks. Profiling showed
that walking and stat'ing these directories was the majority of scan overhead
for some volumes.

## Decision

The scanner now filters `--exclude` directories at scan time using walkdir's
`filter_entry`, which prevents descent into excluded subtrees entirely. Output
layers (CSV writer, tree renderer) also continue to filter on excluded directory
names for completeness, but the primary filtering happens during the walk.

## Consequences

- Scan time drops significantly for volumes with many or deep excluded
  directories.
- The database no longer contains a complete picture of excluded subtrees. Size
  calculations (`--du`) reflect only non-excluded content, which is the desired
  behavior.
- Output-layer filtering remains as a safety net but is rarely exercised in
  practice since excluded directories are never scanned into the DB.
