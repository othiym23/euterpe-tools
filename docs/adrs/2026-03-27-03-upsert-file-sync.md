# UPSERT-Based File Sync

- **Status**: Accepted
- **Date**: 2026-03-27

## Context

The original `replace_files` function deleted all file rows for a directory and
re-inserted them on each rescan. This worked when `files.id` had no dependents,
but SP2.1 adds `metadata`, `cue_sheets`, and `embedded_images` tables that
reference `files(id)` with `ON DELETE RESTRICT`. The delete+insert approach
would either:

1. Fail with a foreign key violation (if metadata exists), or
2. Require deleting all metadata before every rescan (losing work).

## Decision

Replace the delete+insert pattern with
`INSERT ... ON CONFLICT(dir_id, filename) DO UPDATE SET`. This preserves file
IDs across rescans so that metadata rows remain correctly associated with their
files.

Key behaviors:

- **Stable file IDs**: A file that exists across rescans keeps the same
  `files.id`. Metadata, images, and cue sheets remain linked.
- **Staleness detection**: When a file's `mtime` changes, `metadata_scanned_at`
  is set to `NULL` via a `CASE` expression in the UPSERT. The metadata scanner
  uses this to identify files needing re-reading.
- **Explicit removal**: Files no longer on disk are identified by comparing the
  UPSERT set against existing rows. Before deleting a removed file,
  `delete_file_dependents` cleans up all child rows (metadata, images, cue
  sheets) and decrements blob ref counts.
- **Orphan blob cleanup**: `cleanup_orphan_blobs` returns hashes of blobs with
  zero ref count. Callers are responsible for removing the corresponding CAS
  files. The `gc_orphan_blobs` operation provides a full scan for any blobs on
  disk not referenced by the database.

## Consequences

- **Positive**: Metadata survives filesystem rescans. Only changed files need
  re-reading, enabling incremental metadata scanning.
- **Positive**: The UPSERT approach has no measurable performance difference
  from delete+insert at tested scale (2000 files / 20 directories).
- **Negative**: Every future table that references `files(id)` must be handled
  in `delete_file_dependents` and `delete_directory_dependents`. Forgetting to
  add a new table there will cause foreign key violations on file removal.
- **Negative**: Orphan blob cleanup is the caller's responsibility. The DAO
  layer returns orphan hashes but has no filesystem dependency.
