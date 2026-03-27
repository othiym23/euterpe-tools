# 02. Btrfs COW Reflinks for File Copies

Date: 2026-03-24

## Status

Accepted

## Context

etp-anime copies media files from source directories (Sonarr downloads, manual
downloads) into the organized anime collection. Anime episodes are typically 1–3
GB each, and a full series import can involve 12–50 files. The source and
destination directories reside on the same Btrfs volume on a Synology NAS
running DSM 7.3.

A naive copy would duplicate the file data on disk, doubling storage consumption
for files that exist in both the source and destination. Since the NAS manages a
large media collection on spinning disks in a RAID 6 array, both storage
capacity and write I/O are meaningful constraints.

Btrfs supports copy-on-write (COW) reflinks via `cp --reflink=always`, which
creates a new directory entry pointing to the same underlying data extents. The
copy is nearly instant regardless of file size and consumes no additional disk
space until one of the copies is modified (which never happens for media files).

## Decision

etp-anime uses `cp --reflink=always` for all file copies. On non-Linux platforms
(e.g., macOS during development), the tool detects the platform and falls back
to a regular `cp` with a warning, since APFS and HFS+ do not support Btrfs-style
reflinks.

The tool does not attempt to detect the filesystem type at runtime. The
`--reflink=always` flag will fail with a clear error if used on a non-COW
filesystem on Linux, which is preferable to silently consuming double the
storage.

## Consequences

- Importing a full series uses negligible additional disk space — the files
  occupy space only once even though they appear in both the source and
  destination directories.
- Copy operations complete in milliseconds rather than minutes, making the
  interactive workflow responsive even for large BD remux files.
- etp-anime is effectively tied to running on the NAS where the Btrfs volume is
  mounted. Running it on macOS or another platform works for testing with
  `--dry-run` but produces a warning and a full copy for actual file operations.
- Source files can be safely deleted later without affecting the copies, since
  Btrfs tracks reference counts on data extents.
- The tool assumes source and destination are on the same Btrfs volume. Cross-
  volume copies (which would fail with `--reflink=always`) are not supported and
  will produce a clear error.
