# 01. Keep-Both Conflict Resolution for Multi-Source Ingestion

Date: 2026-04-02

## Status

Accepted

## Context

When importing anime files into the organized collection, the destination
directory may already contain a file for the same episode — from a different
release group, a different quality encode, or even the same encode obtained from
a different source. Previously the only options were to replace the existing
file or skip the incoming one. Users collecting from multiple sources (Sonarr,
manual downloads, batch BD rips) sometimes want to keep both versions of an
episode while deciding which to retain long-term.

The conflict resolver already compares metadata (release group, source type,
video codec, audio codecs) and CRC32 hashes to determine whether two files are
identical or merely share the same episode slot. When metadata matches and CRC32
matches, the files are byte-identical and auto-replace is safe. All other cases
— same metadata with different CRC, or different metadata entirely — benefit
from a "keep both" option.

## Decision

Add a "both" choice to the interactive conflict resolution prompt alongside
"keep", "replace", and "skip". When the user selects "both":

- The existing file is left in place.
- If the intended destination path already exists (same filename, which happens
  when metadata matches), the incoming file is disambiguated by appending its
  CRC32 hash to the stem: `Show - s1e01 [Group BD,1080p,HEVC] [A1B2C3D4].mkv`.
  The CRC is computed on demand if not already known from earlier conflict
  checking or filename parsing.
- If the intended destination path does not already exist (fuzzy episode match
  found a differently-named file), the incoming file is written to its original
  intended path with no renaming needed, since there is no filename collision.

The "both" action is returned from the conflict resolver but does **not** remove
the existing file (unlike "replace", which unlinks the existing path before
returning).

## Consequences

- Multiple copies of the same episode can coexist in the destination directory,
  one per distinct encode or source.
- CRC32-based disambiguation produces stable, content-addressed filenames that
  won't collide even if the same episode is imported from many sources.
- Manifest validation and downstream tooling must tolerate multiple files per
  episode slot rather than assuming a 1:1 mapping.
- The CRC32 computed during conflict resolution is stashed on the `SourceFile`'s
  `parsed.hash_code` field, avoiding redundant recomputation if the hash is
  needed later in the pipeline.
