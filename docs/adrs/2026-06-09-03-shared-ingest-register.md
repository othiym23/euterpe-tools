# ADR: Shared Ingest Register Across Anime, Movies, and Television

**Date:** 2026-06-09 **Status:** Accepted

## Context

`etp anime`, `etp movies`, and `etp television` all draw source files from the
same downloads directory (`/volume1/docker/pvr/data/downloads`). The anime
triage flow tracked already-copied files in a private register at
`cache_dir("triage")/copied.json`. With three commands consuming one pool, a
per-command register would let the same file be processed (or at least
re-surfaced) by more than one command.

## Decision

One register, shared by all ingest commands, implemented in
`pylib/etp_lib/ingest_register.py`:

- Location: `cache_dir("ingest")/copied.json` — a JSON array of resolved
  absolute source paths, the same format the anime register used.
- Saves are atomic (write temp file, `os.replace`).
- **Migration:** `load_register()` merges in the legacy
  `cache_dir("triage")/copied.json` when present; the first `save_register()`
  writes the merged set to the new location. The legacy file is left in place as
  a read-only fallback and can be deleted once the new register has been
  written.

`etp anime` was refactored to use the shared module (its only observable change
is the register location). The movies/television planner filters registered
sources out of plans unless `--force` is given; apply records sources after
placement.

A worst-case migration failure is benign by construction: a file missing from
the register is re-planned, and apply then detects its destination already
exists — no data loss is possible, only a redundant plan entry.

## Consequences

- A file ingested by any command is never double-counted by another.
- The register keys on paths as seen by the machine that ran the ingest
  (`/volume1/...` on the NAS). Running ingests from the laptop against
  `/Volumes/...` mounts would record different keys; ingestion is expected to
  run on the NAS.
- Entries explicitly marked `skip` in a plan manifest are _not_ registered —
  they resurface in the next plan until resolved or ingested.
