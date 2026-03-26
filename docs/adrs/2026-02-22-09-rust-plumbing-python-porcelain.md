# Use Rust plumbing with Python porcelain

Date: 2026-02-22

## Status

Accepted

## Context

The toolkit needs two layers: fast, reliable core operations (scanning, database
writes, metadata reads) and flexible orchestration (running scans across
multiple directories, composing workflows, interactive use). The existing Python
orchestrator (`catalog-nas.py`) already demonstrates the pattern — it calls the
Rust binary for heavy lifting and handles workflow logic in Python.

## Decision

Follow the Git model: all `etp-*` commands are plumbing — single-purpose tools
whether implemented in Rust (`etp-csv`, `etp-tree`, `etp-find`) or Python
(`etp-anime`, `etp-catalog`). The sole porcelain is `etp`, a Git-style
dispatcher that finds and exec's `etp-<subcommand>`.

Rust plumbing handles performance-critical work (scanning, database, I/O).
Python plumbing handles domain-specific workflows (anime collection management,
catalog orchestration) where flexibility and iteration speed matter more than
raw performance.

The Python plumbing is an installable package (`uv tool install .`) with entry
points in `pyproject.toml`. Rust binaries are deployed to
`~/.local/libexec/etp/` (FHS convention for plumbing not intended for direct
user invocation). The dispatcher searches libexec then `$PATH`.

## Consequences

- Rust handles all performance-critical and correctness-critical work (disk I/O,
  database operations, metadata parsing).
- Python handles domain workflows where flexibility and iteration speed matter.
- New plumbing commands can be added in either language without affecting the
  dispatcher.
- The `etp` dispatcher searches `~/.local/libexec/etp/` then `$PATH`.
- Python plumbing is deployed via `uv tool install` on the NAS (requires Python
  3.14 and uv).
