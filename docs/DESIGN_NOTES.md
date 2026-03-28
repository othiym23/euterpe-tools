# Design Notes

Implementation details and architecture for euterpe-tools. For conventions and
commands, see [CLAUDE.md](../CLAUDE.md). For architectural decisions, see
[docs/adrs/](adrs/).

## Repository Structure

- `crates/` — Rust libraries (etp-lib, etp-cue)
- `cmd/` — all plumbing commands (Rust binaries and Python entry points)
- `pylib/` — Python shared library (`etp_lib`)
- `conf/` — KDL configuration files

## Rust Crates

Library crate (`crates/etp-lib/src/lib.rs`) re-exports shared modules:

- `ops.rs` — shared operations used by all binary crates
- `scanner.rs` — walkdir-based scanning; skips unchanged directories by mtime
- `csv_writer.rs` — sorted CSV output (`path,size,ctime,mtime`)
- `tree.rs` — tree rendering with ICU4X collation for Unicode-aware sorting
- `finder.rs` — regex matching against file records
- `metadata.rs` — audio metadata reading with dual backend: lofty for most
  formats, mediainfo subprocess for WMA/MKA. Extension-based dispatch. Tag names
  normalized to `lowercase_snake_case`. See
  `docs/adrs/2026-03-28-01-mediainfo-over-taglib.md`.
- `cas.rs` — content-addressable blob storage using BLAKE3 hashing with atomic
  filesystem writes (safe on Btrfs)
- `db/mod.rs` — SQLite connection factory (WAL mode, foreign keys, cache
  pragmas); dual-path init: new databases use clean `schema.sql`, existing
  databases use incremental `migrations/`. FK enforcement disabled during
  migrations for table recreation compatibility.
- `db/dao.rs` — all database queries (scan CRUD, file UPSERT, metadata, blobs,
  images, cue sheets, move tracking). `FULL_PATH_SQL` constant for path
  reconstruction used across query functions.
- `config.rs` — KDL configuration parsing
- `paths.rs` — XDG/native path resolution (etcetera crate)
- `profiling.rs` — self-instrumentation (feature-gated behind `profiling`)

Standalone library crate (`etp-cue/`):

- CUE sheet parser, MusicBrainz disc ID computation (SHA-1 + custom Base64), and
  three display formatters (album summary, CUEtools TOC, EAC TOC)
- Supports multi-file CUE sheets via per-file duration accumulation
- No database dependency — pure data transformation

Each binary crate has a `build.rs` that embeds the short git hash in
`--version`. Binary crates: `etp-csv`, `etp-tree`, `etp-find`, `etp-meta`,
`etp-cas`, `etp-query`.

## Python Package

Python commands live in `cmd/etp/etp_commands/`:

- `dispatcher.py` — git-style dispatcher (`etp <cmd>` → `etp-<cmd>`)
- `anime.py` — interactive anime collection manager
- `catalog.py` — KDL-configured catalog orchestrator

Python shared library lives in `pylib/etp_lib/`:

- `paths.py` — XDG-based path resolution and binary search
- `media_parser.py` — tokenizer/parser for anime/media file paths
- `anidb.py`, `tvdb.py` — API clients with local caching

`conf/` contains KDL configuration files.

## Database

SQLite with sqlx, WAL mode, single-threaded tokio (`current_thread`). The
canonical schema is `etp-lib/schema.sql`. Pool is `max_connections(1)` — all
queries are sequential. FK enforcement is disabled during migration execution
(some migrations recreate tables referenced by foreign keys).

Defaults: database is `<dir>/.etp.db`. The scanner indexes everything on disk
(no default excludes). Display-time filtering hides system files and user
excludes — see "Display Filtering" below.

File sync uses UPSERT to preserve file IDs across rescans. When a file's mtime
changes, `metadata_scanned_at` is cleared so the metadata scanner re-reads it.
See `docs/adrs/2026-03-27-03-upsert-file-sync.md`.

File-move tracking: after all directories are flushed, a reconciliation pass
matches removed files against newly appeared files by size, then verifies with
streaming BLAKE3 hash. Matched files get an UPDATE to `dir_id` + `filename`,
preserving their ID and all dependent metadata. Unmatched files are deleted with
dependent cleanup.

## Display Filtering

The scanner indexes everything on disk. Filtering happens at display time via
two independent layers:

1. **System files** (`@eaDir`, `@eaStream`, `.etp.db*`, etc.) — NAS/OS
   byproducts. Hidden from listings by default, but included in `--du` size
   calculations. Shown with `--include-system-files`. Patterns are exact name
   matches against file/directory names.

2. **User excludes** (`.*` by default) — glob patterns matched against filenames
   only (not the full path, since absolute paths may contain unrelated
   dot-directories like macOS tempdir components). Hidden from both listings and
   size calculations.

System files are exempt from user exclude matching. Without this, `.etp.db`
would be caught by the `.*` dotfile pattern and hidden even when
`--include-system-files` is passed. The two filter layers are independent:
system file visibility is controlled by `--[no-]include-system-files`, while
user excludes are controlled by `--exclude` and `--ignore`.

`FilterConfig` in `ops.rs` bundles both pattern lists and the include flag,
providing `should_show()` (for full path + filename checks) and
`should_show_name()` (for individual name checks in tree rendering).

## CLI Boolean Flag Pairs

For flags where both the positive and negative form are meaningful (e.g.,
`--scan` / `--no-scan`, `--include-system-files` / `--no-include-system-files`),
both forms are defined as separate clap args with `default_value_t = false`.
Resolution uses `ops::resolve_bool_pair()`:

- Only `--flag` passed → true
- Only `--no-flag` passed → false
- Neither passed → default
- Both passed → prints a warning to stderr and uses the default

This avoids clap's `overrides_with` (which silently picks the last one) in favor
of explicit conflict detection. The warning helps users who may be combining
flags from shell aliases or scripts without realizing the conflict.

## Profiling

Self-instrumented via `tracing` + `tracing-chrome`, gated behind the `profiling`
Cargo feature. Trace files are named `etp-trace-<binary>-<timestamp>.json` and
written to cwd. On Linux, `/proc/self/io` and `/proc/self/status` metrics are
sampled at phase boundaries. The feature adds no runtime cost when `--profile`
is not passed.

```bash
just build-profile      # native with profiling
just build-nas-profile  # NAS with profiling
etp-csv /path/to/dir --profile
# Open trace in Perfetto: https://ui.perfetto.dev
```

## Cross-Compilation

`.cargo/config.toml` sets the linker for `x86_64-unknown-linux-musl` to
`x86_64-linux-musl-gcc`. Two options:

1. **musl toolchain**: `brew install filosottile/musl-cross/musl-cross`, then
   `rustup target add x86_64-unknown-linux-musl`
2. **cross (Docker-based)**: Must use the git version
   (`cargo install cross --git https://github.com/cross-rs/cross`) — the
   crates.io release (0.2.5) lacks ARM64 Docker image support and fails on Apple
   Silicon.
