# Design Notes

Implementation details and architecture for euterpe-tools. For conventions and
commands, see [CLAUDE.md](../CLAUDE.md). For architectural decisions, see
[docs/adrs/](adrs/).

## Rust Crates

Library crate (`etp-lib/src/lib.rs`) re-exports shared modules:

- `ops.rs` — shared operations used by all binary crates
- `scanner.rs` — walkdir-based scanning; skips unchanged directories by mtime
- `csv_writer.rs` — sorted CSV output (`path,size,ctime,mtime`)
- `tree.rs` — tree rendering with ICU4X collation for Unicode-aware sorting
- `finder.rs` — regex matching against file records
- `metadata.rs` — audio metadata reading via lofty, tag normalization to
  `lowercase_snake_case`, embedded image and cue sheet extraction
- `cas.rs` — content-addressable blob storage using BLAKE3 hashing with atomic
  filesystem writes
- `db/mod.rs` — SQLite connection factory (WAL mode, foreign keys, cache
  pragmas); dual-path init: new databases use clean `schema.sql`, existing
  databases use incremental `migrations/`
- `db/dao.rs` — all database queries (scan CRUD, file UPSERT, metadata, blobs,
  images, cue sheets)
- `config.rs` — KDL configuration parsing
- `paths.rs` — XDG/native path resolution (etcetera crate)
- `profiling.rs` — self-instrumentation (feature-gated behind `profiling`)

Each binary crate has a `build.rs` that embeds the short git hash in
`--version`.

## Python Package

`etp/` is an installable Python package (`uv tool install .`):

- `src/etp_commands/dispatcher.py` — git-style dispatcher (`etp <cmd>` →
  `etp-<cmd>`)
- `src/etp_commands/anime.py` — interactive anime collection manager
  (triage/series/episode subcommands)
- `src/etp_commands/catalog.py` — KDL-configured catalog orchestrator
- `src/etp_lib/paths.py` — XDG-based path resolution and binary search
- `src/etp_lib/media_parser.py` — tokenizer/parser for anime/media file paths

`scripts/` contains the legacy Python orchestrator (`catalog-nas.py`) driven by
`catalog.toml`, superseded by `etp catalog` with KDL config.

`conf/` contains KDL configuration files (`catalog.kdl`).

## Database

SQLite with sqlx, WAL mode, single-threaded tokio (`current_thread`). The
canonical schema is `etp-lib/schema.sql`.

Defaults: database is `<dir>/.etp.db`, exclude is `@eaDir` (Synology metadata
directories, filtered at scan time so walkdir never descends into them).

File sync uses UPSERT to preserve file IDs across rescans. When a file's mtime
changes, `metadata_scanned_at` is cleared so the metadata scanner re-reads it.
See `docs/adrs/2026-03-27-03-upsert-file-sync.md`.

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
