# euterpe-tools

Incremental CLI filesystem scanner that produces CSV metadata indexes and text
representations of filesystem trees. Designed for NAS use (spinning disks, RAID
6 with two parity disks) at 200K-500K file scale. For performance, intended to
be run on a Synology DiskStation running DSM 7.3.

Cargo workspace with four crates:

- `etp-lib` — library crate (all shared logic)
- `etp-csv` — CSV output binary
- `etp-tree` — tree output binary
- `etp-find` — regex-based file search binary

Python porcelain in `etp/`:

- `etp` — git-style dispatcher (`etp <cmd>` → `etp-<cmd>`)
- `etp-catalog` — KDL-configured catalog orchestrator
- `kdl/` — vendored `kdl-py` 1.2.0 (KDL 1 parser, no install step needed)

## Build & run

```bash
just build-smoketest  # native (aarch64-apple-darwin), verify all crates compile
just build            # native (aarch64-apple-darwin)
just build-nas        # NAS (static binary)
just build-nas-cross  # alternative via cross tool
just build-profile    # native with profiling instrumentation
just build-nas-profile # NAS with profiling instrumentation
just deploy           # check + test + build + mount NAS + copy everything

# Usage
etp-csv <directory> [--output <file.csv>] [--db <file.db>] [--exclude <name>...] [--find <pattern> [-i]] [--no-scan] [-v]
etp-tree <directory> [--db <file.db>] [--exclude <name>...] [--find <pattern> [-i]] [--no-scan] [--du [--du-subs]] [-N] [-I <pattern>...] [-a] [-v]
etp-find <pattern> [-R <directory>] [--tree=<file>] [--csv=<file>] [--size] [-i] [--db <path>] [--exclude <name>...] [--no-scan] [-v]

# Via dispatcher
etp tree <directory> [args...]
etp find <pattern> [-R <directory>] [args...]
etp catalog [--dry-run] [config.kdl]
```

Defaults: output is `<dir>/index.csv`, database is `<dir>/.etp.db`, exclude is
`@eaDir` (Synology metadata directories, filtered at scan time so walkdir never
descends into them). `etp-tree` hides dotfiles by default (`-a` to show).
Database and CSV output are written into the scanned directory by default (they
become part of the scan).

## Architecture

Library crate (`etp-lib/src/lib.rs`) re-exports shared modules:

- `ops.rs` — shared operations: `validate_directory`, `parse_ignore_patterns`,
  `run_scan_to_db`, `write_csv_from_db`, `render_tree_from_db`, `render_du`,
  `stream_find_matches`, `collect_find_matches`, `write_find_csv`,
  `render_find_tree`
- `scanner.rs` — walkdir-based scanning; skips unchanged directories by mtime
- `csv_writer.rs` — sorted CSV (`path,size,ctime,mtime`)
- `tree.rs` — tree rendering with ICU4X collation for Unicode-aware sorting
- `finder.rs` — regex matching against file records
- `db/` — SQLite database layer (`dao.rs` for queries, `mod.rs` for connection)
- `config.rs` — KDL configuration parsing
- `paths.rs` — XDG-based path resolution
- `profiling.rs` — self-instrumentation (feature-gated, see below)

Each binary crate has a `build.rs` that embeds the short git hash in
`--version`.

### Key design decisions

Architectural decisions are recorded in `docs/adrs/` using the naming convention
`YYYY-MM-DD-NN-decision-name.md`. Key decisions affecting implementation:

- **Incremental scanning**: directory mtime is the cache key. Unchanged
  directories cost one stat call instead of N.
- **Unix-only**: uses `std::os::unix::fs::MetadataExt` for ctime/mtime.
- **SQLite database** for scan state persistence — see
  `docs/adrs/2026-02-22-01-sqlx-sqlite-database.md`. Uses sqlx with WAL mode and
  foreign keys enabled.
- **ICU4X collation** for all output sorting (CSV and tree) — see
  `docs/adrs/2026-02-15-02-icu4x-collation.md`.
- **Explicit deletion** — all foreign keys use `ON DELETE RESTRICT`. Application
  code must delete child rows before parent rows. Never use `ON DELETE CASCADE`.
  See `docs/adrs/2026-02-22-10-explicit-deletion-no-cascade.md`.

## Testing

Unit tests in each module. CLI snapshot tests use trycmd — see
`docs/adrs/2026-02-14-02-trycmd-snapshot-tests.md`. Tests run via
`cargo-nextest` (`cargo nextest run`) for parallel execution and concise output.
Install with `cargo install --locked cargo-nextest`.

- `etp-csv/tests/cmd/` — CSV snapshot tests (4 tests)
- `etp-tree/tests/cmd/` — tree snapshot tests (3 tests)
- `etp-find/tests/cmd/` — find snapshot tests (5 tests)

### trycmd tests

Each `tests/cmd/<name>.toml` defines one CLI invocation. Optional `<name>.in/`
directory provides fixture files when `fs.sandbox = true`. Use
`fs.sandbox = true` and pass `.` as the directory for deterministic output
paths.

```toml
bin.name = "etp-csv"
args = [".", "--some-flag"]
status = "success"
stdout = ""
fs.sandbox = true
```

## Profiling

Self-instrumented profiling via `tracing` + `tracing-chrome`, gated behind the
`profiling` Cargo feature. See
`docs/adrs/2026-03-25-01-self-instrumented-profiling.md`.

```bash
just build-profile      # native with profiling
just build-nas-profile  # NAS with profiling

# Run with profiling enabled (writes trace file to cwd)
etp-csv /path/to/dir --profile
etp-tree /path/to/dir --profile
etp-find pattern -R /path/to/dir --profile
etp catalog --profile

# Open trace in Perfetto: https://ui.perfetto.dev
```

Trace files are named `etp-trace-<binary>-<timestamp>.json` and written to the
current working directory. On Linux, `/proc/self/io` and `/proc/self/status`
metrics are sampled at phase boundaries. The `profiling` feature adds no runtime
cost when `--profile` is not passed (tracing macros are no-ops without a
subscriber).

## Scripts

`scripts/` contains the legacy Python orchestrator (`catalog-nas.py`) driven by
`catalog.toml`. This is superseded by `etp/etp-catalog` with KDL config.

`etp/` contains the current Python porcelain:

- `etp` — git-style dispatcher
- `etp-catalog` — KDL-configured catalog orchestrator
- `test_catalog.py` — pytest tests

`conf/` contains KDL configuration files:

- `catalog.kdl` — catalog scan configuration

```bash
cd etp && uv sync     # creates .venv with kdl-py, ruff, pyright, pytest
just check            # clippy + ruff + pyright
just test             # cargo nextest + pytest (scripts + etp)
```

## Formatting

Always run `just format` before finishing work. This runs `cargo fmt` (Rust),
`ruff format` (Python), and `prettier` (Markdown).

## Git workflow

Branch protection is enabled on `main`. All changes must go through a feature
branch and pull request — never commit directly to `main`.

Large multi-subproject efforts (e.g., SP1.1–SP1.4) use a long-lived feature
branch. Individual subproject branches merge into the feature branch via PR.
Only merge the feature branch to `main` once the entire effort is complete and
production-ready — never merge partial subprojects to `main`.

## Documentation

- Implementation plans: `docs/plans/YYYY-MM-DD-plan-name.md`
- Architecture decision records: `docs/adrs/YYYY-MM-DD-NN-decision-name.md`

Record new architectural decisions as ADRs. Use the Nygard template (Status,
Context, Decision, Consequences). Keep each ADR concise. When a decision
supersedes an earlier one, update the status of both ADRs with cross-references.

After an implementation plan is decided upon, but before beginning
implementation work, save it in the implementation plan directory. Before
committing, check whether there significant enough changes to the plan to
justify updating or correcting it. Implementation plans should be considered
immutable after the branch related to the subproject has been merged or the PR
related to the implementation plan has been closed.

## Cross-compilation

`.cargo/config.toml` sets the linker for `x86_64-unknown-linux-musl` to
`x86_64-linux-musl-gcc`. Two options:

1. **musl toolchain**: `brew install filosottile/musl-cross/musl-cross`, then
   `rustup target add x86_64-unknown-linux-musl`
2. **cross (Docker-based)**: Must use the git version
   (`cargo install cross --git https://github.com/cross-rs/cross`) — the
   crates.io release (0.2.5) lacks ARM64 Docker image support and fails on Apple
   Silicon.
