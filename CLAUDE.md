# euterpe-tools

Incremental CLI filesystem scanner that produces CSV metadata indexes and text
representations of filesystem trees. Designed for NAS use (spinning disks, RAID
6 with two parity disks) at 200K-500K file scale. For performance, intended to
be run on a Synology DiskStation running DSM 7.3.

Cargo workspace with three crates:

- `etp-lib` — library crate (all shared logic)
- `etp-csv` — CSV output binary
- `etp-tree` — tree output binary

## Build & run

```bash
just build            # native (aarch64-apple-darwin)
just build-nas        # NAS (static binary)
just build-nas-cross  # alternative via cross tool
just deploy           # check + test + build + mount NAS + copy everything

# Usage
etp-csv <directory> [--output <file.csv>] [--state <file.state>] [--exclude <name>...] [-v]
etp-tree <directory> [--state <file.state>] [--exclude <name>...] [-N] [-I <pattern>...] [-a] [-v]
```

Defaults: output is `<dir>/index.csv`, state is `<dir>/.fsscan.state`, exclude
is `@eaDir` (Synology metadata directories). `etp-tree` hides dotfiles by
default (`-a` to show). State file and CSV output are written into the scanned
directory by default (they become part of the scan).

## Architecture

Library crate (`etp-lib/src/lib.rs`) re-exports shared modules:

- `ops.rs` — shared operations: `validate_directory`, `resolve_state_path`,
  `load_state`, `run_scan`, `save_state`, `write_csv`, `render_tree`,
  `parse_ignore_patterns`
- `state.rs` — `ScanState`: `HashMap<String, DirEntry>`, rkyv serialized with
  `FSSN` magic + version header. `LoadOutcome` enum for validation
- `scanner.rs` — walkdir-based scanning; skips unchanged directories by mtime
- `csv_writer.rs` — sorted CSV (`path,size,ctime,mtime`)
- `tree.rs` — tree rendering with ICU4X collation for Unicode-aware sorting

Each binary crate has a `build.rs` that embeds the short git hash in
`--version`.

### Key design decisions

Architectural decisions are recorded in `docs/adrs/` using the naming convention
`YYYY-MM-DD-NN-decision-name.md`. Key decisions affecting implementation:

- **Incremental scanning**: directory mtime is the cache key. Unchanged
  directories cost one stat call instead of N.
- **Unix-only**: uses `std::os::unix::fs::MetadataExt` for ctime/mtime.
- **rkyv 0.8** for state serialization — see
  `docs/adrs/2026-02-15-01-rkyv-state-serialization.md`. `ScanState.dirs` uses
  `String` keys (not `PathBuf`) for rkyv compatibility. `save()` writes to
  `.tmp` then renames for atomicity. Changing `FileEntry` or `DirEntry` structs
  invalidates state files; bump `VERSION` if the format changes.
- **ICU4X collation** for tree sort order — see
  `docs/adrs/2026-02-15-02-icu4x-collation.md`. CSV uses byte-order sorting for
  determinism.

## Testing

Unit tests in each module. CLI snapshot tests use trycmd — see
`docs/adrs/2026-02-14-02-trycmd-snapshot-tests.md`.

- `etp-csv/tests/cmd/` — CSV snapshot tests (4 tests)
- `etp-tree/tests/cmd/` — tree snapshot tests (3 tests)

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

## Scripts

`scripts/` contains a Python orchestrator (`catalog-nas.py`) that drives the
scanner across multiple directory trees, configured via `catalog.toml`. Tests in
`test_catalog.py`.

```bash
cd scripts && uv sync     # creates .venv with ruff, pyright, pytest
just check                 # clippy + ruff + pyright
just test                  # cargo test + pytest
```

## Formatting

Always run `just format` before finishing work. This runs `cargo fmt` (Rust),
`ruff format` (Python), and `prettier` (Markdown).

## Git workflow

Branch protection is enabled on `main`. All changes must go through a feature
branch and pull request — never commit directly to `main`.

## Documentation

- Implementation plans: `docs/plans/YYYY-MM-DD-plan-name.md`
- Architecture decision records: `docs/adrs/YYYY-MM-DD-NN-decision-name.md`

Record new architectural decisions as ADRs. Use the Nygard template (Status,
Context, Decision, Consequences). Keep each ADR concise. When a decision
supersedes an earlier one, update the status of both ADRs with cross-references.

## Cross-compilation

`.cargo/config.toml` sets the linker for `x86_64-unknown-linux-musl` to
`x86_64-linux-musl-gcc`. Two options:

1. **musl toolchain**: `brew install filosottile/musl-cross/musl-cross`, then
   `rustup target add x86_64-unknown-linux-musl`
2. **cross (Docker-based)**: Must use the git version
   (`cargo install cross --git https://github.com/cross-rs/cross`) — the
   crates.io release (0.2.5) lacks ARM64 Docker image support and fails on Apple
   Silicon.
