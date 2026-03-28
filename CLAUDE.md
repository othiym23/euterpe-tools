# euterpe-tools

Incremental CLI filesystem scanner and audio metadata manager for a Synology
NAS. See [docs/DESIGN_NOTES.md](docs/DESIGN_NOTES.md) for architecture details.

## Repository layout

```
crates/          Rust libraries (etp-lib, etp-cue)
cmd/             All plumbing commands, any language
  etp-csv/       Rust — CSV output
  etp-tree/      Rust — tree output
  etp-find/      Rust — regex file search
  etp-meta/      Rust — metadata scan/read/cue
  etp-scan/      Rust — standalone directory scanner
  etp-cas/       Rust — CAS blob operations
  etp-query/     Rust — database queries
  etp/           Python — porcelain dispatcher + anime + catalog
pylib/           Python shared library (etp_lib)
conf/            KDL configuration files
docs/            Plans, ADRs, design notes
```

## Build & run

```bash
just build-smoketest  # verify all crates compile
just build            # native release (aarch64-apple-darwin)
just build-nas        # NAS release (x86_64-unknown-linux-musl, static)
just deploy           # check + test + build + copy to NAS

# Rust plumbing (in cmd/)
etp-scan <directory> [--db <path>] [-e <name>...] [-v]
etp-csv <directory> [--output <file.csv>] [--db <file.db>] [--exclude <name>...] [-v]
etp-tree <directory> [--db <file.db>] [--exclude <name>...] [--du [--du-subs]] [-v]
etp-find <pattern> [-R <directory>] [--tree=<file>] [--csv=<file>] [--size] [-i] [--db <path>] [-v]
etp-meta scan [-R <directory>] [--db <path>] [-e <name>...] [--force] [-v]
etp-meta read <file> [--images]
etp-meta cue <file> [--audio-file PATH...] [--format summary|cuetools|eac]
etp-cas store <file>
etp-cas get <hash> [-o PATH]
etp-cas gc --db <path> [-v]
etp-cas list
etp-query --db <path> files|tags|find|stats|size|sql [args...]

# Python porcelain (in cmd/etp/)
etp tree <directory> [args...]
etp find <pattern> [-R <directory>] [args...]
etp catalog [--dry-run] [config.kdl]
etp anime triage|series|episode [args...]
```

## Testing

```bash
just check  # clippy + ruff + pyright + ty + prettier
just test   # cargo nextest + pytest
```

Tests run via `cargo-nextest`. CLI snapshot tests use trycmd — each
`tests/cmd/<name>.toml` defines one CLI invocation. Use `fs.sandbox = true` and
pass `.` as the directory for deterministic output paths.

## Formatting

Always run `just format` before finishing work. This runs `cargo fmt` (Rust),
`ruff format` (Python), and `prettier` (Markdown).

Python targets `>=3.14`, so PEP 758 applies: use the unparenthesized
`except X, Y:` style for multi-exception clauses. This is what `ruff format`
enforces.

## Conventions

- **Unix-only**: uses `std::os::unix::fs::MetadataExt` for ctime/mtime.
- **Explicit deletion**: all foreign keys use `ON DELETE RESTRICT`. Application
  code must delete child rows before parent rows. Never use `ON DELETE CASCADE`.
- **Python dependencies welcome**: `uv` is available on the NAS. Prefer stdlib
  when sufficient, but don't reimplement what a well-maintained package does.

## Git workflow

Branch protection is enabled on `main`. All changes go through feature branches
and pull requests.

Large multi-subproject efforts use a long-lived feature branch. Individual
subproject branches merge into the feature branch via PR. Only merge the feature
branch to `main` once the entire effort is complete.

## Documentation

- Implementation plans: `docs/plans/YYYY-MM-DD-plan-name.md`
- Architecture decision records: `docs/adrs/YYYY-MM-DD-NN-decision-name.md`
- Architecture and implementation: `docs/DESIGN_NOTES.md`

Record new architectural decisions as ADRs (Nygard template). When a decision
supersedes an earlier one, update both with cross-references.
